# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for the MicroBatcher machinery."""

from __future__ import annotations

import asyncio
import inspect
from unittest.mock import AsyncMock

import pytest

from asanypath.batcher import H2_BATCH_THRESHOLD, MicroBatcher, _make_flush, make_batcher


def _tracking_flush(results_fn=None):
    """Build a flush_fn that resolves futures (like _make_flush) but also records calls.

    *results_fn* receives the items list and returns per-item results.
    Defaults to returning ``None`` for every item.
    """
    calls: list[dict] = []

    async def _flush(*, items, futures, use_h2, **kw):
        calls.append({"items": list(items), "use_h2": use_h2, **kw})
        results = results_fn(items) if results_fn else [None] * len(futures)
        for fut, val in zip(futures, results):
            if not fut.done():
                fut.set_result(val)

    _flush.calls = calls  # type: ignore[attr-defined]
    return _flush


class TestMicroBatcher:
    """Unit tests for MicroBatcher registration and batching."""

    def test_init(self):
        b = MicroBatcher()
        assert b._pending == {}
        assert b._scheduled == set()
        assert b._ops == {}

    def test_register(self):
        b = MicroBatcher()
        submit = b.register(
            "get",
            flush_fn=AsyncMock(),
            key_fn=lambda **kw: "k",
        )
        assert callable(submit)
        assert inspect.iscoroutinefunction(submit)
        assert hasattr(b, "get")
        assert inspect.iscoroutinefunction(b.get)

    def test_getattr_raises_for_unregistered(self):
        b = MicroBatcher()
        with pytest.raises(AttributeError, match="nonexistent"):
            b.nonexistent

    async def test_single_submit(self):
        flush = _tracking_flush(lambda items: [b"data"] * len(items))
        b = MicroBatcher()
        b.register("get", flush_fn=flush, key_fn=lambda **kw: "k")
        result = await b.get(item="path/to/obj")
        assert result == b"data"
        assert len(flush.calls) == 1
        assert flush.calls[0]["items"] == ["path/to/obj"]

    async def test_concurrent_submits_batched(self):
        """Multiple submits in same tick are batched into one flush call."""
        flush = _tracking_flush(lambda items: [f"got-{i}" for i in items])
        b = MicroBatcher()
        b.register("get", flush_fn=flush, key_fn=lambda **kw: "k")

        results = await asyncio.gather(
            b.get(item="file1.txt"),
            b.get(item="file2.txt"),
            b.get(item="file3.txt"),
        )
        assert len(flush.calls) == 1
        assert set(flush.calls[0]["items"]) == {"file1.txt", "file2.txt", "file3.txt"}
        assert set(results) == {"got-file1.txt", "got-file2.txt", "got-file3.txt"}

    async def test_different_keys_separate_batches(self):
        """Different key_fn results -> separate flush calls."""
        flush = _tracking_flush()
        b = MicroBatcher()
        b.register("get", flush_fn=flush, key_fn=lambda bucket="", **kw: bucket)

        await asyncio.gather(
            b.get(item="a.txt", bucket="b1"),
            b.get(item="b.txt", bucket="b2"),
        )
        assert len(flush.calls) == 2

    @pytest.mark.parametrize(
        "count,expected_h2",
        [
            (1, False),
            (H2_BATCH_THRESHOLD, True),
        ],
        ids=["below_threshold", "at_threshold"],
    )
    async def test_h2_flag(self, count, expected_h2):
        """use_h2 reflects whether batch size >= H2_BATCH_THRESHOLD."""
        flush = _tracking_flush()
        b = MicroBatcher()
        b.register("get", flush_fn=flush, key_fn=lambda **kw: "k")

        items = [f"file{i}.txt" for i in range(count)]
        await asyncio.gather(*(b.get(item=x) for x in items))
        assert flush.calls[0]["use_h2"] is expected_h2

    async def test_kwargs_forwarded_to_flush(self):
        """Extra kwargs from submit are forwarded to the flush function."""
        flush = _tracking_flush()
        b = MicroBatcher()
        b.register("get", flush_fn=flush, key_fn=lambda region="", **kw: region)

        await b.get(item="obj.txt", region="us-east-1")
        assert flush.calls[0]["region"] == "us-east-1"


class TestMakeFlush:
    """Test the _make_flush factory."""

    @pytest.mark.parametrize(
        "return_value,wrap,expected",
        [
            ([b"data1", b"data2"], None, [b"data1", b"data2"]),
            ([b"raw1", b"raw2"], bytes.decode, ["raw1", "raw2"]),
        ],
        ids=["plain", "with_wrap"],
    )
    async def test_distributes_results(self, return_value, wrap, expected):
        batch_fn = AsyncMock(return_value=return_value)
        flush = _make_flush(batch_fn, wrap=wrap) if wrap else _make_flush(batch_fn)

        loop = asyncio.get_running_loop()
        futures = [loop.create_future() for _ in expected]

        await flush(items=[f"f{i}" for i in range(len(expected))], futures=futures, use_h2=False)
        for fut, exp in zip(futures, expected):
            assert fut.result() == exp

    async def test_exception_propagates_to_futures(self):
        batch_fn = AsyncMock(side_effect=RuntimeError("boom"))
        flush = _make_flush(batch_fn)

        loop = asyncio.get_running_loop()
        f1 = loop.create_future()
        f2 = loop.create_future()

        await flush(items=["a", "b"], futures=[f1, f2], use_h2=False)
        with pytest.raises(RuntimeError, match="boom"):
            f1.result()
        with pytest.raises(RuntimeError, match="boom"):
            f2.result()

    async def test_none_results_when_batch_returns_none(self):
        batch_fn = AsyncMock(return_value=None)
        flush = _make_flush(batch_fn)

        loop = asyncio.get_running_loop()
        f1 = loop.create_future()

        await flush(items=["a"], futures=[f1], use_h2=False)
        assert f1.result() is None

    async def test_custom_items_key(self):
        batch_fn = AsyncMock(return_value=["ok"])
        flush = _make_flush(batch_fn, items_key="paths")

        loop = asyncio.get_running_loop()
        f1 = loop.create_future()

        await flush(items=["x.txt"], futures=[f1], use_h2=False)
        batch_fn.assert_called_once()
        assert batch_fn.call_args.kwargs["paths"] == ["x.txt"]


class TestMakeBatcher:
    """Test the make_batcher convenience builder."""

    def test_registers_ops(self):
        batch_get = AsyncMock()
        batch_put = AsyncMock()

        b = make_batcher(
            key_fn=lambda **kw: "k",
            ops={
                "get": {"batch_fn": batch_get},
                "put": {"batch_fn": batch_put, "unpack_put": True},
            },
        )
        assert hasattr(b, "get")
        assert hasattr(b, "put")
        assert "get" in b._ops
        assert "put" in b._ops
        assert b._ops["put"].unpack_put is True

    async def test_end_to_end(self):
        batch_fn = AsyncMock(return_value=[b"content"])
        b = make_batcher(
            key_fn=lambda **kw: "bucket",
            ops={"get": {"batch_fn": batch_fn}},
        )
        result = await b.get(item="myfile.txt")
        assert result == b"content"
        batch_fn.assert_called_once()
