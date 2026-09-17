# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Generic micro-batcher for native Rust batch operations.

Transparently collects concurrent calls arriving in the same event-loop
tick and dispatches them as a single batch, amortizing the PyO3 bridge
overhead across all concurrent operations.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

# Batch size threshold: use HTTP/2 multiplexing when batch is large enough
# to benefit from stream multiplexing over fewer connections; use HTTP/1.1
# (parallel TCP connections) for smaller batches where framing overhead hurts.
H2_BATCH_THRESHOLD = 20


class MicroBatcher:
    """Collects calls arriving in the same event-loop tick into one batch.

    Each operation is registered with :meth:`register`, which returns a
    submit coroutine callers use to enqueue work.  At the end of the
    current tick, all enqueued items are flushed together via the
    backend-provided *flush_fn*.

    Parameters
    ----------
    key_fn:
        Callable that produces a hashable grouping key from the keyword
        arguments passed to the submit function.  Calls with the same
        key are batched together.
    """

    def __init__(self) -> None:
        self._pending: dict[str, list] = {}
        self._scheduled: set[str] = set()
        self._ops: dict[str, _OpSpec] = {}

    def __getattr__(self, name: str) -> Callable[..., Coroutine]:
        raise AttributeError(name)

    # -----------------------------------------------------------------
    # Registration
    # -----------------------------------------------------------------

    def register(
        self,
        op: str,
        *,
        flush_fn: Callable[..., Coroutine],
        key_fn: Callable[..., str],
        item_key: str = "item",
        unpack_put: bool = False,
    ) -> Callable[..., Coroutine]:
        """Register a batch operation and return a *submit* coroutine.

        Parameters
        ----------
        op:
            Unique name for this operation (e.g. ``"get"``, ``"put"``).
        flush_fn:
            ``async def flush_fn(items, futures, use_h2, **kwargs)``
            Called with the collected items list and their futures.
        key_fn:
            ``def key_fn(**kwargs) -> str`` — returns a grouping key.
        item_key:
            Name of the keyword argument that carries the per-call item
            (default ``"item"``).
        unpack_put:
            If *True*, the item is expected to be a tuple and will be
            appended as-is (for put-style ops with ``(path, data)``).
        """
        spec = _OpSpec(
            op=op, flush_fn=flush_fn, key_fn=key_fn, item_key=item_key, unpack_put=unpack_put
        )
        self._ops[op] = spec

        async def submit(**kwargs: Any) -> Any:
            return await self._submit(spec, kwargs)

        # Also make it accessible via attribute: batcher.get(item=..., ...)
        setattr(self, op, submit)
        return submit

    # -----------------------------------------------------------------
    # Internal machinery
    # -----------------------------------------------------------------

    async def _submit(self, spec: _OpSpec, kwargs: dict) -> Any:
        item = kwargs.pop(spec.item_key)
        key = f"{spec.op}|{spec.key_fn(**kwargs)}"
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        pending = self._pending.setdefault(key, [])
        pending.append((item, fut))
        if key not in self._scheduled:
            self._scheduled.add(key)
            asyncio.get_running_loop().call_soon(self._flush, key, spec, kwargs)
        return await fut

    def _flush(self, key: str, spec: _OpSpec, kwargs: dict) -> None:
        items = self._pending.pop(key, [])
        self._scheduled.discard(key)
        if not items:
            return
        collected = [i for i, _ in items]
        futures = [f for _, f in items]
        use_h2 = len(collected) >= H2_BATCH_THRESHOLD
        asyncio.ensure_future(
            spec.flush_fn(items=collected, futures=futures, use_h2=use_h2, **kwargs)
        )


class _OpSpec:
    """Internal descriptor for a registered operation."""

    __slots__ = ("op", "flush_fn", "key_fn", "item_key", "unpack_put")

    def __init__(self, *, op: str, flush_fn, key_fn, item_key: str, unpack_put: bool):
        self.op = op
        self.flush_fn = flush_fn
        self.key_fn = key_fn
        self.item_key = item_key
        self.unpack_put = unpack_put


# ---------------------------------------------------------------------------
# Generic flush factory + batcher builder
# ---------------------------------------------------------------------------


def _make_flush(
    batch_fn: Callable[..., Coroutine],
    *,
    items_key: str = "items",
    wrap: Callable | None = None,
) -> Callable[..., Coroutine]:
    """Create a flush function for a batch operation.

    Parameters
    ----------
    batch_fn:
        The native Rust batch function to call.
    items_key:
        Keyword argument name to pass the items list to *batch_fn*.
    wrap:
        Optional callable applied to each result value (e.g. ``bytes``).
    """

    async def _flush(items: list, futures: list, use_h2: bool, **kw: Any) -> None:
        try:
            kw[items_key] = items
            results = await batch_fn(use_h2=use_h2, **kw) or [None] * len(futures)
            for fut, val in zip(futures, results):
                if not fut.done():
                    fut.set_result(wrap(val) if wrap else val)
        except Exception as exc:
            for fut in futures:
                if not fut.done():
                    fut.set_exception(exc)

    return _flush


def make_batcher(
    key_fn: Callable[..., str],
    ops: dict[str, dict[str, Any]],
) -> MicroBatcher:
    """Build a :class:`MicroBatcher` from a declarative ops table.

    Parameters
    ----------
    key_fn:
        Callable producing a grouping key from operation kwargs.
    ops:
        Mapping of operation name to a dict with keys:

        - ``batch_fn`` (required): the native Rust batch function.
        - ``items_key`` (default ``"items"``): kwarg name for the items list.
        - ``returns_results`` (default ``True``): whether the batch fn
          returns per-item results.
        - ``wrap`` (default ``None``): optional per-result transform.
        - ``unpack_put`` (default ``False``): passed to
          :meth:`MicroBatcher.register`.
    """
    b = MicroBatcher()
    for name, spec in ops.items():
        flush = _make_flush(
            spec["batch_fn"],
            items_key=spec.get("items_key", "items"),
            wrap=spec.get("wrap"),
        )
        b.register(
            name,
            flush_fn=flush,
            key_fn=key_fn,
            unpack_put=spec.get("unpack_put", False),
        )
    return b
