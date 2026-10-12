# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from pathlib import Path

from asanypath import _resume
from asanypath._resume import ResumeMeta, read_meta, write_meta
from asanypath.cloud import CloudPathMixin

# --- Sidecar meta ----------------------------------------------------------


def test_meta_roundtrip(tmp_path):
    m = ResumeMeta(identity="etag-abc", size=1234)
    p = tmp_path / "x.part.meta"
    write_meta(p, m)
    assert read_meta(p) == m


def test_meta_missing_returns_none(tmp_path):
    assert read_meta(tmp_path / "nope.meta") is None


def test_meta_corrupt_returns_none(tmp_path):
    p = tmp_path / "bad.meta"
    p.write_bytes(b"not json{{{")
    assert read_meta(p) is None


# --- Fake range-capable remote --------------------------------------------


class _FakeRemote(CloudPathMixin):
    def __init__(self, uri, payload, *, identity="etag1", has_range=True, has_head=True):
        self._uri = uri
        self._payload = payload
        self._identity = identity
        self._supports_range_read = has_range
        self._has_head = has_head
        self.range_calls: list[tuple[int, int]] = []
        self.full_reads = 0

    def __str__(self) -> str:
        return self._uri

    async def _resume_identity(self):
        if not self._has_head:
            return None, None
        return len(self._payload), self._identity

    async def _range_read(self, start, end):
        self.range_calls.append((start, end))
        return self._payload[start : end + 1]

    async def _read_bytes_uncached(self):
        self.full_reads += 1
        return self._payload


PAYLOAD = bytes(range(256)) * 8  # 2048 bytes


async def test_download_fresh(tmp_path):
    p = _FakeRemote("s3://b/obj", PAYLOAD)
    dst = tmp_path / "out.bin"
    result = await p.download(dst, chunk_size=512)
    assert result == dst
    assert dst.read_bytes() == PAYLOAD
    assert not (tmp_path / "out.bin.part").exists()
    assert not (tmp_path / "out.bin.part.meta").exists()
    assert p.range_calls[0][0] == 0  # started from the beginning


async def test_download_full_fallback_no_head(tmp_path):
    p = _FakeRemote("s3://b/obj", PAYLOAD, has_head=False)
    dst = tmp_path / "out.bin"
    await p.download(dst)
    assert dst.read_bytes() == PAYLOAD
    assert p.full_reads == 1
    assert p.range_calls == []


async def test_download_full_fallback_no_range(tmp_path):
    p = _FakeRemote("s3://b/obj", PAYLOAD, has_range=False)
    dst = tmp_path / "out.bin"
    await p.download(dst)
    assert dst.read_bytes() == PAYLOAD
    assert p.full_reads == 1


async def test_resume_continues_from_partial(tmp_path):
    dst = tmp_path / "out.bin"
    part = tmp_path / "out.bin.part"
    meta = tmp_path / "out.bin.part.meta"
    part.write_bytes(PAYLOAD[:800])  # simulate interrupted download
    write_meta(meta, ResumeMeta(identity="etag1", size=len(PAYLOAD)))

    p = _FakeRemote("s3://b/obj", PAYLOAD)
    await p.download(dst, chunk_size=512)
    assert dst.read_bytes() == PAYLOAD
    # resumed at 800: no range request covers bytes before the saved offset
    assert all(start >= 800 for start, _ in p.range_calls)


async def test_resume_identity_mismatch_restarts(tmp_path):
    dst = tmp_path / "out.bin"
    part = tmp_path / "out.bin.part"
    meta = tmp_path / "out.bin.part.meta"
    part.write_bytes(b"stale-bytes" * 50)
    write_meta(meta, ResumeMeta(identity="OLD-etag", size=len(PAYLOAD)))

    p = _FakeRemote("s3://b/obj", PAYLOAD, identity="etag1")
    await p.download(dst, chunk_size=512)
    assert dst.read_bytes() == PAYLOAD
    assert p.range_calls[0][0] == 0  # restarted from scratch


async def test_resume_size_mismatch_restarts(tmp_path):
    dst = tmp_path / "out.bin"
    part = tmp_path / "out.bin.part"
    meta = tmp_path / "out.bin.part.meta"
    part.write_bytes(PAYLOAD[:400])
    write_meta(meta, ResumeMeta(identity="etag1", size=99999))

    p = _FakeRemote("s3://b/obj", PAYLOAD)
    await p.download(dst, chunk_size=512)
    assert dst.read_bytes() == PAYLOAD
    assert p.range_calls[0][0] == 0


async def test_resume_false_ignores_partial(tmp_path):
    dst = tmp_path / "out.bin"
    part = tmp_path / "out.bin.part"
    meta = tmp_path / "out.bin.part.meta"
    part.write_bytes(PAYLOAD[:800])
    write_meta(meta, ResumeMeta(identity="etag1", size=len(PAYLOAD)))

    p = _FakeRemote("s3://b/obj", PAYLOAD)
    await p.download(dst, resume=False, chunk_size=512)
    assert dst.read_bytes() == PAYLOAD
    assert p.range_calls[0][0] == 0


async def test_download_uses_resume_module(tmp_path):
    # guard against accidental import break
    assert hasattr(_resume, "ResumeMeta")
    assert isinstance(Path(tmp_path), Path)


# --- Real _resume_identity (HEAD parsing) ----------------------------------


class _HeadRemote(CloudPathMixin):
    def __init__(self, headers, *, fail=False):
        self._headers = headers
        self._fail = fail

    @property
    def _item_path(self) -> str:
        return "k"

    async def _get_native_kwargs(self):
        return {}

    def _get_batcher(self):
        headers = self._headers
        fail = self._fail

        class _Batcher:
            async def head(self, item, **kwargs):
                if fail:
                    raise RuntimeError("no HEAD support")
                return headers

        return _Batcher()


async def test_resume_identity_etag():
    p = _HeadRemote({"Content-Length": "100", "ETag": '"abc"'})
    assert await p._resume_identity() == (100, "abc")


async def test_resume_identity_last_modified_fallback():
    p = _HeadRemote({"content-length": "50", "Last-Modified": "Wed, 01 Jan 2026 00:00:00 GMT"})
    assert await p._resume_identity() == (50, "Wed, 01 Jan 2026 00:00:00 GMT")


async def test_resume_identity_bad_content_length():
    p = _HeadRemote({"Content-Length": "notanint", "ETag": "x"})
    assert await p._resume_identity() == (None, "x")


async def test_resume_identity_head_unavailable():
    p = _HeadRemote({}, fail=True)
    assert await p._resume_identity() == (None, None)
