# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import asyncio
from pathlib import Path

import pytest

from asanypath.cache import (
    DownloadCache,
    _default_cache_dir,
    cache_enabled,
    configure_cache,
    get_cache,
    reset_cache,
)
from asanypath.cloud import CloudPathMixin


@pytest.fixture(autouse=True)
def _reset():
    reset_cache()
    yield
    reset_cache()


# --- Default directory resolution -----------------------------------------


def test_default_dir_explicit_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ASANYPATH_CACHE_DIR", str(tmp_path / "explicit"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    assert _default_cache_dir() == tmp_path / "explicit"


def test_default_dir_xdg(monkeypatch, tmp_path):
    monkeypatch.delenv("ASANYPATH_CACHE_DIR", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    assert _default_cache_dir() == tmp_path / "xdg" / "asanypath"


def test_default_dir_home_fallback(monkeypatch, tmp_path):
    monkeypatch.delenv("ASANYPATH_CACHE_DIR", raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "home"))
    assert _default_cache_dir() == tmp_path / "home" / ".cache" / "asanypath"


# --- DownloadCache core ----------------------------------------------------


async def test_get_or_fetch_miss_then_hit(tmp_path):
    dc = DownloadCache(tmp_path / "c")
    calls = 0

    async def fetch():
        nonlocal calls
        calls += 1
        return b"payload"

    assert await dc.get_or_fetch("k", fetch) == b"payload"
    assert await dc.get_or_fetch("k", fetch) == b"payload"
    assert calls == 1  # second read served from disk
    dc.close()


async def test_get_or_fetch_dogpile_single_fetch(tmp_path):
    dc = DownloadCache(tmp_path / "c")
    calls = 0

    async def fetch():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return b"x"

    results = await asyncio.gather(*(dc.get_or_fetch("k", fetch) for _ in range(10)))
    assert results == [b"x"] * 10
    assert calls == 1  # concurrent misses collapsed by per-key lock
    dc.close()


async def test_ttl_expiry(tmp_path):
    dc = DownloadCache(tmp_path / "c", ttl=0.2)
    calls = 0

    async def fetch():
        nonlocal calls
        calls += 1
        return b"v"

    await dc.get_or_fetch("k", fetch)
    await asyncio.sleep(0.3)
    await dc.get_or_fetch("k", fetch)
    assert calls == 2  # expired entry re-fetched
    dc.close()


async def test_delete_and_clear(tmp_path):
    dc = DownloadCache(tmp_path / "c")
    await dc.get_or_fetch("a", lambda: _const(b"1"))
    await dc.get_or_fetch("b", lambda: _const(b"2"))
    await dc.delete("a")
    calls = {"n": 0}

    async def fetch():
        calls["n"] += 1
        return b"1"

    await dc.get_or_fetch("a", fetch)
    assert calls["n"] == 1  # deleted -> re-fetched
    await dc.clear()
    await dc.get_or_fetch("b", fetch)
    assert calls["n"] == 2  # cleared -> re-fetched
    dc.close()


async def _const(value: bytes) -> bytes:
    return value


# --- Module configuration --------------------------------------------------


def test_cache_enabled_default_off():
    assert cache_enabled(None) is False
    assert cache_enabled(True) is True
    assert cache_enabled(False) is False


def test_configure_enabled_flips_default():
    configure_cache(enabled=True)
    assert cache_enabled(None) is True
    assert cache_enabled(False) is False  # explicit opt-out still wins
    configure_cache(enabled=False)
    assert cache_enabled(None) is False


def test_configure_directory_and_singleton(tmp_path):
    configure_cache(directory=tmp_path / "cfg")
    c1 = get_cache()
    c2 = get_cache()
    assert c1 is c2  # singleton
    assert (tmp_path / "cfg").is_dir()


# --- Integration via CloudPathMixin.read_bytes -----------------------------


class _FakePath(CloudPathMixin):
    def __init__(self, uri: str, payload: bytes) -> None:
        self._uri = uri
        self._payload = payload
        self.fetches = 0

    def __str__(self) -> str:
        return self._uri

    async def _read_bytes_uncached(self) -> bytes:
        self.fetches += 1
        return self._payload


async def test_read_bytes_bypasses_cache_by_default(tmp_path):
    configure_cache(directory=tmp_path / "c")
    p = _FakePath("s3://b/k", b"data")
    assert await p.read_bytes() == b"data"
    assert await p.read_bytes() == b"data"
    assert p.fetches == 2  # no caching unless opted in


async def test_read_bytes_use_cache_true(tmp_path):
    configure_cache(directory=tmp_path / "c")
    p = _FakePath("s3://b/k", b"data")
    assert await p.read_bytes(use_cache=True) == b"data"
    assert await p.read_bytes(use_cache=True) == b"data"
    assert p.fetches == 1  # second served from cache


async def test_read_bytes_global_default_on(tmp_path):
    configure_cache(directory=tmp_path / "c", enabled=True)
    p = _FakePath("s3://b/k", b"data")
    await p.read_bytes()
    await p.read_bytes()
    assert p.fetches == 1  # enabled default caches
    # explicit opt-out bypasses even when default on
    q = _FakePath("s3://b/k2", b"d2")
    await q.read_bytes(use_cache=False)
    await q.read_bytes(use_cache=False)
    assert q.fetches == 2


async def test_read_text_uses_cache(tmp_path):
    configure_cache(directory=tmp_path / "c")
    p = _FakePath("s3://b/k", b"hello")
    assert await p.read_text(use_cache=True) == "hello"
    assert await p.read_text(use_cache=True) == "hello"
    assert p.fetches == 1


async def test_different_keys_cached_independently(tmp_path):
    configure_cache(directory=tmp_path / "c", enabled=True)
    a = _FakePath("s3://b/a", b"A")
    b = _FakePath("s3://b/b", b"B")
    assert await a.read_bytes() == b"A"
    assert await b.read_bytes() == b"B"
    assert await a.read_bytes() == b"A"
    assert a.fetches == 1
    assert b.fetches == 1


# --- Env + configure branches ---------------------------------------------


def test_env_int_parsing(monkeypatch):
    from asanypath.cache import _env_int

    monkeypatch.delenv("ASANYPATH_X", raising=False)
    assert _env_int("ASANYPATH_X", 7) == 7
    monkeypatch.setenv("ASANYPATH_X", "123")
    assert _env_int("ASANYPATH_X", 7) == 123
    monkeypatch.setenv("ASANYPATH_X", "nope")
    assert _env_int("ASANYPATH_X", 7) == 7
    monkeypatch.setenv("ASANYPATH_X", "-5")
    assert _env_int("ASANYPATH_X", 7) == 7


def test_configure_size_ttl_resets_instance(tmp_path):
    configure_cache(directory=tmp_path / "c", size_limit=5_000_000, ttl=10)
    c1 = get_cache()
    configure_cache(size_limit=6_000_000)  # closes + replaces the live instance
    c2 = get_cache()
    assert c1 is not c2


def test_get_cache_size_limit_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ASANYPATH_CACHE_SIZE_LIMIT", "1048576")
    configure_cache(directory=tmp_path / "c")
    assert get_cache()._cache.size_limit == 1048576
