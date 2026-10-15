# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import asyncio
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from asanypath_native import DownloadCache as NativeDownloadCache

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


@pytest.mark.parametrize("payload", [b"", bytes(range(256)), b"\x80\x04N."])
async def test_native_roundtrip_returns_bytes(tmp_path, payload):
    dc = DownloadCache(tmp_path / "c")
    assert await dc.get_or_fetch("k", lambda: _const(payload)) == payload

    async def unexpected_fetch():
        pytest.fail("A persisted byte value must be a cache hit")

    dc.close()
    dc = DownloadCache(tmp_path / "c")
    result = await dc.get_or_fetch("k", unexpected_fetch)
    assert isinstance(result, bytes)
    assert result == payload
    dc.close()


async def test_native_lru_and_exact_byte_budget(tmp_path):
    dc = DownloadCache(tmp_path / "c", size_limit=10)
    await dc.get_or_fetch("a", lambda: _const(b"1111"))
    await dc.get_or_fetch("b", lambda: _const(b"2222"))
    assert dc._cache.get("a") == b"1111"  # refresh a's LRU position
    await dc.get_or_fetch("c", lambda: _const(b"3333"))
    assert dc._cache.get("a") == b"1111"
    assert dc._cache.get("b") is None
    assert dc._cache.get("c") == b"3333"
    with sqlite3.connect(tmp_path / "c" / "native-v1" / "cache.sqlite3") as connection:
        used = connection.execute(
            "SELECT SUM(length(CAST(key AS BLOB)) + length(value)) FROM entries"
        ).fetchone()[0]
    assert used == 10
    dc.close()


def test_native_budget_counts_utf8_keys_and_replacements(tmp_path):
    cache = NativeDownloadCache(str(tmp_path), size_limit=6)
    cache.set("\N{LATIN SMALL LETTER E WITH ACUTE}", b"1234")
    assert cache.get("\N{LATIN SMALL LETTER E WITH ACUTE}") == b"1234"
    cache.set("\N{LATIN SMALL LETTER E WITH ACUTE}", b"12345")
    assert cache.get("\N{LATIN SMALL LETTER E WITH ACUTE}") is None
    cache.set("a", b"12345")
    cache.set("a", b"x")
    cache.set("b", b"123")
    assert cache.get("a") == b"x"
    assert cache.get("b") == b"123"
    cache.close()


async def test_oversized_objects_not_retained(tmp_path):
    dc = DownloadCache(tmp_path / "c", size_limit=5)
    await dc.get_or_fetch("a", lambda: _const(b"1234"))
    calls = 0

    async def fetch():
        nonlocal calls
        calls += 1
        return b"too large"

    assert await dc.get_or_fetch("b", fetch) == b"too large"
    assert await dc.get_or_fetch("b", fetch) == b"too large"
    assert calls == 2
    assert dc._cache.get("a") == b"1234"
    dc.close()


async def test_expiry_survives_reopen(tmp_path):
    dc = DownloadCache(tmp_path / "c", ttl=0.1)
    await dc.get_or_fetch("a", lambda: _const(b"old"))
    dc.close()
    await asyncio.sleep(0.2)
    dc = DownloadCache(tmp_path / "c")
    assert await dc.get_or_fetch("a", lambda: _const(b"new")) == b"new"
    dc.close()


def test_expired_entries_reclaimed_before_lru(tmp_path):
    cache = NativeDownloadCache(str(tmp_path), size_limit=10)
    cache.set("a", b"1111")
    cache.set("b", b"2222", expire=-1)
    cache.set("c", b"3333")
    assert cache.get("a") == b"1111"
    assert cache.get("b") is None
    assert cache.get("c") == b"3333"
    cache.close()


async def test_legacy_cache_never_opened_or_cleared(tmp_path):
    legacy = tmp_path / "cache.db"
    legacy.write_bytes(b"not a SQLite database; legacy cache must never be opened")
    dc = DownloadCache(tmp_path)
    assert await dc.get_or_fetch("a", lambda: _const(b"new")) == b"new"
    await dc.clear()
    dc.close()
    assert legacy.read_bytes() == b"not a SQLite database; legacy cache must never be opened"


def test_tampered_blob_is_returned_as_bytes_not_deserialized(tmp_path, monkeypatch):
    import pickle

    def forbidden(*args, **kwargs):
        pytest.fail("Cache reads must never invoke Python pickle")

    monkeypatch.setattr(pickle, "load", forbidden)
    monkeypatch.setattr(pickle, "loads", forbidden)
    cache = NativeDownloadCache(str(tmp_path), size_limit=1024)
    cache.set("k", b"original")
    with sqlite3.connect(tmp_path / "native-v1" / "cache.sqlite3") as connection:
        connection.execute("UPDATE entries SET value = ? WHERE key = 'k'", (b"\x80\x04N.",))
    assert cache.get("k") == b"\x80\x04N."
    cache.close()


async def test_corrupt_native_database_raises(tmp_path):
    directory = tmp_path / "native-v1"
    directory.mkdir()
    (directory / "cache.sqlite3").write_bytes(b"invalid database")
    with pytest.raises(OSError, match="Download cache database"):
        DownloadCache(tmp_path)


def test_invalid_native_value_type_raises(tmp_path):
    cache = NativeDownloadCache(str(tmp_path), size_limit=1024)
    cache.set("k", b"value")
    with sqlite3.connect(tmp_path / "native-v1" / "cache.sqlite3") as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute("UPDATE entries SET value = 'not bytes' WHERE key = 'k'")
    with pytest.raises(OSError, match="Download cache database"):
        cache.get("k")
    cache.close()


def test_unwritable_cache_path_raises(tmp_path):
    directory = tmp_path / "file"
    directory.write_bytes(b"not a directory")
    with pytest.raises(OSError, match="Create download cache directory"):
        DownloadCache(directory)


async def test_fetch_error_is_not_cached(tmp_path):
    dc = DownloadCache(tmp_path)

    async def fetch():
        raise OSError("download failed")

    with pytest.raises(OSError, match="download failed"):
        await dc.get_or_fetch("a", fetch)
    assert await dc.get_or_fetch("a", lambda: _const(b"ok")) == b"ok"
    dc.close()


@pytest.mark.parametrize("size_limit", [0, -1])
def test_invalid_size_limit_raises(tmp_path, size_limit):
    with pytest.raises(ValueError, match="size_limit must be positive"):
        DownloadCache(tmp_path, size_limit=size_limit)
    configure_cache(directory=tmp_path, size_limit=size_limit)
    with pytest.raises(ValueError, match="size_limit must be positive"):
        get_cache()


@pytest.mark.parametrize("ttl", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_ttl_raises(tmp_path, ttl):
    cache = NativeDownloadCache(str(tmp_path), size_limit=1024)
    with pytest.raises(ValueError, match="ttl must be finite"):
        cache.set("a", b"b", expire=ttl)
    cache.close()


def test_closed_cache_raises_and_close_is_idempotent(tmp_path):
    cache = NativeDownloadCache(str(tmp_path), size_limit=1024)
    cache.close()
    cache.close()
    with pytest.raises(ValueError, match="closed"):
        cache.get("a")
    with pytest.raises(ValueError, match="closed"):
        cache.set("a", b"b")
    with pytest.raises(ValueError, match="closed"):
        cache.delete("a")
    with pytest.raises(ValueError, match="closed"):
        cache.clear()


def test_cache_shared_across_processes(tmp_path):
    cache = NativeDownloadCache(str(tmp_path), size_limit=32)
    cache.set("parent", b"p")
    script = """
import sys
from asanypath_native import DownloadCache
cache = DownloadCache(sys.argv[1], size_limit=32)
for i in range(20):
    cache.set(sys.argv[2] + str(i), b"value")
cache.close()
"""
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from asanypath_native import DownloadCache; "
            "c = DownloadCache(sys.argv[1], 32); "
            "assert c.get('parent') == b'p'; c.close()",
            str(tmp_path),
        ],
        check=True,
        timeout=30,
    )
    processes = [
        subprocess.Popen([sys.executable, "-c", script, str(tmp_path), str(index)])
        for index in range(3)
    ]
    try:
        for process in processes:
            assert process.wait(timeout=30) == 0
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
    cache.set("final", b"ok")
    assert cache.get("final") == b"ok"
    with sqlite3.connect(tmp_path / "native-v1" / "cache.sqlite3") as connection:
        used = connection.execute(
            "SELECT SUM(length(CAST(key AS BLOB)) + length(value)) FROM entries"
        ).fetchone()[0]
    assert used <= 32
    cache.close()


def test_concurrent_first_open_across_processes(tmp_path):
    script = """
import sys
from asanypath_native import DownloadCache
cache = DownloadCache(sys.argv[1], size_limit=1024)
cache.set(sys.argv[2], b"shared")
cache.close()
"""
    processes = [
        subprocess.Popen([sys.executable, "-c", script, str(tmp_path), str(index)])
        for index in range(4)
    ]
    try:
        for process in processes:
            assert process.wait(timeout=30) == 0
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
    cache = NativeDownloadCache(str(tmp_path), size_limit=1024)
    for index in range(4):
        assert cache.get(str(index)) == b"shared"
    cache.close()


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
