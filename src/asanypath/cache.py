# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Opt-in on-disk cache for downloaded object/URL content.

Caching is **off by default**: an unconfigured read path is byte-for-byte
unchanged, so non-caching callers pay zero overhead. Opt in per call with
``read_bytes(use_cache=True)`` / ``open(..., use_cache=True)``, or flip the
process-wide default with :func:`configure_cache(enabled=True)`.

Backed by :mod:`diskcache` (an optional dependency; install ``asanypath[cache]``).
Raw bytes are stored keyed by the path's string form; the blocking SQLite/file
I/O runs in a worker thread so the event loop stays responsive.
"""

from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from diskcache import Cache


def _env_int(name: str, default: int | None) -> int | None:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


# 1 GiB total on-disk cache by default; override via ASANYPATH_CACHE_SIZE_LIMIT.
_DEFAULT_SIZE_LIMIT = 1024 * 1024 * 1024
# No expiry by default (size-bounded LRU); override via ASANYPATH_CACHE_TTL (seconds).
_DEFAULT_TTL: float | None = None


def _default_cache_dir() -> Path:
    """Resolve the default cache directory, honoring XDG conventions."""
    explicit = os.getenv("ASANYPATH_CACHE_DIR")
    if explicit:
        return Path(explicit).expanduser()
    xdg = os.getenv("XDG_CACHE_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".cache"
    return base / "asanypath"


class DownloadCache:
    """Async wrapper over a :class:`diskcache.Cache` storing raw bytes.

    Concurrent misses for the same key are collapsed by a per-key lock so a
    stampede of identical reads triggers a single fetch.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        size_limit: int = _DEFAULT_SIZE_LIMIT,
        ttl: float | None = _DEFAULT_TTL,
    ) -> None:
        from diskcache import Cache

        self._cache: Cache = Cache(
            str(directory), size_limit=size_limit, eviction_policy="least-recently-used"
        )
        self._ttl = ttl
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def get_or_fetch(self, key: str, fetch: Callable[[], Awaitable[bytes]]) -> bytes:
        """Return cached bytes for *key*, else call *fetch*, store, and return."""
        hit = await asyncio.to_thread(self._cache.get, key)
        if hit is not None:
            return cast(bytes, hit)
        async with self._locks[key]:
            hit = await asyncio.to_thread(self._cache.get, key)
            if hit is not None:
                return cast(bytes, hit)
            data = await fetch()
            await asyncio.to_thread(self._cache.set, key, data, expire=self._ttl)
            return data

    async def delete(self, key: str) -> None:
        """Drop the cached entry for *key* (no-op if absent)."""
        await asyncio.to_thread(self._cache.delete, key)

    async def clear(self) -> None:
        """Remove all cached entries."""
        await asyncio.to_thread(self._cache.clear)

    def close(self) -> None:
        self._cache.close()


# --- Module-level configuration / singleton -------------------------------

_cache: DownloadCache | None = None
_directory: Path | None = None
_size_limit: int | None = None
_ttl: float | None = _DEFAULT_TTL
_default_enabled = False


def configure_cache(
    *,
    directory: str | os.PathLike[str] | None = None,
    size_limit: int | None = None,
    ttl: float | None = None,
    enabled: bool | None = None,
) -> None:
    """Configure the shared download cache.

    *directory* defaults to ``~/.cache/asanypath`` (honoring ``$ASANYPATH_CACHE_DIR``
    and ``$XDG_CACHE_HOME``). *size_limit* is the total on-disk cap in bytes.
    *ttl* is a per-entry expiry in seconds (``None`` = no expiry). *enabled* sets
    the process-wide default for ``use_cache`` when a call doesn't specify one.

    Resets the active cache instance so the new settings take effect lazily.
    """
    global _cache, _directory, _size_limit, _ttl, _default_enabled
    if directory is not None:
        _directory = Path(directory).expanduser()
    if size_limit is not None:
        _size_limit = size_limit
    if ttl is not None:
        _ttl = ttl
    if enabled is not None:
        _default_enabled = enabled
    if _cache is not None:
        _cache.close()
        _cache = None


def get_cache() -> DownloadCache:
    """Return the shared :class:`DownloadCache`, creating it on first use."""
    global _cache
    if _cache is None:
        directory = _directory or _default_cache_dir()
        directory.mkdir(parents=True, exist_ok=True)
        size_limit = (
            _size_limit
            if _size_limit is not None
            else _env_int("ASANYPATH_CACHE_SIZE_LIMIT", _DEFAULT_SIZE_LIMIT)
        ) or _DEFAULT_SIZE_LIMIT
        ttl = _ttl if _ttl is not None else _env_int("ASANYPATH_CACHE_TTL", None)
        _cache = DownloadCache(directory, size_limit=size_limit, ttl=ttl)
    return _cache


def cache_enabled(use_cache: bool | None) -> bool:
    """Resolve a per-call ``use_cache`` against the process-wide default."""
    return _default_enabled if use_cache is None else use_cache


def reset_cache() -> None:
    """Close and forget the active cache (primarily for tests)."""
    global _cache, _directory, _size_limit, _ttl, _default_enabled
    if _cache is not None:
        _cache.close()
    _cache = None
    _directory = None
    _size_limit = None
    _ttl = _DEFAULT_TTL
    _default_enabled = False
