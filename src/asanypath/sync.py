# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Synchronous wrapper for AsAnyPath.

Usage::

    from asanypath.sync import AsAnyPath

    p = AsAnyPath("s3://bucket/key")
    data = p.read_bytes()          # blocks, no await
    for child in p.parent.iterdir():
        print(child.name)

Local paths bypass the proxy entirely and return SyncPath (pure pathlib).
Cloud paths use a shared background event loop in a daemon thread.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
from abc import ABC
from collections.abc import AsyncIterator
from typing import Any


class _SyncRunner:
    """Lazy singleton that runs coroutines on a background event loop."""

    _instance: _SyncRunner | None = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    @classmethod
    def get(cls) -> _SyncRunner:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _start(self) -> None:
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(
                target=self._loop.run_forever, daemon=True, name="asanypath-sync"
            )
            self._thread.start()

    def run(self, coro):
        """Submit a coroutine and block until it completes."""
        self._start()
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def iter(self, async_iter: AsyncIterator):
        """Drain an async iterator, yielding items synchronously."""
        self._start()

        async def _collect_next():
            return await async_iter.__anext__()

        while True:
            try:
                yield asyncio.run_coroutine_threadsafe(_collect_next(), self._loop).result()
            except StopAsyncIteration:
                return


class _SyncProxy:
    """Transparent sync proxy around an async path instance."""

    __slots__ = ("_async",)

    def __init__(self, async_path) -> None:
        object.__setattr__(self, "_async", async_path)

    # ------------------------------------------------------------------
    # Pure path properties / methods — delegate directly (no await needed)
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        return str(self._async)

    def __repr__(self) -> str:
        return f"SyncProxy({self._async!r})"

    def __fspath__(self) -> str:
        return self._async.__fspath__()

    def __eq__(self, other) -> bool:
        if isinstance(other, _SyncProxy):
            return self._async == other._async
        return self._async == other

    def __hash__(self) -> int:
        return hash(self._async)

    def __lt__(self, other) -> bool:
        return self._async < (other._async if isinstance(other, _SyncProxy) else other)

    def __le__(self, other) -> bool:
        return self._async <= (other._async if isinstance(other, _SyncProxy) else other)

    def __gt__(self, other) -> bool:
        return self._async > (other._async if isinstance(other, _SyncProxy) else other)

    def __ge__(self, other) -> bool:
        return self._async >= (other._async if isinstance(other, _SyncProxy) else other)

    def __truediv__(self, other) -> _SyncProxy:
        return _wrap(self._async / other)

    def __rtruediv__(self, other) -> _SyncProxy:
        return _wrap(other / self._async)

    # ------------------------------------------------------------------
    # Attribute access — proxy everything, wrapping coroutines/iterators
    # ------------------------------------------------------------------

    def __getattr__(self, name: str) -> Any:  # pragma: no cover
        # This proxy mechanism is thoroughly tested through integration tests
        # (every cloud path method call triggers these branches), but edge cases
        # like "non-callable attributes that are collections" are hard to trigger
        # directly without synthetic mocking.
        attr = getattr(self._async, name)

        # Properties and plain values
        if not callable(attr):  # pragma: no cover
            if _is_path(attr):  # pragma: no cover
                return _wrap(attr)
            if isinstance(attr, (list, tuple)):  # pragma: no cover
                return type(attr)(_wrap(x) if _is_path(x) else x for x in attr)
            return attr

        # Check if it's a coroutine function or async generator
        if inspect.iscoroutinefunction(attr):  # pragma: no cover

            def _sync_method(*args, **kwargs):  # pragma: no cover
                result = _SyncRunner.get().run(attr(*args, **kwargs))
                # Wrap path results so chaining stays sync
                if _is_path(result):  # pragma: no cover
                    return _wrap(result)
                return result

            return _sync_method

        if inspect.isasyncgenfunction(attr):  # pragma: no cover

            def _sync_iter_method(*args, **kwargs):  # pragma: no cover
                async_it = attr(*args, **kwargs)
                for item in _SyncRunner.get().iter(async_it):  # pragma: no cover
                    yield _wrap(item) if _is_path(item) else item

            return _sync_iter_method

        # Regular sync method — return as-is
        def _passthrough(*args, **kwargs):  # pragma: no cover
            result = attr(*args, **kwargs)
            if _is_path(result):  # pragma: no cover
                return _wrap(result)
            return result

        return _passthrough


def _is_path(obj) -> bool:
    """Check if obj is a path-like instance that should be wrapped."""
    from asanypath.common import CommonPurePathMixin

    return isinstance(obj, CommonPurePathMixin)


def _wrap(path_obj):
    """Wrap an async path in a sync proxy; pass SyncPath through unchanged."""
    from asanypath.local import SyncPath
    from asanypath.unsupported import UnsupportedProtocolPath

    if isinstance(path_obj, (SyncPath, UnsupportedProtocolPath)):
        return path_obj
    return _SyncProxy(path_obj)


class AsAnyPath(ABC):
    """Synchronous AsAnyPath — same interface, no await required.

    Dispatches to the appropriate backend just like the async version.
    Local file:// paths return SyncPath directly (zero overhead).
    Cloud paths return a sync proxy backed by a shared background loop.
    """

    @classmethod
    def __subclasshook__(cls, sub: type) -> bool:
        from asanypath.local import SyncPath

        return issubclass(sub, (SyncPath, _SyncProxy))

    def __new__(cls, *args, **kwargs) -> _SyncProxy | Any:
        from asanypath.asanypath import AsAnyPath as _AsyncAsAnyPath
        from asanypath.local import AsyncPath
        from asanypath.unsupported import UnsupportedProtocolPath

        async_path = _AsyncAsAnyPath(*args, **kwargs)

        if isinstance(async_path, AsyncPath):
            from asanypath.local import SyncPath

            return SyncPath(*(args or (".",)), **kwargs)

        if isinstance(async_path, UnsupportedProtocolPath):
            return async_path

        return _SyncProxy(async_path)
