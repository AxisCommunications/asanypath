# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

import fnmatch
import inspect
import io
import time
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from os import PathLike
from os.path import expanduser
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal

import msgspec

if TYPE_CHECKING:
    from typing import Self

from asanypath._transfer import (
    async_destination_state,
    has_async_transfer_api,
    require_force,
    run_transfer_async,
)
from asanypath.batcher import H2_BATCH_THRESHOLD, MicroBatcher, make_batcher
from asanypath.common import SCHEME_SEP, CommonPurePathMixin
from asanypath.options import AccessPolicy, AccessPolicyPatch, BackendOptions


class CloudPathMixin(CommonPurePathMixin):
    """Base class for cloud path implementations.

    Subclasses must provide:

    - ``_item_path`` — property returning the backend-specific object key
      (e.g. the S3 key, GCS object path, Azure blob path).
    - ``_native_kwargs`` — property returning a dict of auth / endpoint kwargs
      that are forwarded to every batcher call and native function.
    - ``_batcher_key_fn`` — staticmethod producing a cache key string from
      ``**_native_kwargs``.
    - ``_batcher_ops`` — dict describing batch operations (passed to
      :func:`make_batcher`).

    Optionally, set the class attribute ``_is_dir_batch_fn`` to a native
    batch-is-dir coroutine (e.g. ``s3_is_dir_batch``) for an efficient
    ``walk()`` implementation.  If not set, ``walk()`` falls back to
    per-entry ``is_dir()`` calls via ``asyncio.gather``.
    """

    protocol: str = "cloud"  # Default protocol, should be overridden by subclasses

    # TTL-based listing cache: {cache_key: (timestamp, [uri, ...])}
    _listing_cache: dict[str, tuple[float, list[str]]] = {}
    listing_cache_ttl: float = 60.0  # seconds; set to 0 to disable

    # Subclasses may set this to a native batch-is-dir function.
    _is_dir_batch_fn = None

    # Declarative batcher configuration — override in subclasses.
    _batcher_key_fn: staticmethod = None  # type: ignore[assignment]
    _batcher_ops: dict | None = None

    # Shared batcher instance cache: {cache_key: MicroBatcher}
    _batcher_instances: dict[str, MicroBatcher] = {}

    # Per-class cached env config — subclasses override _create_env_config().
    _env_config: SimpleNamespace | None = None

    # Subclasses set True once they implement _range_read with native support.
    _supports_range_read: bool = False

    # Subclasses set this to a native server-side copy batch fn (e.g. s3_copy_batch).
    _copy_batch_fn = None

    def _can_server_side_copy(self, dst: Any) -> bool:
        """True when *dst* is the same backend + credentials, so a native
        server-side copy (no client round-trip) can be used."""
        return (
            type(self)._copy_batch_fn is not None
            and type(self) is type(dst)
            and self._native_kwargs == dst._native_kwargs
        )

    async def _server_side_copy(
        self,
        dst: Any,
        *,
        want_size: bool = False,
        destination_backend_options: BackendOptions | None = None,
    ) -> int:
        """Copy this object to *dst* via the backend's native copy API.

        Returns the object size when *want_size* (for progress reporting),
        else 0 — no bytes transit the client either way.
        """
        kwargs = {
            "pairs": [(self._item_path, dst._item_path)],
            "use_h2": False,
            **self._native_kwargs,
        }
        if destination_backend_options is not None:
            kwargs["options_json"] = msgspec.json.encode(destination_backend_options).decode()
        await type(self)._copy_batch_fn(**kwargs)
        if not want_size:
            return 0
        try:
            st = await dst.stat()
        except Exception:  # noqa: BLE001
            return 0
        return int(getattr(st, "st_size", 0) or 0)

    def __bytes__(self) -> bytes:
        return str(self).encode()

    def __fspath__(self) -> str:
        raise TypeError(f"{type(self).__name__} is not a local filesystem path")

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    @property
    def _item_path(self) -> str:
        """The backend-specific object / blob / key path.  Override in subclasses."""
        raise NotImplementedError(f"{type(self).__name__} must define _item_path")

    @property
    def _native_kwargs(self) -> dict:
        """Auth / endpoint kwargs forwarded to batcher and native calls."""
        raise NotImplementedError(f"{type(self).__name__} must define _native_kwargs")

    async def is_dir(self) -> bool:
        """Check if path is a directory prefix. Override in subclasses."""
        raise NotImplementedError(f"{type(self).__name__} must define is_dir()")

    def _get_batcher(self) -> MicroBatcher:
        """Return a cached :class:`MicroBatcher` built from class attrs."""
        key_fn = type(self)._batcher_key_fn
        ops = type(self)._batcher_ops
        if key_fn is None or ops is None:
            raise NotImplementedError(
                f"{type(self).__name__} must define _batcher_key_fn and _batcher_ops"
            )
        cache_key = key_fn(**self._native_kwargs)
        batcher = self._batcher_instances.get(cache_key)
        if batcher is None:
            batcher = make_batcher(key_fn=key_fn, ops=ops)
            self._batcher_instances[cache_key] = batcher
        return batcher

    @property
    def env_config(self) -> SimpleNamespace:
        """Lazy per-class env config. Subclasses override _create_env_config()."""
        if self._env_config is None:
            type(self)._env_config = self._create_env_config()
        return self._env_config  # type: ignore[return-value]

    @env_config.setter
    def env_config(self, value: SimpleNamespace | None) -> None:
        type(self)._env_config = value

    @classmethod
    def _create_env_config(cls) -> SimpleNamespace:
        """Build env config from environment variables. Override in subclasses."""
        raise NotImplementedError(f"{cls.__name__} must define _create_env_config()")

    # ------------------------------------------------------------------
    # HTTP request plumbing (delegated to Rust native via subclasses)
    # ------------------------------------------------------------------

    async def request(
        self,
        method: Literal["GET", "PUT", "POST", "HEAD", "DELETE", "OPTIONS", "PATCH"],
        url: str,
        **kwargs: Any,
    ) -> AsyncGenerator[bytes, None]:
        """Low-level request implementation.  Override in subclasses."""
        raise NotImplementedError(f"{type(self).__name__} does not implement request()")
        yield  # pragma: no cover — makes this an async generator

    async def _request(
        self,
        method: Literal["GET", "PUT", "POST", "HEAD", "DELETE", "OPTIONS", "PATCH"],
        url: str = "",
        **kwargs: Any,
    ) -> list[bytes]:
        """Buffer the full response from request(). Retries handled by Rust/reqwest."""
        return [chunk async for chunk in self.request(method, url, **kwargs)]

    # ------------------------------------------------------------------
    # Generic batcher-backed I/O (shared across all cloud backends)
    # ------------------------------------------------------------------

    async def exists(self) -> bool:
        """Check if object exists."""
        return await self._get_batcher().exists(
            item=self._item_path,
            **self._native_kwargs,
        )

    async def is_file(self) -> bool:
        """True if the path points to an object (not a directory prefix)."""
        if not await self.exists():
            return False
        return not await self.is_dir()

    async def read_bytes(self) -> bytes:
        """Read object content as bytes."""
        return await self._get_batcher().get(
            item=self._item_path,
            **self._native_kwargs,
        )

    async def iter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        """Yield object bytes in chunks without blocking the event loop."""
        if chunk_size is not None and chunk_size <= 0:
            raise ValueError("chunk_size must be None or a positive integer")
        if chunk_size is None:
            chunk_size = _CloudFile._DEFAULT_CHUNK
        if not self._supports_range_read:
            data = await self.read_bytes()
            for offset in range(0, len(data), chunk_size):
                yield data[offset : offset + chunk_size]
            return
        # Stream sequential windows until the object is exhausted. A short read
        # (or an empty read past end-of-object) signals EOF — no size probe needed.
        pos = 0
        while True:
            chunk = await self._range_read(pos, pos + chunk_size - 1)
            if not chunk:
                break
            yield chunk
            if len(chunk) < chunk_size:
                break
            pos += len(chunk)

    async def _range_read(self, start: int, end: int) -> bytes:
        """Read bytes [start, end] from the object via range request."""
        from asanypath_native import range_read

        return bytes(
            await range_read(path=self._item_path, start=start, end=end, **self._native_kwargs)
        )

    async def read_text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """Read object content as text."""
        return (await self.read_bytes()).decode(encoding=encoding, errors=errors)

    async def write_bytes(
        self, data: bytes, *, backend_options: BackendOptions | None = None
    ) -> int:
        """Write bytes to object."""
        options_json = (
            msgspec.json.encode(backend_options).decode() if backend_options is not None else "{}"
        )
        await self._get_batcher().put(
            item=(
                self._item_path,
                data,
                options_json,
            ),
            **self._native_kwargs,
        )
        return len(data)

    async def write_text(
        self,
        data: str,
        encoding: str = "utf-8",
        *,
        backend_options: BackendOptions | None = None,
    ) -> int:
        """Write text to object."""
        encoded = data.encode(encoding)
        if backend_options is None:
            return await self.write_bytes(encoded)
        return await self.write_bytes(encoded, backend_options=backend_options)

    async def unlink(self, missing_ok: bool = False) -> None:
        """Delete object."""
        try:
            await self._get_batcher().delete(
                item=self._item_path,
                **self._native_kwargs,
            )
        except FileNotFoundError:  # pragma: no cover
            if not missing_ok:  # pragma: no cover
                raise

    async def _fetch_listing(self) -> list[str]:
        """Fetch directory listing and return URIs."""
        containers = await self._list_containers()
        if containers is not None:
            return containers
        try:
            return await self._get_batcher().list(
                item=self._item_path,
                **self._native_kwargs,
            )
        except FileNotFoundError:
            raise NotADirectoryError(20, f"Not a directory: '{self}'")

    async def _list_containers(self) -> list[str] | None:
        """Return bucket/container/repo URIs when this path is the service root.

        Returns ``None`` (the default) when a bucket/container/repo is already
        selected, so callers fall through to normal object listing. Backends
        with a listable root (S3, GCS, Azure, Artifactory) override this to
        power both ``ls s3://`` and top-level shell completion. The empty-root
        check is a cheap in-memory test, so normal listings pay no extra cost.
        """
        return None

    # ------------------------------------------------------------------
    # Cached directory listing
    # ------------------------------------------------------------------

    def _listing_cache_key(self) -> str:
        """Cache key for this path's directory listing."""
        return str(self)

    async def iterdir(self, *, fresh: bool = False) -> AsyncIterator[Self]:
        """Iterate over directory contents with TTL caching.

        Results are cached for ``listing_cache_ttl`` seconds.
        Pass ``fresh=True`` to bypass the cache and force a network fetch.
        """
        cache_key = self._listing_cache_key()
        cache = type(self)._listing_cache

        if not fresh and self.listing_cache_ttl > 0:
            entry = cache.get(cache_key)
            if entry is not None:
                ts, uris = entry
                if (time.monotonic() - ts) < self.listing_cache_ttl:
                    for uri in uris:
                        yield type(self)(uri)
                    return

        uris = await self._fetch_listing()

        if self.listing_cache_ttl > 0:
            cache[cache_key] = (time.monotonic(), uris)

        for uri in uris:
            yield type(self)(uri)

    def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support chmod")

    @classmethod
    def cwd(cls) -> Self:
        raise NotImplementedError(f"{cls.__name__} does not support cwd")

    def expanduser(self) -> Self:
        return self

    async def get_access_policy(
        self, *, backend_options: BackendOptions | None = None
    ) -> AccessPolicy:
        raise NotImplementedError(f"{type(self).__name__} does not support access policies")

    def group(self) -> str:
        raise NotImplementedError(f"{type(self).__name__} does not support group")

    def hardlink_to(self, target: str | bytes | PathLike[str] | PathLike[bytes]) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support hardlink_to")

    @classmethod
    def home(cls) -> Self:
        return cls(cls.protocol + SCHEME_SEP, expanduser("~"))

    def is_absolute(self) -> bool:
        return True

    def is_block_device(self) -> bool:
        return False

    def is_char_device(self) -> bool:
        return False

    def is_fifo(self) -> bool:
        return False

    def is_junction(self) -> bool:
        return False

    def is_mount(self) -> bool:
        return False

    def is_reserved(self) -> bool:
        return False

    def is_socket(self) -> bool:
        return False

    def is_symlink(self) -> bool:
        return False

    def lchmod(self, mode: int) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support lchmod")

    def lstat(self):
        raise NotImplementedError(f"{type(self).__name__} does not support lstat")

    def owner(self) -> str:
        raise NotImplementedError(f"{type(self).__name__} does not support owner")

    def readlink(self) -> Self:
        raise NotImplementedError(f"{type(self).__name__} does not support readlink")

    def resolve(self, strict: bool = False) -> Self:
        return self

    def samefile(self, other_path: str | PathLike[str]) -> bool:
        return str(self.resolve()) == str(type(self)(other_path).resolve())

    def symlink_to(
        self,
        target: str | bytes | PathLike[str] | PathLike[bytes],
        target_is_directory: bool = False,
    ) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support symlink_to")

    # ------------------------------------------------------------------
    # Default implementations (copy+delete rename, glob via iterdir, etc.)
    # Backends may override for better performance.
    # ------------------------------------------------------------------

    async def replace(
        self,
        target: str,
        *,
        destination_backend_options: BackendOptions | None = None,
        on_progress: Callable[[int, int, Self, Self], None | Awaitable[None]] | None = None,
        on_done: Callable[[Self, Self, int, bool, BaseException | None], None | Awaitable[None]]
        | None = None,
    ) -> Self:
        """Replace object at target."""
        return await self.rename(
            target,
            force=True,
            destination_backend_options=destination_backend_options,
            on_progress=on_progress,
            on_done=on_done,
        )

    async def rename(
        self,
        target: str,
        *,
        force: bool = False,
        atomic: bool = False,
        chunk_size: int | None = None,
        destination_backend_options: BackendOptions | None = None,
        on_progress: Callable[[int, int, Self, Self], None | Awaitable[None]] | None = None,
        on_done: Callable[[Self, Self, int, bool, BaseException | None], None | Awaitable[None]]
        | None = None,
    ) -> Self:
        """Rename/move object via copy + delete."""
        dst = type(self)(target)

        async def _copy_file() -> int:
            if not needs_copy:
                return 0
            if self._can_server_side_copy(dst):
                return await self._server_side_copy(
                    dst,
                    want_size=on_progress is not None,
                    destination_backend_options=destination_backend_options,
                )
            data = await self.read_bytes()
            if destination_backend_options is None:
                await dst.write_bytes(data)
            else:
                await dst.write_bytes(data, backend_options=destination_backend_options)
            return len(data)

        needs_copy, same_path, destination_exists = await async_destination_state(self, dst)
        if needs_copy and destination_exists and not force:
            require_force(force, dst)

        await run_transfer_async(
            src=self,
            dst=dst,
            copy_fn=_copy_file,
            remove_fn=self.unlink,
            remove_src=not same_path,
            on_progress=on_progress,
            on_done=on_done,
        )
        return dst

    async def move(
        self,
        target: str,
        *,
        force: bool = False,
        atomic: bool = False,
        chunk_size: int | None = None,
        destination_backend_options: BackendOptions | None = None,
        on_progress: Callable[[int, int, Self, Self], None | Awaitable[None]] | None = None,
        on_done: Callable[[Self, Self, int, bool, BaseException | None], None | Awaitable[None]]
        | None = None,
    ) -> Self:
        return await self.rename(
            target,
            force=force,
            atomic=atomic,
            chunk_size=chunk_size,
            destination_backend_options=destination_backend_options,
            on_progress=on_progress,
            on_done=on_done,
        )

    async def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        """Create empty object or update timestamp."""
        if not exist_ok and await self.exists():
            raise FileExistsError(17, f"File exists: '{self}'")
        await self.write_bytes(b"")

    async def update_access_policy(
        self, policy_patch: AccessPolicyPatch, *, backend_options: BackendOptions | None = None
    ) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support access policies")

    async def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """No-op for cloud backends (directories are virtual prefixes)."""

    async def rmdir(self, *, recursive: bool = False) -> None:
        """Remove directory prefix.

        With ``recursive=True``, deletes all objects under this prefix.
        Without it, only succeeds if the prefix is empty (no children).
        """
        if not recursive:
            children = [c async for c in self.iterdir()]
            if children:
                raise OSError(39, f"Directory not empty: '{self}'")
            return
        # Recursive: collect all files, then unlink
        files = []
        async for child in self.iterdir():
            if await child.is_dir():
                await child.rmdir(recursive=True)
            else:
                files.append(child)
        for f in files:
            await f.unlink()

    async def copy(
        self,
        dst,
        *,
        recursive: bool = False,
        remove_src: bool = False,
        force: bool = False,
        atomic: bool = False,
        chunk_size: int | None = None,
        destination_backend_options: BackendOptions | None = None,
        on_progress: Callable[[int, int, Self, Self], None | Awaitable[None]] | None = None,
        on_done: Callable[[Self, Self, int, bool, BaseException | None], None | Awaitable[None]]
        | None = None,
    ):
        """Copy this path to *dst*.

        *dst* can be any path type (cross-backend supported).
        With ``recursive=True``, copies entire directory tree.
        With ``remove_src=True``, deletes source after copy (move semantics).
        Returns the destination path.
        """
        from asanypath import AsAnyPath

        dst = dst if has_async_transfer_api(dst) else AsAnyPath(str(dst))
        same_native = self._can_server_side_copy(dst)
        # chunk_size is a local streaming hint; cloud writes are single PUTs, so
        # it is ignored (not an error) for non-local destinations.
        if atomic and getattr(dst, "protocol", None) != "file" and not same_native:
            raise NotImplementedError(
                "atomic=True is only supported for local or same-backend destinations"
            )
        if getattr(dst, "protocol", None) == "file" and destination_backend_options is not None:
            raise ValueError("destination_backend_options require a cloud destination")
        if getattr(dst, "protocol", None) == "file" and (atomic or chunk_size):
            return await dst.copy_from(
                self,
                force=force,
                atomic=atomic,
                chunk_size=chunk_size,
                on_progress=on_progress,
                on_done=on_done,
            )
        transferred = 0
        error: BaseException | None = None

        async def _emit_progress(delta: int) -> None:
            nonlocal transferred
            if on_progress is None or delta <= 0:  # pragma: no cover
                return
            transferred += delta
            maybe = on_progress(delta, transferred, self, dst)
            if inspect.isawaitable(maybe):  # pragma: no cover
                await maybe

        async def _emit_done() -> None:
            if on_done is None:  # pragma: no cover
                return
            maybe = on_done(self, dst, transferred, error is None, error)
            if inspect.isawaitable(maybe):  # pragma: no cover
                await maybe

        async def _copy_recursive(src_node: Self, dst_node: Self) -> None:
            if await src_node.is_dir():
                await dst_node.mkdir(parents=True, exist_ok=True)
                async for child in src_node.iterdir():
                    await _copy_recursive(child, dst_node / child.name)
            else:

                async def _copy_leaf() -> int:
                    if not needs_copy:
                        return 0
                    if src_node._can_server_side_copy(dst_node):
                        return await src_node._server_side_copy(
                            dst_node,
                            want_size=on_progress is not None,
                            destination_backend_options=destination_backend_options,
                        )
                    await dst_node.parent.mkdir(parents=True, exist_ok=True)
                    data = await src_node.read_bytes()
                    if destination_backend_options is None:
                        await dst_node.write_bytes(data)
                    else:
                        await dst_node.write_bytes(
                            data, backend_options=destination_backend_options
                        )
                    return len(data)

                needs_copy, same_path, destination_exists = await async_destination_state(
                    src_node, dst_node
                )
                if not needs_copy:
                    if same_path:
                        return
                elif destination_exists and not force:
                    require_force(force, dst_node)

                await run_transfer_async(
                    src=src_node,
                    dst=dst_node,
                    copy_fn=_copy_leaf,
                    remove_fn=src_node.unlink,
                    remove_src=remove_src and not same_path,
                    on_progress=lambda delta, transferred, _src, _dst: _emit_progress(delta),
                    on_done=None,
                )

        try:
            if recursive and await self.is_dir():
                same_path = str(self) == str(dst)
                await _copy_recursive(self, dst)
                if remove_src and not same_path:
                    await self.rmdir()
            elif await self.is_dir():
                raise IsADirectoryError(21, f"Is a directory: '{self}' (use recursive=True)")
            else:

                async def _copy_file() -> int:
                    if not needs_copy:
                        return 0
                    if self._can_server_side_copy(dst):
                        return await self._server_side_copy(
                            dst,
                            want_size=on_progress is not None,
                            destination_backend_options=destination_backend_options,
                        )
                    await dst.parent.mkdir(parents=True, exist_ok=True)
                    data = await self.read_bytes()
                    if destination_backend_options is None:
                        await dst.write_bytes(data)
                    else:
                        await dst.write_bytes(data, backend_options=destination_backend_options)
                    return len(data)

                needs_copy, same_path, destination_exists = await async_destination_state(self, dst)
                if not needs_copy:
                    if same_path:
                        return 0
                elif destination_exists and not force:
                    require_force(force, dst)

                await run_transfer_async(
                    src=self,
                    dst=dst,
                    copy_fn=_copy_file,
                    remove_fn=self.unlink,
                    remove_src=remove_src and not same_path,
                    on_progress=lambda delta, transferred, _src, _dst: _emit_progress(delta),
                    on_done=None,
                )
        except BaseException as exc:  # noqa: BLE001
            error = exc
            raise
        finally:
            await _emit_done()
        return dst

    def open(
        self,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        *,
        backend_options: BackendOptions | None = None,
    ):
        """Return a file-like object backed by cloud read/write.

        Supports both sync (``with``) and async (``async with``) context managers.
        Read mode fetches the full object on first read; write mode buffers and
        uploads on close.

        The *buffering* parameter controls how much data is fetched per
        range-read request (analogous to local file buffer size):

        - ``-1`` (default): 8 MiB chunks (optimal for cloud latency).
        - ``0``: unbuffered — each ``read(n)`` issues one range request.
        - ``N > 0``: fetch in *N*-byte chunks.
        """
        if backend_options is not None and "r" in mode:
            raise ValueError("backend_options are only supported for write modes")
        return _CloudFile(self, mode, buffering, encoding, errors, newline, backend_options)

    async def glob(
        self, pattern: str, *, case_sensitive: bool | None = None
    ) -> AsyncIterator[Self]:
        """Glob pattern matching via iterdir."""
        async for child in self.iterdir():
            if fnmatch.fnmatch(child.name, pattern):
                yield child

    async def rglob(
        self, pattern: str, *, case_sensitive: bool | None = None
    ) -> AsyncIterator[Self]:
        """Recursive glob pattern matching."""
        async for child in self.iterdir():
            if fnmatch.fnmatch(child.name, pattern):
                yield child
            if await child.is_dir():
                async for grandchild in child.rglob(pattern, case_sensitive=case_sensitive):
                    yield grandchild

    async def walk(self) -> AsyncIterator[tuple[Self, list[Self], list[Self]]]:
        """Recursively walk directory tree.

        Uses ``_is_dir_batch_fn`` for an efficient batch check when available,
        otherwise falls back to per-entry ``is_dir()`` via ``asyncio.gather``.
        """
        entries = [entry async for entry in self.iterdir()]
        batch_fn = type(self)._is_dir_batch_fn
        if entries and batch_fn is not None:
            prefixes = [e._item_path for e in entries]
            is_dir_results = await batch_fn(
                prefixes=prefixes,
                use_h2=len(prefixes) >= H2_BATCH_THRESHOLD,
                **self._native_kwargs,
            )
        elif entries:
            import asyncio

            is_dir_results = await asyncio.gather(*(e.is_dir() for e in entries))
        else:
            is_dir_results = []
        dirs = [e for e, d in zip(entries, is_dir_results) if d]
        files = [e for e, d in zip(entries, is_dir_results) if not d]
        yield self, dirs, files
        for d in dirs:
            async for result in d.walk():
                yield result


# ---------------------------------------------------------------------------
# Cloud file handle
# ---------------------------------------------------------------------------


class _CloudRawIO(io.RawIOBase):
    """Raw I/O stream backed by cloud range reads.

    Each ``readinto()`` call issues a single range request for the requested
    number of bytes.  Wrap in ``io.BufferedReader`` to get chunked prefetch.
    """

    def __init__(self, path, size: int):
        super().__init__()
        self._path = path
        self._size = size
        self._pos = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def readinto(self, b):
        if self._pos >= self._size:
            return 0
        n_want = min(len(b), self._size - self._pos)
        end = self._pos + n_want - 1
        from asanypath.sync import _SyncRunner

        data = _SyncRunner.get().run(self._path._range_read(self._pos, end))
        n = len(data)
        b[:n] = data
        self._pos += n
        return n

    def seek(self, offset, whence=0):
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = self._size + offset
        self._pos = max(0, min(self._pos, self._size))
        return self._pos

    def tell(self):
        return self._pos


class _CloudFile:
    """File handle backed by cloud read/write with lazy range-read support.

    Supports both sync (``with``) and async (``async with``) context managers.

    Read mode behaviour depends on file size and ``buffering``:

    - **Small files** (size <= chunk_size) or no range-read support:
      fetch everything on enter (single GET).
    - **Large files** with range-read support: return a
      ``BufferedReader(_CloudRawIO)`` that fetches chunks lazily.

    Write mode buffers all writes and uploads on close.
    """

    _DEFAULT_CHUNK = 8 * 1024 * 1024  # 8 MiB — good default for cloud latency

    def __init__(self, path, mode, buffering, encoding, errors, newline, backend_options=None):
        self._path = path
        self._mode = mode
        self._buffering = buffering
        self._encoding = encoding
        self._errors = errors
        self._newline = newline
        self._backend_options = backend_options
        self._buf: io.BytesIO | None = None
        self._text_wrapper: io.TextIOWrapper | None = None
        self._reader: io.BufferedReader | None = None
        self._closed = False
        self._sync = False

    @property
    def _chunk_size(self) -> int:
        """Effective chunk size for range reads."""
        if self._buffering <= 0:
            return self._DEFAULT_CHUNK
        return self._buffering

    def _make_buffer(self, data: bytes | None = None) -> io.IOBase:
        """Create an in-memory buffer (for small files or write mode)."""
        if "r" in self._mode:
            self._buf = io.BytesIO(data or b"")
        else:
            self._buf = io.BytesIO()
        if "b" not in self._mode:
            self._text_wrapper = io.TextIOWrapper(
                self._buf,
                encoding=self._encoding or "utf-8",
                errors=self._errors or "strict",
                newline=self._newline,
            )
            return self._text_wrapper
        return self._buf

    def _make_reader(self, size: int) -> io.IOBase:
        """Create a lazily-filled reader backed by range reads."""
        raw = _CloudRawIO(self._path, size)
        if self._buffering == 0:
            # Unbuffered: return raw directly (binary only)
            if "b" not in self._mode:
                raise ValueError("can't have unbuffered text I/O")
            return raw
        reader = io.BufferedReader(raw, buffer_size=self._chunk_size)
        self._reader = reader
        if "b" not in self._mode:
            return io.TextIOWrapper(
                reader,
                encoding=self._encoding or "utf-8",
                errors=self._errors or "strict",
                newline=self._newline,
            )
        return reader

    def _flush_write(self) -> bytes:
        """Extract buffered write data."""
        if self._text_wrapper is not None:
            self._text_wrapper.flush()
        return self._buf.getvalue()

    def _get_size(self, headers: dict) -> int | None:
        """Extract content-length from HEAD response headers."""
        for k, v in headers.items():
            if k.lower() == "content-length":
                try:
                    return int(v)
                except (ValueError, TypeError):
                    return None
        return None

    # ------------------------------------------------------------------
    # Sync context manager
    # ------------------------------------------------------------------

    def __enter__(self):
        from asanypath.sync import _SyncRunner

        self._sync = True
        runner = _SyncRunner.get()
        if "r" in self._mode:
            if self._path._supports_range_read:
                try:
                    headers = runner.run(
                        self._path._get_batcher().head(
                            item=self._path._item_path, **self._path._native_kwargs
                        )
                    )
                    size = self._get_size(headers)
                    if size is not None and size > self._chunk_size:
                        return self._make_reader(size)
                except Exception:  # pragma: no cover
                    pass
            # Small file or no range-read: fetch everything
            data = runner.run(self._path.read_bytes())
            return self._make_buffer(data)
        return self._make_buffer()

    def __exit__(self, exc_type, exc_val, exc_tb):
        from asanypath.sync import _SyncRunner

        if ("w" in self._mode or "a" in self._mode) and exc_type is None:
            data = self._flush_write()
            if self._backend_options is None:
                _SyncRunner.get().run(self._path.write_bytes(data))
            else:
                _SyncRunner.get().run(
                    self._path.write_bytes(data, backend_options=self._backend_options)
                )
        if self._reader is not None:
            self._reader.close()
        self._closed = True
        return False  # pragma: no cover

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self):
        self._sync = False
        if "r" in self._mode:
            if self._path._supports_range_read:
                try:
                    headers = await self._path._get_batcher().head(
                        item=self._path._item_path, **self._path._native_kwargs
                    )
                    size = self._get_size(headers)
                    if size is not None and size > self._chunk_size:
                        return self._make_reader(size)
                except Exception:  # pragma: no cover
                    pass
            # Small file or no range-read: fetch everything
            data = await self._path.read_bytes()
            return self._make_buffer(data)
        return self._make_buffer()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if ("w" in self._mode or "a" in self._mode) and exc_type is None:
            data = self._flush_write()
            if self._backend_options is None:
                await self._path.write_bytes(data)
            else:
                await self._path.write_bytes(data, backend_options=self._backend_options)
        if self._reader is not None:
            self._reader.close()
        self._closed = True
        return False  # pragma: no cover
