# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

import inspect
import os
import shutil
import typing
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from pathlib import Path

from anyio import Path as AnyIOPath
from anyio import to_thread

from asanypath._transfer import (
    _DEFAULT_CHUNK_SIZE,
    _atomic_temp_path,
    _commit_temp_sync,
    _validate_chunk_size,
    async_destination_state,
    has_async_transfer_api,
    has_write_api,
    is_remote_destination,
    local_copy_file_sync,
    require_force,
    run_sync_maybe,
    run_transfer_async,
    run_transfer_sync,
    sync_destination_state,
)
from asanypath.common import CommonPurePathMixin
from asanypath.options import (
    AccessAction,
    AccessGrant,
    AccessPolicy,
    AccessPolicyPatch,
    BackendOptions,
)

# Files smaller than this are read/written synchronously to avoid thread pool overhead.
# 1 MiB is well within the kernel page cache for typical workloads.
_SYNC_THRESHOLD = 1 * 1024 * 1024


# SyncPath/AsyncPath always store a pathlib.Path in _path (protocol="file"),
# unlike the cloud backends whose CommonPurePathMixin stores a yarl URL.
S: typing.TypeAlias = "SyncPath"
T: typing.TypeAlias = "AsyncPath"


async def _local_copy_file_async(
    src_path: Path, dst_path: Path, *, chunk_size: int | None, atomic: bool, force: bool
) -> int:
    return await to_thread.run_sync(
        lambda: local_copy_file_sync(
            src_path, dst_path, chunk_size=chunk_size, atomic=atomic, force=force
        )
    )


def _looks_remote(dst) -> bool:
    return is_remote_destination(dst)


def _resolve_sync_remote_dst(dst):
    if not _looks_remote(dst):
        return None
    if has_write_api(dst):
        return dst
    from asanypath.sync import AsAnyPath

    return AsAnyPath(str(dst))


def _resolve_async_remote_dst(dst):
    if not _looks_remote(dst):
        return None
    if has_async_transfer_api(dst):
        return dst
    from asanypath import AsAnyPath

    return AsAnyPath(str(dst))


def _dir_total_bytes(path: Path) -> int:
    total = 0
    for root, _, files in os.walk(path):
        base = Path(root)
        for name in files:
            try:
                total += (base / name).stat().st_size
            except OSError:
                pass
    return total


def _follow_symlinks(backend_options: BackendOptions | None) -> bool:
    if backend_options is None:
        return True
    follow_symlinks = backend_options.provider.get("follow_symlinks", True)
    if not isinstance(follow_symlinks, bool):
        raise TypeError("backend_options.provider['follow_symlinks'] must be a bool")
    return follow_symlinks


def _access_policy(path: Path, *, follow_symlinks: bool) -> AccessPolicy:
    stat = os.stat(path, follow_symlinks=follow_symlinks)
    action_bits: tuple[tuple[AccessAction, int], ...] = (
        ("read", 4),
        ("write", 2),
        ("execute", 1),
    )

    def actions(shift: int) -> frozenset[AccessAction]:
        return frozenset(action for action, bit in action_bits if stat.st_mode >> shift & bit)

    try:
        import grp
        import pwd

        owner = pwd.getpwuid(stat.st_uid).pw_name
        group = grp.getgrgid(stat.st_gid).gr_name
    except (ImportError, KeyError):
        owner = group = None

    return AccessPolicy(
        owner=owner,
        group=group,
        grants=(
            AccessGrant("owner", actions(6)),
            AccessGrant("group", actions(3)),
            AccessGrant("everyone", actions(0)),
        ),
    )


def _update_access_policy(
    path: Path, policy_patch: AccessPolicyPatch, *, follow_symlinks: bool
) -> None:
    if policy_patch.owner is not None or policy_patch.group is not None:
        import grp
        import pwd

        uid = -1 if policy_patch.owner is None else pwd.getpwnam(policy_patch.owner).pw_uid
        gid = -1 if policy_patch.group is None else grp.getgrnam(policy_patch.group).gr_gid
        os.chown(path, uid, gid, follow_symlinks=follow_symlinks)

    mode = os.stat(path, follow_symlinks=follow_symlinks).st_mode
    shifts = {"owner": 6, "group": 3, "everyone": 0}
    for grant in policy_patch.grants:
        bits = sum({"read": 4, "write": 2, "execute": 1}[action] for action in grant.actions)
        shift = shifts[grant.principal]
        mode = mode & ~(0o7 << shift) | bits << shift
    os.chmod(path, mode, follow_symlinks=follow_symlinks)


class SyncPath(CommonPurePathMixin):
    """Sync path implementation for local filesystem — zero async overhead."""

    protocol: str = "file"  # type: ignore
    _path: Path

    @classmethod
    def _cast(cls, path: Path) -> S:
        """Fast internal constructor from existing pathlib.Path (no parsing)."""
        obj = object.__new__(cls)
        obj._path = path
        return obj

    def __fspath__(self) -> str:
        return str(self._path)

    def __getattr__(self, name):
        return getattr(self._path, name)

    def iterdir(self) -> Generator[S]:
        for entry in self._path.iterdir():
            yield self._cast(entry)

    def glob(self, *args, **kwargs) -> Generator[S]:
        for item in self._path.glob(*args, **kwargs):
            yield self._cast(item)

    def rglob(self, *args, **kwargs) -> Generator[S]:
        for item in self._path.rglob(*args, **kwargs):
            yield self._cast(item)

    def walk(self, *args, **kwargs) -> Generator[tuple[S, list[str], list[str]]]:
        for root, dirs, files in os.walk(self._path, *args, **kwargs):
            yield self._cast(Path(root)), dirs, files

    def expanduser(self) -> S:
        return self._cast(self._path.expanduser())

    def get_access_policy(self, *, backend_options: BackendOptions | None = None) -> AccessPolicy:
        return _access_policy(self._path, follow_symlinks=_follow_symlinks(backend_options))

    def update_access_policy(
        self, policy_patch: AccessPolicyPatch, *, backend_options: BackendOptions | None = None
    ) -> None:
        _update_access_policy(
            self._path, policy_patch, follow_symlinks=_follow_symlinks(backend_options)
        )

    def resolve(self, strict=False) -> S:
        return self._cast(self._path.resolve(strict=strict))

    def rename(self, target, *, force: bool = False, atomic: bool = False) -> S:
        remote_target = _resolve_sync_remote_dst(target)
        if remote_target is not None:
            return self.copy(
                remote_target,
                recursive=self._path.is_dir(),
                remove_src=True,
                force=force,
                atomic=atomic,
            )
        # Use target._path directly when available so the returned SyncPath
        # stores the identical Path object — avoids Python 3.14+ differences
        # in what Path.rename() returns via with_segments().
        target_path = target._path if isinstance(target, SyncPath) else Path(str(target))
        target_sync = self._cast(target_path)
        needs_copy, same_path, destination_exists = sync_destination_state(self, target_sync)
        if not needs_copy:
            if same_path:
                return target_sync
            self._path.unlink()
            return target_sync
        if destination_exists and not force:
            require_force(force, target_sync)
        self._path.rename(target_path)
        return target_sync

    def move(self, target, *, force: bool = False, atomic: bool = False) -> S:
        return self.rename(target, force=force, atomic=atomic)

    def replace(self, target) -> S:
        return self._cast(self._path.replace(target))

    def readlink(self) -> S:
        return self._cast(self._path.readlink())

    def iter_bytes(self, chunk_size: int | None = None) -> Generator[bytes]:
        """Yield file contents in byte chunks (``chunk_size=None`` uses a default)."""
        if chunk_size is not None and chunk_size <= 0:
            raise ValueError("chunk_size must be None or a positive integer")
        if chunk_size is None:
            chunk_size = _DEFAULT_CHUNK_SIZE
        with self._path.open("rb") as f:
            while chunk := f.read(chunk_size):
                yield chunk

    def checksums(self) -> dict[str, str]:
        """Compute MD5, SHA-1 and SHA-256 checksums of a local file."""
        from hashlib import md5, sha1, sha256

        data = self._path.read_bytes()
        return {
            "md5": md5(data).hexdigest(),
            "sha1": sha1(data).hexdigest(),
            "sha256": sha256(data).hexdigest(),
        }

    def copy(
        self,
        dst,
        *,
        recursive: bool = False,
        remove_src: bool = False,
        force: bool = False,
        atomic: bool = False,
        chunk_size: int | None = None,
        on_progress: Callable[[int, int, SyncPath, SyncPath], None] | None = None,
        on_done: Callable[[SyncPath, SyncPath, int, bool, BaseException | None], None]
        | None = None,
    ) -> S:
        _validate_chunk_size(chunk_size)
        remote_dst = _resolve_sync_remote_dst(dst)
        if remote_dst is not None:
            if atomic:
                raise NotImplementedError("atomic=True is only supported for local destinations")
            # chunk_size (a local streaming hint) is ignored for cloud destinations.

            def _copy_remote_file() -> int:
                needs_copy, same_path, destination_exists = sync_destination_state(self, remote_dst)
                if not needs_copy:
                    return 0
                if destination_exists and not force:
                    require_force(force, remote_dst)
                run_sync_maybe(remote_dst.parent.mkdir(parents=True, exist_ok=True))
                data = self._path.read_bytes()
                written = run_sync_maybe(remote_dst.write_bytes(data))
                return written if isinstance(written, int) else len(data)

            def _copy_remote_tree() -> int:
                total = 0
                for root, _, files in os.walk(self._path):
                    relative_root = Path(root).relative_to(self._path)
                    for name in files:
                        source_file = self._cast(Path(root) / name)
                        relative = relative_root / name
                        destination_file = remote_dst / relative.as_posix()
                        needs_copy, _, destination_exists = sync_destination_state(
                            source_file, destination_file
                        )
                        if not needs_copy:
                            continue
                        if destination_exists and not force:
                            require_force(force, destination_file)
                        run_sync_maybe(destination_file.parent.mkdir(parents=True, exist_ok=True))
                        data = source_file._path.read_bytes()
                        written = run_sync_maybe(destination_file.write_bytes(data))
                        total += written if isinstance(written, int) else len(data)
                return total

            if self._path.is_dir():
                if not recursive:
                    raise IsADirectoryError(21, f"Is a directory: '{self}' (use recursive=True)")
                return run_transfer_sync(
                    src=self,
                    dst=remote_dst,
                    copy_fn=_copy_remote_tree,
                    remove_fn=lambda: shutil.rmtree(self._path),
                    remove_src=remove_src,
                    on_progress=on_progress,
                    on_done=on_done,
                )

            return run_transfer_sync(
                src=self,
                dst=remote_dst,
                copy_fn=_copy_remote_file,
                remove_fn=self._path.unlink,
                remove_src=remove_src,
                on_progress=on_progress,
                on_done=on_done,
            )

        dst_path = Path(str(dst))
        dst_sync = self._cast(dst_path)

        if recursive and self._path.is_dir():
            same_path = str(self) == str(dst_sync)

            def _copy_tree() -> int:
                for root, dirs, files in os.walk(self._path):
                    relative = Path(root).relative_to(self._path)
                    destination_root = dst_path / relative
                    destination_root.mkdir(parents=True, exist_ok=True)
                    for directory in dirs:
                        (destination_root / directory).mkdir(exist_ok=True)
                    for name in files:
                        source_file = self._cast(Path(root) / name)
                        destination_file = self._cast(destination_root / name)
                        needs_copy, same_path, destination_exists = sync_destination_state(
                            source_file, destination_file
                        )
                        if not needs_copy:
                            continue
                        if destination_exists and not force:
                            require_force(force, destination_file)
                        local_copy_file_sync(
                            source_file._path,
                            destination_file._path,
                            chunk_size=chunk_size,
                            atomic=atomic,
                            force=force,
                        )
                return _dir_total_bytes(self._path)

            return run_transfer_sync(
                src=self,
                dst=dst_sync,
                copy_fn=_copy_tree,
                remove_fn=lambda: shutil.rmtree(self._path),
                remove_src=remove_src and not same_path,
                on_progress=on_progress,
                on_done=on_done,
            )

        if self._path.is_dir():
            raise IsADirectoryError(21, f"Is a directory: '{self}' (use recursive=True)")

        def _copy_file() -> int:
            needs_copy, same_path, destination_exists = sync_destination_state(self, dst_sync)
            if not needs_copy:
                return 0
            if destination_exists and not force:
                require_force(force, dst_sync)
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            return local_copy_file_sync(
                self._path, dst_path, chunk_size=chunk_size, atomic=atomic, force=force
            )

        return run_transfer_sync(
            src=self,
            dst=dst_sync,
            copy_fn=_copy_file,
            remove_fn=self._path.unlink,
            remove_src=remove_src and str(self) != str(dst_sync),
            on_progress=on_progress,
            on_done=on_done,
        )

    def rmdir(self, *, recursive: bool = False) -> None:
        if recursive:
            shutil.rmtree(self._path)
        else:
            self._path.rmdir()


class AsyncPath(CommonPurePathMixin, AnyIOPath):
    """Async path implementation for local filesystem."""

    protocol: str = "file"  # type: ignore
    _path: Path  # type: ignore[assignment]

    def __init__(self, *parts, limiter=None) -> None:
        # AnyIOPath constructors and methods (cwd/home/absolute/...) pass
        # limiter=... through to child path objects and later dispatch work via
        # to_thread.run_sync(..., limiter=self._limiter).
        self._limiter = limiter
        super().__init__(*parts)

    @classmethod
    def _cast(cls, path: Path) -> T:
        """Fast internal constructor from existing pathlib.Path (no parsing)."""
        obj = object.__new__(cls)
        obj._path = path
        # _cast() bypasses __init__, so keep _limiter in sync with AnyIOPath's
        # expected instance contract.
        obj._limiter = None
        return obj

    async def expanduser(self) -> T:
        return self._cast(self._path.expanduser())

    async def get_access_policy(
        self, *, backend_options: BackendOptions | None = None
    ) -> AccessPolicy:
        return await to_thread.run_sync(
            lambda: _access_policy(self._path, follow_symlinks=_follow_symlinks(backend_options))
        )

    async def update_access_policy(
        self, policy_patch: AccessPolicyPatch, *, backend_options: BackendOptions | None = None
    ) -> None:
        await to_thread.run_sync(
            lambda: _update_access_policy(
                self._path, policy_patch, follow_symlinks=_follow_symlinks(backend_options)
            )
        )

    async def resolve(self) -> T:
        return self._cast(self._path.resolve())

    async def rename(self, target, *, force: bool = False, atomic: bool = False) -> T:
        remote_target = _resolve_async_remote_dst(target)
        if remote_target is not None:
            return await self.copy(
                remote_target,
                recursive=self._path.is_dir(),
                remove_src=True,
                force=force,
                atomic=atomic,
            )
        target_obj = self._cast(Path(str(target)))
        needs_copy, same_path, destination_exists = await async_destination_state(self, target_obj)
        if not needs_copy:
            if same_path:
                return target_obj
            self._path.unlink()
            return target_obj
        if destination_exists and not force:
            require_force(force, target_obj)
        return self._cast(self._path.rename(target_obj._path))

    async def move(self, target, *, force: bool = False, atomic: bool = False) -> T:
        return await self.rename(target, force=force, atomic=atomic)

    async def replace(self, *args, **kwargs) -> T:
        return self._cast(self._path.replace(*args, **kwargs))

    async def readlink(self) -> T:
        return self._cast(self._path.readlink())

    async def exists(self) -> bool:
        return self._path.exists()

    async def stat(self, *args, **kwargs):
        return self._path.stat(*args, **kwargs)

    async def is_file(self) -> bool:
        return self._path.is_file()

    async def is_dir(self) -> bool:
        return self._path.is_dir()

    async def read_bytes(self) -> bytes:
        if os.path.getsize(self._path) < _SYNC_THRESHOLD:
            return self._path.read_bytes()
        return await to_thread.run_sync(self._path.read_bytes)

    async def iter_bytes(self, chunk_size: int | None = None) -> AsyncGenerator[bytes]:
        """Yield file contents in byte chunks (``chunk_size=None`` uses a default)."""
        if chunk_size is not None and chunk_size <= 0:
            raise ValueError("chunk_size must be None or a positive integer")
        if chunk_size is None:
            chunk_size = _DEFAULT_CHUNK_SIZE
        async with await self.open("rb") as f:
            while chunk := await f.read(chunk_size):
                yield chunk

    async def read_text(self, *args, **kwargs) -> str:
        if os.path.getsize(self._path) < _SYNC_THRESHOLD:
            return self._path.read_text(*args, **kwargs)
        return await to_thread.run_sync(lambda: self._path.read_text(*args, **kwargs))

    async def write_bytes(self, data: bytes) -> int:
        if len(data) < _SYNC_THRESHOLD:
            return self._path.write_bytes(data)
        return await to_thread.run_sync(lambda: self._path.write_bytes(data))

    async def write_text(self, data: str, *args, **kwargs) -> int:
        if len(data) < _SYNC_THRESHOLD:
            return self._path.write_text(data, *args, **kwargs)
        return await to_thread.run_sync(lambda: self._path.write_text(data, *args, **kwargs))

    async def iterdir(self) -> AsyncGenerator[T]:
        for entry in self._path.iterdir():
            yield self._cast(entry)

    async def glob(self, *args, **kwargs) -> AsyncGenerator[T]:
        for item in self._path.glob(*args, **kwargs):
            yield self._cast(item)

    async def rglob(self, *args, **kwargs) -> AsyncGenerator[T]:
        for item in self._path.rglob(*args, **kwargs):
            yield self._cast(item)

    async def walk(self, *args, **kwargs) -> AsyncGenerator[tuple[T, list[str], list[str]]]:
        for root, dirs, files in os.walk(self._path, *args, **kwargs):
            yield self._cast(Path(root)), dirs, files

    async def checksums(self) -> dict[str, str]:
        """Compute MD5, SHA-1 and SHA-256 checksums of a local file."""
        from hashlib import md5, sha1, sha256

        data = await self.read_bytes()  # uses size-dispatch internally
        return {
            "md5": md5(data).hexdigest(),
            "sha1": sha1(data).hexdigest(),
            "sha256": sha256(data).hexdigest(),
        }

    async def copy(
        self,
        dst,
        *,
        recursive: bool = False,
        remove_src: bool = False,
        force: bool = False,
        atomic: bool = False,
        chunk_size: int | None = None,
        on_progress: Callable[[int, int, AsyncPath, AsyncPath], None | Awaitable[None]]
        | None = None,
        on_done: Callable[
            [AsyncPath, AsyncPath, int, bool, BaseException | None],
            None | Awaitable[None],
        ]
        | None = None,
    ) -> T:
        _validate_chunk_size(chunk_size)
        remote_dst = _resolve_async_remote_dst(dst)
        if remote_dst is not None:
            if atomic:
                raise NotImplementedError("atomic=True is only supported for local destinations")
            # chunk_size (a local streaming hint) is ignored for cloud destinations.

            async def _copy_remote_file() -> int:
                needs_copy, same_path, destination_exists = await async_destination_state(
                    self, remote_dst
                )
                if not needs_copy:
                    return 0
                if destination_exists and not force:
                    require_force(force, remote_dst)
                maybe_mkdir = remote_dst.parent.mkdir(parents=True, exist_ok=True)
                if inspect.isawaitable(maybe_mkdir):
                    await maybe_mkdir
                data = await self.read_bytes()
                written = remote_dst.write_bytes(data)
                if inspect.isawaitable(written):
                    written = await written
                return written if isinstance(written, int) else len(data)

            async def _copy_remote_tree() -> int:
                total = 0
                for root, _, files in os.walk(self._path):
                    relative_root = Path(root).relative_to(self._path)
                    for name in files:
                        source_file = self._cast(Path(root) / name)
                        relative = relative_root / name
                        destination_file = remote_dst / relative.as_posix()
                        needs_copy, _, destination_exists = await async_destination_state(
                            source_file, destination_file
                        )
                        if not needs_copy:
                            continue
                        if destination_exists and not force:
                            require_force(force, destination_file)
                        maybe_mkdir = destination_file.parent.mkdir(parents=True, exist_ok=True)
                        if inspect.isawaitable(maybe_mkdir):
                            await maybe_mkdir
                        data = await source_file.read_bytes()
                        written = destination_file.write_bytes(data)
                        if inspect.isawaitable(written):
                            written = await written
                        total += written if isinstance(written, int) else len(data)
                return total

            async def _remove_tree() -> None:
                await to_thread.run_sync(lambda: shutil.rmtree(self._path))

            async def _remove_file() -> None:
                self._path.unlink()

            if self._path.is_dir():
                if not recursive:
                    raise IsADirectoryError(21, f"Is a directory: '{self}' (use recursive=True)")
                return await run_transfer_async(
                    src=self,
                    dst=remote_dst,
                    copy_fn=_copy_remote_tree,
                    remove_fn=_remove_tree,
                    remove_src=remove_src,
                    on_progress=on_progress,
                    on_done=on_done,
                )

            return await run_transfer_async(
                src=self,
                dst=remote_dst,
                copy_fn=_copy_remote_file,
                remove_fn=_remove_file,
                remove_src=remove_src,
                on_progress=on_progress,
                on_done=on_done,
            )

        dst_path = Path(str(dst))
        dst_async = self._cast(dst_path)

        if recursive and self._path.is_dir():
            same_path = str(self) == str(dst_async)

            async def _copy_tree() -> int:
                for root, dirs, files in os.walk(self._path):
                    relative = Path(root).relative_to(self._path)
                    destination_root = dst_path / relative
                    await to_thread.run_sync(
                        lambda: destination_root.mkdir(parents=True, exist_ok=True)
                    )
                    for directory in dirs:
                        await to_thread.run_sync(
                            lambda: (destination_root / directory).mkdir(exist_ok=True)
                        )
                    for name in files:
                        source_file = self._cast(Path(root) / name)
                        destination_file = self._cast(destination_root / name)
                        needs_copy, same_path, destination_exists = await async_destination_state(
                            source_file, destination_file
                        )
                        if not needs_copy:
                            continue
                        if destination_exists and not force:
                            require_force(force, destination_file)
                        await _local_copy_file_async(
                            source_file._path,
                            destination_file._path,
                            chunk_size=chunk_size,
                            atomic=atomic,
                            force=force,
                        )
                return await to_thread.run_sync(lambda: _dir_total_bytes(self._path))

            async def _remove_tree() -> None:
                await to_thread.run_sync(lambda: shutil.rmtree(self._path))

            return await run_transfer_async(
                src=self,
                dst=dst_async,
                copy_fn=_copy_tree,
                remove_fn=_remove_tree,
                remove_src=remove_src and not same_path,
                on_progress=on_progress,
                on_done=on_done,
            )

        if self._path.is_dir():
            raise IsADirectoryError(21, f"Is a directory: '{self}' (use recursive=True)")

        async def _copy_file() -> int:
            needs_copy, same_path, destination_exists = await async_destination_state(
                self, dst_async
            )
            if not needs_copy:
                return 0
            if destination_exists and not force:
                require_force(force, dst_async)
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            return await _local_copy_file_async(
                self._path, dst_path, chunk_size=chunk_size, atomic=atomic, force=force
            )

        async def _remove_file() -> None:
            self._path.unlink()

        return await run_transfer_async(
            src=self,
            dst=dst_async,
            copy_fn=_copy_file,
            remove_fn=_remove_file,
            remove_src=remove_src and str(self) != str(dst_async),
            on_progress=on_progress,
            on_done=on_done,
        )

    async def copy_from(
        self,
        src,
        *,
        force: bool = False,
        atomic: bool = False,
        chunk_size: int | None = None,
        on_progress: Callable[[int, int, object, AsyncPath], None | Awaitable[None]] | None = None,
        on_done: Callable[
            [object, AsyncPath, int, bool, BaseException | None], None | Awaitable[None]
        ]
        | None = None,
    ) -> T:
        _validate_chunk_size(chunk_size)
        transferred = 0
        error: BaseException | None = None

        async def _emit_progress(delta: int) -> None:
            nonlocal transferred
            if on_progress is None or delta <= 0:
                return
            transferred += delta
            maybe = on_progress(delta, transferred, src, self)
            if inspect.isawaitable(maybe):
                await maybe

        async def _emit_done() -> None:
            if on_done is None:
                return
            maybe = on_done(src, self, transferred, error is None, error)
            if inspect.isawaitable(maybe):
                await maybe

        async def _copy_file() -> int:
            needs_copy, same_path, destination_exists = await async_destination_state(src, self)
            if not needs_copy:
                return 0
            if destination_exists and not force:
                require_force(force, self)
            self._path.parent.mkdir(parents=True, exist_ok=True)
            target_path = _atomic_temp_path(self._path) if atomic else self._path
            total = 0
            try:
                with target_path.open("wb") as dst_file:
                    if chunk_size and getattr(src, "_supports_range_read", False):
                        async for chunk in src.iter_bytes(chunk_size):
                            dst_file.write(chunk)
                            total += len(chunk)
                            await _emit_progress(len(chunk))
                    elif chunk_size:
                        raise NotImplementedError("chunk_size is not supported for this source")
                    else:
                        data = await src.read_bytes()
                        dst_file.write(data)
                        total = len(data)
                if atomic:
                    await to_thread.run_sync(
                        lambda: _commit_temp_sync(target_path, self._path, force=force)
                    )
                return total
            except BaseException:
                if atomic:
                    target_path.unlink(missing_ok=True)
                raise

        try:
            await run_transfer_async(
                src=src,
                dst=self,
                copy_fn=_copy_file,
                on_progress=None,
                on_done=None,
            )
        except BaseException as exc:
            error = exc
            raise
        finally:
            await _emit_done()
        return self

    async def rmdir(self, *, recursive: bool = False) -> None:
        if recursive:
            await to_thread.run_sync(lambda: shutil.rmtree(self._path))
        else:
            self._path.rmdir()
