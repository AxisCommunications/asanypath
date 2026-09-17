# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

import inspect
import os
import shutil
import uuid
from collections.abc import Awaitable, Callable
from hashlib import sha256
from pathlib import Path
from typing import Any

_CHECKSUM_PREFERENCE = ("sha256", "sha1", "md5")

# Default streaming chunk size for chunk_size=None ("auto").
_DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


def _validate_chunk_size(chunk_size: int | None) -> None:
    if chunk_size is not None and chunk_size < 0:
        raise ValueError("chunk_size must be None, 0, or a positive integer")


def _atomic_temp_path(dst_path: Path) -> Path:
    return dst_path.with_name(f".{dst_path.name}.tmp-{uuid.uuid4().hex}")


def _commit_temp_sync(tmp_path: Path, dst_path: Path, *, force: bool) -> None:
    try:
        if force:
            tmp_path.replace(dst_path)
        else:
            os.link(tmp_path, dst_path)
            tmp_path.unlink()
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def _copy_file_data_sync(src_path: Path, dst_path: Path, *, chunk_size: int | None) -> int:
    # chunk_size: None → auto (stream with default size); 0 → disabled (one-shot);
    # positive → stream with that size.
    if chunk_size == 0:
        shutil.copy2(src_path, dst_path)
        return src_path.stat().st_size
    if chunk_size is None:
        chunk_size = _DEFAULT_CHUNK_SIZE
    total = 0
    with src_path.open("rb") as src_file, dst_path.open("wb") as dst_file:
        while chunk := src_file.read(chunk_size):
            dst_file.write(chunk)
            total += len(chunk)
    shutil.copystat(src_path, dst_path)
    return total


def local_copy_file_sync(
    src_path: Path, dst_path: Path, *, chunk_size: int | None, atomic: bool, force: bool
) -> int:
    """Copy one local file, optionally staging via a temp file for atomic commit."""
    if atomic:
        tmp_path = _atomic_temp_path(dst_path)
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        copied = _copy_file_data_sync(src_path, tmp_path, chunk_size=chunk_size)
        _commit_temp_sync(tmp_path, dst_path, force=force)
        return copied
    return _copy_file_data_sync(src_path, dst_path, chunk_size=chunk_size)


def _normalized_checksums(checksums: dict[str, str]) -> dict[str, str]:
    return {key.lower().replace("-", ""): value.strip().lower() for key, value in checksums.items()}


def is_remote_destination(dst: Any) -> bool:
    protocol = getattr(dst, "protocol", None)
    if protocol is not None:
        return protocol != "file"
    value = str(dst)
    return "://" in value and not value.startswith("file:")


def has_async_transfer_api(dst: Any) -> bool:
    return inspect.iscoroutinefunction(
        getattr(dst, "exists", None)
    ) and inspect.iscoroutinefunction(getattr(dst, "write_bytes", None))


def has_write_api(dst: Any) -> bool:
    return callable(getattr(dst, "write_bytes", None))


def run_sync_maybe(value: Any) -> Any:
    if inspect.isawaitable(value):
        from asanypath.sync import _SyncRunner

        return _SyncRunner.get().run(value)
    return value


async def async_destination_state(src: Any, dst: Any) -> tuple[bool, bool, bool]:
    """Return ``(needs_copy, same_path, destination_exists)``."""
    same_path = str(src) == str(dst)
    if same_path:
        return (False, True, True)
    if not await dst.exists():
        return (True, False, False)
    if await dst.is_dir():
        raise IsADirectoryError(21, f"Is a directory: '{dst}'")

    try:
        src_checksums = _normalized_checksums(await src.checksums())
        dst_checksums = _normalized_checksums(await dst.checksums())
    except NotImplementedError:
        src_checksums = {}
        dst_checksums = {}
    for algorithm in _CHECKSUM_PREFERENCE:
        if algorithm in src_checksums and algorithm in dst_checksums:
            return (src_checksums[algorithm] != dst_checksums[algorithm], False, True)

    src_data = await src.read_bytes()
    dst_data = await dst.read_bytes()
    return (sha256(src_data).digest() != sha256(dst_data).digest(), False, True)


def sync_destination_state(src: Any, dst: Any) -> tuple[bool, bool, bool]:
    """Return ``(needs_copy, same_path, destination_exists)``."""
    same_path = str(src) == str(dst)
    if same_path:
        return (False, True, True)
    if not run_sync_maybe(dst.exists()):
        return (True, False, False)
    if run_sync_maybe(dst.is_dir()):
        raise IsADirectoryError(21, f"Is a directory: '{dst}'")

    src_checksums = _normalized_checksums(src.checksums())
    dst_checksums = _normalized_checksums(run_sync_maybe(dst.checksums()))
    for algorithm in _CHECKSUM_PREFERENCE:
        if algorithm in src_checksums and algorithm in dst_checksums:
            return (src_checksums[algorithm] != dst_checksums[algorithm], False, True)

    return (
        sha256(src.read_bytes()).digest() != sha256(run_sync_maybe(dst.read_bytes())).digest(),
        False,
        True,
    )


def require_force(force: bool, dst: Any) -> None:
    if not force:
        raise FileExistsError(17, f"File exists with different content: '{dst}'")


def run_transfer_sync(
    *,
    src: Any,
    dst: Any,
    copy_fn: Callable[[], int],
    remove_fn: Callable[[], None] | None = None,
    remove_src: bool = False,
    on_progress: Callable[[int, int, Any, Any], None] | None = None,
    on_done: Callable[[Any, Any, int, bool, BaseException | None], None] | None = None,
) -> Any:
    transferred = 0
    error: BaseException | None = None
    try:
        delta = copy_fn()
        if delta > 0 and on_progress is not None:
            transferred += delta
            on_progress(delta, transferred, src, dst)
        if remove_src and remove_fn is not None:
            remove_fn()
        return dst
    except BaseException as exc:  # noqa: BLE001  # pragma: no cover
        error = exc  # pragma: no cover
        raise  # pragma: no cover
    finally:
        if on_done is not None:
            on_done(src, dst, transferred, error is None, error)


async def run_transfer_async(
    *,
    src: Any,
    dst: Any,
    copy_fn: Callable[[], Awaitable[int]],
    remove_fn: Callable[[], Awaitable[None]] | None = None,
    remove_src: bool = False,
    on_progress: Callable[[int, int, Any, Any], None | Awaitable[None]] | None = None,
    on_done: Callable[[Any, Any, int, bool, BaseException | None], None | Awaitable[None]]
    | None = None,
) -> Any:
    transferred = 0
    error: BaseException | None = None
    try:
        delta = await copy_fn()
        if delta > 0 and on_progress is not None:
            transferred += delta
            maybe = on_progress(delta, transferred, src, dst)
            if inspect.isawaitable(maybe):
                await maybe
        if remove_src and remove_fn is not None:
            await remove_fn()
        return dst
    except BaseException as exc:  # noqa: BLE001  # pragma: no cover
        error = exc  # pragma: no cover
        raise  # pragma: no cover
    finally:
        if on_done is not None:
            maybe = on_done(src, dst, transferred, error is None, error)
            if inspect.isawaitable(maybe):  # pragma: no cover
                await maybe  # pragma: no cover
