# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

import asyncio
import inspect
import os
import shutil
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import aclosing, asynccontextmanager
from contextvars import ContextVar, Token
from functools import wraps
from hashlib import sha256
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from asanypath.exceptions import UnsupportedProtocolError

_CHECKSUM_PREFERENCE = ("sha256", "sha1", "md5")

# Default streaming chunk size for chunk_size=None ("auto").
_DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


class RangeNotSupportedError(OSError):
    def __init__(self, data: bytes) -> None:
        super().__init__("Server does not support range requests")
        self.data = data


class TransferBudget:
    """Share size-aware range concurrency within one transfer operation."""

    def __init__(
        self,
        *,
        floor: int = 8,
        ceiling: int = 32,
        min_range_size: int = _DEFAULT_CHUNK_SIZE,
        per_file_cap: int = 8,
    ) -> None:
        if floor < 1 or ceiling < floor or min_range_size < 1 or per_file_cap < 1:
            raise ValueError(
                "require 1 <= floor <= ceiling and positive range size and per-file cap"
            )
        self.floor = floor
        self.ceiling = ceiling
        self.min_range_size = min_range_size
        self.per_file_cap = per_file_cap
        self.active_files = 0
        self._slots = asyncio.Semaphore(ceiling)
        self._tokens: ContextVar[tuple[Token[TransferBudget | None], ...]] = ContextVar(
            "transfer_budget_tokens", default=()
        )

    def __enter__(self) -> TransferBudget:
        self._tokens.set((*self._tokens.get(), _transfer_budget.set(self)))
        return self

    def __exit__(self, *exc: Any) -> None:
        tokens = self._tokens.get()
        _transfer_budget.reset(tokens[-1])
        self._tokens.set(tokens[:-1])


_transfer_budget: ContextVar[TransferBudget | None] = ContextVar("transfer_budget", default=None)
_registered_transfer: ContextVar[TransferBudget | None] = ContextVar(
    "registered_transfer", default=None
)


@asynccontextmanager
async def transfer_scope() -> AsyncIterator[TransferBudget]:
    budget = _transfer_budget.get()
    if budget is None:
        loop = asyncio.get_running_loop()
        budget = getattr(loop, "_asanypath_transfer_budget", None)
        if budget is None:
            budget = TransferBudget()
            setattr(loop, "_asanypath_transfer_budget", budget)
    if _registered_transfer.get() is budget:
        yield budget
        return
    budget_token = _transfer_budget.set(budget)
    registered_token = _registered_transfer.set(budget)
    budget.active_files += 1
    try:
        await asyncio.sleep(0)
        yield budget
    finally:
        budget.active_files -= 1
        _registered_transfer.reset(registered_token)
        _transfer_budget.reset(budget_token)


async def iter_transfer_chunks(src: Any, chunk_size: int | None = None) -> AsyncIterator[bytes]:
    """Read supported sources in bounded, ordered, size-aware parallel ranges."""
    async with transfer_scope() as budget:
        if (
            chunk_size
            and chunk_size < budget.min_range_size
            and getattr(src, "_supports_range_read", False)
        ):
            async with aclosing(src.iter_bytes(chunk_size)) as chunks:
                while True:
                    async with budget._slots:
                        try:
                            chunk = await anext(chunks)
                        except StopAsyncIteration:
                            break
                    yield chunk
            return
        metadata = None
        size = None
        if getattr(src, "_supports_range_read", False):
            try:
                stat_fn = getattr(src, "_transfer_stat", src.stat)
                metadata = await stat_fn()
                if getattr(metadata, "supports_ranges", True):
                    size = getattr(metadata, "st_size", None)
            except (OSError, RuntimeError, NotImplementedError, AttributeError):
                pass
        spare_streams = min(budget.per_file_cap, budget.floor // max(1, budget.active_files))
        if (
            not isinstance(size, int)
            or size < 2 * budget.min_range_size
            or chunk_size == 0
            or (chunk_size is None and spare_streams < 2)
        ):
            async with budget._slots:
                data = await src.read_bytes()
            window = chunk_size or _DEFAULT_CHUNK_SIZE
            for offset in range(0, len(data), window):
                yield data[offset : offset + window]
            return

        window = max(chunk_size or budget.min_range_size, budget.min_range_size)

        async def read_range(start: int, end: int) -> bytes:
            async with budget._slots:
                if getattr(src, "_conditional_range_read", False) and any(
                    getattr(metadata, field, None)
                    for field in ("etag", "last_modified", "version_id")
                ):
                    data = await src._range_read(
                        start,
                        end,
                        etag=getattr(metadata, "etag", None),
                        last_modified=getattr(metadata, "last_modified", None),
                        version_id=getattr(metadata, "version_id", None),
                        total_size=size,
                    )
                else:
                    data = await src._range_read(start, end)
            if len(data) != end - start + 1:
                raise OSError(f"Invalid range response for bytes {start}-{end}")
            return data

        offset = 0
        if getattr(src, "_probe_range_support", False):
            end = min(size, window) - 1
            try:
                first = await read_range(0, end)
            except RangeNotSupportedError as error:
                if len(error.data) != size:
                    raise OSError("Source changed during ranged transfer") from error
                for offset in range(0, size, window):
                    yield error.data[offset : offset + window]
                return
            yield first
            offset = end + 1
        while offset < size:
            streams = max(
                1,
                min(
                    budget.per_file_cap, budget.floor // max(1, budget.active_files), size // window
                ),
            )
            tasks = []
            for _ in range(streams):
                if offset >= size:
                    break
                end = min(size, offset + window) - 1
                tasks.append(asyncio.create_task(read_range(offset, end)))
                offset = end + 1
            try:
                chunks = await asyncio.gather(*tasks)
                for chunk in chunks:
                    yield chunk
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        final_metadata = await stat_fn()
        for field in ("st_size", "st_mtime", "etag", "version_id", "checksums"):
            before = getattr(metadata, field, None)
            after = getattr(final_metadata, field, None)
            if before is not None and before != after:
                raise OSError("Source changed during ranged transfer")


TransferParams = ParamSpec("TransferParams")
TransferResult = TypeVar("TransferResult")


def _transfer_operation(
    fn: Callable[TransferParams, Awaitable[TransferResult]],
) -> Callable[TransferParams, Awaitable[TransferResult]]:
    @wraps(fn)
    async def wrapped(
        *args: TransferParams.args, **kwargs: TransferParams.kwargs
    ) -> TransferResult:
        async with transfer_scope():
            return await fn(*args, **kwargs)

    return wrapped


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
        return bool(protocol != "file")
    value = str(dst)
    return "://" in value and not value.startswith("file:")


def has_async_transfer_api(dst: Any) -> bool:
    try:
        exists = getattr(dst, "exists", None)
        write_bytes = getattr(dst, "write_bytes", None)
    except UnsupportedProtocolError:
        return False
    return inspect.iscoroutinefunction(exists) and inspect.iscoroutinefunction(write_bytes)


def has_write_api(dst: Any) -> bool:
    try:
        return callable(getattr(dst, "write_bytes", None))
    except UnsupportedProtocolError:
        return False


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


@_transfer_operation
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
