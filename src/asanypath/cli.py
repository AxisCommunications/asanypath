# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Rich-click powered CLI for AsAnyPath."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import math
import os
import shlex
import stat as statmod
import sys
import time
from collections.abc import AsyncIterator
from datetime import datetime
from email.utils import parsedate_to_datetime
from os import getenv
from pathlib import Path

import rich_click as click
from rich.console import Console, Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

from asanypath import AsAnyPath
from asanypath._version import __version__
from asanypath.asanypath import PROTOCOL_MAP

console = Console()

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}

DEFAULT_CONCURRENCY = 8

# Silence the harmless ``RuntimeError: Event loop is closed`` that pooled
# SSH/FTP subprocess transports raise from ``__del__`` after ``asyncio.run``
# has already torn down their loop. Only that exact message is suppressed.
_prev_unraisablehook = sys.unraisablehook


def _filtered_unraisablehook(unraisable):  # type: ignore[no-untyped-def]
    exc = unraisable.exc_value
    if isinstance(exc, RuntimeError) and "Event loop is closed" in str(exc):
        return
    _prev_unraisablehook(unraisable)


sys.unraisablehook = _filtered_unraisablehook


def _fmt_bytes(n: float) -> str:
    if n < 1024:
        return f"{n:.0f} B"
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        n /= 1024.0
        if n < 1024:
            return f"{n:.1f} {unit}"
    return f"{n:.1f} PiB"


def _fmt_bytes_ratio(transferred: float, total: float) -> tuple[str, str]:
    """Format two byte values with a shared unit. Returns (ratio_str, unit)."""
    if total < 1024:
        return f"{transferred:.0f} / {total:.0f}", "B"
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        transferred_scaled = transferred / (1024 ** (1 + ["KiB", "MiB", "GiB", "TiB"].index(unit)))
        total_scaled = total / (1024 ** (1 + ["KiB", "MiB", "GiB", "TiB"].index(unit)))
        if total_scaled < 1024:
            return f"{transferred_scaled:.1f} / {total_scaled:.1f}", unit
    transferred_scaled = transferred / (1024**5)
    total_scaled = total / (1024**5)
    return f"{transferred_scaled:.1f} / {total_scaled:.1f}", "PiB"


def _make_overall_progress(*, show_transfer: bool = False) -> Progress:
    cols = [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
    ]
    if show_transfer:
        cols.extend(
            [
                TextColumn("[cyan]{task.fields[bytes_str]}[/cyan]"),
                TextColumn("[green]{task.fields[speed_str]}[/green]"),
            ]
        )
    cols.extend(
        [
            TextColumn("[dim]elapsed[/dim]"),
            TimeElapsedColumn(),
            TextColumn("[dim]eta[/dim]"),
            TimeRemainingColumn(),
        ]
    )
    return Progress(*cols, console=console, transient=False)


def _make_file_progress(*, show_transfer: bool = False) -> Progress:
    cols = [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ]
    if show_transfer:
        cols.extend(
            [
                TextColumn("[cyan]{task.fields[bytes_str]}[/cyan]"),
                TextColumn("[green]{task.fields[speed_str]}[/green]"),
            ]
        )
    cols.extend(
        [
            TextColumn("[dim]elapsed[/dim]"),
            TimeElapsedColumn(),
        ]
    )
    return Progress(*cols, console=console, transient=False)


def _fmt_elapsed(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


def _task_label(path_like: str, *, max_len: int = 40) -> str:
    """Compact task label: full relative path with leading ellipsis when long."""
    if len(path_like) <= max_len:
        return path_like
    return f"…{path_like[-(max_len - 1) :]}"


class _TransferWork:
    """Wrapper holding both transfer callable and source object for metadata."""

    def __init__(self, fn, src_obj=None):
        self.fn = fn
        self.src_obj = src_obj

    async def __call__(self, **kwargs):
        return await self.fn(**kwargs)


async def _run_concurrent(
    items: list[tuple[str, object]],
    *,
    description: str,
    verbosity: int,
    concurrency: int,
    show_transfer: bool = False,
) -> tuple[int, list[tuple[str, BaseException]], float]:
    """Run a pre-collected list of work items concurrently."""

    async def _iter_items() -> AsyncIterator[tuple[str, object]]:
        for item in items:
            yield item

    return await _run_concurrent_stream(
        _iter_items(),
        description=description,
        verbosity=verbosity,
        concurrency=concurrency,
        show_transfer=show_transfer,
        total=len(items),
    )


async def _run_concurrent_stream(
    items: AsyncIterator[tuple[str, object]],
    *,
    description: str,
    verbosity: int,
    concurrency: int,
    show_transfer: bool = False,
    total: int | None = None,
) -> tuple[int, list[tuple[str, BaseException]], float]:
    """Run items from an async stream with up to ``concurrency`` workers."""
    style = (
        "none"
        if verbosity <= 0 or (total is not None and total <= 1)
        else "bar"
        if verbosity == 1
        else "tasks"
    )
    failures: list[tuple[str, BaseException]] = []
    succeeded = 0
    worker_count = max(1, concurrency)

    overall_progress = (
        _make_overall_progress(show_transfer=show_transfer) if style != "none" else None
    )
    file_progress = _make_file_progress(show_transfer=show_transfer) if style == "tasks" else None
    overall = None
    discovered = 0
    total_bytes = 0
    total_started = time.monotonic()
    file_state: dict[int, dict[str, float]] = {}
    if overall_progress is not None:
        overall_progress.start()
        overall = overall_progress.add_task(
            f"{description}… (j={concurrency})",
            total=total if total is not None else 0,
            bytes_str="0 B",
            speed_str="0 B/s",
        )
    if file_progress is not None:
        file_progress.start()

    queue: asyncio.Queue[tuple[str, object] | None] = asyncio.Queue(maxsize=worker_count * 2)

    async def _run_one(name: str, work):
        nonlocal total_bytes
        nonlocal succeeded
        # Extract source object and size if available
        src_obj = None
        src_size = None
        if isinstance(work, _TransferWork):
            src_obj = work.src_obj
        if src_obj is not None:
            src_size = await _get_size(src_obj)
        sub = (
            file_progress.add_task(
                name,
                bytes_str="--",
                speed_str="--",
            )
            if file_progress is not None
            else None
        )
        sub_started = time.monotonic()
        sub_transferred = 0
        if sub is not None:
            file_state[sub] = {
                "started": sub_started,
                "transferred": 0.0,
                "total": float(src_size) if src_size else 0.0,
            }

        async def _on_progress(delta: int, transferred: int, src, dst):
            nonlocal total_bytes
            nonlocal sub_transferred
            if delta > 0:
                total_bytes += delta
            sub_transferred = max(sub_transferred, transferred)
            if overall_progress is None:
                return
            now = time.monotonic()
            total_elapsed = max(now - total_started, 1e-6)
            total_speed = total_bytes / total_elapsed
            if overall is not None:
                overall_progress.update(
                    overall,
                    bytes_str=_fmt_bytes(float(total_bytes)),
                    speed_str=f"{_fmt_bytes(total_speed)}/s",
                )
            if sub is not None and file_progress is not None:
                src_size = file_state[sub].get("total", 0.0)
                file_state[sub]["transferred"] = float(sub_transferred)
                # Format as "transferred / total" with shared unit
                if src_size > 0:
                    ratio_str, unit = _fmt_bytes_ratio(float(transferred), src_size)
                    bytes_str = f"{ratio_str} {unit}"
                else:
                    bytes_str = _fmt_bytes(float(transferred))
                update_kwargs = {"bytes_str": bytes_str}
                if delta > 0:
                    sub_elapsed = max(now - sub_started, 1e-6)
                    sub_speed = transferred / sub_elapsed
                    update_kwargs["speed_str"] = f"{_fmt_bytes(sub_speed)}/s"
                file_progress.update(sub, **update_kwargs)

        try:
            if callable(work):
                kwargs = {}
                if show_transfer and overall_progress is not None:
                    kwargs["on_progress"] = _on_progress
                try:
                    res = work(**kwargs)
                except TypeError as exc:
                    # Backward compatibility for callables/mocks that don't
                    # accept the optional on_progress callback.
                    if kwargs and "on_progress" in str(exc):
                        res = work()
                    else:
                        raise
                if inspect.isawaitable(res):
                    await res
            else:
                await work
            succeeded += 1
        except BaseException as exc:  # noqa: BLE001
            failures.append((name, exc))
        finally:
            if sub is not None and file_progress is not None:
                file_state.pop(sub, None)
                file_progress.remove_task(sub)
            if overall_progress is not None and overall is not None:
                overall_progress.update(overall, advance=1)

    refresh_stop = asyncio.Event()

    async def _refresh_metrics() -> None:
        if not show_transfer or overall_progress is None:
            return
        while not refresh_stop.is_set():
            await asyncio.sleep(0.1)
            now = time.monotonic()
            total_elapsed = max(now - total_started, 1e-6)
            total_speed = total_bytes / total_elapsed
            if overall is not None:
                overall_progress.update(
                    overall,
                    bytes_str=_fmt_bytes(float(total_bytes)),
                    speed_str=f"{_fmt_bytes(total_speed)}/s",
                )
            if file_progress is None:
                continue
            for task_id, state in list(file_state.items()):
                transferred = max(0.0, state["transferred"])
                total = max(0.0, state.get("total", 0.0))
                if transferred <= 0:
                    continue
                started = state["started"]
                elapsed = max(now - started, 1e-6)
                speed = transferred / elapsed
                # Format as "transferred / total" with shared unit
                if total > 0:
                    ratio_str, unit = _fmt_bytes_ratio(transferred, total)
                    bytes_str = f"{ratio_str} {unit}"
                else:
                    bytes_str = _fmt_bytes(transferred)
                file_progress.update(
                    task_id,
                    bytes_str=bytes_str,
                    speed_str=f"{_fmt_bytes(speed)}/s",
                )

    async def _producer() -> None:
        nonlocal discovered
        try:
            async for item in items:
                discovered += 1
                if overall_progress is not None and overall is not None and total is None:
                    overall_progress.update(overall, total=discovered)
                await queue.put(item)
                # Keep workers responsive even when the producer can enumerate quickly.
                if discovered % 64 == 0:
                    await asyncio.sleep(0)
        finally:
            for _ in range(worker_count):
                await queue.put(None)

    async def _worker() -> None:
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                name, work = item
                await _run_one(name, work)
            finally:
                queue.task_done()

    t0 = time.monotonic()
    producer_task = asyncio.create_task(_producer())
    worker_tasks = [asyncio.create_task(_worker()) for _ in range(worker_count)]
    refresh_task = asyncio.create_task(_refresh_metrics())
    live_ctx = contextlib.nullcontext()
    if overall_progress is not None:
        live_renderable = (
            Group(overall_progress, file_progress)
            if file_progress is not None
            else overall_progress
        )
        live_ctx = Live(
            live_renderable,
            console=console,
            transient=True,
            refresh_per_second=10,
        )

    try:
        with live_ctx:
            await queue.join()
            await asyncio.gather(*worker_tasks)
            await producer_task
    finally:
        refresh_stop.set()
        refresh_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await refresh_task
        elapsed = (
            overall_progress.tasks[0].elapsed or 0.0
            if overall_progress is not None and overall_progress.tasks
            else time.monotonic() - t0
        )
        if file_progress is not None:
            file_progress.stop()
        if overall_progress is not None:
            overall_progress.stop()

    for name, exc in failures:
        console.print(f"[red]error[/red] {name}: {exc}")
    return succeeded, failures, elapsed


LS_COLOR_THEMES: dict[str, dict[str, str]] = {
    "default": {
        "root": "bold cyan",
        "name": "bright_white",
        "size": "magenta",
        "created": "yellow",
        "modified": "green",
        "guide": "bright_black",
    },
    "ocean": {
        "root": "bold bright_cyan",
        "name": "bright_white",
        "size": "bright_blue",
        "created": "cyan",
        "modified": "bright_green",
        "guide": "blue",
    },
    "sunset": {
        "root": "bold bright_yellow",
        "name": "bright_white",
        "size": "bright_magenta",
        "created": "bright_red",
        "modified": "bright_yellow",
        "guide": "bright_black",
    },
}

LS_DEPTH_GRADIENTS: dict[str, tuple[str, ...]] = {
    "default": ("bright_white", "bright_cyan", "cyan", "blue", "magenta", "bright_magenta"),
    "ocean": ("bright_white", "bright_cyan", "cyan", "bright_blue", "blue", "bright_green"),
    "sunset": (
        "bright_white",
        "bright_yellow",
        "yellow",
        "bright_red",
        "magenta",
        "bright_magenta",
    ),
}


def mask_token(token: str | None, max_visible: int = 4) -> str:
    """Mask token for safe display, showing only first N chars."""
    if not token:
        return "(not set)"
    if len(token) <= max_visible:
        return token
    return token[:max_visible] + "*" * (len(token) - max_visible)


def _maybe_rewrite_scp_style(value: str, check_config: bool = True) -> str:
    """Rewrite scp-style ``[user@]host:path`` to ``ssh://[user@]host/path``.

    When ``check_config=True`` (default), only rewrites if ``host`` matches
    an entry in the user's ``~/.ssh/config`` (avoids hijacking local paths
    that happen to contain a colon). When ``check_config=False`` (completion
    mode), rewrites any scp-style prefix optimistically.
    """
    if "://" in value or ":" not in value:
        return value
    head, _, tail = value.partition(":")
    user = None
    host = head
    if "@" in head:
        user, _, host = head.partition("@")
    # Reject Windows drive letters (defensive) and obvious local paths.
    if not host or len(host) == 1 or "/" in host or host.startswith("."):
        return value
    if check_config:
        try:
            from asanypath.ssh import _discover_ssh_config, _resolve_alias
        except ImportError:
            return value
        if not _discover_ssh_config():
            return value
        resolved_host, _, _ = _resolve_alias(host, None, None)
        if resolved_host == host:
            # No Host block matched — leave alone (don't probe DNS).
            return value
    prefix = f"{user}@" if user else ""
    # scp semantics: a leading ``/`` means absolute, anything else (including
    # empty) is relative to the remote home. ``/~`` is translated by
    # SSHPath._item_path to SFTP cwd (= home after login).
    if tail.startswith("/"):
        return f"ssh://{prefix}{host}{tail}"
    return f"ssh://{prefix}{host}/~/{tail}"


def _resolve_path_arg(ctx: click.Context, param: click.Parameter, value: str | None) -> str:
    """Click callback: return value, falling back to a single line read from stdin."""
    if value is not None:
        return _maybe_rewrite_scp_style(value)
    if sys.stdin.isatty():
        raise click.UsageError("Missing argument 'PATH' (or pipe a path via stdin).")
    line = sys.stdin.readline().strip()
    if not line:
        raise click.UsageError("Received empty path from stdin.")
    return _maybe_rewrite_scp_style(line)


def _rewrite_path_arg(ctx: click.Context, param: click.Parameter, value: str | None) -> str | None:
    """Click callback: apply scp-style rewrite, no stdin fallback."""
    return _maybe_rewrite_scp_style(value) if value is not None else value


def _rewrite_paths_arg(
    ctx: click.Context, param: click.Parameter, value: tuple[str, ...]
) -> tuple[str, ...]:
    """Click callback: apply scp-style rewrite to each path, no stdin fallback."""
    return tuple(_maybe_rewrite_scp_style(v) for v in value)


def _resolve_paths_arg(
    ctx: click.Context, param: click.Parameter, value: tuple[str, ...]
) -> tuple[str, ...]:
    """Return paths, falling back to reading one path per line from stdin when empty."""
    if value:
        return tuple(_maybe_rewrite_scp_style(v) for v in value)
    if sys.stdin.isatty():
        raise click.UsageError("Missing argument 'PATHS' (or pipe paths via stdin).")
    paths = tuple(_maybe_rewrite_scp_style(line.strip()) for line in sys.stdin if line.strip())
    if not paths:
        raise click.UsageError("Received no paths from stdin.")
    return paths


def _fmt_mode(mode: int) -> str:
    return f"({mode & 0o7777:04o}/{statmod.filemode(mode)})"


def _fmt_ids(uid: int | None, gid: int | None) -> tuple[str, str]:
    uid_name = "?"
    gid_name = "?"
    if uid is not None:
        try:
            import pwd

            uid_name = pwd.getpwuid(uid).pw_name
        except Exception:
            uid_name = "?"
    if gid is not None:
        try:
            import grp

            gid_name = grp.getgrgid(gid).gr_name
        except Exception:
            gid_name = "?"
    uid_str = f"({uid:5d}/ {uid_name})" if uid is not None else "(    ?/ ?)"
    gid_str = f"({gid:5d}/ {gid_name})" if gid is not None else "(    ?/ ?)"
    return uid_str, gid_str


def _fmt_time(ts: float | None, ns: int | None) -> str:
    if ts is None:
        return "-"
    dt = datetime.fromtimestamp(ts).astimezone()
    frac_ns = int(ns % 1_000_000_000) if ns is not None else int((ts % 1) * 1_000_000_000)
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{frac_ns:09d} {dt.strftime('%z')}"


def _fmt_http_date(value: str, fmt: str = "%Y-%m-%d %H:%M:%S.%f %z") -> str:
    try:
        return parsedate_to_datetime(value).astimezone().strftime(fmt)
    except Exception:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone().strftime(fmt)
        except Exception:
            return value


# Shell completion infrastructure
_COMPLETION_CACHE: dict[str, tuple[float, list[str]]] = {}
_COMPLETION_CACHE_TTL = 30.0
_COMPLETION_TIMEOUT = 5.0


def _completion_debug(msg: str) -> None:
    """Emit completion diagnostics when explicitly enabled."""
    if os.environ.get("ASANYPATH_COMPLETION_DEBUG"):
        print(f"[asanypath completion] {msg}", file=sys.stderr)


def _ensure_endswith_slash(path: str) -> str:
    """Ensure a path ends with a slash, for completion purposes."""
    return path.rstrip("/") + "/"


async def _complete_path_prefix_async(prefix: str) -> list[str]:
    """Async helper to complete cloud paths. Returns up to 50 candidates."""
    if not prefix or "://" not in prefix:
        return []

    try:
        p = AsAnyPath(prefix)
        # Get parent directory for enumeration
        parent = p.parent if not prefix.endswith("/") else p

        async def _enumerate() -> list[str]:
            candidates: list[str] = []
            # Container/bucket/repo fast path: when at a service root the
            # backend lists top-level names directly. These are always
            # "directories", so mark them and skip per-entry is_dir probes.
            list_containers = getattr(parent, "_list_containers", None)
            if list_containers is not None:
                names = await list_containers()
                if names is not None:
                    return sorted(map(_ensure_endswith_slash, names))

            # SSH fast path: use OpenSSH subprocess so that a pre-existing
            # ControlMaster socket makes this near-instant.
            list_dir_remote = getattr(parent, "_list_dir_remote", None)
            if list_dir_remote is not None:
                entries = await list_dir_remote()
                if entries is not None:
                    for name, is_dir in entries:
                        child = parent / name
                        candidate = str(child)
                        if is_dir:
                            candidate = _ensure_endswith_slash(candidate)
                        candidates.append(candidate)
                        if len(candidates) >= 50:
                            break
                    return sorted(candidates)

            async for child in parent.iterdir():
                candidate = str(child)
                try:
                    if await child.is_dir():
                        candidate = _ensure_endswith_slash(candidate)
                except Exception:
                    # Best effort: keep candidate even if dir detection fails.
                    pass
                candidates.append(candidate)
                if len(candidates) >= 50:
                    break
            return sorted(candidates)

        # asyncio.wait_for keeps the timeout compatible with Python 3.10
        # (asyncio.timeout is 3.11+).
        return await asyncio.wait_for(_enumerate(), _COMPLETION_TIMEOUT)
    except FileNotFoundError:
        _completion_debug(f"not found for prefix: {prefix}")
        return []
    except NotImplementedError:
        _completion_debug(f"not implemented for prefix: {prefix}")
        return []
    except asyncio.TimeoutError:
        _completion_debug(f"timeout after {_COMPLETION_TIMEOUT}s for prefix: {prefix}")
        return []
    except Exception as exc:
        _completion_debug(f"error for prefix {prefix!r}: {exc}")
        return []


def _complete_cloud_path(prefix: str) -> list[str]:
    """Complete cloud paths with caching (shallow, max 50 candidates)."""
    if not prefix or "://" not in prefix:
        return []

    # Check cache
    cache_key = prefix
    if cache_key in _COMPLETION_CACHE:
        cached_time, cached_results = _COMPLETION_CACHE[cache_key]
        if time.time() - cached_time < _COMPLETION_CACHE_TTL:
            return cached_results

    try:
        results = asyncio.run(_complete_path_prefix_async(prefix))
        _COMPLETION_CACHE[cache_key] = (time.time(), results)
        return results
    except Exception as exc:
        _completion_debug(f"runner failed for prefix {prefix!r}: {exc}")
        return []


def _complete_local_path(prefix: str, limit: int = 50) -> list[str]:
    """Complete local paths for plain tokens (no URI scheme)."""
    if "://" in prefix:
        return []

    try:
        expanded = os.path.expanduser(prefix)
        sep = os.sep
        altsep = os.altsep

        has_sep = sep in expanded or (altsep is not None and altsep in expanded)
        if has_sep:
            parent_part, name_part = os.path.split(expanded)
            parent = Path(parent_part) if parent_part else Path(".")
        else:
            parent = Path(".")
            name_part = expanded

        out: list[str] = []
        for child in parent.iterdir():
            if not child.name.startswith(name_part):
                continue
            suggestion = str(child)
            if child.is_dir() and not suggestion.endswith("/"):
                suggestion += "/"
            out.append(suggestion)
            if len(out) >= limit:
                break
        return sorted(out)
    except Exception:
        return []


def _normalize_completion_prefix(ctx: click.Context, incomplete: str) -> str:
    """Normalize shell-split tokens (notably bash ':' word-break behavior)."""
    if "://" in incomplete:
        return incomplete

    comp_words = os.environ.get("COMP_WORDS")
    comp_cword = os.environ.get("COMP_CWORD")
    if not comp_words or comp_cword is None:
        return incomplete

    try:
        words = comp_words.split()
        cword = int(comp_cword)
    except Exception:
        return incomplete

    if cword <= 0 or cword >= len(words):
        return incomplete

    current = words[cword]
    previous = words[cword - 1]

    if "://" in current:
        return current

    # Bash commonly splits "s3://bucket/prefix" into ["s3", "//bucket/prefix"].
    if current.startswith("//") and previous in PROTOCOL_MAP:
        return f"{previous}:{current}"

    if previous.endswith("://"):
        return previous + current

    if previous.endswith(":"):
        scheme = previous[:-1]
        if scheme in PROTOCOL_MAP:
            return previous + current

    return incomplete


def _path_complete(ctx: click.Context, param: click.Parameter, incomplete: str) -> list[str]:
    """Click shell completion callback for path arguments."""
    if not incomplete:
        return []

    normalized = _normalize_completion_prefix(ctx, incomplete)
    scp_original = None  # Track original scp-style prefix for result conversion.
    if "://" not in normalized:
        # Support scp-style shorthand (for example "cvat:") during completion
        # without requiring SSH config validation (use check_config=False).
        rewritten = _maybe_rewrite_scp_style(normalized, check_config=False)
        if "://" in rewritten:
            scp_original = normalized
            normalized = rewritten

    if "://" in normalized:
        try:
            # Cloud completion may require network operations
            # Catch any errors to avoid breaking shell completion
            results = _complete_cloud_path(normalized)
        except Exception:
            # On timeout or connection error, return empty
            results = []

        def _matches(candidate: str, prefix: str) -> bool:
            if candidate.startswith(prefix):
                return True
            if "://" not in candidate or "://" not in prefix:
                return False

            c_scheme, c_right = candidate.split("://", 1)
            p_scheme, p_right = prefix.split("://", 1)
            if c_scheme != p_scheme:
                return False

            if c_right.startswith(p_right):
                return True

            # Some backends canonicalize to a fuller path prefix than the
            # user typed (for example art://repo/... -> art://host/.../repo/...).
            return f"/{p_right}" in f"/{c_right}"

        def _render(candidate: str, prefix: str) -> str:
            if "://" not in candidate or "://" not in prefix:
                return candidate
            c_scheme, c_right = candidate.split("://", 1)
            p_scheme, p_right = prefix.split("://", 1)
            if c_scheme != p_scheme:
                return candidate
            if p_scheme == "ssh" and scp_original:
                # Convert ssh://host/~/path back to scp-style host:path
                if "@" in c_right:
                    user, _, rest = c_right.partition("@")
                    host, _, path = rest.partition("/")
                else:
                    host, _, path = c_right.partition("/")
                # Extract original host from scp_original (e.g., "cvat:.confi" -> "cvat")
                if "@" in scp_original:
                    orig_user, _, orig_rest = scp_original.partition("@")
                    orig_host = orig_rest.partition(":")[0]
                    orig_user_prefix = f"{orig_user}@"
                else:
                    orig_host = scp_original.partition(":")[0]
                    orig_user_prefix = ""
                if host == orig_host:
                    # Match: convert back to scp format
                    if path.startswith("~/"):
                        # Relative path from home
                        return f"{orig_user_prefix}{orig_host}:{path[2:]}"
                    elif path == "~/":
                        # Just home
                        return f"{orig_user_prefix}{orig_host}:"
                    else:
                        # Absolute path
                        return f"{orig_user_prefix}{orig_host}:{path}"
            if p_scheme != "art":
                return candidate
            marker = f"/{p_right}"
            idx = c_right.find(marker)
            if idx < 0:
                return candidate
            suffix = c_right[idx + len(marker) :]
            return f"{p_scheme}://{p_right}{suffix}"

        filtered = [_render(r, normalized) for r in results if _matches(r, normalized)]
        return list(dict.fromkeys(filtered))[:50]

    # Preserve local path completion and also suggest cloud URI schemes.
    local = _complete_local_path(incomplete)
    schemes = [f"{scheme}://" for scheme in sorted(PROTOCOL_MAP) if scheme.startswith(incomplete)]
    merged = list(dict.fromkeys(local + schemes))
    return merged[:50]


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__, message="%(prog)s, version %(version)s")
def cli():
    """AsAnyPath CLI - unified interface for local and cloud storage.

    Work with files across many cloud providers and the local
    filesystem using a consistent async-first interface.

    Shell Completions:
    Path completions (local and cloud) for ls, cp, mv, rm, stat, cat, exists,
    touch, checksums, mkdir, presign, and sync.
    Activate in bash (add to ~/.bashrc to persist):  eval "$(asanypath completion bash)"
    """
    pass


# --- Root Inspect-like Commands ---


@cli.command(short_help="List supported protocols", context_settings=CONTEXT_SETTINGS)
def protocols():
    """Show all supported URI protocols and their implementations."""
    protocol_descriptions = {
        "art": "JFrog Artifactory repositories",
        "az": "Azure Blob Storage containers",
        "file": "Local filesystem paths",
        "ftp": "FTP servers (creds via ~/.netrc)",
        "ftps": "Explicit FTPS (AUTH TLS); creds via ~/.netrc",
        "gs": "Google Cloud Storage buckets",
        "http": "Unauthenticated HTTP resources",
        "https": "HTTPS resources",
        "s3": "Amazon S3 buckets",
        "ssh": "SSH/SFTP servers; aliases honored from ~/.ssh/config",
    }

    table = Table(
        title="Supported Protocols",
        box=None,
        show_edge=False,
        pad_edge=False,
        expand=False,
        padding=(0, 2),
    )
    table.add_column("PROTOCOL", style="cyan", no_wrap=True)
    table.add_column("EXAMPLE", style="bright_white", no_wrap=True)
    table.add_column("DESCRIPTION", style="green")

    for scheme, klass in sorted(PROTOCOL_MAP.items()):
        desc = protocol_descriptions.get(scheme, "Backend implementation")
        table.add_row(scheme, f"{scheme}://{klass.__name__}/some_url", desc)

    with console.capture() as capture:
        console.print(table, width=120)
    click.echo(capture.get())


def _default_completion_exe() -> str:
    """Best-effort executable path used by completion backend calls."""
    argv0 = Path(sys.argv[0]).expanduser()
    if argv0.exists():
        try:
            return str(argv0.resolve())
        except Exception:
            return str(argv0)

    from shutil import which

    found = which("asanypath")
    if found:
        return found
    return "asanypath"


@cli.command(
    "completion", short_help="Print shell completion script", context_settings=CONTEXT_SETTINGS
)
@click.argument("shell", required=False, default="bash", type=click.Choice(["bash"]))
@click.option(
    "--exe",
    "exe_path",
    default=None,
    help="Executable path used by completion backend calls (defaults to "
    "current asanypath executable).",
)
def completion(shell: str, exe_path: str | None) -> None:
    """Emit a shell completion script for asanypath (bash).

    Activate it in your current shell:

        eval "$(asanypath completion bash)"

    Persist it by adding that same line to your ~/.bashrc.

    Important: use *this* command — not Click's generic
    'eval "$(_ASANYPATH_COMPLETE=bash_source asanypath)"'. The generic script
    leaves ':' in COMP_WORDBREAKS, so bash splits URI schemes and cloud paths
    like 's3://bucket/…' or 'art://repo/…' never complete. This script drops
    ':' from the word-break set so the whole URI stays a single token.

    Tip: 'export ASANYPATH_COMPLETION_DEBUG=1' prints per-TAB diagnostics.
    """
    del shell  # currently only bash is supported
    exe = exe_path or _default_completion_exe()
    quoted_exe = shlex.quote(exe)

    script = f"""# Keep URI schemes (e.g. s3://) as a single completion token.
COMP_WORDBREAKS=${{COMP_WORDBREAKS//:}}

_asanypath_completion() {{
    local IFS=$'\\n'
    local response
    response=$(env COMP_WORDS=\"${{COMP_WORDS[*]}}\" COMP_CWORD=$COMP_CWORD \\
        _ASANYPATH_COMPLETE=bash_complete {quoted_exe})

    # Default to regular spacing; switch to nospace only for dir-only results.
    compopt +o nospace 2>/dev/null || true

    COMPREPLY=()
    local has_dir=0
    local has_file=0
    local type value
    for completion in $response; do
        IFS=',' read -r type value <<< \"$completion\"
        if [[ $type == 'dir' ]]; then
            COMPREPLY=()
            compopt -o dirnames
        elif [[ $type == 'file' ]]; then
            COMPREPLY=()
            compopt -o default
        elif [[ $type == 'plain' ]]; then
            COMPREPLY+=(\"$value\")
            if [[ $value == */ ]]; then
                has_dir=1
            else
                has_file=1
            fi
        fi
    done

    if [[ $has_dir -eq 1 && $has_file -eq 0 ]]; then
        compopt -o nospace
    else
        compopt +o nospace 2>/dev/null || true
    fi

    return 0
}}

complete -o nosort -F _asanypath_completion asanypath
"""
    click.echo(script)


@cli.command(short_help="Check if path exists", context_settings=CONTEXT_SETTINGS)
@click.argument(
    "path", type=str, required=False, callback=_resolve_path_arg, shell_complete=_path_complete
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
def exists(path: str, token: str | None):
    """Check if FILE exists at given path.

    PATH can also be supplied via stdin: echo s3://bucket/key | asanypath exists

    Examples:
        asanypath exists s3://bucket/key
        asanypath exists art://art.example.com/repo/file --token mytoken
    """

    async def run():
        try:
            kwargs = {}
            if token:
                kwargs["token"] = token
            p = AsAnyPath(path, **kwargs)
            result = await p.exists()
            if result:
                console.print(f"[green]✓[/green] {path} exists")
                return 0
            else:
                console.print(f"[yellow]✗[/yellow] {path} does not exist")
                return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


@cli.command(short_help="Get path statistics", context_settings=CONTEXT_SETTINGS)
@click.argument(
    "path", type=str, required=False, callback=_resolve_path_arg, shell_complete=_path_complete
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
def stat(path: str, token: str | None):
    """Display file statistics for PATH.

    PATH can also be supplied via stdin: echo s3://bucket/key | asanypath stat

    Example:
        asanypath stat s3://bucket/key
    """

    async def run():
        try:
            kwargs = {}
            if token:
                kwargs["token"] = token
            p = AsAnyPath(path, **kwargs)

            try:
                st = await p.stat()
                mode = getattr(st, "st_mode", None)
                ftype = (
                    "directory" if (mode is not None and statmod.S_ISDIR(mode)) else "regular file"
                )
                size = getattr(st, "st_size", "?")
                blocks = getattr(st, "st_blocks", "?")
                blksize = getattr(st, "st_blksize", "?")
                dev = getattr(st, "st_dev", None)
                inode = getattr(st, "st_ino", "?")
                nlink = getattr(st, "st_nlink", "?")
                uid = getattr(st, "st_uid", None)
                gid = getattr(st, "st_gid", None)
                uid_txt, gid_txt = _fmt_ids(uid, gid)
                dev_txt = "?"
                if isinstance(dev, int):
                    try:
                        dev_txt = f"{os.major(dev)},{os.minor(dev)}"
                    except Exception:
                        dev_txt = str(dev)
                mode_txt = _fmt_mode(mode) if isinstance(mode, int) else "(?/?)"
                atime = _fmt_time(getattr(st, "st_atime", None), getattr(st, "st_atime_ns", None))
                mtime = _fmt_time(getattr(st, "st_mtime", None), getattr(st, "st_mtime_ns", None))
                ctime = _fmt_time(getattr(st, "st_ctime", None), getattr(st, "st_ctime_ns", None))
                birth_ts = getattr(st, "st_birthtime", None)
                birth_ns = getattr(st, "st_birthtime_ns", None)
                birth = _fmt_time(birth_ts, birth_ns) if birth_ts is not None else "-"
            except NotImplementedError:
                # Fallback for HTTPPath which lacks stat(): scrape metadata from HEAD.
                try:
                    resp = await p.request("HEAD", url=str(p))
                    head_headers = resp.headers
                except Exception:
                    raise NotImplementedError(
                        "stat() not implemented for general HTTPPaths"
                    ) from None
                headers = {k.lower(): v for k, v in head_headers.items()}
                size = headers.get("content-length", "?")
                if isinstance(size, str) and size.isdigit():
                    size_int = int(size)
                    size = size_int
                    blocks = math.ceil(size_int / 512) if size_int > 0 else 0
                else:
                    blocks = "?"
                blksize = "?"
                ftype = "regular file"
                dev_txt = "?"
                inode = headers.get("etag", headers.get("x-amz-version-id", "?"))
                nlink = "?"
                uid_txt, gid_txt = "(    ?/ ?)", "(    ?/ ?)"
                mode_txt = "(?/?)"
                access_raw = headers.get("date")
                modify_raw = headers.get("last-modified")
                atime = _fmt_http_date(access_raw) if access_raw else "-"
                mtime = _fmt_http_date(modify_raw) if modify_raw else "-"
                ctime = mtime if mtime != "-" else atime
                birth = "-"

            console.print(f"  File: {path}")
            console.print(f"  Size: {size:<14} Blocks: {blocks:<10} IO Block: {blksize:<6} {ftype}")
            console.print(f"Device: {dev_txt:<8} Inode: {inode:<11} Links: {nlink}")
            console.print(f"Access: {mode_txt}  Uid: {uid_txt}   Gid: {gid_txt}")
            console.print(f"Access: {atime}")
            console.print(f"Modify: {mtime}")
            console.print(f"Change: {ctime}")
            console.print(f" Birth: {birth}")
            return 0
        except NotImplementedError as e:
            console.print(f"[yellow]Not implemented:[/yellow] {e}")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


# --- Read Commands ---


@cli.command("cat", short_help="Print file contents", context_settings=CONTEXT_SETTINGS)
@click.argument(
    "path", type=str, required=False, callback=_resolve_path_arg, shell_complete=_path_complete
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
@click.option("--binary", is_flag=True, help="Output as hex dump instead of text")
def cat(path: str, token: str | None, binary: bool):
    """Read and print file contents.

    PATH can also be supplied via stdin: echo s3://bucket/file | asanypath cat

    Examples:
        asanypath cat s3://bucket/file.txt
        asanypath cat art://art.example.com/repo/file --token mytoken
    """

    async def run():
        try:
            kwargs = {}
            if token:
                kwargs["token"] = token
            p = AsAnyPath(path, **kwargs)

            if binary:
                data = await p.read_bytes()
                console.print(data.hex())
            else:
                text = await p.read_text()
                console.print(text, end="")
            return 0
        except NotImplementedError:
            console.print("[yellow]Operation not supported for this path type[/yellow]")
            return 1
        except FileNotFoundError:
            console.print(f"[red]Error:[/red] {path} not found")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


@cli.command("ls", short_help="List directory contents", context_settings=CONTEXT_SETTINGS)
@click.argument(
    "path", default=".", required=False, callback=_rewrite_path_arg, shell_complete=_path_complete
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
@click.option("--recursive", "-r", is_flag=True, help="Recursively list subtree as a tree")
@click.option(
    "--depth",
    "-d",
    default=None,
    type=click.IntRange(min=1),
    help="Max recursion depth (implies -r)",
)
@click.option(
    "--long", "-l", "long_format", is_flag=True, help="Show size/created/modified metadata"
)
@click.option(
    "--simple", "-1", "simple", is_flag=True, help="Output one path per line (for piping)"
)
@click.option(
    "--color",
    default="default",
    help="Set tree color theme name; use 'off' to disable colors.",
)
@click.option(
    "--listing",
    default=None,
    help="HTTP directory-listing mode: 'python', 'apache', 'nginx', or a CSS selector.",
)
@click.option(
    "--listing-attr",
    default=None,
    help="HTML attribute holding the link URL (default: 'href').",
)
def ls(
    path: str,
    token: str | None,
    recursive: bool,
    depth: int | None,
    long_format: bool,
    simple: bool,
    color: str,
    listing: str | None,
    listing_attr: str | None,
):
    """List directory contents.

    Defaults to the current directory if no PATH is given.
    Use -1/--simple for plain one-path-per-line output suitable for piping.

    Examples:
        asanypath ls
        asanypath ls s3://bucket/prefix/
        asanypath ls -1 s3://bucket/prefix/ | xargs asanypath cat
        asanypath ls -r s3://bucket/prefix/
        asanypath ls -r -d 2 s3://bucket/prefix/
        asanypath ls -r --color ocean s3://bucket/prefix/
        asanypath ls art://art.example.com/repo/ --token mytoken
    """

    async def run():
        try:
            if depth is not None:
                recursive_depth = depth
            elif recursive:
                recursive_depth = -1
            else:
                recursive_depth = 0

            kwargs = {}
            if token:
                kwargs["token"] = token
            if listing:
                kwargs["listing"] = listing
            if listing_attr:
                kwargs["listing_attr"] = listing_attr
            p = AsAnyPath(path, **kwargs)

            color_mode = color.strip().lower()
            colors_enabled = color_mode != "off"
            palette = LS_COLOR_THEMES.get(color_mode, LS_COLOR_THEMES["default"])
            depth_gradient = LS_DEPTH_GRADIENTS.get(color_mode, LS_DEPTH_GRADIENTS["default"])

            def sty(key: str) -> str | None:
                if not colors_enabled:
                    return None
                return palette.get(key)

            def depth_sty(depth: int, *, bold: bool = False) -> str | None:
                if not colors_enabled:
                    return None
                style = depth_gradient[min(depth, len(depth_gradient) - 1)]
                return f"bold {style}" if bold else style

            async def _entry_metadata(item: AsAnyPath) -> dict[str, str]:
                if not long_format:
                    return {"size": "", "created": "", "modified": ""}
                size_val = ""
                created_val = ""
                modified_val = ""

                try:
                    st = await item.stat()
                    st_size = getattr(st, "st_size", None)
                    st_ctime = getattr(st, "st_ctime", None)
                    st_mtime = getattr(st, "st_mtime", None)

                    if isinstance(st_size, int):
                        size_val = str(st_size)
                    if isinstance(st_ctime, (int, float)):
                        created_val = datetime.fromtimestamp(st_ctime).strftime("%Y-%m-%d %H:%M:%S")
                    if isinstance(st_mtime, (int, float)):
                        modified_val = datetime.fromtimestamp(st_mtime).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                except Exception:
                    pass

                return {
                    "size": size_val,
                    "created": created_val,
                    "modified": modified_val,
                }

            async def _children(path_obj: AsAnyPath) -> list[AsAnyPath]:
                return sorted([child async for child in path_obj.iterdir()], key=lambda x: x.name)

            async def _collect(
                path_obj: AsAnyPath, depth: int = 0, branch_state: tuple[bool, ...] = ()
            ):
                children = await _children(path_obj)
                for index, child in enumerate(children):
                    is_last = index == len(children) - 1
                    state = branch_state + (is_last,)
                    child_is_dir: bool | None = None
                    should_descend = recursive_depth != 0 and (
                        recursive_depth < 0 or depth + 1 < recursive_depth
                    )
                    if should_descend or getattr(child, "protocol", None) == "s3":
                        try:
                            child_is_dir = await child.is_dir()
                        except Exception:
                            child_is_dir = None

                    metadata = await _entry_metadata(child)
                    yield {
                        "item": child,
                        "is_dir": child_is_dir,
                        "metadata": metadata,
                        "branch_state": state,
                    }
                    if should_descend and child_is_dir:
                        async for row in _collect(child, depth + 1, state):
                            yield row

            def _tree_text(name: str, branch_state: tuple[bool, ...], is_dir: bool | None) -> Text:
                if not branch_state:
                    return Text(name, style=depth_sty(0, bold=bool(is_dir)) or sty("name"))

                text = Text()
                for depth_index, ancestor_is_last in enumerate(branch_state[:-1], start=1):
                    if ancestor_is_last:
                        text.append("    ")
                    else:
                        text.append("│   ", style=depth_sty(depth_index) or sty("guide"))

                branch_depth = len(branch_state)
                branch_glyph = "└── " if branch_state[-1] else "├── "
                text.append(branch_glyph, style=depth_sty(branch_depth) or sty("guide"))
                text.append(name, style=depth_sty(branch_depth, bold=bool(is_dir)) or sty("name"))
                return text

            rows = [row async for row in _collect(p)]
            found_any = bool(rows)

            if not found_any:
                console.print("[dim](empty)[/dim]")
                return 0

            if simple:
                for row in rows:
                    click.echo(str(row["item"]))
                return 0

            table = Table(
                box=None,
                show_edge=False,
                pad_edge=False,
                expand=False,
                padding=(0, 1),
            )
            table.add_column("NAME", style=sty("name"), header_style=sty("name"), no_wrap=True)
            if long_format:
                table.add_column(
                    "SIZE",
                    style=sty("size"),
                    header_style=sty("size"),
                    justify="right",
                    no_wrap=True,
                )
                table.add_column(
                    "CREATED",
                    style=sty("created"),
                    header_style=sty("created"),
                    justify="right",
                    no_wrap=True,
                )
                table.add_column(
                    "MODIFIED",
                    style=sty("modified"),
                    header_style=sty("modified"),
                    justify="right",
                    no_wrap=True,
                )

            console.print(Text(path, style=sty("root")))
            for row in rows:
                cols = [_tree_text(row["item"].name, row["branch_state"], row["is_dir"])]
                if long_format:
                    cols.extend(
                        [
                            row["metadata"]["size"],
                            row["metadata"]["created"],
                            row["metadata"]["modified"],
                        ]
                    )
                table.add_row(*cols)
            console.print(table)

            return 0
        except NotImplementedError:
            console.print("[yellow]Operation not supported for this path type[/yellow]")
            return 1
        except NotADirectoryError:
            console.print(f"[red]Error:[/red] {path} is not a directory")
            return 1
        except FileNotFoundError:
            console.print(f"[red]Error:[/red] {path} not found")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


@cli.command("touch", short_help="Create empty file", context_settings=CONTEXT_SETTINGS)
@click.argument(
    "path", type=str, required=False, callback=_resolve_path_arg, shell_complete=_path_complete
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
def touch(path: str, token: str | None):
    """Create an empty file or update its timestamp.

    PATH can also be supplied via stdin: echo s3://bucket/file | asanypath touch

    Examples:
        asanypath touch s3://bucket/file.txt
        asanypath touch art://repo/path/file.txt --token mytoken
    """

    async def run():
        try:
            kwargs = {}
            if token:
                kwargs["token"] = token
            p = AsAnyPath(path, **kwargs)
            await p.touch(exist_ok=True)
            console.print(f"[green]\u2713[/green] Touched {path}")
            return 0
        except NotImplementedError:
            console.print("[yellow]Operation not supported for this path type[/yellow]")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


@cli.command("checksums", short_help="Show file checksums", context_settings=CONTEXT_SETTINGS)
@click.argument(
    "path", type=str, required=False, callback=_resolve_path_arg, shell_complete=_path_complete
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
def checksums(path: str, token: str | None):
    """Display checksums for a file.

    PATH can also be supplied via stdin: echo s3://bucket/file | asanypath checksums

    Shows available hash digests (md5, sha1, sha256, etc.) depending on the
    backend.

    Examples:
        asanypath checksums s3://bucket/file.txt
        asanypath checksums /local/file.txt
    """

    async def run():
        try:
            kwargs = {}
            if token:
                kwargs["token"] = token
            p = AsAnyPath(path, **kwargs)
            result = await p.checksums()
            if not result:
                console.print("[yellow]No checksums available[/yellow]")
                return 0
            for algo, value in sorted(result.items()):
                console.print(f"  {algo}: {value}")
            return 0
        except NotImplementedError:
            console.print("[yellow]Operation not supported for this path type[/yellow]")
            return 1
        except FileNotFoundError:
            console.print(f"[red]Error:[/red] {path} not found")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


# --- Mutate Commands ---


@cli.command("mkdir", short_help="Create directory", context_settings=CONTEXT_SETTINGS)
@click.argument("path", type=str, required=False, callback=_resolve_path_arg)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
@click.option("--parents", "-p", is_flag=True, help="Create parent directories")
@click.option("--exist-ok", is_flag=True, help="Don't error if directory exists")
def mkdir(path: str, token: str | None, parents: bool, exist_ok: bool):
    """Create directory at PATH.

    PATH can also be supplied via stdin: echo s3://bucket/newdir | asanypath mkdir

    Examples:
        asanypath mkdir s3://bucket/newdir
        asanypath mkdir s3://bucket/a/b/c --parents
    """

    async def run():
        try:
            kwargs = {}
            if token:
                kwargs["token"] = token
            p = AsAnyPath(path, **kwargs)

            await p.mkdir(parents=parents, exist_ok=exist_ok)
            console.print(f"[green]✓[/green] Created {path}")
            return 0
        except NotImplementedError:
            console.print("[yellow]Operation not supported for this path type[/yellow]")
            return 1
        except FileExistsError:
            if exist_ok:
                console.print(f"[yellow]→[/yellow] {path} already exists")
                return 0
            console.print(f"[red]Error:[/red] {path} already exists")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


@cli.command("rm", short_help="Delete file", context_settings=CONTEXT_SETTINGS)
@click.argument(
    "paths", nargs=-1, type=str, callback=_resolve_paths_arg, shell_complete=_path_complete
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
@click.option("--force", "-f", is_flag=True, help="Don't error if file doesn't exist")
@click.option("--recursive", "recursive", "-r", is_flag=True, help="Remove directories recursively")
@click.option("--verbose", "verbose", "-v", is_flag=True, help="Print each removed path")
def rm(paths: tuple[str, ...], token: str | None, force: bool, recursive: bool, verbose: bool):
    """Delete files or directories.

    Paths can also be supplied via stdin (one per line):
        asanypath ls -1 s3://bucket/prefix/ | asanypath rm

    Examples:
        asanypath rm s3://bucket/file.txt
        asanypath rm -r s3://bucket/prefix/
        asanypath rm a.txt b.txt c.txt
        asanypath rm art://art.example.com/repo/file --token mytoken --force
    """

    async def run():
        try:
            kwargs = {}
            if token:
                kwargs["token"] = token

            path_objs = [AsAnyPath(path, **kwargs) for path in paths]
            for p in path_objs:
                exists, is_dir = await _exists_or_virtual_dir(p)
                if not exists and force:
                    if verbose:
                        console.print(f"removed '{p}'")
                    continue
                if not exists:
                    raise FileNotFoundError(2, f"No such file or directory: '{p}'")
                if is_dir and not recursive:
                    raise IsADirectoryError(21, f"cannot remove '{p}': Is a directory")

                if is_dir:
                    await p.rmdir(recursive=True)
                    if verbose:
                        console.print(f"removed '{p}'")
                else:
                    await p.unlink(missing_ok=force)
                    if verbose:
                        console.print(f"removed '{p}'")

            if not verbose:
                console.print(f"[green]✓[/green] Deleted {len(paths)} path(s)")
            return 0
        except NotImplementedError:
            console.print("[yellow]Operation not supported for this path type[/yellow]")
            return 1
        except (FileNotFoundError, IsADirectoryError) as e:
            console.print(f"[red]Error:[/red] {e}")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


@cli.command("mv", short_help="Move/rename file", context_settings=CONTEXT_SETTINGS)
@click.argument(
    "paths", nargs=-1, type=str, callback=_rewrite_paths_arg, shell_complete=_path_complete
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
@click.option("--force", "force", is_flag=True, help="Overwrite differing existing destinations")
@click.option(
    "--atomic",
    "atomic",
    is_flag=True,
    help="Stage local destination writes before finalizing",
)
@click.option(
    "--chunk-size",
    default=None,
    type=click.IntRange(min=0),
    help="Streaming chunk size for local-destination transfers (default: auto; 0 disables)",
)
@click.option(
    "--verbose",
    "verbosity",
    "-v",
    count=True,
    help="Show progress (-v=overall bar, -vv=per-file rows)",
)
@click.option(
    "--concurrency",
    "-j",
    default=DEFAULT_CONCURRENCY,
    type=click.IntRange(min=1),
    help=f"Maximum parallel moves (default: {DEFAULT_CONCURRENCY})",
)
def mv(
    paths: tuple[str, ...],
    token: str | None,
    force: bool,
    atomic: bool,
    chunk_size: int | None,
    verbosity: int,
    concurrency: int,
):
    """Move/rename files or directories.

    Destination defaults to the current directory when omitted.

    Example:
        asanypath mv s3://bucket/file.txt
        asanypath mv s3://bucket/old.txt s3://bucket/new.txt
        asanypath mv a.txt b.txt destdir/
    """

    async def run():
        try:
            if len(paths) < 1:
                raise click.UsageError("mv expects at least one SRC argument.")

            src_paths = list(paths[:-1]) if len(paths) > 1 else list(paths)
            dst_path_raw = paths[-1] if len(paths) > 1 else "."

            kwargs = {}
            if token:
                kwargs["token"] = token

            src_objs = [AsAnyPath(src, **kwargs) for src in src_paths]
            dst_obj = AsAnyPath(dst_path_raw, **kwargs)

            dst_exists, dst_is_dir = await _exists_or_virtual_dir(dst_obj)
            # A trailing separator ("dir/", or a bare "host:" that expands to
            # "ssh://host/~/") means move INTO the directory even if it doesn't
            # exist yet — matches mv/scp semantics.
            dst_is_explicit_dir = dst_path_raw.rstrip().endswith(("/", os.sep))
            dst_is_dir = dst_is_dir or dst_is_explicit_dir

            if len(src_objs) > 1 and not dst_is_dir:
                await dst_obj.mkdir(parents=True, exist_ok=True)
                dst_is_dir = True

            items: list[tuple[str, object]] = []
            for src in src_objs:
                src_exists, src_is_dir = await _exists_or_virtual_dir(src)
                if not src_exists:
                    raise FileNotFoundError(2, f"No such file or directory: '{src}'")
                if len(src_objs) > 1:
                    target = dst_obj / src.name
                elif src_is_dir:
                    target = (dst_obj / src.name) if dst_exists and dst_is_dir else dst_obj
                else:
                    target = (dst_obj / src.name) if dst_is_dir else dst_obj

                kw = {
                    "remove_src": True,
                    "force": force,
                    "atomic": atomic,
                    "chunk_size": chunk_size,
                }
                if src_is_dir:
                    kw["recursive"] = True

                items.append(
                    (
                        src.name,
                        _TransferWork(
                            _make_simple_copy_work(src, target, verbosity, copy_kwargs=kw),
                            src,
                        ),
                    )
                )

            async def _iter_move_items() -> AsyncIterator[tuple[str, object]]:
                for item in items:
                    yield item

            succeeded, failures, elapsed = await _run_concurrent_stream(
                _iter_move_items(),
                description="Moving",
                verbosity=verbosity,
                concurrency=concurrency,
                show_transfer=True,
                total=len(items),
            )
            if failures:
                return 2
            console.print(
                f"[green]✓[/green] Moved {succeeded} path(s) in just {_fmt_elapsed(elapsed)}"
            )
            return 0
        except NotImplementedError:
            console.print("[yellow]Operation not supported for this path type[/yellow]")
            return 1
        except (FileNotFoundError, FileExistsError, ValueError) as e:
            console.print(f"[red]Error:[/red] {e}")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


# --- Root Copy Command ---


@cli.command("cp", short_help="Copy file or directory", context_settings=CONTEXT_SETTINGS)
@click.argument(
    "paths", nargs=-1, type=str, callback=_rewrite_paths_arg, shell_complete=_path_complete
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
@click.option("--force", "force", is_flag=True, help="Overwrite differing existing destinations")
@click.option(
    "--atomic",
    "atomic",
    is_flag=True,
    help="Stage local destination writes before finalizing",
)
@click.option(
    "--chunk-size",
    default=None,
    type=click.IntRange(min=0),
    help="Streaming chunk size for local-destination transfers (default: auto; 0 disables)",
)
@click.option("--recursive", "recursive", "-r", is_flag=True, help="Copy directories recursively")
@click.option(
    "--verbose",
    "verbosity",
    "-v",
    count=True,
    help="Show progress (-v=overall bar, -vv=per-file rows)",
)
@click.option(
    "--concurrency",
    "-j",
    default=DEFAULT_CONCURRENCY,
    type=click.IntRange(min=1),
    help=f"Maximum parallel copies (default: {DEFAULT_CONCURRENCY})",
)
@click.option(
    "--listing",
    default=None,
    help="HTTP directory-listing mode: 'python', 'apache', 'nginx', or a CSS selector.",
)
@click.option(
    "--listing-attr",
    default=None,
    help="HTML attribute holding the link URL (default: 'href').",
)
def cp(
    paths: tuple[str, ...],
    token: str | None,
    force: bool,
    atomic: bool,
    chunk_size: int | None,
    recursive: bool,
    verbosity: int,
    concurrency: int,
    listing: str | None,
    listing_attr: str | None,
):
    """Copy files or directories.

    Destination defaults to the current directory when omitted.

    Use '-' as the source to read from stdin (binary).

    Supports multiple source paths to one destination directory.

    Examples:
        asanypath cp s3://bucket/file.txt
        asanypath cp src.txt dst.txt
        asanypath cp -r s3://bucket/src s3://bucket/dst
        asanypath cp a.txt b.txt destdir/
        pg_dump | gzip | asanypath cp - s3://bucket/backup.sql.gz
    """

    async def run():
        try:
            if len(paths) < 1:
                raise click.UsageError("cp expects at least one SRC argument.")

            src_paths = list(paths[:-1]) if len(paths) > 1 else list(paths)
            dst_path_raw = paths[-1] if len(paths) > 1 else "."

            kwargs = {}
            if token:
                kwargs["token"] = token
            if listing:
                kwargs["listing"] = listing
            if listing_attr:
                kwargs["listing_attr"] = listing_attr

            # Handle '-' as stdin source
            if src_paths == ["-"]:
                content = sys.stdin.buffer.read()
                dst_obj = AsAnyPath(dst_path_raw, **kwargs)
                await dst_obj.write_bytes(content)
                if verbosity:
                    console.print(f"  [dim]stdin → {dst_path_raw}[/dim]")
                return 0

            src_objs = [AsAnyPath(src, **kwargs) for src in src_paths]
            dst_obj = AsAnyPath(dst_path_raw, **kwargs)

            dst_exists, dst_is_dir = await _exists_or_virtual_dir(dst_obj)
            # A trailing separator ("dir/", or a bare "host:" that expands to
            # "ssh://host/~/") means copy INTO the directory even if it doesn't
            # exist yet — matches cp/scp semantics.
            dst_is_explicit_dir = dst_path_raw.rstrip().endswith(("/", os.sep))
            dst_is_dir = dst_is_dir or dst_is_explicit_dir

            if len(src_objs) > 1 and not dst_is_dir:
                await dst_obj.mkdir(parents=True, exist_ok=True)
                dst_is_dir = True

            plan: list[tuple[AsAnyPath, bool, AsAnyPath]] = []
            for src in src_objs:
                src_exists, src_is_dir = await _exists_or_virtual_dir(src)
                if not src_exists:
                    raise FileNotFoundError(2, f"No such file or directory: '{src}'")
                if src_is_dir and not recursive:
                    raise IsADirectoryError(
                        21, f"omitting directory '{src}' (use -r to copy recursively)"
                    )

                if len(src_objs) > 1:
                    target = dst_obj / src.name
                elif src_is_dir:
                    target = (dst_obj / src.name) if dst_exists and dst_is_dir else dst_obj
                else:
                    target = (dst_obj / src.name) if dst_is_dir else dst_obj

                plan.append((src, src_is_dir, target))

            async def _iter_copy_items() -> AsyncIterator[tuple[str, object]]:
                for src, src_is_dir, target in plan:
                    if src_is_dir:
                        # Walk and enqueue per-file copies as soon as found.
                        async for rel, src_file in _collect_files(src):
                            dst_file = target / rel

                            yield (
                                _task_label(rel),
                                _TransferWork(
                                    _make_simple_copy_work(
                                        src_file,
                                        dst_file,
                                        verbosity,
                                        copy_kwargs={
                                            "force": force,
                                            "atomic": atomic,
                                            "chunk_size": chunk_size,
                                        },
                                    ),
                                    src_file,
                                ),
                            )
                    else:
                        yield (
                            src.name,
                            _TransferWork(
                                _make_simple_copy_work(
                                    src,
                                    target,
                                    verbosity,
                                    copy_kwargs={
                                        "force": force,
                                        "atomic": atomic,
                                        "chunk_size": chunk_size,
                                    },
                                ),
                                src,
                            ),
                        )

            succeeded, failures, elapsed = await _run_concurrent_stream(
                _iter_copy_items(),
                description="Copying",
                verbosity=verbosity,
                concurrency=concurrency,
                show_transfer=True,
            )
            if failures:
                return 2
            console.print(
                f"[green]✓[/green] Copied {succeeded} file(s) in just {_fmt_elapsed(elapsed)}"
            )
            return 0
        except NotImplementedError:
            console.print("[yellow]Operation not supported for this path type[/yellow]")
            return 1
        except (FileNotFoundError, IsADirectoryError, FileExistsError, ValueError) as e:
            console.print(f"[red]Error:[/red] {e}")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


# --- Sync Command ---


async def _get_mtime(path_obj: AsAnyPath) -> float | None:
    """Get modification time for a path. Returns None if unavailable."""
    try:
        st = await path_obj.stat()
        mtime = getattr(st, "st_mtime", None)
        if isinstance(mtime, (int, float)):
            return float(mtime)
    except (NotImplementedError, FileNotFoundError, Exception):
        pass
    return None


async def _get_size(path_obj: AsAnyPath) -> int | None:
    """Get file size for a path. Returns None if unavailable."""
    try:
        st = await path_obj.stat()
        size = getattr(st, "st_size", None)
        if isinstance(size, int):
            return size
    except (NotImplementedError, FileNotFoundError, Exception):
        pass
    return None


async def _emit_progress(
    on_progress,
    delta: int,
    transferred: int,
    src_obj: AsAnyPath,
    dst_obj: AsAnyPath,
) -> None:
    if on_progress is None:
        return
    result = on_progress(delta, transferred, src_obj, dst_obj)
    if inspect.isawaitable(result):
        await result


def _make_simple_copy_work(
    src_obj: AsAnyPath,
    dst_obj: AsAnyPath,
    verbosity: int,
    copy_kwargs: dict | None = None,
) -> object:
    """Create a copy work callable with standard progress fallback."""
    copy_kw = copy_kwargs or {}

    async def _run_copy(*, on_progress=None):
        async def _call(progress_cb):
            cb_kw = {"on_progress": progress_cb} if progress_cb is not None else {}
            return await src_obj.copy(dst_obj, **copy_kw, **cb_kw)

        return await _copy_with_progress_fallback(
            src_obj=src_obj,
            dst_obj=dst_obj,
            on_progress=on_progress,
            copy_call=_call,
            poll_interval=0.1 if verbosity >= 2 else None,
        )

    return _run_copy


async def _copy_with_progress_fallback(
    *,
    src_obj: AsAnyPath,
    dst_obj: AsAnyPath,
    on_progress,
    copy_call,
    poll_interval: float | None = None,
):
    """Run copy and backfill progress from destination size when callbacks are absent."""
    progress_emitted = False
    last_polled = 0

    async def _progress_cb(delta: int, transferred: int, src, dst):
        nonlocal progress_emitted
        progress_emitted = True
        await _emit_progress(on_progress, delta, transferred, src, dst)

    monitor_task = None
    if (
        on_progress is not None
        and poll_interval is not None
        and getattr(dst_obj, "protocol", None) == "file"
    ):

        async def _monitor() -> None:
            nonlocal last_polled
            while True:
                await asyncio.sleep(poll_interval)
                size = await _get_size(dst_obj)
                if isinstance(size, int) and size > last_polled:
                    delta = size - last_polled
                    last_polled = size
                    await _emit_progress(on_progress, delta, size, src_obj, dst_obj)

        monitor_task = asyncio.create_task(_monitor())

    try:
        out = await copy_call(_progress_cb if on_progress is not None else None)
    finally:
        if monitor_task is not None:
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task

    if on_progress is not None and not progress_emitted:
        final_size = await _get_size(dst_obj)
        if isinstance(final_size, int) and final_size > last_polled:
            await _emit_progress(
                on_progress,
                final_size - last_polled,
                final_size,
                src_obj,
                dst_obj,
            )

    return out


async def _exists_or_virtual_dir(path_obj: AsAnyPath) -> tuple[bool, bool]:
    """Return ``(exists, is_dir)`` including prefix-only virtual directories."""
    exists = await path_obj.exists()
    exists = exists if isinstance(exists, bool) else False
    if exists:
        is_dir = await path_obj.is_dir()
        return True, is_dir if isinstance(is_dir, bool) else False
    try:
        is_dir = await path_obj.is_dir()
    except Exception:  # noqa: BLE001
        return False, False
    is_dir = is_dir if isinstance(is_dir, bool) else False
    return is_dir, is_dir


async def _collect_files(
    root: AsAnyPath,
    prefix: str = "",
    *,
    on_file=None,
) -> AsyncIterator[tuple[str, AsAnyPath]]:
    """Recursively collect all files under root, returning (relative_path, path_obj) pairs."""
    try:
        async for child in root.iterdir():
            rel = f"{prefix}{child.name}" if prefix else child.name
            is_dir = await child.is_dir()
            is_dir = is_dir if isinstance(is_dir, bool) else False
            if is_dir:
                async for nested_rel, nested_child in _collect_files(
                    child,
                    f"{rel}/",
                    on_file=on_file,
                ):
                    yield nested_rel, nested_child
            else:
                if on_file is not None:
                    on_file(rel, child)
                yield rel, child
    except (NotADirectoryError, FileNotFoundError):
        return


async def _needs_sync(
    src_obj: AsAnyPath,
    dst_obj: AsAnyPath,
    *,
    size_only: bool = False,
) -> bool:
    """Determine if src needs to be synced to dst.

    Strategy: if dst doesn't exist, sync. Otherwise compare size, then mtime.
    """
    if not await dst_obj.exists():
        return True

    src_size = await _get_size(src_obj)
    dst_size = await _get_size(dst_obj)
    if src_size is not None and dst_size is not None and src_size != dst_size:
        return True

    if size_only:
        return False

    src_mtime = await _get_mtime(src_obj)
    dst_mtime = await _get_mtime(dst_obj)
    if src_mtime is not None and dst_mtime is not None:
        return src_mtime > dst_mtime

    # If we can't determine mtime, transfer if sizes differ or are unknown
    if src_size is None or dst_size is None:
        return True
    return False


@cli.command("presign", short_help="Generate presigned URL", context_settings=CONTEXT_SETTINGS)
@click.argument("path", type=str, callback=_rewrite_path_arg, shell_complete=_path_complete)
@click.option("--expires", "-e", default=3600, type=int, help="URL validity in seconds")
@click.option(
    "--method",
    "-m",
    default="GET",
    type=click.Choice(["GET", "PUT", "HEAD", "DELETE"]),
    help="HTTP method the URL will be used for",
)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
def presign(path: str, expires: int, method: str, token: str | None) -> None:
    """Generate a presigned URL for a cloud object.

    The URL allows unauthenticated access for the specified duration.
    Supports S3, Azure Blob Storage, and GCS backends.
    """

    async def _presign() -> str:
        kwargs = {}
        if token:
            kwargs["token"] = token
        p = AsAnyPath(path, **kwargs)
        return await p.presign(expires=expires, method=method)

    try:
        url = asyncio.run(_presign())
        console.print(url)
    except (ValueError, NotImplementedError) as e:
        console.print(f"[red]Error:[/red] {e}", style="bold")
        raise SystemExit(1)


@cli.command("sync", short_help="Sync directories", context_settings=CONTEXT_SETTINGS)
@click.argument("src", type=str, shell_complete=_path_complete)
@click.argument("dst", type=str, shell_complete=_path_complete)
@click.option("--token", default=None, help="Bearer token for authenticated backends")
@click.option("--delete", "delete_extra", is_flag=True, help="Delete files in DST not in SRC")
@click.option(
    "--dry-run", "-n", is_flag=True, help="Show what would be transferred without doing it"
)
@click.option("--size-only", is_flag=True, help="Compare only file sizes, skip mtime check")
@click.option(
    "--verbose",
    "verbosity",
    "-v",
    count=True,
    help="Show progress (-v=overall bar, -vv=per-file rows)",
)
@click.option(
    "--concurrency",
    "-j",
    default=DEFAULT_CONCURRENCY,
    type=click.IntRange(min=1),
    help=f"Maximum parallel transfers (default: {DEFAULT_CONCURRENCY})",
)
@click.option("--exclude", multiple=True, help="Exclude files matching glob pattern (repeatable)")
@click.option(
    "--listing",
    default=None,
    help="HTTP directory-listing mode: 'python', 'apache', 'nginx', or a CSS selector.",
)
@click.option(
    "--listing-attr",
    default=None,
    help="HTML attribute holding the link URL (default: 'href').",
)
def sync(
    src: str,
    dst: str,
    token: str | None,
    delete_extra: bool,
    dry_run: bool,
    size_only: bool,
    verbosity: int,
    concurrency: int,
    exclude: tuple[str, ...],
    listing: str | None,
    listing_attr: str | None,
):
    """Sync files from SRC to DST (like aws s3 sync or rsync).

    Only transfers files that are new or modified. Works across different
    cloud backends (e.g. S3 to Azure, local to GCS).

    Examples:
        asanypath sync ./local-dir s3://bucket/prefix/
        asanypath sync s3://bucket/src az://container/dst
        asanypath sync gs://bucket/data ./local-backup --delete
        asanypath sync s3://src s3://dst --dry-run
        asanypath sync ./src s3://bucket --exclude '*.tmp' --exclude '.git/*'
    """
    import fnmatch

    async def run():
        try:
            kwargs = {}
            if token:
                kwargs["token"] = token
            if listing:
                kwargs["listing"] = listing
            if listing_attr:
                kwargs["listing_attr"] = listing_attr

            src_obj = AsAnyPath(src, **kwargs)
            dst_obj = AsAnyPath(dst, **kwargs)

            # Ensure src exists
            src_exists, src_is_dir = await _exists_or_virtual_dir(src_obj)
            if not src_exists:
                console.print(f"[red]Error:[/red] source not found: {src}")
                return 1

            if not src_is_dir:
                # Single file sync
                target = dst_obj
                dst_exists, dst_is_dir = await _exists_or_virtual_dir(dst_obj)
                if dst_exists and dst_is_dir:
                    target = dst_obj / src_obj.name

                if await _needs_sync(src_obj, target, size_only=size_only):
                    if dry_run:
                        console.print(f"[dim](dry-run)[/dim] {src_obj} -> {target}")
                    else:
                        await src_obj.copy(target, force=True)
                        console.print("[green]✓[/green] Synced 1 file")
                else:
                    console.print("[green]✓[/green] Already up-to-date")
                return 0

            # Directory sync
            scan_count = 0

            def _on_scanned(_rel, _obj):
                nonlocal scan_count
                scan_count += 1
                if scan_count % 200 == 0:
                    scan.update(f"[dim]Scanning source... found {scan_count} file(s)[/dim]")

            with console.status("[dim]Scanning source...[/dim]") as scan:
                src_files = [
                    (rel, obj) async for rel, obj in _collect_files(src_obj, on_file=_on_scanned)
                ]

            # Apply exclude patterns
            if exclude:
                filtered = []
                for rel, obj in src_files:
                    if not any(fnmatch.fnmatch(rel, pat) for pat in exclude):
                        filtered.append((rel, obj))
                excluded_count = len(src_files) - len(filtered)
                src_files = filtered
                if excluded_count and verbosity:
                    console.print(f"[dim]Excluded {excluded_count} file(s)[/dim]")

            # Determine what needs syncing
            to_transfer: list[tuple[str, AsAnyPath, AsAnyPath]] = []
            checked = 0
            with console.status("[dim]Comparing source and destination...[/dim]") as compare:
                for rel, src_file in src_files:
                    checked += 1
                    if checked % 200 == 0:
                        compare.update(f"[dim]Comparing... {checked}/{len(src_files)}[/dim]")
                    dst_file = dst_obj / rel
                    if await _needs_sync(src_file, dst_file, size_only=size_only):
                        to_transfer.append((rel, src_file, dst_file))

            # Handle --delete: find files in dst not in src
            to_delete: list[tuple[str, AsAnyPath]] = []
            if delete_extra:
                dst_exists = await dst_obj.exists()
                if dst_exists:
                    dst_files = [(rel, obj) async for rel, obj in _collect_files(dst_obj)]
                    src_rel_set = {rel for rel, _ in src_files}
                    for rel, dst_file in dst_files:
                        if rel not in src_rel_set:
                            to_delete.append((rel, dst_file))

            # Summary
            if not to_transfer and not to_delete:
                console.print("[green]✓[/green] Already up-to-date")
                return 0

            if dry_run:
                for rel, src_file, dst_file in to_transfer:
                    console.print(f"[dim](would copy)[/dim]  {src_file} -> {dst_file}")
                for rel, dst_file in to_delete:
                    console.print(f"[dim](would delete)[/dim] {dst_file}")
                console.print(
                    f"\n[dim]Dry run: {len(to_transfer)} to copy, {len(to_delete)} to delete[/dim]"
                )
                return 0

            # Transfer files in parallel.
            transfer_items: list[tuple[str, object]] = []
            for _, s, d in to_transfer:
                transfer_items.append(
                    (
                        s.name,
                        _TransferWork(
                            _make_simple_copy_work(s, d, verbosity, copy_kwargs={"force": True}),
                            s,
                        ),
                    )
                )

            async def _iter_transfer_items() -> AsyncIterator[tuple[str, object]]:
                for item in transfer_items:
                    yield item

            transferred, copy_failures, copy_elapsed = await _run_concurrent_stream(
                _iter_transfer_items(),
                description="Syncing",
                verbosity=verbosity,
                concurrency=concurrency,
                show_transfer=True,
                total=len(transfer_items),
            )

            # Delete extra files in parallel.
            deleted = 0
            delete_failures: list = []
            delete_elapsed = 0.0
            if to_delete:
                deleted, delete_failures, delete_elapsed = await _run_concurrent(
                    [(p.name, p.unlink(missing_ok=True)) for _, p in to_delete],
                    description="Deleting",
                    verbosity=verbosity,
                    concurrency=concurrency,
                )

            if copy_failures or delete_failures:
                return 2

            parts = []
            if transferred:
                parts.append(f"{transferred} copied")
            if deleted:
                parts.append(f"{deleted} deleted")
            skipped = len(src_files) - transferred
            if skipped:
                parts.append(f"{skipped} up-to-date")
            console.print(
                f"[green]✓[/green] Sync complete: {', '.join(parts)}"
                f" in just {_fmt_elapsed(copy_elapsed + delete_elapsed)}"
            )
            return 0

        except NotImplementedError:
            console.print("[yellow]Operation not supported for this path type[/yellow]")
            return 1
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            return 2

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


# --- Auth/Env Commands ---


@cli.group(short_help="Auth and environment diagnostics", context_settings=CONTEXT_SETTINGS)
def auth():
    """Inspect authentication and environment configuration."""
    pass


@auth.command("show", short_help="Show auth configuration", context_settings=CONTEXT_SETTINGS)
@click.argument("path", type=str, required=False, callback=_rewrite_path_arg)
def auth_show(path: str | None):
    """Show active authentication and environment configuration.

    If PATH is provided, show auth config specifically for that backend.

    Examples:
        asanypath auth show
        asanypath auth show s3://bucket
        asanypath auth show art://art.example.com
    """
    from urllib.parse import urlparse

    # Determine backend from path
    if path:
        scheme = urlparse(path).scheme.lower() or "file"
    else:
        scheme = None

    console.print("[bold]Authentication Configuration[/bold]")

    # S3 env vars
    if scheme is None or scheme == "s3":
        console.print("\n[cyan]AWS S3:[/cyan]")
        console.print(f"  AWS_PROFILE: {getenv('AWS_PROFILE', '(not set)')}")
        console.print(f"  AWS_REGION: {getenv('AWS_REGION', '(not set)')}")
        console.print(f"  AWS_ACCESS_KEY_ID: {mask_token(getenv('AWS_ACCESS_KEY_ID'))}")
        console.print(f"  AWS_SECRET_ACCESS_KEY: {mask_token(getenv('AWS_SECRET_ACCESS_KEY'))}")
        console.print(f"  AWS_SESSION_TOKEN: {mask_token(getenv('AWS_SESSION_TOKEN'))}")
        creds_file = Path.home() / ".aws" / "credentials"
        console.print(f"  Credentials file: {creds_file} {'✓' if creds_file.exists() else '✗'}")

    # GCS env vars
    if scheme is None or scheme == "gs":
        console.print("\n[cyan]Google Cloud Storage:[/cyan]")
        console.print(f"  GCP_PROJECT: {getenv('GCP_PROJECT', '(not set)')}")
        console.print(f"  GOOGLE_CLOUD_PROJECT: {getenv('GOOGLE_CLOUD_PROJECT', '(not set)')}")
        creds_file = getenv("GOOGLE_APPLICATION_CREDENTIALS", "(not set)")
        exists = Path(creds_file).exists() if creds_file != "(not set)" else False
        console.print(f"  GOOGLE_APPLICATION_CREDENTIALS: {creds_file} {'✓' if exists else '✗'}")

    # Azure env vars
    if scheme is None or scheme == "az":
        console.print("\n[cyan]Azure Blob Storage:[/cyan]")
        console.print(f"  AZURE_STORAGE_ACCOUNT: {getenv('AZURE_STORAGE_ACCOUNT', '(not set)')}")
        console.print(f"  AZURE_STORAGE_KEY: {mask_token(getenv('AZURE_STORAGE_KEY'))}")
        console.print(f"  AZURE_STORAGE_SAS_TOKEN: {mask_token(getenv('AZURE_STORAGE_SAS_TOKEN'))}")

    # Artifactory env vars
    if scheme is None or scheme == "art":
        console.print("\n[cyan]JFrog Artifactory:[/cyan]")
        console.print(
            f"  ARTIFACTORY_IDENTITY_TOKEN: {mask_token(getenv('ARTIFACTORY_IDENTITY_TOKEN'))}"
        )

    # SSH env vars + ssh_config
    if scheme is None or scheme == "ssh":
        console.print("\n[cyan]SSH / SFTP:[/cyan]")
        console.print(f"  SSH_USER: {getenv('SSH_USER', '(not set)')}")
        console.print(f"  SSH_KEY_FILE: {getenv('SSH_KEY_FILE', '(not set)')}")
        console.print(f"  SSH_KNOWN_HOSTS: {getenv('SSH_KNOWN_HOSTS', '(default)')}")
        try:
            from asanypath.ssh import SSHPath, _discover_ssh_config

            cfg_paths = _discover_ssh_config()
            if cfg_paths:
                for p in cfg_paths:
                    console.print(f"  ssh_config: {p} ✓")
            else:
                console.print("  ssh_config: (none found)")
            if path and scheme == "ssh":
                try:
                    console.print(f"  resolved target: {SSHPath(path).resolved_target}")
                except Exception as exc:  # noqa: BLE001
                    console.print(f"  resolved target: [red](error: {exc})[/red]")
        except ImportError:
            console.print("  ssh backend: [yellow](optional dependency not installed)[/yellow]")
        console.print(
            "  [dim]Passwords are not read from env; use keys/agent or ~/.ssh/config.[/dim]"
        )

    # FTP(S) env vars + netrc
    if scheme is None or scheme in ("ftp", "ftps"):
        console.print("\n[cyan]FTP / FTPS:[/cyan]")
        console.print(f"  FTP_USER: {getenv('FTP_USER', '(default: anonymous)')}")
        console.print(f"  FTP_HOST: {getenv('FTP_HOST', '(not set)')}")
        console.print(f"  FTP_PORT: {getenv('FTP_PORT', '(default: 21)')}")
        netrc_path = Path(getenv("NETRC") or (Path.home() / ".netrc"))
        console.print(f"  netrc: {netrc_path} {'✓' if netrc_path.exists() else '✗'}")
        if path and scheme in ("ftp", "ftps") and netrc_path.exists():
            try:
                import netrc as _netrc
                from urllib.parse import urlparse

                host = urlparse(path).hostname
                rc = _netrc.netrc(str(netrc_path))
                entry = rc.authenticators(host) if host else None
                marker = "✓ entry found" if entry else "✗ no entry"
                console.print(f"  netrc entry for {host}: {marker}")
            except Exception as exc:  # noqa: BLE001
                console.print(f"  netrc lookup: [red](error: {exc})[/red]")
        console.print("  [dim]Passwords are not read from env; use ~/.netrc.[/dim]")

    console.print("\n[dim]Note: Tokens are masked to prevent accidental exposure.[/dim]")


def main():
    """Entry point for CLI."""
    cli()


if __name__ == "__main__":
    main()
