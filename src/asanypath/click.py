# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""click parameter type for local and remote paths.

``click.Path`` validates with ``os.stat()``, so for remote URIs such as
``s3://bucket/key`` its ``exists``, ``file_okay`` and ``dir_okay`` checks are
skipped (``exists=False``) or always fail (``exists=True``), even with
``path_type=AsAnyPath``. :class:`ClickPath` validates remote paths through
asanypath and leaves local paths to ``click.Path``.

Usage::

    import click
    from asanypath.click import ClickPath

    @click.command()
    @click.option("--src", type=ClickPath(exists=True, dir_okay=False))
    @click.option("--out", type=ClickPath(file_okay=False))
    def main(src, out):
        ...

Shell completion (``eval "$(_PROG_COMPLETE=bash_source prog)"`` and the zsh and
fish equivalents) completes remote paths too: ``s3://bucket/pre<TAB>`` lists
that prefix and ``s<TAB>`` offers ``s3://`` and the other schemes. Importing
this module registers :class:`BashComplete` for click's bash completion, so
URIs complete as one word.

Requires click, e.g. through the ``cli`` extra: ``asanypath[cli]``.
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

try:
    import click
    from click.shell_completion import (
        BashComplete as _ClickBashComplete,
    )
    from click.shell_completion import (
        CompletionItem,
        add_completion_class,
    )
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "asanypath.click requires click; install asanypath with the 'cli' extra"
    ) from exc

from asanypath import AsAnyPath as _AsyncPath
from asanypath.asanypath import PROTOCOL_MAP
from asanypath.exceptions import AsAnyPathError
from asanypath.local import SyncPath
from asanypath.sync import AsAnyPath

__all__ = ["BashComplete", "ClickPath"]

# Characters in bash's default COMP_WORDBREAKS that occur in URIs.
_BASH_BREAKS = (":", "@")

_SOURCE_BASH = """\
%(complete_func)s() {
    local IFS=$'\\n'
    local response
    response=$(env COMP_WORDS="${COMP_WORDS[*]}" COMP_CWORD=$COMP_CWORD \\
        %(complete_var)s=bash_complete %(complete_exe)s)

    COMPREPLY=()
    local type value nospace=0
    for completion in $response; do
        IFS=',' read -r type value <<< "$completion"
        if [[ $type == 'dir' ]]; then
            COMPREPLY=()
            compopt -o dirnames
        elif [[ $type == 'file' ]]; then
            COMPREPLY=()
            compopt -o default
        elif [[ $type == 'plain' ]]; then
            COMPREPLY+=("$value")
            [[ $value == */ ]] && nospace=1
        fi
    done
    # Stay in a directory (or scheme://) so TAB can continue into it.
    if [[ $nospace -eq 1 && ${#COMPREPLY[@]} -eq 1 ]]; then
        compopt -o nospace
    fi
    return 0
}

complete -o nosort -F %(complete_func)s %(prog_name)s
"""


class BashComplete(_ClickBashComplete):
    """click's bash completion, keeping URIs and ``user@host:path`` one word.

    Bash splits words at ``:`` and ``@``, so ``s3://b/k`` reaches click as the
    arguments ``s3``, ``:`` and ``//b/k``. They are joined again here, and
    replies drop the part before the word bash replaces. A single directory
    candidate (ending in ``/``) gets no trailing space.

    Set :attr:`complete_exe` to call a specific executable from the script
    instead of the command as typed.
    """

    source_template = _SOURCE_BASH
    complete_exe = "$1"
    _cut = 0

    def source_vars(self) -> dict[str, Any]:  # type: ignore[override]  # private TypedDict in click 8.2+
        return {**super().source_vars(), "complete_exe": self.complete_exe}

    def get_completion_args(self) -> tuple[list[str], str]:
        args, incomplete = super().get_completion_args()
        words = _join_breaks([*args, incomplete] if incomplete else args)
        if not incomplete:
            return words, ""
        # Readline replaces the text after the last break, empty right after one.
        typed = "" if incomplete in _BASH_BREAKS else incomplete
        self._cut = len(words[-1]) - len(typed)
        return words[:-1], words[-1]

    def format_completion(self, item: CompletionItem) -> str:
        if self._cut and item.type == "plain":
            item = CompletionItem(item.value[self._cut :], type=item.type, help=item.help)
        return super().format_completion(item)


def _join_breaks(words: list[str]) -> list[str]:
    """Join words bash split at ``:`` or ``@``: ``s3 : //b`` -> ``s3://b``."""
    out: list[str] = []
    glue = False
    for word in words:
        if out and (glue or word in _BASH_BREAKS):
            glue = word in _BASH_BREAKS
            out[-1] += word
        else:
            glue = False
            out.append(word)
    return out


add_completion_class(BashComplete)


def _completion_debug(msg: str) -> None:
    if os.environ.get("ASANYPATH_COMPLETION_DEBUG"):
        print(f"[asanypath completion] {msg}", file=sys.stderr)


class ClickPath(click.Path):
    """``click.Path`` that also validates remote AsAnyPath URIs.

    Takes the same arguments as ``click.Path``. ``path_type`` defaults to the
    synchronous :class:`asanypath.sync.AsAnyPath`.

    Local paths, including ``file://`` URIs, behave exactly like ``click.Path``.
    For remote paths:

    * ``exists=True`` requires an object or a non-empty prefix at the path.
    * ``file_okay=False`` rejects a path that is only a file and ``dir_okay=False``
      a path that is only a directory. On object stores a key can be both.
    * ``readable``, ``writable``, ``executable`` and ``resolve_path`` are not
      checked.
    * If the backend cannot be queried (missing credentials, network errors,
      unsupported protocol), ``exists=True`` reports the error and ``exists=False``
      accepts the path unchecked, as ``click.Path`` does for a local ``OSError``.

    Remote checks send requests while arguments are parsed: ``exists()``, plus
    ``is_dir()`` or ``is_file()`` when a kind is disallowed.

    Shell completion lists remote directories (with a trailing ``/``) and,
    unless ``file_okay=False``, files, within :attr:`completion_timeout`
    seconds. Local paths are left to the shell's file completion, as with
    ``click.Path``. Set ``ASANYPATH_COMPLETION_DEBUG=1`` to see why a remote
    completion came back empty.
    """

    #: Seconds a remote listing may take before completion gives up.
    completion_timeout: float = 5.0

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("path_type", AsAnyPath)
        super().__init__(**kwargs)

    def convert(self, value: Any, param: click.Parameter | None, ctx: click.Context | None) -> Any:
        try:
            path = AsAnyPath(value)
        except AsAnyPathError as exc:
            self.fail(str(exc), param, ctx)

        if isinstance(path, SyncPath):
            return super().convert(os.fspath(path), param, ctx)

        label = f"{self.name.title()} {click.utils.format_filename(str(path))!r}"
        try:
            if not path.exists():
                if self.exists:
                    self.fail(f"{label} does not exist.", param, ctx)
                return self._coerce(path)
            if not self.file_okay and not path.is_dir():
                self.fail(f"{label} is a file.", param, ctx)
            if not self.dir_okay and not path.is_file():
                self.fail(f"{label} is a directory.", param, ctx)
        except click.BadParameter:
            raise
        except Exception as exc:  # backends raise different types for auth/network errors
            if self.exists:
                self.fail(f"{label} could not be checked: {exc}", param, ctx)
        return self._coerce(path)

    def _coerce(self, path: Any) -> Any:
        """Return ``path`` as ``path_type``; remote paths have no ``os.fspath()``."""
        if self.type is None or self.type is str:
            return str(path)
        if self.type is bytes:
            return os.fsencode(str(path))
        if isinstance(path, self.type):
            return path
        return self.type(str(path))

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> list[CompletionItem]:
        schemes = [f"{s}://" for s in sorted(PROTOCOL_MAP) if f"{s}://".startswith(incomplete)]
        if incomplete and schemes and "://" not in incomplete:
            # Plain items replace the shell's file completion, so add local matches.
            return [CompletionItem(c) for c in schemes + _local_matches(incomplete)]
        uri = self._completion_uri(incomplete)
        if "://" not in uri:
            return super().shell_complete(ctx, param, incomplete)
        # Keep what was typed (shorthand, scp-style) and add the child names.
        head = incomplete[: len(incomplete) - len(uri.rsplit("/", 1)[1])]
        return [
            CompletionItem(head + name)
            for name in self._complete_remote(uri)
            if (head + name).startswith(incomplete)
        ]

    def _completion_uri(self, value: str) -> str:
        """The URI to list for ``value``. Override to accept CLI shorthands."""
        return value

    def _complete_remote(self, uri: str) -> list[str]:
        """Sorted child names of ``uri``'s directory starting with its last
        segment; directories end with ``/``."""
        directory, _, prefix = uri.rpartition("/")

        async def listing() -> list[str]:
            names = []
            async for entry in _AsyncPath(f"{directory}/").scandir():
                name = entry.path.name
                if not name.startswith(prefix) or (name[:1] == "." and prefix[:1] != "."):
                    continue
                if entry.is_dir:
                    names.append(f"{name}/")
                elif self.file_okay:
                    names.append(name)
            return sorted(names)

        try:
            return asyncio.run(asyncio.wait_for(listing(), self.completion_timeout))
        except Exception as exc:  # completion must never break the shell
            _completion_debug(f"{type(exc).__name__} listing {uri!r}: {exc}")
            return []


def _local_matches(word: str) -> list[str]:
    """Entries of the current directory starting with ``word``."""
    try:
        with os.scandir() as entries:
            return sorted(
                f"{e.name}/" if e.is_dir() else e.name for e in entries if e.name.startswith(word)
            )
    except OSError:
        return []
