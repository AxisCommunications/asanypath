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

Requires click, e.g. through the ``cli`` extra: ``asanypath[cli]``.
"""

from __future__ import annotations

import os
from typing import Any

try:
    import click
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "asanypath.click requires click; install asanypath with the 'cli' extra"
    ) from exc

from asanypath.exceptions import AsAnyPathError
from asanypath.local import SyncPath
from asanypath.sync import AsAnyPath

__all__ = ["ClickPath"]


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
    """

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
