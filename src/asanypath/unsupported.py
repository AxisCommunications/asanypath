# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

from pathlib import Path
from typing import Any, NoReturn
from urllib.parse import urlsplit

from anyio import Path as AioPath

from asanypath.common import CommonPurePathMixin
from asanypath.exceptions import UnsupportedProtocolError


class UnsupportedProtocolPath(CommonPurePathMixin):
    """Path-like placeholder for unknown protocols.

    Pure path operations continue to work, while backend and I/O operations
    fail with UnsupportedProtocolError when invoked.
    """

    _asanypath_placeholder = True

    def __init__(
        self,
        part: str | Path | AioPath | CommonPurePathMixin | None = None,
        *parts: str | Path | AioPath | CommonPurePathMixin,
    ) -> None:
        raw = str(part)
        parsed = urlsplit(raw)
        self.protocol = (parsed.scheme or "unsupported").lower()
        super().__init__(part, *parts)

    def _raise_unsupported(self, operation: str) -> NoReturn:
        raise UnsupportedProtocolError(
            f"Unsupported protocol: {self.protocol} (operation: {operation}, path: {self})"
        )

    def __bytes__(self) -> bytes:
        return str(self).encode()

    def __fspath__(self) -> str:
        self._raise_unsupported("__fspath__")

    @classmethod
    def cwd(cls) -> NoReturn:
        raise UnsupportedProtocolError("Unsupported protocol operation: cwd")

    @classmethod
    def home(cls) -> NoReturn:
        raise UnsupportedProtocolError("Unsupported protocol operation: home")

    def __getattr__(self, name: str) -> Any:
        # Reached only after normal MRO lookup fails, i.e. for every backend/I/O
        # operation (pure-path ops live on CommonPurePathMixin and resolve before
        # here). Underscore/dunder names stay genuine AttributeErrors so pickle and
        # getattr(obj, name, default) capability probes keep working.
        if name.startswith("_"):
            raise AttributeError(name)
        self._raise_unsupported(name)
