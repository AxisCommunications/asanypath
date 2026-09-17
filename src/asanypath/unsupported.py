# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

from os import PathLike
from pathlib import Path
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
        self._protocol = (parsed.scheme or "unsupported").lower()
        super().__init__(part, *parts)

    @property
    def protocol(self) -> str:
        return self._protocol

    def _raise_unsupported(self, operation: str) -> None:
        raise UnsupportedProtocolError(
            f"Unsupported protocol: {self.protocol} (operation: {operation}, path: {self})"
        )

    def __bytes__(self) -> bytes:
        return str(self).encode()

    def __fspath__(self) -> str:
        self._raise_unsupported("__fspath__")

    def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        self._raise_unsupported("chmod")

    @classmethod
    def cwd(cls):
        raise UnsupportedProtocolError("Unsupported protocol operation: cwd")

    def exists(self) -> bool:
        self._raise_unsupported("exists")

    def expanduser(self):
        self._raise_unsupported("expanduser")

    def glob(self, pattern: str, *, case_sensitive: bool | None = None):
        self._raise_unsupported("glob")

    def group(self) -> str:
        self._raise_unsupported("group")

    def hardlink_to(self, target: str | bytes | PathLike[str | bytes]) -> None:
        self._raise_unsupported("hardlink_to")

    @classmethod
    def home(cls):
        raise UnsupportedProtocolError("Unsupported protocol operation: home")

    def is_block_device(self) -> bool:
        self._raise_unsupported("is_block_device")

    def is_char_device(self) -> bool:
        self._raise_unsupported("is_char_device")

    def is_dir(self) -> bool:
        self._raise_unsupported("is_dir")

    def is_fifo(self) -> bool:
        self._raise_unsupported("is_fifo")

    def is_file(self) -> bool:
        self._raise_unsupported("is_file")

    def is_junction(self) -> bool:
        self._raise_unsupported("is_junction")

    def is_mount(self) -> bool:
        self._raise_unsupported("is_mount")

    def is_socket(self) -> bool:
        self._raise_unsupported("is_socket")

    def is_symlink(self) -> bool:
        self._raise_unsupported("is_symlink")

    def iter_bytes(self, chunk_size: int | None = None):
        self._raise_unsupported("iter_bytes")

    def iterdir(self):
        self._raise_unsupported("iterdir")

    def lchmod(self, mode: int) -> None:
        self._raise_unsupported("lchmod")

    def lstat(self):
        self._raise_unsupported("lstat")

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        self._raise_unsupported("mkdir")

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ):
        self._raise_unsupported("open")

    def owner(self) -> str:
        self._raise_unsupported("owner")

    def read_bytes(self) -> bytes:
        self._raise_unsupported("read_bytes")

    def read_text(
        self,
        encoding: str | None = None,
        errors: str | None = None,
    ) -> str:
        self._raise_unsupported("read_text")

    def readlink(self):
        self._raise_unsupported("readlink")

    def rename(self, target: str | Path | CommonPurePathMixin):
        self._raise_unsupported("rename")

    def replace(self, target: str | Path | CommonPurePathMixin):
        self._raise_unsupported("replace")

    def resolve(self, strict: bool = False):
        self._raise_unsupported("resolve")

    def rglob(self, pattern: str, *, case_sensitive: bool | None = None):
        self._raise_unsupported("rglob")

    def rmdir(self) -> None:
        self._raise_unsupported("rmdir")

    def samefile(self, other_path: str | PathLike[str]) -> bool:
        self._raise_unsupported("samefile")

    def stat(self, *, follow_symlinks: bool = True):
        self._raise_unsupported("stat")

    def symlink_to(
        self,
        target: str | bytes | PathLike[str | bytes] | CommonPurePathMixin,
        target_is_directory: bool = False,
    ) -> None:
        self._raise_unsupported("symlink_to")

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        self._raise_unsupported("touch")

    def unlink(self, missing_ok: bool = False) -> None:
        self._raise_unsupported("unlink")

    def walk(
        self,
        top_down: bool = True,
        on_error=None,
        follow_symlinks: bool = False,
    ):
        self._raise_unsupported("walk")

    def write_bytes(self, data: bytes) -> int:
        self._raise_unsupported("write_bytes")

    def write_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        self._raise_unsupported("write_text")

    def checksums(self) -> dict[str, str]:
        self._raise_unsupported("checksums")
