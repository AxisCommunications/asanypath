# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from os import PathLike, getenv, stat_result
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from typing import Self

from anyio import AsyncFile
from anyio import Path as AioPath

from asanypath.exceptions import InvalidPathError
from asanypath.unsupported import UnsupportedProtocolPath

if TYPE_CHECKING:
    from asanypath.options import AccessPolicy, AccessPolicyPatch, BackendOptions


class _LazyDict(dict):
    def _ensure(self):
        if not self:
            from asanypath.artifactory import ArtifactoryPath
            from asanypath.azure import AzurePath
            from asanypath.gcs import GCSPath
            from asanypath.http import HTTPPath, HTTPSPath
            from asanypath.local import AsyncPath
            from asanypath.s3 import S3Path

            self.update(
                file=AsyncPath,
                s3=S3Path,
                gs=GCSPath,
                az=AzurePath,
                http=HTTPPath,
                https=HTTPSPath,
                art=ArtifactoryPath,
            )
            try:
                from asanypath.ssh import SSHPath

                self.update(ssh=SSHPath)
            except ImportError:
                pass
            try:
                from asanypath.ftp import FTPPath, FTPSPath

                self.update(ftp=FTPPath, ftps=FTPSPath)
            except ImportError:
                pass

    def items(self):
        self._ensure()
        return super().items()

    def __iter__(self):
        self._ensure()
        return super().__iter__()

    def __contains__(self, key):
        self._ensure()
        return super().__contains__(key)

    def __missing__(self, key):
        self._ensure()
        if key not in self:
            return UnsupportedProtocolPath
        return self[key]  # KeyError if truly unknown


PROTOCOL_MAP = _LazyDict()


def _match_base_url_flavor(url: str) -> type | None:
    """Check if an HTTP(S) URL matches any backend's configured BASE_URL."""
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower() or "file"

    if scheme in ("http", "https"):
        url_host_path = f"{parsed.netloc}{parsed.path}".rstrip("/")
        for proto, klass in PROTOCOL_MAP.items():
            if proto in ("http", "https", "file"):
                continue

            env_names = [
                f"{proto.upper()}_BASE_URL",
                *getattr(klass, "_BASE_URL_ENV_ALIASES", {}).get(proto, ()),
            ]
            for base_url in filter(None, map(getenv, env_names)):
                bp = urlsplit(base_url if "://" in base_url else f"https://{base_url}")
                if not bp.netloc:
                    break
                bp_path = bp.path.strip("/")
                prefix = f"{bp.netloc}/{bp_path}" if bp_path else bp.netloc
                if url_host_path == prefix or url_host_path.startswith(f"{prefix}/"):
                    return klass
    return PROTOCOL_MAP[scheme]


_T = TypeVar("_T", bound="AsAnyPurePath")


class AsAnyPurePath(ABC):
    """Virtual superclass for pure path manipulation (no I/O).

    Similar to pathlib.PurePath, this provides only path string manipulation
    methods that do not access the filesystem. Concrete implementations can
    share a common implementation for these methods via CloudPathMixin or similar.

    Dispatches to the appropriate concrete implementation based on the URL protocol.
    """

    def __new__(
        cls,
        arg: str | Path | AioPath | _T | None = ".",
        *args: str | Path | AioPath | _T,
        **kwargs,
    ) -> _T:
        if arg is None:
            raise InvalidPathError("expected str, bytes or os.PathLike object, not NoneType")
        flavor = _match_base_url_flavor(str(arg))
        # Skip check for local paths — they delegate to pathlib dynamically.
        from asanypath.local import AsyncPath, SyncPath

        if flavor not in (AsyncPath, SyncPath) and not getattr(
            flavor, "_asanypath_placeholder", False
        ):
            if missing := "\n* ".join(cls.__abstractmethods__ - set(dir(flavor))):
                raise NotImplementedError(
                    f"{flavor.__name__} is missing implementations for methods:\n* {missing}"
                )
        return flavor(arg, *args, **kwargs) if kwargs else flavor(arg, *args)

    @classmethod
    def __subclasshook__(cls, sub: type) -> bool:
        from asanypath.local import SyncPath

        return sub in [*PROTOCOL_MAP.values(), SyncPath]

    @abstractmethod
    def __rtruediv__(self, other: str | PathLike[str]) -> Self:
        pass

    @abstractmethod
    def __truediv__(self, other: str | PathLike[str]) -> Self:
        pass

    @property
    @abstractmethod
    def anchor(self) -> str:
        pass

    @abstractmethod
    def as_posix(self) -> str:
        pass

    @abstractmethod
    def as_uri(self) -> str:
        pass

    @property
    @abstractmethod
    def drive(self) -> str:
        pass

    @abstractmethod
    def full_match(self, pattern: str, *, case_sensitive: bool | None = None) -> bool:
        pass

    @abstractmethod
    def is_absolute(self) -> bool:
        pass

    @abstractmethod
    def is_relative_to(self, other: str | PathLike[str]) -> bool:
        pass

    @abstractmethod
    def joinpath(self, *args: str | PathLike[str]) -> Self:
        pass

    @abstractmethod
    def match(self, pattern: str, *, case_sensitive: bool | None = None) -> bool:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def parent(self) -> Self:
        pass

    @property
    @abstractmethod
    def parents(self) -> tuple[Self, ...]:
        pass

    @property
    @abstractmethod
    def parts(self) -> tuple[str, ...]:
        pass

    @abstractmethod
    def relative_to(self, *other: str | PathLike[str], walk_up: bool = False) -> Self:
        pass

    @property
    @abstractmethod
    def root(self) -> str:
        pass

    @property
    @abstractmethod
    def stem(self) -> str:
        pass

    @property
    @abstractmethod
    def suffix(self) -> str:
        pass

    @property
    @abstractmethod
    def suffixes(self) -> list[str]:
        pass

    @abstractmethod
    def with_name(self, name: str) -> Self:
        pass

    @abstractmethod
    def with_parent(self, parent: str | PathLike[str] | Self) -> Self:
        pass

    @abstractmethod
    def with_segments(self, *pathsegments: str | PathLike[str] | Self) -> Self:
        pass

    @abstractmethod
    def with_stem(self, stem: str) -> Self:
        pass

    @abstractmethod
    def with_suffix(self, suffix: str) -> Self:
        pass


class AsAnyPath(AsAnyPurePath):
    """Virtual superclass for all AsAnyPath implementations.

    Similar to pathlib.Path, this extends AsAnyPurePath with filesystem I/O methods.
    Exposes the full interface of all path implementations, and dispatches to the
    appropriate implementation based on the protocol of the input path.
    """

    @abstractmethod
    def __bytes__(self) -> bytes:
        pass

    @abstractmethod
    def __fspath__(self) -> str:
        pass

    @abstractmethod
    def absolute(self) -> Self:
        pass

    @abstractmethod
    def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        pass

    @classmethod
    @abstractmethod
    def cwd(cls) -> Self:
        pass

    @abstractmethod
    async def exists(self) -> bool:
        pass

    @abstractmethod
    def expanduser(self) -> Self:
        pass

    @abstractmethod
    async def get_access_policy(
        self, *, backend_options: BackendOptions | None = None
    ) -> AccessPolicy:
        """Return access controls managed directly by this backend."""
        pass

    @abstractmethod
    async def glob(
        self, pattern: str, *, case_sensitive: bool | None = None
    ) -> AsyncIterator[Self]:
        pass

    @abstractmethod
    def group(self) -> str:
        pass

    @abstractmethod
    def hardlink_to(self, target: str | bytes | PathLike[str | bytes]) -> None:
        pass

    @classmethod
    @abstractmethod
    def home(cls) -> Self:
        pass

    @abstractmethod
    def is_block_device(self) -> bool:
        pass

    @abstractmethod
    def is_char_device(self) -> bool:
        pass

    @abstractmethod
    async def is_dir(self) -> bool:
        pass

    @abstractmethod
    def is_fifo(self) -> bool:
        pass

    @abstractmethod
    async def is_file(self) -> bool:
        pass

    @abstractmethod
    def is_junction(self) -> bool:
        pass

    @abstractmethod
    def is_mount(self) -> bool:
        pass

    @abstractmethod
    def is_socket(self) -> bool:
        pass

    @abstractmethod
    def is_symlink(self) -> bool:
        pass

    @abstractmethod
    async def iterdir(self) -> AsyncIterator[Self]:
        pass

    @abstractmethod
    def lchmod(self, mode: int) -> None:
        pass

    @abstractmethod
    def lstat(self) -> stat_result:
        pass

    @abstractmethod
    async def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        pass

    @abstractmethod
    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        *,
        backend_options: BackendOptions | None = None,
    ) -> AsyncFile[Any]:
        pass

    @abstractmethod
    def owner(self) -> str:
        pass

    @abstractmethod
    async def read_bytes(self) -> bytes:
        pass

    @abstractmethod
    async def iter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        """Yield file contents in byte chunks (``chunk_size=None`` uses a default)."""
        ...

    @abstractmethod
    async def read_text(
        self,
        encoding: str | None = None,
        errors: str | None = None,
    ) -> str:
        pass

    @abstractmethod
    def readlink(self) -> Self:
        pass

    @abstractmethod
    async def rename(
        self,
        target: str | Path | Self,
        *,
        force: bool = False,
        destination_backend_options: BackendOptions | None = None,
    ) -> Self:
        pass

    @abstractmethod
    async def replace(
        self,
        target: str | Path | Self,
        *,
        destination_backend_options: BackendOptions | None = None,
    ) -> Self:
        pass

    @abstractmethod
    def resolve(self, strict: bool = False) -> Self:
        pass

    @abstractmethod
    async def rglob(
        self, pattern: str, *, case_sensitive: bool | None = None
    ) -> AsyncIterator[Self]:
        pass

    @abstractmethod
    async def rmdir(self) -> None:
        pass

    @abstractmethod
    def samefile(self, other_path: str | PathLike[str]) -> bool:
        pass

    @abstractmethod
    async def stat(self, *, follow_symlinks: bool = True) -> stat_result:
        pass

    @abstractmethod
    def symlink_to(
        self,
        target: str | bytes | PathLike[str | bytes] | Self,
        target_is_directory: bool = False,
    ) -> None:
        pass

    @abstractmethod
    async def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        pass

    @abstractmethod
    async def unlink(self, missing_ok: bool = False) -> None:
        pass

    @abstractmethod
    async def update_access_policy(
        self,
        policy_patch: AccessPolicyPatch,
        *,
        backend_options: BackendOptions | None = None,
    ) -> None:
        """Apply targeted access-control changes managed by this backend."""
        pass

    @abstractmethod
    async def walk(
        self,
        top_down: bool = True,
        on_error: Callable[[OSError], object] | None = None,
        follow_symlinks: bool = False,
    ) -> AsyncIterator[tuple[Self, list[str], list[str]]]:
        pass

    @abstractmethod
    async def write_bytes(
        self, data: bytes, *, backend_options: BackendOptions | None = None
    ) -> int:
        pass

    @abstractmethod
    async def write_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        *,
        backend_options: BackendOptions | None = None,
    ) -> int:
        pass

    @abstractmethod
    async def checksums(self) -> dict[str, str]:
        pass


def register_protocol(protocol: str, path_class: type[AsAnyPurePath]) -> None:
    """
    Register a custom protocol handler.

    This allows users to extend AsAnyPath with custom cloud storage providers
    or other path implementations.

    Args:
        protocol: The protocol scheme (e.g., 'custom', 'mycloud')
        path_class: A AsAnyPurePath (or AsAnyPath) subclass to handle this protocol

    Raises:
        ValueError: If protocol is invalid or path_class is not a AsAnyPurePath subclass

    Example:
        >>> class MyCloudPath(AsAnyPath):
        ...     # implementation
        ...     pass
        >>> register_protocol("mycloud", MyCloudPath)
        >>> p = AsAnyPath("mycloud://bucket/key")
    """
    if not protocol:
        raise ValueError("Protocol cannot be empty")

    if not isinstance(path_class, type) or not issubclass(path_class, AsAnyPurePath):
        raise ValueError(f"{path_class} must be a subclass of AsAnyPurePath")

    PROTOCOL_MAP[protocol.lower()] = path_class


def list_protocols() -> list[str]:
    """
    List all registered protocols.

    Returns:
        A sorted list of all registered protocol schemes
    """
    return sorted(set(PROTOCOL_MAP))
