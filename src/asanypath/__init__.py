# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from importlib import import_module
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any

from asanypath.asanypath import (
    PROTOCOL_MAP,
    AsAnyPath,
    AsAnyPurePath,
    list_protocols,
    register_protocol,
)
from asanypath.exceptions import (
    AsAnyPathError,
    InvalidPathError,
    PathIsADirectoryError,
    PathNotADirectoryError,
    PathNotFoundError,
    UnsupportedProtocolError,
)
from asanypath.unsupported import UnsupportedProtocolPath

if TYPE_CHECKING:
    from asanypath._transfer import TransferBudget
    from asanypath.artifactory import ArtifactoryPath
    from asanypath.azure import AzurePath
    from asanypath.cache import configure_cache
    from asanypath.cloud import CloudPathMixin
    from asanypath.common import CommonPurePathMixin, DirEntry
    from asanypath.ftp import FTPPath, FTPSPath  # noqa: F401
    from asanypath.gcs import GCSPath
    from asanypath.local import AsyncPath, SyncPath
    from asanypath.options import AccessGrant, AccessPolicy, AccessPolicyPatch, BackendOptions
    from asanypath.s3 import S3Path
    from asanypath.ssh import SSHPath

try:
    from asanypath._version import __version__
except ModuleNotFoundError:  # pragma: no cover
    __version__ = "0.0.0-dev"

# Backends load on first attribute access, so importing one never pays for all.
_LAZY_EXPORTS = {
    "TransferBudget": "asanypath._transfer",
    "ArtifactoryPath": "asanypath.artifactory",
    "AzurePath": "asanypath.azure",
    "configure_cache": "asanypath.cache",
    "CloudPathMixin": "asanypath.cloud",
    "CommonPurePathMixin": "asanypath.common",
    "DirEntry": "asanypath.common",
    "FTPPath": "asanypath.ftp",
    "FTPSPath": "asanypath.ftp",
    "GCSPath": "asanypath.gcs",
    "AsyncPath": "asanypath.local",
    "SyncPath": "asanypath.local",
    "AccessGrant": "asanypath.options",
    "AccessPolicy": "asanypath.options",
    "AccessPolicyPatch": "asanypath.options",
    "BackendOptions": "asanypath.options",
    "S3Path": "asanypath.s3",
    "SSHPath": "asanypath.ssh",
}


def __getattr__(name: str) -> Any:
    module = _LAZY_EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY_EXPORTS})


__all__ = [
    "AsAnyPurePath",
    "AsAnyPath",
    "AsyncPath",
    "SyncPath",
    "TransferBudget",
    "BackendOptions",
    "AccessGrant",
    "AccessPolicy",
    "AccessPolicyPatch",
    "S3Path",
    "CloudPathMixin",
    "CommonPurePathMixin",
    "DirEntry",
    "GCSPath",
    "AzurePath",
    "ArtifactoryPath",
    "SSHPath",
    "configure_cache",
    "register_protocol",
    "list_protocols",
    "AsAnyPathError",
    "InvalidPathError",
    "UnsupportedProtocolError",
    "UnsupportedProtocolPath",
    "PathNotFoundError",
    "PathIsADirectoryError",
    "PathNotADirectoryError",
    "PROTOCOL_MAP",
]

if find_spec("aioftp") is not None:
    __all__.extend(["FTPPath", "FTPSPath"])
