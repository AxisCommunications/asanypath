# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from asanypath.artifactory import ArtifactoryPath
from asanypath.asanypath import (
    PROTOCOL_MAP,
    AsAnyPath,
    AsAnyPurePath,
    list_protocols,
    register_protocol,
)
from asanypath.azure import AzurePath
from asanypath.cloud import CloudPathMixin
from asanypath.common import CommonPurePathMixin
from asanypath.exceptions import (
    AsAnyPathError,
    InvalidPathError,
    PathIsADirectoryError,
    PathNotADirectoryError,
    PathNotFoundError,
    UnsupportedProtocolError,
)
from asanypath.gcs import GCSPath
from asanypath.local import AsyncPath, SyncPath
from asanypath.options import AccessGrant, AccessPolicy, AccessPolicyPatch, BackendOptions
from asanypath.s3 import S3Path
from asanypath.unsupported import UnsupportedProtocolPath

try:
    from asanypath.ssh import SSHPath  # noqa: F401

    _HAS_SSH = True
except ImportError:  # pragma: no cover
    _HAS_SSH = False

try:
    from asanypath.ftp import FTPPath, FTPSPath  # noqa: F401

    _HAS_FTP = True
except ImportError:  # pragma: no cover
    _HAS_FTP = False

try:
    from asanypath._version import __version__
except ModuleNotFoundError:  # pragma: no cover
    __version__ = "0.0.0-dev"
__all__ = [
    "AsAnyPurePath",
    "AsAnyPath",
    "AsyncPath",
    "SyncPath",
    "BackendOptions",
    "AccessGrant",
    "AccessPolicy",
    "AccessPolicyPatch",
    "S3Path",
    "CloudPathMixin",
    "CommonPurePathMixin",
    "GCSPath",
    "AzurePath",
    "ArtifactoryPath",
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

if _HAS_FTP:
    __all__.extend(["FTPPath", "FTPSPath"])

if _HAS_SSH:
    __all__.append("SSHPath")
