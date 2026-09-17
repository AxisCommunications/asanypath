# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

from ._version import __version__ as __version__
from .artifactory import ArtifactoryPath as ArtifactoryPath
from .asanypath import (
    PROTOCOL_MAP as PROTOCOL_MAP,
)
from .asanypath import (
    AsAnyPath as AsAnyPath,
)
from .asanypath import (
    AsAnyPurePath as AsAnyPurePath,
)
from .asanypath import (
    list_protocols as list_protocols,
)
from .asanypath import (
    register_protocol as register_protocol,
)
from .azure import AzurePath as AzurePath
from .cloud import CloudPathMixin as CloudPathMixin
from .common import CommonPurePathMixin as CommonPurePathMixin
from .exceptions import (
    AsAnyPathError as AsAnyPathError,
)
from .exceptions import (
    InvalidPathError as InvalidPathError,
)
from .exceptions import (
    PathIsADirectoryError as PathIsADirectoryError,
)
from .exceptions import (
    PathNotADirectoryError as PathNotADirectoryError,
)
from .exceptions import (
    PathNotFoundError as PathNotFoundError,
)
from .exceptions import (
    UnsupportedProtocolError as UnsupportedProtocolError,
)
from .ftp import FTPPath as FTPPath
from .ftp import FTPSPath as FTPSPath
from .gcs import GCSPath as GCSPath
from .http import HTTPPath as HTTPPath
from .http import HTTPSPath as HTTPSPath
from .local import AsyncPath as AsyncPath
from .local import SyncPath as SyncPath
from .options import AccessGrant as AccessGrant
from .options import AccessPolicy as AccessPolicy
from .options import AccessPolicyPatch as AccessPolicyPatch
from .options import BackendOptions as BackendOptions
from .s3 import S3Path as S3Path
from .ssh import SSHPath as SSHPath
from .unsupported import UnsupportedProtocolPath as UnsupportedProtocolPath

__all__: list[str]
