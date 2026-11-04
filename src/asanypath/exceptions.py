# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Exceptions for AsAnyPath."""


class AsAnyPathError(Exception):
    """Base exception for AsAnyPath."""

    pass


class InvalidPathError(AsAnyPathError, TypeError):
    """Raised when a path argument is invalid."""

    pass


class UnsupportedProtocolError(AsAnyPathError):
    """Raised when a protocol is not supported."""

    pass


class PathNotFoundError(AsAnyPathError, FileNotFoundError):
    """Raised when a path does not exist."""

    pass


class PathIsADirectoryError(AsAnyPathError, IsADirectoryError):
    """Raised when a file operation is attempted on a directory."""

    pass


class PathNotADirectoryError(AsAnyPathError, NotADirectoryError):
    """Raised when a directory operation is attempted on a file."""

    pass


class AuthenticationError(AsAnyPathError, PermissionError):
    """Raised when a service rejects or requires credentials (HTTP 401).

    ``uri`` is the path the request was for. When the backend recorded where
    its credentials for that service came from, the message says so.
    """

    def __init__(self, message: str, uri: str | None = None) -> None:
        hint = credential_hint(uri) if uri else None
        super().__init__(f"{message}; {hint}" if hint else message)
        self.uri = uri


# Hint per service prefix (e.g. "art://host/artifactory") naming where the
# credentials sent to it came from; set when a path resolves its credentials.
_CREDENTIAL_HINTS: dict[str, str] = {}


def set_credential_hint(service: str, hint: str) -> None:
    """Record where the credentials for URIs under ``service`` come from."""
    _CREDENTIAL_HINTS[service.rstrip("/")] = hint


def credential_hint(uri: str) -> str | None:
    """The recorded hint for the longest service prefix of ``uri``."""
    matches = [s for s in _CREDENTIAL_HINTS if uri == s or uri.startswith(f"{s}/")]
    if matches:
        return _CREDENTIAL_HINTS[max(matches, key=len)]
    if uri.startswith(("http://", "https://")) and "/artifactory/" in uri:
        return (
            "if this is Artifactory, add its host to ARTIFACTORY_BASE_URL so the "
            "Artifactory token is sent"
        )
    return None
