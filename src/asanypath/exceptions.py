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
