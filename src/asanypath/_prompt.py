# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Whether backends may prompt the user on the terminal."""

from __future__ import annotations

import sys
from os import getenv


def interactive() -> bool:
    """Whether a backend may prompt the user (credentials, host keys)."""
    if getenv("ASANYPATH_INTERACTIVE") == "0":
        return False
    try:
        return sys.stdin.isatty()
    except (AttributeError, ValueError):  # pragma: no cover
        return False  # pragma: no cover
