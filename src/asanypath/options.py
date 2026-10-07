# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Operation-scoped configuration for backend requests."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Literal

import msgspec


def _env_bytes(name: str, default: int) -> int:
    """Read a positive integer byte count from *name*, else return *default*."""
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


# Bytes held in memory per in-flight upload part/chunk. Objects smaller than this
# upload in a single request; larger ones stream in parts of this size — so this
# is the peak resident memory per upload (parts upload sequentially, no fan-out).
# Tunable via ``ASANYPATH_UPLOAD_CHUNK_SIZE``; also overridable per call through
# ``copy(chunk_size=...)`` and, for writes, ``open(..., buffering=...)``.
UPLOAD_CHUNK_SIZE: int = _env_bytes("ASANYPATH_UPLOAD_CHUNK_SIZE", 8 * 1024 * 1024)

# Bytes an open write handle keeps in memory before spilling to a temp file.
# Tunable via ``ASANYPATH_SPOOL_MAX_SIZE``.
SPOOL_MAX_SIZE: int = _env_bytes("ASANYPATH_SPOOL_MAX_SIZE", 16 * 1024 * 1024)

# S3 rejects multipart parts smaller than 5 MiB (except the last); GCS resumable
# chunks must be 256 KiB-aligned. Backends clamp the effective size accordingly.
S3_MIN_PART_SIZE: int = 5 * 1024 * 1024
GCS_CHUNK_ALIGN: int = 256 * 1024


class BackendOptions(msgspec.Struct, frozen=True, kw_only=True):
    """Provider-native options attached to one backend operation.

    ``headers`` and ``query`` contain request values. ``provider`` contains
    JSON-compatible object-resource fields for backends that support them (GCS).
    Backend-owned authentication and protocol-critical request values cannot be
    overridden.
    """

    headers: Mapping[str, str] = msgspec.field(default_factory=dict)
    query: Mapping[str, str] = msgspec.field(default_factory=dict)
    provider: Mapping[str, Any] = msgspec.field(default_factory=dict)


AccessAction = Literal["read", "write", "execute"]
PolicyPrincipal = str


class AccessGrant(msgspec.Struct, frozen=True):
    """Actions granted to one normalized policy principal."""

    principal: PolicyPrincipal
    actions: frozenset[AccessAction]


class AccessPolicy(msgspec.Struct, frozen=True, kw_only=True):
    """Resource access policy represented by normalized grants."""

    owner: str | None = None
    group: str | None = None
    grants: tuple[AccessGrant, ...] = ()
    provider: Mapping[str, Any] = msgspec.field(default_factory=dict)


class AccessPolicyPatch(msgspec.Struct, frozen=True, kw_only=True):
    """Targeted changes to a resource access policy."""

    owner: str | None = None
    group: str | None = None
    grants: tuple[AccessGrant, ...] = ()
    provider: Mapping[str, Any] = msgspec.field(default_factory=dict)
