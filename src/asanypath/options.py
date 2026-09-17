# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Operation-scoped configuration for backend requests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

import msgspec


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
