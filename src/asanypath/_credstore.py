# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Per-host password cache backed by the OS keyring.

Stores ``"<epoch>|<password>"`` so callers can enforce a TTL on top of the
keyring's own (effectively unlimited) lifetime.

Headless hosts without a Secret Service backend (or any usable backend)
silently no-op: ``load`` returns ``None``, ``save``/``forget`` swallow the
``NoKeyringError``. We refuse to fall back to ``keyrings.alt`` (plaintext
on disk) — re-prompting is strictly safer than leaking.
"""

from __future__ import annotations

from os import getenv
from time import time

try:
    import keyring
    from keyring.errors import NoKeyringError
except ImportError:  # pragma: no cover - keyring is a hard dep, defensive
    keyring = None  # type: ignore[assignment]
    NoKeyringError = Exception  # type: ignore[misc, assignment]


SERVICE = "asanypath"
DEFAULT_TTL_SECONDS = 3600

# Module-level safety knob: tests and CI flip this off.
_ENABLED = getenv("ASANYPATH_CREDSTORE", "1") != "0"


def _ok() -> bool:
    if not _ENABLED or keyring is None:
        return False
    # ``get_keyring()`` returns a ``fail.Keyring`` (or the ``null`` backend)
    # when no real backend is available; their ``priority`` is <= 0. Some
    # backends (e.g. SecretService on a headless host) raise from the
    # ``priority`` property itself when their runtime deps are missing —
    # treat that as "not available".
    try:
        backend = keyring.get_keyring()
        priority = backend.priority
    except Exception:  # noqa: BLE001
        return False
    if priority <= 0:
        return False
    # Refuse the plaintext fallback shipped by ``keyrings.alt``.
    if type(backend).__module__.startswith("keyrings.alt"):
        return False
    return True


def load(key: str, *, ttl: int = DEFAULT_TTL_SECONDS) -> str | None:
    """Return the cached password for ``key`` if still within ``ttl`` seconds."""
    ring = keyring
    if not _ok() or ring is None:
        return None
    try:
        raw = ring.get_password(SERVICE, key)
    except (NoKeyringError, Exception):  # noqa: BLE001
        return None
    if not raw or "|" not in raw:
        forget(key)
        return None
    ts_str, _, password = raw.partition("|")
    try:
        ts = float(ts_str)
    except ValueError:
        forget(key)
        return None
    if (time() - ts) > ttl:
        forget(key)
        return None
    return password


def save(key: str, password: str) -> None:
    """Persist ``password`` for ``key`` with a fresh timestamp."""
    ring = keyring
    if not _ok() or ring is None:
        return
    try:
        ring.set_password(SERVICE, key, f"{time()}|{password}")
    except (NoKeyringError, Exception):  # noqa: BLE001
        pass


def forget(key: str) -> None:
    """Drop any cached entry for ``key``; silent if absent."""
    ring = keyring
    if not _ok() or ring is None:
        return
    try:
        ring.delete_password(SERVICE, key)
    except (NoKeyringError, Exception):  # noqa: BLE001
        pass
