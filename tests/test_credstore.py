# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for the keyring-backed credential cache."""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from asanypath import _credstore

# Capture the original _ok before any test/fixture patches it.
_REAL_OK = _credstore._ok


@pytest.fixture(autouse=True)
def _ensure_keyring_stub(monkeypatch):
    if _credstore.keyring is not None:
        return
    backend = SimpleNamespace(priority=10)
    stub = SimpleNamespace(
        set_password=lambda *_a, **_k: None,
        get_password=lambda *_a, **_k: None,
        delete_password=lambda *_a, **_k: None,
        get_keyring=lambda: backend,
    )
    monkeypatch.setattr(_credstore, "keyring", stub)


@pytest.fixture
def _ok_enabled(monkeypatch):
    monkeypatch.setattr(_credstore, "_ENABLED", True)
    monkeypatch.setattr(_credstore, "_ok", lambda: True)


def test_save_and_load_roundtrip(monkeypatch, _ok_enabled):
    store: dict[tuple[str, str], str] = {}
    monkeypatch.setattr(
        _credstore.keyring,
        "set_password",
        lambda svc, key, val: store.__setitem__((svc, key), val),
    )
    monkeypatch.setattr(_credstore.keyring, "get_password", lambda svc, key: store.get((svc, key)))
    _credstore.save("k", "secret")
    assert _credstore.load("k") == "secret"


@pytest.mark.parametrize(
    "stored",
    [
        pytest.param(f"{time.time() - 7200}|old-pw", id="expired"),
        pytest.param("no-separator", id="malformed-no-sep"),
        pytest.param("not-a-float|pw", id="malformed-bad-timestamp"),
    ],
)
def test_load_bad_entry_returns_none_and_forgets(monkeypatch, _ok_enabled, stored):
    store: dict[tuple[str, str], str] = {(_credstore.SERVICE, "k"): stored}
    monkeypatch.setattr(_credstore.keyring, "get_password", lambda svc, key: store.get((svc, key)))
    monkeypatch.setattr(
        _credstore.keyring,
        "delete_password",
        lambda svc, key: store.pop((svc, key), None),
    )
    assert _credstore.load("k", ttl=3600) is None
    assert (_credstore.SERVICE, "k") not in store


def test_disabled_via_env(monkeypatch):
    monkeypatch.setattr(_credstore, "_ENABLED", False)
    assert _REAL_OK() is False
    assert _credstore.load("k") is None
    _credstore.save("k", "v")  # no-op, must not raise
    _credstore.forget("k")  # no-op, must not raise


class _PriorityRaises:
    @property
    def priority(self):
        raise RuntimeError("no dbus")


class _NullBackend:
    priority = 0


class _AltBackend:
    priority = 5


_AltBackend.__module__ = "keyrings.alt.file"


@pytest.mark.parametrize(
    "backend_factory",
    [
        pytest.param(_PriorityRaises, id="priority-raises"),
        pytest.param(_NullBackend, id="priority-zero"),
        pytest.param(_AltBackend, id="keyrings.alt-plaintext"),
    ],
)
def test_ok_false_for_unusable_backend(monkeypatch, backend_factory):
    monkeypatch.setattr(_credstore, "_ENABLED", True)
    monkeypatch.setattr(_credstore.keyring, "get_keyring", lambda: backend_factory())
    assert _REAL_OK() is False


def test_load_swallows_keyring_errors(monkeypatch, _ok_enabled):
    def _boom(*_a, **_k):
        raise RuntimeError("backend dead")

    monkeypatch.setattr(_credstore.keyring, "get_password", _boom)
    assert _credstore.load("k") is None


def test_save_swallows_keyring_errors(monkeypatch, _ok_enabled):
    def _boom(*_a, **_k):
        raise RuntimeError("backend dead")

    monkeypatch.setattr(_credstore.keyring, "set_password", _boom)
    _credstore.save("k", "v")  # must not raise
