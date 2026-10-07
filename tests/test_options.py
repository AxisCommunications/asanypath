# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Upload tuning configuration (env-var parsing)."""

from __future__ import annotations

import pytest

from asanypath.options import _env_bytes


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, 777),  # unset -> default
        ("", 777),  # empty -> default
        ("1048576", 1048576),  # explicit bytes
        ("0", 777),  # non-positive -> default
        ("-5", 777),  # negative -> default
        ("notanumber", 777),  # invalid -> default
    ],
)
def test_env_bytes(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("ASANYPATH_TEST_KNOB", raising=False)
    else:
        monkeypatch.setenv("ASANYPATH_TEST_KNOB", value)
    assert _env_bytes("ASANYPATH_TEST_KNOB", 777) == expected
