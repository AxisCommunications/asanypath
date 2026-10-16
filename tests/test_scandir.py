# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""scandir(): one listing call, then concurrent probes only for unknown fields."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from rich.text import Text

from asanypath import AsyncPath, DirEntry, SyncPath
from asanypath.cli import _columns
from asanypath.common import CommonPurePathMixin, parse_listing_mtime


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "sub").mkdir()
    (tmp_path / "f.txt").write_text("abc")
    (tmp_path / "broken").symlink_to(tmp_path / "missing")
    return tmp_path


@pytest.mark.parametrize(
    "text,expected",
    [
        ("2024-01-02T03:04:05.000Z", 1704164645.0),
        ("Tue, 02 Jan 2024 03:04:05 GMT", 1704164645.0),
        ("not a date", None),
        ("bad GMT", None),
        (None, None),
    ],
)
def test_parse_listing_mtime(text, expected):
    assert parse_listing_mtime(text) == expected


async def test_local_scandir(tree):
    entries = {e.path.name: e async for e in AsyncPath(tree).scandir(stat=True)}
    assert isinstance(entries["f.txt"].path, AsyncPath)
    assert entries["f.txt"][1:3] == (False, 3)
    assert entries["f.txt"].mtime is not None
    assert entries["sub"].is_dir is True
    # A dangling symlink is listed as a non-directory without metadata.
    assert entries["broken"][1:] == (False, None, None)


async def test_local_scandir_without_stat(tree):
    entries = {e.path.name: e async for e in AsyncPath(tree).scandir()}
    assert entries["f.txt"] == DirEntry(entries["f.txt"].path, False)


def test_sync_local_scandir(tree):
    entries = {e.path.name: e for e in SyncPath(tree).scandir(stat=True)}
    assert isinstance(entries["f.txt"].path, SyncPath)
    assert entries["f.txt"].size == 3
    assert entries["sub"].is_dir is True


class _Listed(CommonPurePathMixin):
    """Backend whose listing knows only child paths (the default _list_entries)."""

    protocol = "mem"

    def __init__(self, children):
        self._children = children

    async def iterdir(self):
        for child in self._children:
            yield child


def _child(is_dir=None, stat=None):
    child = MagicMock()
    child.is_dir = AsyncMock(return_value=is_dir) if is_dir is not None else AsyncMock()
    child.stat = AsyncMock(return_value=stat)
    return child


async def test_default_listing_probes_missing_fields():
    file = _child(False, SimpleNamespace(st_size=5, st_mtime=7.0))
    folder = _child(True)
    entries = [e async for e in _Listed([file, folder]).scandir(stat=True)]
    assert entries == [DirEntry(file, False, 5, 7.0), DirEntry(folder, True)]
    folder.stat.assert_not_awaited()


async def test_probe_failures_leave_fields_unknown():
    child = _child()
    child.is_dir.side_effect = NotImplementedError
    child.stat.side_effect = PermissionError
    entries = [e async for e in _Listed([child]).scandir(stat=True)]
    assert entries == [DirEntry(child)]


async def test_complete_entries_skip_probes():
    child = _child()

    class Complete(_Listed):
        async def _list_entries(self, *, stat=False, fresh=False):
            return [DirEntry(child, False, 1, 2.0)]

    assert [e async for e in Complete([]).scandir(stat=True)] == [DirEntry(child, False, 1, 2.0)]
    child.is_dir.assert_not_awaited()
    child.stat.assert_not_awaited()


@pytest.mark.parametrize(
    "width,expected",
    [
        (80, "a  bbb  ccc  dd  e"),
        (14, "a    ccc  e\nbbb  dd"),
        (8, "a    dd\nbbb  e\nccc"),
        (3, "a\nbbb\nccc\ndd\ne"),
    ],
)
def test_columns_fill_column_major(width, expected):
    names = [Text(n) for n in ("a", "bbb", "ccc", "dd", "e")]
    assert _columns(names, width).plain == expected
