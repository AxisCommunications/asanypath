# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import subprocess
import sys

import click
import pytest
from click.testing import CliRunner

from asanypath.click import ClickPath
from asanypath.local import SyncPath
from asanypath.s3 import S3Path
from asanypath.sync import AsAnyPath


def invoke(param_type, *args):
    """Run a command taking any number of ``param_type`` arguments; return (result, values)."""
    seen = {}

    @click.command()
    @click.argument("paths", nargs=-1, type=param_type)
    def cmd(paths):
        seen["paths"] = paths

    result = CliRunner().invoke(cmd, list(args))
    return result, seen.get("paths")


class FakeStore(dict):
    """Maps URI -> (is_file, is_dir) and records the backend calls made."""

    def __init__(self):
        super().__init__()
        self.calls = []


@pytest.fixture
def fake_s3(monkeypatch):
    store = FakeStore()

    def fake(method):
        async def check(self):
            store.calls.append((method, str(self)))
            is_file, is_dir = store.get(str(self), (False, False))
            return {"exists": is_file or is_dir, "is_file": is_file, "is_dir": is_dir}[method]

        return check

    for method in ("exists", "is_file", "is_dir"):
        monkeypatch.setattr(S3Path, method, fake(method))
    return store


# --- local paths: same behaviour as click.Path ---------------------------------------


def test_local_returns_syncpath(tmp_path):
    (tmp_path / "f.txt").write_text("x")
    result, paths = invoke(ClickPath(exists=True), str(tmp_path / "f.txt"))
    assert result.exit_code == 0, result.output
    assert isinstance(paths[0], SyncPath)
    assert isinstance(paths[0], AsAnyPath)
    assert paths[0] == SyncPath(tmp_path / "f.txt")


def test_local_missing_with_exists_fails(tmp_path):
    result, _ = invoke(ClickPath(exists=True), str(tmp_path / "missing"))
    assert result.exit_code == 2
    assert "does not exist" in result.output


def test_local_missing_without_exists_is_accepted(tmp_path):
    result, paths = invoke(ClickPath(), str(tmp_path / "missing"))
    assert result.exit_code == 0, result.output
    assert paths[0] == SyncPath(tmp_path / "missing")


def test_local_kind_checks(tmp_path):
    (tmp_path / "f.txt").write_text("x")
    result, _ = invoke(ClickPath(file_okay=False), str(tmp_path / "f.txt"))
    assert result.exit_code == 2
    assert "is a file" in result.output
    result, _ = invoke(ClickPath(dir_okay=False), str(tmp_path))
    assert result.exit_code == 2
    assert "is a directory" in result.output


def test_local_file_uri(tmp_path):
    (tmp_path / "f.txt").write_text("x")
    result, paths = invoke(ClickPath(exists=True, dir_okay=False), (tmp_path / "f.txt").as_uri())
    assert result.exit_code == 0, result.output
    assert paths[0] == SyncPath(tmp_path / "f.txt")


def test_local_resolve_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result, paths = invoke(ClickPath(resolve_path=True), "rel/x")
    assert result.exit_code == 0, result.output
    assert paths[0] == SyncPath(tmp_path / "rel" / "x")


def test_local_dash():
    result, paths = invoke(ClickPath(allow_dash=True), "-")
    assert result.exit_code == 0, result.output
    assert str(paths[0]) == "-"


# --- remote paths ----------------------------------------------------------------------


def test_remote_returns_asanypath(fake_s3):
    fake_s3["s3://bucket/key"] = (True, False)
    result, paths = invoke(ClickPath(exists=True), "s3://bucket/key")
    assert result.exit_code == 0, result.output
    assert isinstance(paths[0], AsAnyPath)
    assert str(paths[0]) == "s3://bucket/key"


def test_remote_missing_with_exists_fails(fake_s3):
    result, _ = invoke(ClickPath(exists=True), "s3://bucket/missing")
    assert result.exit_code == 2
    assert "'s3://bucket/missing' does not exist" in result.output


def test_remote_missing_without_exists_is_accepted(fake_s3):
    result, paths = invoke(ClickPath(file_okay=False), "s3://bucket/new/")
    assert result.exit_code == 0, result.output
    assert str(paths[0]) == "s3://bucket/new"


def test_remote_file_rejected_when_file_not_okay(fake_s3):
    fake_s3["s3://bucket/key"] = (True, False)
    result, _ = invoke(ClickPath(file_okay=False), "s3://bucket/key")
    assert result.exit_code == 2
    assert "Directory 's3://bucket/key' is a file" in result.output


def test_remote_dir_rejected_when_dir_not_okay(fake_s3):
    fake_s3["s3://bucket/prefix"] = (False, True)
    result, _ = invoke(ClickPath(dir_okay=False), "s3://bucket/prefix")
    assert result.exit_code == 2
    assert "File 's3://bucket/prefix' is a directory" in result.output


@pytest.mark.parametrize("kwargs", [{"file_okay": False}, {"dir_okay": False}])
def test_remote_key_that_is_file_and_dir_is_accepted(fake_s3, kwargs):
    fake_s3["s3://bucket/both"] = (True, True)
    result, _ = invoke(ClickPath(exists=True, **kwargs), "s3://bucket/both")
    assert result.exit_code == 0, result.output


def test_remote_exists_only_costs_one_request(fake_s3):
    fake_s3["s3://bucket/key"] = (True, False)
    result, _ = invoke(ClickPath(exists=True), "s3://bucket/key")
    assert result.exit_code == 0, result.output
    assert fake_s3.calls == [("exists", "s3://bucket/key")]


def test_remote_backend_error(monkeypatch):
    async def broken(self):
        raise RuntimeError("no credentials")

    monkeypatch.setattr(S3Path, "exists", broken)
    result, paths = invoke(ClickPath(), "s3://bucket/key")
    assert result.exit_code == 0, result.output
    assert str(paths[0]) == "s3://bucket/key"

    result, _ = invoke(ClickPath(exists=True), "s3://bucket/key")
    assert result.exit_code == 2
    assert "could not be checked: no credentials" in result.output


def test_unsupported_protocol():
    result, paths = invoke(ClickPath(), "nope://host/x")
    assert result.exit_code == 0, result.output
    assert str(paths[0]) == "nope://host/x"

    result, _ = invoke(ClickPath(exists=True), "nope://host/x")
    assert result.exit_code == 2
    assert "Unsupported protocol: nope" in result.output


@pytest.mark.parametrize(
    "path_type, expected", [(str, "s3://bucket/key"), (bytes, b"s3://bucket/key")]
)
def test_remote_path_type(fake_s3, path_type, expected):
    result, paths = invoke(ClickPath(path_type=path_type), "s3://bucket/key")
    assert result.exit_code == 0, result.output
    assert paths[0] == expected


def test_mixed_local_and_remote(fake_s3, tmp_path):
    fake_s3["s3://bucket/key"] = (True, False)
    result, paths = invoke(ClickPath(exists=True), str(tmp_path), "s3://bucket/key")
    assert result.exit_code == 0, result.output
    assert [str(p) for p in paths] == [str(tmp_path), "s3://bucket/key"]


def test_default_value_is_converted(fake_s3):
    fake_s3["s3://bucket/default"] = (False, True)
    seen = {}

    @click.command()
    @click.option(
        "--out", type=ClickPath(exists=True, file_okay=False), default="s3://bucket/default"
    )
    def cmd(out):
        seen["out"] = out

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code == 0, result.output
    assert isinstance(seen["out"], AsAnyPath)
    assert str(seen["out"]) == "s3://bucket/default"


def test_import_asanypath_does_not_import_click():
    out = subprocess.run(
        [sys.executable, "-c", "import sys, asanypath; print('click' in sys.modules)"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert out == "False"
