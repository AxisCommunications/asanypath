# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import subprocess
import sys
from types import SimpleNamespace

import click
import pytest
from click.shell_completion import get_completion_class
from click.testing import CliRunner

import asanypath.click
from asanypath.click import BashComplete, ClickPath
from asanypath.common import DirEntry
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


# --- shell completion ----------------------------------------------------------------


def complete(param_type, incomplete):
    return [(c.value, c.type) for c in param_type.shell_complete(None, None, incomplete)]


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "sub" / "inner").mkdir(parents=True)
    (tmp_path / ".hidden").mkdir()
    (tmp_path / "a.txt").touch()
    (tmp_path / "sub" / "b.txt").touch()
    return tmp_path


@pytest.fixture
def fake_listing(monkeypatch):
    """Replace remote listings: URI -> [(name, is_dir)]; records the URIs listed."""
    listings: dict = {}
    listed: list = []

    def make(uri):
        async def scandir():
            listed.append(uri)
            if isinstance(listings.get(uri), Exception):
                raise listings[uri]
            for name, is_dir in listings.get(uri, ()):
                yield DirEntry(SimpleNamespace(name=name), is_dir=is_dir)

        return SimpleNamespace(scandir=scandir)

    monkeypatch.setattr(asanypath.click, "_AsyncPath", make)
    return listings, listed


def test_complete_remote_directory(tree):
    base = f"file://{tree}/"
    assert complete(ClickPath(), base) == [(f"{base}a.txt", "plain"), (f"{base}sub/", "plain")]
    assert complete(ClickPath(), f"{base}s") == [(f"{base}sub/", "plain")]
    assert complete(ClickPath(), f"{base}sub/in") == [(f"{base}sub/inner/", "plain")]


def test_complete_remote_hidden_only_when_typed(tree):
    assert complete(ClickPath(), f"file://{tree}/.") == [(f"file://{tree}/.hidden/", "plain")]


def test_complete_remote_dirs_only_when_files_not_okay(tree):
    assert complete(ClickPath(file_okay=False), f"file://{tree}/") == [
        (f"file://{tree}/sub/", "plain")
    ]


def test_complete_service_root_lists_containers(fake_listing):
    listings, listed = fake_listing
    listings["s3://"] = [("beta", True), ("alpha", True)]
    assert complete(ClickPath(), "s3://") == [("s3://alpha/", "plain"), ("s3://beta/", "plain")]
    assert complete(ClickPath(), "s3://al") == [("s3://alpha/", "plain")]
    assert listed == ["s3://", "s3://"]


def test_complete_keeps_typed_shorthand(fake_listing):
    # art://repo/... expands to the configured service; replies keep the shorthand.
    listings, _ = fake_listing
    listings["art://repo/"] = [("datasets", True), ("docs", True), ("x", False)]
    assert complete(ClickPath(), "art://repo/d") == [
        ("art://repo/datasets/", "plain"),
        ("art://repo/docs/", "plain"),
    ]


def test_complete_backend_error_is_empty(fake_listing):
    listings, _ = fake_listing
    listings["s3://bucket/"] = PermissionError("denied")
    assert complete(ClickPath(), "s3://bucket/k") == []


def test_complete_scheme_names_with_local_matches(tmp_path, monkeypatch):
    (tmp_path / "src").mkdir()
    monkeypatch.chdir(tmp_path)
    assert complete(ClickPath(), "s") == [
        ("s3://", "plain"),
        ("ssh://", "plain"),
        ("src/", "plain"),
    ]
    assert complete(ClickPath(), "s3:") == [("s3://", "plain")]


@pytest.mark.parametrize("incomplete", ["", "/tmp/x", "./rel"])
def test_complete_local_paths_left_to_the_shell(incomplete):
    assert complete(ClickPath(), incomplete) == [(incomplete, "file")]


def test_bash_completion_class_is_registered():
    assert get_completion_class("bash") is BashComplete


def bash_complete(monkeypatch, words, cword):
    """Run click's bash completion for a two-path command as bash would call it."""

    @click.command()
    @click.argument("src", type=ClickPath())
    @click.argument("dst", type=ClickPath())
    def cmd(src, dst):
        pass

    monkeypatch.setenv("COMP_WORDS", words)
    monkeypatch.setenv("COMP_CWORD", str(cword))
    return BashComplete(cmd, {}, "prog", "_PROG_COMPLETE").complete().splitlines()


def test_bash_rejoins_words_split_at_colon(tree, monkeypatch):
    # bash splits file:///x/s into "file", ":", "///x/s" and replaces only the last.
    out = bash_complete(monkeypatch, f"prog file : //{tree}/s", 3)
    assert out == [f"plain,//{tree}/sub/"]


def test_bash_rejoins_earlier_arguments(tree, monkeypatch):
    words = f"prog file : //{tree}/a.txt file : //{tree}/sub/i"
    assert bash_complete(monkeypatch, words, 6) == [f"plain,//{tree}/sub/inner/"]


def test_bash_reply_right_after_colon(monkeypatch):
    # The cursor right after "file:" makes ":" the current word; readline replaces
    # the empty text after it.
    assert bash_complete(monkeypatch, "prog file :", 2) == ["plain,//"]


def test_bash_rejoins_user_at_host(fake_listing, monkeypatch):
    listings, _ = fake_listing
    listings["ssh://u@host/"] = [("etc", True)]
    out = bash_complete(monkeypatch, "prog ssh : //u @ host/e", 5)
    assert out == ["plain,host/etc/"]


def test_bash_source_calls_complete_exe():
    script = BashComplete(click.Command("prog"), {}, "prog", "_PROG_COMPLETE")
    script._check_version = lambda: None
    assert "_PROG_COMPLETE=bash_complete $1)" in script.source()
    script.complete_exe = "/opt/prog"
    source = script.source()
    assert "_PROG_COMPLETE=bash_complete /opt/prog)" in source
    assert "complete -o nosort -F _prog_completion prog" in source
