# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for the CLI module."""

from __future__ import annotations

import importlib.util
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

import asanypath.cli as cli_module
from asanypath.cli import cli

_HAS_ASYNCSSH = importlib.util.find_spec("asyncssh") is not None


@pytest.fixture
def runner():
    """Provide a Click CLI test runner."""
    return CliRunner()


# ---------------------------------------------------------------------------
# protocols
# ---------------------------------------------------------------------------


def test_protocols_lists_all_backends(runner):
    result = runner.invoke(cli, ["protocols"])
    assert result.exit_code == 0
    assert "s3" in result.output
    assert "gs" in result.output
    assert "az" in result.output
    assert "art" in result.output


def test_completion_bash_outputs_script(runner):
    result = runner.invoke(cli, ["completion", "bash", "--exe", "/tmp/asanypath-bin"])
    assert result.exit_code == 0
    assert "_asanypath_completion()" in result.output
    assert "_ASANYPATH_COMPLETE=bash_complete /tmp/asanypath-bin" in result.output
    assert "complete -o nosort -F _asanypath_completion asanypath" in result.output


async def test_complete_path_prefix_lists_containers(monkeypatch):
    """At a service root, completion uses the container fast path (no iterdir)."""

    class _FakeRoot:
        @property
        def parent(self):
            return self

        async def _list_containers(self):
            return ["s3://beta", "s3://alpha/"]

        async def iterdir(self):
            raise AssertionError("iterdir must not run at container root")
            yield  # pragma: no cover

    monkeypatch.setattr(cli_module, "AsAnyPath", lambda _prefix: _FakeRoot())
    out = await cli_module._complete_path_prefix_async("s3://")
    assert out == ["s3://alpha/", "s3://beta/"]


def test_path_complete_cloud_returns_canonical_suggestions(monkeypatch):
    monkeypatch.setattr(
        cli_module,
        "_complete_cloud_path",
        lambda _: [
            "s3://cta-dev/mikpen/subdir",
            "s3://cta-dev/mikpen/test.txt",
            "s3://cta-dev/mikpen/test2.txt",
        ],
    )

    out = cli_module._path_complete(MagicMock(), MagicMock(), "s3://cta-dev/mikpen/")

    assert out == [
        "s3://cta-dev/mikpen/subdir",
        "s3://cta-dev/mikpen/test.txt",
        "s3://cta-dev/mikpen/test2.txt",
    ]


def test_path_complete_cloud_filters_by_typed_prefix(monkeypatch):
    monkeypatch.setattr(
        cli_module,
        "_complete_cloud_path",
        lambda _: [
            "s3://cta-dev/mikpen/subdir/",
            "s3://cta-dev/mikpen/test.txt",
            "s3://cta-dev/mikpen/test2.txt",
        ],
    )

    out = cli_module._path_complete(MagicMock(), MagicMock(), "s3://cta-dev/mikpen/sub")

    assert out == ["s3://cta-dev/mikpen/subdir/"]


def test_path_complete_cloud_keeps_directory_suffix(monkeypatch):
    monkeypatch.setattr(
        cli_module,
        "_complete_cloud_path",
        lambda _: [
            "s3://cta-dev/mikpen/subdir/",
            "s3://cta-dev/mikpen/test.txt",
        ],
    )

    out = cli_module._path_complete(MagicMock(), MagicMock(), "s3://cta-dev/mikpen/sub")

    assert out == ["s3://cta-dev/mikpen/subdir/"]


def test_path_complete_cloud_matches_canonicalized_prefix(monkeypatch):
    monkeypatch.setattr(
        cli_module,
        "_complete_cloud_path",
        lambda _: [
            "art://artifactory.example.com/artifactory/cta-datastore/datasets/",
            "art://artifactory.example.com/artifactory/cta-datastore/docs/",
        ],
    )

    out = cli_module._path_complete(MagicMock(), MagicMock(), "art://cta-datastore/d")

    assert out == [
        "art://cta-datastore/datasets/",
        "art://cta-datastore/docs/",
    ]


def test_path_complete_rewrites_scp_style_prefix_for_cloud_completion(monkeypatch):
    monkeypatch.setattr(
        cli_module,
        "_complete_cloud_path",
        lambda _: [
            "ssh://cvat/~/",
            "ssh://cvat/~/docs/",
            "ssh://cvat/~/notes.txt",
        ],
    )
    monkeypatch.setattr(
        cli_module,
        "_maybe_rewrite_scp_style",
        lambda value, check_config=True: "ssh://cvat/~/" if value == "cvat:" else value,
    )

    out = cli_module._path_complete(MagicMock(), MagicMock(), "cvat:")

    assert out == [
        "cvat:",
        "cvat:docs/",
        "cvat:notes.txt",
    ]


def test_path_complete_scp_style_keeps_local_completion_when_not_rewritten(monkeypatch):
    monkeypatch.setattr(
        cli_module, "_maybe_rewrite_scp_style", lambda value, check_config=True: value
    )
    monkeypatch.setattr(cli_module, "_complete_local_path", lambda _: ["cvat:/tmp/"])

    out = cli_module._path_complete(MagicMock(), MagicMock(), "cvat:")

    assert out == ["cvat:/tmp/"]


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "args,stdin,return_value,expected_exit,expected_output",
    [
        pytest.param(["exists", "s3://bucket/file"], None, True, 0, "exists", id="true"),
        pytest.param(["exists", "s3://bucket/file"], None, False, 1, "does not exist", id="false"),
        pytest.param(["exists"], "s3://bucket/file.txt\n", True, 0, "exists", id="stdin"),
    ],
)
def test_exists(runner, args, stdin, return_value, expected_exit, expected_output):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.exists = AsyncMock(return_value=return_value)
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, args, input=stdin)
        assert result.exit_code == expected_exit
        assert expected_output in result.output.lower()


def test_exists_with_token(runner):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.exists = AsyncMock(return_value=True)
        mock_cls.return_value = mock_instance

        result = runner.invoke(
            cli, ["exists", "art://art.example.com/repo/file", "--token", "mytoken"]
        )
        assert result.exit_code == 0
        mock_cls.assert_called()


# ---------------------------------------------------------------------------
# stat
# ---------------------------------------------------------------------------


def test_stat_renders_gnu_like_fields(runner):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        st = __import__("os").stat(".")
        mock_instance = AsyncMock()
        mock_instance.stat = AsyncMock(return_value=st)
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["stat", "."])
        assert result.exit_code == 0
        assert "File:" in result.output
        assert "Size:" in result.output
        assert "Access:" in result.output
        assert "Modify:" in result.output
        assert "Change:" in result.output


def test_stat_fallback_to_head_metadata(runner):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.stat = AsyncMock(side_effect=NotImplementedError())
        mock_instance.is_dir = AsyncMock(return_value=False)

        mock_response = MagicMock()
        mock_response.headers = {
            "Content-Length": "42",
            "Date": "Mon, 13 Apr 2026 11:07:46 GMT",
            "Last-Modified": "Mon, 13 Apr 2026 11:00:00 GMT",
            "ETag": '"abc123"',
        }
        mock_instance.request = AsyncMock(return_value=mock_response)
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["stat", "s3://bucket/key"])
        assert result.exit_code == 0
        assert "Size:" in result.output
        assert "42" in result.output
        assert "Modify:" in result.output


# ---------------------------------------------------------------------------
# cat – setup helpers
# ---------------------------------------------------------------------------


def _cat_text(mock_cls):
    m = AsyncMock()
    m.read_text = AsyncMock(return_value="hello world")
    mock_cls.return_value = m


def _cat_binary(mock_cls):
    m = AsyncMock()
    m.read_bytes = AsyncMock(return_value=b"\xde\xad\xbe\xef")
    mock_cls.return_value = m


def _cat_missing(mock_cls):
    m = AsyncMock()
    m.read_text = AsyncMock(side_effect=FileNotFoundError())
    mock_cls.return_value = m


@pytest.mark.parametrize(
    "setup,args,stdin,expected_exit,expected_in",
    [
        pytest.param(_cat_text, ["cat", "s3://b/f.txt"], None, 0, ["hello world"], id="text"),
        pytest.param(
            _cat_binary, ["cat", "s3://b/f.bin", "--binary"], None, 0, ["deadbeef"], id="binary"
        ),
        pytest.param(_cat_missing, ["cat", "s3://b/missing.txt"], None, 1, [], id="not_found"),
        pytest.param(_cat_text, ["cat"], "s3://b/f.txt\n", 0, ["hello world"], id="stdin"),
        pytest.param(lambda m: None, ["cat"], "\n", 2, [], id="stdin_empty_errors"),
    ],
)
def test_cat(runner, setup, args, stdin, expected_exit, expected_in):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        setup(mock_cls)
        result = runner.invoke(cli, args, input=stdin)
        assert result.exit_code == expected_exit
        for text in expected_in:
            assert text in result.output.lower()


# ---------------------------------------------------------------------------
# ls – setup helpers
# ---------------------------------------------------------------------------


def _ls_empty(mock_cls):
    m = AsyncMock()

    async def empty_iterdir():
        return
        yield  # noqa: RET504

    m.iterdir = empty_iterdir
    mock_cls.return_value = m


def _ls_items(mock_cls):
    m = AsyncMock()
    item1 = AsyncMock()
    item1.__str__ = lambda s: "s3://bucket/file1.txt"
    item1.name = "file1.txt"
    item2 = AsyncMock()
    item2.__str__ = lambda s: "s3://bucket/file2.txt"
    item2.name = "file2.txt"

    async def list_items():
        yield item1
        yield item2

    m.iterdir = list_items
    mock_cls.return_value = m


def _ls_tree(mock_cls):
    root = AsyncMock()
    dir_item = AsyncMock()
    dir_item.__str__ = lambda s: "s3://bucket/dir"
    dir_item.name = "dir"
    dir_item.stat = AsyncMock(side_effect=NotImplementedError())
    dir_item.is_dir = AsyncMock(return_value=True)
    file_item = AsyncMock()
    file_item.__str__ = lambda s: "s3://bucket/dir/file.txt"
    file_item.name = "file.txt"
    file_item.stat = AsyncMock(side_effect=NotImplementedError())
    file_item.is_dir = AsyncMock(return_value=False)

    async def root_iterdir():
        yield dir_item

    async def dir_iterdir():
        yield file_item

    root.iterdir = root_iterdir
    dir_item.iterdir = dir_iterdir
    mock_cls.return_value = root


def _ls_nested(mock_cls):
    root, *_ = _make_nested_tree()
    mock_cls.return_value = root


def _make_nested_tree():
    """Build root -> dir -> nested -> deep.txt mock tree."""
    root = AsyncMock()
    dir_item = AsyncMock()
    dir_item.name = "dir"
    dir_item.stat = AsyncMock(side_effect=NotImplementedError())
    dir_item.is_dir = AsyncMock(return_value=True)
    nested_dir = AsyncMock()
    nested_dir.name = "nested"
    nested_dir.stat = AsyncMock(side_effect=NotImplementedError())
    nested_dir.is_dir = AsyncMock(return_value=True)
    deep_file = AsyncMock()
    deep_file.name = "deep.txt"
    deep_file.stat = AsyncMock(side_effect=NotImplementedError())
    deep_file.is_dir = AsyncMock(return_value=False)

    async def root_iterdir():
        yield dir_item

    async def dir_iterdir():
        yield nested_dir

    async def nested_iterdir():
        yield deep_file

    root.iterdir = root_iterdir
    dir_item.iterdir = dir_iterdir
    nested_dir.iterdir = nested_iterdir
    return root, dir_item, nested_dir, deep_file


@pytest.mark.parametrize(
    "setup,args,expected_exit,expected_in,not_in,call_with",
    [
        pytest.param(_ls_empty, ["ls", "s3://bucket/"], 0, ["empty"], [], None, id="empty"),
        pytest.param(
            _ls_items, ["ls", "s3://bucket/"], 0, ["file1.txt", "file2.txt"], [], None, id="items"
        ),
        pytest.param(
            _ls_tree, ["ls", "-r", "s3://bucket/"], 0, ["dir", "file.txt"], [], None, id="recursive"
        ),
        pytest.param(
            _ls_nested,
            ["ls", "-r", "-d", "2", "s3://bucket/"],
            0,
            ["dir", "nested"],
            ["deep.txt"],
            None,
            id="depth_limit",
        ),
        pytest.param(
            _ls_nested, ["ls", "-r", "s3://bucket/"], 0, ["deep.txt"], [], None, id="unbounded"
        ),
        pytest.param(_ls_empty, ["ls"], 0, [], [], ".", id="no_arg_default"),
        pytest.param(
            _ls_items,
            ["ls", "-1", "s3://bucket/"],
            0,
            [],
            ["├", "└", "NAME"],
            None,
            id="simple_-1",
        ),
        pytest.param(
            _ls_items,
            ["ls", "--simple", "s3://bucket/"],
            0,
            [],
            ["├", "└", "NAME"],
            None,
            id="simple_long",
        ),
    ],
)
def test_ls(runner, setup, args, expected_exit, expected_in, not_in, call_with):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        setup(mock_cls)
        result = runner.invoke(cli, args)
        assert result.exit_code == expected_exit
        for text in expected_in:
            assert text in result.output
        for text in not_in:
            assert text not in result.output
        if call_with is not None:
            mock_cls.assert_called_once_with(call_with)


# ---------------------------------------------------------------------------
# touch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "side_effect,args,stdin,expected_exit",
    [
        pytest.param(None, ["touch", "s3://bucket/file.txt"], None, 0, id="success"),
        pytest.param(
            NotImplementedError(), ["touch", "s3://bucket/file.txt"], None, 1, id="not_implemented"
        ),
        pytest.param(None, ["touch"], "s3://bucket/file.txt\n", 0, id="stdin"),
    ],
)
def test_touch(runner, side_effect, args, stdin, expected_exit):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.touch = AsyncMock(side_effect=side_effect)
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, args, input=stdin)
        assert result.exit_code == expected_exit
        if expected_exit == 0:
            assert "touched" in result.output.lower()


# ---------------------------------------------------------------------------
# checksums
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "return_value,args,stdin,expected_exit,expected_in_output",
    [
        pytest.param(
            {"md5": "abc123", "sha256": "def456"},
            ["checksums", "s3://bucket/file.txt"],
            None,
            0,
            ["md5", "abc123", "sha256"],
            id="success",
        ),
        pytest.param(
            {}, ["checksums", "s3://bucket/file.txt"], None, 0, ["no checksums"], id="empty"
        ),
        pytest.param(
            FileNotFoundError(),
            ["checksums", "s3://bucket/file.txt"],
            None,
            1,
            [],
            id="not_found",
        ),
        pytest.param(
            {"md5": "abc123"}, ["checksums"], "s3://bucket/file.txt\n", 0, ["md5"], id="stdin"
        ),
    ],
)
def test_checksums(runner, return_value, args, stdin, expected_exit, expected_in_output):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        if isinstance(return_value, Exception):
            mock_instance.checksums = AsyncMock(side_effect=return_value)
        else:
            mock_instance.checksums = AsyncMock(return_value=return_value)
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, args, input=stdin)
        assert result.exit_code == expected_exit
        for text in expected_in_output:
            assert text in result.output.lower()


# ---------------------------------------------------------------------------
# rm – setup helpers
# ---------------------------------------------------------------------------


def _rm_file(mock_cls):
    m = AsyncMock()
    m.exists = AsyncMock(return_value=True)
    m.is_dir = AsyncMock(return_value=False)
    m.unlink = AsyncMock()
    mock_cls.return_value = m


def _rm_missing(mock_cls):
    m = AsyncMock()
    m.exists = AsyncMock(return_value=False)
    mock_cls.return_value = m


def _rm_dir(mock_cls):
    m = AsyncMock()
    m.exists = AsyncMock(return_value=True)
    m.is_dir = AsyncMock(return_value=True)
    mock_cls.return_value = m


def _rm_multi(mock_cls):
    a = AsyncMock()
    a.exists = AsyncMock(return_value=True)
    a.is_dir = AsyncMock(return_value=False)
    a.unlink = AsyncMock()
    b = AsyncMock()
    b.exists = AsyncMock(return_value=True)
    b.is_dir = AsyncMock(return_value=False)
    b.unlink = AsyncMock()
    mock_cls.side_effect = [a, b]


@pytest.mark.parametrize(
    "setup,args,stdin,expected_exit,expected_in",
    [
        pytest.param(_rm_file, ["rm", "s3://bucket/file.txt"], None, 0, ["deleted"], id="success"),
        pytest.param(
            _rm_missing, ["rm", "s3://bucket/missing.txt", "--force"], None, 0, [], id="force_ok"
        ),
        pytest.param(
            _rm_missing, ["rm", "s3://bucket/missing.txt"], None, 1, [], id="no_force_fails"
        ),
        pytest.param(
            _rm_dir,
            ["rm", "s3://bucket/dir"],
            None,
            1,
            ["is a directory"],
            id="dir_requires_recursive",
        ),
        pytest.param(
            _rm_multi, ["rm", "s3://bucket/a.txt", "s3://bucket/b.txt"], None, 0, [], id="multiple"
        ),
        pytest.param(_rm_file, ["rm"], "s3://bucket/file.txt\n", 0, ["deleted"], id="stdin_single"),
        pytest.param(
            _rm_multi, ["rm"], "s3://bucket/a.txt\ns3://bucket/b.txt\n", 0, [], id="stdin_multi"
        ),
    ],
)
def test_rm(runner, setup, args, stdin, expected_exit, expected_in):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        setup(mock_cls)
        result = runner.invoke(cli, args, input=stdin)
        assert result.exit_code == expected_exit
        for text in expected_in:
            assert text in result.output.lower()


# ---------------------------------------------------------------------------
# mkdir
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "args,stdin",
    [
        pytest.param(["mkdir", "s3://bucket/newdir"], None, id="simple"),
        pytest.param(["mkdir", "s3://bucket/a/b/c", "--parents"], None, id="with_parents"),
        pytest.param(["mkdir"], "s3://bucket/newdir\n", id="stdin"),
    ],
)
def test_mkdir(runner, args, stdin):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.mkdir = AsyncMock()
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, args, input=stdin)
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# mv – setup helpers
# ---------------------------------------------------------------------------


def _mv_single(mock_cls):
    src = AsyncMock()
    src.exists = AsyncMock(return_value=True)
    src.is_dir = AsyncMock(return_value=False)
    src.read_bytes = AsyncMock(return_value=b"x")
    src.unlink = AsyncMock()
    dst_raw = AsyncMock()
    dst_raw.protocol = "s3"
    dst = AsyncMock()
    dst.exists = AsyncMock(return_value=False)
    dst.is_dir = AsyncMock(return_value=False)
    dst.write_bytes = AsyncMock()
    dst.parent.mkdir = AsyncMock()
    dst.protocol = "s3"
    mock_cls.side_effect = [src, dst_raw, dst]


def _mv_multi(mock_cls):
    src1 = AsyncMock()
    src1.name = "a.txt"
    src1.exists = AsyncMock(return_value=True)
    src1.is_dir = AsyncMock(return_value=False)
    src1.read_bytes = AsyncMock(return_value=b"a")
    src1.unlink = AsyncMock()
    src2 = AsyncMock()
    src2.name = "b.txt"
    src2.exists = AsyncMock(return_value=True)
    src2.is_dir = AsyncMock(return_value=False)
    src2.read_bytes = AsyncMock(return_value=b"b")
    src2.unlink = AsyncMock()
    dst_raw = AsyncMock()
    dst_raw.protocol = "s3"
    dst = AsyncMock()
    dst.exists = AsyncMock(return_value=True)
    dst.is_dir = AsyncMock(return_value=True)
    dst.protocol = "s3"
    dst_a = AsyncMock()
    dst_a.write_bytes = AsyncMock()
    dst_a.parent.mkdir = AsyncMock()
    dst_b = AsyncMock()
    dst_b.write_bytes = AsyncMock()
    dst_b.parent.mkdir = AsyncMock()
    dst.__truediv__.side_effect = [dst_a, dst_b]
    mock_cls.side_effect = [src1, src2, dst_raw, dst]


@pytest.mark.parametrize(
    "setup,args,expected_exit,expected_in",
    [
        pytest.param(
            _mv_single,
            ["mv", "s3://bucket/old.txt", "s3://bucket/new.txt"],
            0,
            ["moved"],
            id="success",
        ),
        pytest.param(lambda m: None, ["mv"], 2, [], id="no_args_error"),
        pytest.param(
            _mv_multi,
            ["mv", "s3://bucket/a.txt", "s3://bucket/b.txt", "s3://bucket/dest/"],
            0,
            [],
            id="multiple_sources",
        ),
    ],
)
def test_mv(runner, setup, args, expected_exit, expected_in):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        setup(mock_cls)
        result = runner.invoke(cli, args)
        assert result.exit_code == expected_exit
        for text in expected_in:
            assert text in result.output.lower()


def test_mv_forwards_atomic_and_chunk_size(runner):
    src = AsyncMock()
    src.name = "old.txt"
    src.exists = AsyncMock(return_value=True)
    src.is_dir = AsyncMock(return_value=False)
    src.copy = AsyncMock(return_value=None)
    dst = AsyncMock()
    dst.exists = AsyncMock(return_value=False)
    dst.is_dir = AsyncMock(return_value=False)

    with patch("asanypath.cli.AsAnyPath", side_effect=[src, dst]):
        result = runner.invoke(
            cli,
            ["mv", "s3://bucket/old.txt", "s3://bucket/new.txt", "--atomic", "--chunk-size", "5"],
        )

    assert result.exit_code == 0, result.output
    src.copy.assert_awaited_once()
    assert src.copy.await_args.kwargs["atomic"] is True
    assert src.copy.await_args.kwargs["chunk_size"] == 5


# ---------------------------------------------------------------------------
# auth show
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "args",
    [
        ["auth", "show"],
        ["auth", "show", "s3://bucket"],
    ],
    ids=["no_args", "s3_path"],
)
def test_auth_show(runner, args):
    result = runner.invoke(cli, args)
    assert result.exit_code == 0


def test_auth_show_masking(runner):
    """Verify tokens are masked in output."""
    import os

    os.environ["ARTIFACTORY_IDENTITY_TOKEN"] = "secret123456"
    try:
        result = runner.invoke(cli, ["auth", "show", "art://art.com"])
        assert result.exit_code == 0
        assert "sece*" not in result.output
        assert "ARTIFACTORY_IDENTITY_TOKEN" in result.output
    finally:
        del os.environ["ARTIFACTORY_IDENTITY_TOKEN"]


# ---------------------------------------------------------------------------
# cp / mv – single-argument default destination (real filesystem)
# ---------------------------------------------------------------------------


def test_cp_single_arg_copies_to_cwd(runner, tmp_path, monkeypatch):
    """cp with one path arg copies to the current directory."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    src_file = source_dir / "source.txt"
    src_file.write_text("hello")
    destination_dir = tmp_path / "destination"
    destination_dir.mkdir()
    monkeypatch.chdir(destination_dir)
    result = runner.invoke(cli, ["cp", str(src_file)])
    assert result.exit_code == 0
    assert (destination_dir / "source.txt").exists()


def test_mv_single_arg_moves_to_cwd(runner, tmp_path, monkeypatch):
    """mv with one path arg moves to the current directory."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    src_file = source_dir / "source.txt"
    src_file.write_text("hello")
    destination_dir = tmp_path / "destination"
    destination_dir.mkdir()
    monkeypatch.chdir(destination_dir)
    result = runner.invoke(cli, ["mv", str(src_file)])
    assert result.exit_code == 0
    assert (destination_dir / "source.txt").exists()
    assert not src_file.exists()


def test_cp_trailing_slash_creates_missing_dir(runner, tmp_path, monkeypatch):
    """cp file dest/ (dir absent) copies INTO dest/, creating it — not a file 'dest'."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "0001.patch").write_text("patch")
    result = runner.invoke(cli, ["cp", "./0001.patch", "dest-missing/"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "dest-missing").is_dir()
    assert (tmp_path / "dest-missing" / "0001.patch").read_text() == "patch"


def test_mv_trailing_slash_creates_missing_dir(runner, tmp_path, monkeypatch):
    """mv file dest/ (dir absent) moves INTO dest/, creating it."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "0001.patch").write_text("patch")
    result = runner.invoke(cli, ["mv", "./0001.patch", "dest-missing/"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "dest-missing" / "0001.patch").is_file()
    assert not (tmp_path / "0001.patch").exists()


# ---------------------------------------------------------------------------
# ls –1 / --simple – pairwise output equality (can't be a single param)
# ---------------------------------------------------------------------------


def test_ls_simple_and_verbose_flags_same_output(runner):
    """-1 and --simple produce identical output."""
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        _ls_items(mock_cls)
        r1 = runner.invoke(cli, ["ls", "-1", "s3://bucket/"])
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        _ls_items(mock_cls)
        r2 = runner.invoke(cli, ["ls", "--simple", "s3://bucket/"])
    assert r1.output == r2.output


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------
# presign
# ---------------------------------------------------------------------------


def test_presign_s3(runner):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.presign = AsyncMock(
            return_value="https://s3.amazonaws.com/bucket/key?X-Amz-Signature=abc"
        )
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["presign", "s3://bucket/key"])
        assert result.exit_code == 0
        assert "X-Amz-Signature=abc" in result.output


def test_presign_custom_expires(runner):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.presign = AsyncMock(return_value="https://example.com/url")
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["presign", "s3://bucket/key", "--expires", "900"])
        assert result.exit_code == 0
        mock_instance.presign.assert_called_once_with(expires=900, method="GET")


def test_presign_put_method(runner):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.presign = AsyncMock(return_value="https://example.com/url")
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["presign", "s3://b/k", "-m", "PUT"])
        assert result.exit_code == 0
        mock_instance.presign.assert_called_once_with(expires=3600, method="PUT")


def test_presign_not_implemented(runner):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.presign = AsyncMock(side_effect=NotImplementedError("not supported"))
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["presign", "art://host/repo/file"])
        assert result.exit_code == 1
        assert "not supported" in result.output


def test_presign_with_token(runner):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.presign = AsyncMock(return_value="https://example.com/url")
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["presign", "s3://b/k", "--token", "tok123"])
        assert result.exit_code == 0
        mock_cls.assert_called_once_with("s3://b/k", token="tok123")


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------


@pytest.fixture
def sync_dirs(tmp_path):
    """Provide src and dst directories for sync tests."""
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    return src, dst


def test_sync_local_directories(runner, sync_dirs):
    """sync copies new files from source to destination."""
    src, dst = sync_dirs
    (src / "sub").mkdir()
    (src / "a.txt").write_text("hello")
    (src / "sub" / "b.txt").write_text("world")

    result = runner.invoke(cli, ["sync", str(src), str(dst)])
    assert result.exit_code == 0
    assert (dst / "a.txt").read_text() == "hello"
    assert (dst / "sub" / "b.txt").read_text() == "world"


def test_sync_skips_up_to_date(runner, sync_dirs):
    """sync skips files that already exist and are up-to-date."""
    src, dst = sync_dirs
    (src / "a.txt").write_text("hello")
    (dst / "a.txt").write_text("hello")

    result = runner.invoke(cli, ["sync", str(src), str(dst), "-v"])
    assert result.exit_code == 0
    assert "up-to-date" in result.output.lower()


def test_sync_delete_extra_files(runner, sync_dirs):
    """sync --delete removes files in dst that are not in src."""
    src, dst = sync_dirs
    (src / "keep.txt").write_text("keep")
    (dst / "keep.txt").write_text("keep")
    (dst / "extra.txt").write_text("extra")

    result = runner.invoke(cli, ["sync", str(src), str(dst), "--delete", "-v"])
    assert result.exit_code == 0
    assert (dst / "keep.txt").exists()
    assert not (dst / "extra.txt").exists()


def test_sync_dry_run(runner, sync_dirs):
    """sync --dry-run shows what would be done without making changes."""
    src, dst = sync_dirs
    (src / "new.txt").write_text("new")

    result = runner.invoke(cli, ["sync", str(src), str(dst), "--dry-run"])
    assert result.exit_code == 0
    assert "dry" in result.output.lower()
    assert not (dst / "new.txt").exists()


def test_sync_exclude_pattern(runner, sync_dirs):
    """sync --exclude filters out matching files."""
    src, dst = sync_dirs
    (src / "keep.txt").write_text("keep")
    (src / "skip.tmp").write_text("skip")

    result = runner.invoke(cli, ["sync", str(src), str(dst), "--exclude", "*.tmp"])
    assert result.exit_code == 0
    assert (dst / "keep.txt").exists()
    assert not (dst / "skip.tmp").exists()


def test_sync_source_not_found(runner, sync_dirs):
    """sync errors when source does not exist."""
    _, dst = sync_dirs
    result = runner.invoke(cli, ["sync", "/nonexistent/path", str(dst)])
    assert result.exit_code == 1
    assert "not found" in result.output.lower()


def test_sync_intercloud_mock(runner):
    """sync works between different cloud backends (mocked)."""
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        src_root = AsyncMock()
        src_root.exists = AsyncMock(return_value=True)
        src_root.is_dir = AsyncMock(return_value=True)

        src_file = AsyncMock()
        src_file.name = "data.bin"
        src_file.__str__ = lambda s: "s3://bucket/data.bin"
        src_file.is_dir = AsyncMock(return_value=False)
        src_file.exists = AsyncMock(return_value=True)
        src_file.stat = AsyncMock(return_value=AsyncMock(st_size=100, st_mtime=1000.0))
        src_file.read_bytes = AsyncMock(return_value=b"data")

        async def src_iterdir():
            yield src_file

        src_root.iterdir = src_iterdir

        dst_root = AsyncMock()
        dst_root.exists = AsyncMock(return_value=True)
        dst_root.is_dir = AsyncMock(return_value=True)

        dst_file = AsyncMock()
        dst_file.name = "data.bin"
        dst_file.__str__ = lambda s: "az://container/data.bin"
        dst_file.exists = AsyncMock(return_value=False)
        dst_file.write_bytes = AsyncMock(return_value=4)
        dst_file.parent = AsyncMock()
        dst_file.parent.mkdir = AsyncMock()

        dst_root.__truediv__ = lambda self, other: dst_file

        async def dst_iterdir():
            return
            yield

        dst_root.iterdir = dst_iterdir

        def make_path(path, **kwargs):
            if "s3://" in path:
                return src_root
            return dst_root

        mock_cls.side_effect = make_path

        result = runner.invoke(cli, ["sync", "s3://bucket/", "az://container/"])
        assert result.exit_code == 0
        src_file.copy.assert_called_once_with(dst_file, force=True)


def test_cp_virtual_dir_source_mock(runner):
    """cp -r accepts prefix-only cloud directories where exists() is false but is_dir() is true."""
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        src_root = AsyncMock()
        src_root.exists = AsyncMock(return_value=False)
        src_root.is_dir = AsyncMock(return_value=True)
        src_root.name = "daily_train_torch"

        src_file = AsyncMock()
        src_file.name = "data.bin"
        src_file.copy = AsyncMock(return_value=None)

        async def src_iterdir():
            yield src_file

        src_root.iterdir = src_iterdir

        dst_root = AsyncMock()
        dst_root.exists = AsyncMock(return_value=False)
        dst_root.is_dir = AsyncMock(return_value=False)

        dst_dir = AsyncMock()
        dst_dir.exists = AsyncMock(return_value=False)
        dst_dir.is_dir = AsyncMock(return_value=False)

        dst_file = AsyncMock()
        dst_file.exists = AsyncMock(return_value=False)
        dst_file.is_dir = AsyncMock(return_value=False)

        dst_root.__truediv__ = lambda self, other: dst_dir
        dst_dir.__truediv__ = lambda self, other: dst_file

        def make_path(path, **kwargs):
            if "s3://" in path:
                return src_root
            return dst_root

        mock_cls.side_effect = make_path

        result = runner.invoke(
            cli,
            ["cp", "-r", "s3://bucket/daily_train_torch/", "kool"],
        )
        assert result.exit_code == 0, result.output
        src_file.copy.assert_called_once()


def test_sync_virtual_dir_source_mock(runner):
    """sync accepts prefix-only cloud directories where exists() is false but is_dir() is true."""
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        src_root = AsyncMock()
        src_root.exists = AsyncMock(return_value=False)
        src_root.is_dir = AsyncMock(return_value=True)

        src_file = AsyncMock()
        src_file.name = "data.bin"
        src_file.__str__ = lambda s: "s3://bucket/data.bin"
        src_file.is_dir = AsyncMock(return_value=False)
        src_file.exists = AsyncMock(return_value=True)
        src_file.stat = AsyncMock(return_value=AsyncMock(st_size=100, st_mtime=1000.0))
        src_file.copy = AsyncMock(return_value=None)

        async def src_iterdir():
            yield src_file

        src_root.iterdir = src_iterdir

        dst_root = AsyncMock()
        dst_root.exists = AsyncMock(return_value=True)
        dst_root.is_dir = AsyncMock(return_value=True)

        dst_file = AsyncMock()
        dst_file.name = "data.bin"
        dst_file.__str__ = lambda s: "az://container/data.bin"
        dst_file.exists = AsyncMock(return_value=False)
        dst_file.parent = AsyncMock()
        dst_file.parent.mkdir = AsyncMock()

        dst_root.__truediv__ = lambda self, other: dst_file

        async def dst_iterdir():
            return
            yield

        dst_root.iterdir = dst_iterdir

        def make_path(path, **kwargs):
            if "s3://" in path:
                return src_root
            return dst_root

        mock_cls.side_effect = make_path

        result = runner.invoke(cli, ["sync", "s3://bucket/", "az://container/"])
        assert result.exit_code == 0, result.output
        src_file.copy.assert_called_once_with(dst_file, force=True)


# ---------------------------------------------------------------------------
# tiered -v progress for cp / mv / sync
# ---------------------------------------------------------------------------


@pytest.fixture
def cp_tree(tmp_path):
    """Source dir with five files; empty dest dir."""
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    for i in range(5):
        (src / f"file{i}.txt").write_text(f"content{i}")
    return src, dst


@pytest.mark.parametrize("flags", [[], ["-v"], ["-vv"]], ids=["silent", "bar", "tasks"])
def test_cp_tiered_verbosity(runner, cp_tree, flags):
    src, dst = cp_tree
    result = runner.invoke(cli, ["cp", "-r", str(src), str(dst), *flags])
    assert result.exit_code == 0, result.output
    assert (dst / "file0.txt").read_text() == "content0"
    assert "in just" in result.output


@pytest.mark.parametrize("flags", [[], ["-v"], ["-vv"]], ids=["silent", "bar", "tasks"])
def test_sync_tiered_verbosity(runner, cp_tree, flags):
    src, dst = cp_tree
    dst.mkdir()
    result = runner.invoke(cli, ["sync", str(src), str(dst), *flags])
    assert result.exit_code == 0, result.output
    assert (dst / "file0.txt").read_text() == "content0"
    assert "in just" in result.output


def test_cp_concurrency_flag(runner, cp_tree):
    src, dst = cp_tree
    result = runner.invoke(cli, ["cp", "-r", str(src), str(dst), "-j", "2"])
    assert result.exit_code == 0
    for i in range(5):
        assert (dst / f"file{i}.txt").read_text() == f"content{i}"


def test_cp_forwards_atomic_and_chunk_size(runner):
    src = AsyncMock()
    src.name = "src.txt"
    src.exists = AsyncMock(return_value=True)
    src.is_dir = AsyncMock(return_value=False)
    src.copy = AsyncMock(return_value=None)
    dst = AsyncMock()
    dst.exists = AsyncMock(return_value=False)
    dst.is_dir = AsyncMock(return_value=False)

    with patch("asanypath.cli.AsAnyPath", side_effect=[src, dst]):
        result = runner.invoke(
            cli,
            ["cp", "s3://bucket/src.txt", "s3://bucket/dst.txt", "--atomic", "--chunk-size", "5"],
        )

    assert result.exit_code == 0, result.output
    src.copy.assert_awaited_once()
    assert src.copy.await_args.kwargs["atomic"] is True
    assert src.copy.await_args.kwargs["chunk_size"] == 5


def test_cp_failure_is_best_effort(runner, cp_tree, monkeypatch):
    """One failure must not cancel the other parallel copies."""
    src, dst = cp_tree

    from asanypath.local import AsyncPath

    orig_copy = AsyncPath.copy

    async def failing_copy(self, target, **kw):
        if self.name == "file2.txt":
            raise RuntimeError("boom")
        return await orig_copy(self, target, **kw)

    monkeypatch.setattr(AsyncPath, "copy", failing_copy)
    result = runner.invoke(cli, ["cp", "-r", str(src), str(dst)])
    assert result.exit_code == 2
    # Surviving siblings still copied.
    assert (dst / "file0.txt").exists()
    assert (dst / "file4.txt").exists()
    assert not (dst / "file2.txt").exists()


# ---------------------------------------------------------------------------
# protocols / auth show: SSH and FTP integration
# ---------------------------------------------------------------------------


def test_protocols_lists_ssh_and_ftp(runner):
    result = runner.invoke(cli, ["protocols"])
    assert result.exit_code == 0
    if _HAS_ASYNCSSH:
        assert "ssh" in result.output
        assert "~/.ssh/config" in result.output
    else:
        assert "ssh" not in result.output
    assert "ftp" in result.output
    assert "ftps" in result.output
    assert "~/.netrc" in result.output


def test_auth_show_ssh_section_present(runner, monkeypatch):
    monkeypatch.setenv("SSH_CONFIG", "")
    result = runner.invoke(cli, ["auth", "show", "ssh://h/x"])
    assert result.exit_code == 0
    assert "SSH / SFTP" in result.output
    if _HAS_ASYNCSSH:
        assert "SSH_USER" in result.output
    else:
        assert "optional dependency not installed" in result.output
    assert "SSH_PASSWORD" not in result.output
    assert "not read from env" in result.output


@pytest.mark.skipif(not _HAS_ASYNCSSH, reason="requires asyncssh extra")
def test_auth_show_ssh_prints_resolved_target(runner, tmp_path, monkeypatch):
    cfg = tmp_path / "config"
    cfg.write_text("Host myalias\n  HostName real.example.com\n  Port 2222\n")
    monkeypatch.setenv("SSH_CONFIG", str(cfg))
    from asanypath.ssh import SSHPath

    SSHPath._env_config = None
    result = runner.invoke(cli, ["auth", "show", "ssh://myalias/x"])
    assert result.exit_code == 0
    assert "real.example.com:2222" in result.output


def test_auth_show_ftp_section_present(runner):
    result = runner.invoke(cli, ["auth", "show", "ftp://h/x"])
    assert result.exit_code == 0
    assert "FTP / FTPS" in result.output
    assert "FTP_USER" in result.output
    assert "FTP_PASSWORD" not in result.output
    assert "~/.netrc" in result.output


def test_auth_show_ftp_reports_netrc_entry(runner, tmp_path, monkeypatch):
    f = tmp_path / "netrc"
    f.write_text("machine ftp.example.com\n  login alice\n  password s3cret\n")
    f.chmod(0o600)
    monkeypatch.setenv("NETRC", str(f))
    result = runner.invoke(cli, ["auth", "show", "ftp://ftp.example.com/x"])
    assert result.exit_code == 0
    assert "entry found" in result.output


# ---------------------------------------------------------------------------
# scp-style sugar
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _HAS_ASYNCSSH, reason="requires asyncssh extra")
def test_scp_style_rewrites_alias(tmp_path, monkeypatch):
    cfg = tmp_path / "config"
    cfg.write_text("Host prod-1\n  HostName 10.0.0.5\n  User deploy\n  Port 2222\n")
    monkeypatch.setenv("SSH_CONFIG", str(cfg))
    from asanypath.cli import _maybe_rewrite_scp_style
    from asanypath.ssh import SSHPath

    SSHPath._env_config = None
    # Absolute paths preserved.
    assert _maybe_rewrite_scp_style("prod-1:/var/log") == "ssh://prod-1/var/log"
    assert _maybe_rewrite_scp_style("alice@prod-1:/var/log") == "ssh://alice@prod-1/var/log"
    # Relative paths and empty path are home-relative (scp semantics).
    assert _maybe_rewrite_scp_style("prod-1:") == "ssh://prod-1/~/"
    assert _maybe_rewrite_scp_style("alice@prod-1:") == "ssh://alice@prod-1/~/"
    assert _maybe_rewrite_scp_style("prod-1:foo") == "ssh://prod-1/~/foo"
    assert _maybe_rewrite_scp_style("prod-1:foo/bar") == "ssh://prod-1/~/foo/bar"


def test_scp_style_leaves_url_alone(monkeypatch):
    monkeypatch.setenv("SSH_CONFIG", "")
    from asanypath.cli import _maybe_rewrite_scp_style

    assert _maybe_rewrite_scp_style("s3://bucket/key") == "s3://bucket/key"
    assert _maybe_rewrite_scp_style("ssh://h/x") == "ssh://h/x"


def test_scp_style_leaves_local_path_alone(monkeypatch):
    monkeypatch.setenv("SSH_CONFIG", "")
    from asanypath.cli import _maybe_rewrite_scp_style

    assert _maybe_rewrite_scp_style("/abs/path") == "/abs/path"
    assert _maybe_rewrite_scp_style("./rel:path") == "./rel:path"
    assert _maybe_rewrite_scp_style("plain.txt") == "plain.txt"


@pytest.mark.skipif(not _HAS_ASYNCSSH, reason="requires asyncssh extra")
def test_scp_style_unknown_host_not_rewritten(tmp_path, monkeypatch):
    cfg = tmp_path / "config"
    cfg.write_text("Host knownhost\n  HostName real.example.com\n")
    monkeypatch.setenv("SSH_CONFIG", str(cfg))
    from asanypath.cli import _maybe_rewrite_scp_style
    from asanypath.ssh import SSHPath

    SSHPath._env_config = None
    # No matching Host block -> passes through unchanged.
    assert _maybe_rewrite_scp_style("unknownhost:/path") == "unknownhost:/path"
