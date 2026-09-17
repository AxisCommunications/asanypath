# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for SSHPath (pure-Python SFTP backend via asyncssh)."""

from __future__ import annotations

import stat as stat_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("asyncssh")

from asanypath import AsAnyPath
from asanypath.options import AccessGrant, AccessPolicyPatch
from asanypath.ssh import SSHPath, disconnect_all
from tests.conftest import classtest_factory

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _attrs(mode: int = 0o100644, size: int = 0, mtime: int = 1700000000) -> SimpleNamespace:
    """Build an SFTPAttrs-like object."""
    return SimpleNamespace(
        permissions=mode,
        size=size,
        mtime=mtime,
        atime=mtime,
        uid=1000,
        gid=1000,
    )


def _file_handle(read_data: bytes = b"") -> MagicMock:
    """Mock asyncssh SFTPClientFile usable as `async with sftp.open(...) as f:`."""
    f = MagicMock()
    f.read = AsyncMock(return_value=read_data)
    f.write = AsyncMock()
    f.seek = AsyncMock()
    f.__aenter__ = AsyncMock(return_value=f)
    f.__aexit__ = AsyncMock(return_value=None)
    return f


@pytest.fixture(autouse=True)
def _clear_conn_cache():
    disconnect_all()
    yield
    disconnect_all()


@pytest.fixture
def sftp():
    """Patch SSHPath._sftp to return a configurable mock SFTP client."""
    mock_sftp = MagicMock()
    mock_sftp.stat = AsyncMock()
    mock_sftp.lstat = AsyncMock()
    mock_sftp.remove = AsyncMock()
    mock_sftp.mkdir = AsyncMock()
    mock_sftp.makedirs = AsyncMock()
    mock_sftp.rmdir = AsyncMock()
    mock_sftp.rmtree = AsyncMock()
    mock_sftp.rename = AsyncMock()
    mock_sftp.posix_rename = AsyncMock()
    mock_sftp.utime = AsyncMock()
    mock_sftp.chmod = AsyncMock()
    mock_sftp.chown = AsyncMock()
    mock_sftp.readdir = AsyncMock(return_value=[])
    mock_sftp.open = MagicMock(return_value=_file_handle())
    with patch.object(SSHPath, "_sftp", new=AsyncMock(return_value=mock_sftp)):
        yield mock_sftp


def _ssh(url: str = "ssh://alice@host.example:22/home/alice/file.txt") -> SSHPath:
    return SSHPath(url)


# ---------------------------------------------------------------------------
# Inherited abstract-method stubs (must be overridden — failing by default)
# ---------------------------------------------------------------------------

testbase = classtest_factory("_TestSSHPath", SSHPath)


class TestSSHPath(testbase):
    __test__ = True

    # ---- URL parsing & registration ----

    def test_protocol_registered(self):
        assert "ssh" in AsAnyPath("ssh://h/p").drive

    def test_dispatch_via_asanypath(self):
        assert isinstance(AsAnyPath("ssh://h/p"), SSHPath)

    def test_url_parses_user_host_port(self):
        p = _ssh()
        assert p._host == "host.example"
        assert p._port == 22
        assert p._user == "alice"
        assert p._item_path == "/home/alice/file.txt"

    def test_empty_path_means_home(self):
        # ``/~`` encodes "remote home" → SFTP cwd (= home after login).
        assert SSHPath("ssh://h/~/")._item_path == "."
        assert SSHPath("ssh://h/~/foo")._item_path == "./foo"

    async def test_access_policy_uses_sftp_attributes(self, sftp):
        sftp.stat.return_value = _attrs(0o100640)
        policy = await _ssh().get_access_policy()
        assert policy.grants == (
            AccessGrant("owner", frozenset({"read", "write"})),
            AccessGrant("group", frozenset({"read"})),
            AccessGrant("everyone", frozenset()),
        )

    async def test_update_access_policy_uses_sftp_chmod(self, sftp):
        sftp.stat.return_value = _attrs(0o100644)
        await _ssh().update_access_policy(
            AccessPolicyPatch(grants=(AccessGrant("group", frozenset({"read", "write"})),))
        )
        sftp.chmod.assert_awaited_once_with("/home/alice/file.txt", 0o100664)
        # Explicit absolute paths untouched.
        assert SSHPath("ssh://h/")._item_path == "/"
        assert SSHPath("ssh://h/foo")._item_path == "/foo"

    def test_url_defaults_when_port_missing(self):
        p = SSHPath("ssh://bob@h/x")
        assert p._port == 22

    # ---- env config ----

    def test_env_config_defaults(self, monkeypatch):
        for v in (
            "SSH_HOST",
            "SSH_PORT",
            "SSH_USER",
            "SSH_KEY_FILE",
            "SSH_KNOWN_HOSTS",
        ):
            monkeypatch.delenv(v, raising=False)
        monkeypatch.setenv("USER", "carol")
        monkeypatch.setenv("SSH_CONFIG", "")  # disable ssh_config discovery
        SSHPath._env_config = None
        cfg = SSHPath._create_env_config()
        assert cfg.port == 22
        assert cfg.user == "carol"
        assert cfg.known_hosts is None
        # password is a cache slot (not read from env); defaults to None
        assert cfg.password is None

    def test_env_config_known_hosts_none_disables_check(self, monkeypatch):
        monkeypatch.setenv("SSH_KNOWN_HOSTS", "none")
        SSHPath._env_config = None
        cfg = SSHPath._create_env_config()
        assert cfg.known_hosts == ()

    def test_env_config_known_hosts_path(self, monkeypatch):
        monkeypatch.setenv("SSH_KNOWN_HOSTS", "/tmp/kh")
        SSHPath._env_config = None
        cfg = SSHPath._create_env_config()
        assert cfg.known_hosts == "/tmp/kh"

    def test_env_config_key_file_wrapped_in_list(self, monkeypatch):
        monkeypatch.setenv("SSH_KEY_FILE", "/keys/id_ed25519")
        SSHPath._env_config = None
        cfg = SSHPath._create_env_config()
        assert cfg.client_keys == ["/keys/id_ed25519"]

    def test_explicit_kwargs_override_env(self, monkeypatch):
        monkeypatch.setenv("SSH_USER", "envuser")
        SSHPath._env_config = None
        p = SSHPath("ssh://h/x", username="kwuser")
        assert p._user == "kwuser"

    def test_client_keys_string_wrapped(self):
        p = SSHPath("ssh://h/x", client_keys="/k")
        assert p._client_keys == ["/k"]

    # ---- stat / exists ----

    async def test_exists_true(self, sftp):
        sftp.stat.return_value = _attrs()
        assert await _ssh().exists() is True

    async def test_exists_false_when_missing(self, sftp):
        sftp.stat.side_effect = FileNotFoundError
        assert await _ssh().exists() is False

    @pytest.mark.parametrize(
        "mode, expected",
        [
            (stat_module.S_IFDIR | 0o755, True),
            (stat_module.S_IFREG | 0o644, False),
        ],
    )
    async def test_is_dir(self, sftp, mode, expected):
        sftp.stat.return_value = _attrs(mode=mode)
        assert await _ssh().is_dir() is expected

    async def test_is_dir_false_when_missing(self, sftp):
        sftp.stat.side_effect = FileNotFoundError
        assert await _ssh().is_dir() is False

    async def test_exists_false_on_sftp_no_such_file(self, sftp):
        # asyncssh raises SFTPNoSuchFile (not a FileNotFoundError subclass) for
        # a missing path; exists() must still return False, not raise.
        import asyncssh

        sftp.stat.side_effect = asyncssh.sftp.SFTPNoSuchFile("No such file or directory")
        assert await _ssh().exists() is False

    async def test_is_dir_false_on_sftp_no_such_file(self, sftp):
        import asyncssh

        sftp.stat.side_effect = asyncssh.sftp.SFTPNoSuchFile("No such file or directory")
        assert await _ssh().is_dir() is False

    async def test_unlink_missing_ok_on_sftp_no_such_file(self, sftp):
        import asyncssh

        sftp.remove.side_effect = asyncssh.sftp.SFTPNoSuchFile("No such file or directory")
        await _ssh().unlink(missing_ok=True)  # must not raise

    @pytest.mark.parametrize(
        "mode, expected",
        [
            (stat_module.S_IFREG | 0o644, True),
            (stat_module.S_IFDIR | 0o755, False),
        ],
    )
    async def test_is_file(self, sftp, mode, expected):
        sftp.stat.return_value = _attrs(mode=mode)
        assert await _ssh().is_file() is expected

    @pytest.mark.parametrize(
        "mode, expected",
        [
            (stat_module.S_IFLNK | 0o777, True),
            (stat_module.S_IFREG | 0o644, False),
        ],
    )
    async def test_is_symlink(self, sftp, mode, expected):
        sftp.lstat.return_value = _attrs(mode=mode)
        assert await _ssh().is_symlink() is expected

    async def test_stat(self, sftp):
        sftp.stat.return_value = _attrs(mode=0o100644, size=42, mtime=12345)
        st = await _ssh().stat()
        assert st.st_size == 42
        assert st.st_mtime == 12345
        assert st.st_mode == 0o100644
        assert st.st_uid == 1000

    async def test_checksums(self, sftp):
        sftp.open.return_value = _file_handle(read_data=b"hello")
        sums = await _ssh().checksums()
        assert set(sums) == {"md5", "sha1", "sha256"}
        # md5("hello")
        assert sums["md5"] == "5d41402abc4b2a76b9719d911017c592"

    # ---- read / write ----

    async def test_read_bytes(self, sftp):
        sftp.open.return_value = _file_handle(read_data=b"payload")
        assert await _ssh().read_bytes() == b"payload"
        sftp.open.assert_called_once_with("/home/alice/file.txt", "rb")

    async def test_read_text(self, sftp):
        sftp.open.return_value = _file_handle(read_data=b"hej")
        assert await _ssh().read_text() == "hej"

    async def test_write_bytes(self, sftp):
        handle = _file_handle()
        sftp.open.return_value = handle
        n = await _ssh().write_bytes(b"hello")
        assert n == 5
        handle.write.assert_awaited_once_with(b"hello")

    async def test_write_text(self, sftp):
        handle = _file_handle()
        sftp.open.return_value = handle
        await _ssh().write_text("world")
        handle.write.assert_awaited_once_with(b"world")

    async def test_range_read(self, sftp):
        handle = _file_handle(read_data=b"23456")
        sftp.open.return_value = handle
        out = await _ssh()._range_read(2, 6)
        assert out == b"23456"
        handle.seek.assert_awaited_once_with(2)
        handle.read.assert_awaited_once_with(5)

    # ---- unlink / mkdir / rmdir ----

    async def test_unlink(self, sftp):
        await _ssh().unlink()
        sftp.remove.assert_awaited_once_with("/home/alice/file.txt")

    async def test_unlink_missing_ok(self, sftp):
        sftp.remove.side_effect = FileNotFoundError
        await _ssh().unlink(missing_ok=True)

    async def test_unlink_raises_when_missing(self, sftp):
        sftp.remove.side_effect = FileNotFoundError
        with pytest.raises(FileNotFoundError):
            await _ssh().unlink()

    async def test_mkdir(self, sftp):
        await _ssh().mkdir()
        sftp.mkdir.assert_awaited_once_with("/home/alice/file.txt")

    async def test_mkdir_parents(self, sftp):
        await _ssh().mkdir(parents=True, exist_ok=True)
        sftp.makedirs.assert_awaited_once()

    async def test_mkdir_exist_ok_swallows(self, sftp):
        sftp.mkdir.side_effect = FileExistsError
        await _ssh().mkdir(exist_ok=True)

    async def test_mkdir_raises_when_exists(self, sftp):
        sftp.mkdir.side_effect = FileExistsError
        with pytest.raises(FileExistsError):
            await _ssh().mkdir()

    async def test_rmdir(self, sftp):
        await _ssh().rmdir()
        sftp.rmdir.assert_awaited_once()

    async def test_rmdir_recursive(self, sftp):
        await _ssh().rmdir(recursive=True)
        sftp.rmtree.assert_awaited_once()

    # ---- rename / replace ----

    async def test_rename_same_host_is_atomic(self, sftp):
        sftp.stat.side_effect = FileNotFoundError
        renamed = await _ssh().rename("ssh://alice@host.example:22/home/alice/new.txt")
        sftp.rename.assert_awaited_once_with("/home/alice/file.txt", "/home/alice/new.txt")
        assert isinstance(renamed, SSHPath)

    async def test_replace_uses_posix_rename(self, sftp):
        await _ssh().replace("ssh://alice@host.example:22/home/alice/new.txt")
        sftp.posix_rename.assert_awaited_once()

    async def test_rename_cross_host_falls_back_to_copy(self, sftp):
        sftp.open.return_value = _file_handle(read_data=b"x")
        sftp.stat.return_value = _attrs(mode=stat_module.S_IFREG | 0o644)
        with patch.object(SSHPath, "unlink", new=AsyncMock()):
            renamed = await _ssh().rename("ssh://alice@other.example:22/home/alice/x")
        assert isinstance(renamed, SSHPath)
        sftp.rename.assert_not_called()

    # ---- touch ----

    async def test_touch_creates_when_missing(self, sftp):
        sftp.stat.side_effect = FileNotFoundError
        handle = _file_handle()
        sftp.open.return_value = handle
        await _ssh().touch()
        handle.write.assert_awaited_once_with(b"")

    async def test_touch_updates_mtime_when_exists(self, sftp):
        sftp.stat.return_value = _attrs()
        await _ssh().touch()
        sftp.utime.assert_awaited_once()

    async def test_touch_raises_when_exists_and_not_exist_ok(self, sftp):
        sftp.stat.return_value = _attrs()
        with pytest.raises(FileExistsError):
            await _ssh().touch(exist_ok=False)

    # ---- iterdir / walk ----

    @staticmethod
    def _name_entry(filename: str, mode: int) -> SimpleNamespace:
        return SimpleNamespace(filename=filename, attrs=SimpleNamespace(permissions=mode))

    async def test_iterdir_yields_children(self, sftp):
        sftp.readdir.return_value = [
            self._name_entry(".", stat_module.S_IFDIR | 0o755),
            self._name_entry("..", stat_module.S_IFDIR | 0o755),
            self._name_entry("a.txt", stat_module.S_IFREG | 0o644),
            self._name_entry("sub", stat_module.S_IFDIR | 0o755),
        ]
        children = [c async for c in SSHPath("ssh://alice@host.example/d").iterdir()]
        names = [c.name for c in children]
        assert names == ["a.txt", "sub"]
        assert all(isinstance(c, SSHPath) for c in children)

    async def test_iterdir_raises_when_not_a_directory(self, sftp):
        sftp.readdir.side_effect = FileNotFoundError
        with pytest.raises(NotADirectoryError):
            [c async for c in SSHPath("ssh://alice@host.example/d").iterdir()]

    async def test_walk_topdown(self, sftp):
        # /root contains file f1 and subdir sub; sub contains file f2.
        root_entries = [
            self._name_entry("f1", stat_module.S_IFREG | 0o644),
            self._name_entry("sub", stat_module.S_IFDIR | 0o755),
        ]
        sub_entries = [
            self._name_entry("f2", stat_module.S_IFREG | 0o644),
        ]

        async def readdir(path: str):
            if path.endswith("/sub"):
                return sub_entries
            return root_entries

        sftp.readdir.side_effect = readdir

        root = SSHPath("ssh://alice@host.example/root")
        # Force SFTP fallback (no subprocess spawning in unit tests).
        with patch.object(SSHPath, "_run_remote", new=AsyncMock(return_value=("", -1))):
            triples = [t async for t in root.walk()]
        assert len(triples) == 2
        # First yield: root with one dir and one file.
        first_root, first_dirs, first_files = triples[0]
        assert first_dirs == ["sub"]
        assert first_files == ["f1"]
        # Second yield: sub with no dirs and one file.
        _, sub_dirs, sub_files = triples[1]
        assert sub_dirs == []
        assert sub_files == ["f2"]

    async def test_walk_on_error_callback(self, sftp):
        sftp.readdir.side_effect = FileNotFoundError("nope")
        seen = []
        with patch.object(SSHPath, "_run_remote", new=AsyncMock(return_value=("", -1))):
            async for _ in SSHPath("ssh://alice@h/x").walk(on_error=seen.append):
                pass
        assert len(seen) == 1
        assert isinstance(seen[0], FileNotFoundError)

    # ---- open() unsupported ----

    def test_open_raises(self):
        with pytest.raises(NotImplementedError):
            _ssh().open("r")


# ---------------------------------------------------------------------------
# Connection pooling
# ---------------------------------------------------------------------------


async def test_get_conn_reuses_cached_connection():
    fake_conn = MagicMock()
    fake_conn.is_closed.return_value = False
    with patch(
        "asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake_conn)
    ) as mock_connect:
        p = SSHPath("ssh://alice@h:22/x")
        c1 = await p._get_conn()
        c2 = await p._get_conn()
        assert c1 is c2
        assert mock_connect.await_count == 1


async def test_get_conn_reconnects_when_closed():
    closed = MagicMock()
    closed.is_closed.return_value = True
    fresh = MagicMock()
    fresh.is_closed.return_value = False

    # First call returns "closed", but cache check on second call sees closed -> reconnect.
    with patch(
        "asanypath.ssh.asyncssh.connect",
        new=AsyncMock(side_effect=[closed, fresh]),
    ) as mock_connect:
        p = SSHPath("ssh://alice@h:22/x")
        await p._get_conn()
        c2 = await p._get_conn()
        assert c2 is fresh
        assert mock_connect.await_count == 2


async def test_sftp_reuse_does_not_require_exit_status_attribute():
    fake_sftp = object()

    fake_conn = MagicMock()
    fake_conn.is_closed.return_value = False
    fake_conn.start_sftp_client = AsyncMock(return_value=fake_sftp)

    with patch("asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake_conn)):
        p = SSHPath("ssh://alice@h:22/x")
        s1 = await p._sftp()
        s2 = await p._sftp()

    assert s1 is fake_sftp
    assert s2 is fake_sftp
    fake_conn.start_sftp_client.assert_awaited_once()


# ---------------------------------------------------------------------------
# _run_remote: OpenSSH subprocess helper
# ---------------------------------------------------------------------------


async def test_run_remote_builds_correct_ssh_args():
    """_run_remote uses original alias, BatchMode, and no extra flags by default."""
    captured: list[list[str]] = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        proc = MagicMock()
        proc.communicate = AsyncMock(return_value=(b"output\n", b""))
        proc.returncode = 0
        proc.kill = MagicMock()
        return proc

    with (
        patch("shutil.which", return_value="/usr/bin/ssh"),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
    ):
        p = SSHPath("ssh://myhost/path")
        out, rc = await p._run_remote("echo hi")

    assert rc == 0
    assert out == "output\n"
    args = captured[0]
    assert args[-1] == "echo hi"  # last arg: the command
    assert args[-2] == "myhost"  # second-to-last: target host
    assert "-o" in args
    assert "BatchMode=yes" in args
    # No explicit user or port because neither was typed in the URL
    assert "-l" not in args
    assert "-p" not in args


async def test_run_remote_passes_typed_user_and_port():
    """_run_remote passes -l/-p only when explicitly typed in the URL."""
    captured: list[list[str]] = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        proc = MagicMock()
        proc.communicate = AsyncMock(return_value=(b"", b""))
        proc.returncode = 0
        proc.kill = MagicMock()
        return proc

    with (
        patch("shutil.which", return_value="/usr/bin/ssh"),
        patch("asyncio.create_subprocess_exec", side_effect=fake_exec),
    ):
        p = SSHPath("ssh://alice@myhost:2222/path")
        await p._run_remote("pwd")

    args = captured[0]
    assert "-l" in args
    assert args[args.index("-l") + 1] == "alice"
    assert "-p" in args
    assert args[args.index("-p") + 1] == "2222"


async def test_run_remote_returns_empty_when_ssh_not_found():
    with patch("shutil.which", return_value=None):
        p = SSHPath("ssh://h/x")
        out, rc = await p._run_remote("echo hi")
    assert out == ""
    assert rc == -1


# ---------------------------------------------------------------------------
# _list_dir_remote: single-level listing via find
# ---------------------------------------------------------------------------


async def test_list_dir_remote_parses_find_output():
    find_output = "d\tsubdir\nf\tfile.txt\nd\tanother\n"
    p = SSHPath("ssh://h/some/path")
    with patch.object(SSHPath, "_run_remote", new=AsyncMock(return_value=(find_output, 0))):
        result = await p._list_dir_remote()
    assert result is not None
    assert ("subdir", True) in result
    assert ("file.txt", False) in result
    assert ("another", True) in result


async def test_list_dir_remote_returns_none_when_find_fails():
    p = SSHPath("ssh://h/some/path")
    with patch.object(SSHPath, "_run_remote", new=AsyncMock(return_value=("", 1))):
        result = await p._list_dir_remote()
    assert result is None


async def test_list_dir_remote_returns_none_when_output_has_no_tab():
    p = SSHPath("ssh://h/some/path")
    with patch.object(SSHPath, "_run_remote", new=AsyncMock(return_value=("noprintsupport\n", 0))):
        result = await p._list_dir_remote()
    assert result is None


# ---------------------------------------------------------------------------
# walk: remote-find fast path and SFTP fallback
# ---------------------------------------------------------------------------


async def test_walk_fast_path_uses_remote_find(sftp):
    """walk() parses remote find output and emits correct triples."""
    # Flat find output: root contains subdir/ and file.txt; subdir has f2.txt
    find_output = "d\tsubdir\nf\tfile.txt\nf\tsubdir/f2.txt\n"
    p = SSHPath("ssh://h/root")
    with patch.object(SSHPath, "_run_remote", new=AsyncMock(return_value=(find_output, 0))):
        triples = [t async for t in p.walk()]

    assert len(triples) == 2
    root_path, root_dirs, root_files = triples[0]
    assert root_dirs == ["subdir"]
    assert root_files == ["file.txt"]

    sub_path, sub_dirs, sub_files = triples[1]
    assert sub_dirs == []
    assert sub_files == ["f2.txt"]
    # SFTP was not used
    sftp.readdir.assert_not_called()


async def test_walk_falls_back_to_sftp_when_find_unavailable(sftp):
    """walk() falls back to SFTP readdir when _run_remote returns empty."""
    sftp.readdir.return_value = []

    p = SSHPath("ssh://h/root")
    with patch.object(SSHPath, "_run_remote", new=AsyncMock(return_value=("", 1))):
        triples = [t async for t in p.walk()]

    sftp.readdir.assert_called_once()
    assert triples == [(p, [], [])]


async def test_walk_falls_back_to_sftp_when_printf_unsupported(sftp):
    """walk() falls back when find output contains no tab (no -printf support)."""
    sftp.readdir.return_value = []

    p = SSHPath("ssh://h/root")
    # rc=0 but output has no tab character — not GNU find
    with patch.object(SSHPath, "_run_remote", new=AsyncMock(return_value=("./file\n", 0))):
        [t async for t in p.walk()]  # noqa: F841 (intentional consumption for side effects)

    sftp.readdir.assert_called_once()


async def test_walk_fast_path_bottom_up(sftp):
    """walk(top_down=False) emits leaves before parents via fast path."""
    find_output = "d\tsubdir\nf\tsubdir/f.txt\n"
    p = SSHPath("ssh://h/root")
    with patch.object(SSHPath, "_run_remote", new=AsyncMock(return_value=(find_output, 0))):
        triples = [t async for t in p.walk(top_down=False)]

    # Bottom-up: subdir first, then root
    assert len(triples) == 2
    first_base, _, first_files = triples[0]
    assert first_base.name == "subdir"
    assert first_files == ["f.txt"]
    second_base, second_dirs, _ = triples[1]
    assert second_base.name == "root"
    assert second_dirs == ["subdir"]


async def test_disconnect_all_closes_and_clears():
    fake = MagicMock()
    fake.is_closed.return_value = False
    with patch("asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake)):
        await SSHPath("ssh://alice@h:22/x")._get_conn()
    from asanypath.ssh import _CONN_CACHE

    assert _CONN_CACHE
    disconnect_all()
    assert not _CONN_CACHE
    fake.close.assert_called_once()


# ---------------------------------------------------------------------------
# ssh_config alias resolution
# ---------------------------------------------------------------------------


@pytest.fixture
def ssh_config_file(tmp_path, monkeypatch):
    """Write a tmp OpenSSH client config and point SSH_CONFIG at it."""
    cfg = tmp_path / "config"
    cfg.write_text(
        "Host myalias\n"
        "  HostName real.example.com\n"
        "  User deploy\n"
        "  Port 2222\n"
        "Host other\n"
        "  HostName real.example.com\n"
        "  User deploy\n"
        "  Port 2222\n"
    )
    monkeypatch.setenv("SSH_CONFIG", str(cfg))
    SSHPath._env_config = None
    yield cfg


def test_alias_resolved_eagerly(ssh_config_file):
    p = SSHPath("ssh://myalias/var/log/foo")
    assert p._host == "myalias"  # typed host preserved
    assert p._resolved_host == "real.example.com"
    assert p._resolved_user == "deploy"
    assert p._resolved_port == 2222


def test_str_keeps_original_url_after_alias_resolution(ssh_config_file):
    p = SSHPath("ssh://myalias/var/log/foo")
    assert str(p) == "ssh://myalias/var/log/foo"


def test_resolved_target_property(ssh_config_file):
    p = SSHPath("ssh://myalias/var/log/foo")
    assert p.resolved_target == "ssh://deploy@real.example.com:2222/var/log/foo"


def test_repr_surfaces_resolution(ssh_config_file):
    p = SSHPath("ssh://myalias/x")
    r = repr(p)
    assert "ssh://myalias/x" in r
    assert "deploy@real.example.com:2222" in r


def test_unknown_alias_passes_through(ssh_config_file):
    p = SSHPath("ssh://nosuchhost/x")
    assert p._resolved_host == "nosuchhost"
    # Port falls back to default; no User in config.
    assert p._resolved_port == 22


def test_two_aliases_same_target_share_conn(ssh_config_file):
    fake = MagicMock()
    fake.is_closed.return_value = False
    with patch("asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake)) as mock_connect:

        async def go():
            p1 = SSHPath("ssh://myalias/x")
            p2 = SSHPath("ssh://other/y")
            assert p1._conn_key == p2._conn_key
            await p1._get_conn()
            await p2._get_conn()

        import asyncio as _asyncio

        _asyncio.run(go())
        assert mock_connect.await_count == 1


def test_explicit_kwargs_override_alias(ssh_config_file):
    p = SSHPath("ssh://myalias/x", port=9999)
    assert p._resolved_port == 9999


def test_connect_receives_config_paths(ssh_config_file):
    fake = MagicMock()
    fake.is_closed.return_value = False
    with patch("asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake)) as mock_connect:
        import asyncio as _asyncio

        _asyncio.run(SSHPath("ssh://myalias/x")._get_conn())
        kwargs = mock_connect.await_args.kwargs
        assert kwargs["config"] == [str(ssh_config_file)]
        # Original alias passed so asyncssh re-applies IdentityFile etc.
        assert kwargs["host"] == "myalias"


def test_connect_does_not_override_alias_user_or_port_with_env_defaults(
    ssh_config_file, monkeypatch
):
    fake = MagicMock()
    fake.is_closed.return_value = False
    monkeypatch.setenv("SSH_USER", "localdefault")
    monkeypatch.setenv("SSH_PORT", "22")
    SSHPath._env_config = None
    with patch("asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake)) as mock_connect:
        import asyncio as _asyncio

        _asyncio.run(SSHPath("ssh://myalias/x")._get_conn())
        kwargs = mock_connect.await_args.kwargs
        # Let ssh_config Host block provide alias-specific User/Port.
        assert "username" not in kwargs
        assert "port" not in kwargs
        assert kwargs["host"] == "myalias"


def test_no_ssh_config_means_no_resolution(monkeypatch):
    monkeypatch.setenv("SSH_CONFIG", "")
    SSHPath._env_config = None
    p = SSHPath("ssh://alice@host.example:22/x")
    assert (p._resolved_host, p._resolved_port, p._resolved_user) == (
        "host.example",
        22,
        "alice",
    )


# ----------------------------------------------------------------------
# Interactive auth (asyncssh callbacks)
# ----------------------------------------------------------------------


def test_get_conn_passes_client_factory_when_interactive(monkeypatch):
    from asanypath import ssh as ssh_mod

    monkeypatch.setattr(ssh_mod, "_interactive", lambda: True)
    ssh_mod._CONN_CACHE.clear()
    ssh_mod._CONN_LOCKS.clear()
    fake = MagicMock()
    fake.is_closed.return_value = False
    with patch("asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake)) as mock_connect:
        import asyncio as _asyncio

        _asyncio.run(SSHPath("ssh://alice@host.example/x")._get_conn())
        factory = mock_connect.await_args.kwargs.get("client_factory")
        assert callable(factory)
        client = factory()
        assert isinstance(client, ssh_mod._InteractiveSSHClient)
        assert "alice@host.example" in client._label


def test_get_conn_no_client_factory_when_not_interactive(monkeypatch):
    from asanypath import ssh as ssh_mod

    monkeypatch.setattr(ssh_mod, "_interactive", lambda: False)
    ssh_mod._CONN_CACHE.clear()
    ssh_mod._CONN_LOCKS.clear()
    fake = MagicMock()
    fake.is_closed.return_value = False
    with patch("asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake)) as mock_connect:
        import asyncio as _asyncio

        _asyncio.run(SSHPath("ssh://alice@host.example/x")._get_conn())
        assert "client_factory" not in mock_connect.await_args.kwargs


def test_get_conn_no_client_factory_when_password_supplied(monkeypatch):
    from asanypath import ssh as ssh_mod

    monkeypatch.setattr(ssh_mod, "_interactive", lambda: True)
    ssh_mod._CONN_CACHE.clear()
    ssh_mod._CONN_LOCKS.clear()
    fake = MagicMock()
    fake.is_closed.return_value = False
    with patch("asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake)) as mock_connect:
        import asyncio as _asyncio

        _asyncio.run(SSHPath("ssh://alice@host.example/x", password="secret")._get_conn())
        assert "client_factory" not in mock_connect.await_args.kwargs


def test_password_propagates_to_derived_paths():
    """The password kwarg must survive on derived paths (parent, /, ...)."""
    SSHPath._env_config = None
    try:
        p = SSHPath("ssh://host.example/a/b.txt", password="secret", known_hosts=())
        assert p._password == "secret"
        assert p.parent._password == "secret"
        assert (p.parent / "c.txt")._password == "secret"
    finally:
        SSHPath._env_config = None


def test_interactive_client_password_auth_prompts(monkeypatch):
    from asanypath import ssh as ssh_mod

    monkeypatch.setattr(ssh_mod.getpass, "getpass", lambda msg="": "prompted-pw")
    client = ssh_mod._InteractiveSSHClient("alice@host")
    import asyncio as _asyncio

    assert _asyncio.run(client.password_auth_requested()) == "prompted-pw"
    # Cached: second call doesn't re-prompt even if getpass would error.
    monkeypatch.setattr(
        ssh_mod.getpass, "getpass", lambda msg="": (_ for _ in ()).throw(RuntimeError("nope"))
    )
    assert _asyncio.run(client.password_auth_requested()) == "prompted-pw"


def test_interactive_client_kbdint_responds_to_prompts(monkeypatch):
    from asanypath import ssh as ssh_mod

    monkeypatch.setattr(ssh_mod.getpass, "getpass", lambda msg="": f"hidden:{msg}")
    monkeypatch.setattr("builtins.input", lambda msg="": f"echo:{msg}")
    client = ssh_mod._InteractiveSSHClient("alice@host")
    import asyncio as _asyncio

    prompts = [("Password: ", False), ("Code: ", True)]
    out = _asyncio.run(client.kbdint_challenge_received("name", "instr", "en", prompts))
    assert out == ["hidden:Password: ", "echo:Code: "]


def test_interactive_kbdint_empty_prompts_returns_empty():
    from asanypath import ssh as ssh_mod

    client = ssh_mod._InteractiveSSHClient("x")
    import asyncio as _asyncio

    assert _asyncio.run(client.kbdint_challenge_received("", "", "en", [])) == []


# ----------------------------------------------------------------------
# Credstore wiring
# ----------------------------------------------------------------------


def test_ssh_uses_cached_password_then_skips_factory(monkeypatch):
    """Cache hit: pass cached pw to connect, no client_factory."""
    from asanypath import _credstore
    from asanypath import ssh as ssh_mod

    monkeypatch.setattr(ssh_mod, "_interactive", lambda: True)
    monkeypatch.setattr(_credstore, "load", lambda key, **_: "cached-pw")
    saved: list = []
    monkeypatch.setattr(_credstore, "save", lambda k, v: saved.append((k, v)))
    ssh_mod._CONN_CACHE.clear()
    ssh_mod._CONN_LOCKS.clear()
    fake = MagicMock()
    fake.is_closed.return_value = False
    with patch("asanypath.ssh.asyncssh.connect", new=AsyncMock(return_value=fake)) as mock_connect:
        import asyncio as _asyncio

        _asyncio.run(SSHPath("ssh://alice@host.example/x")._get_conn())
    kw = mock_connect.await_args.kwargs
    assert kw.get("password") == "cached-pw"
    assert "client_factory" not in kw
    assert saved == []


def test_ssh_stale_cache_forgets_and_retries_interactive(monkeypatch):
    """Cache hit + PermissionDenied: forget, retry with factory."""
    import asyncssh as _asyncssh

    from asanypath import _credstore
    from asanypath import ssh as ssh_mod

    monkeypatch.setattr(ssh_mod, "_interactive", lambda: True)
    monkeypatch.setattr(_credstore, "load", lambda key, **_: "stale-pw")
    forgotten: list = []
    monkeypatch.setattr(_credstore, "forget", lambda k: forgotten.append(k))
    monkeypatch.setattr(_credstore, "save", lambda k, v: None)
    ssh_mod._CONN_CACHE.clear()
    ssh_mod._CONN_LOCKS.clear()
    fake = MagicMock()
    fake.is_closed.return_value = False
    calls: list = []

    async def fake_connect(**kw):
        calls.append(kw)
        if "password" in kw and kw["password"] == "stale-pw":
            raise _asyncssh.PermissionDenied(reason="bad pw")
        return fake

    with patch("asanypath.ssh.asyncssh.connect", new=fake_connect):
        import asyncio as _asyncio

        _asyncio.run(SSHPath("ssh://alice@host.example/x")._get_conn())
    assert len(calls) == 2
    assert "client_factory" in calls[1]
    assert len(forgotten) == 1


def test_ssh_saves_password_after_successful_prompt(monkeypatch):
    """Interactive prompt succeeded → save password to credstore."""
    from asanypath import _credstore
    from asanypath import ssh as ssh_mod

    monkeypatch.setattr(ssh_mod, "_interactive", lambda: True)
    monkeypatch.setattr(_credstore, "load", lambda key, **_: None)
    saved: list = []
    monkeypatch.setattr(_credstore, "save", lambda k, v: saved.append((k, v)))
    ssh_mod._CONN_CACHE.clear()
    ssh_mod._CONN_LOCKS.clear()
    fake = MagicMock()
    fake.is_closed.return_value = False

    async def fake_connect(**kw):
        # Simulate asyncssh calling the factory + prompt during auth.
        factory = kw.get("client_factory")
        if factory is not None:
            client = factory()
            client._cached_password = "fresh-pw"
        return fake

    with patch("asanypath.ssh.asyncssh.connect", new=fake_connect):
        import asyncio as _asyncio

        _asyncio.run(SSHPath("ssh://alice@host.example/x")._get_conn())
    assert len(saved) == 1
    assert saved[0][1] == "fresh-pw"
