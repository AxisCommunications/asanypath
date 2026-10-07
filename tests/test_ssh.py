# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for SSHPath (native russh SFTP backend)."""

from __future__ import annotations

import stat as stat_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from asanypath import AsAnyPath
from asanypath.options import AccessGrant, AccessPolicyPatch
from asanypath.ssh import SSHPath, disconnect_all
from tests.conftest import classtest_factory

# ---------------------------------------------------------------------------
# Mode constants
# ---------------------------------------------------------------------------

DIR_MODE = stat_module.S_IFDIR | 0o755  # 0o040755
FILE_MODE = stat_module.S_IFREG | 0o644  # 0o100644
LNK_MODE = stat_module.S_IFLNK | 0o777  # 0o120777


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stat(
    *,
    size: int = 0,
    mtime: int = 1700000000,
    atime: int = 1700000000,
    uid: int = 1000,
    gid: int = 1000,
    permissions: int = FILE_MODE,
) -> tuple[int, int, int, int, int, int]:
    """Build an ``ssh_stat``/``ssh_lstat`` result tuple."""
    return (size, mtime, atime, uid, gid, permissions)


def _entry(
    name: str,
    permissions: int,
    *,
    size: int = 0,
    mtime: int = 1700000000,
    atime: int = 1700000000,
    uid: int = 1000,
    gid: int = 1000,
) -> tuple[str, int, int, int, int, int, int]:
    """Build an ``ssh_list`` entry tuple."""
    return (name, size, mtime, atime, uid, gid, permissions)


@pytest.fixture(autouse=True)
def _clear_conn_cache():
    disconnect_all()
    yield
    disconnect_all()


@pytest.fixture
def ssh():
    """Patch every native ``ssh_*`` data function on ``asanypath.ssh``.

    Data functions are async (``AsyncMock``); the returned namespace exposes
    them so tests can set ``.return_value`` / ``.side_effect`` and assert calls.
    """
    mocks = SimpleNamespace(
        read=AsyncMock(return_value=b""),
        read_range=AsyncMock(return_value=b""),
        write=AsyncMock(return_value=0),
        stat=AsyncMock(return_value=_stat()),
        lstat=AsyncMock(return_value=_stat()),
        setstat=AsyncMock(return_value=None),
        list=AsyncMock(return_value=[]),
        mkdir=AsyncMock(return_value=None),
        rmdir=AsyncMock(return_value=None),
        unlink=AsyncMock(return_value=None),
        rename=AsyncMock(return_value=None),
    )
    with (
        patch("asanypath.ssh.ssh_read", mocks.read),
        patch("asanypath.ssh.ssh_read_range", mocks.read_range),
        patch("asanypath.ssh.ssh_write", mocks.write),
        patch("asanypath.ssh.ssh_stat", mocks.stat),
        patch("asanypath.ssh.ssh_lstat", mocks.lstat),
        patch("asanypath.ssh.ssh_setstat", mocks.setstat),
        patch("asanypath.ssh.ssh_list", mocks.list),
        patch("asanypath.ssh.ssh_mkdir", mocks.mkdir),
        patch("asanypath.ssh.ssh_rmdir", mocks.rmdir),
        patch("asanypath.ssh.ssh_unlink", mocks.unlink),
        patch("asanypath.ssh.ssh_rename", mocks.rename),
    ):
        yield mocks


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
        # Explicit absolute paths untouched.
        assert SSHPath("ssh://h/")._item_path == "/"
        assert SSHPath("ssh://h/foo")._item_path == "/foo"

    def test_url_defaults_when_port_missing(self):
        p = SSHPath("ssh://bob@h/x")
        assert p._port == 22

    # ---- access policy ----

    async def test_access_policy_uses_sftp_attributes(self, ssh):
        ssh.stat.return_value = _stat(permissions=0o100640)
        policy = await _ssh().get_access_policy()
        assert policy.grants == (
            AccessGrant("owner", frozenset({"read", "write"})),
            AccessGrant("group", frozenset({"read"})),
            AccessGrant("everyone", frozenset()),
        )

    async def test_update_access_policy_sets_mode(self, ssh):
        ssh.stat.return_value = _stat(permissions=0o100644)
        await _ssh().update_access_policy(
            AccessPolicyPatch(grants=(AccessGrant("group", frozenset({"read", "write"})),))
        )
        assert ssh.setstat.await_args.kwargs["permissions"] == 0o100664

    async def test_update_access_policy_sets_ownership(self, ssh):
        await _ssh().update_access_policy(AccessPolicyPatch(owner="1001", group="2002"))
        assert ssh.setstat.await_args.kwargs["uid"] == 1001
        assert ssh.setstat.await_args.kwargs["gid"] == 2002

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

    async def test_exists_true(self, ssh):
        ssh.stat.return_value = _stat()
        assert await _ssh().exists() is True

    async def test_exists_false_when_missing(self, ssh):
        ssh.stat.side_effect = RuntimeError("No such file")
        assert await _ssh().exists() is False

    @pytest.mark.parametrize(
        "mode, expected",
        [
            (DIR_MODE, True),
            (FILE_MODE, False),
        ],
    )
    async def test_is_dir(self, ssh, mode, expected):
        ssh.stat.return_value = _stat(permissions=mode)
        assert await _ssh().is_dir() is expected

    async def test_is_dir_false_when_missing(self, ssh):
        ssh.stat.side_effect = RuntimeError("No such file")
        assert await _ssh().is_dir() is False

    @pytest.mark.parametrize(
        "mode, expected",
        [
            (FILE_MODE, True),
            (DIR_MODE, False),
        ],
    )
    async def test_is_file(self, ssh, mode, expected):
        ssh.stat.return_value = _stat(permissions=mode)
        assert await _ssh().is_file() is expected

    @pytest.mark.parametrize(
        "mode, expected",
        [
            (LNK_MODE, True),
            (FILE_MODE, False),
        ],
    )
    async def test_is_symlink(self, ssh, mode, expected):
        ssh.lstat.return_value = _stat(permissions=mode)
        assert await _ssh().is_symlink() is expected

    async def test_stat(self, ssh):
        ssh.stat.return_value = _stat(size=42, mtime=12345, permissions=0o100644)
        st = await _ssh().stat()
        assert st.st_size == 42
        assert st.st_mtime == 12345
        assert st.st_mode == 0o100644
        assert st.st_uid == 1000

    async def test_checksums(self, ssh):
        ssh.read.return_value = b"hello"
        sums = await _ssh().checksums()
        assert set(sums) == {"md5", "sha1", "sha256"}
        # md5("hello")
        assert sums["md5"] == "5d41402abc4b2a76b9719d911017c592"

    # ---- read / write ----

    async def test_read_bytes(self, ssh):
        ssh.read.return_value = b"payload"
        assert await _ssh().read_bytes() == b"payload"
        assert ssh.read.await_args.kwargs["path"] == "/home/alice/file.txt"

    async def test_read_bytes_maps_missing(self, ssh):
        ssh.read.side_effect = RuntimeError("No such file")
        with pytest.raises(FileNotFoundError):
            await _ssh().read_bytes()

    async def test_read_text(self, ssh):
        ssh.read.return_value = b"hej"
        assert await _ssh().read_text() == "hej"

    async def test_write_bytes(self, ssh):
        ssh.write.return_value = 5
        n = await _ssh().write_bytes(b"hello")
        assert n == 5
        assert ssh.write.await_args.kwargs["data"] == b"hello"

    async def test_write_text(self, ssh):
        ssh.write.return_value = 5
        await _ssh().write_text("world")
        assert ssh.write.await_args.kwargs["data"] == b"world"

    async def test_range_read(self, ssh):
        ssh.read_range.return_value = b"23456"
        out = await _ssh()._range_read(2, 6)
        assert out == b"23456"
        assert ssh.read_range.await_args.kwargs["start"] == 2
        assert ssh.read_range.await_args.kwargs["end"] == 6

    # ---- unlink / mkdir / rmdir ----

    async def test_unlink(self, ssh):
        await _ssh().unlink()
        assert ssh.unlink.await_args.kwargs["path"] == "/home/alice/file.txt"

    async def test_unlink_missing_ok(self, ssh):
        ssh.unlink.side_effect = RuntimeError("No such file")
        await _ssh().unlink(missing_ok=True)  # must not raise

    async def test_unlink_raises_when_missing(self, ssh):
        ssh.unlink.side_effect = RuntimeError("No such file")
        with pytest.raises(FileNotFoundError):
            await _ssh().unlink()

    async def test_mkdir(self, ssh):
        ssh.stat.side_effect = RuntimeError("No such file")  # does not pre-exist
        await _ssh().mkdir()
        assert ssh.mkdir.await_args.kwargs["path"] == "/home/alice/file.txt"
        assert ssh.mkdir.await_args.kwargs["parents"] is False

    async def test_mkdir_parents(self, ssh):
        await _ssh().mkdir(parents=True, exist_ok=True)
        assert ssh.mkdir.await_args.kwargs["parents"] is True

    async def test_mkdir_exist_ok_swallows(self, ssh):
        ssh.mkdir.side_effect = RuntimeError("already exists")
        await _ssh().mkdir(exist_ok=True)  # must not raise

    async def test_mkdir_exist_ok_swallows_generic_failure(self, ssh):
        # Some servers (e.g. atmoz/sftp) return an unclassifiable error for an
        # existing dir; exist_ok must stay idempotent via an is_dir re-check.
        ssh.mkdir.side_effect = RuntimeError("Failure")
        ssh.stat.return_value = _stat(permissions=DIR_MODE)  # path is an existing dir
        await _ssh().mkdir(exist_ok=True)  # must not raise

    async def test_mkdir_generic_failure_reraises_when_not_dir(self, ssh):
        # Generic failure with no existing dir behind it must still propagate.
        ssh.mkdir.side_effect = RuntimeError("Failure")
        ssh.stat.side_effect = RuntimeError("No such file")  # not a dir
        with pytest.raises(RuntimeError):
            await _ssh().mkdir(exist_ok=True)

    async def test_mkdir_raises_when_exists(self, ssh):
        ssh.stat.return_value = _stat()  # exists → pre-check raises
        with pytest.raises(FileExistsError):
            await _ssh().mkdir()

    async def test_upload_buffer_small_single_write(self, ssh):
        from io import BytesIO

        with patch.object(SSHPath, "write_bytes", new=AsyncMock()) as wb:
            await _ssh()._upload_buffer(BytesIO(b"small"), 5)
        wb.assert_awaited_once_with(b"small")

    async def test_upload_buffer_streams_chunks_with_append(self, ssh):
        from io import BytesIO

        calls: list[tuple[bool, bytes]] = []

        async def write_chunk(*, path, data, truncate, **kwargs):
            calls.append((truncate, data))
            return len(data)

        with (
            patch.object(SSHPath, "_STREAM_CHUNK", 4),
            patch("asanypath.ssh.ssh_write_chunk", new=write_chunk),
        ):
            await _ssh()._upload_buffer(BytesIO(b"abcdefghij"), 10)
        # First chunk truncates, the rest append; reassembled bytes are intact.
        assert [t for t, _ in calls] == [True, False, False]
        assert b"".join(d for _, d in calls) == b"abcdefghij"

    async def test_rmdir(self, ssh):
        await _ssh().rmdir()
        assert ssh.rmdir.await_args.kwargs["path"] == "/home/alice/file.txt"

    async def test_rmdir_recursive_removes_children_first(self, ssh):
        ssh.list.return_value = [_entry("a.txt", FILE_MODE)]
        await SSHPath("ssh://alice@host.example/d").rmdir(recursive=True)
        # Child file removed, then the directory itself.
        ssh.unlink.assert_awaited_once()
        assert ssh.unlink.await_args.kwargs["path"] == "/d/a.txt"
        ssh.rmdir.assert_awaited_once()

    # ---- rename / replace ----

    async def test_rename_same_host_is_atomic(self, ssh):
        ssh.stat.side_effect = RuntimeError("No such file")  # destination missing
        renamed = await _ssh().rename("ssh://alice@host.example:22/home/alice/new.txt")
        assert ssh.rename.await_args.kwargs["src"] == "/home/alice/file.txt"
        assert ssh.rename.await_args.kwargs["dst"] == "/home/alice/new.txt"
        assert isinstance(renamed, SSHPath)

    async def test_replace_same_host_uses_native_rename(self, ssh):
        await _ssh().replace("ssh://alice@host.example:22/home/alice/new.txt")
        ssh.rename.assert_awaited_once()
        # Replace clears an existing destination first.
        ssh.unlink.assert_awaited()

    async def test_rename_cross_host_falls_back_to_copy(self, ssh):
        ssh.stat.side_effect = RuntimeError("No such file")  # destination missing
        ssh.read.return_value = b"x"
        ssh.write.return_value = 1
        renamed = await _ssh().rename("ssh://alice@other.example:22/home/alice/x")
        assert isinstance(renamed, SSHPath)
        ssh.rename.assert_not_awaited()

    # ---- touch ----

    async def test_touch_creates_when_missing(self, ssh):
        ssh.stat.side_effect = RuntimeError("No such file")
        await _ssh().touch()
        assert ssh.write.await_args.kwargs["data"] == b""

    async def test_touch_updates_mtime_when_exists(self, ssh):
        ssh.stat.return_value = _stat()
        await _ssh().touch()
        assert ssh.setstat.await_args.kwargs["mtime"] is not None
        assert ssh.setstat.await_args.kwargs["atime"] is not None

    async def test_touch_raises_when_exists_and_not_exist_ok(self, ssh):
        ssh.stat.return_value = _stat()
        with pytest.raises(FileExistsError):
            await _ssh().touch(exist_ok=False)

    # ---- iterdir / walk ----

    async def test_iterdir_yields_children(self, ssh):
        ssh.list.return_value = [
            _entry(".", DIR_MODE),
            _entry("..", DIR_MODE),
            _entry("a.txt", FILE_MODE),
            _entry("sub", DIR_MODE),
        ]
        children = [c async for c in SSHPath("ssh://alice@host.example/d").iterdir()]
        names = [c.name for c in children]
        assert names == ["a.txt", "sub"]
        assert all(isinstance(c, SSHPath) for c in children)

    async def test_iterdir_raises_when_not_a_directory(self, ssh):
        ssh.list.side_effect = RuntimeError("No such file")
        with pytest.raises(NotADirectoryError):
            [c async for c in SSHPath("ssh://alice@host.example/d").iterdir()]

    async def test_walk_topdown(self, ssh):
        # /root contains file f1 and subdir sub; sub contains file f2.
        async def fake_list(*, path, **_):
            if path.endswith("/sub"):
                return [_entry("f2", FILE_MODE)]
            return [_entry("f1", FILE_MODE), _entry("sub", DIR_MODE)]

        ssh.list.side_effect = fake_list

        root = SSHPath("ssh://alice@host.example/root")
        triples = [t async for t in root.walk()]
        assert len(triples) == 2
        # First yield: root with one dir and one file.
        _, first_dirs, first_files = triples[0]
        assert first_dirs == ["sub"]
        assert first_files == ["f1"]
        # Second yield: sub with no dirs and one file.
        _, sub_dirs, sub_files = triples[1]
        assert sub_dirs == []
        assert sub_files == ["f2"]

    async def test_walk_bottom_up(self, ssh):
        async def fake_list(*, path, **_):
            if path.endswith("/sub"):
                return [_entry("f.txt", FILE_MODE)]
            return [_entry("sub", DIR_MODE)]

        ssh.list.side_effect = fake_list

        root = SSHPath("ssh://alice@host.example/root")
        triples = [t async for t in root.walk(top_down=False)]
        assert len(triples) == 2
        first_base, _, first_files = triples[0]
        assert first_base.name == "sub"
        assert first_files == ["f.txt"]
        second_base, second_dirs, _ = triples[1]
        assert second_base.name == "root"
        assert second_dirs == ["sub"]

    async def test_walk_on_error_callback(self, ssh):
        ssh.list.side_effect = RuntimeError("No such file")
        seen = []
        async for _ in SSHPath("ssh://alice@h/x").walk(on_error=seen.append):
            pass
        assert len(seen) == 1
        assert isinstance(seen[0], NotADirectoryError)

    # ---- open() unsupported ----

    def test_open_raises(self):
        with pytest.raises(NotImplementedError):
            _ssh().open("r")


# ---------------------------------------------------------------------------
# disconnect_all
# ---------------------------------------------------------------------------


def test_disconnect_all_calls_native():
    with patch("asanypath.ssh.ssh_disconnect_all") as native:
        disconnect_all()
    native.assert_called_once()


# ---------------------------------------------------------------------------
# ssh_config alias resolution (native ssh_resolve_config)
# ---------------------------------------------------------------------------


@pytest.fixture
def resolve_config(monkeypatch):
    """Point ssh_config discovery at a dummy path and stub the native resolver."""
    monkeypatch.setenv("SSH_CONFIG", "/tmp/cfg")
    SSHPath._env_config = None
    mock = MagicMock(return_value=("real.example.com", 2222, "deploy", None))
    monkeypatch.setattr("asanypath.ssh.ssh_resolve_config", mock)
    yield mock
    SSHPath._env_config = None


def test_alias_resolved_eagerly(resolve_config):
    p = SSHPath("ssh://myalias/var/log/foo")
    assert p._host == "myalias"  # typed host preserved
    assert p._resolved_host == "real.example.com"
    assert p._resolved_user == "deploy"
    assert p._resolved_port == 2222


def test_str_keeps_original_url_after_alias_resolution(resolve_config):
    p = SSHPath("ssh://myalias/var/log/foo")
    assert str(p) == "ssh://myalias/var/log/foo"


def test_resolved_target_property(resolve_config):
    p = SSHPath("ssh://myalias/var/log/foo")
    assert p.resolved_target == "ssh://deploy@real.example.com:2222/var/log/foo"


def test_repr_surfaces_resolution(resolve_config):
    p = SSHPath("ssh://myalias/x")
    r = repr(p)
    assert "ssh://myalias/x" in r
    assert "deploy@real.example.com:2222" in r


def test_unknown_alias_passes_through(resolve_config):
    resolve_config.return_value = (None, None, None, None)
    p = SSHPath("ssh://nosuchhost/x")
    assert p._resolved_host == "nosuchhost"
    # Port falls back to default; no User in config.
    assert p._resolved_port == 22


def test_explicit_kwargs_override_alias(resolve_config):
    p = SSHPath("ssh://myalias/x", port=9999)
    assert p._resolved_port == 9999


def test_no_ssh_config_means_no_resolution(monkeypatch):
    monkeypatch.setenv("SSH_CONFIG", "")
    SSHPath._env_config = None
    p = SSHPath("ssh://alice@host.example:22/x")
    assert (p._resolved_host, p._resolved_port, p._resolved_user) == (
        "host.example",
        22,
        "alice",
    )


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
