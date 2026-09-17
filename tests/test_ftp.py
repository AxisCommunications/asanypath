# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for FTPPath / FTPSPath (optional aioftp backend)."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

aioftp = pytest.importorskip("aioftp")

from asanypath import AsAnyPath  # noqa: E402
from asanypath.ftp import FTPPath, FTPSPath, _parse_mlst_mtime, disconnect_all  # noqa: E402
from asanypath.options import AccessGrant, AccessPolicyPatch  # noqa: E402
from tests.conftest import classtest_factory  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stream(read_data: bytes = b"") -> MagicMock:
    """Mock aioftp DataConnectionThrottleStreamIO usable as async ctx mgr."""
    s = MagicMock()
    s.read = AsyncMock(return_value=read_data)
    s.write = AsyncMock()
    s.__aenter__ = AsyncMock(return_value=s)
    s.__aexit__ = AsyncMock(return_value=None)
    return s


class _AsyncLister:
    """Mimic aioftp's AbstractAsyncLister (supports `async for`)."""

    def __init__(self, items):
        self._items = items

    def __aiter__(self):
        async def gen():
            for item in self._items:
                yield item

        return gen()


def _status_error(code: str = "550") -> aioftp.StatusCodeError:
    return aioftp.StatusCodeError(("2xx",), (code,), ["err"])


@pytest.fixture(autouse=True)
def _clear_client_cache():
    disconnect_all()
    yield
    disconnect_all()


@pytest.fixture
def client():
    """Patch FTPPath._ensure_client to return a configurable mock aioftp.Client."""
    c = MagicMock()
    c.exists = AsyncMock(return_value=True)
    c.is_dir = AsyncMock(return_value=False)
    c.is_file = AsyncMock(return_value=True)
    c.stat = AsyncMock(return_value={})
    c.remove_file = AsyncMock()
    c.remove_directory = AsyncMock()
    c.remove = AsyncMock()
    c.make_directory = AsyncMock()
    c.rename = AsyncMock()
    c.list = MagicMock(return_value=_AsyncLister([]))
    c.download_stream = MagicMock(return_value=_stream())
    c.upload_stream = MagicMock(return_value=_stream())
    c.close = MagicMock()
    c.command = AsyncMock()
    with patch.object(FTPPath, "_ensure_client", new=AsyncMock(return_value=c)):
        yield c


def _ftp(url: str = "ftp://alice:pw@host.example:21/home/alice/file.txt") -> FTPPath:
    return FTPPath(url)


# ---------------------------------------------------------------------------
# Inherited abstract-method stubs
# ---------------------------------------------------------------------------

testbase = classtest_factory("_TestFTPPath", FTPPath)


class TestFTPPath(testbase):
    __test__ = True

    # ---- URL parsing & registration ----

    def test_protocol_registered(self):
        assert "ftp" in AsAnyPath("ftp://h/p").drive

    async def test_update_access_policy_uses_site_chmod(self, client):
        client.stat.return_value = {"unix.mode": "0644"}
        await _ftp().update_access_policy(
            AccessPolicyPatch(grants=(AccessGrant("everyone", frozenset({"read", "write"})),))
        )
        client.command.assert_awaited_once_with("SITE CHMOD 646 /home/alice/file.txt", "2xx")

    def test_dispatch_via_asanypath(self):
        assert isinstance(AsAnyPath("ftp://h/p"), FTPPath)

    def test_ftps_dispatch(self):
        assert isinstance(AsAnyPath("ftps://h/p"), FTPSPath)

    def test_url_parses_user_password_host(self):
        p = _ftp()
        assert p._host == "host.example"
        assert p._port == 21
        assert p._user == "alice"
        assert p._password == "pw"
        assert p._item_path == "/home/alice/file.txt"

    def test_default_port_used_when_missing(self):
        p = FTPPath("ftp://h/x")
        assert p._port == 21

    # ---- env config ----

    def test_env_config_defaults(self, monkeypatch):
        for v in ("FTP_HOST", "FTP_PORT", "FTP_USER"):
            monkeypatch.delenv(v, raising=False)
        FTPPath._env_config = None
        cfg = FTPPath._create_env_config()
        assert cfg.port == 21
        assert cfg.user == "anonymous"
        assert not hasattr(cfg, "password")

    def test_env_config_overrides(self, monkeypatch):
        monkeypatch.setenv("FTP_HOST", "h")
        monkeypatch.setenv("FTP_PORT", "2121")
        monkeypatch.setenv("FTP_USER", "bob")
        FTPPath._env_config = None
        cfg = FTPPath._create_env_config()
        assert cfg.host == "h"
        assert cfg.port == 2121
        assert cfg.user == "bob"

    def test_explicit_kwargs_override_env(self, monkeypatch):
        monkeypatch.setenv("FTP_USER", "envuser")
        FTPPath._env_config = None
        p = FTPPath("ftp://h/x", username="kwuser")
        assert p._user == "kwuser"

    def test_ftps_class_marks_tls(self):
        p = FTPSPath("ftps://h/x")
        assert p._tls is True
        assert p._native_kwargs["tls"] is True

    # ---- stat / exists ----

    @pytest.mark.parametrize("value", [True, False])
    async def test_exists(self, client, value):
        client.exists.return_value = value
        assert await _ftp().exists() is value

    async def test_is_dir_true(self, client):
        client.is_dir.return_value = True
        assert await _ftp().is_dir() is True

    async def test_is_dir_false_on_status_error(self, client):
        client.is_dir.side_effect = _status_error()
        assert await _ftp().is_dir() is False

    async def test_is_file_true(self, client):
        client.is_file.return_value = True
        assert await _ftp().is_file() is True

    async def test_is_file_false_on_status_error(self, client):
        client.is_file.side_effect = _status_error()
        assert await _ftp().is_file() is False

    async def test_is_symlink_always_false(self, client):
        assert await _ftp().is_symlink() is False

    async def test_stat(self, client):
        client.stat.return_value = {
            "size": "1234",
            "modify": "20240115093045",
            "type": "file",
            "unix.mode": "0644",
        }
        st = await _ftp().stat()
        assert st.st_size == 1234
        assert st.st_mtime == _parse_mlst_mtime("20240115093045")
        assert st.st_mode == 0o644

    async def test_stat_handles_missing_fields(self, client):
        client.stat.return_value = {"type": "file"}
        st = await _ftp().stat()
        assert st.st_size == 0
        assert st.st_mtime is None
        assert st.st_mode is None

    async def test_checksums(self, client):
        client.download_stream.return_value = _stream(read_data=b"hello")
        sums = await _ftp().checksums()
        assert set(sums) == {"md5", "sha1", "sha256"}
        assert sums["md5"] == "5d41402abc4b2a76b9719d911017c592"

    # ---- read / write ----

    async def test_read_bytes(self, client):
        client.download_stream.return_value = _stream(read_data=b"payload")
        assert await _ftp().read_bytes() == b"payload"
        client.download_stream.assert_called_once_with("/home/alice/file.txt")

    async def test_read_text(self, client):
        client.download_stream.return_value = _stream(read_data=b"hej")
        assert await _ftp().read_text() == "hej"

    async def test_write_bytes(self, client):
        s = _stream()
        client.upload_stream.return_value = s
        n = await _ftp().write_bytes(b"hello")
        assert n == 5
        s.write.assert_awaited_once_with(b"hello")

    async def test_write_text(self, client):
        s = _stream()
        client.upload_stream.return_value = s
        await _ftp().write_text("world")
        s.write.assert_awaited_once_with(b"world")

    async def test_range_read_raises(self, client):
        with pytest.raises(NotImplementedError):
            await _ftp()._range_read(0, 10)

    # ---- unlink ----

    async def test_unlink(self, client):
        await _ftp().unlink()
        client.remove_file.assert_awaited_once_with("/home/alice/file.txt")

    async def test_unlink_missing_ok(self, client):
        client.remove_file.side_effect = _status_error("550")
        await _ftp().unlink(missing_ok=True)

    async def test_unlink_raises_when_missing(self, client):
        client.remove_file.side_effect = _status_error("550")
        with pytest.raises(FileNotFoundError):
            await _ftp().unlink()

    # ---- mkdir / rmdir ----

    async def test_mkdir(self, client):
        await _ftp().mkdir()
        client.make_directory.assert_awaited_once_with("/home/alice/file.txt", parents=False)

    async def test_mkdir_parents(self, client):
        await _ftp().mkdir(parents=True)
        client.make_directory.assert_awaited_once_with("/home/alice/file.txt", parents=True)

    async def test_mkdir_exist_ok_skips_when_exists(self, client):
        client.exists.return_value = True
        await _ftp().mkdir(exist_ok=True)
        client.make_directory.assert_not_called()

    async def test_mkdir_raises_when_exists(self, client):
        client.exists.return_value = False
        client.make_directory.side_effect = _status_error("550")
        with pytest.raises(FileExistsError):
            await _ftp().mkdir()

    async def test_rmdir(self, client):
        await _ftp().rmdir()
        client.remove_directory.assert_awaited_once()

    async def test_rmdir_recursive(self, client):
        await _ftp().rmdir(recursive=True)
        client.remove.assert_awaited_once()

    # ---- rename / replace ----

    async def test_rename_same_host(self, client):
        client.exists.return_value = False
        renamed = await _ftp().rename("ftp://alice:pw@host.example:21/home/alice/new.txt")
        client.rename.assert_awaited_once_with("/home/alice/file.txt", "/home/alice/new.txt")
        assert isinstance(renamed, FTPPath)

    async def test_rename_cross_host_falls_back(self, client):
        client.download_stream.return_value = _stream(read_data=b"x")
        client.exists.return_value = True
        client.is_file.return_value = True
        with patch.object(FTPPath, "unlink", new=AsyncMock()):
            renamed = await _ftp().rename("ftp://alice:pw@other.example:21/home/alice/x")
        assert isinstance(renamed, FTPPath)
        client.rename.assert_not_called()

    async def test_replace_raises(self, client):
        with pytest.raises(NotImplementedError):
            await _ftp().replace("ftp://alice:pw@host.example:21/x")

    # ---- touch ----

    async def test_touch_creates_when_missing(self, client):
        client.exists.return_value = False
        s = _stream()
        client.upload_stream.return_value = s
        await _ftp().touch()
        s.write.assert_awaited_once_with(b"")

    async def test_touch_noop_when_exists(self, client):
        client.exists.return_value = True
        await _ftp().touch()
        client.upload_stream.assert_not_called()

    async def test_touch_raises_when_exists_and_not_exist_ok(self, client):
        client.exists.return_value = True
        with pytest.raises(FileExistsError):
            await _ftp().touch(exist_ok=False)

    # ---- iterdir / walk ----

    async def test_iterdir_yields_children(self, client):
        from pathlib import PurePosixPath

        client.list.return_value = _AsyncLister(
            [
                (PurePosixPath("/d/a.txt"), {"type": "file", "size": "0", "modify": ""}),
                (PurePosixPath("/d/sub"), {"type": "dir", "size": "0", "modify": ""}),
            ]
        )
        children = [c async for c in FTPPath("ftp://alice@host.example/d").iterdir()]
        assert [c.name for c in children] == ["a.txt", "sub"]
        assert all(isinstance(c, FTPPath) for c in children)

    async def test_iterdir_raises_when_not_a_directory(self, client):
        client.list.side_effect = _status_error("550")
        with pytest.raises(NotADirectoryError):
            [c async for c in FTPPath("ftp://alice@host.example/d").iterdir()]

    async def test_walk_topdown(self, client):
        from pathlib import PurePosixPath

        def list_for(path):
            if path.endswith("/sub"):
                return _AsyncLister([(PurePosixPath(f"{path}/f2"), {"type": "file", "modify": ""})])
            return _AsyncLister(
                [
                    (PurePosixPath(f"{path}/f1"), {"type": "file", "modify": ""}),
                    (PurePosixPath(f"{path}/sub"), {"type": "dir", "modify": ""}),
                ]
            )

        client.list.side_effect = list_for

        root = FTPPath("ftp://alice@host.example/root")
        triples = [t async for t in root.walk()]
        assert len(triples) == 2
        _, dirs1, files1 = triples[0]
        assert [d.name for d in dirs1] == ["sub"]
        assert [f.name for f in files1] == ["f1"]
        _, dirs2, files2 = triples[1]
        assert dirs2 == []
        assert [f.name for f in files2] == ["f2"]

    async def test_walk_on_error_callback(self, client):
        client.list.side_effect = _status_error("550")
        seen = []
        async for _ in FTPPath("ftp://alice@h/x").walk(on_error=seen.append):
            pass
        assert len(seen) == 1
        assert isinstance(seen[0], aioftp.StatusCodeError)

    # ---- open() unsupported ----

    def test_open_raises(self):
        with pytest.raises(NotImplementedError):
            _ftp().open("r")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "modify, expected",
    [
        (
            "20240115093045",
            datetime(2024, 1, 15, 9, 30, 45, tzinfo=timezone.utc).timestamp(),
        ),
        (
            "20240115093045.250000",
            datetime(2024, 1, 15, 9, 30, 45, 250000, tzinfo=timezone.utc).timestamp(),
        ),
        (None, None),
        ("", None),
    ],
)
def test_parse_mlst_mtime(modify, expected):
    assert _parse_mlst_mtime(modify) == expected


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------


async def test_ensure_client_reuses_open_client():
    fake = MagicMock()
    fake.stream = SimpleNamespace()  # truthy "live" sentinel
    fake.connect = AsyncMock()
    fake.login = AsyncMock()
    with patch("asanypath.ftp.aioftp.Client", return_value=fake) as ctor:
        p = FTPPath("ftp://alice:pw@h:21/x")
        c1 = await p._ensure_client()
        c2 = await p._ensure_client()
        assert c1 is c2
        ctor.assert_called_once()


async def test_ensure_client_reconnects_when_stream_gone():
    closed = MagicMock()
    closed.stream = None
    fresh = MagicMock()
    fresh.stream = SimpleNamespace()
    for c in (closed, fresh):
        c.connect = AsyncMock()
        c.login = AsyncMock()
    with patch("asanypath.ftp.aioftp.Client", side_effect=[closed, fresh]) as ctor:
        p = FTPPath("ftp://alice:pw@h:21/x")
        await p._ensure_client()
        # Force the cached client to look closed before next call.
        from asanypath.ftp import _CLIENT_CACHE

        _CLIENT_CACHE[p._client_key].stream = None
        c2 = await p._ensure_client()
        assert c2 is fresh
        assert ctor.call_count == 2


async def test_disconnect_all_closes_and_clears():
    fake = MagicMock()
    fake.stream = SimpleNamespace()
    fake.connect = AsyncMock()
    fake.login = AsyncMock()
    with patch("asanypath.ftp.aioftp.Client", return_value=fake):
        await FTPPath("ftp://alice@h:21/x")._ensure_client()
    from asanypath.ftp import _CLIENT_CACHE

    assert _CLIENT_CACHE
    disconnect_all()
    assert not _CLIENT_CACHE
    fake.close.assert_called_once()


# ---------------------------------------------------------------------------
# ~/.netrc credential lookup
# ---------------------------------------------------------------------------


@pytest.fixture
def netrc_file(tmp_path, monkeypatch):
    """Write a tmp netrc, set NETRC, return the path."""
    f = tmp_path / "netrc"
    f.write_text("machine ftp.example.com\n  login alice\n  password s3cret\n  account dept42\n")
    f.chmod(0o600)
    monkeypatch.setenv("NETRC", str(f))
    FTPPath._env_config = None
    yield f


def test_netrc_populates_user_password_account(netrc_file):
    p = FTPPath("ftp://ftp.example.com/x")
    assert p._user == "alice"
    assert p._password == "s3cret"
    assert p._account == "dept42"


async def test_netrc_account_forwarded_to_login(netrc_file):
    fake = MagicMock()
    fake.stream = SimpleNamespace()
    fake.connect = AsyncMock()
    fake.login = AsyncMock()
    with patch("asanypath.ftp.aioftp.Client", return_value=fake):
        await FTPPath("ftp://ftp.example.com/x")._ensure_client()
    fake.login.assert_awaited_once_with("alice", "s3cret", account="dept42")


@pytest.mark.parametrize(
    "url, kwargs, expected_user, expected_password",
    [
        pytest.param("ftp://bob:other@ftp.example.com/x", {}, "bob", "other", id="url-overrides"),
        pytest.param(
            "ftp://ftp.example.com/x", {"password": "kw"}, "alice", "kw", id="kwarg-overrides"
        ),
    ],
)
def test_explicit_credentials_override_netrc(
    netrc_file, url, kwargs, expected_user, expected_password
):
    p = FTPPath(url, **kwargs)
    assert p._user == expected_user
    assert p._password == expected_password


def test_netrc_missing_falls_back_to_anonymous(monkeypatch):
    monkeypatch.setenv("NETRC", "/nonexistent/netrc")
    FTPPath._env_config = None
    p = FTPPath("ftp://anyhost/x")
    assert p._user == "anonymous"
    assert p._password == "anonymous@"
    assert p._account is None


def test_netrc_parse_error_warns_and_falls_back(tmp_path, monkeypatch):
    bad = tmp_path / "netrc"
    bad.write_text("garbage that won't parse\n")
    bad.chmod(0o600)
    monkeypatch.setenv("NETRC", str(bad))
    FTPPath._env_config = None
    # Reset the module-level "warned" flag so this test sees the warning.
    import asanypath.ftp as ftp_mod

    monkeypatch.setattr(ftp_mod, "_NETRC_WARNED", False)
    with pytest.warns(UserWarning, match="netrc could not be parsed"):
        p = FTPPath("ftp://anyhost/x")
    assert p._user == "anonymous"


# ----------------------------------------------------------------------
# Interactive login retry
# ----------------------------------------------------------------------


def test_login_retries_with_prompted_password_when_interactive(monkeypatch):
    from asanypath import ftp as ftp_mod

    monkeypatch.setattr(ftp_mod, "_interactive", lambda: True)
    monkeypatch.setattr(ftp_mod.getpass, "getpass", lambda msg="": "secret-pw")

    first = MagicMock()
    first.connect = AsyncMock()
    first.login = AsyncMock(side_effect=_status_error("530"))
    first.close = MagicMock()
    second = MagicMock()
    second.connect = AsyncMock()
    second.login = AsyncMock()
    clients = iter([first, second])

    monkeypatch.setattr(ftp_mod.aioftp, "Client", lambda **kw: next(clients))

    import asyncio as _asyncio

    p = FTPPath("ftp://anyhost/x")
    result = _asyncio.run(p._ensure_client())
    assert result is second
    second.login.assert_awaited_once_with("anonymous", "secret-pw")
    first.close.assert_called_once()


def test_login_does_not_retry_when_not_interactive(monkeypatch):
    from asanypath import ftp as ftp_mod

    monkeypatch.setattr(ftp_mod, "_interactive", lambda: False)

    bad = MagicMock()
    bad.connect = AsyncMock()
    bad.login = AsyncMock(side_effect=_status_error("530"))
    bad.close = MagicMock()
    monkeypatch.setattr(ftp_mod.aioftp, "Client", lambda **kw: bad)

    import asyncio as _asyncio

    p = FTPPath("ftp://anyhost/x")
    with pytest.raises(aioftp.StatusCodeError):
        _asyncio.run(p._ensure_client())
    bad.close.assert_called_once()


# ----------------------------------------------------------------------
# Credstore wiring
# ----------------------------------------------------------------------


def test_ftp_uses_cached_password_skips_prompt(monkeypatch):
    """Cache hit: login with cached pw, no prompt."""
    from asanypath import _credstore
    from asanypath import ftp as ftp_mod

    monkeypatch.setattr(ftp_mod, "_interactive", lambda: True)
    monkeypatch.setattr(_credstore, "load", lambda key, **_: "cached-pw")
    monkeypatch.setattr(
        ftp_mod.getpass,
        "getpass",
        lambda msg="": (_ for _ in ()).throw(RuntimeError("should not prompt")),
    )
    c = MagicMock()
    c.connect = AsyncMock()
    c.login = AsyncMock()
    monkeypatch.setattr(ftp_mod.aioftp, "Client", lambda **kw: c)

    import asyncio as _asyncio

    result = _asyncio.run(FTPPath("ftp://alice@host/x")._ensure_client())
    assert result is c
    c.login.assert_awaited_once_with("alice", "cached-pw")


def test_ftp_stale_cache_forgets_and_reprompts(monkeypatch):
    """Cache hit + 530 → forget, prompt, login, save."""
    from asanypath import _credstore
    from asanypath import ftp as ftp_mod

    monkeypatch.setattr(ftp_mod, "_interactive", lambda: True)
    monkeypatch.setattr(_credstore, "load", lambda key, **_: "stale-pw")
    forgotten: list = []
    saved: list = []
    monkeypatch.setattr(_credstore, "forget", lambda k: forgotten.append(k))
    monkeypatch.setattr(_credstore, "save", lambda k, v: saved.append((k, v)))
    monkeypatch.setattr(ftp_mod.getpass, "getpass", lambda msg="": "fresh-pw")

    first = MagicMock()
    first.connect = AsyncMock()
    first.login = AsyncMock(side_effect=_status_error("530"))
    first.close = MagicMock()
    second = MagicMock()
    second.connect = AsyncMock()
    second.login = AsyncMock()
    clients = iter([first, second])
    monkeypatch.setattr(ftp_mod.aioftp, "Client", lambda **kw: next(clients))

    import asyncio as _asyncio

    result = _asyncio.run(FTPPath("ftp://alice@host/x")._ensure_client())
    assert result is second
    assert len(forgotten) == 1
    assert len(saved) == 1 and saved[0][1] == "fresh-pw"
    second.login.assert_awaited_once_with("alice", "fresh-pw")
