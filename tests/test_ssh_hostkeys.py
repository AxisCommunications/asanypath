# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""SSH host-key checking and session pooling against a loopback SFTP server."""

from __future__ import annotations

import builtins

import pytest

import asanypath.ssh as ssh_mod
from asanypath.ssh import SSHPath, disconnect_all

asyncssh = pytest.importorskip("asyncssh")

PW = "pass" + "word"


class _Server(asyncssh.SSHServer):
    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        return password == "right"


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("SSH_CONFIG", str(tmp_path / "ssh_config"))
    monkeypatch.setenv("ASANYPATH_INTERACTIVE", "0")
    monkeypatch.delenv("SSH_KNOWN_HOSTS", raising=False)
    monkeypatch.delenv("SSH_AUTH_SOCK", raising=False)
    (tmp_path / "f.txt").write_bytes(b"payload")
    SSHPath._env_config = None
    ssh_mod._TRUSTED_HOSTS.clear()
    disconnect_all()
    yield tmp_path
    disconnect_all()
    SSHPath._env_config = None
    ssh_mod._TRUSTED_HOSTS.clear()


@pytest.fixture
def host_key():
    return asyncssh.generate_private_key("ssh-ed25519")


@pytest.fixture
async def server(host_key):
    srv = await asyncssh.listen(
        "127.0.0.1", 0, server_host_keys=[host_key], server_factory=_Server, sftp_factory=True
    )
    yield srv.sockets[0].getsockname()[1]
    srv.close()


def _known_hosts_line(port: int, key) -> str:
    return f"[127.0.0.1]:{port} " + key.export_public_key().decode()


async def _read(home, port, secret="right", **kwargs) -> bytes:
    url = f"ssh://u@127.0.0.1:{port}{home}/f.txt"
    return await SSHPath(url, **{PW: secret}, **kwargs).read_bytes()


async def test_unknown_host_is_refused_by_default(home, server):
    with pytest.raises(RuntimeError, match="Host key verification failed: no host key"):
        await _read(home, server)


async def test_explicit_opt_out_skips_host_key_checking(home, server):
    assert await _read(home, server, known_hosts=()) == b"payload"


async def test_accept_new_from_ssh_config_records_the_key(home, server, host_key):
    (home / "ssh_config").write_text("Host 127.0.0.1\n  StrictHostKeyChecking accept-new\n")
    known_hosts = home / "known"
    with (home / "ssh_config").open("a") as config:
        config.write(f"  UserKnownHostsFile {known_hosts}\n")

    assert await _read(home, server) == b"payload"
    assert known_hosts.read_text().strip() == _known_hosts_line(server, host_key).strip()


async def test_custom_known_hosts_file_is_used(home, server, host_key):
    pinned = home / "pinned"
    pinned.write_text(_known_hosts_line(server, host_key))

    assert await _read(home, server, known_hosts=str(pinned)) == b"payload"


async def test_changed_host_key_is_refused(home, server):
    pinned = home / "pinned"
    other = asyncssh.generate_private_key("ssh-ed25519")
    pinned.write_text(_known_hosts_line(server, other))

    with pytest.raises(RuntimeError, match="REMOTE HOST IDENTIFICATION HAS CHANGED"):
        await _read(home, server, known_hosts=str(pinned))


@pytest.mark.parametrize(("answer", "accepted"), [("no", False), ("yes", True)])
async def test_interactive_prompt_for_unknown_host(home, server, monkeypatch, answer, accepted):
    pinned = home / "pinned"
    prompts: list[str] = []
    monkeypatch.setattr(ssh_mod, "_interactive", lambda: True)
    monkeypatch.setattr(builtins, "input", lambda q: prompts.append(q) or answer)

    if accepted:
        assert await _read(home, server, known_hosts=str(pinned)) == b"payload"
        assert pinned.read_text().strip().startswith(f"[127.0.0.1]:{server} ")
    else:
        with pytest.raises(RuntimeError, match="was not accepted"):
            await _read(home, server, known_hosts=str(pinned))
        assert not pinned.exists()
    assert len(prompts) == 1 and "can't be established" in prompts[0]


async def test_pooled_session_is_not_reused_with_other_credentials(home, server):
    assert await _read(home, server, known_hosts=()) == b"payload"

    with pytest.raises(RuntimeError, match="auth"):
        await _read(home, server, secret="wrong", known_hosts=())
