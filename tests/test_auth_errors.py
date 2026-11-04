# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""HTTP 401 raises AuthenticationError with the server's reason and a hint."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from asanypath import AsAnyPathError, AuthenticationError, exceptions
from asanypath.artifactory import ArtifactoryPath
from asanypath.http import HTTPPath

ARTIFACTORY_401 = (
    b'{"errors" : [ {"status" : 401, "message" : "Props Authentication Token not found"} ]}'
)
S3_403 = b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
SERVICE = "art://h.example.com/artifactory"


@pytest.fixture(autouse=True)
def _no_recorded_hints(monkeypatch):
    monkeypatch.setattr(exceptions, "_CREDENTIAL_HINTS", {})
    monkeypatch.setattr(ArtifactoryPath, "_service_tokens", {})
    monkeypatch.setattr(ArtifactoryPath, "_env_config", None)
    monkeypatch.setattr("asanypath._credstore.load", lambda key, **_: None)
    monkeypatch.setattr("asanypath._credstore.save", lambda key, value: None)
    monkeypatch.setattr("asanypath.artifactory._jfrog_conf_token", lambda base_url: None)


@pytest.fixture
def server():
    """GET /artifactory/... -> 401 (Artifactory body), /denied -> 403 (S3 body)."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            status, body = (403, S3_403) if self.path == "/denied" else (401, ARTIFACTORY_401)
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


async def test_401_is_an_authentication_error_with_the_reason(server):
    uri = f"{server}/artifactory/repo/model.bin"
    with pytest.raises(AuthenticationError) as info:
        await HTTPPath(uri).read_bytes()
    err = info.value
    assert isinstance(err, PermissionError) and isinstance(err, AsAnyPathError)
    assert err.uri == uri
    assert str(err).startswith(
        f"Authentication failed (Props Authentication Token not found): '{uri}'"
    )
    # A plain HTTPS URL that looks like Artifactory gets no token: say how to fix that.
    assert "add its host to ARTIFACTORY_BASE_URL" in str(err)


async def test_401_names_the_recorded_credential_source(server):
    exceptions.set_credential_hint(f"{server}/artifactory/", "check THE_TOKEN")
    with pytest.raises(AuthenticationError, match="; check THE_TOKEN$"):
        await HTTPPath(f"{server}/artifactory/repo/x").read_bytes()


async def test_403_stays_a_permission_error_with_the_reason(server):
    with pytest.raises(PermissionError) as info:
        await HTTPPath(f"{server}/denied").read_bytes()
    assert not isinstance(info.value, AuthenticationError)
    assert str(info.value) == f"Permission denied (Access Denied): '{server}/denied'"


def test_hint_of_the_longest_matching_service():
    exceptions.set_credential_hint("art://h", "outer")
    exceptions.set_credential_hint("art://h/artifactory", "inner")
    assert exceptions.credential_hint("art://h/artifactory/repo/x") == "inner"
    assert exceptions.credential_hint("art://h/other") == "outer"
    assert exceptions.credential_hint("art://hx/artifactory") is None
    assert exceptions.credential_hint("s3://bucket/key") is None
    err = AuthenticationError(
        "Authentication failed: 'art://h/artifactory/x'", "art://h/artifactory/x"
    )
    assert str(err).endswith("; inner")


@pytest.mark.parametrize(
    "source,expected",
    [
        ("param", "check the token passed as token="),
        ("env", "check ARTIFACTORY_IDENTITY_TOKEN"),
        ("keyring", "cached in the keyring as 'art:h.example.com/artifactory'"),
        ("jfrog", "from the JFrog CLI config"),
    ],
)
def test_artifactory_records_where_its_token_came_from(monkeypatch, source, expected):
    kwargs = {}
    if source == "param":
        kwargs["token"] = "t"
    elif source == "env":
        monkeypatch.setenv("ARTIFACTORY_IDENTITY_TOKEN", "t")
        monkeypatch.setenv("ARTIFACTORY_BASE_URL", "https://h.example.com/artifactory")
    elif source == "keyring":
        monkeypatch.setattr("asanypath._credstore.load", lambda key, **_: "t")
    else:
        monkeypatch.setattr("asanypath.artifactory._jfrog_conf_token", lambda base_url: "t")
    ArtifactoryPath(f"{SERVICE}/repo/file.bin", **kwargs)
    assert expected in exceptions.credential_hint(f"{SERVICE}/repo/file.bin")
