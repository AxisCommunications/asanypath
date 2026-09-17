# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for HTTPPath auth and HTTPResponse."""

from __future__ import annotations

from base64 import b64encode
from unittest.mock import AsyncMock, patch

import pytest

from asanypath.http import HTTPPath, HTTPResponse, _build_auth_headers

# ---------------------------------------------------------------------------
# _build_auth_headers
# ---------------------------------------------------------------------------


class TestBuildAuthHeaders:
    def test_no_env_vars(self, monkeypatch):
        monkeypatch.delenv("HTTP_AUTH_USER", raising=False)
        monkeypatch.delenv("HTTP_AUTH_PASSWORD", raising=False)
        monkeypatch.delenv("HTTP_AUTH_TOKEN", raising=False)
        monkeypatch.delenv("HTTP_AUTH_HEADER", raising=False)
        assert _build_auth_headers() == []

    @pytest.mark.parametrize("password", ["secret", ""])
    def test_basic_auth(self, monkeypatch, password):
        monkeypatch.setenv("HTTP_AUTH_USER", "alice")
        monkeypatch.setenv("HTTP_AUTH_PASSWORD", password)
        monkeypatch.delenv("HTTP_AUTH_TOKEN", raising=False)
        headers = _build_auth_headers()
        expected = b64encode(f"alice:{password}".encode()).decode("ascii")
        assert headers == [("authorization", f"Basic {expected}")]

    def test_bearer_auth(self, monkeypatch):
        monkeypatch.delenv("HTTP_AUTH_USER", raising=False)
        monkeypatch.delenv("HTTP_AUTH_PASSWORD", raising=False)
        monkeypatch.setenv("HTTP_AUTH_TOKEN", "mytoken123")
        monkeypatch.delenv("HTTP_AUTH_HEADER", raising=False)
        headers = _build_auth_headers()
        assert headers == [("authorization", "Bearer mytoken123")]

    def test_custom_header_auth(self, monkeypatch):
        monkeypatch.delenv("HTTP_AUTH_USER", raising=False)
        monkeypatch.delenv("HTTP_AUTH_PASSWORD", raising=False)
        monkeypatch.setenv("HTTP_AUTH_TOKEN", "key-abc")
        monkeypatch.setenv("HTTP_AUTH_HEADER", "X-API-Key")
        headers = _build_auth_headers()
        assert headers == [("X-API-Key", "key-abc")]

    def test_basic_takes_priority_over_bearer(self, monkeypatch):
        monkeypatch.setenv("HTTP_AUTH_USER", "bob")
        monkeypatch.setenv("HTTP_AUTH_PASSWORD", "pass")
        monkeypatch.setenv("HTTP_AUTH_TOKEN", "ignored")
        headers = _build_auth_headers()
        assert headers[0][0] == "authorization"
        assert headers[0][1].startswith("Basic ")


# ---------------------------------------------------------------------------
# HTTPResponse
# ---------------------------------------------------------------------------


class TestHTTPResponse:
    @pytest.mark.parametrize("status, ok", [(200, True), (404, False)])
    def test_ok(self, status, ok):
        resp = HTTPResponse(status_code=status, body=b"", headers={})
        assert resp.ok is ok

    def test_json(self):
        resp = HTTPResponse(status_code=200, body=b'{"key": "value"}', headers={})
        assert resp.json() == {"key": "value"}

    @pytest.mark.parametrize(
        "body, encoding, expected",
        [
            ("héllo".encode(), None, "héllo"),
            ("hello".encode("latin-1"), "latin-1", "hello"),
        ],
    )
    def test_text(self, body, encoding, expected):
        resp = HTTPResponse(status_code=200, body=body, headers={})
        kwargs = {"encoding": encoding} if encoding else {}
        assert resp.text(**kwargs) == expected


# ---------------------------------------------------------------------------
# HTTPPath auth integration
# ---------------------------------------------------------------------------


class TestHTTPPathAuth:
    @pytest.mark.parametrize(
        "env_token, input_headers, check",
        [
            # no env, no input -> no merging needed
            (None, None, lambda merged: merged is None),
            # token env, no input -> auth header injected
            ("tok", None, lambda merged: ("authorization", "Bearer tok") in merged),
            # token env + input authorization -> input wins, single auth header
            (
                "tok",
                {"authorization": "Custom xyz"},
                lambda merged: (
                    [v for k, v in merged if k.lower() == "authorization"] == ["Custom xyz"]
                ),
            ),
        ],
        ids=["no-auth", "env-only", "input-overrides-env"],
    )
    def test_merge_headers(self, monkeypatch, env_token, input_headers, check):
        monkeypatch.delenv("HTTP_AUTH_USER", raising=False)
        monkeypatch.delenv("HTTP_AUTH_PASSWORD", raising=False)
        monkeypatch.delenv("HTTP_AUTH_HEADER", raising=False)
        if env_token is None:
            monkeypatch.delenv("HTTP_AUTH_TOKEN", raising=False)
        else:
            monkeypatch.setenv("HTTP_AUTH_TOKEN", env_token)
        p = HTTPPath("http://example.com/file")
        assert check(p._merge_headers(input_headers))

    async def test_exists_passes_auth_headers(self, monkeypatch):
        monkeypatch.setenv("HTTP_AUTH_TOKEN", "tok")
        monkeypatch.delenv("HTTP_AUTH_USER", raising=False)
        monkeypatch.delenv("HTTP_AUTH_HEADER", raising=False)
        p = HTTPPath("http://example.com/file")
        with patch("asanypath.http.http_exists", new=AsyncMock(return_value=True)) as mock:
            result = await p.exists()
        assert result is True
        _, kwargs = mock.call_args
        assert ("authorization", "Bearer tok") in kwargs["headers"]

    async def test_read_bytes_passes_auth_headers(self, monkeypatch):
        monkeypatch.setenv("HTTP_AUTH_TOKEN", "secret")
        monkeypatch.delenv("HTTP_AUTH_USER", raising=False)
        monkeypatch.delenv("HTTP_AUTH_HEADER", raising=False)
        p = HTTPPath("http://example.com/data")
        with patch("asanypath.http.http_get", new=AsyncMock(return_value=b"content")) as mock:
            data = await p.read_bytes()
        assert data == b"content"
        _, kwargs = mock.call_args
        assert ("authorization", "Bearer secret") in kwargs["headers"]

    async def test_request_returns_httpresponse(self, monkeypatch):
        monkeypatch.delenv("HTTP_AUTH_USER", raising=False)
        monkeypatch.delenv("HTTP_AUTH_TOKEN", raising=False)
        p = HTTPPath("http://example.com/api")
        with patch(
            "asanypath.http.http_request",
            new=AsyncMock(return_value=(200, b'{"ok":true}', {"content-type": "application/json"})),
        ):
            resp = await p.request("GET")
        assert isinstance(resp, HTTPResponse)
        assert resp.status_code == 200
        assert resp.ok is True
        assert resp.json() == {"ok": True}

    async def test_request_json_kwarg(self, monkeypatch):
        monkeypatch.delenv("HTTP_AUTH_USER", raising=False)
        monkeypatch.delenv("HTTP_AUTH_TOKEN", raising=False)
        p = HTTPPath("http://example.com/api")
        with patch(
            "asanypath.http.http_request",
            new=AsyncMock(return_value=(201, b"", {})),
        ) as mock:
            resp = await p.request("POST", json={"key": "value"})
        assert resp.status_code == 201
        _, kwargs = mock.call_args
        assert kwargs["body"] == b'{"key":"value"}'
        assert ("content-type", "application/json") in kwargs["headers"]

    async def test_request_non_2xx_does_not_raise(self, monkeypatch):
        monkeypatch.delenv("HTTP_AUTH_USER", raising=False)
        monkeypatch.delenv("HTTP_AUTH_TOKEN", raising=False)
        p = HTTPPath("http://example.com/missing")
        with patch(
            "asanypath.http.http_request",
            new=AsyncMock(return_value=(404, b"not found", {})),
        ):
            resp = await p.request("GET")
        assert resp.status_code == 404
        assert resp.ok is False
