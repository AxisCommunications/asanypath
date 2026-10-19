# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Transport hardening: redirects, error messages and download-cache files."""

from __future__ import annotations

import os
import stat
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from asanypath import cache as cache_module
from asanypath.http import HTTPPath


def _serve(handler_cls):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


@pytest.fixture
def redirect_servers():
    """``origin`` redirects ``/same`` to itself and ``/cross`` to ``other``;
    both record the headers of every request they receive."""
    seen: dict[str, list[dict[str, str]]] = {"origin": [], "other": []}

    def handler(name, redirects):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                seen[name].append({k.lower(): v for k, v in self.headers.items()})
                if self.path in redirects:
                    self.send_response(302)
                    self.send_header("Location", redirects[self.path])
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"ok")

        return Handler

    other = _serve(handler("other", {}))
    other_url = f"http://127.0.0.1:{other.server_address[1]}"
    origin = _serve(handler("origin", {"/same": "/target", "/cross": f"{other_url}/target"}))
    yield f"http://127.0.0.1:{origin.server_address[1]}", seen
    origin.shutdown()
    other.shutdown()


@pytest.mark.parametrize("header", ["authorization", "x-api-key"])
async def test_cross_origin_redirect_drops_auth_headers(redirect_servers, monkeypatch, header):
    origin, seen = redirect_servers
    monkeypatch.setenv("HTTP_AUTH_TOKEN", "secret-token")
    monkeypatch.setenv("HTTP_AUTH_HEADER", header)

    assert await HTTPPath(f"{origin}/cross").read_bytes() == b"ok"

    assert header in seen["origin"][0]
    assert header not in seen["other"][0]


async def test_same_origin_redirect_keeps_auth_headers(redirect_servers, monkeypatch):
    origin, seen = redirect_servers
    monkeypatch.setenv("HTTP_AUTH_TOKEN", "secret-token")
    monkeypatch.setenv("HTTP_AUTH_HEADER", "x-api-key")

    assert await HTTPPath(f"{origin}/same").read_bytes() == b"ok"

    assert [h.get("x-api-key") for h in seen["origin"]] == ["secret-token"] * 2


async def test_transport_errors_do_not_include_query_strings():
    with pytest.raises(RuntimeError) as excinfo:
        await HTTPPath("http://127.0.0.1:9/blob?sv=1&sig=SECRET-SIGNATURE").read_bytes()

    assert "SECRET-SIGNATURE" not in str(excinfo.value)
    assert "127.0.0.1:9/blob" in str(excinfo.value)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions")
async def test_download_cache_files_are_private(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_module, "_cache", None)
    old_umask = os.umask(0o022)
    try:
        cache_module.configure_cache(directory=str(tmp_path / "cache"))
        cache = cache_module.get_cache()

        async def fetch():
            return b"private"

        await cache.get_or_fetch("key", fetch)
        cache.close()
    finally:
        os.umask(old_umask)

    store = tmp_path / "cache" / "native-v1"
    assert stat.S_IMODE(store.stat().st_mode) == 0o700
    for entry in store.iterdir():
        assert stat.S_IMODE(entry.stat().st_mode) == 0o600, entry.name
