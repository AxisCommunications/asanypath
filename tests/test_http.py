# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for HTTPPath and helpers (assemble_xml_chunks, _TagCollector)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from asanypath.http import HTTPPath, HTTPSPath, _TagCollector, assemble_xml_chunks

_HTTP_NATIVE = "asanypath.http"


def _hp(url: str = "https://example.com/file.txt") -> HTTPPath:
    return HTTPPath(url)


# ---------------------------------------------------------------------------
# _TagCollector
# ---------------------------------------------------------------------------


def test_tag_collector_collects_matching_tags():
    tc = _TagCollector({"Name": "name", "Size": "size"})
    tc.start("Name", {})
    tc.data("hello")
    tc.end("Name")
    tc.start("Size", {})
    tc.data("42")
    tc.end("Size")
    result = tc.close()
    assert ("name", "hello") in result
    assert ("size", "42") in result


def test_tag_collector_ignores_non_matching_tags():
    tc = _TagCollector({"Name": "name"})
    tc.start("Other", {})
    tc.data("ignored")
    tc.end("Other")
    assert tc.close() == []


def test_tag_collector_concatenates_split_data():
    tc = _TagCollector({"Key": "key"})
    tc.start("Key", {})
    tc.data("part1")
    tc.data("part2")
    tc.end("Key")
    assert tc.close() == [("key", "part1part2")]


# ---------------------------------------------------------------------------
# assemble_xml_chunks
# ---------------------------------------------------------------------------


async def test_assemble_xml_chunks_single_tag():
    xml = b"<root><Key>hello</Key></root>"
    results = [pair async for pair in assemble_xml_chunks([xml], tags="Key")]
    assert results == [("Key", "hello")]


async def test_assemble_xml_chunks_with_namespace():
    ns = "http://example.com"
    xml = f'<root xmlns="{ns}"><Name>val</Name></root>'.encode()
    results = [pair async for pair in assemble_xml_chunks([xml], ns=ns, tags=["Name"])]
    assert results == [("Name", "val")]


async def test_assemble_xml_chunks_multiple_chunks():
    chunk1 = b"<root><K>a</K><K"
    chunk2 = b">b</K></root>"
    results = [pair async for pair in assemble_xml_chunks([chunk1, chunk2], tags=["K"])]
    assert [v for _, v in results] == ["a", "b"]


# ---------------------------------------------------------------------------
# request
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method,body",
    [
        ("GET", None),
        ("HEAD", None),
        ("DELETE", None),
        ("OPTIONS", None),
        ("PUT", b"data"),
        ("POST", b"data"),
        ("PATCH", b"data"),
    ],
    ids=["get", "head", "delete", "options", "put", "post", "patch"],
)
async def test_request_dispatches_verb(method, body):
    p = _hp()
    mock_fn = AsyncMock(return_value=(200, b"ok", {}))
    with patch(f"{_HTTP_NATIVE}.http_request", new=mock_fn):
        resp = await p.request(method, "https://x.com", data=body)
        assert resp.body == b"ok"
        assert resp.status_code == 200
        mock_fn.assert_awaited_once()
        assert mock_fn.call_args[0][0] == method


async def test_request_passes_headers_as_list():
    p = _hp()
    mock_fn = AsyncMock(return_value=(200, b"", {}))
    with patch(f"{_HTTP_NATIVE}.http_request", new=mock_fn):
        await p.request("GET", "https://x.com", headers={"X-A": "1"})
        call_kw = mock_fn.call_args.kwargs
        assert ("X-A", "1") in call_kw["headers"]


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


async def test_exists():
    p = _hp()
    with patch(f"{_HTTP_NATIVE}.http_exists", new=AsyncMock(return_value=True)):
        assert await p.exists() is True


def test_presigned_url_query_preserved():
    """Percent-encoded query chars (e.g. AWS SigV4 %2F) must survive construction."""
    url = (
        "https://example.com/bucket/key.txt"
        "?X-Amz-Credential=AKIA%2F20260603%2Fus-east-1%2Fs3%2Faws4_request"
        "&X-Amz-Signature=abc123"
    )
    assert str(HTTPSPath(url)) == url


@pytest.fixture
def _head_forbidden_server():
    """Local HTTP server: HEAD → 403, GET → 200 (mimics S3 presigned-GET URL)."""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):  # silence
            pass

        def do_HEAD(self):
            self.send_response(403)
            self.end_headers()

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "5")
            self.end_headers()
            self.wfile.write(b"hello")

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}/file.txt"
    srv.shutdown()


async def test_exists_falls_back_from_head_to_get_on_403(_head_forbidden_server):
    """Presigned URLs sign one method only; HEAD→ranged-GET fallback handles it."""
    assert await HTTPPath(_head_forbidden_server).exists() is True


# ---------------------------------------------------------------------------
# read_bytes / read_text
# ---------------------------------------------------------------------------


async def test_read_bytes():
    p = _hp()
    with patch(f"{_HTTP_NATIVE}.http_get", new=AsyncMock(return_value=b"data")):
        assert await p.read_bytes() == b"data"


@pytest.mark.parametrize(
    "raw,newline,expected",
    [
        (b"a\r\nb\rc", None, "a\nb\nc"),
        (b"a\r\nb", "", "a\r\nb"),
        (b"a\r\nb", "\r\n", "a\nb"),
    ],
    ids=["default_normalise", "empty_passthrough", "custom_newline"],
)
async def test_read_text_newline(raw, newline, expected):
    p = _hp()
    with patch(f"{_HTTP_NATIVE}.http_get", new=AsyncMock(return_value=raw)):
        assert await p.read_text(newline=newline) == expected


# ---------------------------------------------------------------------------
# write_bytes / write_text
# ---------------------------------------------------------------------------


async def test_write_bytes():
    p = _hp()
    with patch(f"{_HTTP_NATIVE}.http_put", new=AsyncMock()):
        n = await p.write_bytes(b"hello")
        assert n == 5


async def test_write_bytes_wraps_error():
    p = _hp()
    with patch(f"{_HTTP_NATIVE}.http_put", new=AsyncMock(side_effect=RuntimeError("boom"))):
        with pytest.raises(OSError, match="Failed to write"):
            await p.write_bytes(b"x")


@pytest.mark.parametrize(
    "text,newline,expected_bytes",
    [
        ("a\nb", None, None),  # uses os.linesep — just check it calls write_bytes
        ("a\nb", "\r\n", b"a\r\nb"),
        ("a\nb", "", b"a\nb"),
        ("a\nb", "\n", b"a\nb"),
    ],
    ids=["default_linesep", "crlf", "empty_passthrough", "lf_noop"],
)
async def test_write_text(text, newline, expected_bytes):
    p = _hp()
    with patch.object(p, "write_bytes", new=AsyncMock(return_value=5)) as mock_wb:
        await p.write_text(text, newline=newline)
        mock_wb.assert_awaited_once()
        if expected_bytes is not None:
            assert mock_wb.call_args[0][0] == expected_bytes


# ---------------------------------------------------------------------------
# rename / replace
# ---------------------------------------------------------------------------


async def test_rename():
    p = _hp("https://example.com/old.txt")
    with (
        patch.object(HTTPPath, "exists", new=AsyncMock(return_value=False)),
        patch.object(HTTPPath, "read_bytes", new=AsyncMock(return_value=b"content")),
        patch.object(HTTPPath, "write_bytes", new=AsyncMock(return_value=7)),
        patch.object(HTTPPath, "unlink", new=AsyncMock()),
    ):
        new = await p.rename("https://example.com/new.txt")
        assert str(new).endswith("new.txt")


async def test_replace_delegates_to_rename():
    p = _hp()
    with patch.object(p, "rename", new=AsyncMock(return_value="done")) as mock_rename:
        result = await p.replace("https://other.com/f.txt")
        mock_rename.assert_awaited_once_with("https://other.com/f.txt", force=True)
        assert result == "done"


# ---------------------------------------------------------------------------
# touch
# ---------------------------------------------------------------------------


async def test_touch_success():
    p = _hp()
    with (
        patch(f"{_HTTP_NATIVE}.http_put", new=AsyncMock()),
        patch(f"{_HTTP_NATIVE}.http_exists", new=AsyncMock(return_value=False)),
    ):
        await p.touch()  # should not raise


async def test_touch_exist_ok_false_raises():
    p = _hp()
    with patch(f"{_HTTP_NATIVE}.http_exists", new=AsyncMock(return_value=True)):
        with pytest.raises(FileExistsError):
            await p.touch(exist_ok=False)


async def test_touch_custom_mode_raises():
    p = _hp()
    with pytest.raises(NotImplementedError, match="Custom mode"):
        await p.touch(mode=0o777)


# ---------------------------------------------------------------------------
# unlink
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exists_val,missing_ok,should_raise",
    [
        (True, False, False),
        (False, True, False),
        (False, False, True),
    ],
    ids=["exists_ok", "missing_ok", "missing_raises"],
)
async def test_unlink(exists_val, missing_ok, should_raise):
    p = _hp()
    with (
        patch(f"{_HTTP_NATIVE}.http_exists", new=AsyncMock(return_value=exists_val)),
        patch(f"{_HTTP_NATIVE}.http_delete", new=AsyncMock()),
    ):
        if should_raise:
            with pytest.raises(FileNotFoundError):
                await p.unlink(missing_ok=missing_ok)
        else:
            await p.unlink(missing_ok=missing_ok)


async def test_unlink_wraps_generic_error():
    p = _hp()
    with (
        patch(f"{_HTTP_NATIVE}.http_exists", new=AsyncMock(return_value=True)),
        patch(f"{_HTTP_NATIVE}.http_delete", new=AsyncMock(side_effect=RuntimeError("boom"))),
    ):
        with pytest.raises(OSError, match="Failed to delete"):
            await p.unlink()


# ---------------------------------------------------------------------------
# checksums
# ---------------------------------------------------------------------------


async def test_checksums():
    p = _hp()
    with patch(f"{_HTTP_NATIVE}.http_get", new=AsyncMock(return_value=b"hello")):
        result = await p.checksums()
        assert "md5" in result and "sha1" in result and "sha256" in result


# ---------------------------------------------------------------------------
# NotImplementedError stubs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method,args",
    [
        ("mkdir", ()),
        ("rmdir", ()),
    ],
    ids=["mkdir", "rmdir"],
)
async def test_not_implemented(method, args):
    p = _hp()
    coro_or_gen = getattr(p, method)(*args)
    with pytest.raises(NotImplementedError):
        if hasattr(coro_or_gen, "__anext__"):
            await coro_or_gen.__anext__()
        else:
            await coro_or_gen


def test_open_not_implemented():
    p = _hp()
    with pytest.raises(NotImplementedError, match="does not support open"):
        p.open()


# ---------------------------------------------------------------------------
# HTTPSPath
# ---------------------------------------------------------------------------


def test_https_protocol():
    p = HTTPSPath("https://example.com/file.txt")
    assert p.protocol == "https"


# ---------------------------------------------------------------------------
# Directory listing (iterdir / walk / glob)
# ---------------------------------------------------------------------------


async def test_is_dir_requires_listing():
    p = HTTPPath("http://h/d/", listing=None)
    with pytest.raises(ValueError, match="listing=None"):
        await p.is_dir()


async def test_is_dir_default_is_auto():
    # listing defaults to "auto" → no error, returns based on trailing slash
    assert await HTTPPath("http://h/d/").is_dir() is True
    assert HTTPPath("http://h/d/")._listing == "auto"


async def test_is_dir_trailing_slash_with_listing():
    assert await HTTPPath("http://h/d/", listing="python").is_dir() is True
    assert await HTTPPath("http://h/d", listing="python").is_dir() is False


async def test_iterdir_requires_listing():
    p = HTTPPath("http://h/d/", listing=None)
    with pytest.raises(ValueError, match="listing=None"):
        async for _ in p.iterdir():
            pass


async def test_iterdir_preset_python():
    p = HTTPPath("http://h/d/", listing="python")
    fake = AsyncMock(return_value=["http://h/d/a.txt", "http://h/d/sub/"])
    with patch(f"{_HTTP_NATIVE}.http_scrape_links", new=fake):
        children = [c async for c in p.iterdir()]
    # str() strips trailing slashes; check trailing-slash hint instead
    assert [str(c) for c in children] == ["http://h/d/a.txt", "http://h/d/sub"]
    assert [c._trailing_slash for c in children] == [False, True]
    # selector/attr from preset
    assert fake.call_args.args[1] == "li a[href]"
    assert fake.call_args.kwargs["attr"] == "href"


async def test_iterdir_custom_selector_and_attr():
    p = HTTPPath("http://h/d/", listing="a.entry", listing_attr="data-href")
    fake = AsyncMock(return_value=["http://h/d/x"])
    with patch(f"{_HTTP_NATIVE}.http_scrape_links", new=fake):
        [c async for c in p.iterdir()]
    assert fake.call_args.args[1] == "a.entry"
    assert fake.call_args.kwargs["attr"] == "data-href"


async def test_iterdir_appends_trailing_slash():
    p = HTTPPath("http://h/d", listing="python")
    fake = AsyncMock(return_value=[])
    with patch(f"{_HTTP_NATIVE}.http_scrape_links", new=fake):
        [c async for c in p.iterdir()]
    assert fake.call_args.args[0] == "http://h/d/"


async def test_iterdir_propagates_listing_to_children():
    p = HTTPPath("http://h/d/", listing="apache", listing_attr="href")
    fake = AsyncMock(return_value=["http://h/d/sub/"])
    with patch(f"{_HTTP_NATIVE}.http_scrape_links", new=fake):
        children = [c async for c in p.iterdir()]
    assert children[0]._listing == "apache"
    assert children[0]._listing_attr == "href"


async def test_walk_recurses():
    p = HTTPPath("http://h/d/", listing="python")

    async def fake_scrape(url, *_a, **_kw):
        return {
            "http://h/d/": ["http://h/d/a.txt", "http://h/d/sub/"],
            "http://h/d/sub/": ["http://h/d/sub/b.txt"],
        }[url]

    with patch(f"{_HTTP_NATIVE}.http_scrape_links", new=AsyncMock(side_effect=fake_scrape)):
        triples = [(str(r), ds, fs) async for r, ds, fs in p.walk()]
    assert triples == [
        ("http://h/d", ["sub"], ["a.txt"]),
        ("http://h/d/sub", [], ["b.txt"]),
    ]


async def test_walk_top_down_false():
    p = HTTPPath("http://h/d/", listing="python")

    async def fake_scrape(url, *_a, **_kw):
        return {
            "http://h/d/": ["http://h/d/a.txt", "http://h/d/sub/"],
            "http://h/d/sub/": ["http://h/d/sub/b.txt"],
        }[url]

    with patch(f"{_HTTP_NATIVE}.http_scrape_links", new=AsyncMock(side_effect=fake_scrape)):
        roots = [str(r) async for r, _, _ in p.walk(top_down=False)]
    assert roots == ["http://h/d/sub", "http://h/d"]


async def test_walk_on_error():
    p = HTTPPath("http://h/d/", listing="python")

    async def boom(url, *_a, **_kw):
        raise PermissionError("nope")

    seen = []
    with patch(f"{_HTTP_NATIVE}.http_scrape_links", new=AsyncMock(side_effect=boom)):
        triples = [t async for t in p.walk(on_error=seen.append)]
    assert triples == []
    assert len(seen) == 1
    assert isinstance(seen[0], PermissionError)


async def test_glob_matches_descendants():
    p = HTTPPath("http://h/d/", listing="python")

    async def fake_scrape(url, *_a, **_kw):
        return {
            "http://h/d/": ["http://h/d/a.txt", "http://h/d/b.log", "http://h/d/sub/"],
            "http://h/d/sub/": ["http://h/d/sub/c.txt"],
        }[url]

    with patch(f"{_HTTP_NATIVE}.http_scrape_links", new=AsyncMock(side_effect=fake_scrape)):
        matches = sorted([str(m) async for m in p.glob("*.txt")])
    assert matches == ["http://h/d/a.txt", "http://h/d/sub/c.txt"]


async def test_asanypath_factory_forwards_listing_kwarg():
    from asanypath import AsAnyPath

    p = AsAnyPath("http://h/d/", listing="python")
    assert isinstance(p, HTTPPath)
    assert p._listing == "python"


# ---------------------------------------------------------------------------
# Integration: real python -m http.server (exercises Rust scraping end-to-end)
# ---------------------------------------------------------------------------


@pytest.fixture
def http_server(tmp_path):
    """Spin up `python -m http.server` rooted at a temp dir; return base URL."""
    import socket
    import subprocess
    import sys
    import time
    import urllib.request

    # pick a free port
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    # build a small tree
    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "b.log").write_text("beta")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "c.txt").write_text("gamma")

    proc = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(port), "--bind", "127.0.0.1"],
        cwd=tmp_path,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base = f"http://127.0.0.1:{port}/"
    try:
        # wait until reachable
        for _ in range(50):
            try:
                urllib.request.urlopen(base, timeout=0.2).read()
                break
            except Exception:
                time.sleep(0.05)
        else:
            raise RuntimeError("http.server did not start")
        yield base
    finally:
        proc.terminate()
        proc.wait(timeout=5)


async def test_integration_iterdir_python_server(http_server):
    p = HTTPPath(http_server, listing="python")
    names = sorted(str(c).rsplit("/", 1)[-1] or "sub" for c in [c async for c in p.iterdir()])
    # python http.server lists files and a "sub/" dir; trailing slash stripped by str()
    assert "a.txt" in names
    assert "b.log" in names
    assert "sub" in names


async def test_integration_walk_python_server(http_server):
    p = HTTPPath(http_server, listing="python")
    triples = [t async for t in p.walk()]
    # root + one subdir
    assert len(triples) == 2
    root, dirs, files = triples[0]
    assert sorted(files) == ["a.txt", "b.log"]
    assert dirs == ["sub"]
    _, _, sub_files = triples[1]
    assert sub_files == ["c.txt"]


async def test_integration_glob_python_server(http_server):
    p = HTTPPath(http_server, listing="python")
    matches = sorted([str(m).rsplit("/", 1)[-1] async for m in p.glob("*.txt")])
    assert matches == ["a.txt", "c.txt"]


# ---------------------------------------------------------------------------
# stat (HEAD-derived partial metadata)
# ---------------------------------------------------------------------------


async def test_stat_size_and_mtime_from_headers():
    p = _hp()
    fake = AsyncMock(
        return_value={
            "Content-Length": "1234",
            "Last-Modified": "Wed, 21 Oct 2015 07:28:00 GMT",
        }
    )
    with patch(f"{_HTTP_NATIVE}.http_head", new=fake):
        st = await p.stat()
    assert st.st_size == 1234
    assert st.st_mtime == st.st_ctime == 1445412480.0


async def test_stat_missing_headers_yield_none():
    p = _hp()
    with patch(f"{_HTTP_NATIVE}.http_head", new=AsyncMock(return_value={})):
        st = await p.stat()
    assert st.st_size is None
    assert st.st_mtime is None
    assert st.st_ctime is None


async def test_stat_lowercase_header_keys():
    p = _hp()
    fake = AsyncMock(
        return_value={"content-length": "42", "last-modified": "Wed, 21 Oct 2015 07:28:00 GMT"}
    )
    with patch(f"{_HTTP_NATIVE}.http_head", new=fake):
        st = await p.stat()
    assert st.st_size == 42
    assert st.st_mtime == 1445412480.0


async def test_integration_stat_python_server(http_server):
    p = HTTPPath(http_server + "a.txt")
    st = await p.stat()
    # python http.server sends Content-Length and Last-Modified for files
    assert st.st_size == len("alpha")
    assert st.st_mtime is not None and st.st_mtime > 0


# ---------------------------------------------------------------------------
# auto-trailing-slash directory probing
# ---------------------------------------------------------------------------


async def test_integration_is_dir_auto_appends_slash(http_server):
    # URL without trailing slash should still be detected as a directory.
    base_no_slash = http_server.rstrip("/")
    p = HTTPPath(base_no_slash, listing="python")
    assert p._trailing_slash is False
    assert await p.is_dir() is True
    # is_dir() side-effect: trailing slash is remembered on the instance.
    assert p._trailing_slash is True
    assert await p.is_file() is False


async def test_integration_iterdir_without_trailing_slash(http_server):
    base_no_slash = http_server.rstrip("/")
    p = HTTPPath(base_no_slash, listing="python")
    names = sorted(str(c).rsplit("/", 1)[-1] for c in [c async for c in p.iterdir()])
    assert "a.txt" in names
    assert "b.log" in names
    # After iterdir, the instance knows it's a directory.
    assert p._trailing_slash is True
