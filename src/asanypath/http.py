# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

from base64 import b64encode
from collections.abc import AsyncGenerator, AsyncIterator
from os import getenv, linesep
from typing import Literal, TypeVar
from xml.etree.ElementTree import XMLParser

import msgspec

from asanypath.cloud import CloudPathMixin
from asanypath_native import (
    http_delete,
    http_exists,
    http_get,
    http_head,
    http_options,
    http_patch,
    http_post,
    http_put,
    http_scrape_links,
)

try:
    from asanypath_native import http_request
except ImportError:  # pragma: no cover
    http_request = None  # type: ignore[assignment]

T = TypeVar("T", bound="HTTPPath")


class _TagCollector:
    """SAX-style target for stdlib XMLParser that collects matching tag texts."""

    __slots__ = ("_tagmap", "_current", "_text", "results")

    def __init__(self, tagmap: dict[str, str]):
        self._tagmap = tagmap
        self._current: str | None = None
        self._text: list[str] = []
        self.results: list[tuple[str, str]] = []

    def start(self, tag, attrib):
        if tag in self._tagmap:
            self._current = tag
            self._text = []

    def end(self, tag):
        if tag == self._current:
            self.results.append((self._tagmap[tag], "".join(self._text)))
            self._current = None

    def data(self, data):
        if self._current is not None:
            self._text.append(data)

    def close(self):
        return self.results


async def assemble_xml_chunks(
    chunks: list[bytes], ns: str | None = None, tags: str | list[str] = None
) -> AsyncGenerator[tuple[str, str], None]:
    if isinstance(tags, str):
        tags = [tags]
    tagmap = {t: t for t in tags}
    if isinstance(ns, str):
        tagmap = {f"{{{ns}}}{t}": t for t in tags}
    target = _TagCollector(tagmap)
    parser = XMLParser(target=target)
    for chunk in chunks:
        parser.feed(chunk)
        while target.results:
            yield target.results.pop(0)
    parser.close()
    while target.results:  # pragma: no cover
        yield target.results.pop(0)  # pragma: no cover


_NATIVE_VERBS = {
    "GET": http_get,
    "PUT": http_put,
    "POST": http_post,
    "HEAD": http_head,
    "DELETE": http_delete,
    "OPTIONS": http_options,
    "PATCH": http_patch,
}


# ---------------------------------------------------------------------------
# HTTPResponse — structured return from request()
# ---------------------------------------------------------------------------


class HTTPResponse(msgspec.Struct, frozen=True):
    """Structured response from HTTPPath.request()."""

    status_code: int
    body: bytes
    headers: dict[str, str]

    @property
    def ok(self) -> bool:
        """True if status_code is 2xx."""
        return 200 <= self.status_code < 300

    def json(self) -> object:
        """Parse body as JSON."""
        return msgspec.json.decode(self.body)

    def text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """Decode body as text."""
        return self.body.decode(encoding=encoding, errors=errors)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def _build_auth_headers() -> list[tuple[str, str]]:
    """Build auth headers from environment variables.

    Detection order:
    - HTTP_AUTH_USER + HTTP_AUTH_PASSWORD → Basic auth
    - HTTP_AUTH_TOKEN (+ optional HTTP_AUTH_HEADER) → Bearer or custom header
    """
    user = getenv("HTTP_AUTH_USER")
    password = getenv("HTTP_AUTH_PASSWORD")
    if user and password is not None:
        encoded = b64encode(f"{user}:{password}".encode()).decode("ascii")
        return [("authorization", f"Basic {encoded}")]

    token = getenv("HTTP_AUTH_TOKEN")
    if token:
        header_name = getenv("HTTP_AUTH_HEADER", "authorization")
        if header_name.lower() == "authorization":
            return [("authorization", f"Bearer {token}")]
        return [(header_name, token)]

    return []


class HTTPPath(CloudPathMixin):
    """Async path implementation for HTTP, backed by Rust/reqwest.

    Authentication is configured via environment variables:
    - HTTP_AUTH_USER + HTTP_AUTH_PASSWORD → Basic auth
    - HTTP_AUTH_TOKEN → Bearer token (or custom header via HTTP_AUTH_HEADER)

    Auth headers are automatically injected into all requests (read_bytes,
    write_bytes, exists, etc.) and into the explicit request() method.

    Directory listing (``iterdir``/``walk``/``glob``) defaults to a generic
    ``a[href]`` scrape (``listing="auto"``) which works for Python's
    ``http.server``, Apache, and nginx autoindex pages. Pass an explicit
    preset (``"python"``, ``"apache"``, ``"nginx"``) or a raw CSS selector
    for tighter matching, ``listing_attr`` to override the extracted
    attribute (default ``href``), or ``listing=None`` to disable listing
    entirely (in which case dir-style methods raise ``ValueError``).

    Examples:
        AsAnyPath("http://localhost:8000/")  # auto
        AsAnyPath("http://server/dir/", listing="td a[href]")
    """

    protocol: str = "http"

    #: Named selector presets for common directory-listing servers.
    #: Each entry is (css_selector, attribute_name).
    LISTING_PRESETS: dict[str, tuple[str, str]] = {
        "auto": ("a[href]", "href"),
        "python": ("li a[href]", "href"),
        "apache": ("td a[href]", "href"),
        "nginx": ("a[href]", "href"),
    }

    def __init__(
        self,
        *parts,
        listing: str | None = "auto",
        listing_attr: str | None = None,
    ) -> None:
        super().__init__(*parts)
        self._listing = listing
        self._listing_attr = listing_attr
        # Pathlib normalises trailing slashes away; preserve the hint so
        # iterdir/is_dir can distinguish directory-style URLs.
        self._trailing_slash = bool(parts) and str(parts[-1]).endswith("/")

    def _resolve_listing(self) -> tuple[str, str] | None:
        """Return (selector, attr) for the configured listing, or None if unset."""
        if self._listing is None:
            return None
        preset = self.LISTING_PRESETS.get(self._listing)
        if preset is not None:
            sel, attr = preset
            return sel, self._listing_attr or attr
        return self._listing, self._listing_attr or "href"

    def _auth_headers(self) -> list[tuple[str, str]]:
        """Return auth headers for this path. Override for per-instance auth."""
        return _build_auth_headers()

    def _merge_headers(
        self,
        user_headers: dict[str, str] | list[tuple[str, str]] | None,
    ) -> list[tuple[str, str]] | None:
        """Merge auth headers with user-supplied headers."""
        auth = self._auth_headers()
        if not user_headers and not auth:
            return None
        merged = list(auth)
        if user_headers:
            extra = (
                list(user_headers.items()) if isinstance(user_headers, dict) else list(user_headers)
            )
            # User-supplied headers override auth headers with same name
            extra_keys = {ek.lower() for ek, _ in extra}
            merged = [(k, v) for k, v in merged if k.lower() not in extra_keys]
            merged.extend(extra)
        return merged or None

    async def request(
        self,
        method: Literal["GET", "PUT", "POST", "HEAD", "DELETE", "OPTIONS", "PATCH"],
        url: str | None = None,
        *,
        headers: dict[str, str] | list[tuple[str, str]] | None = None,
        data: bytes | None = None,
        json: object | None = None,
    ) -> HTTPResponse:
        """Execute an HTTP request, returning a structured HTTPResponse.

        Unlike read_bytes/write_bytes, this does NOT raise on non-2xx status.

        Args:
            method: HTTP verb.
            url: Target URL. Defaults to str(self).
            headers: Extra headers (merged with auth headers).
            data: Raw body bytes for PUT/POST/PATCH.
            json: JSON-serializable object (sets Content-Type and body).
        """
        if url is None:
            url = str(self)
        if http_request is None:
            raise ImportError(
                "http_request not available; upgrade asanypath-native to use request()"
            )
        if json is not None:
            data = msgspec.json.encode(json)
            headers = dict(headers) if isinstance(headers, dict) else dict(headers or [])
            headers.setdefault("content-type", "application/json")
        merged = self._merge_headers(headers)
        status_code, body, resp_headers = await http_request(
            method,
            url,
            body=data,
            headers=merged,
        )
        return HTTPResponse(status_code=status_code, body=body, headers=resp_headers)

    async def exists(self) -> bool:
        return await http_exists(str(self), headers=self._merge_headers(None))

    async def _probe_dir_slash(self) -> bool:
        """Probe ``url + "/"`` via HEAD; on success, mark self as directory."""
        if self._trailing_slash:
            return True
        if await http_exists(str(self) + "/", headers=self._merge_headers(None)):
            self._trailing_slash = True
            return True
        return False

    async def is_dir(self) -> bool:
        """True if this path looks like a directory listing.

        When ``listing`` is set (default ``"auto"``), URLs ending in ``/``
        are directories. URLs without a trailing slash are probed via HEAD
        on ``url + "/"`` and, on success, transparently treated as
        directories (the trailing slash is remembered on the instance).
        If listing has been explicitly disabled (``listing=None``),
        raises ValueError.
        """
        if self._resolve_listing() is None:
            raise ValueError(
                "HTTPPath.is_dir() called with listing=None; "
                "pass a 'listing' kwarg (e.g. 'auto', 'python', 'apache', "
                "'nginx', or a CSS selector)"
            )
        return await self._probe_dir_slash()

    async def is_file(self) -> bool:
        if self._resolve_listing() is None:
            raise ValueError("HTTPPath.is_file() called with listing=None")
        if self._trailing_slash:
            return False
        if await self._probe_dir_slash():  # pragma: no cover
            return False  # pragma: no cover
        return await self.exists()  # pragma: no cover

    async def iterdir(self) -> AsyncIterator[T]:
        """Yield child paths by scraping links from this URL.

        Uses the path's ``listing`` configuration (default ``"auto"``).
        Each child inherits the same ``listing`` and ``listing_attr`` so
        recursive operations (walk, glob, cp -r, sync) work naturally.
        """
        resolved = self._resolve_listing()
        if resolved is None:
            raise ValueError(
                "HTTPPath.iterdir() called with listing=None; "
                "pass a 'listing' kwarg (e.g. 'auto', 'python', 'apache', "
                "'nginx', or a CSS selector)"
            )
        selector, attr = resolved
        url = str(self)
        if not url.endswith("/"):
            url = url + "/"
        self._trailing_slash = True
        # children retain trailing-slash hint from the link text
        urls = await http_scrape_links(
            url,
            selector,
            attr=attr,
            base_url=url,
            headers=self._merge_headers(None),
        )
        for link in urls:
            yield type(self)(
                link,
                listing=self._listing,
                listing_attr=self._listing_attr,
            )

    async def walk(self) -> AsyncIterator[tuple[T, list[T], list[T]]]:
        """Recursively walk the listing tree, yielding (root, dirs, files)."""
        if self._resolve_listing() is None:
            raise ValueError("HTTPPath.walk() called with listing=None")
        dirs: list[T] = []
        files: list[T] = []
        async for child in self.iterdir():
            if child._trailing_slash:
                dirs.append(child)
            else:
                files.append(child)
        yield self, dirs, files
        for d in dirs:
            async for triple in d.walk():
                yield triple

    async def glob(self, pattern: str, *, case_sensitive: bool | None = None) -> AsyncIterator[T]:
        """Match descendants against a glob pattern (uses ``listing``)."""
        if self._resolve_listing() is None:
            raise ValueError("HTTPPath.glob() called with listing=None")
        async for root, dirs, files in self.walk():
            for entry in (*dirs, *files):
                if entry.match(pattern, case_sensitive=case_sensitive):
                    yield entry

    async def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        raise NotImplementedError

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        raise NotImplementedError(f"{type(self).__name__} does not support open()")

    async def read_bytes(self) -> bytes:
        return await http_get(str(self), headers=self._merge_headers(None))

    async def read_text(
        self,
        encoding: str = "utf-8",
        errors: str = "strict",
        newline: Literal[None, "", "\n", "\r", "\r\n"] = None,
    ) -> str:
        text = (await self.read_bytes()).decode(encoding=encoding, errors=errors)
        match newline:
            case None:
                return text.replace("\r\n", "\n").replace("\r", "\n")
            case "":
                return text
            case _:
                return text.replace(newline, "\n")

    async def rename(self, target: str, *, force: bool = False) -> T:
        return await super().rename(target, force=force)

    async def replace(self, target: str) -> T:
        return await self.rename(target, force=True)

    async def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        if not exist_ok and await self.exists():
            raise FileExistsError(17, f"File exists: '{self}'")
        if mode != 0o666:
            raise NotImplementedError("Custom mode is not supported for HTTPPath touch")
        await self.write_bytes(b"")

    async def unlink(self, missing_ok: bool = False) -> None:
        try:
            if not (missing_ok or await self.exists()):
                raise FileNotFoundError(2, f"No such file or directory: '{self}'")
            await http_delete(str(self), headers=self._merge_headers(None))
        except FileNotFoundError:
            if not missing_ok:
                raise
        except Exception as e:
            raise OSError(f"Failed to delete {self}: {e}") from None

    async def rglob(self, pattern: str, *, case_sensitive: bool | None = None) -> AsyncIterator[T]:
        if self._resolve_listing() is None:
            raise ValueError("HTTPPath.rglob() called with listing=None")
        async for entry in self.glob(pattern, case_sensitive=case_sensitive):
            yield entry

    async def rmdir(self) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support rmdir")

    async def stat(self, *, follow_symlinks: bool = True):
        """Return partial file metadata derived from a HEAD request.

        Only ``st_size`` (from ``Content-Length``) and ``st_mtime``/``st_ctime``
        (from ``Last-Modified``) are populated; HTTP has no equivalent for the
        remaining ``stat`` fields. Returns a ``SimpleNamespace`` matching the
        cloud-backend convention (see ``S3Path.stat``).
        """
        from datetime import datetime, timezone
        from email.utils import parsedate_to_datetime
        from types import SimpleNamespace

        headers = await http_head(str(self), headers=self._merge_headers(None))
        size_raw = headers.get("Content-Length") or headers.get("content-length")
        size = int(size_raw) if size_raw is not None else None
        last_modified = headers.get("Last-Modified") or headers.get("last-modified")
        mtime: float | None = None
        if last_modified:
            try:
                mtime = parsedate_to_datetime(last_modified).timestamp()
            except (TypeError, ValueError):
                try:
                    mtime = (
                        datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S GMT")
                        .replace(tzinfo=timezone.utc)
                        .timestamp()
                    )
                except ValueError:
                    mtime = None
        return SimpleNamespace(st_size=size, st_ctime=mtime, st_mtime=mtime)

    async def checksums(self) -> dict[str, str]:
        from hashlib import md5, sha1, sha256

        data = await self.read_bytes()
        return {
            "md5": md5(data).hexdigest(),
            "sha1": sha1(data).hexdigest(),
            "sha256": sha256(data).hexdigest(),
        }

    async def write_bytes(self, data: bytes) -> int:
        try:
            merged = self._merge_headers(None) or []
            merged.append(("content-length", str(len(data))))
            await http_put(str(self), body=data, headers=merged)
        except Exception as e:
            raise OSError(f"Failed to write to {self}: {e}") from e
        return len(data)

    async def write_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: Literal[None, "", "\n", "\r", "\r\n"] = None,
    ) -> int:
        if newline is None:
            newline = linesep
        if newline not in ("", "\n"):
            data = data.replace("\n", newline)
        encoded_data = data.encode(encoding=encoding or "utf-8", errors=errors or "strict")
        return await self.write_bytes(encoded_data)


class HTTPSPath(HTTPPath):
    """Async path implementation for HTTPS."""

    protocol: str = "https"
