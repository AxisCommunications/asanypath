# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

import asyncio
import fnmatch
import re
from collections.abc import AsyncIterator
from datetime import datetime
from email.utils import parsedate_to_datetime
from os import PathLike, getenv, sep
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, Any, NamedTuple, TypeVar
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from typing import Self

from anyio import Path as AioPath
from yarl import URL

from asanypath.exceptions import InvalidPathError, UnsupportedProtocolError

T = TypeVar("T", bound="CommonPurePathMixin")

SCHEME_SEP = "://"
OS_SEP = sep

# Upper bound on concurrent per-entry probes when a listing lacks metadata.
_FILL_CONCURRENCY = 32


class DirEntry(NamedTuple):
    """One directory listing entry with the metadata the backend reported.

    ``None`` means the field is unknown: the listing did not include it and
    it was not requested (or could not be fetched) when filling entries.
    """

    path: Any
    is_dir: bool | None = None
    size: int | None = None
    mtime: float | None = None


def parse_listing_mtime(text: str | None) -> float | None:
    """Parse an ISO-8601 or RFC 1123 listing timestamp to a POSIX float."""
    if not text:
        return None
    try:
        if text.endswith("GMT"):
            return parsedate_to_datetime(text).timestamp()
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _needs_fill(entry: DirEntry, stat: bool) -> bool:
    return entry.is_dir is None or (
        stat and not entry.is_dir and (entry.size is None or entry.mtime is None)
    )


async def _fill_entry(entry: DirEntry, stat: bool) -> DirEntry:
    """Probe the fields of *entry* the listing left unknown."""
    path = entry.path
    is_dir = entry.is_dir
    if is_dir is None:
        try:
            is_dir = await path.is_dir()
        except (OSError, NotImplementedError, ValueError):
            pass
    size, mtime = entry.size, entry.mtime
    if stat and not is_dir and (size is None or mtime is None):
        try:
            st = await path.stat()
        except (OSError, NotImplementedError, ValueError):
            pass
        else:
            if size is None:
                size = getattr(st, "st_size", None)
            if mtime is None:
                mtime = getattr(st, "st_mtime", None)
    return DirEntry(path, is_dir, size, mtime)


# ---------------------------------------------------------------------------
# Cached regex patterns and base URL resolution (avoid repeated work)
# ---------------------------------------------------------------------------

# Pre-compiled: strip "file:" prefix variants
_FILE_PREFIX_RE: re.Pattern = re.compile(r"^file:/{,2}")
# Pre-compiled: collapse 3+ slashes
_MULTI_SLASH_RE: re.Pattern = re.compile(r"/{3,}")
# Pre-compiled: strip trailing slashes (preserves ://)
_TRAILING_SLASH_RE: re.Pattern = re.compile(r"(?<!:)/+$")

# Per-protocol compiled regex: matches "protocol://" prefix (case-insensitive).
# Bounded by number of protocols (~6 entries max).
_SCHEME_RE_CACHE: dict[str, re.Pattern] = {}

# Per-protocol resolved base URL prefix (from env vars).
# Bounded by number of protocols (~6 entries max).
# _BASE_URL_PREFIX_SENTINEL distinguishes "resolved to None" from "not yet resolved".
_BASE_URL_PREFIX_SENTINEL = object()
_BASE_URL_PREFIX_CACHE: dict[str, str | None | object] = {}


def _get_scheme_re(protocol: str) -> re.Pattern:
    """Get or compile the scheme-matching regex for a protocol."""
    pat = _SCHEME_RE_CACHE.get(protocol)
    if pat is None:
        pat = re.compile(rf"^({protocol}:/{{,2}})?", re.IGNORECASE)
        _SCHEME_RE_CACHE[protocol] = pat
    return pat


def _get_base_url_prefix(protocol: str, aliases: dict[str, tuple[str, ...]]) -> str | None:
    """Resolve and cache the base URL prefix for a protocol from env vars.

    Cache is bounded by number of protocols (finite, small).
    """
    cached = _BASE_URL_PREFIX_CACHE.get(protocol, _BASE_URL_PREFIX_SENTINEL)
    if cached is not _BASE_URL_PREFIX_SENTINEL:
        return cached  # type: ignore[return-value]
    env_names = [
        f"{protocol.upper()}_BASE_URL",
        *aliases.get(protocol, ()),
    ]
    base_url = next((getenv(name) for name in env_names if getenv(name)), None)
    if not base_url:
        _BASE_URL_PREFIX_CACHE[protocol] = None
        return None
    parsed = urlsplit(base_url if "://" in base_url else f"{protocol}://{base_url}")
    if not parsed.netloc:
        _BASE_URL_PREFIX_CACHE[protocol] = None
        return None
    base_path = parsed.path.strip("/")
    result = f"{parsed.netloc}/{base_path}" if base_path else parsed.netloc
    _BASE_URL_PREFIX_CACHE[protocol] = result
    return result


class CommonPurePathMixin:
    """Common implementation for pure path manipulation methods shared by all path types."""

    protocol: str = "common"  # Default protocol, should be overridden by subclasses

    _BASE_URL_ENV_ALIASES: dict[str, tuple[str, ...]] = {
        "art": ("ARTIFACTORY_BASE_URL", "ART_BASE_URL"),
    }

    # Explicit constructor options (credentials, endpoint, ...) that derived
    # paths (``/``, ``parent``, ``with_name``, ...) carry over. Backends set
    # this per instance; it is never shared between independent paths.
    _init_options: dict[str, Any] = {}
    # True when the options authenticate against the host named in the URL,
    # so they must not follow a derived path to a different authority.
    _options_bound_to_authority: bool = False

    def __init__(
        self,
        part: str | Path | AioPath | CommonPurePathMixin | None = None,
        *parts: str | Path | AioPath | CommonPurePathMixin,
    ) -> None:
        """Parse path parts into internal representation."""
        if not parts and part is None:
            raise InvalidPathError("Cannot construct path from None or empty parts")
        if self.protocol == "file":
            # Fast path — skip regex and URL expansion
            s = str(part)
            if s.startswith("file:"):
                s = _FILE_PREFIX_RE.sub("", s, count=1)
            self._path = Path(s, *parts)
            return
        part = self._expand_protocol_base_url(str(part))
        scheme_re = _get_scheme_re(self.protocol)
        part = scheme_re.sub(f"{self.protocol}{SCHEME_SEP}", part, count=1)
        if part.endswith(SCHEME_SEP) and parts:
            part = part[:-1]
        elif part.endswith(":"):
            part = part + OS_SEP
        joined = OS_SEP.join(map(str, (part, *parts)))
        joined = _MULTI_SLASH_RE.sub("//", joined)
        joined = _TRAILING_SLASH_RE.sub("", joined)
        # Preserve pre-encoded query strings (e.g. AWS SigV4 %2F separators in
        # presigned URLs) — yarl would otherwise percent-decode them and break
        # the signature.
        self._path = URL(joined, encoded="?" in joined)

    def _derive(self, *parts: Any) -> Self:
        """Build a path from *parts* that keeps this path's explicit options."""
        options = self._init_options
        if options and self._options_bound_to_authority and not self._keeps_authority(parts):
            probe = object.__new__(type(self))
            CommonPurePathMixin.__init__(probe, *parts)
            if probe._path.raw_authority != self._path.raw_authority:
                options = {}
        return type(self)(*parts, **options)

    def _keeps_authority(self, parts: tuple) -> bool:
        """True when a path built from *parts* provably has this path's
        authority, so :meth:`_derive` can skip parsing a probe path."""
        auth = self._path.raw_authority
        if not auth or not parts:
            return False
        if parts[0] == self.drive:
            return len(parts) > 1 and parts[1] == auth
        # Base-URL expansion of the leading part only rewrites bare shorthand
        # hosts; a qualified authority survives joining unchanged.
        return parts[0] == str(self) and ("." in auth or ":" in auth or auth == "localhost")

    def __repr__(self) -> str:
        return f"{type(self).__name__}('{self}')"

    def __str__(self) -> str:
        return str(self._path)

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            return self._path == other._path
        if isinstance(other, str):
            return str(self) == other
        return False

    def __hash__(self) -> int:
        return hash(self._path)

    def __lt__(self, other) -> bool:
        if isinstance(other, (CommonPurePathMixin, str)):
            return str(self) < str(other)
        return NotImplemented

    def __le__(self, other) -> bool:
        if isinstance(other, (CommonPurePathMixin, str)):
            return str(self) <= str(other)
        return NotImplemented

    def __gt__(self, other) -> bool:
        if isinstance(other, (CommonPurePathMixin, str)):
            return str(self) > str(other)
        return NotImplemented

    def __ge__(self, other) -> bool:
        if isinstance(other, (CommonPurePathMixin, str)):
            return str(self) >= str(other)
        return NotImplemented

    def __truediv__(self, other: T | str | PathLike[str]) -> Self:
        if isinstance(other, self.__class__):
            other = other.parts[bool(other.root) :]
        elif isinstance(other, str):
            if SCHEME_SEP in other and not other.startswith(self.protocol):
                raise UnsupportedProtocolError(f"Cannot join {self.protocol} path with {other}")
            other = [other.replace(self.protocol + SCHEME_SEP, "", 1)]
        elif isinstance(other, PurePath) and not other.is_absolute():
            # e.g. the result of relative_to(): dst_root / src.relative_to(src_root)
            other = [other.as_posix()]
        else:
            raise UnsupportedProtocolError(f"Cannot join {self.protocol} path with {other}")

        return self._derive(str(self), *other)

    def __rtruediv__(self, other: T | str | PathLike[str]) -> Self:
        if not isinstance(other, type(self)):
            other = self._derive(other)
        return other / self

    def absolute(self) -> Self:
        return self if self.protocol != "file" else type(self)(self._path.absolute())

    @property
    def anchor(self) -> str:
        return self.drive + self.root

    def as_posix(self) -> str:
        return str(self).replace(f"{self.protocol}{SCHEME_SEP}", OS_SEP)

    def as_uri(self) -> str:
        return str(self) if self.protocol != "file" else f"{self.protocol}{SCHEME_SEP}{self}"

    @property
    def drive(self) -> str:
        return self.protocol + SCHEME_SEP if self.protocol != "file" else ""

    def full_match(self, pattern: str, *, case_sensitive: bool | None = None) -> bool:
        if not pattern:
            raise ValueError("empty pattern")
        repat = (
            fnmatch.translate(pattern.replace("**", "@@@"))
            .replace(".*", "[^/]*")
            .replace("@@@", ".*")
        )
        flags = bool(case_sensitive) * re.IGNORECASE
        return re.match(repat, str(self), flags) is not None

    def is_absolute(self) -> bool:
        return bool(self.anchor)

    def is_relative_to(self, other: str | PathLike[str]) -> bool:
        try:
            self._relative_part(other)
            return True
        except ValueError:
            return False

    def joinpath(self, *other: T | str) -> Self:
        return self.__truediv__(*other)

    def match(self, pattern: str, *, case_sensitive: bool | None = None) -> bool:
        if not pattern:
            raise ValueError("empty pattern")
        repat = fnmatch.translate(pattern.replace("**", "*")).replace(".*", "[^/]*")
        flags = bool(case_sensitive) * re.IGNORECASE
        return re.match(f".*(?:{repat})", str(self), flags) is not None

    @property
    def name(self) -> str:
        return self.parts[-1]

    def _parent(self, idx: int = 1) -> Self:
        remaining = self.parts[:-idx] or [self.drive]
        return self._derive(*remaining)

    parent = property(_parent)

    @property
    def parents(self) -> tuple[Self, ...]:
        return tuple(self._parent(idx) for idx in range(1, len(self.parts) - bool(self.drive)))

    @property
    def parts(self) -> tuple[str, ...]:
        match self.protocol:
            case "file":
                drive, parts = [], self._path.parts
            case _:
                drive = [self.drive]
                parts = [p for p in str(self._path).split(SCHEME_SEP)[1].split(OS_SEP) if p]
        return (*drive, *parts)

    def _relative_part(self, other: str | PathLike[str], *, walk_up: bool = False) -> Any:
        """Return the part of this path below *other* as a relative path.

        Remote paths are compared segment by segment below the same scheme and
        bucket/host, so ``s3://b/ab`` is not relative to ``s3://b/a``. As in
        pathlib, a path that is not below *other* raises ``ValueError`` (also for
        another backend: a different anchor) and a non-path raises ``TypeError``.
        """
        from asanypath.sync import _SyncProxy

        if isinstance(other, _SyncProxy):
            other = other._async
        if not isinstance(other, (str, PathLike)):
            raise TypeError(
                f"argument should be a str or an os.PathLike object, not {type(other).__name__!r}"
            )
        error = ValueError(f"{self} is not in the subpath of {other}")
        if self.protocol == "file":
            if isinstance(other, CommonPurePathMixin) and other.protocol != "file":
                raise error  # a remote path has no os.fspath() for pathlib to compare
            return self._path.relative_to(other)
        base = str(other)
        if not base.lower().startswith(self.protocol + SCHEME_SEP):
            raise error  # relative, or another protocol
        # Parse without the subclass __init__: no credentials are needed to compare paths
        probe = object.__new__(type(self))
        CommonPurePathMixin.__init__(probe, base)
        mine, theirs = self.parts, probe.parts
        if mine[:2] != theirs[:2]:  # different bucket/host: no common anchor
            raise error
        common = 0
        while common < min(len(mine), len(theirs)) and mine[common] == theirs[common]:
            common += 1
        if common < len(theirs) and not walk_up:
            raise error
        # URLs normalize "..", so unlike pathlib *other* never has segments that cannot be walked
        return PurePosixPath(*[".."] * (len(theirs) - common), *mine[common:])

    def relative_to(
        self, other: str | PathLike[str], *, walk_up: bool = False
    ) -> Self | PurePosixPath:
        """Return this path relative to *other*.

        A relative remote path has no bucket or host, so it is returned as a
        :class:`~pathlib.PurePosixPath`; join it onto any path with ``/``.
        """
        if self.protocol == "file":
            return self._derive(self._relative_part(other, walk_up=walk_up))
        return self._relative_part(other, walk_up=walk_up)

    @property
    def root(self) -> str:
        try:
            default_root = str(self._path.host)
        except AttributeError:
            default_root = self._path.root
        return default_root

    @property
    def stem(self) -> str:
        return self.name.rsplit(".", 1)[0]

    @property
    def suffix(self) -> str:
        try:
            return self.suffixes[-1]
        except IndexError:
            return ""

    @property
    def suffixes(self) -> list[str]:
        _, *suffixes = self.name.rsplit(".")
        return list(map(".{}".format, suffixes))

    def with_name(self, name: str) -> Self:
        return self.parent / name

    def with_parent(self, parent: str | PathLike[str] | Self) -> Self:
        return self._derive(parent) / self.name

    def with_segments(self, *pathsegments: str | PathLike[str] | Self) -> Self:
        return self._derive(*pathsegments)

    def with_stem(self, stem: str) -> Self:
        return self.parent / (stem + self.suffix)

    def with_suffix(self, suffix: str) -> Self:
        return self.parent / (self.stem + suffix)

    async def _list_entries(self, *, stat: bool = False, fresh: bool = False) -> list[DirEntry]:
        """Return this directory's entries with whatever metadata one listing
        call yields. Backends whose listing reports types, sizes or times
        override this; the default knows only the child paths.
        """
        return [DirEntry(child) async for child in self.iterdir()]  # type: ignore[attr-defined]

    async def scandir(self, *, stat: bool = False) -> AsyncIterator[DirEntry]:
        """Yield :class:`DirEntry` items for this directory.

        ``is_dir`` is always resolved. With ``stat=True``, ``size`` and
        ``mtime`` are resolved for non-directories too. Fields the listing
        already reported cost nothing; the rest are probed concurrently.
        """
        entries = await self._list_entries(stat=stat)
        missing = [i for i, e in enumerate(entries) if _needs_fill(e, stat)]
        if missing:
            limit = asyncio.Semaphore(_FILL_CONCURRENCY)

            async def fill(entry: DirEntry) -> DirEntry:
                async with limit:
                    return await _fill_entry(entry, stat)

            filled = await asyncio.gather(*(fill(entries[i]) for i in missing))
            for i, entry in zip(missing, filled):
                entries[i] = entry
        for entry in entries:
            yield entry

    def _resolve_base_url_prefix(self) -> str | None:
        """Resolve base URL host/path prefix for this protocol from environment.

        Uses module-level cache bounded by protocol count (~6 entries).
        """
        return _get_base_url_prefix(self.protocol, self._BASE_URL_ENV_ALIASES)

    def _expand_protocol_base_url(self, part: str) -> str:
        """Expand shorthand protocol URLs using configured base URL.

        Example with ARTIFACTORY_BASE_URL=artifacts.example.com/artifactory:
        art://repo/path -> art://artifacts.example.com/artifactory/repo/path
        art:// -> art://artifacts.example.com/artifactory
        """
        if self.protocol == "file":
            return part

        base_prefix = self._resolve_base_url_prefix()

        # Convert http(s):// URLs that match our base URL to our protocol scheme
        if base_prefix and self.protocol not in ("http", "https"):
            for http_scheme in ("https://", "http://"):
                if part.lower().startswith(http_scheme):
                    rest = part[len(http_scheme) :]
                    bp = base_prefix.rstrip("/")
                    if rest.rstrip("/") == bp or rest.startswith(f"{bp}/"):
                        return f"{self.protocol}{SCHEME_SEP}{rest}"

        if not base_prefix:
            return part

        scheme_re = _get_scheme_re(self.protocol)
        normalized = scheme_re.sub(f"{self.protocol}{SCHEME_SEP}", part, count=1)
        normalized_rest = normalized.split(SCHEME_SEP, 1)[1].lstrip("/")
        base_prefix = base_prefix.rstrip("/")
        if not normalized_rest:
            # The bare root (e.g. ``art://``) is the configured service root.
            return f"{self.protocol}{SCHEME_SEP}{base_prefix}"
        if normalized_rest == base_prefix or normalized_rest.startswith(f"{base_prefix}/"):
            return normalized

        parsed = urlsplit(normalized)
        host = parsed.netloc
        # Heuristic: only expand shorthand hosts (e.g., repository names),
        # not fully qualified hostnames, localhost, or explicit host:port.
        if not host or "." in host or ":" in host or host == "localhost":
            return normalized

        rel_path = f"{host}{parsed.path}".lstrip("/")
        expanded = f"{self.protocol}{SCHEME_SEP}{base_prefix}/{rel_path}".rstrip("/")
        if parsed.query:
            expanded = f"{expanded}?{parsed.query}"
        if parsed.fragment:
            expanded = f"{expanded}#{parsed.fragment}"
        return expanded
