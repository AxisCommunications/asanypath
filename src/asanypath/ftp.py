# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""FTP / FTPS path implementation using :mod:`aioftp` (optional dependency).

URL format::

    ftp://[user[:password]@]host[:port]/path/to/file
    ftps://[user[:password]@]host[:port]/path/to/file   # explicit TLS (AUTH TLS)

Recommended secret store: ``~/.netrc`` (stdlib :mod:`netrc`; honored when no
password is in the URL/kwargs). Password env vars are deliberately **not**
supported.

When stdin is a TTY and the server rejects the initial login, the backend
prompts once via :mod:`getpass` and retries. Set ``ASANYPATH_INTERACTIVE=0``
to force non-interactive behavior in scripts.

Non-secret env defaults (used only when the URL doesn't supply)::

    FTP_USER  (default: "anonymous")
    FTP_PORT  (default: 21 for both ftp and ftps)
    FTP_HOST

``ftps://`` is **explicit** FTPS (RFC 4217, ``AUTH TLS`` over port 21). For
implicit FTPS pass ``port=990`` — the same TLS context applies. lftp-style
bookmarks (``~/.lftp/rc``) are not consulted; ``~/.netrc`` is the only
credential store.

Install this backend with ``pip install asanypath[ftp]``.

FTP is a deprecated protocol with no atomic rename, no guaranteed range reads,
and no rich metadata.  This implementation embraces the "any" in
:mod:`asanypath` only for the motivated user — the operations that cannot be
implemented reliably raise :class:`NotImplementedError` rather than silently
degrading:

* :meth:`FTPPath.replace`     — FTP rename is not atomic
* :meth:`FTPPath._range_read` — REST support is server-dependent
* :meth:`FTPPath.open`        — no random-access file handles

Connections (one logged-in :class:`aioftp.Client` each) are pooled per
``(host, port, user, tls)`` at module level.  Each operation is serialized
behind a per-pool :class:`asyncio.Lock` since a single FTP control channel
cannot multiplex commands.  Call :func:`disconnect_all` to close everything.
"""

from __future__ import annotations

import asyncio
import getpass
import netrc
import sys
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from os import getenv
from types import SimpleNamespace
from typing import TYPE_CHECKING, TypeVar

import aioftp

from asanypath.cloud import CloudPathMixin
from asanypath.options import AccessGrant, AccessPolicy, AccessPolicyPatch, BackendOptions

if TYPE_CHECKING:
    from typing import Self


T = TypeVar("T")

_CLIENT_CACHE: dict[tuple, aioftp.Client] = {}
_CLIENT_LOCKS: dict[tuple, asyncio.Lock] = {}


def disconnect_all() -> None:
    """Close every cached FTP client."""
    for client in list(_CLIENT_CACHE.values()):
        try:
            # ``quit`` is the polite shutdown but it's a coroutine; in a
            # synchronous cleanup we can only close the underlying streams.
            client.close()
        except Exception:  # noqa: BLE001  # pragma: no cover
            pass  # pragma: no cover
    _CLIENT_CACHE.clear()
    _CLIENT_LOCKS.clear()


_UNSET = object()


def _interactive() -> bool:
    """Whether the backend may prompt the user for credentials."""
    if getenv("ASANYPATH_INTERACTIVE") == "0":
        return False
    try:
        return sys.stdin.isatty()
    except (AttributeError, ValueError):  # pragma: no cover
        return False  # pragma: no cover


async def _prompt_password(label: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, getpass.getpass, f"Password for {label}: ")


def _parse_mlst_mtime(modify: str | None) -> float | None:
    """Parse an MLST ``modify`` timestamp (``YYYYMMDDHHMMSS[.ffffff]``)."""
    if not modify:
        return None
    fmt = "%Y%m%d%H%M%S.%f" if "." in modify else "%Y%m%d%H%M%S"
    return datetime.strptime(modify, fmt).replace(tzinfo=timezone.utc).timestamp()


_NETRC_WARNED = False


def _netrc_lookup(host: str | None) -> tuple[str | None, str | None, str | None]:
    """Return ``(login, password, account)`` for ``host`` from ``~/.netrc``.

    Honors ``$NETRC`` like curl/requests. Returns ``(None, None, None)`` when
    the file is missing or the host has no entry. Emits a single warning when
    the file exists but cannot be parsed (e.g., unsafe permissions).
    """
    if not host:
        return (None, None, None)
    path = getenv("NETRC") or None
    try:
        rc = netrc.netrc(path)
    except FileNotFoundError:
        return (None, None, None)
    except netrc.NetrcParseError as exc:
        global _NETRC_WARNED
        if not _NETRC_WARNED:
            warnings.warn(
                f"~/.netrc could not be parsed ({exc}); falling back to anonymous FTP.",
                stacklevel=2,
            )
            _NETRC_WARNED = True
        return (None, None, None)
    auth = rc.authenticators(host)
    if auth is None:
        return (None, None, None)
    login, account, password = auth
    return (login, password, account)


class FTPPath(CloudPathMixin):
    """Async FTP path backed by :mod:`aioftp`."""

    protocol: str = "ftp"
    _tls: bool = False
    _default_port: int = 21
    _supports_range_read: bool = False

    def __init__(
        self,
        *parts,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        account: str | None = None,
    ) -> None:
        super().__init__(*parts)
        cfg = self.env_config
        self._host = host or self._path.host or cfg.host
        self._port = port or self._path.port or cfg.port
        url_pw = self._path.password
        url_user = self._path.user
        # Username precedence: kwarg > URL > netrc > env default.
        # Password precedence: kwarg > URL > netrc > anonymous default.
        netrc_user, netrc_pw, netrc_account = _netrc_lookup(self._host)
        self._user = username or url_user or netrc_user or cfg.user
        if password is not None:
            self._password = password
        elif url_pw is not None:
            self._password = url_pw
        elif netrc_pw is not None and (username is None and url_user is None):
            # Only use netrc password when we also took the netrc username,
            # so we don't pair the wrong credential with an overridden user.
            self._password = netrc_pw
        elif self._user in (None, "anonymous"):
            self._password = "anonymous@"
        else:
            # Real user but no password: leave None so the keyring cache or
            # the interactive prompt can supply one.
            self._password = None
        self._account = account if account is not None else netrc_account
        # Promote explicit params into class cache so derived instances
        # (parent, /, with_name, ...) inherit them.
        if host:
            cfg.host = host
        if port:
            cfg.port = port
        if username:
            cfg.user = username

    @classmethod
    def _create_env_config(cls) -> SimpleNamespace:
        port = getenv("FTP_PORT")
        return SimpleNamespace(
            host=getenv("FTP_HOST"),
            port=int(port) if port else cls._default_port,
            user=getenv("FTP_USER") or "anonymous",
        )

    # ------------------------------------------------------------------
    # CloudPathMixin hooks
    # ------------------------------------------------------------------

    @property
    def _item_path(self) -> str:
        return self._path.path or "/"

    @property
    def _native_kwargs(self) -> dict:
        return {"host": self._host, "port": self._port, "user": self._user, "tls": self._tls}

    # ------------------------------------------------------------------
    # Connection pool
    # ------------------------------------------------------------------

    @property
    def _client_key(self) -> tuple:
        return (self._host, self._port, self._user, self._tls)

    async def _ensure_client(self) -> aioftp.Client:
        key = self._client_key
        client = _CLIENT_CACHE.get(key)
        if client is not None and client.stream is not None:
            return client
        from asanypath import _credstore

        cred_key = f"ftp:{self._host or ''}:{self._port or ''}:{self._user or ''}"
        password = self._password
        used_cache = False
        if password is None and _interactive():
            cached = _credstore.load(cred_key)
            if cached is not None:
                password = cached
                used_cache = True
        try:
            client = await self._connect_and_login(password)
        except aioftp.StatusCodeError:
            if used_cache:
                _credstore.forget(cred_key)
            if not _interactive():
                raise
            label = f"{self._user}@{self._host}" if self._user else self._host or "ftp"
            prompted = await _prompt_password(label)
            client = await self._connect_and_login(prompted)
            _credstore.save(cred_key, prompted)
        _CLIENT_CACHE[key] = client
        return client

    async def _connect_and_login(self, password: str | None) -> aioftp.Client:
        client = aioftp.Client(ssl=True if self._tls else None)
        await client.connect(self._host, self._port)
        try:
            if self._account is not None:
                await client.login(self._user, password, account=self._account)
            else:
                await client.login(self._user, password)
        except BaseException:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass
            raise
        return client

    async def _op(self, fn: Callable[[aioftp.Client], Awaitable[T]]) -> T:
        """Run ``fn(client)`` serialized on the per-pool lock."""
        key = self._client_key
        lock = _CLIENT_LOCKS.setdefault(key, asyncio.Lock())
        async with lock:
            client = await self._ensure_client()
            return await fn(client)

    # ------------------------------------------------------------------
    # Stat / type checks
    # ------------------------------------------------------------------

    async def exists(self) -> bool:
        return await self._op(lambda c: c.exists(self._item_path))

    async def is_dir(self) -> bool:
        try:
            return await self._op(lambda c: c.is_dir(self._item_path))
        except aioftp.StatusCodeError:
            return False

    async def is_file(self) -> bool:
        try:
            return await self._op(lambda c: c.is_file(self._item_path))
        except aioftp.StatusCodeError:
            return False

    async def is_symlink(self) -> bool:
        # FTP has no portable symlink probe.
        return False

    async def stat(self, *, follow_symlinks: bool = True):
        info = await self._op(lambda c: c.stat(self._item_path))
        size = info.get("size")
        mode_raw = info.get("unix.mode")
        return SimpleNamespace(
            st_size=int(size) if size is not None else 0,
            st_mtime=_parse_mlst_mtime(info.get("modify")),
            st_atime=None,
            st_ctime=_parse_mlst_mtime(info.get("create") or info.get("modify")),
            st_mode=int(mode_raw, 8) if isinstance(mode_raw, str) else mode_raw,
            st_uid=None,
            st_gid=None,
        )

    async def get_access_policy(
        self, *, backend_options: BackendOptions | None = None
    ) -> AccessPolicy:
        """Return mode grants when the server exposes MLST ``unix.mode``."""
        info = await self._op(lambda client: client.stat(self._item_path))
        mode_raw = info.get("unix.mode")
        if not isinstance(mode_raw, str):
            raise NotImplementedError("FTP server does not expose POSIX mode metadata")
        mode = int(mode_raw, 8)

        def actions(shift: int) -> frozenset[str]:
            return frozenset(
                action
                for action, bit in (("read", 4), ("write", 2), ("execute", 1))
                if mode >> shift & bit
            )

        return AccessPolicy(
            grants=(
                AccessGrant("owner", actions(6)),
                AccessGrant("group", actions(3)),
                AccessGrant("everyone", actions(0)),
            )
        )

    async def update_access_policy(
        self, policy_patch: AccessPolicyPatch, *, backend_options: BackendOptions | None = None
    ) -> None:
        """Apply mode grants with the optional FTP ``SITE CHMOD`` extension."""
        if policy_patch.owner is not None or policy_patch.group is not None:
            raise NotImplementedError("FTP does not support portable ownership changes")
        if not policy_patch.grants:
            return
        info = await self._op(lambda client: client.stat(self._item_path))
        mode_raw = info.get("unix.mode")
        if not isinstance(mode_raw, str):
            raise NotImplementedError("FTP server does not expose POSIX mode metadata")
        mode = int(mode_raw, 8)
        shifts = {"owner": 6, "group": 3, "everyone": 0}
        for grant in policy_patch.grants:
            if grant.principal not in shifts:
                raise ValueError(f"unsupported FTP policy principal: {grant.principal}")
            bits = sum({"read": 4, "write": 2, "execute": 1}[action] for action in grant.actions)
            shift = shifts[grant.principal]
            mode = mode & ~(0o7 << shift) | (bits << shift)
        command = f"SITE CHMOD {mode & 0o777:03o} {self._item_path}"
        await self._op(lambda client: client.command(command, "2xx"))

    async def checksums(self) -> dict[str, str]:
        from hashlib import md5, sha1, sha256

        data = await self.read_bytes()
        return {
            "md5": md5(data).hexdigest(),
            "sha1": sha1(data).hexdigest(),
            "sha256": sha256(data).hexdigest(),
        }

    # ------------------------------------------------------------------
    # Read / write
    # ------------------------------------------------------------------

    async def read_bytes(self) -> bytes:
        path = self._item_path

        async def _read(client: aioftp.Client) -> bytes:
            async with client.download_stream(path) as stream:
                return await stream.read()

        return await self._op(_read)

    async def write_bytes(self, data: bytes) -> int:
        path = self._item_path

        async def _write(client: aioftp.Client) -> int:
            async with client.upload_stream(path) as stream:
                await stream.write(data)
            return len(data)

        return await self._op(_write)

    async def _range_read(self, start: int, end: int) -> bytes:
        raise NotImplementedError(
            f"{type(self).__name__}._range_read() is unreliable on FTP "
            "(REST support is server-dependent); use read_bytes()."
        )

    # ------------------------------------------------------------------
    # Directory / unlink / rename
    # ------------------------------------------------------------------

    async def unlink(self, missing_ok: bool = False) -> None:
        path = self._item_path
        try:
            await self._op(lambda c: c.remove_file(path))
        except aioftp.StatusCodeError as e:
            if missing_ok and _is_missing(e):
                return
            if _is_missing(e):
                raise FileNotFoundError(2, f"No such file: '{self}'") from e
            raise

    async def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        path = self._item_path
        if exist_ok and await self._op(lambda c: c.exists(path)):
            return
        try:
            await self._op(lambda c: c.make_directory(path, parents=parents))
        except aioftp.StatusCodeError as e:
            if exist_ok:
                return
            raise FileExistsError(17, f"File exists: '{self}'") from e

    async def rmdir(self, *, recursive: bool = False) -> None:
        path = self._item_path
        if recursive:
            await self._op(lambda c: c.remove(path))
            return
        await self._op(lambda c: c.remove_directory(path))

    async def rename(self, target: str | Self, *, force: bool = False) -> Self:
        target_path = target if isinstance(target, type(self)) else type(self)(str(target))
        from asanypath._transfer import async_destination_state, require_force

        needs_copy, same_path, destination_exists = await async_destination_state(self, target_path)
        if not needs_copy:
            if same_path:
                return target_path
            await self.unlink()
            return target_path
        if destination_exists:
            if not force:
                require_force(force, target_path)
            return await super().rename(str(target), force=True)
        if (target_path._host, target_path._port) == (self._host, self._port):
            src, dst = self._item_path, target_path._item_path
            await self._op(lambda c: c.rename(src, dst))
            return target_path
        return await super().rename(str(target))

    async def replace(self, target: str | Self) -> Self:
        raise NotImplementedError(
            f"{type(self).__name__}.replace() is not supported: FTP rename is "
            "not atomic and cannot guarantee replace semantics. Use rename() "
            "after explicit unlink() if you accept the race."
        )

    async def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        if await self.exists():
            if not exist_ok:
                raise FileExistsError(17, f"File exists: '{self}'")
            # FTP has no portable MFMT/UTIME; rewrite a zero-byte payload only if the
            # file is already empty would still be a lie. Refuse rather than fake it.
            return
        await self.write_bytes(b"")

    async def iterdir(self, *, fresh: bool = False) -> AsyncIterator[Self]:
        path = self._item_path

        async def _collect(client: aioftp.Client) -> list:
            return [item async for item in client.list(path)]

        try:
            entries = await self._op(_collect)
        except aioftp.StatusCodeError as e:
            raise NotADirectoryError(20, f"Not a directory: '{self}'") from e
        for entry_path, _info in entries:
            yield self / entry_path.name

    async def walk(
        self,
        top_down: bool = True,
        on_error=None,
        follow_symlinks: bool = False,
    ) -> AsyncIterator[tuple[Self, list[str], list[str]]]:
        path = self._item_path

        async def _collect(client: aioftp.Client) -> list:
            return [item async for item in client.list(path)]

        try:
            entries = await self._op(_collect)
        except aioftp.StatusCodeError as e:
            if on_error is not None:
                on_error(e)
                return
            raise
        dirs: list[Self] = []
        files: list[Self] = []
        for entry_path, info in entries:
            child = self / entry_path.name
            if info.get("type") == "dir":
                dirs.append(child)
            else:
                files.append(child)
        if top_down:
            yield self, [d.name for d in dirs], [f.name for f in files]
        for d in dirs:
            async for triple in d.walk(
                top_down=top_down, on_error=on_error, follow_symlinks=follow_symlinks
            ):
                yield triple
        if not top_down:
            yield self, [d.name for d in dirs], [f.name for f in files]

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        raise NotImplementedError(
            f"{type(self).__name__}.open() not implemented; "
            "use read_bytes/write_bytes/read_text/write_text."
        )


class FTPSPath(FTPPath):
    """FTP over explicit TLS (FTPES, AUTH TLS)."""

    protocol: str = "ftps"
    _tls: bool = True
    _default_port: int = 21


def _is_missing(err: aioftp.StatusCodeError) -> bool:
    """True if the server reply carries a 550 'no such file' code."""
    return any(str(c) == "550" for c in err.received_codes)
