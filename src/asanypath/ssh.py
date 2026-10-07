# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""SSH/SFTP path implementation backed by a native russh client.

SFTP runs off-GIL in Rust (``asanypath-native``); no Python SSH library is
required at runtime.

URL format::

    ssh://[user@]host[:port]/path/to/file

Recommended: define hosts in ``~/.ssh/config`` and use ``ssh://alias/path``.
Aliases are resolved eagerly via the native OpenSSH config parser; see
:attr:`SSHPath.resolved_target`. Two aliases that resolve to the same
``(host, port, user)`` share one pooled connection.

Authentication uses, in order: an explicit ``password=`` kwarg (also answers
PAM/keyboard-interactive password prompts), a key file (``IdentityFile`` from
``~/.ssh/config``, ``SSH_KEY_FILE``, or the ``client_keys`` kwarg), then the
SSH agent. Password env vars are deliberately **not** supported.

Non-secret env defaults (used only when the URL and ssh_config don't supply):

    SSH_USER         (default: $USER)
    SSH_KEY_FILE     single path; pass a list via kwarg for several
    SSH_KNOWN_HOSTS  "none" to disable host-key check; otherwise a file path.
                     Default: ~/.ssh/known_hosts (strict verification).
    SSH_CONFIG       OpenSSH client config path; "" disables; otherwise
                     ~/.ssh/config and /etc/ssh/ssh_config are consulted
                     when present.

Connections are pooled per resolved ``(host, port, user)`` in the native
layer. Call :func:`disconnect_all` to close everything (tests/shutdown).
"""

from __future__ import annotations

import asyncio
import stat as stat_module
from collections.abc import AsyncIterator
from os import getenv
from pathlib import Path
from time import time
from types import SimpleNamespace
from typing import IO, TYPE_CHECKING, NoReturn, cast, overload

from asanypath_native import (
    ssh_disconnect_all,
    ssh_list,
    ssh_lstat,
    ssh_mkdir,
    ssh_read,
    ssh_read_range,
    ssh_rename,
    ssh_resolve_config,
    ssh_rmdir,
    ssh_setstat,
    ssh_stat,
    ssh_unlink,
    ssh_write,
    ssh_write_chunk,
)
from yarl import URL

from asanypath.cloud import CloudPathMixin
from asanypath.options import (
    UPLOAD_CHUNK_SIZE,
    AccessGrant,
    AccessPolicy,
    AccessPolicyPatch,
    BackendOptions,
)

if TYPE_CHECKING:
    from typing import Self


def disconnect_all() -> None:
    """Close and drop all pooled native SFTP sessions."""
    ssh_disconnect_all()


_UNSET = object()

# Max concurrent in-flight SFTP delete ops during recursive rmdir. Deletes
# pipeline safely over the pooled session (unlike concurrent writes); bounded so
# we don't flood the connection/server.
_RMDIR_CONCURRENCY = 16


@overload
def _map_native_error(exc: Exception, path: str) -> NoReturn: ...
@overload
def _map_native_error(
    exc: Exception, path: str, *, ignore: type[BaseException] | tuple[type[BaseException], ...]
) -> None: ...
def _map_native_error(
    exc: Exception,
    path: str,
    *,
    ignore: type[BaseException] | tuple[type[BaseException], ...] = (),
) -> None:
    """Translate a native russh/SFTP error into the stdlib OSError family.

    Errors whose mapped type matches *ignore* are swallowed (return) instead of
    raised, so callers can express ``missing_ok``/``exist_ok`` without re-catching.
    """
    msg = str(exc).lower()
    mapped: Exception
    if "no such file" in msg or "not found" in msg or "does not exist" in msg:
        mapped = FileNotFoundError(2, "No such file or directory", path)
    elif "permission denied" in msg:
        mapped = PermissionError(13, "Permission denied", path)
    elif "not a directory" in msg:
        mapped = NotADirectoryError(20, "Not a directory", path)
    elif "not empty" in msg:
        mapped = OSError(39, "Directory not empty", path)
    elif "already exists" in msg or "file exists" in msg:
        mapped = FileExistsError(17, "File exists", path)
    else:
        raise exc
    if ignore and isinstance(mapped, ignore):
        return
    raise mapped from exc


def _discover_ssh_config() -> list[str]:
    """Return the list of OpenSSH client config files to consult.

    Honors ``$SSH_CONFIG`` as a single override (empty string disables
    discovery entirely). Otherwise looks for ``~/.ssh/config`` and
    ``/etc/ssh/ssh_config`` in OpenSSH order, returning paths that exist
    or are likely to exist. Missing files are skipped by the resolver.
    """
    override = getenv("SSH_CONFIG")
    if override is not None:
        return [override] if override else []
    # Always include both paths; the native resolver skips missing files.
    paths = [
        str(Path.home() / ".ssh" / "config"),
        "/etc/ssh/ssh_config",
    ]
    return paths


def _resolve_alias(
    host: str | None,
    port: int | None,
    user: str | None,
) -> tuple[str | None, int | None, str | None, str | None]:
    """Expand ``(host, port, user)`` via OpenSSH config; return resolved values
    plus the configured ``IdentityFile`` (for native key auth).

    Falls back to the supplied values when no config is available or no
    match is found. ``port`` / ``user`` come back as ``None`` when neither
    the supplied value nor the config provides one, so the caller can
    decide on an env-level default.
    """
    cfg_paths = _discover_ssh_config()
    if not host or not cfg_paths:
        return host, port, user, None
    try:
        cfg_host, cfg_port, cfg_user, cfg_identity = ssh_resolve_config(host, cfg_paths)
    except Exception:  # noqa: BLE001
        return host, port, user, None
    resolved_host = cfg_host or host
    resolved_port = port or cfg_port
    resolved_user = user or cfg_user
    return resolved_host, resolved_port, resolved_user, cfg_identity


class SSHPath(CloudPathMixin):
    """Async SFTP path backed by a native russh client."""

    protocol: str = "ssh"
    _supports_range_read: bool = True
    # Bytes per streamed chunk; objects below one chunk write in a single call.
    # First chunk truncates, the rest append, so no offset tracking is needed.
    _STREAM_CHUNK = UPLOAD_CHUNK_SIZE

    def __init__(
        self,
        *parts,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        client_keys: list[str] | str | None = None,
        known_hosts: str | tuple | None = _UNSET,  # type: ignore[assignment]
    ) -> None:
        super().__init__(*parts)
        cfg = self.env_config
        # Distinguish "typed" values (kwarg/URL) from env defaults so
        # ssh_config gets a chance to fill in port/user before we fall
        # back to SSH_PORT/SSH_USER.
        url = cast(URL, self._path)
        typed_host = host or url.host
        typed_port = port or url.port
        typed_user = username or url.user
        self._typed_port = typed_port
        self._typed_user = typed_user
        self._host = typed_host or cfg.host
        self._port = typed_port or cfg.port
        self._user = typed_user or cfg.user
        self._password = password if password is not None else cfg.password
        keys = client_keys if client_keys is not None else cfg.client_keys
        self._client_keys = [keys] if isinstance(keys, str) else keys
        self._known_hosts = known_hosts if known_hosts is not _UNSET else cfg.known_hosts
        # Eager alias resolution. str(self) keeps the original URL;
        # .resolved_target surfaces what we connect to; the connection
        # pool keys on the resolved tuple.
        r_host, r_port, r_user, r_identity = _resolve_alias(self._host, typed_port, typed_user)
        self._resolved_host = r_host
        self._resolved_port = r_port if r_port is not None else cfg.port
        self._resolved_user = r_user if r_user is not None else cfg.user
        # ssh_config IdentityFile, used for native key auth when no explicit key.
        self._config_identity = r_identity
        # Promote explicit params into class cache so derived instances
        # (parent, /, with_name, ...) inherit them — mirrors S3/GCS.
        if host:
            cfg.host = host
        if port:
            cfg.port = port
        if username:
            cfg.user = username
        if password is not None:
            cfg.password = password
        if client_keys is not None:
            cfg.client_keys = keys
        if known_hosts is not _UNSET:
            cfg.known_hosts = known_hosts

    def __repr__(self) -> str:
        typed = (self._host, self._port, self._user)
        resolved = (self._resolved_host, self._resolved_port, self._resolved_user)
        if typed == resolved:
            return f"{type(self).__name__}({str(self)!r})"
        return f"{type(self).__name__}({str(self)!r} → {self.resolved_target})"

    @property
    def resolved_target(self) -> str:
        """Connection target after ssh_config alias expansion."""
        user = f"{self._resolved_user}@" if self._resolved_user else ""
        port = (
            f":{self._resolved_port}" if self._resolved_port and self._resolved_port != 22 else ""
        )
        return f"ssh://{user}{self._resolved_host}{port}{self._item_path}"

    @classmethod
    def _create_env_config(cls) -> SimpleNamespace:
        key_file = getenv("SSH_KEY_FILE")
        known = getenv("SSH_KNOWN_HOSTS")
        if known and known.lower() == "none":
            known_hosts: object = ()  # disables host-key checking
        elif known:
            known_hosts = known
        else:
            known_hosts = None  # default: ~/.ssh/known_hosts (strict)
        port = getenv("SSH_PORT")
        config_paths = _discover_ssh_config()
        return SimpleNamespace(
            host=getenv("SSH_HOST"),
            port=int(port) if port else 22,
            user=getenv("SSH_USER") or getenv("USER"),
            password=None,
            client_keys=[key_file] if key_file else None,
            known_hosts=known_hosts,
            config_paths=config_paths,
        )

    # ------------------------------------------------------------------
    # CloudPathMixin hooks
    # ------------------------------------------------------------------

    @property
    def _item_path(self) -> str:
        # ``/~`` / ``/~/foo`` encode "remote home" (scp-style) -- translate
        # to SFTP cwd (= user's home after login).
        p = cast(URL, self._path).path or "/"
        if p == "/~":
            return "."
        if p.startswith("/~/"):
            return "." + p[2:]
        return p

    @property
    def _native_kwargs(self) -> dict:
        # Single source of connection truth for the native russh backend,
        # spread as ``**self._native_kwargs`` like every other backend.
        # Auth precedence mirrors ssh: explicit password, then key file
        # (explicit kwarg/env, else ssh_config IdentityFile), else agent.
        keys = self._client_keys or ()
        key_path = keys[0] if keys else getattr(self, "_config_identity", None)
        if key_path is not None:
            key_path = str(Path(key_path).expanduser())
        if self._password is not None:
            use_agent = False
            key_path = None
        elif key_path is not None:
            use_agent = False
        else:
            use_agent = True
        return {
            "host": self._resolved_host,
            "port": self._resolved_port,
            "user": self._resolved_user,
            "password": self._password,
            "key_path": key_path,
            "key_passphrase": None,
            "use_agent": use_agent,
            # Host-key checking is disabled for ``None`` (and SSH_KNOWN_HOSTS=
            # none -> ()); any other value keeps strict verification.
            "strict_host_key": self._known_hosts not in (None, ()),
        }

    # ------------------------------------------------------------------
    # Stat / type checks
    # ------------------------------------------------------------------

    async def _attrs(self, follow_symlinks: bool = True):
        # Reuse attrs cached by a parent ``iterdir`` to save a round-trip
        # per child (e.g. when ``ls -l`` stats every entry).
        cached = getattr(self, "_cached_attrs", None)
        if cached is not None and follow_symlinks:
            return cached
        # ssh_stat follows symlinks; ssh_lstat does not (for is_symlink()).
        stat_fn = ssh_stat if follow_symlinks else ssh_lstat
        try:
            size, mtime, atime, uid, gid, permissions = await stat_fn(
                path=self._item_path, **self._native_kwargs
            )
        except Exception as e:  # noqa: BLE001
            _map_native_error(e, str(self))
        return SimpleNamespace(
            size=size, mtime=mtime, atime=atime, uid=uid, gid=gid, permissions=permissions
        )

    async def exists(self) -> bool:
        try:
            await self._attrs()
            return True
        except FileNotFoundError:
            return False

    async def is_dir(self) -> bool:
        try:
            attrs = await self._attrs()
        except FileNotFoundError:
            return False
        return stat_module.S_ISDIR(attrs.permissions or 0)

    async def is_file(self) -> bool:
        try:
            attrs = await self._attrs()
        except FileNotFoundError:
            return False
        return stat_module.S_ISREG(attrs.permissions or 0)

    async def is_symlink(self) -> bool:
        try:
            attrs = await self._attrs(follow_symlinks=False)
        except FileNotFoundError:
            return False
        return stat_module.S_ISLNK(attrs.permissions or 0)

    async def stat(self, *, follow_symlinks: bool = True):
        attrs = await self._attrs(follow_symlinks=follow_symlinks)
        return SimpleNamespace(
            st_size=attrs.size,
            st_mtime=attrs.mtime,
            st_atime=attrs.atime,
            st_ctime=attrs.mtime,
            st_mode=attrs.permissions,
            st_uid=attrs.uid,
            st_gid=attrs.gid,
        )

    async def get_access_policy(
        self, *, backend_options: BackendOptions | None = None
    ) -> AccessPolicy:
        """Return POSIX permissions and numeric ownership from SFTP attributes."""
        attrs = await self._attrs()
        mode = attrs.permissions or 0

        def actions(shift: int) -> frozenset[str]:
            return frozenset(
                action
                for action, bit in (("read", 4), ("write", 2), ("execute", 1))
                if mode >> shift & bit
            )

        return AccessPolicy(
            owner=str(attrs.uid) if attrs.uid is not None else None,
            group=str(attrs.gid) if attrs.gid is not None else None,
            grants=(
                AccessGrant("owner", actions(6)),
                AccessGrant("group", actions(3)),
                AccessGrant("everyone", actions(0)),
            ),
        )

    async def update_access_policy(
        self, policy_patch: AccessPolicyPatch, *, backend_options: BackendOptions | None = None
    ) -> None:
        """Apply POSIX ownership and mode grants through SFTP setstat."""
        if policy_patch.owner is not None or policy_patch.group is not None:
            try:
                uid = None if policy_patch.owner is None else int(policy_patch.owner)
                gid = None if policy_patch.group is None else int(policy_patch.group)
            except ValueError as error:
                raise ValueError("SSH ownership must use numeric uid and gid strings") from error
            await ssh_setstat(
                path=self._item_path,
                permissions=None,
                uid=uid,
                gid=gid,
                atime=None,
                mtime=None,
                **self._native_kwargs,
            )
        if policy_patch.grants:
            attrs = await self._attrs()
            mode = attrs.permissions or 0
            shifts = {"owner": 6, "group": 3, "everyone": 0}
            for grant in policy_patch.grants:
                if grant.principal not in shifts:
                    raise ValueError(f"unsupported SSH policy principal: {grant.principal}")
                bits = sum(
                    {"read": 4, "write": 2, "execute": 1}[action] for action in grant.actions
                )
                shift = shifts[grant.principal]
                mode = mode & ~(0o7 << shift) | (bits << shift)
            await ssh_setstat(
                path=self._item_path,
                permissions=mode,
                uid=None,
                gid=None,
                atime=None,
                mtime=None,
                **self._native_kwargs,
            )

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
        try:
            return bytes(await ssh_read(path=self._item_path, **self._native_kwargs))
        except Exception as e:  # noqa: BLE001
            _map_native_error(e, str(self))

    async def write_bytes(self, data: bytes) -> int:
        try:
            return await ssh_write(path=self._item_path, data=data, **self._native_kwargs)
        except Exception as e:  # noqa: BLE001
            _map_native_error(e, str(self))

    async def _upload_buffer(
        self,
        fileobj: IO[bytes],
        size: int,
        *,
        backend_options: BackendOptions | None = None,
        chunk_size: int | None = None,
    ) -> None:
        """Stream large objects chunk-by-chunk (bounded memory) via APPEND writes."""
        chunk_bytes = chunk_size or self._STREAM_CHUNK
        if size < chunk_bytes:
            await self.write_bytes(fileobj.read())
            return
        truncate = True
        while True:
            chunk = fileobj.read(chunk_bytes)
            if not chunk:
                break
            try:
                await ssh_write_chunk(
                    path=self._item_path,
                    data=chunk,
                    truncate=truncate,
                    **self._native_kwargs,
                )
            except Exception as e:  # noqa: BLE001
                _map_native_error(e, str(self))
            truncate = False

    async def _range_read(self, start: int, end: int) -> bytes:
        try:
            return bytes(
                await ssh_read_range(
                    path=self._item_path, start=start, end=end, **self._native_kwargs
                )
            )
        except Exception as e:  # noqa: BLE001
            _map_native_error(e, str(self))

    # ------------------------------------------------------------------
    # Directory / unlink / rename
    # ------------------------------------------------------------------

    async def unlink(self, missing_ok: bool = False) -> None:
        try:
            await ssh_unlink(path=self._item_path, **self._native_kwargs)
        except Exception as e:  # noqa: BLE001
            _map_native_error(e, str(self), ignore=FileNotFoundError if missing_ok else ())

    async def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        if not (parents or exist_ok) and await self.exists():
            raise FileExistsError(17, "File exists", str(self))
        try:
            await ssh_mkdir(path=self._item_path, parents=parents, **self._native_kwargs)
        except Exception as e:  # noqa: BLE001
            try:
                _map_native_error(e, str(self), ignore=FileExistsError if exist_ok else ())
            except Exception:
                # Some servers (e.g. atmoz/sftp) return a generic, unclassifiable
                # error for an existing dir; re-check so exist_ok stays idempotent.
                if exist_ok and await self.is_dir():
                    return
                raise

    async def rmdir(self, *, recursive: bool = False) -> None:
        if not recursive:
            # ssh_list yields nothing for an empty dir; a clean ENOTEMPTY beats
            # the raw server error _map_native_error would otherwise classify.
            async for _ in self.iterdir():
                raise OSError(39, f"Directory not empty: '{self}'")
            await self._native_rmdir()
            return
        await self._rmtree()

    async def _native_rmdir(self) -> None:
        try:
            await ssh_rmdir(path=self._item_path, **self._native_kwargs)
        except Exception as e:  # noqa: BLE001
            _map_native_error(e, str(self))

    async def _rmtree(self) -> None:
        await self._rmtree_bounded(asyncio.Semaphore(_RMDIR_CONCURRENCY))

    async def _rmtree_bounded(self, sem: asyncio.Semaphore) -> None:
        """Delete this directory and its contents with bounded concurrency.

        SFTP has no recursive remove; children are removed concurrently (the
        semaphore bounds in-flight deletes) and the semaphore is only held around
        a single network op — never across recursion — so it cannot deadlock.
        """

        async def _remove(child: Self) -> None:
            if await child.is_dir():  # cached from iterdir — no round-trip
                await child._rmtree_bounded(sem)
            else:
                async with sem:
                    await child.unlink()

        children = [_remove(child) async for child in self.iterdir()]
        for result in await asyncio.gather(*children, return_exceptions=True):
            if isinstance(result, BaseException):
                raise result
        async with sem:
            await self._native_rmdir()

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
            try:
                await ssh_rename(
                    src=self._item_path, dst=target_path._item_path, **self._native_kwargs
                )
            except Exception as e:  # noqa: BLE001
                _map_native_error(e, str(self))
            return target_path
        return await super().rename(str(target))

    async def replace(self, target: str | Self) -> Self:
        target_path = target if isinstance(target, type(self)) else type(self)(str(target))
        if (target_path._host, target_path._port) == (self._host, self._port):
            # russh-sftp has no posix_rename; emulate atomic-ish replace
            # by removing an existing destination first.
            await target_path.unlink(missing_ok=True)
            try:
                await ssh_rename(
                    src=self._item_path, dst=target_path._item_path, **self._native_kwargs
                )
            except Exception as e:  # noqa: BLE001
                _map_native_error(e, str(self))
            return target_path
        return await super().replace(str(target))

    async def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        if await self.exists():
            if not exist_ok:
                raise FileExistsError(17, f"File exists: '{self}'")
            now = int(time())
            await ssh_setstat(
                path=self._item_path,
                permissions=None,
                uid=None,
                gid=None,
                atime=now,
                mtime=now,
                **self._native_kwargs,
            )
            return
        await self.write_bytes(b"")

    async def iterdir(self, *, fresh: bool = False) -> AsyncIterator[Self]:
        try:
            entries = await ssh_list(path=self._item_path, **self._native_kwargs)
        except Exception as e:  # noqa: BLE001
            # A missing path / non-dir surfaces as "not a directory" for iterdir;
            # other failures keep their mapped type.
            _map_native_error(e, str(self), ignore=FileNotFoundError)
            raise NotADirectoryError(20, f"Not a directory: '{self}'") from e
        for name, size, mtime, atime, uid, gid, permissions in entries:
            if name in (".", ".."):
                continue
            # ``read_dir`` already returned attrs for every entry; cache
            # them so ``stat()``/``is_dir()`` don't issue another stat.
            attrs = SimpleNamespace(
                size=size, mtime=mtime, atime=atime, uid=uid, gid=gid, permissions=permissions
            )
            yield self._child(str(self._path / name), _cached_attrs=attrs)

    async def walk(
        self,
        top_down: bool = True,
        on_error=None,
        follow_symlinks: bool = False,
    ) -> AsyncIterator[tuple[Self, list[str], list[str]]]:
        """Walk the directory tree via native SFTP (recursive ``readdir``).

        Children reuse the attrs cached by ``iterdir`` so ``is_dir()`` costs
        no extra round trip.
        """
        try:
            children = [child async for child in self.iterdir()]
        except (FileNotFoundError, NotADirectoryError) as e:
            if on_error is not None:
                on_error(e)
                return
            raise
        dirs: list[Self] = []
        files: list[Self] = []
        for child in children:
            if await child.is_dir():
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
