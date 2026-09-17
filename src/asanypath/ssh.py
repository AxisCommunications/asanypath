# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""SSH/SFTP path implementation using asyncssh.

URL format::

    ssh://[user@]host[:port]/path/to/file

Recommended: define hosts in ``~/.ssh/config`` and use ``ssh://alias/path``.
Aliases are resolved eagerly via the OpenSSH client config parser; see
:attr:`SSHPath.resolved_target`. Two aliases that resolve to the same
``(host, port, user)`` share one pooled connection.

Password env vars are deliberately **not** supported -- use key-based auth
(agent or ``IdentityFile`` in ``~/.ssh/config``), or pass ``password=`` as a
kwarg for programmatic use.

When stdin is a TTY and no ``password=`` was supplied, the backend prompts
lazily via :mod:`getpass` for password / keyboard-interactive challenges
(PAM, OTP, 2FA). Set ``ASANYPATH_INTERACTIVE=0`` to force non-interactive
behavior in scripts.

Non-secret env defaults (used only when the URL and ssh_config don't supply):

    SSH_USER         (default: $USER)
    SSH_KEY_FILE     single path; pass a list via kwarg for several
    SSH_KNOWN_HOSTS  "none" to disable host-key check; otherwise a file path.
                     Default: asyncssh's default (~/.ssh/known_hosts)
    SSH_CONFIG       OpenSSH client config path; "" disables; otherwise
                     ~/.ssh/config and /etc/ssh/ssh_config are consulted
                     when present.

Connections are pooled per resolved ``(host, port, user)`` at module level;
one cached ``SFTPClient`` is reused for the lifetime of each connection.
Call :func:`disconnect_all` to close everything (mainly for tests/shutdown).
"""

from __future__ import annotations

import asyncio
import getpass
import stat as stat_module
import sys
from collections.abc import AsyncIterator
from os import getenv
from pathlib import Path
from time import time
from types import SimpleNamespace
from typing import TYPE_CHECKING

import asyncssh
from asyncssh.config import SSHClientConfig

from asanypath.cloud import CloudPathMixin
from asanypath.options import AccessGrant, AccessPolicy, AccessPolicyPatch, BackendOptions

if TYPE_CHECKING:
    from typing import Self


_CONN_CACHE: dict[tuple, asyncssh.SSHClientConnection] = {}
_CONN_LOCKS: dict[tuple, asyncio.Lock] = {}


def disconnect_all() -> None:
    """Close every cached SSH connection."""
    for conn in list(_CONN_CACHE.values()):
        try:
            conn.close()
        except Exception:  # noqa: BLE001  # pragma: no cover
            pass  # pragma: no cover
    _CONN_CACHE.clear()
    _CONN_LOCKS.clear()


_UNSET = object()

# asyncssh's ``SFTPNoSuchFile`` does not subclass ``FileNotFoundError``/``OSError``,
# so existence checks must translate it explicitly (else exists()/is_dir() raise
# instead of returning False, breaking copy/rename to new remote paths).
_sftp_no_such_file = getattr(asyncssh.sftp, "SFTPNoSuchFile", None)
_NOT_FOUND_ERRORS: tuple[type[BaseException], ...] = (
    (FileNotFoundError, _sftp_no_such_file) if _sftp_no_such_file else (FileNotFoundError,)
)


def _interactive() -> bool:
    """Whether the backend may prompt the user for credentials."""
    if getenv("ASANYPATH_INTERACTIVE") == "0":
        return False
    try:
        return sys.stdin.isatty()
    except (AttributeError, ValueError):  # pragma: no cover
        return False  # pragma: no cover


class _InteractiveSSHClient(asyncssh.SSHClient):
    """SSHClient that prompts via :mod:`getpass` for missing credentials.

    Used only when stdin is a TTY and no static password was supplied.
    The prompted password is cached on the instance so a single login
    attempt doesn't re-ask after a failed kbdint round.
    """

    def __init__(self, label: str) -> None:
        self._label = label
        self._cached_password: str | None = None

    async def _prompt(self, msg: str, *, hidden: bool = True) -> str:
        loop = asyncio.get_running_loop()
        fn = getpass.getpass if hidden else input
        return await loop.run_in_executor(None, fn, msg)

    async def password_auth_requested(self) -> str | None:
        if self._cached_password is None:
            self._cached_password = await self._prompt(f"Password for {self._label}: ")
        return self._cached_password

    async def kbdint_auth_requested(self) -> str:
        return ""

    async def kbdint_challenge_received(
        self, name: str, instructions: str, lang: str, prompts
    ) -> list[str] | None:
        if not prompts:
            return []
        if name:
            sys.stderr.write(f"{name}\n")
        if instructions:
            sys.stderr.write(f"{instructions}\n")
        responses: list[str] = []
        for prompt, echo in prompts:
            # Reuse cached password for password-like challenges so the user
            # isn't prompted twice when the server tries password and then
            # keyboard-interactive with a "Password:" prompt.
            if not echo and self._cached_password and "password" in prompt.lower():
                responses.append(self._cached_password)
            else:
                responses.append(await self._prompt(prompt, hidden=not echo))
        return responses


def _discover_ssh_config() -> list[str]:
    """Return the list of OpenSSH client config files to consult.

    Honors ``$SSH_CONFIG`` as a single override (empty string disables
    discovery entirely). Otherwise looks for ``~/.ssh/config`` and
    ``/etc/ssh/ssh_config`` in OpenSSH order, returning paths that exist
    or are likely to exist. asyncssh will handle missing files gracefully.
    """
    override = getenv("SSH_CONFIG")
    if override is not None:
        return [override] if override else []
    # Always include both paths; asyncssh handles missing files
    paths = [
        str(Path.home() / ".ssh" / "config"),
        "/etc/ssh/ssh_config",
    ]
    return paths


def _resolve_alias(
    host: str | None,
    port: int | None,
    user: str | None,
) -> tuple[str | None, int | None, str | None]:
    """Expand ``(host, port, user)`` via OpenSSH config; return resolved values.

    Falls back to the supplied values when no config is available or no
    match is found. ``port`` / ``user`` come back as ``None`` when neither
    the supplied value nor the config provides one, so the caller can
    decide on an env-level default.
    """
    cfg_paths = _discover_ssh_config()
    if not host or not cfg_paths:
        return host, port, user
    try:
        local_user = getpass.getuser()
    except Exception:  # noqa: BLE001
        local_user = ""
    try:
        cfg = SSHClientConfig.load(None, cfg_paths, False, False, False, local_user, (), host, ())
    except Exception:  # noqa: BLE001
        return host, port, user
    resolved_host = cfg.get("Hostname") or host
    resolved_port_raw = port or cfg.get("Port")
    resolved_port = int(resolved_port_raw) if resolved_port_raw is not None else None
    resolved_user = user or cfg.get("User")
    return resolved_host, resolved_port, resolved_user


class SSHPath(CloudPathMixin):
    """Async SFTP path backed by :mod:`asyncssh`."""

    protocol: str = "ssh"
    _supports_range_read: bool = True

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
        typed_host = host or self._path.host
        typed_port = port or self._path.port
        typed_user = username or self._path.user
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
        r_host, r_port, r_user = _resolve_alias(self._host, typed_port, typed_user)
        self._resolved_host = r_host
        self._resolved_port = r_port if r_port is not None else cfg.port
        self._resolved_user = r_user if r_user is not None else cfg.user
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
            known_hosts: object = ()  # asyncssh: empty tuple disables checking
        elif known:
            known_hosts = known
        else:
            known_hosts = None  # asyncssh default (~/.ssh/known_hosts)
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
        p = self._path.path or "/"
        if p == "/~":
            return "."
        if p.startswith("/~/"):
            return "." + p[2:]
        return p

    @property
    def _native_kwargs(self) -> dict:
        return {
            "host": self._resolved_host,
            "port": self._resolved_port,
            "user": self._resolved_user,
        }

    # ------------------------------------------------------------------
    # Connection / SFTP pool
    # ------------------------------------------------------------------

    @property
    def _conn_key(self) -> tuple:
        # Pool by *resolved* target so two aliases pointing at the same
        # machine share one connection.
        return (self._resolved_host, self._resolved_port, self._resolved_user)

    async def _get_conn(self) -> asyncssh.SSHClientConnection:
        key = self._conn_key
        lock = _CONN_LOCKS.setdefault(key, asyncio.Lock())
        async with lock:
            conn = _CONN_CACHE.get(key)
            if conn is None or conn.is_closed():
                conn = await self._connect_fresh()
                _CONN_CACHE[key] = conn
            return conn

    def _credstore_key(self) -> str:
        return (
            f"ssh:{self._resolved_host or self._host or ''}"
            f":{self._resolved_port or ''}"
            f":{self._resolved_user or ''}"
        )

    async def _connect_fresh(self) -> asyncssh.SSHClientConnection:
        # Pass the original (typed) host so asyncssh re-applies
        # ssh_config (IdentityFile, ProxyJump, UserKnownHostsFile, …)
        # using the alias as the Host pattern. Our resolved values are
        # only used for pool keying / cache identity.
        from asanypath import _credstore

        base_kwargs: dict = {
            "host": self._host,
            "known_hosts": self._known_hosts,
            "config": self.env_config.config_paths or (),
        }
        # Only pass typed URL/kwarg user/port. If they came from env defaults,
        # let asyncssh apply ssh_config alias User/Port instead.
        if self._typed_port is not None:
            base_kwargs["port"] = self._typed_port
        if self._typed_user is not None:
            base_kwargs["username"] = self._typed_user
        if self._client_keys is not None:
            base_kwargs["client_keys"] = self._client_keys
        if self._password is not None:
            return await asyncssh.connect(password=self._password, **base_kwargs)

        cred_key = self._credstore_key()
        cached = _credstore.load(cred_key) if _interactive() else None
        if cached is not None:
            try:
                return await asyncssh.connect(password=cached, **base_kwargs)
            except asyncssh.PermissionDenied:
                _credstore.forget(cred_key)
        if not _interactive():
            return await asyncssh.connect(**base_kwargs)

        label = self._resolved_host or self._host or "ssh"
        if self._resolved_user:
            label = f"{self._resolved_user}@{label}"
        captured: list[_InteractiveSSHClient] = []

        def _factory() -> _InteractiveSSHClient:
            inst = _InteractiveSSHClient(label)
            captured.append(inst)
            return inst

        conn = await asyncssh.connect(client_factory=_factory, **base_kwargs)
        if captured and captured[0]._cached_password:
            _credstore.save(cred_key, captured[0]._cached_password)
        return conn

    async def _sftp(self) -> asyncssh.SFTPClient:
        conn = await self._get_conn()
        sftp = getattr(conn, "_asanypath_sftp", None)
        # asyncssh.SFTPClient doesn't expose ``exit_status`` on all versions.
        # Reuse cached client unless an explicit non-None ``exit_status``
        # attribute exists (e.g. from test doubles / alternate implementations).
        sftp_exit_status = getattr(sftp, "exit_status", None) if sftp is not None else None
        if sftp is None or sftp_exit_status is not None:
            sftp = await conn.start_sftp_client()
            conn._asanypath_sftp = sftp  # type: ignore[attr-defined]
        return sftp

    async def _run_remote(self, cmd: str, *, timeout: float | None = 15.0) -> tuple[str, int]:
        """Run a shell command on the remote host via OpenSSH subprocess.

        Uses the original typed host alias so that OpenSSH applies the full
        ``~/.ssh/config`` — including ControlMaster, ProxyJump, and
        IdentityFile — exactly as the user configured it.  This means a
        pre-existing ControlMaster socket makes this call near-instant.

        Returns ``(stdout_text, returncode)``.  Never raises.
        """
        import shutil

        ssh_bin = shutil.which("ssh")
        if ssh_bin is None:
            return "", -1
        args: list[str] = [ssh_bin, "-o", "BatchMode=yes"]
        # Pass only URL/kwarg-typed user and port; let OpenSSH apply the
        # Host block for everything else so ControlMaster matching works.
        if self._typed_user is not None:
            args += ["-l", self._typed_user]
        if self._typed_port is not None:
            args += ["-p", str(self._typed_port)]
        args.append(self._host or self._resolved_host or "")
        args.append(cmd)
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            coro = proc.communicate()
            stdout_b, _ = (
                await asyncio.wait_for(coro, timeout=timeout) if timeout is not None else await coro
            )
            return stdout_b.decode("utf-8", errors="replace"), proc.returncode or 0
        except Exception:
            if proc is not None:
                try:
                    proc.kill()
                except Exception:  # noqa: BLE001
                    pass
            return "", -1

    async def _list_dir_remote(self) -> list[tuple[str, bool]] | None:
        """List immediate children via OpenSSH subprocess (1 SSH call).

        Returned as ``[(name, is_dir), ...]`` or ``None`` when the subprocess
        approach is unavailable (no ``ssh`` binary) or the remote doesn't
        support GNU find ``-printf``.
        """
        import shlex

        path_q = shlex.quote(self._item_path)
        cmd = f"find {path_q} -maxdepth 1 -mindepth 1 -printf '%y\\t%f\\n' 2>/dev/null"
        output, rc = await self._run_remote(cmd, timeout=5.0)
        if rc != 0 or "\t" not in output:
            return None
        result: list[tuple[str, bool]] = []
        for line in output.splitlines():
            if "\t" not in line:
                continue
            type_char, name = line.split("\t", 1)
            if not name:
                continue
            result.append((name, type_char == "d"))
        return result

    # ------------------------------------------------------------------
    # Stat / type checks
    # ------------------------------------------------------------------

    async def _attrs(self, follow_symlinks: bool = True):
        # Reuse attrs cached by a parent ``iterdir`` to save a round-trip
        # per child (e.g. when ``ls -l`` stats every entry).
        cached = getattr(self, "_cached_attrs", None)
        if cached is not None and follow_symlinks:
            return cached
        sftp = await self._sftp()
        try:
            if follow_symlinks:
                return await sftp.stat(self._item_path)
            return await sftp.lstat(self._item_path)
        except _NOT_FOUND_ERRORS as e:
            raise FileNotFoundError(2, "No such file or directory", str(self)) from e

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
        """Apply POSIX ownership and mode grants through SFTP."""
        sftp = await self._sftp()
        if policy_patch.owner is not None or policy_patch.group is not None:
            try:
                uid = -1 if policy_patch.owner is None else int(policy_patch.owner)
                gid = -1 if policy_patch.group is None else int(policy_patch.group)
            except ValueError as error:
                raise ValueError("SSH ownership must use numeric uid and gid strings") from error
            await sftp.chown(self._item_path, uid, gid)
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
            await sftp.chmod(self._item_path, mode)

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
        sftp = await self._sftp()
        async with sftp.open(self._item_path, "rb") as f:
            return await f.read()

    async def write_bytes(self, data: bytes) -> int:
        sftp = await self._sftp()
        async with sftp.open(self._item_path, "wb") as f:
            await f.write(data)
        return len(data)

    async def _range_read(self, start: int, end: int) -> bytes:
        sftp = await self._sftp()
        async with sftp.open(self._item_path, "rb") as f:
            await f.seek(start)
            return await f.read(end - start + 1)

    # ------------------------------------------------------------------
    # Directory / unlink / rename
    # ------------------------------------------------------------------

    async def unlink(self, missing_ok: bool = False) -> None:
        sftp = await self._sftp()
        try:
            await sftp.remove(self._item_path)
        except _NOT_FOUND_ERRORS:
            if not missing_ok:
                raise FileNotFoundError(2, "No such file or directory", str(self)) from None

    async def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        sftp = await self._sftp()
        try:
            if parents:
                await sftp.makedirs(self._item_path, exist_ok=exist_ok)
            else:
                await sftp.mkdir(self._item_path)
        except FileExistsError:
            if not exist_ok:
                raise

    async def rmdir(self, *, recursive: bool = False) -> None:
        sftp = await self._sftp()
        if recursive:
            await sftp.rmtree(self._item_path)
            return
        await sftp.rmdir(self._item_path)

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
            sftp = await self._sftp()
            await sftp.rename(self._item_path, target_path._item_path)
            return target_path
        return await super().rename(str(target))

    async def replace(self, target: str | Self) -> Self:
        target_path = target if isinstance(target, type(self)) else type(self)(str(target))
        if (target_path._host, target_path._port) == (self._host, self._port):
            sftp = await self._sftp()
            # posix_rename atomically replaces dest if it exists.
            await sftp.posix_rename(self._item_path, target_path._item_path)
            return target_path
        return await super().replace(str(target))

    async def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        if await self.exists():
            if not exist_ok:
                raise FileExistsError(17, f"File exists: '{self}'")
            sftp = await self._sftp()
            now = time()
            await sftp.utime(self._item_path, (now, now))
            return
        await self.write_bytes(b"")

    async def iterdir(self, *, fresh: bool = False) -> AsyncIterator[Self]:
        sftp = await self._sftp()
        try:
            names = await sftp.readdir(self._item_path)
        except (FileNotFoundError, NotADirectoryError) as e:
            raise NotADirectoryError(20, f"Not a directory: '{self}'") from e
        for entry in names:
            if entry.filename in (".", ".."):
                continue
            child = self / entry.filename
            # ``readdir`` already returned attrs for every entry; cache
            # them so ``stat()``/``is_dir()`` don't issue another LSTAT.
            child._cached_attrs = entry.attrs  # type: ignore[attr-defined]
            yield child

    async def walk(
        self,
        top_down: bool = True,
        on_error=None,
        follow_symlinks: bool = False,
    ) -> AsyncIterator[tuple[Self, list[str], list[str]]]:
        """Walk directory tree.

        Tries a single remote ``find`` call first (benefits from OpenSSH
        ControlMaster — effectively free when a master socket exists).
        Falls back to sequential SFTP ``readdir`` when ``ssh`` is absent or
        the remote find doesn't support GNU ``-printf``.
        """
        import shlex
        from collections import defaultdict

        path_q = shlex.quote(self._item_path)
        follow_flag = "-L " if follow_symlinks else ""
        find_cmd = f"find {follow_flag}{path_q} -mindepth 1 -printf '%y\\t%P\\n' 2>/dev/null"
        output, rc = await self._run_remote(find_cmd, timeout=30.0)

        if rc == 0 and "\t" in output:
            # Fast path: reconstruct the tree from a flat find listing.
            children_map: dict[str, list[tuple[str, bool]]] = defaultdict(list)
            for line in output.splitlines():
                if "\t" not in line:
                    continue
                type_char, rel_path = line.split("\t", 1)
                if not rel_path:
                    continue
                parts = rel_path.split("/")
                parent_rel = "/".join(parts[:-1])
                name = parts[-1]
                children_map[parent_rel].append((name, type_char == "d"))

            async def _emit(rel_parent: str, base: SSHPath):  # type: ignore[name-defined]
                entry_list = children_map.get(rel_parent, [])
                dirs = [n for n, is_d in entry_list if is_d]
                files = [n for n, is_d in entry_list if not is_d]
                if top_down:
                    yield base, dirs, files
                for n, is_d in entry_list:
                    if is_d:
                        child_rel = f"{rel_parent}/{n}" if rel_parent else n
                        async for triple in _emit(child_rel, base / n):
                            yield triple
                if not top_down:
                    yield base, dirs, files

            async for triple in _emit("", self):
                yield triple
            return

        # SFTP fallback: one readdir round trip per directory.
        sftp = await self._sftp()
        try:
            entries = await sftp.readdir(self._item_path)
        except (FileNotFoundError, NotADirectoryError) as e:
            if on_error is not None:
                on_error(e)
                return
            raise
        dirs: list[Self] = []
        files: list[Self] = []
        for entry in entries:
            if entry.filename in (".", ".."):
                continue
            child = self / entry.filename
            if stat_module.S_ISDIR(entry.attrs.permissions or 0):
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
