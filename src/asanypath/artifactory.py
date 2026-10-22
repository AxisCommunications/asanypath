# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""JFrog Artifactory path implementation with Bearer token authentication."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from os import getenv
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Literal

import msgspec

if TYPE_CHECKING:
    pass

from asanypath_native import (
    art_copy_batch,
    art_delete_batch,
    art_exists_batch,
    art_get_batch,
    art_get_permission_target,
    art_list_batch,
    art_list_repos,
    art_put_batch,
    art_put_permission_target,
    art_put_stream,
    art_storage_info,
)

from asanypath.cloud import CloudPathMixin, ServiceEndpointMixin, _service_origin
from asanypath.common import DirEntry
from asanypath.options import (
    UPLOAD_CHUNK_SIZE,
    AccessGrant,
    AccessPolicy,
    AccessPolicyPatch,
    BackendOptions,
)


def _jfrog_conf_token(base_url: str) -> str | None:
    """Return an access token from the JFrog CLI config for ``base_url``'s service.

    Reads ``$JFROG_CLI_HOME`` (default ``~/.jfrog``) and only uses a server
    whose configured URL has exactly this origin (scheme, host and port): a
    token is never sent to a service it wasn't configured for.
    """
    root = Path(getenv("JFROG_CLI_HOME") or (Path.home() / ".jfrog"))
    target = _service_origin(base_url) if base_url else None
    if target is None:
        return None
    for name in ("jfrog-cli.conf.v7", "jfrog-cli.conf.v6", "jfrog-cli.conf.v5", "jfrog-cli.conf"):
        try:
            data = json.loads((root / name).read_text())
        except (OSError, ValueError):
            continue
        servers = data.get("servers") if isinstance(data, dict) else None
        for server in servers or ():
            urls = (server.get("url"), server.get("artifactoryUrl"))
            if any(url and _service_origin(url) == target for url in urls):
                if token := server.get("accessToken"):
                    return token
    return None


class ArtifactoryPath(CloudPathMixin, ServiceEndpointMixin):
    """Async path implementation for JFrog Artifactory with Bearer token auth.

    Extends HTTPSPath to add authentication via Authorization: Bearer <token> header
    for authenticated requests to Artifactory servers.

    Example:
        ```python
        import asyncio
        import hashlib
        from asanypath import AsAnyPath

        async def main():
            # Using direct URL with token parameter
            path = AsAnyPath("art://artifactory.example.com/artifactory/generic/file.txt",
                           token="your-token")

            # Or using environment variable ARTIFACTORY_IDENTITY_TOKEN
            path = AsAnyPath("art://artifactory.example.com/artifactory/generic/file.txt")

            content = await path.read_bytes()
            await path.write_bytes(b"new content")
        ```
    """

    protocol: str = "art"
    _options_bound_to_authority = True
    _supports_range_read: bool = True
    # Artifactory has no multipart API: objects at/above this size stream a single
    # PUT body from disk (bounded memory) instead of buffering the whole object.
    _STREAM_THRESHOLD = UPLOAD_CHUNK_SIZE
    _copy_batch_fn = staticmethod(art_copy_batch)
    # Tokens per service (``art:<host>/artifactory``), mirroring the keyring so
    # repeated constructions skip it. Never applied to a different service.
    _service_tokens: dict[str, str] = {}

    @staticmethod
    def _batcher_key_fn(base_url: str, token: str, **_kw) -> str:
        return f"art|{base_url}|{token[:8]}"

    _batcher_ops = {
        "get": {"batch_fn": art_get_batch, "items_key": "paths", "wrap": bytes},
        "exists": {"batch_fn": art_exists_batch, "items_key": "paths"},
        "delete": {"batch_fn": art_delete_batch, "items_key": "paths"},
        "list": {"batch_fn": art_list_batch, "items_key": "paths"},
        "put": {"batch_fn": art_put_batch, "unpack_put": True},
    }

    def __init__(self, *args, token: str | None = None, **kwargs):
        """Initialize ArtifactoryPath with optional Bearer token.

        Args:
            *args: Path arguments passed to parent HTTPSPath.
            token: Bearer token for authentication. If not provided, will attempt to read
                   from ARTIFACTORY_IDENTITY_TOKEN environment variable.
            **kwargs: Additional keyword arguments passed to parent HTTPSPath.

        Raises:
            ValueError: If token is not provided and ARTIFACTORY_IDENTITY_TOKEN env var is not set.
        """
        super().__init__(*args, **kwargs)
        self._bind_path_attrs()
        if not self._service_key:
            raise ValueError(
                "Artifactory path needs a host (art://<host>/artifactory/...), "
                "or set ARTIFACTORY_BASE_URL to use art:// shorthand."
            )

        from asanypath import _credstore

        cred_key = f"art:{self._service_key}"
        known = ArtifactoryPath._service_tokens
        if token:
            # Persist user-supplied tokens once per service, not per derived path.
            if known.get(cred_key) != token:
                _credstore.save(cred_key, token)
                known[cred_key] = token
            self._token = token
            self._init_options = {"token": token}
        else:
            self._token = self._env_token() or known.get(cred_key)
            if not self._token:
                self._token = _credstore.load(cred_key) or _jfrog_conf_token(self._service_key)
                if self._token:
                    known[cred_key] = self._token
        if not self._token:
            raise ValueError(
                "Artifactory token required via 'token' parameter, "
                "ARTIFACTORY_IDENTITY_TOKEN environment variable, the JFrog CLI config "
                "(~/.jfrog/jfrog-cli.conf), or a previously cached token (see keyring)."
            )
        # Cached folder hint — set by iterdir() to avoid redundant API calls
        self._is_folder: bool | None = None
        self._cached_info: dict | None = None

    @classmethod
    def _create_env_config(cls) -> SimpleNamespace:
        """Build env config from Artifactory environment variables."""
        return SimpleNamespace(
            token=getenv("ARTIFACTORY_IDENTITY_TOKEN"),
        )

    # ------------------------------------------------------------------
    # CloudPathMixin hooks
    # ------------------------------------------------------------------

    def _env_token(self) -> str | None:
        """``ARTIFACTORY_IDENTITY_TOKEN``, only for the service named by
        ``ARTIFACTORY_BASE_URL`` (or ``ART_BASE_URL``)."""
        token = self.env_config.token
        if token and self._ambient_credentials_allowed(
            self._resolve_base_url_prefix(),
            self._service_key,
            credential="ARTIFACTORY_IDENTITY_TOKEN",
            setting="ARTIFACTORY_BASE_URL",
        ):
            return token
        return None

    @property
    def _service_key(self) -> str:
        """``host/artifactory``: the service key for the keyring, the JFrog CLI
        config and token scoping. Derived as before endpoint normalization so
        stored credentials keep matching."""
        raw = str(self)[len("art://") :]
        idx = raw.find("/artifactory/")
        return raw[: idx + len("/artifactory")] if idx >= 0 else raw.split("/")[0]

    @property
    def _item_path(self) -> str:
        return self._repo_path

    def _bind_path_attrs(self) -> None:
        raw = str(self)[len("art://") :]
        idx = raw.find("/artifactory/")
        if idx < 0 and raw.rstrip("/").endswith("/artifactory"):
            idx = len(raw.rstrip("/")) - len("/artifactory")
        if idx >= 0:
            self._endpoint_url = "https://" + raw[: idx + len("/artifactory")]
            self._repo_path = raw[idx + len("/artifactory/") :]
        else:
            self._endpoint_url = "https://" + raw.split("/")[0]
            self._repo_path = "/".join(raw.split("/")[1:])
        self._is_folder = None
        self._cached_info = None

    @property
    def _native_kwargs(self) -> dict:
        return {
            "base_url": self._endpoint_url,
            "token": self._token,
        }

    def _identity_parts(self) -> tuple:
        # Keep cache identity scoped even when the token came from keyring/JFrog.
        token_fp = hashlib.sha256(self._token.encode()).hexdigest()
        return (*super()._identity_parts(), self._service_key, token_fp)

    # ------------------------------------------------------------------
    # Backend-specific operations
    # ------------------------------------------------------------------

    async def _upload_buffer(
        self, fileobj, size: int, *, backend_options=None, chunk_size: int | None = None
    ) -> None:
        """Upload a spooled write buffer; large objects stream a single PUT from disk."""
        if size < (chunk_size or self._STREAM_THRESHOLD):
            data = fileobj.read()
            if backend_options is None:
                await self.write_bytes(data)
            else:
                await self.write_bytes(data, backend_options=backend_options)
            return
        # Stream from a real file path when we have one (e.g. a local copy
        # source); otherwise spill the anonymous buffer to a named temp file.
        name = getattr(fileobj, "name", None)
        if isinstance(name, str) and os.path.exists(name):
            await self._stream_upload(name, size, backend_options=backend_options)
            return
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            shutil.copyfileobj(fileobj, tmp, 1024 * 1024)
            tmp_path = tmp.name
        try:
            await self._stream_upload(tmp_path, size, backend_options=backend_options)
        finally:
            os.unlink(tmp_path)

    async def _stream_upload(self, file_path: str, size: int, *, backend_options=None) -> None:
        options_json = (
            msgspec.json.encode(backend_options).decode() if backend_options is not None else "{}"
        )
        await art_put_stream(
            key=self._item_path,
            file_path=file_path,
            size=size,
            options_json=options_json,
            **self._native_kwargs,
        )
        self._invalidate_listing()

    async def _storage_info(self) -> dict:
        """Fetch item metadata from the Artifactory storage API (cached).

        Timestamps are parsed to epoch floats at ingestion so consumers
        never deal with raw strings.
        """
        if self._cached_info is None:
            raw = await art_storage_info(path=self._repo_path, **self._native_kwargs)
            info: dict = {}
            if "size" in raw:
                try:
                    info["st_size"] = int(raw["size"])
                except (ValueError, TypeError):
                    info["st_size"] = 0
            if "created" in raw:
                info["st_ctime"] = self._parse_art_ts(raw["created"])
            if "lastModified" in raw:
                info["st_mtime"] = self._parse_art_ts(raw["lastModified"])
            if "is_dir" in raw:
                info["children"] = []  # marker; iterdir uses art_list
            checksums = {}
            for k, v in raw.items():
                if k.startswith("checksum_"):
                    checksums[k[len("checksum_") :]] = v
            if checksums:
                info["checksums"] = checksums
            self._cached_info = info
        return self._cached_info

    async def is_dir(self) -> bool:
        if self._is_folder is not None:
            return self._is_folder
        try:
            info = await self._storage_info()
            self._is_folder = "children" in info
            return self._is_folder
        except (FileNotFoundError, PermissionError):
            return False

    async def is_file(self) -> bool:
        if self._is_folder is not None:
            return not self._is_folder
        try:
            info = await self._storage_info()
            self._is_folder = "children" in info
            return not self._is_folder
        except (FileNotFoundError, PermissionError):
            return False

    async def _list_entries(self, *, stat: bool = False, fresh: bool = False) -> list[DirEntry]:
        containers = await self._list_containers()
        if containers is not None:
            return [DirEntry(self._child(uri), True) for uri in containers]
        batcher = self._get_batcher()
        results = await batcher.list(
            item=self._repo_path,
            base_url=self._endpoint_url,
            token=self._token,
        )
        base = str(self).rstrip("/")
        entries = []
        for uri, is_folder in results:
            child_name = uri.rstrip("/").rsplit("/", 1)[-1]
            child = self._confined_child(f"{base}/{child_name}", _is_folder=is_folder)
            if child is not None:
                entries.append(DirEntry(child, is_folder))
        return entries

    async def _list_containers(self) -> list[str] | None:
        """List repositories when at the server root (``art://<host>/artifactory/``).

        Returns None once a repository is selected, so normal object listing
        proceeds.
        """
        base = self._endpoint_url
        if self._repo_path.strip("/"):
            return None
        names = await art_list_repos(base_url=base, token=self._token, use_h2=False)
        return [f"art://{base.removeprefix('https://')}/{name}" for name in names]

    @staticmethod
    def _parse_art_ts(ts: str | None) -> float | None:
        """Parse an Artifactory ISO-8601 timestamp to a POSIX float."""
        if not ts:
            return None
        # Normalize "2019-10-16T14:57:25.000+0000" → "2019-10-16T14:57:25.000+00:00"
        if len(ts) > 5 and ts[-5] in "+-" and ":" not in ts[-5:]:
            ts = ts[:-2] + ":" + ts[-2:]
        # fromisoformat() doesn't support "Z" until Python 3.11
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(ts).astimezone(timezone.utc).timestamp()
        except ValueError:
            return None

    async def stat(self, *, follow_symlinks: bool = True) -> SimpleNamespace:
        info = await self._storage_info()
        return SimpleNamespace(
            st_size=info.get("st_size", 0),
            st_ctime=info.get("st_ctime"),
            st_mtime=info.get("st_mtime"),
            checksums=info.get("checksums"),
        )

    async def _transfer_stat(self) -> SimpleNamespace:
        self._cached_info = None
        return await self.stat()

    async def checksums(self) -> dict[str, str]:
        """Return checksums (md5, sha1, sha256) from the Artifactory storage API."""
        info = await self._storage_info()
        return info.get("checksums", {})

    @staticmethod
    def _permission_target_name(backend_options: BackendOptions | None) -> str:
        if backend_options is None:
            raise ValueError("Artifactory policy operations require provider['permission_target']")
        name = backend_options.provider.get("permission_target")
        if not isinstance(name, str) or not name:
            raise ValueError("Artifactory policy operations require provider['permission_target']")
        return name

    async def get_access_policy(
        self, *, backend_options: BackendOptions | None = None
    ) -> AccessPolicy:
        """Return the explicitly selected Artifactory permission target."""
        name = self._permission_target_name(backend_options)
        data = await art_get_permission_target(name=name, **self._native_kwargs)
        if isinstance(data, str):
            data = msgspec.json.decode(data)
        if not isinstance(data, Mapping):
            raise TypeError("native Artifactory permission target response must be a mapping")
        grants = []
        principals = data.get("principals")
        if isinstance(principals, Mapping):
            for kind in ("users", "groups"):
                entries = principals.get(kind)
                if not isinstance(entries, Mapping):
                    continue
                for principal, permissions in entries.items():
                    if not isinstance(principal, str) or not isinstance(permissions, list):
                        continue
                    actions: frozenset[Literal["read", "write"]] = frozenset(
                        action
                        for action, permission in (("read", "r"), ("write", "w"))
                        if permission in permissions
                    )
                    if actions:
                        grants.append(AccessGrant(f"{kind[:-1]}:{principal}", actions))
        return AccessPolicy(
            grants=tuple(grants),
            provider={"artifactory_permission_target": data},
        )

    async def update_access_policy(
        self, policy_patch: AccessPolicyPatch, *, backend_options: BackendOptions | None = None
    ) -> None:
        """Replace the explicitly selected Artifactory permission target."""
        name = self._permission_target_name(backend_options)
        target = policy_patch.provider.get("artifactory_permission_target")
        if not isinstance(target, Mapping):
            raise ValueError(
                "Artifactory policy replacement requires provider['artifactory_permission_target']"
            )
        await art_put_permission_target(
            name=name,
            target_json=msgspec.json.encode(target).decode(),
            **self._native_kwargs,
        )

    async def presign(self, *, expires: int = 3600, method: str = "GET") -> str:
        """Presigned URLs are not supported for Artifactory."""
        raise NotImplementedError(
            "Artifactory does not support client-side presigned URL generation. "
            "Use the Artifactory 'signed URL' REST API if your server supports it."
        )
