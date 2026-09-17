# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""JFrog Artifactory path implementation with Bearer token authentication."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timezone
from os import getenv
from types import SimpleNamespace
from typing import TYPE_CHECKING, Literal

import msgspec

if TYPE_CHECKING:
    from typing import Self

from asanypath.cloud import CloudPathMixin
from asanypath.options import AccessGrant, AccessPolicy, AccessPolicyPatch, BackendOptions
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
    art_storage_info,
)


class ArtifactoryPath(CloudPathMixin):
    """Async path implementation for JFrog Artifactory with Bearer token auth.

    Extends HTTPSPath to add authentication via Authorization: Bearer <token> header
    for authenticated requests to Artifactory servers.

    Example:
        ```python
        import asyncio
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
    _supports_range_read: bool = True
    _copy_batch_fn = staticmethod(art_copy_batch)

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
        # Pre-compute base_url and repo_path for native dispatch
        raw = str(self)[len("art://") :]  # "host/artifactory/repo/path"
        idx = raw.find("/artifactory/")
        if idx >= 0:
            self._base_url = raw[: idx + len("/artifactory")]
            self._repo_path = raw[idx + len("/artifactory/") :]
        else:
            self._base_url = raw.split("/")[0]
            self._repo_path = "/".join(raw.split("/")[1:])

        from asanypath import _credstore

        cred_key = f"art:{self._base_url}"
        self._token = token or self.env_config.token
        if not self._token:
            self._token = _credstore.load(cred_key)
        if not self._token:
            raise ValueError(
                "Artifactory token required via 'token' parameter, "
                "ARTIFACTORY_IDENTITY_TOKEN environment variable, or a "
                "previously cached token (see keyring)."
            )
        # Promote explicit token into class-level cache so that derived
        # instances (created by parent, /, with_name, etc.) pick it up
        # without needing the env var set, and persist it for next runs.
        if token:
            self.env_config.token = token
            _credstore.save(cred_key, token)
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

    @property
    def _item_path(self) -> str:
        return self._repo_path

    @property
    def _native_kwargs(self) -> dict:
        return {
            "base_url": self._base_url,
            "token": self._token,
        }

    # ------------------------------------------------------------------
    # Backend-specific operations
    # ------------------------------------------------------------------

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

    async def iterdir(self) -> AsyncIterator[Self]:
        containers = await self._list_containers()
        if containers is not None:
            for uri in containers:
                yield type(self)(uri, token=self._token)
            return
        batcher = self._get_batcher()
        results = await batcher.list(
            item=self._repo_path,
            base_url=self._base_url,
            token=self._token,
        )
        base = str(self).rstrip("/")
        for uri, is_folder in results:
            child_name = uri.rsplit("/", 1)[-1]
            child_path = type(self)(f"{base}/{child_name}", token=self._token)
            child_path._is_folder = is_folder
            yield child_path

    async def _list_containers(self) -> list[str] | None:
        """List repositories when at the server root (``art://<host>/artifactory/``).

        Returns None once a repository is selected, so normal object listing
        proceeds. Handles both the canonical ``host/artifactory`` base and the
        shorthand parse where the trailing ``artifactory`` lands in the repo
        slot.
        """
        base = self._base_url
        repo = self._repo_path.strip("/")
        if repo == "artifactory":
            base = f"{base}/artifactory"
            repo = ""
        if repo:
            return None
        if not base or base == "artifactory":
            return None
        names = await art_list_repos(base_url=base, token=self._token, use_h2=False)
        return [f"art://{base}/{name}" for name in names]

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
        )

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
