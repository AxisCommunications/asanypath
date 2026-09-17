# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Azure Blob Storage path implementation (HTTP-style adapter).

Uses the Azure Blob Storage REST API:
https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from os import getenv
from types import SimpleNamespace

import msgspec

from asanypath.cloud import CloudPathMixin
from asanypath.options import AccessGrant, AccessPolicy, AccessPolicyPatch, BackendOptions
from asanypath_native import (
    az_copy_batch,
    az_delete_batch,
    az_exists_batch,
    az_get_batch,
    az_get_container_acl,
    az_head_batch,
    az_is_dir,
    az_is_dir_batch,
    az_list_batch,
    az_list_containers,
    az_presign,
    az_put_batch,
    az_put_container_acl,
)

AZURE_API_VERSION = "2023-11-03"


def _parse_connection_string(cs: str | None) -> dict[str, str]:
    """Parse an Azure Storage connection string into asanypath config keys."""
    if not cs:
        return {}
    parts: dict[str, str] = {}
    for seg in cs.split(";"):
        key, sep, val = seg.partition("=")
        if sep and key.strip():
            parts[key.strip().lower()] = val.strip()
    out: dict[str, str] = {}
    if name := parts.get("accountname"):
        out["account_name"] = name
    if key := parts.get("accountkey"):
        out["account_key"] = key
    if sas := parts.get("sharedaccesssignature"):
        out["sas_token"] = sas
    if endpoint := parts.get("blobendpoint"):
        out["endpoint_url"] = endpoint
    elif name := parts.get("accountname"):
        proto = parts.get("defaultendpointsprotocol", "https")
        suffix = parts.get("endpointsuffix", "core.windows.net")
        out["endpoint_url"] = f"{proto}://{name}.blob.{suffix}"
    return out


class AzurePath(CloudPathMixin):
    """Async path implementation for Azure Blob Storage.

    Uses the Azure Blob Storage REST API via the shared-key or SAS authentication.
    Supports: exists, is_file, is_dir, iterdir, read/write, unlink, touch, rename, stat.

    Example:
        ```python
        path = AzurePath("az://container/blob/path.txt",
                        account_name="myaccount",
                        account_key="base64-encoded-key")
        content = await path.read_bytes()
        ```
    """

    protocol: str = "az"
    _supports_range_read: bool = True
    _is_dir_batch_fn = staticmethod(az_is_dir_batch)
    _copy_batch_fn = staticmethod(az_copy_batch)

    @staticmethod
    def _batcher_key_fn(
        endpoint: str,
        container: str,
        account_name: str = "",
        account_key: str | None = None,
        sas_token: str | None = None,
    ) -> str:
        return f"{endpoint}|{container}|{account_name}"

    _batcher_ops = {
        "get": {"batch_fn": az_get_batch, "items_key": "blob_paths", "wrap": bytes},
        "exists": {"batch_fn": az_exists_batch, "items_key": "blob_paths"},
        "head": {"batch_fn": az_head_batch, "items_key": "blob_paths"},
        "delete": {"batch_fn": az_delete_batch, "items_key": "blob_paths"},
        "put": {"batch_fn": az_put_batch},
        "list": {"batch_fn": az_list_batch, "items_key": "prefixes"},
    }

    def __init__(
        self,
        *args,
        endpoint_url: str | None = None,
        account_name: str | None = None,
        account_key: str | None = None,
        sas_token: str | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        cfg = self.env_config
        self._account_name = account_name or cfg.account_name
        self._account_key = account_key or cfg.account_key
        self._sas_token = sas_token or cfg.sas_token
        self._endpoint_url = (
            endpoint_url
            or cfg.endpoint_url
            or (f"https://{self._account_name}.blob.core.windows.net")
        )
        # Promote explicit params into class cache so derived instances
        # (parent, /, with_name, etc.) inherit them.
        if account_name:
            cfg.account_name = account_name
        if account_key:
            cfg.account_key = account_key
        if sas_token:
            cfg.sas_token = sas_token
        if endpoint_url:
            cfg.endpoint_url = endpoint_url

        # URL format: az://container/blob/path
        self._container = self._path.host or ""
        self._blob_path = self._path.path.lstrip("/")

    @classmethod
    def _create_env_config(cls) -> SimpleNamespace:
        """Build env config from Azure environment variables / connection string."""
        conn = _parse_connection_string(getenv("AZURE_STORAGE_CONNECTION_STRING"))
        account_name = getenv("AZURE_STORAGE_ACCOUNT") or conn.get("account_name")
        return SimpleNamespace(
            account_name=account_name,
            account_key=getenv("AZURE_STORAGE_KEY") or conn.get("account_key"),
            sas_token=getenv("AZURE_STORAGE_SAS_TOKEN") or conn.get("sas_token"),
            endpoint_url=(
                getenv("AZURE_STORAGE_ENDPOINT")
                or conn.get("endpoint_url")
                or (f"https://{account_name}.blob.core.windows.net" if account_name else None)
            ),
        )

    # ------------------------------------------------------------------
    # CloudPathMixin hooks
    # ------------------------------------------------------------------

    @property
    def _item_path(self) -> str:
        return self._blob_path

    @property
    def _native_kwargs(self) -> dict:
        return {
            "endpoint": self._endpoint_url,
            "container": self._container,
            "account_name": self._account_name or "",
            "account_key": self._account_key,
            "sas_token": self._sas_token,
        }

    # ------------------------------------------------------------------
    # Backend-specific operations
    # ------------------------------------------------------------------

    async def is_dir(self) -> bool:
        """Check if path is a directory prefix (has blobs with this prefix)."""
        return await az_is_dir(prefix=self._blob_path, **self._native_kwargs)

    async def _list_containers(self) -> list[str] | None:
        """List containers when at the account root (``az://``); else None."""
        if self._container:
            return None
        names = await az_list_containers(
            endpoint=self._endpoint_url,
            account_name=self._account_name or "",
            account_key=self._account_key,
            sas_token=self._sas_token,
            use_h2=False,
        )
        return [f"az://{name}" for name in names]

    async def stat(self, *, follow_symlinks: bool = True) -> object:
        """Get blob metadata from HEAD response."""
        from types import SimpleNamespace

        headers = await self._get_batcher().head(
            item=self._blob_path,
            **self._native_kwargs,
        )
        size = int(headers.get("Content-Length", headers.get("content-length", 0)))
        last_modified = headers.get("Last-Modified", headers.get("last-modified"))
        mtime = None
        if last_modified:
            mtime = (
                datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S GMT")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
        return SimpleNamespace(st_size=size, st_ctime=mtime, st_mtime=mtime)

    async def checksums(self) -> dict[str, str]:
        """Return Content-MD5 from blob properties."""
        headers = await self._get_batcher().head(
            item=self._blob_path,
            **self._native_kwargs,
        )
        md5 = headers.get("Content-MD5", headers.get("content-md5", ""))
        return {"md5": md5}

    async def get_access_policy(
        self, *, backend_options: BackendOptions | None = None
    ) -> AccessPolicy:
        """Return the containing container's public access and stored policies."""
        data = await az_get_container_acl(**self._native_kwargs)
        if isinstance(data, str):
            data = msgspec.json.decode(data)
        if not isinstance(data, Mapping):
            raise TypeError("native Azure ACL response must be a mapping")
        public_access = data.get("public_access")
        grants = (
            (AccessGrant("everyone", frozenset({"read"})),)
            if public_access in {"blob", "container", "true"}
            else ()
        )
        return AccessPolicy(grants=grants, provider={"azure_container_acl": data})

    async def update_access_policy(
        self, policy_patch: AccessPolicyPatch, *, backend_options: BackendOptions | None = None
    ) -> None:
        """Replace the container ACL using ``provider["azure_container_acl"]``."""
        acl = policy_patch.provider.get("azure_container_acl")
        if not isinstance(acl, Mapping):
            raise ValueError("Azure ACL replacement requires provider['azure_container_acl']")
        await az_put_container_acl(
            acl_json=msgspec.json.encode(acl).decode(), **self._native_kwargs
        )

    async def presign(self, *, expires: int = 3600, method: str = "GET") -> str:
        """Generate a presigned URL for this Azure blob using a Service SAS token.

        Requires account_key to be set. If a SAS token is already configured,
        returns the blob URL with the existing SAS token appended.

        Args:
            expires: URL validity in seconds (default 3600).
            method: HTTP method (used to determine SAS permission: r, w, d, c).

        Returns:
            A presigned URL string.
        """
        return az_presign(
            endpoint=self._endpoint_url,
            container=self._container,
            blob_path=self._blob_path,
            account_name=self._account_name or "",
            account_key=self._account_key,
            sas_token=self._sas_token,
            method=method,
            expires=expires,
        )
