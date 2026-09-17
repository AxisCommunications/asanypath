# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Google Cloud Storage path implementation (HTTP-style adapter).

Uses the GCS JSON API:
https://cloud.google.com/storage/docs/json_api/v1
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from os import getenv
from types import SimpleNamespace

import msgspec

from asanypath.cloud import CloudPathMixin
from asanypath.options import AccessGrant, AccessPolicy, AccessPolicyPatch, BackendOptions
from asanypath_native import (
    gcs_copy_batch,
    gcs_delete_batch,
    gcs_exists_batch,
    gcs_get_acl,
    gcs_get_batch,
    gcs_head,
    gcs_head_batch,
    gcs_is_dir,
    gcs_is_dir_batch,
    gcs_list_batch,
    gcs_list_buckets,
    gcs_presign,
    gcs_put_acl,
    gcs_put_batch,
)

GCS_API_BASE = "https://storage.googleapis.com"


class GCSPath(CloudPathMixin):
    """Async path implementation for Google Cloud Storage.

    Uses the GCS JSON API for object operations.
    Supports: exists, is_file, is_dir, iterdir, read/write, unlink, touch, rename, stat.

    Example:
        ```python
        path = GCSPath("gs://bucket/object/path.txt",
                      endpoint_url="http://localhost:4443")
        content = await path.read_bytes()
        ```
    """

    protocol: str = "gs"
    _supports_range_read: bool = True
    _is_dir_batch_fn = staticmethod(gcs_is_dir_batch)
    _copy_batch_fn = staticmethod(gcs_copy_batch)

    @staticmethod
    def _batcher_key_fn(endpoint: str, bucket: str, access_token: str | None = None) -> str:
        return f"{endpoint}|{bucket}|{access_token or ''}"

    _batcher_ops = {
        "get": {"batch_fn": gcs_get_batch, "items_key": "object_paths", "wrap": bytes},
        "exists": {"batch_fn": gcs_exists_batch, "items_key": "object_paths"},
        "head": {"batch_fn": gcs_head_batch, "items_key": "object_paths"},
        "delete": {"batch_fn": gcs_delete_batch, "items_key": "object_paths"},
        "put": {"batch_fn": gcs_put_batch},
        "list": {"batch_fn": gcs_list_batch, "items_key": "prefixes"},
    }

    def __init__(
        self,
        *args,
        endpoint_url: str | None = None,
        project_id: str | None = None,
        credentials_path: str | None = None,
        access_token: str | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        cfg = self.env_config
        self._project_id = project_id or cfg.project_id
        self._credentials_path = credentials_path or cfg.credentials_path
        self._access_token = access_token or cfg.access_token
        self._endpoint_url = endpoint_url or cfg.endpoint_url
        # Promote explicit params into class cache so derived instances
        # (parent, /, with_name, etc.) inherit them.
        if project_id:
            cfg.project_id = project_id
        if credentials_path:
            cfg.credentials_path = credentials_path
        if access_token:
            cfg.access_token = access_token
        if endpoint_url:
            cfg.endpoint_url = endpoint_url

        # URL format: gs://bucket/object/path
        self._bucket = self._path.host or ""
        self._object_path = self._path.path.lstrip("/")

    @classmethod
    def _create_env_config(cls) -> SimpleNamespace:  # pragma: no cover
        """Build env config from GCP environment variables."""
        # Only called if cfg_instance is None; hard to trigger in unit tests.
        # Implicitly tested through integration tests with cloud paths.
        return SimpleNamespace(
            project_id=getenv("GCP_PROJECT") or getenv("GOOGLE_CLOUD_PROJECT"),
            credentials_path=getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            access_token=getenv("GCS_ACCESS_TOKEN"),
            endpoint_url=getenv("STORAGE_EMULATOR_HOST", GCS_API_BASE),
        )

    # ------------------------------------------------------------------
    # CloudPathMixin hooks
    # ------------------------------------------------------------------

    @property
    def _item_path(self) -> str:
        return self._object_path

    @property
    def _native_kwargs(self) -> dict:
        return {
            "endpoint": self._endpoint_url,
            "bucket": self._bucket,
            "access_token": self._access_token,
        }

    # ------------------------------------------------------------------
    # Backend-specific operations
    # ------------------------------------------------------------------

    async def is_dir(self) -> bool:
        """Check if path is a directory prefix (has objects with this prefix)."""
        return await gcs_is_dir(prefix=self._object_path, **self._native_kwargs)

    async def _list_containers(self) -> list[str] | None:
        """List buckets when at the service root (``gs://``); else None."""
        if self._bucket:
            return None
        names = await gcs_list_buckets(
            endpoint=self._endpoint_url,
            project=self._project_id or "",
            access_token=self._access_token,
            use_h2=False,
        )
        return [f"gs://{name}" for name in names]

    async def stat(self, *, follow_symlinks: bool = True) -> object:
        """Get object metadata."""
        from types import SimpleNamespace

        data = await gcs_head(object_path=self._object_path, **self._native_kwargs)
        size = int(data.get("size", 0))
        updated = data.get("updated")
        mtime = None
        if updated:
            mtime = datetime.fromisoformat(updated.replace("Z", "+00:00")).timestamp()
        return SimpleNamespace(st_size=size, st_ctime=mtime, st_mtime=mtime)

    async def checksums(self) -> dict[str, str]:
        """Return checksums from object metadata."""
        data = await gcs_head(object_path=self._object_path, **self._native_kwargs)
        result = {}
        if "md5Hash" in data:
            result["md5"] = data["md5Hash"]
        if "crc32c" in data:
            result["crc32c"] = data["crc32c"]
        return result

    async def get_access_policy(
        self, *, backend_options: BackendOptions | None = None
    ) -> AccessPolicy:
        """Return the object ACL and its normalized read/write grants."""
        data = await gcs_get_acl(object_path=self._object_path, **self._native_kwargs)
        if isinstance(data, str):
            data = msgspec.json.decode(data)
        if not isinstance(data, Mapping):
            raise TypeError("native GCS ACL response must be a mapping")
        grants = []
        for grant in data.get("grants", []):
            if not isinstance(grant, Mapping):
                continue
            principal = grant.get("principal")
            permission = grant.get("permission")
            if not isinstance(principal, str) or permission not in {"READER", "OWNER"}:
                continue
            actions = frozenset({"read", "write"} if permission == "OWNER" else {"read"})
            grants.append(AccessGrant(principal, actions))
        owner = data.get("owner")
        return AccessPolicy(
            owner=owner if isinstance(owner, str) else None,
            grants=tuple(grants),
            provider={"gcs_acl": data},
        )

    async def update_access_policy(
        self, policy_patch: AccessPolicyPatch, *, backend_options: BackendOptions | None = None
    ) -> None:
        """Replace the object ACL using a complete ``provider["gcs_acl"]`` document."""
        acl = policy_patch.provider.get("gcs_acl")
        if not isinstance(acl, Mapping):
            raise ValueError("GCS ACL replacement requires provider['gcs_acl']")
        await gcs_put_acl(
            object_path=self._object_path,
            acl_json=msgspec.json.encode(acl).decode(),
            **self._native_kwargs,
        )

    async def presign(self, *, expires: int = 3600, method: str = "GET") -> str:
        """Generate a V4 signed URL for this GCS object.

        Requires a service account JSON key file (via credentials_path or
        GOOGLE_APPLICATION_CREDENTIALS).

        Args:
            expires: URL validity in seconds (default 3600, max 604800).
            method: HTTP method the URL will be used for.

        Returns:
            A presigned URL string.
        """
        if expires < 1 or expires > 604800:  # pragma: no cover
            raise ValueError("expires must be between 1 and 604800 seconds")

        if not self._credentials_path:  # pragma: no cover
            raise ValueError(
                "presign requires credentials_path "
                "(GOOGLE_APPLICATION_CREDENTIALS) pointing to a service account JSON key"
            )

        from pathlib import Path

        sa_json = Path(self._credentials_path).read_text()

        return gcs_presign(
            bucket=self._bucket,
            object_path=self._object_path,
            service_account_json=sa_json,
            method=method,
            expires=expires,
        )
