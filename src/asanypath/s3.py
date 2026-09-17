# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

from collections.abc import Mapping
from configparser import ConfigParser
from datetime import datetime, timezone
from os import getenv
from pathlib import Path
from types import SimpleNamespace

import msgspec

from asanypath.cloud import CloudPathMixin
from asanypath.options import AccessGrant, AccessPolicy, AccessPolicyPatch, BackendOptions
from asanypath_native import (
    s3_copy_batch,
    s3_delete_batch,
    s3_exists_batch,
    s3_get_acl,
    s3_get_batch,
    s3_head_batch,
    s3_is_dir,
    s3_is_dir_batch,
    s3_list_batch,
    s3_list_buckets,
    s3_presign,
    s3_put_acl,
    s3_put_batch,
)


class S3Path(CloudPathMixin):
    protocol: str = "s3"
    _supports_range_read: bool = True

    @staticmethod
    def _batcher_key_fn(
        endpoint: str,
        bucket: str,
        region: str,
        access_key: str,
        secret_key: str,
        session_token: str | None = None,
    ) -> str:
        return f"{endpoint}|{bucket}|{region}|{access_key}"

    _batcher_ops = {
        "get": {"batch_fn": s3_get_batch, "items_key": "keys", "wrap": bytes},
        "exists": {"batch_fn": s3_exists_batch, "items_key": "keys"},
        "head": {"batch_fn": s3_head_batch, "items_key": "keys"},
        "delete": {"batch_fn": s3_delete_batch, "items_key": "keys"},
        "put": {"batch_fn": s3_put_batch},
        "list": {"batch_fn": s3_list_batch, "items_key": "prefixes"},
    }

    _is_dir_batch_fn = staticmethod(s3_is_dir_batch)
    _copy_batch_fn = staticmethod(s3_copy_batch)

    def __init__(
        self,
        *args,
        endpoint_url: str | None = None,
        aws_region: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        cfg = self.env_config
        self._endpoint_url = endpoint_url or cfg.endpoint_url
        self._region = aws_region or cfg.aws_region
        self._access_key = aws_access_key_id or cfg.aws_access_key_id
        self._secret_key = aws_secret_access_key or cfg.aws_secret_access_key
        self._session_token = aws_session_token or cfg.aws_session_token
        self._bucket = self._path._netloc
        # Promote explicit params into class cache so derived instances
        # (parent, /, with_name, etc.) inherit them.
        if endpoint_url:
            cfg.endpoint_url = endpoint_url
        if aws_region:
            cfg.aws_region = aws_region
        if aws_access_key_id:
            cfg.aws_access_key_id = aws_access_key_id
        if aws_secret_access_key:
            cfg.aws_secret_access_key = aws_secret_access_key
        if aws_session_token:
            cfg.aws_session_token = aws_session_token

    @classmethod
    def _create_env_config(cls) -> SimpleNamespace:
        """Build env config from AWS env vars and credential files."""
        profile = getenv("AWS_PROFILE", "default")
        credsfile = str(
            Path(getenv("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials")).expanduser()
        )
        cfgfile = str(Path(getenv("AWS_CONFIG_FILE", "~/.aws/config")).expanduser())

        cfg = {
            "endpoint_url": getenv("AWS_ENDPOINT_URL"),
            "aws_region": getenv("AWS_REGION", "us-east-1"),
            "aws_access_key_id": getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": getenv("AWS_SECRET_ACCESS_KEY"),
            "aws_session_token": getenv("AWS_SESSION_TOKEN"),
        }

        cp = ConfigParser()
        if cp.read(filenames=credsfile):
            if profile in cp:
                cfg.update(cp[profile])
        if cp.read(filenames=cfgfile):
            section = profile if profile == "default" else f"profile {profile}"
            if section in cp:
                cfg.update(cp[section])

        return SimpleNamespace(**cfg)

    # ------------------------------------------------------------------
    # CloudPathMixin hooks
    # ------------------------------------------------------------------

    @property
    def _item_path(self) -> str:
        return self._path.path.strip("/")

    @property
    def _native_kwargs(self) -> dict:
        return {
            "endpoint": self._endpoint_url,
            "bucket": self._bucket,
            "region": self._region,
            "access_key": self._access_key,
            "secret_key": self._secret_key,
            "session_token": self._session_token or None,
        }

    # ------------------------------------------------------------------
    # Backend-specific operations
    # ------------------------------------------------------------------

    async def is_dir(self) -> bool:
        """Check if path is a directorish."""
        return await s3_is_dir(prefix=self._item_path, **self._native_kwargs)

    async def _list_containers(self) -> list[str] | None:
        """List buckets when at the service root (``s3://``); else None."""
        if self._bucket:
            return None
        names = await s3_list_buckets(
            endpoint=self._endpoint_url,
            region=self._region,
            access_key=self._access_key,
            secret_key=self._secret_key,
            session_token=self._session_token or None,
            use_h2=False,
        )
        return [f"s3://{name}" for name in names]

    async def stat(self, *, follow_symlinks: bool = True):
        """Return file metadata from a HEAD request."""
        headers = await self._get_batcher().head(
            item=self._item_path,
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
        """Return MD5 checksum from the S3 ETag header."""
        headers = await self._get_batcher().head(
            item=self._item_path,
            **self._native_kwargs,
        )
        etag = headers.get("ETag", headers.get("etag", "")).strip('"')
        return {"md5": etag}

    async def get_access_policy(
        self, *, backend_options: BackendOptions | None = None
    ) -> AccessPolicy:
        """Return the object ACL and its normalized read/write grants.

        S3 ACL permissions beyond read/write remain available in ``provider``
        because they do not have portable POSIX equivalents.
        """
        data = await s3_get_acl(key=self._item_path, **self._native_kwargs)
        if isinstance(data, str):
            data = msgspec.json.decode(data)
        if not isinstance(data, Mapping):
            raise TypeError("native S3 ACL response must be a mapping")
        grants = []
        for grant in data.get("grants", []):
            if not isinstance(grant, Mapping):
                continue
            permission = grant.get("permission")
            principal = grant.get("principal")
            if not isinstance(principal, str):
                continue
            actions = frozenset(
                action
                for action, allowed in (
                    ("read", permission in {"READ", "FULL_CONTROL"}),
                    ("write", permission in {"WRITE", "FULL_CONTROL"}),
                )
                if allowed
            )
            if actions:
                grants.append(AccessGrant(principal, actions))
        owner = data.get("owner")
        return AccessPolicy(
            owner=owner if isinstance(owner, str) else None,
            grants=tuple(grants),
            provider={"s3_acl": data},
        )

    async def update_access_policy(
        self, policy_patch: AccessPolicyPatch, *, backend_options: BackendOptions | None = None
    ) -> None:
        """Replace the object ACL using a complete ``provider["s3_acl"]`` document."""
        acl = policy_patch.provider.get("s3_acl")
        if not isinstance(acl, Mapping):
            raise ValueError("S3 ACL replacement requires provider['s3_acl']")
        await s3_put_acl(
            key=self._item_path,
            acl_json=msgspec.json.encode(acl).decode(),
            **self._native_kwargs,
        )

    async def presign(self, *, expires: int = 3600, method: str = "GET") -> str:
        """Generate a presigned URL for this S3 object.

        Uses AWS Signature V4 query-string presigning via native Rust implementation.

        Args:
            expires: URL validity in seconds (default 3600, max 604800).
            method: HTTP method the URL will be used for (GET, PUT, etc.).

        Returns:
            A presigned URL string.
        """
        if expires < 1 or expires > 604800:
            raise ValueError("expires must be between 1 and 604800 seconds")

        return s3_presign(
            endpoint=self._endpoint_url,
            bucket=self._bucket,
            key=self._item_path,
            region=self._region,
            access_key=self._access_key,
            secret_key=self._secret_key,
            session_token=self._session_token,
            method=method,
            expires=expires,
        )
