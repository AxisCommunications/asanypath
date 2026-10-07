# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import cast

import msgspec
from asanypath_native import (
    s3_abort_multipart,
    s3_complete_multipart,
    s3_copy_batch,
    s3_create_multipart,
    s3_delete_batch,
    s3_exists_batch,
    s3_get_acl,
    s3_get_batch,
    s3_get_credentials,
    s3_head_batch,
    s3_is_dir,
    s3_is_dir_batch,
    s3_list_batch,
    s3_list_buckets,
    s3_presign,
    s3_put_acl,
    s3_put_batch,
    s3_upload_part,
)
from yarl import URL

from asanypath.cloud import CloudPathMixin
from asanypath.options import (
    AccessAction,
    AccessGrant,
    AccessPolicy,
    AccessPolicyPatch,
    BackendOptions,
)


def _default_s3_endpoint(region: str) -> str:
    if region == "us-east-1":
        return "https://s3.amazonaws.com"
    suffix = "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"
    return f"https://s3.{region}.{suffix}"


class S3Path(CloudPathMixin):
    protocol: str = "s3"
    _supports_range_read: bool = True
    # AWS CLI defaults: single PUT below the threshold, else fixed-size parts.
    _MULTIPART_THRESHOLD = 8 * 1024 * 1024
    _MULTIPART_PART_SIZE = 8 * 1024 * 1024

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
        self._endpoint_override = endpoint_url or cfg.endpoint_url
        self._region_override = aws_region or cfg.aws_region
        self._endpoint_url = self._endpoint_override or _default_s3_endpoint(
            self._region_override or "us-east-1"
        )
        self._region = self._region_override or "us-east-1"
        self._access_key = aws_access_key_id or cfg.aws_access_key_id
        self._secret_key = aws_secret_access_key or cfg.aws_secret_access_key
        self._session_token = aws_session_token or cfg.aws_session_token
        self._bucket = cast(URL, self._path)._netloc
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
        """Hold explicit per-path overrides; aws-config resolves AWS configuration."""
        return SimpleNamespace(
            endpoint_url=None,
            aws_region=None,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_session_token=None,
        )

    # ------------------------------------------------------------------
    # CloudPathMixin hooks
    # ------------------------------------------------------------------

    @property
    def _item_path(self) -> str:
        return cast(URL, self._path).path.strip("/")

    def _bind_path_attrs(self) -> None:
        self._bucket = cast(URL, self._path)._netloc

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

    async def _get_native_kwargs(self) -> dict:
        if (
            self._access_key
            and self._secret_key
            and self._region_override
            and self._endpoint_override
        ):
            return self._native_kwargs
        access_key, secret_key, session_token, region, aws_endpoint = await s3_get_credentials(
            region_override=self._region_override,
            access_key=self._access_key,
            secret_key=self._secret_key,
            session_token=self._session_token,
        )
        self._region = region
        self._endpoint_url = self._endpoint_override or aws_endpoint or _default_s3_endpoint(region)
        return {
            **self._native_kwargs,
            "access_key": access_key,
            "secret_key": secret_key,
            "session_token": session_token,
        }

    # ------------------------------------------------------------------
    # Backend-specific operations
    # ------------------------------------------------------------------

    async def _upload_buffer(self, fileobj, size: int, *, backend_options=None) -> None:
        """Upload a spooled write buffer, using multipart for large objects."""
        if size < self._MULTIPART_THRESHOLD:
            data = fileobj.read()
            if backend_options is None:
                await self.write_bytes(data)
            else:
                await self.write_bytes(data, backend_options=backend_options)
            return
        await self._multipart_upload(fileobj, backend_options=backend_options)

    async def _multipart_upload(self, fileobj, *, backend_options=None) -> None:
        """Stream the buffer to S3 as multipart parts (memory bounded to one part)."""
        options_json = (
            msgspec.json.encode(backend_options).decode() if backend_options is not None else "{}"
        )
        native_kwargs = await self._get_native_kwargs()
        upload_id = await s3_create_multipart(
            key=self._item_path, options_json=options_json, **native_kwargs
        )
        parts: list[tuple[int, str]] = []
        try:
            part_number = 1
            while True:
                chunk = fileobj.read(self._MULTIPART_PART_SIZE)
                if not chunk:
                    break
                etag = await s3_upload_part(
                    key=self._item_path,
                    upload_id=upload_id,
                    part_number=part_number,
                    data=chunk,
                    **native_kwargs,
                )
                parts.append((part_number, etag))
                part_number += 1
            await s3_complete_multipart(
                key=self._item_path, upload_id=upload_id, parts=parts, **native_kwargs
            )
        except BaseException:
            with suppress(Exception):
                await s3_abort_multipart(key=self._item_path, upload_id=upload_id, **native_kwargs)
            raise
        type(self)._listing_cache.pop(self._listing_cache_key(), None)

    async def is_dir(self) -> bool:
        """Check if path is a directorish."""
        return await s3_is_dir(prefix=self._item_path, **(await self._get_native_kwargs()))

    async def _list_containers(self) -> list[str] | None:
        """List buckets when at the service root (``s3://``); else None."""
        if self._bucket:
            return None
        native_kwargs = await self._get_native_kwargs()
        names = await s3_list_buckets(
            endpoint=self._endpoint_url,
            region=self._region,
            access_key=native_kwargs["access_key"],
            secret_key=native_kwargs["secret_key"],
            session_token=native_kwargs["session_token"],
            use_h2=False,
        )
        return [f"s3://{name}" for name in names]

    async def stat(self, *, follow_symlinks: bool = True):
        """Return file metadata from a HEAD request."""
        native_kwargs = await self._get_native_kwargs()
        headers = await self._get_batcher().head(
            item=self._item_path,
            **native_kwargs,
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
        native_kwargs = await self._get_native_kwargs()
        headers = await self._get_batcher().head(
            item=self._item_path,
            **native_kwargs,
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
        data = await s3_get_acl(key=self._item_path, **(await self._get_native_kwargs()))
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
            actions = cast(
                frozenset[AccessAction],
                frozenset(
                    action
                    for action, allowed in (
                        ("read", permission in {"READ", "FULL_CONTROL"}),
                        ("write", permission in {"WRITE", "FULL_CONTROL"}),
                    )
                    if allowed
                ),
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
            **(await self._get_native_kwargs()),
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

        native_kwargs = await self._get_native_kwargs()
        return s3_presign(
            endpoint=self._endpoint_url,
            bucket=self._bucket,
            key=self._item_path,
            region=self._region,
            access_key=native_kwargs["access_key"],
            secret_key=native_kwargs["secret_key"],
            session_token=native_kwargs["session_token"],
            method=method,
            expires=expires,
        )
