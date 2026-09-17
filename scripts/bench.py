# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

#!/usr/bin/env python3
"""Benchmark asanypath against fsspec, cloudpathlib, and native async SDKs.

Covers S3, Azure Blob, GCS, Artifactory, SSH/SFTP, FTP, and local filesystem
backends. Operations: write, read, exists, iterdir, unlink, cp (cloud: native
server-side copy vs client read+write).

Usage:
    # Local only (no cloud creds needed):
    uv run --extra bench python scripts/bench.py --backend local --rounds 3

    # Against emulators (docker compose up -d):
    uv run --extra bench python scripts/bench.py --backend all --rounds 3

    # SSH/SFTP (atmoz/sftp emulator); disable local ssh_config + agent so the
    # emulator's password auth is used instead of your keys:
    env SSH_CONFIG= SSH_AUTH_SOCK= \\
        uv run --extra bench python scripts/bench.py --backend ssh --rounds 20

    # FTP (delfer/alpine-ftp-server emulator):
    uv run --extra bench python scripts/bench.py --backend ftp --rounds 20

    # Against real cloud (configure creds via env):
    uv run --extra bench python scripts/bench.py --backend s3 --rounds 5

Environment variables for emulators:
    AWS_ENDPOINT_URL=http://localhost:9000        (MinIO/LocalStack)
    AZURE_STORAGE_CONNECTION_STRING=...           (Azurite)
    STORAGE_EMULATOR_HOST=http://localhost:4443   (fake-gcs-server)
    BENCH_SSH_HOST/PORT/USER/PASSWORD/ROOT        (default localhost/2222/bench/bench//upload)
    BENCH_FTP_HOST/PORT/USER/PASSWORD/ROOT        (default localhost/2121/bench/bench//ftp/bench)

``ssh`` is not part of ``--backend all`` because it needs the SSH_CONFIG/
SSH_AUTH_SOCK overrides above; run it explicitly.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import secrets
import shutil
import statistics
import sys
import tempfile
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


def _load_dotenv() -> None:
    """Load .env file from script directory or cwd (no dependency needed)."""
    for candidate in (Path(__file__).resolve().parent.parent / ".env", Path.cwd() / ".env"):
        if candidate.is_file():
            for line in candidate.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())
            break


_load_dotenv()

# Suppress noisy third-party library warnings
warnings.filterwarnings("ignore")
for _mod in ("gcsfs", "s3fs", "adlfs", "fsspec", "aiobotocore", "botocore", "urllib3"):
    logging.getLogger(_mod).setLevel(logging.CRITICAL)

log = logging.getLogger("bench")


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass
class BenchResult:
    op: str
    impl: str
    backend: str
    rounds: int
    size_bytes: int
    concurrency: int
    mean_ms: float
    median_ms: float
    min_ms: float
    max_ms: float
    mean_ops_s: float


def _summarize(
    op: str,
    impl: str,
    backend: str,
    samples_ms: list[float],
    *,
    size_bytes: int,
    concurrency: int,
) -> BenchResult:
    ops_s = [(concurrency * 1000.0) / ms if ms else float("inf") for ms in samples_ms]
    return BenchResult(
        op=op,
        impl=impl,
        backend=backend,
        rounds=len(samples_ms),
        size_bytes=size_bytes,
        concurrency=concurrency,
        mean_ms=statistics.mean(samples_ms),
        median_ms=statistics.median(samples_ms),
        min_ms=min(samples_ms),
        max_ms=max(samples_ms),
        mean_ops_s=statistics.mean(ops_s),
    )


# ---------------------------------------------------------------------------
# Adapter protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class BenchAdapter(Protocol):
    name: str

    async def setup(self) -> None: ...
    async def teardown(self) -> None: ...
    async def write(self, uri: str, data: bytes) -> None: ...
    async def read(self, uri: str) -> bytes: ...
    async def exists(self, uri: str) -> bool: ...
    async def iterdir(self, uri: str) -> list[str]: ...
    async def unlink(self, uri: str) -> None: ...


# ---------------------------------------------------------------------------
# Adapter implementations
# ---------------------------------------------------------------------------


class AsAnyPathAdapter:
    """Adapter for asanypath (our library)."""

    name = "asanypath"

    def __init__(self, backend: str, **path_kwargs):
        self._backend = backend
        self._kwargs = path_kwargs
        # ssh/ftp are real hierarchical filesystems: parent dirs must exist
        # before a write (unlike object stores). Cache created dirs so only the
        # first write to each dir pays the mkdir cost.
        self._hierarchical = backend in ("local", "ssh", "ftp")
        self._made: set[str] = set()

    def _p(self, uri: str):
        from asanypath import AsAnyPath

        return AsAnyPath(uri, **self._kwargs)

    async def setup(self) -> None:
        pass

    async def teardown(self) -> None:
        pass

    def clear_cache(self) -> None:
        from asanypath.cloud import CloudPathMixin

        CloudPathMixin._listing_cache.clear()

    async def write(self, uri: str, data: bytes) -> None:
        p = self._p(uri)
        if self._hierarchical:
            # Build the parent from the URI (not p.parent) so credential kwargs
            # like ssh password survive on the derived path.
            parent_uri = uri.rsplit("/", 1)[0]
            if parent_uri not in self._made:
                await self._p(parent_uri).mkdir(parents=True, exist_ok=True)
                self._made.add(parent_uri)
        await p.write_bytes(data)

    async def read(self, uri: str) -> bytes:
        return await self._p(uri).read_bytes()

    async def exists(self, uri: str) -> bool:
        return await self._p(uri).exists()

    async def iterdir(self, uri: str) -> list[str]:
        return [str(p) async for p in self._p(uri).iterdir()]

    async def unlink(self, uri: str) -> None:
        await self._p(uri).unlink()


class FsspecS3Adapter:
    """Adapter for s3fs (fsspec S3 filesystem)."""

    name = "s3fs"

    def __init__(self):
        self._fs = None

    async def setup(self) -> None:
        import s3fs

        kwargs = {}
        if endpoint := os.getenv("AWS_ENDPOINT_URL"):
            kwargs["endpoint_url"] = endpoint
            kwargs["anon"] = False
        self._fs = s3fs.S3FileSystem(**kwargs)

    async def teardown(self) -> None:
        pass

    def clear_cache(self) -> None:
        if self._fs is not None:
            self._fs.invalidate_cache()

    async def write(self, uri: str, data: bytes) -> None:
        path = uri.replace("s3://", "", 1)
        await asyncio.to_thread(self._fs.pipe, path, data)

    async def read(self, uri: str) -> bytes:
        path = uri.replace("s3://", "", 1)
        return await asyncio.to_thread(self._fs.cat, path)

    async def exists(self, uri: str) -> bool:
        path = uri.replace("s3://", "", 1)
        return await asyncio.to_thread(self._fs.exists, path)

    async def iterdir(self, uri: str) -> list[str]:
        path = uri.replace("s3://", "", 1)
        return await asyncio.to_thread(self._fs.ls, path)

    async def unlink(self, uri: str) -> None:
        path = uri.replace("s3://", "", 1)
        await asyncio.to_thread(self._fs.rm, path)


class FsspecAzureAdapter:
    """Adapter for adlfs (fsspec Azure Blob filesystem)."""

    name = "adlfs"

    def __init__(self):
        self._fs = None

    async def setup(self) -> None:
        import adlfs

        kwargs = {}
        if conn_str := os.getenv("AZURE_STORAGE_CONNECTION_STRING"):
            kwargs["connection_string"] = conn_str
        else:
            kwargs["account_name"] = os.getenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
            kwargs["account_key"] = os.getenv(
                "AZURE_STORAGE_KEY",
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
                "/K1SZFPTOtr/KBHBeksoGMGw==",
            )
        self._fs = adlfs.AzureBlobFileSystem(**kwargs)

    async def teardown(self) -> None:
        pass

    def clear_cache(self) -> None:
        if self._fs is not None:
            self._fs.invalidate_cache()

    async def write(self, uri: str, data: bytes) -> None:
        path = uri.replace("az://", "", 1)
        await asyncio.to_thread(self._fs.pipe, path, data)

    async def read(self, uri: str) -> bytes:
        path = uri.replace("az://", "", 1)
        return await asyncio.to_thread(self._fs.cat, path)

    async def exists(self, uri: str) -> bool:
        path = uri.replace("az://", "", 1)
        return await asyncio.to_thread(self._fs.exists, path)

    async def iterdir(self, uri: str) -> list[str]:
        path = uri.replace("az://", "", 1)
        return await asyncio.to_thread(self._fs.ls, path)

    async def unlink(self, uri: str) -> None:
        path = uri.replace("az://", "", 1)
        await asyncio.to_thread(self._fs.rm, path)


class FsspecGCSAdapter:
    """Adapter for gcsfs (fsspec GCS filesystem)."""

    name = "gcsfs"

    def __init__(self):
        self._fs = None

    async def setup(self) -> None:
        import gcsfs

        kwargs = {"token": "anon"}
        if endpoint := os.getenv("STORAGE_EMULATOR_HOST"):
            kwargs["endpoint_url"] = endpoint
        self._fs = gcsfs.GCSFileSystem(**kwargs)
        # fake-gcs (http) can't serve the secure-gRPC storage-layout probe newer
        # gcsfs uses for zonal/HNS detection (it hangs ~60s/op); pre-seed the
        # cache so the probe is skipped. Real GCS is left untouched.
        if os.getenv("STORAGE_EMULATOR_HOST") and hasattr(self._fs, "_storage_layout_cache"):
            from gcsfs.extended_gcsfs import BucketType

            bucket = os.getenv("BENCH_GCS_BUCKET", "bench-asanypath")
            self._fs._storage_layout_cache[bucket] = BucketType.NON_HIERARCHICAL

    async def teardown(self) -> None:
        pass

    def clear_cache(self) -> None:
        if self._fs is not None:
            self._fs.invalidate_cache()

    async def write(self, uri: str, data: bytes) -> None:
        path = uri.replace("gs://", "", 1)
        await asyncio.to_thread(self._fs.pipe, path, data)

    async def read(self, uri: str) -> bytes:
        path = uri.replace("gs://", "", 1)
        return await asyncio.to_thread(self._fs.cat, path)

    async def exists(self, uri: str) -> bool:
        path = uri.replace("gs://", "", 1)
        return await asyncio.to_thread(self._fs.exists, path)

    async def iterdir(self, uri: str) -> list[str]:
        path = uri.replace("gs://", "", 1)
        return await asyncio.to_thread(self._fs.ls, path)

    async def unlink(self, uri: str) -> None:
        path = uri.replace("gs://", "", 1)
        await asyncio.to_thread(self._fs.rm, path)


class FsspecLocalAdapter:
    """Adapter for fsspec local filesystem."""

    name = "fsspec-local"

    def __init__(self):
        self._fs = None

    async def setup(self) -> None:
        import fsspec

        self._fs = fsspec.filesystem("file")

    async def teardown(self) -> None:
        pass

    async def write(self, uri: str, data: bytes) -> None:
        path = uri.replace("file://", "", 1)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._fs.pipe, path, data)

    async def read(self, uri: str) -> bytes:
        path = uri.replace("file://", "", 1)
        return await asyncio.to_thread(self._fs.cat, path)

    async def exists(self, uri: str) -> bool:
        path = uri.replace("file://", "", 1)
        return await asyncio.to_thread(self._fs.exists, path)

    async def iterdir(self, uri: str) -> list[str]:
        path = uri.replace("file://", "", 1)
        return await asyncio.to_thread(self._fs.ls, path)

    async def unlink(self, uri: str) -> None:
        path = uri.replace("file://", "", 1)
        await asyncio.to_thread(self._fs.rm, path)


class CloudpathlibS3Adapter:
    """Adapter for cloudpathlib S3."""

    name = "cloudpathlib"

    async def setup(self) -> None:
        pass

    async def teardown(self) -> None:
        pass

    async def write(self, uri: str, data: bytes) -> None:
        from cloudpathlib import S3Path

        await asyncio.to_thread(S3Path(uri).write_bytes, data)

    async def read(self, uri: str) -> bytes:
        from cloudpathlib import S3Path

        return await asyncio.to_thread(S3Path(uri).read_bytes)

    async def exists(self, uri: str) -> bool:
        from cloudpathlib import S3Path

        return await asyncio.to_thread(S3Path(uri).exists)

    async def iterdir(self, uri: str) -> list[str]:
        from cloudpathlib import S3Path

        return await asyncio.to_thread(lambda: [str(p) for p in S3Path(uri).iterdir()])

    async def unlink(self, uri: str) -> None:
        from cloudpathlib import S3Path

        await asyncio.to_thread(S3Path(uri).unlink)


class CloudpathlibAzureAdapter:
    """Adapter for cloudpathlib Azure."""

    name = "cloudpathlib"

    async def setup(self) -> None:
        pass

    async def teardown(self) -> None:
        pass

    async def write(self, uri: str, data: bytes) -> None:
        from cloudpathlib import AzureBlobPath

        await asyncio.to_thread(AzureBlobPath(uri).write_bytes, data)

    async def read(self, uri: str) -> bytes:
        from cloudpathlib import AzureBlobPath

        return await asyncio.to_thread(AzureBlobPath(uri).read_bytes)

    async def exists(self, uri: str) -> bool:
        from cloudpathlib import AzureBlobPath

        return await asyncio.to_thread(AzureBlobPath(uri).exists)

    async def iterdir(self, uri: str) -> list[str]:
        from cloudpathlib import AzureBlobPath

        return await asyncio.to_thread(lambda: [str(p) for p in AzureBlobPath(uri).iterdir()])

    async def unlink(self, uri: str) -> None:
        from cloudpathlib import AzureBlobPath

        await asyncio.to_thread(AzureBlobPath(uri).unlink)


class CloudpathlibGCSAdapter:
    """Adapter for cloudpathlib GCS."""

    name = "cloudpathlib"

    async def setup(self) -> None:
        # Point cloudpathlib at the emulator with anonymous REST creds; otherwise
        # google-cloud-storage attempts real auth / a secure-gRPC probe that
        # hangs against fake-gcs (http). Real GCS is left on default auth.
        if endpoint := os.getenv("STORAGE_EMULATOR_HOST"):
            from cloudpathlib import GSClient
            from google.auth.credentials import AnonymousCredentials
            from google.cloud.storage import Client

            client = Client(
                project="test",
                credentials=AnonymousCredentials(),
                client_options={"api_endpoint": endpoint},
            )
            GSClient(storage_client=client).set_as_default_client()

    async def teardown(self) -> None:
        pass

    async def write(self, uri: str, data: bytes) -> None:
        from cloudpathlib import GSPath

        await asyncio.to_thread(GSPath(uri).write_bytes, data)

    async def read(self, uri: str) -> bytes:
        from cloudpathlib import GSPath

        return await asyncio.to_thread(GSPath(uri).read_bytes)

    async def exists(self, uri: str) -> bool:
        from cloudpathlib import GSPath

        return await asyncio.to_thread(GSPath(uri).exists)

    async def iterdir(self, uri: str) -> list[str]:
        from cloudpathlib import GSPath

        return await asyncio.to_thread(lambda: [str(p) for p in GSPath(uri).iterdir()])

    async def unlink(self, uri: str) -> None:
        from cloudpathlib import GSPath

        await asyncio.to_thread(GSPath(uri).unlink)


class NativeS3Adapter:
    """Adapter for aiobotocore (native async S3 SDK)."""

    name = "aiobotocore"

    def __init__(self):
        self._session = None

    async def setup(self) -> None:
        import aiobotocore.session

        self._session = aiobotocore.session.get_session()

    async def teardown(self) -> None:
        pass

    def _client_kwargs(self):
        kwargs = {}
        if endpoint := os.getenv("AWS_ENDPOINT_URL"):
            kwargs["endpoint_url"] = endpoint
        return kwargs

    def _parse(self, uri: str) -> tuple[str, str]:
        path = uri.replace("s3://", "", 1)
        bucket, _, key = path.partition("/")
        return bucket, key

    async def write(self, uri: str, data: bytes) -> None:
        bucket, key = self._parse(uri)
        async with self._session.create_client("s3", **self._client_kwargs()) as client:
            await client.put_object(Bucket=bucket, Key=key, Body=data)

    async def read(self, uri: str) -> bytes:
        bucket, key = self._parse(uri)
        async with self._session.create_client("s3", **self._client_kwargs()) as client:
            resp = await client.get_object(Bucket=bucket, Key=key)
            async with resp["Body"] as stream:
                return await stream.read()

    async def exists(self, uri: str) -> bool:
        bucket, key = self._parse(uri)
        async with self._session.create_client("s3", **self._client_kwargs()) as client:
            try:
                await client.head_object(Bucket=bucket, Key=key)
                return True
            except client.exceptions.NoSuchKey:
                return False
            except Exception as e:
                if "404" in str(e) or "Not Found" in str(e):
                    return False
                raise

    async def iterdir(self, uri: str) -> list[str]:
        bucket, prefix = self._parse(uri)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        async with self._session.create_client("s3", **self._client_kwargs()) as client:
            resp = await client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
            items = []
            for obj in resp.get("Contents", []):
                items.append(f"s3://{bucket}/{obj['Key']}")
            for p in resp.get("CommonPrefixes", []):
                items.append(f"s3://{bucket}/{p['Prefix'].rstrip('/')}")
            return items

    async def unlink(self, uri: str) -> None:
        bucket, key = self._parse(uri)
        async with self._session.create_client("s3", **self._client_kwargs()) as client:
            await client.delete_object(Bucket=bucket, Key=key)


class NativeAzureAdapter:
    """Adapter for azure.storage.blob.aio (native async Azure SDK)."""

    name = "azure-sdk"

    def __init__(self):
        self._client = None

    async def setup(self) -> None:
        from azure.storage.blob.aio import BlobServiceClient

        if conn_str := os.getenv("AZURE_STORAGE_CONNECTION_STRING"):
            self._client = BlobServiceClient.from_connection_string(conn_str)
        else:
            account = os.getenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
            key = os.getenv(
                "AZURE_STORAGE_KEY",
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
                "/K1SZFPTOtr/KBHBeksoGMGw==",
            )
            endpoint = os.getenv(
                "AZURE_STORAGE_ENDPOINT",
                f"http://127.0.0.1:10000/{account}",
            )
            self._client = BlobServiceClient(account_url=endpoint, credential=key)

    async def teardown(self) -> None:
        if self._client:
            await self._client.close()

    def _parse(self, uri: str) -> tuple[str, str]:
        path = uri.replace("az://", "", 1)
        container, _, blob = path.partition("/")
        return container, blob

    async def write(self, uri: str, data: bytes) -> None:
        container, blob = self._parse(uri)
        blob_client = self._client.get_blob_client(container, blob)
        await blob_client.upload_blob(data, overwrite=True)

    async def read(self, uri: str) -> bytes:
        container, blob = self._parse(uri)
        blob_client = self._client.get_blob_client(container, blob)
        stream = await blob_client.download_blob()
        return await stream.readall()

    async def exists(self, uri: str) -> bool:
        container, blob = self._parse(uri)
        blob_client = self._client.get_blob_client(container, blob)
        try:
            await blob_client.get_blob_properties()
            return True
        except Exception:
            return False

    async def iterdir(self, uri: str) -> list[str]:
        container, prefix = self._parse(uri)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        container_client = self._client.get_container_client(container)
        items = []
        async for blob in container_client.walk_blobs(name_starts_with=prefix, delimiter="/"):
            items.append(f"az://{container}/{blob.name.rstrip('/')}")
        return items

    async def unlink(self, uri: str) -> None:
        container, blob = self._parse(uri)
        blob_client = self._client.get_blob_client(container, blob)
        await blob_client.delete_blob()


class NativeGCSAdapter:
    """Adapter for gcloud-aio-storage (native async GCS SDK)."""

    name = "gcloud-aio"

    def __init__(self):
        self._session = None

    async def setup(self) -> None:
        import aiohttp

        self._session = aiohttp.ClientSession()

    async def teardown(self) -> None:
        if self._session:
            await self._session.close()

    def _parse(self, uri: str) -> tuple[str, str]:
        path = uri.replace("gs://", "", 1)
        bucket, _, obj = path.partition("/")
        return bucket, obj

    def _storage_kwargs(self):
        kwargs = {}
        if endpoint := os.getenv("STORAGE_EMULATOR_HOST"):
            kwargs["api_root"] = endpoint
        return kwargs

    async def write(self, uri: str, data: bytes) -> None:
        from gcloud.aio.storage import Storage

        bucket, obj_path = self._parse(uri)
        async with Storage(session=self._session, **self._storage_kwargs()) as storage:
            await storage.upload(bucket, obj_path, data)

    async def read(self, uri: str) -> bytes:
        from gcloud.aio.storage import Storage

        bucket, obj_path = self._parse(uri)
        async with Storage(session=self._session, **self._storage_kwargs()) as storage:
            return await storage.download(bucket, obj_path)

    async def exists(self, uri: str) -> bool:
        from gcloud.aio.storage import Storage

        bucket, obj_path = self._parse(uri)
        try:
            async with Storage(session=self._session, **self._storage_kwargs()) as storage:
                await storage.download_metadata(bucket, obj_path)
                return True
        except Exception:
            return False

    async def iterdir(self, uri: str) -> list[str]:
        from gcloud.aio.storage import Storage

        bucket, prefix = self._parse(uri)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        async with Storage(session=self._session, **self._storage_kwargs()) as storage:
            result = await storage.list_objects(bucket, params={"prefix": prefix, "delimiter": "/"})
            items = []
            for obj in result.get("items", []):
                items.append(f"gs://{bucket}/{obj['name'].rstrip('/')}")
            for p in result.get("prefixes", []):
                items.append(f"gs://{bucket}/{p.rstrip('/')}")
            return items

    async def unlink(self, uri: str) -> None:
        from gcloud.aio.storage import Storage

        bucket, obj_path = self._parse(uri)
        async with Storage(session=self._session, **self._storage_kwargs()) as storage:
            await storage.delete(bucket, obj_path)


class NativeLocalAdapter:
    """Adapter for aiofiles + pathlib (native async local)."""

    name = "aiofiles"

    async def setup(self) -> None:
        pass

    async def teardown(self) -> None:
        pass

    async def write(self, uri: str, data: bytes) -> None:
        import aiofiles

        path = uri.replace("file://", "", 1)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)

    async def read(self, uri: str) -> bytes:
        import aiofiles

        path = uri.replace("file://", "", 1)
        async with aiofiles.open(path, "rb") as f:
            return await f.read()

    async def exists(self, uri: str) -> bool:
        path = uri.replace("file://", "", 1)
        return await asyncio.to_thread(Path(path).exists)

    async def iterdir(self, uri: str) -> list[str]:
        path = uri.replace("file://", "", 1)
        return await asyncio.to_thread(lambda: [str(p) for p in Path(path).iterdir()])

    async def unlink(self, uri: str) -> None:
        path = uri.replace("file://", "", 1)
        await asyncio.to_thread(Path(path).unlink)


class PathlibLocalAdapter:
    """Adapter for pathlib (sync baseline for local)."""

    name = "pathlib"

    async def setup(self) -> None:
        pass

    async def teardown(self) -> None:
        pass

    async def write(self, uri: str, data: bytes) -> None:
        path = uri.replace("file://", "", 1)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(Path(path).write_bytes, data)

    async def read(self, uri: str) -> bytes:
        path = uri.replace("file://", "", 1)
        return await asyncio.to_thread(Path(path).read_bytes)

    async def exists(self, uri: str) -> bool:
        path = uri.replace("file://", "", 1)
        return await asyncio.to_thread(Path(path).exists)

    async def iterdir(self, uri: str) -> list[str]:
        path = uri.replace("file://", "", 1)
        return await asyncio.to_thread(lambda: [str(p) for p in Path(path).iterdir()])

    async def unlink(self, uri: str) -> None:
        path = uri.replace("file://", "", 1)
        await asyncio.to_thread(Path(path).unlink)


class SyncPathLocalAdapter:
    """Adapter for asanypath SyncPath (sync local, no thread overhead)."""

    name = "syncpath"

    async def setup(self) -> None:
        pass

    async def teardown(self) -> None:
        pass

    async def write(self, uri: str, data: bytes) -> None:
        from asanypath import SyncPath

        path = uri.replace("file://", "", 1)
        SyncPath(path).parent.mkdir(parents=True, exist_ok=True)
        SyncPath(path).write_bytes(data)

    async def read(self, uri: str) -> bytes:
        from asanypath import SyncPath

        path = uri.replace("file://", "", 1)
        return SyncPath(path).read_bytes()

    async def exists(self, uri: str) -> bool:
        from asanypath import SyncPath

        path = uri.replace("file://", "", 1)
        return SyncPath(path).exists()

    async def iterdir(self, uri: str) -> list[str]:
        from asanypath import SyncPath

        path = uri.replace("file://", "", 1)
        return [str(p) for p in SyncPath(path).iterdir()]

    async def unlink(self, uri: str) -> None:
        from asanypath import SyncPath

        path = uri.replace("file://", "", 1)
        SyncPath(path).unlink()


class RequestsArtifactoryAdapter:
    """Adapter for requests against Artifactory REST API (sync baseline)."""

    name = "requests-art"

    def __init__(self):
        import requests

        self._session = requests.Session()
        token = os.getenv("ARTIFACTORY_IDENTITY_TOKEN", "")
        self._session.headers["Authorization"] = f"Bearer {token}"

    async def setup(self) -> None:
        pass

    async def teardown(self) -> None:
        self._session.close()

    def _to_url(self, uri: str) -> str:
        return uri.replace("art://", "https://", 1)

    def _to_storage_url(self, uri: str) -> str:
        url = self._to_url(uri)
        return url.replace("/artifactory/", "/artifactory/api/storage/", 1)

    async def write(self, uri: str, data: bytes) -> None:
        def _put():
            r = self._session.put(self._to_url(uri), data=data)
            r.raise_for_status()

        await asyncio.to_thread(_put)

    async def read(self, uri: str) -> bytes:
        def _get():
            r = self._session.get(self._to_url(uri))
            r.raise_for_status()
            return r.content

        return await asyncio.to_thread(_get)

    async def exists(self, uri: str) -> bool:
        def _head():
            r = self._session.head(self._to_url(uri))
            return r.status_code == 200

        return await asyncio.to_thread(_head)

    async def iterdir(self, uri: str) -> list[str]:
        def _list():
            r = self._session.get(self._to_storage_url(uri))
            r.raise_for_status()
            info = r.json()
            base = uri.rstrip("/")
            return [f"{base}/{c['uri'].lstrip('/')}" for c in info.get("children", [])]

        return await asyncio.to_thread(_list)

    async def unlink(self, uri: str) -> None:
        def _del():
            r = self._session.delete(self._to_url(uri))
            r.raise_for_status()

        await asyncio.to_thread(_del)


class AiohttpArtifactoryAdapter:
    """Adapter for raw aiohttp against Artifactory REST API (async baseline)."""

    name = "aiohttp-art"

    def __init__(self):
        self._session = None
        self._token = os.getenv("ARTIFACTORY_IDENTITY_TOKEN", "")

    async def setup(self) -> None:
        import aiohttp

        self._session = aiohttp.ClientSession(headers={"Authorization": f"Bearer {self._token}"})

    async def teardown(self) -> None:
        if self._session:
            await self._session.close()

    def _to_url(self, uri: str) -> str:
        return uri.replace("art://", "https://", 1)

    def _to_storage_url(self, uri: str) -> str:
        url = self._to_url(uri)
        return url.replace("/artifactory/", "/artifactory/api/storage/", 1)

    async def write(self, uri: str, data: bytes) -> None:
        async with self._session.put(self._to_url(uri), data=data) as resp:
            resp.raise_for_status()

    async def read(self, uri: str) -> bytes:
        async with self._session.get(self._to_url(uri)) as resp:
            resp.raise_for_status()
            return await resp.read()

    async def exists(self, uri: str) -> bool:
        async with self._session.head(self._to_url(uri)) as resp:
            return resp.status == 200

    async def iterdir(self, uri: str) -> list[str]:
        url = self._to_storage_url(uri)
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            info = await resp.json()
        base = uri.rstrip("/")
        return [f"{base}/{c['uri'].lstrip('/')}" for c in info.get("children", [])]

    async def unlink(self, uri: str) -> None:
        async with self._session.delete(self._to_url(uri)) as resp:
            resp.raise_for_status()


class AsyncsshSSHAdapter:
    """Baseline: raw asyncssh SFTP (the library asanypath wraps)."""

    name = "asyncssh"

    def __init__(self):
        import asyncssh  # noqa: F401

        self._conn = None
        self._sftp = None
        self._made: set[str] = set()

    async def setup(self) -> None:
        import asyncssh

        self._conn = await asyncssh.connect(
            os.getenv("BENCH_SSH_HOST", "localhost"),
            port=int(os.getenv("BENCH_SSH_PORT", "2222")),
            username=os.getenv("BENCH_SSH_USER", "bench"),
            password=os.getenv("BENCH_SSH_PASSWORD", "bench"),
            known_hosts=None,
        )
        self._sftp = await self._conn.start_sftp_client()

    async def teardown(self) -> None:
        if self._sftp:
            self._sftp.exit()
        if self._conn:
            self._conn.close()

    @staticmethod
    def _path(uri: str) -> str:
        from urllib.parse import urlsplit

        return urlsplit(uri).path

    async def write(self, uri: str, data: bytes) -> None:
        import posixpath

        path = self._path(uri)
        parent = posixpath.dirname(path)
        if parent and parent not in self._made:
            await self._sftp.makedirs(parent, exist_ok=True)
            self._made.add(parent)
        async with self._sftp.open(path, "wb") as f:
            await f.write(data)

    async def read(self, uri: str) -> bytes:
        async with self._sftp.open(self._path(uri), "rb") as f:
            return await f.read()

    async def exists(self, uri: str) -> bool:
        return await self._sftp.exists(self._path(uri))

    async def iterdir(self, uri: str) -> list[str]:
        base = self._path(uri).rstrip("/")
        return [f"{base}/{n}" for n in await self._sftp.listdir(base) if n not in (".", "..")]

    async def unlink(self, uri: str) -> None:
        await self._sftp.remove(self._path(uri))


class AioftpFTPAdapter:
    """Baseline: raw aioftp (the library asanypath wraps)."""

    name = "aioftp"

    def __init__(self):
        import aioftp  # noqa: F401

        self._client = None
        self._made: set[str] = set()

    async def setup(self) -> None:
        import aioftp

        self._client = aioftp.Client()
        await self._client.connect(
            os.getenv("BENCH_FTP_HOST", "localhost"),
            int(os.getenv("BENCH_FTP_PORT", "2121")),
        )
        await self._client.login(
            os.getenv("BENCH_FTP_USER", "bench"),
            os.getenv("BENCH_FTP_PASSWORD", "bench"),
        )

    async def teardown(self) -> None:
        if self._client:
            await self._client.quit()

    @staticmethod
    def _path(uri: str) -> str:
        from urllib.parse import urlsplit

        return urlsplit(uri).path

    async def write(self, uri: str, data: bytes) -> None:
        path = self._path(uri)
        parent = path.rsplit("/", 1)[0]
        if parent and parent not in self._made:
            await self._client.make_directory(parent)
            self._made.add(parent)
        async with self._client.upload_stream(path) as stream:
            await stream.write(data)

    async def read(self, uri: str) -> bytes:
        chunks: list[bytes] = []
        async with self._client.download_stream(self._path(uri)) as stream:
            async for block in stream.iter_by_block():
                chunks.append(block)
        return b"".join(chunks)

    async def exists(self, uri: str) -> bool:
        return await self._client.exists(self._path(uri))

    async def iterdir(self, uri: str) -> list[str]:
        base = self._path(uri).rstrip("/")
        return [str(p) async for p, _ in self._client.list(base)]

    async def unlink(self, uri: str) -> None:
        await self._client.remove_file(self._path(uri))


# ---------------------------------------------------------------------------
# Backend registry
# ---------------------------------------------------------------------------


def _try_load(adapter_cls, label: str):
    """Try to instantiate an adapter; return None with a warning if deps missing."""
    try:
        return adapter_cls() if not adapter_cls.__init__.__code__.co_varnames[1:] else adapter_cls
    except Exception:
        return adapter_cls


def _get_adapters(backend: str) -> list[tuple[str, list]]:
    """Return (backend_name, [adapters]) for the requested backend(s)."""
    backends = {}

    if backend in ("local", "all"):
        adapters = [AsAnyPathAdapter("local")]
        _try_add(adapters, PathlibLocalAdapter, "pathlib")
        _try_add(adapters, SyncPathLocalAdapter, "syncpath")
        _try_add(adapters, NativeLocalAdapter, "aiofiles")
        _try_add(adapters, FsspecLocalAdapter, "fsspec")
        backends["local"] = adapters

    if backend in ("s3", "all"):
        adapters = [AsAnyPathAdapter("s3")]
        _try_add(adapters, FsspecS3Adapter, "s3fs")
        _try_add(adapters, CloudpathlibS3Adapter, "cloudpathlib[s3]")
        _try_add(adapters, NativeS3Adapter, "aiobotocore")
        backends["s3"] = adapters

    if backend in ("az", "azure", "all"):
        adapters = [AsAnyPathAdapter("azure")]
        _try_add(adapters, FsspecAzureAdapter, "adlfs")
        _try_add(adapters, CloudpathlibAzureAdapter, "cloudpathlib[azure]")
        _try_add(adapters, NativeAzureAdapter, "azure-storage-blob")
        backends["azure"] = adapters

    if backend in ("gs", "gcs", "all"):
        adapters = [AsAnyPathAdapter("gcs")]
        _try_add(adapters, FsspecGCSAdapter, "gcsfs")
        _try_add(adapters, CloudpathlibGCSAdapter, "cloudpathlib[gcs]")
        _try_add(adapters, NativeGCSAdapter, "gcloud-aio-storage")
        backends["gcs"] = adapters

    if backend in ("art", "artifactory", "all"):
        adapters = [AsAnyPathAdapter("art")]
        _try_add(adapters, RequestsArtifactoryAdapter, "requests")
        _try_add(adapters, AiohttpArtifactoryAdapter, "aiohttp")
        backends["artifactory"] = adapters

    if backend == "ssh":
        adapters = [
            AsAnyPathAdapter(
                "ssh", password=os.getenv("BENCH_SSH_PASSWORD", "bench"), known_hosts=None
            )
        ]
        _try_add(adapters, AsyncsshSSHAdapter, "asyncssh")
        backends["ssh"] = adapters

    if backend in ("ftp", "all"):
        adapters = [AsAnyPathAdapter("ftp")]
        _try_add(adapters, AioftpFTPAdapter, "aioftp")
        backends["ftp"] = adapters

    if not backends:
        raise ValueError(
            f"Unknown backend: {backend}. Use: local, s3, az, gs, art, ssh, ftp, or all"
        )

    return list(backends.items())


def _try_add(adapters: list, cls, label: str) -> None:
    """Instantiate adapter; skip with warning if import fails."""
    try:
        adapters.append(cls())
    except ImportError as e:
        log.info("skip %s: %s", label, e)
    except Exception as e:
        log.info("skip %s: %s", label, e)


# ---------------------------------------------------------------------------
# URI generators per backend
# ---------------------------------------------------------------------------


def _make_uris(backend: str, base: str, count: int, prefix: str = "") -> list[str]:
    """Generate URIs for a backend."""
    match backend:
        case "local":
            return [f"file://{base}/{prefix}file-{i:04d}.bin" for i in range(count)]
        case "s3":
            bucket = os.getenv("BENCH_S3_BUCKET", "bench-asanypath")
            return [f"s3://{bucket}/{base}/{prefix}file-{i:04d}.bin" for i in range(count)]
        case "azure":
            container = os.getenv("BENCH_AZ_CONTAINER", "benchcontainer")
            return [f"az://{container}/{base}/{prefix}file-{i:04d}.bin" for i in range(count)]
        case "gcs":
            bucket = os.getenv("BENCH_GCS_BUCKET", "bench-asanypath")
            return [f"gs://{bucket}/{base}/{prefix}file-{i:04d}.bin" for i in range(count)]
        case "artifactory":
            art_base = _art_base_url()
            repo = os.getenv("BENCH_ART_REPO", "bench-generic")
            return [
                f"art://{art_base}/{repo}/{base}/{prefix}file-{i:04d}.bin" for i in range(count)
            ]
        case "ssh":
            host = os.getenv("BENCH_SSH_HOST", "localhost")
            port = os.getenv("BENCH_SSH_PORT", "2222")
            user = os.getenv("BENCH_SSH_USER", "bench")
            root = os.getenv("BENCH_SSH_ROOT", "/upload")
            return [
                f"ssh://{user}@{host}:{port}{root}/{base}/{prefix}file-{i:04d}.bin"
                for i in range(count)
            ]
        case "ftp":
            host = os.getenv("BENCH_FTP_HOST", "localhost")
            port = os.getenv("BENCH_FTP_PORT", "2121")
            user = os.getenv("BENCH_FTP_USER", "bench")
            pw = os.getenv("BENCH_FTP_PASSWORD", "bench")
            root = os.getenv("BENCH_FTP_ROOT", "/ftp/bench")
            return [
                f"ftp://{user}:{pw}@{host}:{port}{root}/{base}/{prefix}file-{i:04d}.bin"
                for i in range(count)
            ]
    raise ValueError(f"Unknown backend: {backend}")


def _art_base_url() -> str:
    """Get Artifactory base URL, stripping any protocol prefix."""
    raw = os.getenv("BENCH_ART_BASE_URL", "artifactory.example.com/artifactory")
    for prefix in ("https://", "http://"):
        if raw.startswith(prefix):
            raw = raw[len(prefix) :]
    return raw.rstrip("/")


def _dir_uri(backend: str, base: str, prefix: str = "") -> str:
    """Get directory URI for iterdir."""
    match backend:
        case "local":
            return f"file://{base}/{prefix}".rstrip("/")
        case "s3":
            bucket = os.getenv("BENCH_S3_BUCKET", "bench-asanypath")
            return f"s3://{bucket}/{base}/{prefix}".rstrip("/")
        case "azure":
            container = os.getenv("BENCH_AZ_CONTAINER", "benchcontainer")
            return f"az://{container}/{base}/{prefix}".rstrip("/")
        case "gcs":
            bucket = os.getenv("BENCH_GCS_BUCKET", "bench-asanypath")
            return f"gs://{bucket}/{base}/{prefix}".rstrip("/")
        case "artifactory":
            art_base = _art_base_url()
            repo = os.getenv("BENCH_ART_REPO", "bench-generic")
            return f"art://{art_base}/{repo}/{base}/{prefix}".rstrip("/")
        case "ssh":
            host = os.getenv("BENCH_SSH_HOST", "localhost")
            port = os.getenv("BENCH_SSH_PORT", "2222")
            user = os.getenv("BENCH_SSH_USER", "bench")
            root = os.getenv("BENCH_SSH_ROOT", "/upload")
            return f"ssh://{user}@{host}:{port}{root}/{base}/{prefix}".rstrip("/")
        case "ftp":
            host = os.getenv("BENCH_FTP_HOST", "localhost")
            port = os.getenv("BENCH_FTP_PORT", "2121")
            user = os.getenv("BENCH_FTP_USER", "bench")
            pw = os.getenv("BENCH_FTP_PASSWORD", "bench")
            root = os.getenv("BENCH_FTP_ROOT", "/ftp/bench")
            return f"ftp://{user}:{pw}@{host}:{port}{root}/{base}/{prefix}".rstrip("/")
    raise ValueError(f"Unknown backend: {backend}")


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------


async def _warmup(adapter, backend: str, base: str) -> None:
    """Single write+read to warm up connections."""
    uris = _make_uris(backend, base, 1, prefix="warmup/")
    try:
        await adapter.write(uris[0], b"warmup")
        await adapter.read(uris[0])
        await adapter.unlink(uris[0])
    except Exception:
        pass  # Best-effort warmup


async def _bench_op(
    op_name: str,
    adapter,
    coro_factory,
    *,
    rounds: int,
    concurrency: int,
) -> list[float]:
    """Run an operation multiple rounds, return wall-time ms per round."""
    samples = []
    for _ in range(rounds):
        # Clear listing caches before each iterdir round for fair comparison
        if op_name == "iterdir" and hasattr(adapter, "clear_cache"):
            adapter.clear_cache()
        coros = [coro_factory() for _ in range(concurrency)]
        t0 = time.perf_counter()
        await asyncio.gather(*coros)
        wall_ms = (time.perf_counter() - t0) * 1000.0
        samples.append(wall_ms / concurrency)
    return samples


async def _bench_cp(
    adapter,
    backend: str,
    base: str,
    *,
    rounds: int,
    size_bytes: int,
    concurrency: int,
) -> list[BenchResult]:
    """Benchmark asanypath copy: native server-side vs client read+write.

    Copies a seeded source object to fresh destinations, timing the native
    server-side copy path against the fallback that streams bytes through the
    client (forced by disabling the backend's native copy function).
    """
    import itertools

    from asanypath import AsAnyPath

    payload = os.urandom(size_bytes)
    src_uri = _make_uris(backend, base, 1, prefix=f"cp-src-{adapter.name}/")[0]
    await adapter.write(src_uri, payload)
    src = AsAnyPath(src_uri)
    cls = type(src)
    native_fn = cls._copy_batch_fn
    if native_fn is None:
        return []  # backend has no native copy — nothing to compare

    counter = itertools.count()

    def _copy_to_fresh_dst():
        i = next(counter)
        dst_uri = _make_uris(backend, base, 1, prefix=f"cp-dst-{adapter.name}/{i:06d}-")[0]
        return src.copy(AsAnyPath(dst_uri), force=True)

    # Native server-side copy.
    cls._copy_batch_fn = native_fn
    native = await _bench_op(
        "cp", adapter, _copy_to_fresh_dst, rounds=rounds, concurrency=concurrency
    )
    # Client read+write fallback (native disabled).
    cls._copy_batch_fn = None
    try:
        rw = await _bench_op(
            "cp", adapter, _copy_to_fresh_dst, rounds=rounds, concurrency=concurrency
        )
    finally:
        cls._copy_batch_fn = native_fn

    return [
        _summarize(
            "cp", "asanypath", backend, native, size_bytes=size_bytes, concurrency=concurrency
        ),
        _summarize(
            "cp", "asanypath-rw", backend, rw, size_bytes=size_bytes, concurrency=concurrency
        ),
    ]


async def _run_backend(
    backend: str,
    adapters: list,
    *,
    rounds: int,
    size_bytes: int,
    concurrency: int,
    objects: int,
) -> list[BenchResult]:
    """Run all benchmarks for a single backend."""
    run_id = secrets.token_hex(4)
    results = []

    # Setup base path
    if backend == "local":
        base_dir = tempfile.mkdtemp(prefix="bench_asanypath_")
        base = base_dir
    else:
        base = f"asanypath-bench/{run_id}"

    payload = os.urandom(size_bytes)

    for adapter in adapters:
        try:
            await adapter.setup()
        except Exception as e:
            log.warning("skip %s: setup failed: %s", adapter.name, e)
            continue

        try:
            await _warmup(adapter, backend, base)

            # --- write ---
            write_uris = _make_uris(backend, base, concurrency, prefix=f"write-{adapter.name}/")
            samples = await _bench_op(
                "write",
                adapter,
                lambda uris=write_uris: adapter.write(uris[secrets.randbelow(len(uris))], payload),
                rounds=rounds,
                concurrency=concurrency,
            )
            results.append(
                _summarize(
                    "write",
                    adapter.name,
                    backend,
                    samples,
                    size_bytes=size_bytes,
                    concurrency=concurrency,
                )
            )

            # Seed files for read/exists/iterdir
            seed_uris = _make_uris(backend, base, objects, prefix=f"seed-{adapter.name}/")
            for uri in seed_uris:
                await adapter.write(uri, payload)

            # --- read ---
            samples = await _bench_op(
                "read",
                adapter,
                lambda: adapter.read(seed_uris[secrets.randbelow(len(seed_uris))]),
                rounds=rounds,
                concurrency=concurrency,
            )
            results.append(
                _summarize(
                    "read",
                    adapter.name,
                    backend,
                    samples,
                    size_bytes=size_bytes,
                    concurrency=concurrency,
                )
            )

            # --- exists ---
            samples = await _bench_op(
                "exists",
                adapter,
                lambda: adapter.exists(seed_uris[secrets.randbelow(len(seed_uris))]),
                rounds=rounds,
                concurrency=concurrency,
            )
            results.append(
                _summarize(
                    "exists",
                    adapter.name,
                    backend,
                    samples,
                    size_bytes=size_bytes,
                    concurrency=concurrency,
                )
            )

            # --- iterdir ---
            dir_uri = _dir_uri(backend, base, prefix=f"seed-{adapter.name}/")
            samples = await _bench_op(
                "iterdir",
                adapter,
                lambda: adapter.iterdir(dir_uri),
                rounds=rounds,
                concurrency=concurrency,
            )
            results.append(
                _summarize(
                    "iterdir",
                    adapter.name,
                    backend,
                    samples,
                    size_bytes=size_bytes,
                    concurrency=concurrency,
                )
            )

            # --- unlink ---
            del_uris = _make_uris(backend, base, concurrency, prefix=f"del-{adapter.name}/")
            for uri in del_uris:
                await adapter.write(uri, payload)
            samples = await _bench_op(
                "unlink",
                adapter,
                lambda uris=list(del_uris): (
                    adapter.unlink(uris.pop()) if uris else asyncio.sleep(0)
                ),
                rounds=rounds,
                concurrency=concurrency,
            )
            results.append(
                _summarize(
                    "unlink",
                    adapter.name,
                    backend,
                    samples,
                    size_bytes=size_bytes,
                    concurrency=concurrency,
                )
            )

        except Exception as e:
            log.warning("%s failed: %s", adapter.name, e)
            print(f"  [skip] {adapter.name} (see bench.log)")
        finally:
            await adapter.teardown()

    # cp: native server-side copy vs client read+write (asanypath, cloud only).
    if backend in ("s3", "azure", "gcs", "artifactory"):
        aap = next((a for a in adapters if a.name == "asanypath"), None)
        if aap is not None:
            try:
                results.extend(
                    await _bench_cp(
                        aap,
                        backend,
                        base,
                        rounds=rounds,
                        size_bytes=size_bytes,
                        concurrency=concurrency,
                    )
                )
            except Exception as e:
                log.warning("cp bench failed: %s", e)
                print("  [skip] cp (see bench.log)")

    # Cleanup
    if backend == "local":
        shutil.rmtree(base_dir, ignore_errors=True)

    # Fatal checks
    succeeded = {r.impl for r in results}
    if "asanypath" not in succeeded:
        msg = f"FATAL: asanypath failed on backend '{backend}'. Check bench.log for details."
        print(f"\n{msg}", flush=True)
        raise SystemExit(1)
    others = succeeded - {"asanypath"}
    if not others:
        msg = (
            f"FATAL: all comparison adapters failed on backend '{backend}'. "
            f"Check bench.log for details."
        )
        print(f"\n{msg}", flush=True)
        raise SystemExit(1)

    return results


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _print_results(backend: str, results: list[BenchResult]) -> None:
    """Print results table for a backend."""
    if not results:
        print("  (no adapters succeeded)\n")
        return

    ops = ["write", "read", "exists", "iterdir", "unlink", "cp"]

    header = (
        f"{'op':<8} {'impl':<14} {'n':>3} {'mean(ms)':>10} "
        f"{'median':>10} {'min':>10} {'max':>10} {'ops/s':>10}"
    )
    print(header)
    print("-" * len(header))

    for op in ops:
        op_results = [r for r in results if r.op == op]
        for r in sorted(op_results, key=lambda x: x.mean_ms):
            print(
                f"{r.op:<8} {r.impl:<14} {r.rounds:>3} "
                f"{r.mean_ms:>10.2f} {r.median_ms:>10.2f} {r.min_ms:>10.2f} "
                f"{r.max_ms:>10.2f} {r.mean_ops_s:>10.1f}"
            )
        print()

    # Speedup table
    asanypath_results = {r.op: r for r in results if r.impl == "asanypath"}
    if asanypath_results:
        print("Comparison vs asanypath (ratio = other / asanypath):")
        for op in ops:
            if op not in asanypath_results:
                continue
            base_ms = asanypath_results[op].mean_ms
            op_results = [r for r in results if r.op == op and r.impl != "asanypath"]
            for r in sorted(op_results, key=lambda x: x.mean_ms):
                ratio = r.mean_ms / base_ms if base_ms else float("inf")
                if ratio < 1:
                    print(f"  {op:<8} {r.impl:<14} {ratio:>6.2f}x (other is faster)")
                else:
                    print(f"  {op:<8} {r.impl:<14} {ratio:>6.2f}x (asanypath is faster)")
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _parse_int_list(value: str | None, default: int) -> list[int]:
    if not value:
        return [default]
    result = [int(v.strip()) for v in value.split(",") if v.strip()]
    return result or [default]


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark asanypath vs fsspec, cloudpathlib, and native SDKs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--backend",
        default="local",
        help="Backend to benchmark: local, s3, az, gs, art, ssh, ftp, or all (default: local)",
    )
    parser.add_argument("--rounds", type=int, default=5, help="Rounds per operation (default: 5)")
    parser.add_argument(
        "--size-bytes", type=int, default=4096, help="Payload size in bytes (default: 4096)"
    )
    parser.add_argument(
        "--size-sweep", default="", help="Comma-separated sizes, e.g. 1024,65536,1048576"
    )
    parser.add_argument(
        "--concurrency", type=int, default=1, help="Parallel ops per round (default: 1)"
    )
    parser.add_argument(
        "--concurrency-levels", default="", help="Comma-separated levels, e.g. 1,4,8"
    )
    parser.add_argument(
        "--objects", type=int, default=50, help="Objects to seed for iterdir (default: 50)"
    )
    parser.add_argument(
        "--log", default="bench.log", help="Log file for errors/warnings (default: bench.log)"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Also print warnings to stderr",
    )
    args = parser.parse_args()

    # Configure logging: errors/warnings go to log file, stdout stays clean
    log.setLevel(logging.DEBUG)
    fh = logging.FileHandler(args.log, mode="w")
    fh.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    log.addHandler(fh)
    if args.verbose:
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(logging.WARNING)
        sh.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        log.addHandler(sh)

    # Redirect stderr to log file to suppress noisy third-party prints
    log_fp = open(args.log, "a")  # noqa: SIM115
    _real_stderr = sys.stderr
    sys.stderr = log_fp

    size_values = _parse_int_list(args.size_sweep, args.size_bytes)
    concurrency_values = _parse_int_list(args.concurrency_levels, args.concurrency)

    backend_list = _get_adapters(args.backend)

    for backend_name, adapters in backend_list:
        for size_bytes in size_values:
            for concurrency in concurrency_values:
                print(
                    f"\n{'=' * 70}\n"
                    f"Backend: {backend_name} | size: {size_bytes}B | "
                    f"concurrency: {concurrency} | rounds: {args.rounds} | "
                    f"objects: {args.objects}\n"
                    f"{'=' * 70}"
                )

                results = await _run_backend(
                    backend_name,
                    adapters,
                    rounds=args.rounds,
                    size_bytes=size_bytes,
                    concurrency=concurrency,
                    objects=args.objects,
                )

                _print_results(backend_name, results)

    sys.stderr = _real_stderr
    log_fp.close()
    if Path(args.log).stat().st_size > 0:
        print(f"\nDetails in {args.log}")


if __name__ == "__main__":
    asyncio.run(main())
