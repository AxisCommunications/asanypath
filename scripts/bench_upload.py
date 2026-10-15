# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Compare bounded multipart uploads of real files to an explicitly chosen S3 prefix.

Uses normal AWS credential resolution; never reads repository credential files.
Each case warms up once, uploads to unique keys, verifies contents outside the
timed interval, and deletes only its own objects. Timings include public copy API
overhead for asanypath and upload_file API overhead for boto3.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import statistics
import time
from collections.abc import Awaitable, Iterable
from pathlib import Path
from uuid import uuid4

from asanypath import AsAnyPath, TransferBudget


async def finish_uploads(uploads: Iterable[Awaitable[object]]) -> None:
    pending = asyncio.gather(*uploads, return_exceptions=True)
    try:
        results = await asyncio.shield(pending)
    except asyncio.CancelledError:
        await pending
        raise
    for result in results:
        if isinstance(result, BaseException):
            raise result


def positive(value: str) -> int:
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def digest(path: Path) -> bytes:
    checksum = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(8 * 1024 * 1024):
            checksum.update(chunk)
    return checksum.digest()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help="Representative local shard files")
    parser.add_argument("--destination", required=True, help="s3://bucket/benchmark-prefix")
    parser.add_argument("--endpoint-url", help="The storage endpoint used by both clients")
    parser.add_argument("--profile", help="AWS profile used by both clients")
    parser.add_argument("--streams", nargs="+", type=positive, default=[1, 8])
    parser.add_argument("--part-mib", type=positive, default=8)
    parser.add_argument("--rounds", type=positive, default=3)
    parser.add_argument(
        "--implementations",
        nargs="+",
        choices=["asanypath", "boto3"],
        default=["asanypath", "boto3"],
    )
    args = parser.parse_args()
    if not args.destination.startswith("s3://") or not args.destination[5:].split("/")[0]:
        parser.error("--destination must be an s3://bucket/prefix URI")
    if args.part_mib < 5:
        parser.error("--part-mib must be at least 5 for S3")
    for file in args.files:
        if not file.is_file():
            parser.error(f"Not a file: {file}")
    kwargs = {}
    if args.endpoint_url:
        kwargs["endpoint_url"] = args.endpoint_url
    if args.profile:
        kwargs["profile"] = args.profile
    part_size = args.part_mib * 1024 * 1024
    total = sum(file.stat().st_size for file in args.files)
    expected = [digest(file) for file in args.files]
    base = args.destination.rstrip("/") + "/asanypath-upload-bench-" + uuid4().hex
    bucket = base[5:].split("/", 1)[0]

    for streams in args.streams:
        for implementation in args.implementations:
            client = transfer = None
            if implementation == "boto3":
                import boto3
                from boto3.s3.transfer import S3Transfer, TransferConfig
                from botocore.config import Config

                client = boto3.Session(profile_name=args.profile).client(
                    "s3",
                    endpoint_url=args.endpoint_url,
                    config=Config(max_pool_connections=max(10, streams)),
                )
                # One manager shares its request budget across all files.
                transfer = S3Transfer(
                    client,
                    TransferConfig(
                        multipart_threshold=part_size,
                        multipart_chunksize=part_size,
                        max_concurrency=streams,
                        preferred_transfer_client="classic",
                    ),
                )
            samples = []
            try:
                for round_number in range(args.rounds + 1):
                    uris = [
                        f"{base}/{implementation}/{streams}/{round_number}/{index}"
                        for index in range(len(args.files))
                    ]
                    destinations = [AsAnyPath(uri, **kwargs) for uri in uris]
                    try:
                        start = time.perf_counter()
                        if implementation == "asanypath":
                            with TransferBudget(
                                floor=streams, ceiling=streams, per_file_cap=streams
                            ):
                                await finish_uploads(
                                    (
                                        AsAnyPath(file).copy(dst, chunk_size=part_size)
                                        for file, dst in zip(args.files, destinations, strict=True)
                                    )
                                )
                        else:
                            await finish_uploads(
                                (
                                    asyncio.to_thread(
                                        transfer.upload_file,
                                        str(file),
                                        bucket,
                                        uri[5:].split("/", 1)[1],
                                    )
                                    for file, uri in zip(args.files, uris, strict=True)
                                )
                            )
                        elapsed = time.perf_counter() - start
                        for dst, checksum in zip(destinations, expected, strict=True):
                            actual = hashlib.sha256()
                            async for chunk in dst.iter_bytes(part_size):
                                actual.update(chunk)
                            if actual.digest() != checksum:
                                raise OSError(f"Uploaded content verification failed: {dst}")
                        if round_number:
                            samples.append(elapsed)
                    finally:
                        await asyncio.gather(*(dst.unlink(missing_ok=True) for dst in destinations))
                median = statistics.median(samples)
                print(
                    json.dumps(
                        {
                            "implementation": implementation,
                            "streams": streams,
                            "files": len(args.files),
                            "bytes": total,
                            "part_bytes": part_size,
                            "seconds": samples,
                            "median_seconds": median,
                            "median_mib_per_second": total / (1024 * 1024) / median,
                        }
                    ),
                    flush=True,
                )
            finally:
                if transfer is not None:
                    await asyncio.to_thread(transfer.__exit__, None, None, None)
                if client is not None:
                    client.close()


if __name__ == "__main__":
    asyncio.run(main())
