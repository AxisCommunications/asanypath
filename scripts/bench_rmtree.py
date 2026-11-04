# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Time recursive deletes (``rmdir(recursive=True)``) of a generated tree.

Each round uploads ``dirs x subdirs x files`` small objects under a fresh,
unique prefix below DESTINATION (untimed), deletes that prefix recursively
(timed), and checks that nothing is left. Uses the backend's normal
credential and endpoint resolution, e.g. for the emulators in compose.yml:

    AWS_ENDPOINT_URL=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin \\
    AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \\
        uv run python scripts/bench_rmtree.py s3://bench-asanypath/rmtree-bench
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from uuid import uuid4

from asanypath import AsAnyPath


async def build(root, dirs: int, subdirs: int, files: int, payload: bytes, workers: int) -> int:
    limit = asyncio.Semaphore(workers)

    async def put(path) -> None:
        async with limit:
            await path.write_bytes(payload)

    paths = [
        root / f"d{d:03d}" / f"s{s:03d}" / f"f{f:04d}.bin"
        for d in range(dirs)
        for s in range(subdirs)
        for f in range(files)
    ]
    await asyncio.gather(*(put(p) for p in paths))
    return len(paths)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("destination", help="Prefix to create test trees under")
    parser.add_argument("--dirs", type=int, default=10, help="Top-level directories")
    parser.add_argument("--subdirs", type=int, default=5, help="Subdirectories per directory")
    parser.add_argument("--files", type=int, default=20, help="Files per subdirectory")
    parser.add_argument("--size", type=int, default=16, help="Bytes per file")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--workers", type=int, default=64, help="Concurrent uploads in setup")
    args = parser.parse_args()

    base = AsAnyPath(args.destination)
    payload = b"\0" * args.size
    times = []
    for round_no in range(1, args.rounds + 1):
        root = base / f"run-{uuid4().hex[:12]}"
        count = await build(root, args.dirs, args.subdirs, args.files, payload, args.workers)
        started = time.perf_counter()
        await root.rmdir(recursive=True)
        elapsed = time.perf_counter() - started
        if await root.exists():
            raise SystemExit(f"{root} still exists after rmdir(recursive=True)")
        times.append(elapsed)
        print(f"round {round_no}: {count} objects in {elapsed:.3f}s ({count / elapsed:.0f}/s)")
    print(
        f"rmtree {args.dirs}x{args.subdirs}x{args.files}: median {statistics.median(times):.3f}s"
        f" min {min(times):.3f}s max {max(times):.3f}s"
    )


if __name__ == "__main__":
    asyncio.run(main())
