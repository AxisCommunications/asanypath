# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import asyncio
import threading
from io import BytesIO
from unittest.mock import AsyncMock, patch

import pytest

from asanypath import TransferBudget
from asanypath._transfer import iter_transfer_chunks, upload_parts
from tests.test_azure import _make_azure_path
from tests.test_gcs import _make_gcs_path
from tests.test_s3 import _make_s3path


@pytest.mark.parametrize("floor, cap, concurrency", [(1, 8, 1), (3, 8, 3), (8, 2, 2)])
async def test_upload_overlap_and_buffer_backpressure(floor, cap, concurrency):
    data = bytes(range(40))
    file = BytesIO(data)
    full = asyncio.Event()
    release = asyncio.Event()
    active = 0
    peak = 0
    received = {}

    async def upload(number, chunk):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        received[number] = chunk
        if active == concurrency:
            full.set()
        try:
            await release.wait()
            return number
        finally:
            active -= 1

    with TransferBudget(floor=floor, ceiling=floor, per_file_cap=cap) as budget:
        task = asyncio.create_task(upload_parts(file, len(data), 4, upload))
        try:
            await asyncio.wait_for(full.wait(), 2)
            assert file.tell() == 4 * concurrency
            assert peak == concurrency
        finally:
            release.set()
        assert await asyncio.wait_for(task, 2) == list(range(1, 11))
        assert budget.active_files == 0
    assert b"".join(received[number] for number in sorted(received)) == data


async def test_upload_refills_without_waiting_for_slowest_part():
    first_started = asyncio.Event()
    later_finished = asyncio.Event()
    release = asyncio.Event()

    async def upload(number, chunk):
        if number == 1:
            first_started.set()
            await release.wait()
        elif number == 8:
            later_finished.set()
        return number

    with TransferBudget(floor=2, ceiling=2):
        task = asyncio.create_task(upload_parts(BytesIO(b"x" * 32), 32, 4, upload))
        try:
            await asyncio.wait_for(first_started.wait(), 2)
            await asyncio.wait_for(later_finished.wait(), 2)
            assert not task.done()
        finally:
            release.set()
        assert await asyncio.wait_for(task, 2) == list(range(1, 9))


async def test_many_uploads_share_global_buffer_ceiling():
    full = asyncio.Event()
    release = asyncio.Event()
    files = [BytesIO(b"x" * 40) for _ in range(6)]
    active = 0
    peak = 0

    async def upload(number, chunk):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 3:
            full.set()
        try:
            await release.wait()
        finally:
            active -= 1

    with TransferBudget(floor=3, ceiling=3) as budget:
        tasks = [asyncio.create_task(upload_parts(file, 40, 4, upload)) for file in files]
        try:
            await asyncio.wait_for(full.wait(), 2)
            assert sum(file.tell() for file in files) == 12
        finally:
            release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 2)
        assert budget.active_files == 0
    assert peak == 3
    assert active == 0


async def test_range_reads_and_uploads_share_capacity():
    from types import SimpleNamespace

    active = 0
    peak = 0
    full = asyncio.Event()
    release = asyncio.Event()

    async def request():
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 4:
            full.set()
        try:
            await release.wait()
        finally:
            active -= 1

    async def upload(number, chunk):
        await request()

    async def read(start, end):
        await request()
        return b"x" * (end - start + 1)

    source = SimpleNamespace(
        _supports_range_read=True,
        stat=AsyncMock(return_value=SimpleNamespace(st_size=40)),
        _range_read=read,
    )

    async def download():
        return b"".join([chunk async for chunk in iter_transfer_chunks(source, chunk_size=4)])

    with TransferBudget(floor=4, ceiling=4, min_range_size=4) as budget:
        tasks = [
            asyncio.create_task(download()),
            asyncio.create_task(upload_parts(BytesIO(b"x" * 40), 40, 4, upload)),
        ]
        try:
            await asyncio.wait_for(full.wait(), 2)
            assert peak == 4
        finally:
            release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 2)
        assert budget.active_files == 0
    assert peak == 4


@pytest.mark.parametrize("failure", ["error", "cancel"])
async def test_upload_failure_settles_workers_and_releases_slots(failure):
    started = asyncio.Event()
    active = 0

    async def upload(number, chunk):
        nonlocal active
        active += 1
        if active == 2:
            started.set()
        try:
            if number == 1 and failure == "error":
                await started.wait()
                raise OSError("part failed")
            await asyncio.Event().wait()
        finally:
            active -= 1

    with TransferBudget(floor=2, ceiling=2) as budget:
        task = asyncio.create_task(upload_parts(BytesIO(b"x" * 40), 40, 4, upload))
        await asyncio.wait_for(started.wait(), 2)
        if failure == "cancel":
            task.cancel()
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else OSError):
            await asyncio.wait_for(task, 2)
        assert active == 0
        assert budget.active_files == 0
        assert await upload_parts(BytesIO(b"ok"), 2, 4, AsyncMock(return_value="ok")) == ["ok"]


async def test_cancel_during_disk_read_joins_thread_before_returning():
    started = threading.Event()
    release = threading.Event()
    read_finished = threading.Event()

    class SlowFile(BytesIO):
        def read(self, size=-1):
            started.set()
            assert release.wait(5)
            data = super().read(size)
            read_finished.set()
            return data

    file = SlowFile(b"data")
    upload = AsyncMock()
    task = asyncio.create_task(upload_parts(file, 4, 4, upload))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert read_finished.is_set()
    upload.assert_not_awaited()
    file.close()


async def test_upload_read_error_and_invalid_part_size():
    class BrokenFile(BytesIO):
        def read(self, size=-1):
            raise OSError("disk read failed")

    upload = AsyncMock()
    with pytest.raises(OSError, match="disk read failed"):
        await upload_parts(BrokenFile(), 4, 4, upload)
    with pytest.raises(ValueError, match="positive"):
        await upload_parts(BytesIO(), 0, 0, upload)
    upload.assert_not_awaited()


async def test_s3_completes_out_of_order_parts_in_source_order():
    path = _make_s3path()
    finished = []
    last = asyncio.Event()

    async def upload(*, part_number, data, **kwargs):
        if part_number == 1:
            await last.wait()
        if part_number == 3:
            last.set()
        finished.append(part_number)
        return f"etag-{part_number}"

    complete = AsyncMock()
    abort = AsyncMock()
    with (
        TransferBudget(floor=3),
        patch("asanypath.s3.s3_create_multipart", AsyncMock(return_value="upload-id")),
        patch("asanypath.s3.s3_upload_part", upload),
        patch("asanypath.s3.s3_complete_multipart", complete),
        patch("asanypath.s3.s3_abort_multipart", abort),
    ):
        await asyncio.wait_for(path._multipart_upload(BytesIO(b"abcdef"), 2, size=6), 2)
    assert finished.index(3) < finished.index(1)
    assert complete.await_args.kwargs["parts"] == [(1, "etag-1"), (2, "etag-2"), (3, "etag-3")]
    abort.assert_not_awaited()


@pytest.mark.parametrize("failure", ["part", "complete", "cancel"])
async def test_s3_abort_happens_after_workers_settle(failure):
    path = _make_s3path()
    active = 0
    started = asyncio.Event()
    complete = AsyncMock(side_effect=OSError("complete failed") if failure == "complete" else None)

    async def upload(*, part_number, **kwargs):
        nonlocal active
        active += 1
        if active == 2:
            started.set()
        try:
            if failure == "complete":
                return "etag"
            if failure == "part" and part_number == 1:
                await started.wait()
                raise OSError("part failed")
            await asyncio.Event().wait()
        finally:
            active -= 1

    async def abort(**kwargs):
        assert active == 0

    abort_mock = AsyncMock(side_effect=abort)
    with (
        TransferBudget(floor=2),
        patch("asanypath.s3.s3_create_multipart", AsyncMock(return_value="upload-id")),
        patch("asanypath.s3.s3_upload_part", upload),
        patch("asanypath.s3.s3_complete_multipart", complete),
        patch("asanypath.s3.s3_abort_multipart", abort_mock),
    ):
        task = asyncio.create_task(path._multipart_upload(BytesIO(b"abcdef"), 2, size=6))
        if failure == "cancel":
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else OSError):
            await asyncio.wait_for(task, 2)
    abort_mock.assert_awaited_once()
    if failure != "complete":
        complete.assert_not_awaited()


async def test_s3_cancel_during_initiation_still_aborts():
    path = _make_s3path()
    started = asyncio.Event()
    release = asyncio.Event()

    async def create(**kwargs):
        started.set()
        await release.wait()
        return "late-upload-id"

    abort = AsyncMock()
    upload = AsyncMock()
    with (
        patch("asanypath.s3.s3_create_multipart", create),
        patch("asanypath.s3.s3_upload_part", upload),
        patch("asanypath.s3.s3_abort_multipart", abort),
    ):
        task = asyncio.create_task(path._multipart_upload(BytesIO(b"abcdef"), 2, size=6))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
    assert abort.await_args.kwargs["upload_id"] == "late-upload-id"
    upload.assert_not_awaited()


async def test_s3_abort_failure_is_logged_without_masking_original(caplog):
    path = _make_s3path()
    with (
        patch("asanypath.s3.s3_create_multipart", AsyncMock(return_value="upload-id")),
        patch("asanypath.s3.s3_upload_part", AsyncMock(side_effect=OSError("part failed"))),
        patch("asanypath.s3.s3_abort_multipart", AsyncMock(side_effect=OSError("abort failed"))),
        pytest.raises(OSError, match="part failed"),
    ):
        await path._multipart_upload(BytesIO(b"abcdef"), 2, size=6)
    assert "Failed to abort S3 multipart upload" in caplog.text


async def test_azure_concurrent_blocks_commit_in_source_order_and_use_unique_ids():
    path = _make_azure_path()
    completed = []
    commits = []
    final = asyncio.Event()

    async def upload(*, block_id, data, **kwargs):
        if data == b"ab":
            await final.wait()
        if data == b"ef":
            final.set()
        completed.append(block_id)

    async def commit(*, block_ids, **kwargs):
        commits.append(block_ids)

    with (
        TransferBudget(floor=3),
        patch("asanypath.azure.az_put_block", upload),
        patch("asanypath.azure.az_put_block_list", commit),
    ):
        await asyncio.wait_for(path._block_upload(BytesIO(b"abcdef"), 2, size=6), 2)
        await asyncio.wait_for(path._block_upload(BytesIO(b"abcdef"), 2, size=6), 2)
    assert commits[0][0] != completed[0]
    assert not set(commits[0]) & set(commits[1])
    assert all(ids == sorted(ids) for ids in commits)
    assert len({len(block_id) for ids in commits for block_id in ids}) == 1


@pytest.mark.parametrize("cancel", [False, True])
async def test_azure_failure_never_commits_partial_blob(cancel):
    path = _make_azure_path()
    started = asyncio.Event()
    active = 0

    async def upload(**kwargs):
        nonlocal active
        active += 1
        started.set()
        try:
            if not cancel:
                raise OSError("stage failed")
            await asyncio.Event().wait()
        finally:
            active -= 1

    commit = AsyncMock()
    with (
        patch("asanypath.azure.az_put_block", upload),
        patch("asanypath.azure.az_put_block_list", commit),
    ):
        task = asyncio.create_task(path._block_upload(BytesIO(b"abcdef"), 2, size=6))
        if cancel:
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else OSError):
            await asyncio.wait_for(task, 2)
    assert active == 0
    commit.assert_not_awaited()


async def test_gcs_resumable_session_remains_sequential():
    path = _make_gcs_path()
    active = 0
    offsets = []

    async def upload(*, offset, **kwargs):
        nonlocal active
        active += 1
        try:
            assert active == 1
            await asyncio.sleep(0)
            offsets.append(offset)
        finally:
            active -= 1

    with (
        TransferBudget(floor=4),
        patch("asanypath.gcs.gcs_start_resumable", AsyncMock(return_value="session")),
        patch("asanypath.gcs.gcs_upload_chunk", upload),
    ):
        await path._resumable_upload(BytesIO(b"abcdef"), 6, 2)
    assert offsets == [0, 2, 4]


@pytest.mark.parametrize("backend", ["s3", "azure"])
@pytest.mark.parametrize("synchronous", [False, True])
async def test_native_upload_parts_overlap_on_wire(backend, synchronous):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import parse_qs, urlsplit
    from xml.etree import ElementTree

    part_size = 5 * 1024 * 1024
    payload = b"a" * part_size + b"b" * part_size + b"tail"
    parts = {}
    committed = []
    all_started = threading.Event()
    lock = threading.Lock()
    timeouts = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, body=b"", **headers):
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            query = parse_qs(urlsplit(self.path).query)
            if "uploadId" not in query:
                self.respond(
                    b"<InitiateMultipartUploadResult><UploadId>U</UploadId>"
                    b"</InitiateMultipartUploadResult>"
                )
            else:
                committed.extend(
                    int(part.findtext("PartNumber")) for part in ElementTree.fromstring(body)
                )
                self.respond()

        def do_PUT(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            query = parse_qs(urlsplit(self.path).query)
            if query.get("comp") == ["blocklist"]:
                committed.extend(part.text for part in ElementTree.fromstring(body))
                self.respond()
                return
            number = query.get("partNumber", query.get("blockid"))[0]
            with lock:
                parts[int(number) if backend == "s3" else number] = body
                if len(parts) == 3:
                    all_started.set()
            if not all_started.wait(3):
                timeouts.append(number)
            self.respond(ETag=f'"{number}"')

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    path = (
        _make_s3path(endpoint_url=endpoint)
        if backend == "s3"
        else _make_azure_path(endpoint_url=endpoint)
    )

    def sync_upload():
        with path.open("wb", buffering=part_size) as file:
            file.write(payload)

    try:
        if synchronous:
            await asyncio.wait_for(asyncio.to_thread(sync_upload), 15)
        else:
            with TransferBudget(floor=3, ceiling=3):
                async with path.open("wb", buffering=part_size) as file:
                    file.write(payload)
        assert not timeouts, "parts were serialized instead of overlapping on the wire"
        assert len(parts) == 3
        assert b"".join(parts[number] for number in committed) == payload
    finally:
        all_started.set()
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("recursive", [False, True])
async def test_cross_backend_copy_uses_spooled_upload(recursive, monkeypatch):
    from asanypath.options import BackendOptions
    from asanypath.s3 import S3Path

    source = _make_s3path()
    destination = _make_azure_path()
    options = BackendOptions(headers={"Content-Type": "application/octet-stream"})
    received = []

    async def upload(self, fileobj, size, **kwargs):
        assert fileobj._rolled
        assert size == 12
        assert kwargs == {"chunk_size": 4, "backend_options": options}
        received.append(fileobj.read())

    monkeypatch.setattr("asanypath.cloud.SPOOL_MAX_SIZE", 4)
    monkeypatch.setattr(S3Path, "is_dir", AsyncMock(return_value=False))
    monkeypatch.setattr(source, "iter_bytes", lambda _: chunks())
    monkeypatch.setattr(type(destination), "exists", AsyncMock(return_value=False))
    monkeypatch.setattr(type(destination), "_upload_buffer", upload)
    monkeypatch.setattr(type(destination), "write_bytes", AsyncMock(side_effect=AssertionError))

    async def chunks():
        for chunk in (b"abcd", b"efgh", b"ijkl"):
            yield chunk

    if recursive:
        child = source / "child"
        monkeypatch.setattr(child, "iter_bytes", lambda _: chunks())

        async def children():
            yield child

        monkeypatch.setattr(source, "is_dir", AsyncMock(return_value=True))
        monkeypatch.setattr(source, "iterdir", children)
    with TransferBudget(min_range_size=8):
        await source.copy(
            destination, recursive=recursive, chunk_size=4, destination_backend_options=options
        )
    assert received == [b"abcdefghijkl"]
