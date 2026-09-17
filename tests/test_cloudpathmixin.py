# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from os.path import expanduser
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import msgspec
import pytest

from asanypath.artifactory import ArtifactoryPath
from asanypath.azure import AzurePath
from asanypath.cloud import CloudPathMixin, _CloudFile, _CloudRawIO
from asanypath.gcs import GCSPath
from asanypath.options import AccessPolicyPatch, BackendOptions
from asanypath.s3 import S3Path

# ---------------------------------------------------------------------------
# Test shared CloudPathMixin methods across all concrete backends.
# These test inherited behavior — if they pass for one backend, they pass for all.
# We parametrize to confirm no backend accidentally breaks the contract.
# ---------------------------------------------------------------------------

_BACKENDS = [
    pytest.param(
        lambda path: S3Path(
            path,
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        ),
        id="s3",
    ),
    pytest.param(
        lambda path: GCSPath(
            path, endpoint_url="https://storage.googleapis.com", access_token="tok"
        ),
        id="gcs",
    ),
    pytest.param(
        lambda path: AzurePath(
            path,
            account_name="dev",
            account_key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
            endpoint_url="https://dev.blob.core.windows.net",
        ),
        id="azure",
    ),
    pytest.param(
        lambda path: ArtifactoryPath(path, token="tok"),
        id="artifactory",
    ),
]


def _make(factory, scheme, path="bucket/nested/file.txt"):
    return factory(f"{scheme}://{path}")


@pytest.fixture(params=_BACKENDS)
def cloud_path(request):
    """A cloud path instance from each backend."""
    factory = request.param
    if request.node.callspec.id == "s3":
        return factory("s3://bucket/nested/file.txt")
    elif request.node.callspec.id == "gcs":
        return factory("gs://bucket/nested/file.txt")
    elif request.node.callspec.id == "azure":
        return factory("az://container/nested/file.txt")
    elif request.node.callspec.id == "artifactory":
        return factory("art://host/artifactory/repo/file.txt")


class TestCloudPathShared:
    """Test CloudPathMixin concrete methods via real backend instances."""

    def test_home(self, cloud_path):
        h = type(cloud_path).home()
        assert expanduser("~") in h.as_posix()

    def test_is_absolute(self, cloud_path):
        assert cloud_path.is_absolute() is True

    def test_is_block_device(self, cloud_path):
        assert cloud_path.is_block_device() is False

    def test_is_char_device(self, cloud_path):
        assert cloud_path.is_char_device() is False

    def test_is_fifo(self, cloud_path):
        assert cloud_path.is_fifo() is False

    def test_is_junction(self, cloud_path):
        assert cloud_path.is_junction() is False

    def test_is_mount(self, cloud_path):
        assert cloud_path.is_mount() is False

    def test_is_reserved(self, cloud_path):
        assert cloud_path.is_reserved() is False

    def test_is_socket(self, cloud_path):
        assert cloud_path.is_socket() is False

    def test_is_symlink(self, cloud_path):
        assert cloud_path.is_symlink() is False

    def test_expanduser(self, cloud_path):
        assert cloud_path.expanduser() == cloud_path

    def test_resolve(self, cloud_path):
        assert cloud_path.resolve() == cloud_path

    def test_samefile(self, cloud_path):
        assert cloud_path.samefile(cloud_path)

    def test_bytes(self, cloud_path):
        assert bytes(cloud_path) == str(cloud_path).encode()

    def test_fspath_raises(self, cloud_path):
        with pytest.raises(TypeError):
            cloud_path.__fspath__()

    async def test_artifactory_access_policy_requires_target(self, cloud_path):
        if not isinstance(cloud_path, (S3Path, GCSPath, AzurePath)):
            with pytest.raises(ValueError, match="permission_target"):
                await cloud_path.get_access_policy()
            with pytest.raises(ValueError, match="permission_target"):
                await cloud_path.update_access_policy(AccessPolicyPatch())


# ---------------------------------------------------------------------------
# Tests for specific CloudPathMixin branches not hit by parametrized tests
# ---------------------------------------------------------------------------


def _make_s3():
    return S3Path(
        "s3://bucket/file.txt",
        endpoint_url="https://s3.amazonaws.com",
        aws_region="us-east-1",
        aws_access_key_id="AKID",
        aws_secret_access_key="SECRET",
    )


class TestGetBatcher:
    def test_raises_when_ops_missing(self):
        """CloudPathMixin._get_batcher raises when _batcher_key_fn or _batcher_ops is None."""

        class BareCloud(CloudPathMixin):
            protocol = "bare"
            _batcher_key_fn = None
            _batcher_ops = None

            @property
            def _native_kwargs(self):
                return {}

        p = BareCloud("bare://host/path")
        with pytest.raises(NotImplementedError, match="must define _batcher_key_fn"):
            p._get_batcher()


class TestEnvConfigSetter:
    def test_env_config_setter(self):
        from types import SimpleNamespace

        p = _make_s3()
        ns = SimpleNamespace(test=True)
        p.env_config = ns
        assert p.env_config.test is True
        # Reset
        p.env_config = None


class TestFetchListing:
    async def test_not_a_directory(self):
        p = _make_s3()
        batcher = AsyncMock()
        batcher.list = AsyncMock(side_effect=FileNotFoundError)
        with patch.object(p, "_get_batcher", return_value=batcher):
            with pytest.raises(NotADirectoryError):
                await p._fetch_listing()


class TestBackendOptions:
    async def test_write_bytes_keeps_options_with_upload_item(self):
        path = _make_s3()
        batcher = AsyncMock()
        options = BackendOptions(headers={"x-amz-meta-source": "etl"})

        with patch.object(path, "_get_batcher", return_value=batcher):
            assert await path.write_bytes(b"payload", backend_options=options) == 7

        batcher.put.assert_awaited_once_with(
            item=(path._item_path, b"payload", msgspec.json.encode(options).decode()),
            **path._native_kwargs,
        )

    async def test_open_write_passes_options_on_close(self):
        path = _make_s3()
        options = BackendOptions(provider={"storage_class": "STANDARD_IA"})
        write_bytes = AsyncMock()

        with patch.object(type(path), "write_bytes", new=write_bytes):
            async with path.open("wb", backend_options=options) as file:
                file.write(b"payload")

        write_bytes.assert_awaited_once_with(b"payload", backend_options=options)


class TestCopyCallbacks:
    async def test_copy_rejects_different_existing_destination_without_force(self):
        src = _make_s3()
        dst = S3Path(
            "s3://bucket/dst.txt",
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )

        with (
            patch.object(src, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(dst, "exists", new=AsyncMock(return_value=True)),
            patch.object(dst, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(src, "checksums", new=AsyncMock(return_value={"sha256": "src"})),
            patch.object(dst, "checksums", new=AsyncMock(return_value={"sha256": "dst"})),
            patch.object(type(dst), "write_bytes", new=AsyncMock()) as write_bytes,
        ):
            with pytest.raises(FileExistsError):
                await src.copy(dst)

        write_bytes.assert_not_awaited()

    async def test_copy_skips_identical_existing_destination(self):
        src = _make_s3()
        dst = S3Path(
            "s3://bucket/dst.txt",
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )

        with (
            patch.object(src, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(dst, "exists", new=AsyncMock(return_value=True)),
            patch.object(dst, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(src, "checksums", new=AsyncMock(return_value={"sha256": "same"})),
            patch.object(dst, "checksums", new=AsyncMock(return_value={"sha256": "same"})),
            patch.object(type(dst), "write_bytes", new=AsyncMock()) as write_bytes,
        ):
            assert await src.copy(dst) == dst

        write_bytes.assert_not_awaited()

    async def test_copy_force_overwrites_different_existing_destination(self):
        src = _make_s3()
        dst = S3Path(
            "s3://bucket/dst.txt",
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )

        with (
            patch.object(src, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(dst, "exists", new=AsyncMock(return_value=True)),
            patch.object(dst, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(src, "checksums", new=AsyncMock(return_value={"sha256": "src"})),
            patch.object(dst, "checksums", new=AsyncMock(return_value={"sha256": "dst"})),
            patch.object(S3Path, "_copy_batch_fn", AsyncMock()) as copy_fn,
        ):
            assert await src.copy(dst, force=True) == dst

        copy_fn.assert_awaited_once()

    async def test_copy_to_pathlib_destination_uses_local_backend(self, tmp_path):
        src = _make_s3()
        dst = tmp_path / "download.txt"

        with (
            patch.object(src, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(src, "read_bytes", new=AsyncMock(return_value=b"payload")),
        ):
            copied = await src.copy(dst)

        assert str(copied) == str(dst)
        assert dst.read_bytes() == b"payload"

    async def test_chunked_copy_to_pathlib_destination_is_byte_exact(self, cloud_path, tmp_path):
        data = b"0123456789"

        async def range_read(start, end):
            return data[start : end + 1]

        with (
            patch.object(type(cloud_path), "is_dir", new=AsyncMock(return_value=False)),
            patch.object(
                type(cloud_path),
                "stat",
                new=AsyncMock(return_value=SimpleNamespace(st_size=len(data))),
            ),
            patch.object(type(cloud_path), "_range_read", new=AsyncMock(side_effect=range_read)),
        ):
            await cloud_path.copy(tmp_path / "download.bin", chunk_size=4)

        assert (tmp_path / "download.bin").read_bytes() == data

    async def test_copy_atomic_to_pathlib_destination_preserves_existing_on_failure(
        self, tmp_path, monkeypatch
    ):
        src = _make_s3()
        dst = tmp_path / "download.txt"
        dst.write_bytes(b"old")

        def fail_replace(self, target):
            raise OSError("commit failed")

        monkeypatch.setattr(type(dst), "replace", fail_replace)

        with (
            patch.object(src, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(src, "checksums", new=AsyncMock(return_value={"sha256": "new"})),
            patch.object(src, "read_bytes", new=AsyncMock(return_value=b"new")),
        ):
            with pytest.raises(OSError, match="commit failed"):
                await src.copy(dst, atomic=True, force=True)

        assert dst.read_bytes() == b"old"
        assert not list(tmp_path.glob(".download.txt.tmp-*"))

    async def test_copy_atomic_to_cloud_destination_is_unsupported(self):
        src = _make_s3()
        dst = S3Path(
            "s3://other-bucket/dst.txt",
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )

        with pytest.raises(NotImplementedError, match="atomic"):
            await src.copy(dst, atomic=True)

    async def test_copy_callbacks(self):
        src = _make_s3()
        dst = S3Path(
            "s3://bucket/dst.txt",
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )

        progress_calls: list[tuple[int, int]] = []
        done_calls: list[tuple[int, bool, str | None]] = []

        def on_progress(delta: int, transferred: int, src_path, dst_path) -> None:
            assert src_path == src
            assert dst_path == dst
            progress_calls.append((delta, transferred))

        async def on_done(src_path, dst_path, transferred: int, ok: bool, error):
            assert src_path == src
            assert dst_path == dst
            done_calls.append((transferred, ok, None if error is None else type(error).__name__))

        with (
            patch.object(type(src), "is_dir", new=AsyncMock(return_value=False)),
            patch.object(type(src), "exists", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "_copy_batch_fn", AsyncMock()),
            patch.object(type(dst), "stat", new=AsyncMock(return_value=SimpleNamespace(st_size=7))),
        ):
            await src.copy(dst, on_progress=on_progress, on_done=on_done)

        assert progress_calls[-1] == (7, 7)
        assert done_calls == [(7, True, None)]


class TestRenameReplaceCallbacks:
    async def test_rename_callbacks_called(self):
        src = _make_s3()
        progress_calls: list[tuple[int, int]] = []
        done_calls: list[tuple[int, bool, str | None]] = []

        def on_progress(delta: int, transferred: int, src_path, dst_path) -> None:
            progress_calls.append((delta, transferred))

        async def on_done(src_path, dst_path, transferred: int, ok: bool, error):
            done_calls.append((transferred, ok, None if error is None else type(error).__name__))

        with (
            patch.object(type(src), "exists", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "_copy_batch_fn", AsyncMock()),
            patch.object(S3Path, "stat", new=AsyncMock(return_value=SimpleNamespace(st_size=7))),
            patch.object(type(src), "unlink", new=AsyncMock(return_value=None)),
        ):
            out = await src.rename("s3://bucket/new.txt", on_progress=on_progress, on_done=on_done)

        assert str(out) == "s3://bucket/new.txt"
        assert progress_calls == [(7, 7)]
        assert done_calls == [(7, True, None)]

    async def test_replace_callbacks_called(self):
        src = _make_s3()
        progress_calls: list[tuple[int, int]] = []
        done_calls: list[tuple[int, bool, str | None]] = []

        def on_progress(delta: int, transferred: int, src_path, dst_path) -> None:
            progress_calls.append((delta, transferred))

        async def on_done(src_path, dst_path, transferred: int, ok: bool, error):
            done_calls.append((transferred, ok, None if error is None else type(error).__name__))

        with (
            patch.object(type(src), "exists", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "_copy_batch_fn", AsyncMock()),
            patch.object(S3Path, "stat", new=AsyncMock(return_value=SimpleNamespace(st_size=7))),
            patch.object(type(src), "unlink", new=AsyncMock(return_value=None)),
        ):
            out = await src.replace(
                "s3://bucket/replaced.txt",
                on_progress=on_progress,
                on_done=on_done,
            )

        assert str(out) == "s3://bucket/replaced.txt"
        assert progress_calls == [(7, 7)]
        assert done_calls == [(7, True, None)]

    async def test_move_callbacks_called(self):
        src = _make_s3()
        progress_calls: list[tuple[int, int]] = []
        done_calls: list[tuple[int, bool, str | None]] = []

        def on_progress(delta: int, transferred: int, src_path, dst_path) -> None:
            progress_calls.append((delta, transferred))

        async def on_done(src_path, dst_path, transferred: int, ok: bool, error):
            done_calls.append((transferred, ok, None if error is None else type(error).__name__))

        with (
            patch.object(type(src), "exists", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "_copy_batch_fn", AsyncMock()),
            patch.object(S3Path, "stat", new=AsyncMock(return_value=SimpleNamespace(st_size=7))),
            patch.object(type(src), "unlink", new=AsyncMock(return_value=None)),
        ):
            out = await src.move("s3://bucket/moved.txt", on_progress=on_progress, on_done=on_done)

        assert str(out) == "s3://bucket/moved.txt"
        assert progress_calls == [(7, 7)]
        assert done_calls == [(7, True, None)]


class TestIterdirCache:
    async def test_cache_hit(self):
        p = _make_s3()
        # Pre-populate cache with a recent entry
        import time

        cache_key = p._listing_cache_key()
        type(p)._listing_cache[cache_key] = (
            time.monotonic(),
            ["s3://bucket/a.txt", "s3://bucket/b.txt"],
        )
        results = [child async for child in p.iterdir()]
        assert len(results) == 2
        assert results[0].name == "a.txt"
        # Clean up
        type(p)._listing_cache.clear()


class TestRglob:
    async def test_recursive_descent(self):
        p = _make_s3()
        child_file = _make_s3()
        child_file._path = S3Path(
            "s3://bucket/sub/match.txt",
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )._path

        child_dir = _make_s3()
        child_dir._path = S3Path(
            "s3://bucket/sub",
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )._path

        grandchild = _make_s3()
        grandchild._path = S3Path(
            "s3://bucket/sub/deep.txt",
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )._path

        async def fake_iterdir_root(**kw):
            yield child_dir

        async def fake_iterdir_sub(**kw):
            yield grandchild

        with (
            patch.object(type(p), "iterdir", fake_iterdir_root),
            patch.object(child_dir, "is_dir", new=AsyncMock(return_value=True)),
        ):

            async def patched_rglob(self_inner, pattern, *, case_sensitive=None):
                # Simulate one level deep
                import fnmatch as fn

                if fn.fnmatch(grandchild.name, pattern):
                    yield grandchild

            with patch.object(type(child_dir), "rglob", patched_rglob):
                results = [r async for r in p.rglob("*.txt")]
            assert any("deep.txt" in str(r) for r in results)


class TestWalk:
    async def test_walk_without_batch_fn(self):
        """walk() falls back to asyncio.gather(is_dir()) when _is_dir_batch_fn is None."""
        p = _make_s3()
        child = _make_s3()
        child._path = S3Path(
            "s3://bucket/child.txt",
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )._path

        async def fake_iterdir(self_inner, **kw):
            yield child

        old_batch_fn = type(p)._is_dir_batch_fn
        type(p)._is_dir_batch_fn = None
        try:
            with (
                patch.object(type(p), "iterdir", fake_iterdir),
                patch.object(child, "is_dir", new=AsyncMock(return_value=False)),
            ):
                results = [r async for r in p.walk()]
            assert len(results) == 1
            root, dirs, files = results[0]
            assert len(files) == 1
            assert len(dirs) == 0
        finally:
            type(p)._is_dir_batch_fn = old_batch_fn


class TestCloudTextIO:
    """Cloud text I/O must accept the same options as local/pathlib (one-way contract)."""

    async def test_write_text_honors_newline(self, cloud_path):
        mock_wb = AsyncMock(return_value=0)
        with patch.object(type(cloud_path), "write_bytes", new=mock_wb):
            await cloud_path.write_text("a\nb", newline="\r\n")
        assert mock_wb.call_args[0][0] == b"a\r\nb"

    async def test_write_text_honors_errors(self, cloud_path):
        mock_wb = AsyncMock(return_value=0)
        with patch.object(type(cloud_path), "write_bytes", new=mock_wb):
            await cloud_path.write_text("a\u00e9", encoding="ascii", errors="ignore")
        assert mock_wb.call_args[0][0] == b"a"

    async def test_read_text_universal_newline_default(self, cloud_path):
        with patch.object(type(cloud_path), "read_bytes", new=AsyncMock(return_value=b"a\r\nb")):
            assert await cloud_path.read_text() == "a\nb"

    async def test_read_text_newline_disabled(self, cloud_path):
        with patch.object(type(cloud_path), "read_bytes", new=AsyncMock(return_value=b"a\r\nb")):
            assert await cloud_path.read_text(newline="") == "a\r\nb"


# ---------------------------------------------------------------------------
# _CloudFile tests
# ---------------------------------------------------------------------------


class TestCloudFileOpen:
    """Test CloudPathMixin.open() returns a usable file-like object."""

    def test_open_returns_cloud_file(self, cloud_path):
        cf = cloud_path.open("rb")
        assert hasattr(cf, "__enter__")
        assert hasattr(cf, "__exit__")
        assert hasattr(cf, "__aenter__")
        assert hasattr(cf, "__aexit__")

    async def test_open_read_bytes_async(self, cloud_path):
        content = b"hello cloud"
        with patch.object(type(cloud_path), "read_bytes", new=AsyncMock(return_value=content)):
            async with cloud_path.open("rb") as f:
                assert f.read() == content

    async def test_open_read_text_async(self, cloud_path):
        content = b"hello text"
        with patch.object(type(cloud_path), "read_bytes", new=AsyncMock(return_value=content)):
            async with cloud_path.open("r") as f:
                assert f.read() == "hello text"

    async def test_open_write_bytes_async(self, cloud_path):
        mock_write = AsyncMock()
        with patch.object(type(cloud_path), "write_bytes", new=mock_write):
            async with cloud_path.open("wb") as f:
                f.write(b"new data")
        mock_write.assert_awaited_once_with(b"new data")

    async def test_open_write_text_async(self, cloud_path):
        mock_write = AsyncMock()
        with patch.object(type(cloud_path), "write_bytes", new=mock_write):
            async with cloud_path.open("w") as f:
                f.write("text data")
        mock_write.assert_awaited_once()
        written = mock_write.call_args[0][0]
        assert b"text data" in written

    async def test_iter_bytes_uses_range_reads(self, cloud_path):
        with (
            patch.object(type(cloud_path), "_supports_range_read", True),
            patch.object(
                type(cloud_path),
                "_range_read",
                new=AsyncMock(side_effect=[b"0123", b"4567", b"89"]),
            ) as mock_range_read,
            patch.object(type(cloud_path), "stat", new=AsyncMock()) as mock_stat,
            patch.object(type(cloud_path), "read_bytes", new=AsyncMock()) as mock_read_bytes,
        ):
            chunks = [chunk async for chunk in cloud_path.iter_bytes(4)]
        assert chunks == [b"0123", b"4567", b"89"]
        # Sequential windows until a short read; no size probe (stat) needed.
        assert [args.args for args in mock_range_read.await_args_list] == [(0, 3), (4, 7), (8, 11)]
        mock_stat.assert_not_awaited()
        mock_read_bytes.assert_not_awaited()

    async def test_iter_bytes_single_chunk(self, cloud_path):
        with (
            patch.object(type(cloud_path), "_supports_range_read", True),
            patch.object(
                type(cloud_path), "_range_read", new=AsyncMock(return_value=b"abc")
            ) as mock_range_read,
            patch.object(type(cloud_path), "stat", new=AsyncMock()) as mock_stat,
            patch.object(type(cloud_path), "read_bytes", new=AsyncMock()) as mock_read_bytes,
        ):
            chunks = [chunk async for chunk in cloud_path.iter_bytes(8)]
        assert chunks == [b"abc"]
        assert mock_range_read.await_count == 1  # short read -> stop, no extra request
        mock_stat.assert_not_awaited()
        mock_read_bytes.assert_not_awaited()

    async def test_iter_bytes_stops_on_empty_past_eof(self, cloud_path):
        # Exact-multiple object: the window past EOF returns empty (416 -> b"").
        with (
            patch.object(type(cloud_path), "_supports_range_read", True),
            patch.object(
                type(cloud_path), "_range_read", new=AsyncMock(side_effect=[b"0123", b"4567", b""])
            ),
        ):
            chunks = [chunk async for chunk in cloud_path.iter_bytes(4)]
        assert chunks == [b"0123", b"4567"]

    async def test_iter_bytes_falls_back_without_range_read(self, cloud_path):
        with (
            patch.object(type(cloud_path), "_supports_range_read", False),
            patch.object(type(cloud_path), "read_bytes", new=AsyncMock(return_value=b"0123456789")),
        ):
            chunks = [chunk async for chunk in cloud_path.iter_bytes(4)]
        assert chunks == [b"0123", b"4567", b"89"]

    async def test_iter_bytes_default_chunk_size(self, cloud_path):
        with (
            patch.object(type(cloud_path), "_supports_range_read", True),
            patch.object(type(cloud_path), "_range_read", new=AsyncMock(return_value=b"xy")),
        ):
            chunks = [chunk async for chunk in cloud_path.iter_bytes()]
        assert chunks == [b"xy"]

    async def test_iter_bytes_rejects_invalid_chunk_size(self, cloud_path):
        with pytest.raises(ValueError, match="chunk_size"):
            _ = [chunk async for chunk in cloud_path.iter_bytes(0)]


class _RangePath:
    def __init__(self, data: bytes):
        self._data = data

    async def _range_read(self, start: int, end: int) -> bytes:
        return self._data[start : end + 1]


class TestCloudRawIO:
    def test_readinto_seek_tell(self):
        raw = _CloudRawIO(_RangePath(b"abcdef"), 6)
        buf = bytearray(3)
        n = raw.readinto(buf)
        assert n == 3
        assert bytes(buf) == b"abc"
        assert raw.tell() == 3
        assert raw.seek(-1, 1) == 2
        assert raw.seek(-1, 2) == 5
        eof = bytearray(10)
        n2 = raw.readinto(eof)
        assert n2 == 1
        assert bytes(eof[:1]) == b"f"


class TestCloudFileHelpers:
    def test_chunk_size_and_get_size(self):
        p = _make_s3()
        cf = _CloudFile(p, "rb", -1, None, None, None)
        assert cf._chunk_size == cf._DEFAULT_CHUNK
        assert cf._get_size({"Content-Length": "123"}) == 123
        assert cf._get_size({"Content-Length": "bad"}) is None
        assert cf._get_size({"X": "1"}) is None

    def test_make_reader_unbuffered_text_raises(self):
        p = _make_s3()
        cf = _CloudFile(p, "r", 0, None, None, None)
        with pytest.raises(ValueError):
            cf._make_reader(10)

    async def test_rmdir_non_recursive_non_empty_raises(self):
        p = _make_s3()
        child = _make_s3()

        async def fake_iterdir(**kwargs):
            yield child

        with patch.object(p, "iterdir", fake_iterdir):
            with pytest.raises(OSError):
                await p.rmdir(recursive=False)

    async def test_rmdir_recursive_unlinks_files(self):
        p = _make_s3()
        child_file = _make_s3()
        child_dir = _make_s3()

        child_file.is_dir = AsyncMock(return_value=False)
        child_file.unlink = AsyncMock()
        child_dir.is_dir = AsyncMock(return_value=True)
        child_dir.rmdir = AsyncMock()

        async def fake_root_iterdir(**kwargs):
            yield child_file
            yield child_dir

        with patch.object(p, "iterdir", fake_root_iterdir):
            await p.rmdir(recursive=True)

        child_file.unlink.assert_awaited_once()
        child_dir.rmdir.assert_awaited_once_with(recursive=True)

    async def test_open_no_upload_on_exception(self, cloud_path):
        mock_write = AsyncMock()
        with (
            patch.object(type(cloud_path), "write_bytes", new=mock_write),
            pytest.raises(ValueError),
        ):
            async with cloud_path.open("wb") as f:
                f.write(b"partial")
                raise ValueError("abort")
        mock_write.assert_not_awaited()

    def test_open_read_sync(self, cloud_path):
        content = b"sync read"
        with patch.object(type(cloud_path), "read_bytes", new=AsyncMock(return_value=content)):
            with cloud_path.open("rb") as f:
                assert f.read() == content

    def test_open_write_sync(self, cloud_path):
        mock_write = AsyncMock()
        with patch.object(type(cloud_path), "write_bytes", new=mock_write):
            with cloud_path.open("wb") as f:
                f.write(b"sync write")
        mock_write.assert_awaited_once_with(b"sync write")

    async def test_open_seek_async(self, cloud_path):
        content = b"0123456789"
        with patch.object(type(cloud_path), "read_bytes", new=AsyncMock(return_value=content)):
            async with cloud_path.open("rb") as f:
                f.seek(5)
                assert f.read() == b"56789"


# ---------------------------------------------------------------------------
# Native server-side copy dispatch (same-backend uses native, else falls back)
# ---------------------------------------------------------------------------


def _s3(path: str) -> S3Path:
    return S3Path(
        path,
        endpoint_url="https://s3.amazonaws.com",
        aws_region="us-east-1",
        aws_access_key_id="AKID",
        aws_secret_access_key="SECRET",
    )


class TestServerSideCopy:
    async def test_same_backend_copy_uses_native(self):
        src, dst = _s3("s3://bucket/a.txt"), _s3("s3://bucket/b.txt")
        copy_fn = AsyncMock()
        with (
            patch.object(S3Path, "_copy_batch_fn", copy_fn),
            patch.object(S3Path, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
            patch.object(
                S3Path, "read_bytes", new=AsyncMock(side_effect=AssertionError("no download"))
            ),
        ):
            result = await src.copy(dst)
        assert str(result) == "s3://bucket/b.txt"
        copy_fn.assert_awaited_once()
        assert copy_fn.call_args.kwargs["pairs"] == [("a.txt", "b.txt")]

    async def test_same_backend_copy_always_passes_options_json(self):
        """Native copy binding requires options_json; pass None when unset."""
        src, dst = _s3("s3://bucket/a.txt"), _s3("s3://bucket/b.txt")
        copy_fn = AsyncMock()
        with (
            patch.object(S3Path, "_copy_batch_fn", copy_fn),
            patch.object(S3Path, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
        ):
            await src.copy(dst)
        assert "options_json" in copy_fn.call_args.kwargs
        assert copy_fn.call_args.kwargs["options_json"] is None

    async def test_same_backend_copy_passes_destination_options(self):
        src, dst = _s3("s3://bucket/a.txt"), _s3("s3://bucket/b.txt")
        copy_fn = AsyncMock()
        options = BackendOptions(headers={"x-amz-meta-source": "etl"})
        with (
            patch.object(S3Path, "_copy_batch_fn", copy_fn),
            patch.object(S3Path, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
        ):
            await src.copy(dst, destination_backend_options=options)

        assert copy_fn.call_args.kwargs["options_json"] == msgspec.json.encode(options).decode()

    async def test_cross_bucket_copy_falls_back(self):
        src, dst = _s3("s3://bucket1/a.txt"), _s3("s3://bucket2/b.txt")
        copy_fn = AsyncMock()
        write = AsyncMock(return_value=4)
        with (
            patch.object(S3Path, "_copy_batch_fn", copy_fn),
            patch.object(S3Path, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "read_bytes", new=AsyncMock(return_value=b"data")),
            patch.object(S3Path, "write_bytes", new=write),
        ):
            await src.copy(dst)
        copy_fn.assert_not_awaited()
        write.assert_awaited_once()

    async def test_cross_bucket_copy_passes_destination_options_to_write(self):
        src, dst = _s3("s3://bucket1/a.txt"), _s3("s3://bucket2/b.txt")
        options = BackendOptions(headers={"x-amz-meta-source": "etl"})
        write = AsyncMock(return_value=4)
        with (
            patch.object(S3Path, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "read_bytes", new=AsyncMock(return_value=b"data")),
            patch.object(S3Path, "write_bytes", new=write),
        ):
            await src.copy(dst, destination_backend_options=options)

        write.assert_awaited_once_with(b"data", backend_options=options)

    async def test_copy_rejects_destination_options_for_local_destination(self, tmp_path):
        src = _s3("s3://bucket/a.txt")
        with pytest.raises(ValueError, match="cloud destination"):
            await src.copy(
                tmp_path / "b.txt",
                atomic=True,
                destination_backend_options=BackendOptions(headers={"x-meta": "value"}),
            )

    async def test_rename_uses_native(self):
        src = _s3("s3://bucket/a.txt")
        copy_fn = AsyncMock()
        with (
            patch.object(S3Path, "_copy_batch_fn", copy_fn),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "unlink", new=AsyncMock()),
            patch.object(
                S3Path, "read_bytes", new=AsyncMock(side_effect=AssertionError("no download"))
            ),
        ):
            result = await src.rename("s3://bucket/b.txt")
        assert str(result) == "s3://bucket/b.txt"
        copy_fn.assert_awaited_once()

    async def test_atomic_same_backend_allowed(self):
        src, dst = _s3("s3://bucket/a.txt"), _s3("s3://bucket/b.txt")
        copy_fn = AsyncMock()
        with (
            patch.object(S3Path, "_copy_batch_fn", copy_fn),
            patch.object(S3Path, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
        ):
            await src.copy(dst, atomic=True)
        copy_fn.assert_awaited_once()

    async def test_atomic_cross_bucket_raises(self):
        src, dst = _s3("s3://bucket1/a.txt"), _s3("s3://bucket2/b.txt")
        with pytest.raises(NotImplementedError, match="atomic"):
            await src.copy(dst, atomic=True)

    async def test_chunk_size_cross_backend_ignored(self):
        src, dst = _s3("s3://bucket1/a.txt"), _s3("s3://bucket2/b.txt")
        with (
            patch.object(S3Path, "is_dir", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
            patch.object(S3Path, "read_bytes", new=AsyncMock(return_value=b"data")),
            patch.object(S3Path, "write_bytes", new=AsyncMock(return_value=4)),
        ):
            # chunk_size is not applicable to cloud writes — ignored, not an error.
            await src.copy(dst, chunk_size=1024)
