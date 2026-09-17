# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for GCSPath implementation."""

from __future__ import annotations

import contextlib
from unittest.mock import AsyncMock, patch

import msgspec
import pytest

from asanypath import AccessGrant, AccessPolicyPatch
from asanypath.gcs import GCSPath
from tests.conftest import classtest_factory

testbase = classtest_factory("_TestGCSPath", GCSPath)


def _make_gcs_path(
    path: str = "gs://bucket/object/file.txt",
    project_id: str = "test-project",
    endpoint_url: str = "https://storage.googleapis.com",
    access_token: str = "test-token",
    **kwargs,
) -> GCSPath:
    """Return a GCSPath with test credentials."""
    defaults = dict(
        project_id=project_id,
        endpoint_url=endpoint_url,
        access_token=access_token,
    )
    defaults.update(kwargs)
    return GCSPath(path, **defaults)


class TestGCSPath(testbase):
    """Test the GCSPath implementation."""

    __test__ = True

    @pytest.fixture
    def p(self):
        """Fixture for a sample GCS path."""
        return _make_gcs_path()

    async def test_checksums(self, p):
        metadata = {"md5Hash": "abc123==", "crc32c": "xyz789=="}
        with patch("asanypath.gcs.gcs_head", new=AsyncMock(return_value=metadata)):
            result = await p.checksums()
        assert result == {"md5": "abc123==", "crc32c": "xyz789=="}

    async def test_get_access_policy_decodes_object_acl(self, p):
        native_acl = {
            "owner": "owner@example.com",
            "grants": [
                {"principal": "user:reader@example.com", "permission": "READER"},
                {"principal": "group:editors@example.com", "permission": "OWNER"},
            ],
        }
        with patch("asanypath.gcs.gcs_get_acl", new=AsyncMock(return_value=native_acl)) as mock:
            policy = await p.get_access_policy()

        mock.assert_awaited_once_with(object_path="object/file.txt", **p._native_kwargs)
        assert policy.owner == "owner@example.com"
        assert policy.grants == (
            AccessGrant("user:reader@example.com", frozenset({"read"})),
            AccessGrant("group:editors@example.com", frozenset({"read", "write"})),
        )
        assert policy.provider == {"gcs_acl": native_acl}

    async def test_update_access_policy_replaces_native_acl(self, p):
        native_acl = {
            "owner": "owner@example.com",
            "grants": [{"principal": "user:reader@example.com", "permission": "READER"}],
        }
        policy_patch = AccessPolicyPatch(provider={"gcs_acl": native_acl})
        with patch("asanypath.gcs.gcs_put_acl", new=AsyncMock()) as mock:
            await p.update_access_policy(policy_patch)

        mock.assert_awaited_once_with(
            object_path="object/file.txt",
            acl_json=msgspec.json.encode(native_acl).decode(),
            **p._native_kwargs,
        )

    @pytest.mark.parametrize(
        "return_value,expected",
        [(True, True), (False, False)],
        ids=["true", "false"],
    )
    async def test_exists(self, p, return_value, expected):
        mock_batcher = AsyncMock()
        mock_batcher.exists = AsyncMock(return_value=return_value)
        with patch.object(GCSPath, "_get_batcher", return_value=mock_batcher):
            assert await p.exists() is expected

    @pytest.mark.parametrize(
        "exists_val,is_dir_val,expected",
        [
            (True, False, True),
            (False, False, False),
        ],
        ids=["true_when_exists_and_not_dir", "false_when_not_exists"],
    )
    async def test_is_file(self, p, exists_val, is_dir_val, expected):
        mock_batcher = AsyncMock()
        mock_batcher.exists = AsyncMock(return_value=exists_val)
        patches = [patch.object(GCSPath, "_get_batcher", return_value=mock_batcher)]
        if exists_val:
            patches.append(
                patch("asanypath.gcs.gcs_is_dir", new=AsyncMock(return_value=is_dir_val))
            )
        with contextlib.ExitStack() as stack:
            for p_ in patches:
                stack.enter_context(p_)
            assert await p.is_file() is expected

    @pytest.mark.parametrize(
        "return_value,expected",
        [(True, True), (False, False)],
        ids=["true", "false"],
    )
    async def test_is_dir(self, return_value, expected):
        d = _make_gcs_path("gs://bucket/object")
        with patch("asanypath.gcs.gcs_is_dir", new=AsyncMock(return_value=return_value)):
            assert await d.is_dir() is expected

    @pytest.mark.parametrize(
        "dir_path,list_return,expected_strs",
        [
            (
                "gs://bucket/object",
                ["gs://bucket/object/file.txt", "gs://bucket/object/other.txt"],
                ["gs://bucket/object/file.txt", "gs://bucket/object/other.txt"],
            ),
            (
                "gs://bucket/root",
                ["gs://bucket/root/subdir"],
                ["gs://bucket/root/subdir"],
            ),
            (
                "gs://bucket/pg",
                ["gs://bucket/pg/file1.txt", "gs://bucket/pg/file2.txt"],
                ["gs://bucket/pg/file1.txt", "gs://bucket/pg/file2.txt"],
            ),
        ],
        ids=["basic", "prefixes", "pagination"],
    )
    async def test_iterdir(self, dir_path, list_return, expected_strs):
        d = _make_gcs_path(dir_path)
        mock_batcher = AsyncMock()
        mock_batcher.list = AsyncMock(return_value=list_return)
        with patch.object(GCSPath, "_get_batcher", return_value=mock_batcher):
            results = [str(x) async for x in d.iterdir()]
        for expected in expected_strs:
            assert expected in results

    async def test_iterdir_raises_not_a_directory(self):
        d = _make_gcs_path("gs://bucket/nodir")
        mock_batcher = AsyncMock()
        mock_batcher.list = AsyncMock(side_effect=FileNotFoundError(2, ""))
        with patch.object(GCSPath, "_get_batcher", return_value=mock_batcher):
            with pytest.raises(NotADirectoryError):
                async for _ in d.iterdir():
                    pass

    async def test_list_containers_at_root(self):
        root = _make_gcs_path("gs://")
        with patch(
            "asanypath.gcs.gcs_list_buckets",
            new=AsyncMock(return_value=["alpha", "beta"]),
        ):
            assert await root._list_containers() == ["gs://alpha", "gs://beta"]

    async def test_list_containers_none_when_bucket_set(self, p):
        with patch("asanypath.gcs.gcs_list_buckets", new=AsyncMock()) as mock:
            assert await p._list_containers() is None
        mock.assert_not_called()

    async def test_iterdir_lists_buckets_at_root(self):
        root = _make_gcs_path("gs://")
        with patch(
            "asanypath.gcs.gcs_list_buckets",
            new=AsyncMock(return_value=["alpha", "beta"]),
        ):
            results = [str(x) async for x in root.iterdir()]
        assert results == ["gs://alpha", "gs://beta"]

    async def test_read_bytes(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.get = AsyncMock(return_value=b"\xde\xad\xbe\xef")
        with patch.object(GCSPath, "_get_batcher", return_value=mock_batcher):
            assert await p.read_bytes() == b"\xde\xad\xbe\xef"

    async def test_read_text(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.get = AsyncMock(return_value=b"hello")
        with patch.object(GCSPath, "_get_batcher", return_value=mock_batcher):
            assert await p.read_text() == "hello"

    async def test_write_bytes(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.put = AsyncMock(return_value=None)
        with patch.object(GCSPath, "_get_batcher", return_value=mock_batcher):
            result = await p.write_bytes(b"data")
        assert result == 4

    async def test_write_text(self, p):
        with patch.object(p, "write_bytes", new=AsyncMock(return_value=5)):
            result = await p.write_text("hello")
        assert result == 5

    @pytest.mark.parametrize(
        "side_effect,missing_ok",
        [
            (None, False),
            (FileNotFoundError(2, ""), True),
        ],
        ids=["success", "missing_ok"],
    )
    async def test_unlink(self, p, side_effect, missing_ok):
        mock_batcher = AsyncMock()
        mock_batcher.delete = AsyncMock(return_value=None)
        mock_batcher.exists = AsyncMock(return_value=False)
        if side_effect:
            mock_batcher.delete = AsyncMock(side_effect=side_effect)
        with patch.object(GCSPath, "_get_batcher", return_value=mock_batcher):
            await p.unlink(missing_ok=missing_ok)

    @pytest.mark.parametrize(
        "exist_ok,exists_val,should_raise",
        [
            (True, False, False),
            (False, True, True),
        ],
        ids=["success", "raises_when_exists"],
    )
    async def test_touch(self, p, exist_ok, exists_val, should_raise):
        mock_batcher = AsyncMock()
        mock_batcher.put = AsyncMock(return_value=None)
        mock_batcher.exists = AsyncMock(return_value=exists_val)
        with patch.object(GCSPath, "_get_batcher", return_value=mock_batcher):
            if should_raise:
                with pytest.raises(FileExistsError):
                    await p.touch(exist_ok=exist_ok)
            else:
                await p.touch(exist_ok=exist_ok)

    async def test_rename(self, p):
        dest_str = "gs://bucket/object/renamed.txt"
        copy_fn = AsyncMock()
        mock_batcher = AsyncMock()
        mock_batcher.exists = AsyncMock(return_value=False)
        mock_batcher.delete = AsyncMock(return_value=None)
        with (
            patch.object(GCSPath, "_copy_batch_fn", copy_fn),
            patch.object(GCSPath, "_get_batcher", return_value=mock_batcher),
        ):
            result = await p.rename(dest_str)
        assert str(result) == dest_str
        copy_fn.assert_awaited_once()

    async def test_replace(self, p):
        dest_str = "gs://bucket/object/replaced.txt"
        copy_fn = AsyncMock()
        mock_batcher = AsyncMock()
        mock_batcher.exists = AsyncMock(return_value=False)
        mock_batcher.delete = AsyncMock(return_value=None)
        with (
            patch.object(GCSPath, "_copy_batch_fn", copy_fn),
            patch.object(GCSPath, "_get_batcher", return_value=mock_batcher),
        ):
            result = await p.replace(dest_str)
        assert str(result) == dest_str
        copy_fn.assert_awaited_once()

    async def test_stat(self, p):
        metadata = {
            "size": "1234",
            "updated": "2024-01-01T00:00:00Z",
        }
        with patch("asanypath.gcs.gcs_head", new=AsyncMock(return_value=metadata)):
            result = await p.stat()
            assert result.st_size == 1234
            assert result.st_mtime is not None

    async def test_mkdir(self, p):
        # mkdir is a no-op for GCS
        await p.mkdir()

    async def test_glob(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.list.return_value = [
            "gs://bucket/path/file.txt",
            "gs://bucket/path/other.md",
        ]
        with (
            patch.object(GCSPath, "_get_batcher", return_value=mock_batcher),
            patch("asanypath.gcs.gcs_is_dir", new=AsyncMock(return_value=False)),
        ):
            results = [c async for c in p.parent.glob("*.txt")]
        assert len(results) == 1

    async def test_rglob(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.list.return_value = [
            "gs://bucket/path/file.txt",
        ]
        mock_batcher.exists.return_value = False
        with (
            patch.object(GCSPath, "_get_batcher", return_value=mock_batcher),
            patch("asanypath.gcs.gcs_is_dir", new=AsyncMock(return_value=False)),
        ):
            results = [c async for c in p.parent.rglob("*.txt")]
        assert len(results) == 1

    async def test_walk(self):
        root = _make_gcs_path("gs://bucket/root")

        async def mock_list(item, **kwargs):
            if item == "root":
                return ["gs://bucket/root/file.txt", "gs://bucket/root/sub"]
            if item == "root/sub":
                return ["gs://bucket/root/sub/deep.txt"]
            return []

        mock_batcher = AsyncMock()
        mock_batcher.list = AsyncMock(side_effect=mock_list)

        async def mock_is_dir_batch(prefixes, **kwargs):
            return [p.endswith("sub") for p in prefixes]

        with (
            patch.object(GCSPath, "_get_batcher", return_value=mock_batcher),
            patch.object(
                type(root),
                "_is_dir_batch_fn",
                new=AsyncMock(side_effect=mock_is_dir_batch),
            ),
        ):
            entries = [(r, d[:], f[:]) async for r, d, f in root.walk()]

        assert entries[0][0] == root
        assert any(str(d) == "gs://bucket/root/sub" for d in entries[0][1])
        assert any(str(f) == "gs://bucket/root/file.txt" for f in entries[0][2])
        assert entries[1][0] == _make_gcs_path("gs://bucket/root/sub")
        assert any(str(f) == "gs://bucket/root/sub/deep.txt" for f in entries[1][2])


# ---------------------------------------------------------------------------
# Unit tests for init / properties
# ---------------------------------------------------------------------------


def test_protocol():
    p = _make_gcs_path()
    assert p.protocol == "gs"


def test_bucket_extracted():
    p = _make_gcs_path()
    assert p._bucket == "bucket"


def test_object_path_extracted():
    p = _make_gcs_path()
    assert p._object_path == "object/file.txt"


def test_project_id_from_env():
    import os

    os.environ["GCP_PROJECT"] = "envproject"
    try:
        p = GCSPath(
            "gs://bucket/object.txt",
            endpoint_url="http://localhost:4443",
        )
        assert p._project_id == "envproject"
    finally:
        del os.environ["GCP_PROJECT"]


def test_endpoint_url_stored():
    p = _make_gcs_path(endpoint_url="http://localhost:4443")
    assert p._endpoint_url == "http://localhost:4443"
