# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import msgspec
import pytest

from asanypath import AccessGrant, AccessPolicyPatch
from asanypath.s3 import S3Path
from tests.conftest import classtest_factory

testbase = classtest_factory("_TestS3Path", S3Path)


def s3_path(bucket="mybucket", folder="nested", file="file.txt"):
    return f"s3://{bucket}/{folder}/{file}"


class TestS3Path(testbase):
    __test__ = True

    @pytest.fixture
    def p(self):
        return _make_s3path(s3_path())

    # --- Async I/O tests (alphabetical) ---

    async def test_checksums(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.head.return_value = {"ETag": '"abc123"'}
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            result = await p.checksums()
        assert result == {"md5": "abc123"}

    @pytest.mark.parametrize("as_json", [False, True], ids=["mapping", "json"])
    async def test_get_access_policy_decodes_native_acl(self, p, as_json):
        native_acl = {
            "owner": "owner-id",
            "grants": [
                {"principal": "canonical-user:reader-id", "permission": "READ"},
                {"principal": "uri:AllUsers", "permission": "READ_ACP"},
            ],
        }
        response = msgspec.json.encode(native_acl).decode() if as_json else native_acl
        with patch("asanypath.s3.s3_get_acl", new=AsyncMock(return_value=response)) as mock:
            policy = await p.get_access_policy()

        mock.assert_awaited_once_with(key="nested/file.txt", **p._native_kwargs)
        assert policy.owner == "owner-id"
        assert policy.grants == (AccessGrant("canonical-user:reader-id", frozenset({"read"})),)
        assert policy.provider == {"s3_acl": native_acl}

    async def test_get_access_policy_rejects_malformed_native_acl(self, p):
        with patch("asanypath.s3.s3_get_acl", new=AsyncMock(return_value=[])):
            with pytest.raises(TypeError, match="must be a mapping"):
                await p.get_access_policy()

    async def test_update_access_policy_replaces_native_acl(self, p):
        native_acl = {
            "owner": "owner-id",
            "grants": [{"principal": "canonical-user:reader-id", "permission": "READ"}],
        }
        policy_patch = AccessPolicyPatch(provider={"s3_acl": native_acl})
        with patch("asanypath.s3.s3_put_acl", new=AsyncMock()) as mock:
            await p.update_access_policy(policy_patch)

        mock.assert_awaited_once_with(
            key="nested/file.txt",
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
        mock_batcher.exists.return_value = return_value
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            assert await p.exists() is expected

    async def test_glob(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.list.return_value = [
            "s3://mybucket/nested/file.txt",
            "s3://mybucket/nested/other.md",
        ]
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            results = [c async for c in p.parent.glob("*.txt")]
        assert len(results) == 1
        assert str(results[0]).endswith("file.txt")

    @pytest.mark.parametrize(
        "path,is_dir_result,expected",
        [
            ("s3://mybucket/nested", True, True),
            ("s3://mybucket/nested", False, False),
            (s3_path(), False, False),
        ],
        ids=["true_when_keys_on_prefix", "false_when_empty", "false_when_exact_key"],
    )
    async def test_is_dir(self, path, is_dir_result, expected):
        p = _make_s3path(path)
        with patch("asanypath.s3.s3_is_dir", new=AsyncMock(return_value=is_dir_result)):
            assert await p.is_dir() is expected

    @pytest.mark.parametrize(
        "mocks,expected",
        [
            ((("exists", True), ("is_dir", True)), False),
            ((("exists", False),), False),
            ((("exists", True), ("is_dir", False)), True),
        ],
        ids=["false_when_is_dir", "false_when_not_exists", "true_when_exists_and_not_dir"],
    )
    async def test_is_file(self, p, mocks, expected):
        patched = [
            patch.object(p, method, new=AsyncMock(return_value=retval)).__enter__()
            for method, retval in mocks
        ]
        assert await p.is_file() is expected
        for m in patched:
            m.__exit__(None, None, None)

    @pytest.mark.parametrize(
        "dir_path,list_return,expected_strs",
        [
            (
                "s3://mybucket/nested",
                ["s3://mybucket/nested/file.txt", "s3://mybucket/nested/other.txt"],
                ["s3://mybucket/nested/file.txt", "s3://mybucket/nested/other.txt"],
            ),
            (
                "s3://mybucket/root",
                ["s3://mybucket/root/subdir"],
                ["s3://mybucket/root/subdir"],
            ),
            (
                "s3://mybucket/root",
                ["s3://mybucket/root/file.txt"],
                ["s3://mybucket/root/file.txt"],
            ),
            (
                "s3://mybucket/pg",
                ["s3://mybucket/pg/file1.txt", "s3://mybucket/pg/file2.txt"],
                ["s3://mybucket/pg/file1.txt", "s3://mybucket/pg/file2.txt"],
            ),
        ],
        ids=["basic", "prefixes", "excludes_self", "pagination"],
    )
    async def test_iterdir(self, dir_path, list_return, expected_strs):
        d = _make_s3path(dir_path)
        mock_batcher = AsyncMock()
        mock_batcher.list.return_value = list_return
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            results = [str(x) async for x in d.iterdir()]
        for expected in expected_strs:
            assert expected in results
        assert dir_path not in results or dir_path in expected_strs
        assert all(isinstance(x, S3Path) for x in [_make_s3path(r) for r in results])

    async def test_iterdir_raises_not_a_directory(self):
        d = _make_s3path("s3://mybucket/nodir")
        mock_batcher = AsyncMock()
        mock_batcher.list.side_effect = FileNotFoundError(2, "No such file")
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            with pytest.raises(NotADirectoryError):
                async for _ in d.iterdir():
                    pass

    async def test_list_containers_at_root(self):
        root = _make_s3path("s3://")
        with patch(
            "asanypath.s3.s3_list_buckets",
            new=AsyncMock(return_value=["alpha", "beta"]),
        ):
            assert await root._list_containers() == ["s3://alpha", "s3://beta"]

    async def test_list_containers_none_when_bucket_set(self, p):
        with patch("asanypath.s3.s3_list_buckets", new=AsyncMock()) as mock:
            assert await p._list_containers() is None
        mock.assert_not_called()

    async def test_iterdir_lists_buckets_at_root(self):
        root = _make_s3path("s3://")
        with patch(
            "asanypath.s3.s3_list_buckets",
            new=AsyncMock(return_value=["alpha", "beta"]),
        ):
            results = [str(x) async for x in root.iterdir()]
        assert results == ["s3://alpha", "s3://beta"]

    async def test_mkdir(self, p):
        # Cloud backends treat mkdir as a no-op (virtual prefixes)
        await p.mkdir()

    async def test_read_bytes(self, p):
        data = b"\xde\xad\xbe\xef"
        mock_batcher = AsyncMock()
        mock_batcher.get.return_value = data
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            assert await p.read_bytes() == data

    async def test_read_text(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.get.return_value = b"hello"
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            assert await p.read_text() == "hello"

    async def test_rename(self, p):
        dest_str = "s3://mybucket/nested/renamed.txt"
        copy_fn = AsyncMock()
        with (
            patch.object(S3Path, "_copy_batch_fn", copy_fn),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
            patch.object(p, "unlink", new=AsyncMock()),
        ):
            result = await p.rename(dest_str)
        assert str(result) == dest_str
        copy_fn.assert_awaited_once()

    async def test_replace(self, p):
        dest_str = "s3://mybucket/nested/replaced.txt"
        copy_fn = AsyncMock()
        with (
            patch.object(S3Path, "_copy_batch_fn", copy_fn),
            patch.object(S3Path, "exists", new=AsyncMock(return_value=False)),
            patch.object(p, "unlink", new=AsyncMock()),
        ):
            result = await p.replace(dest_str)
        assert str(result) == dest_str
        copy_fn.assert_awaited_once()

    async def test_rglob(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.list.return_value = [
            "s3://mybucket/nested/file.txt",
        ]
        with (
            patch.object(S3Path, "_get_batcher", return_value=mock_batcher),
            patch("asanypath.s3.s3_is_dir", new=AsyncMock(return_value=False)),
        ):
            results = [c async for c in p.parent.rglob("*.txt")]
        assert len(results) == 1

    async def test_stat(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.head.return_value = {
            "Content-Length": "1234",
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        }
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            result = await p.stat()
            assert result.st_size == 1234
            assert result.st_mtime is not None

    async def test_touch(self, p):
        with patch.object(p, "write_bytes", new=AsyncMock(return_value=0)):
            await p.touch()

    @pytest.mark.parametrize(
        "side_effect,missing_ok",
        [
            (None, False),
            (FileNotFoundError(2, "No such file"), True),
        ],
        ids=["success", "missing_ok"],
    )
    async def test_unlink(self, p, side_effect, missing_ok):
        mock_batcher = AsyncMock()
        mock_batcher.delete.return_value = None
        if side_effect:
            mock_batcher.delete.side_effect = side_effect
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            await p.unlink(missing_ok=missing_ok)

    async def test_walk(self):
        root = _make_s3path("s3://mybucket/root")
        mock_batcher = AsyncMock()
        mock_batcher.list.side_effect = [
            ["s3://mybucket/root/file.txt", "s3://mybucket/root/sub"],
            ["s3://mybucket/root/sub/deep.txt"],
        ]
        with (
            patch.object(S3Path, "_get_batcher", return_value=mock_batcher),
            patch.object(
                type(root),
                "_is_dir_batch_fn",
                new=AsyncMock(
                    side_effect=[
                        [False, True],
                        [False],
                    ]
                ),
            ),
        ):
            entries = [(root_p, dirs[:], files[:]) async for root_p, dirs, files in root.walk()]

        assert entries[0][0] == root
        assert any(str(d) == "s3://mybucket/root/sub" for d in entries[0][1])
        assert any(str(f) == "s3://mybucket/root/file.txt" for f in entries[0][2])
        assert entries[1][0] == _make_s3path("s3://mybucket/root/sub")
        assert any(str(f) == "s3://mybucket/root/sub/deep.txt" for f in entries[1][2])

    async def test_walk_empty_dir(self):
        d = _make_s3path("s3://mybucket/empty")
        mock_batcher = AsyncMock()
        mock_batcher.list.return_value = []
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            entries = [(root_p, dirs, files) async for root_p, dirs, files in d.walk()]

        assert len(entries) == 1
        assert entries[0][1] == []
        assert entries[0][2] == []

    async def test_write_bytes(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.put.return_value = None
        with patch.object(S3Path, "_get_batcher", return_value=mock_batcher):
            result = await p.write_bytes(b"data")
        assert result == 4
        mock_batcher.put.assert_called_once()

    async def test_write_text(self, p):
        written = []

        async def _mock_write_bytes(data):
            written.append(data)
            return len(data)

        with patch.object(p, "write_bytes", side_effect=_mock_write_bytes):
            n = await p.write_text("hello")
        assert b"hello" in written
        assert n == len(b"hello")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_s3path(path: str = "s3://mybucket/mykey", **kwargs) -> S3Path:
    """Return an S3Path with known credentials so tests are self-contained."""
    defaults = dict(
        endpoint_url="https://s3.amazonaws.com",
        aws_region="us-east-1",
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        aws_session_token=None,
    )
    defaults.update(kwargs)
    return S3Path(path, **defaults)


# ---------------------------------------------------------------------------
# S3Path initialisation
# ---------------------------------------------------------------------------


def test_protocol():
    p = _make_s3path("s3://bucket/key")
    assert p.protocol == "s3"


def test_bucket_stored():
    p = _make_s3path("s3://mybucket/some/key")
    assert p._bucket == "mybucket"


def test_auth_params_stored():
    p = _make_s3path(
        "s3://b/k",
        aws_region="eu-west-1",
        aws_access_key_id="KEYID",
        aws_secret_access_key="SECRET",
    )
    assert p._region == "eu-west-1"
    assert p._access_key == "KEYID"
    assert p._secret_key == "SECRET"


def test_path_join_via_truediv():
    p = _make_s3path("s3://bucket/prefix")
    child = p / "subdir/file.txt"
    assert str(child) == "s3://bucket/prefix/subdir/file.txt"


def test_credentials_from_env():
    env = {
        "AWS_REGION": "ap-southeast-1",
        "AWS_ACCESS_KEY_ID": "ENVKEY",
        "AWS_SECRET_ACCESS_KEY": "ENVSECRET",
        "AWS_PROFILE": "default",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_CONFIG_FILE": "/dev/null",
    }
    with (
        patch("asanypath.s3.getenv", side_effect=lambda k, d=None: env.get(k, d)),
        patch("asanypath.s3.ConfigParser.read", return_value=[]),
    ):
        S3Path._env_config = None
        p = S3Path("s3://bucket/key")
    assert p._region == "ap-southeast-1"


# ---------------------------------------------------------------------------
# S3Path presign
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_presign_returns_url_with_signature():
    p = _make_s3path("s3://mybucket/path/to/file.txt")
    url = await p.presign(expires=3600)
    assert url.startswith("https://s3.amazonaws.com/mybucket/path/to/file.txt?")
    assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url
    assert "X-Amz-Credential=" in url
    assert "X-Amz-Expires=3600" in url
    assert "X-Amz-Signature=" in url
    assert "X-Amz-SignedHeaders=host" in url


@pytest.mark.asyncio
async def test_presign_with_session_token():
    p = _make_s3path("s3://mybucket/key", aws_session_token="TOKENABC")
    url = await p.presign(expires=900)
    assert "X-Amz-Security-Token=TOKENABC" in url
    assert "X-Amz-Expires=900" in url


@pytest.mark.asyncio
async def test_presign_put_method():
    p = _make_s3path("s3://mybucket/upload.bin")
    url = await p.presign(method="PUT")
    assert "X-Amz-Signature=" in url


@pytest.mark.asyncio
async def test_presign_invalid_expires():
    p = _make_s3path("s3://mybucket/key")
    with pytest.raises(ValueError, match="between 1 and 604800"):
        await p.presign(expires=0)
    with pytest.raises(ValueError, match="between 1 and 604800"):
        await p.presign(expires=700000)
