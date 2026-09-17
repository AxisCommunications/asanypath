# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for AzurePath implementation."""

from __future__ import annotations

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import msgspec
import pytest

from asanypath import AccessGrant, AccessPolicyPatch
from asanypath.azure import AzurePath
from tests.conftest import classtest_factory

testbase = classtest_factory("_TestAzurePath", AzurePath)


def _make_azure_path(
    path: str = "az://container/blob/file.txt",
    account_name: str = "devstoreaccount1",
    account_key: str = (
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    ),
    endpoint_url: str = "https://devstoreaccount1.blob.core.windows.net",
    **kwargs,
) -> AzurePath:
    """Return an AzurePath with test credentials."""
    defaults = dict(
        account_name=account_name,
        account_key=account_key,
        endpoint_url=endpoint_url,
    )
    defaults.update(kwargs)
    return AzurePath(path, **defaults)


class TestAzurePath(testbase):
    """Test the AzurePath implementation."""

    __test__ = True

    @pytest.fixture
    def p(self):
        """Fixture for a sample Azure path."""
        return _make_azure_path()

    async def test_checksums(self, p):
        headers = {"Content-MD5": "abc123base64=="}
        mock_batcher = MagicMock()
        mock_batcher.head = AsyncMock(return_value=headers)
        with (
            patch.object(AzurePath, "_get_batcher", return_value=mock_batcher),
            patch("asanypath.azure.az_is_dir", new=AsyncMock(return_value=False)),
        ):
            result = await p.checksums()
        assert result == {"md5": "abc123base64=="}

    async def test_get_access_policy_decodes_container_acl(self, p):
        native_acl = {
            "public_access": "blob",
            "signed_identifiers": [
                {"id": "report-readers", "permissions": "r", "expiry": "2026-12-31T00:00:00Z"}
            ],
        }
        with patch(
            "asanypath.azure.az_get_container_acl", new=AsyncMock(return_value=native_acl)
        ) as mock:
            policy = await p.get_access_policy()

        mock.assert_awaited_once_with(**p._native_kwargs)
        assert policy.grants == (AccessGrant("everyone", frozenset({"read"})),)
        assert policy.provider == {"azure_container_acl": native_acl}

    async def test_update_access_policy_replaces_native_acl(self, p):
        native_acl = {
            "public_access": "blob",
            "signed_identifiers": [
                {"id": "report-readers", "permissions": "r", "expiry": "2026-12-31T00:00:00Z"}
            ],
        }
        policy_patch = AccessPolicyPatch(provider={"azure_container_acl": native_acl})
        with patch("asanypath.azure.az_put_container_acl", new=AsyncMock()) as mock:
            await p.update_access_policy(policy_patch)

        mock.assert_awaited_once_with(
            acl_json=msgspec.json.encode(native_acl).decode(), **p._native_kwargs
        )

    @pytest.mark.parametrize(
        "return_value,expected",
        [(True, True), (False, False)],
        ids=["true", "false"],
    )
    async def test_exists(self, p, return_value, expected):
        mock_batcher = MagicMock()
        mock_batcher.exists = AsyncMock(return_value=return_value)
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
            assert await p.exists() is expected

    @pytest.mark.parametrize(
        "exists_val,is_dir_val,expected",
        [
            (True, False, True),
            (False, False, False),
            (True, True, False),
        ],
        ids=["true_when_exists_and_not_dir", "false_when_not_exists", "false_when_is_dir"],
    )
    async def test_is_file(self, p, exists_val, is_dir_val, expected):
        patches = [patch.object(p, "exists", new=AsyncMock(return_value=exists_val))]
        if exists_val:
            patches.append(patch.object(p, "is_dir", new=AsyncMock(return_value=is_dir_val)))
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
        d = _make_azure_path("az://container/blob")
        with patch("asanypath.azure.az_is_dir", new=AsyncMock(return_value=return_value)):
            assert await d.is_dir() is expected

    async def test_iterdir(self):
        d = _make_azure_path("az://container/blob")
        mock_batcher = MagicMock()
        mock_batcher.list = AsyncMock(
            return_value=[
                "az://container/blob/file.txt",
                "az://container/blob/other.txt",
            ]
        )
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
            results = [x async for x in d.iterdir(fresh=True)]
        assert any(str(x) == "az://container/blob/file.txt" for x in results)
        assert any(str(x) == "az://container/blob/other.txt" for x in results)
        assert all(isinstance(x, AzurePath) for x in results)

    async def test_iterdir_raises_not_a_directory(self):
        d = _make_azure_path("az://container/nodir")
        mock_batcher = MagicMock()
        mock_batcher.list = AsyncMock(side_effect=FileNotFoundError(2, ""))
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
            with pytest.raises(NotADirectoryError):
                async for _ in d.iterdir(fresh=True):
                    pass

    async def test_list_containers_at_root(self):
        root = _make_azure_path("az://")
        with patch(
            "asanypath.azure.az_list_containers",
            new=AsyncMock(return_value=["alpha", "beta"]),
        ):
            assert await root._list_containers() == ["az://alpha", "az://beta"]

    async def test_list_containers_none_when_container_set(self, p):
        with patch("asanypath.azure.az_list_containers", new=AsyncMock()) as mock:
            assert await p._list_containers() is None
        mock.assert_not_called()

    async def test_iterdir_lists_containers_at_root(self):
        root = _make_azure_path("az://")
        with patch(
            "asanypath.azure.az_list_containers",
            new=AsyncMock(return_value=["alpha", "beta"]),
        ):
            results = [str(x) async for x in root.iterdir(fresh=True)]
        assert results == ["az://alpha", "az://beta"]

    async def test_read_bytes(self, p):
        data = b"\xde\xad\xbe\xef"
        mock_batcher = MagicMock()
        mock_batcher.get = AsyncMock(return_value=data)
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
            assert await p.read_bytes() == data

    async def test_read_text(self, p):
        mock_batcher = MagicMock()
        mock_batcher.get = AsyncMock(return_value=b"hello")
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
            assert await p.read_text() == "hello"

    async def test_write_bytes(self, p):
        mock_batcher = MagicMock()
        mock_batcher.put = AsyncMock(return_value=None)
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
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
        mock_batcher = MagicMock()
        mock_batcher.delete = AsyncMock(return_value=None)
        if side_effect:
            mock_batcher.delete = AsyncMock(side_effect=side_effect)
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
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
        with (
            patch.object(p, "write_bytes", new=AsyncMock(return_value=0)),
            patch.object(p, "exists", new=AsyncMock(return_value=exists_val)),
        ):
            if should_raise:
                with pytest.raises(FileExistsError):
                    await p.touch(exist_ok=exist_ok)
            else:
                await p.touch(exist_ok=exist_ok)

    async def test_rename(self, p):
        dest_str = "az://container/blob/renamed.txt"
        copy_fn = AsyncMock()
        with (
            patch.object(AzurePath, "_copy_batch_fn", copy_fn),
            patch.object(AzurePath, "exists", new=AsyncMock(return_value=False)),
            patch.object(p, "unlink", new=AsyncMock()),
        ):
            result = await p.rename(dest_str)
        assert str(result) == dest_str
        copy_fn.assert_awaited_once()

    async def test_replace(self, p):
        dest_str = "az://container/blob/replaced.txt"
        copy_fn = AsyncMock()
        with (
            patch.object(AzurePath, "_copy_batch_fn", copy_fn),
            patch.object(AzurePath, "exists", new=AsyncMock(return_value=False)),
            patch.object(p, "unlink", new=AsyncMock()),
        ):
            result = await p.replace(dest_str)
        assert str(result) == dest_str
        copy_fn.assert_awaited_once()

    async def test_stat(self, p):
        headers = {
            "Content-Length": "1234",
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        }
        mock_batcher = MagicMock()
        mock_batcher.head = AsyncMock(return_value=headers)
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
            result = await p.stat()
            assert result.st_size == 1234
            assert result.st_mtime is not None

    async def test_mkdir(self, p):
        # mkdir is a no-op for Azure
        await p.mkdir()

    async def test_rmdir(self, p):
        # Non-recursive rmdir on non-empty dir raises OSError
        mock_batcher = AsyncMock()
        mock_batcher.list.return_value = ["az://container/blob/child.txt"]
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
            with pytest.raises(OSError, match="not empty"):
                await p.rmdir()

    async def test_glob(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.list.return_value = [
            "az://container/path/file.txt",
            "az://container/path/other.md",
        ]
        with patch.object(AzurePath, "_get_batcher", return_value=mock_batcher):
            results = [c async for c in p.parent.glob("*.txt")]
        assert len(results) == 1

    async def test_rglob(self, p):
        mock_batcher = AsyncMock()
        mock_batcher.list.return_value = [
            "az://container/path/file.txt",
        ]
        mock_batcher.exists.return_value = False
        with (
            patch.object(AzurePath, "_get_batcher", return_value=mock_batcher),
            patch("asanypath.azure.az_is_dir", new=AsyncMock(return_value=False)),
        ):
            results = [c async for c in p.parent.rglob("*.txt")]
        assert len(results) == 1

    async def test_open(self, p):
        cf = p.open()
        assert hasattr(cf, "__aenter__")
        assert hasattr(cf, "__enter__")

    async def test_walk(self):
        root = _make_azure_path("az://container/root")

        mock_batcher = MagicMock()

        async def _list_side_effect(item, **kwargs):
            if item == "root":
                return [
                    "az://container/root/file.txt",
                    "az://container/root/sub",
                ]
            elif item == "root/sub":
                return ["az://container/root/sub/deep.txt"]
            return []

        mock_batcher.list = _list_side_effect

        async def _is_dir_batch_side_effect(prefixes, **kwargs):
            return [p.endswith("sub") for p in prefixes]

        with (
            patch.object(AzurePath, "_get_batcher", return_value=mock_batcher),
            patch.object(type(root), "_is_dir_batch_fn", side_effect=_is_dir_batch_side_effect),
        ):
            entries = [(r, d[:], f[:]) async for r, d, f in root.walk()]

        assert entries[0][0] == root
        assert any(str(d) == "az://container/root/sub" for d in entries[0][1])
        assert any(str(f) == "az://container/root/file.txt" for f in entries[0][2])
        assert any(str(e[0]) == "az://container/root/sub" for e in entries[1:])
        assert any(str(f) == "az://container/root/sub/deep.txt" for e in entries[1:] for f in e[2])


# ---------------------------------------------------------------------------
# Unit tests for init / properties
# ---------------------------------------------------------------------------


def test_protocol():
    p = _make_azure_path()
    assert p.protocol == "az"


def test_container_extracted():
    p = _make_azure_path()
    assert p._container == "container"


def test_blob_path_extracted():
    p = _make_azure_path()
    assert p._blob_path == "blob/file.txt"


def test_account_name_from_env():
    import os

    os.environ["AZURE_STORAGE_ACCOUNT"] = "envaccount"
    try:
        p = AzurePath(
            "az://container/blob.txt",
            account_key="key",
            endpoint_url="http://localhost:10000",
        )
        assert p._account_name == "envaccount"
    finally:
        del os.environ["AZURE_STORAGE_ACCOUNT"]


def test_endpoint_url_stored():
    p = _make_azure_path(endpoint_url="http://localhost:10000/devstoreaccount1")
    assert p._endpoint_url == "http://localhost:10000/devstoreaccount1"


# ---------------------------------------------------------------------------
# AzurePath presign
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_presign_with_account_key():
    p = _make_azure_path("az://container/blob/file.txt")
    url = await p.presign(expires=3600)
    assert "container/blob/file.txt" in url
    assert "sv=" in url
    assert "sr=b" in url
    assert "sp=r" in url
    assert "sig=" in url
    assert "st=" in url
    assert "se=" in url


@pytest.mark.asyncio
async def test_presign_with_existing_sas():
    p = _make_azure_path(
        "az://container/blob/file.txt",
        account_key=None,
        sas_token="sv=2023-11-03&sr=c&sig=abc123",
    )
    url = await p.presign()
    assert url.endswith("?sv=2023-11-03&sr=c&sig=abc123")


@pytest.mark.asyncio
async def test_presign_no_credentials_raises(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_KEY", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_SAS_TOKEN", raising=False)
    p = _make_azure_path("az://container/blob/file.txt", account_key=None)
    with pytest.raises(ValueError, match="presign requires"):
        await p.presign()
