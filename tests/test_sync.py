# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for asanypath.sync module."""

from __future__ import annotations

import pytest

from asanypath.exceptions import UnsupportedProtocolError
from asanypath.local import SyncPath
from asanypath.sync import AsAnyPath, _SyncProxy
from asanypath.unsupported import UnsupportedProtocolPath


class TestSyncLocal:
    """Local paths should return SyncPath directly (no proxy)."""

    def test_no_argument_path_is_current_directory(self):
        assert str(AsAnyPath()) == "."

    def test_local_returns_syncpath(self, tmp_path):
        p = AsAnyPath(str(tmp_path / "file.txt"))
        assert isinstance(p, SyncPath)

    def test_local_is_an_asanypath_instance(self, tmp_path):
        p = AsAnyPath(str(tmp_path / "file.txt"))
        assert isinstance(p, AsAnyPath)

    def test_local_read_write(self, tmp_path):
        target = tmp_path / "hello.txt"
        target.write_text("hello sync")
        p = AsAnyPath(str(target))
        assert p.read_text() == "hello sync"

    def test_local_iterdir(self, tmp_path):
        (tmp_path / "a.txt").touch()
        (tmp_path / "b.txt").touch()
        p = AsAnyPath(str(tmp_path))
        names = sorted(child.name for child in p.iterdir())
        assert names == ["a.txt", "b.txt"]

    def test_local_sorted(self, tmp_path):
        (tmp_path / "c.txt").touch()
        (tmp_path / "a.txt").touch()
        (tmp_path / "b.txt").touch()
        p = AsAnyPath(str(tmp_path))
        children = sorted(p.iterdir())
        assert [c.name for c in children] == ["a.txt", "b.txt", "c.txt"]


class TestSyncProxy:
    """Cloud paths should return a _SyncProxy with blocking methods."""

    def test_s3_returns_proxy(self):
        p = AsAnyPath("s3://bucket/key.txt")
        assert isinstance(p, _SyncProxy)

    def test_cloud_is_an_asanypath_instance(self):
        p = AsAnyPath("s3://bucket/key.txt")
        assert isinstance(p, AsAnyPath)

    def test_sync_factory_forwards_kwargs_to_backend(self):
        p = AsAnyPath(
            "s3://bucket/key.txt",
            endpoint_url="http://minio:9000",
            aws_region="us-east-1",
            aws_access_key_id="x",
            aws_secret_access_key="y",
        )
        assert isinstance(p, _SyncProxy)
        async_obj = object.__getattribute__(p, "_async")
        assert async_obj._endpoint_url == "http://minio:9000"
        assert async_obj._region == "us-east-1"
        assert async_obj._access_key == "x"
        assert async_obj._secret_key == "y"

    def test_proxy_str(self):
        p = AsAnyPath("s3://bucket/key.txt")
        assert str(p) == "s3://bucket/key.txt"

    def test_proxy_name(self):
        p = AsAnyPath("s3://bucket/key.txt")
        assert p.name == "key.txt"

    def test_proxy_sorted(self):
        paths = [
            AsAnyPath("s3://bucket/c.txt"),
            AsAnyPath("s3://bucket/a.txt"),
            AsAnyPath("s3://bucket/b.txt"),
        ]
        assert [str(p) for p in sorted(paths)] == [
            "s3://bucket/a.txt",
            "s3://bucket/b.txt",
            "s3://bucket/c.txt",
        ]

    def test_proxy_parent(self):
        p = AsAnyPath("s3://bucket/dir/key.txt")
        parent = p.parent
        assert isinstance(parent, _SyncProxy)
        assert str(parent) == "s3://bucket/dir"

    def test_proxy_truediv(self):
        p = AsAnyPath("s3://bucket/dir")
        child = p / "file.txt"
        assert isinstance(child, _SyncProxy)
        assert child.name == "file.txt"

    def test_proxy_hash(self):
        a = AsAnyPath("s3://bucket/key")
        b = AsAnyPath("s3://bucket/key")
        assert hash(a) == hash(b)
        assert {a, b} == {a}

    def test_proxy_eq(self):
        a = AsAnyPath("s3://bucket/key")
        b = AsAnyPath("s3://bucket/key")
        assert a == b

    def test_proxy_parts(self):
        p = AsAnyPath("gs://bucket/a/b/c.txt")
        assert p.stem == "c"
        assert p.suffix == ".txt"

    @pytest.fixture
    def mock_s3(self, monkeypatch):
        """Patch S3Path async methods for testing without network."""
        from asanypath.s3 import S3Path

        async def _exists(self):
            return True

        async def _read_bytes(self):
            return b"mock data"

        async def _write_bytes(self, data):
            return len(data)

        async def _iterdir(self, **kwargs):
            yield S3Path("s3://bucket/a.txt")
            yield S3Path("s3://bucket/b.txt")

        monkeypatch.setattr(S3Path, "exists", _exists)
        monkeypatch.setattr(S3Path, "read_bytes", _read_bytes)
        monkeypatch.setattr(S3Path, "write_bytes", _write_bytes)
        monkeypatch.setattr(S3Path, "iterdir", _iterdir)

    def test_sync_exists(self, mock_s3):
        p = AsAnyPath("s3://bucket/key.txt")
        assert p.exists() is True

    def test_sync_read_bytes(self, mock_s3):
        p = AsAnyPath("s3://bucket/key.txt")
        assert p.read_bytes() == b"mock data"

    def test_sync_write_bytes(self, mock_s3):
        p = AsAnyPath("s3://bucket/key.txt")
        assert p.write_bytes(b"hello") == 5

    def test_sync_iterdir(self, mock_s3):
        p = AsAnyPath("s3://bucket")
        names = [child.name for child in p.iterdir()]
        assert names == ["a.txt", "b.txt"]

    def test_iterdir_returns_proxies(self, mock_s3):
        p = AsAnyPath("s3://bucket")
        for child in p.iterdir():
            assert isinstance(child, _SyncProxy)


class TestSyncUnsupportedProtocol:
    def test_unknown_protocol_returns_placeholder(self):
        p = AsAnyPath("nope://server/file.txt")
        assert isinstance(p, UnsupportedProtocolPath)

    def test_unknown_protocol_raises_on_use(self):
        p = AsAnyPath("nope://server/file.txt")
        with pytest.raises(UnsupportedProtocolError, match="nope"):
            p.read_bytes()
