# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

from asanypath import AccessGrant, AccessPolicyPatch, AsyncPath, BackendOptions
from asanypath.local import SyncPath
from tests.conftest import classtest_factory

testbase = classtest_factory("_TestAsyncPath", AsyncPath)


@pytest.mark.asyncio
async def test_async_access_policy_reflects_posix_mode(tmp_path):
    path = AsyncPath(tmp_path / "policy.txt")
    await path.write_text("content")
    await path.chmod(0o640)

    policy = await path.get_access_policy()

    assert policy.owner == await path.owner()
    assert policy.group == await path.group()
    assert policy.grants == (
        AccessGrant("owner", frozenset({"read", "write"})),
        AccessGrant("group", frozenset({"read"})),
        AccessGrant("everyone", frozenset()),
    )


@pytest.mark.asyncio
async def test_async_update_access_policy_updates_mode(tmp_path):
    path = AsyncPath(tmp_path / "policy.txt")
    await path.write_text("content")
    await path.chmod(0o600)

    await path.update_access_policy(
        AccessPolicyPatch(
            grants=(
                AccessGrant("group", frozenset({"read"})),
                AccessGrant("everyone", frozenset({"read"})),
            )
        )
    )

    assert (await path.stat()).st_mode & 0o777 == 0o644


@pytest.mark.asyncio
async def test_async_access_policy_honors_backend_options(tmp_path):
    target = tmp_path / "target.txt"
    target.write_text("content")
    target.chmod(0o600)
    link = tmp_path / "policy-link.txt"
    link.symlink_to(target)

    policy = await AsyncPath(link).get_access_policy(
        backend_options=BackendOptions(provider={"follow_symlinks": False})
    )

    assert policy.grants[0] == AccessGrant("owner", frozenset({"read", "write", "execute"}))


def test_sync_update_access_policy_updates_mode(tmp_path):
    path = SyncPath(tmp_path / "policy.txt")
    path.write_text("content")
    path.chmod(0o600)

    path.update_access_policy(
        AccessPolicyPatch(grants=(AccessGrant("everyone", frozenset({"read"})),))
    )

    assert path.stat().st_mode & 0o777 == 0o604


class _AsyncRemoteDestination:
    protocol = "s3"

    def __init__(self, *, exists: bool = False, checksum: str = "remote"):
        self.parent = type("Parent", (), {"mkdir": AsyncMock()})()
        self.exists = AsyncMock(return_value=exists)
        self.is_dir = AsyncMock(return_value=False)
        self.checksums = AsyncMock(return_value={"sha256": checksum})
        self.read_bytes = AsyncMock(return_value=b"remote")
        self.write_bytes = AsyncMock(return_value=7)
        self.children: dict[str, _AsyncRemoteDestination] = {}

    def __str__(self):
        return "s3://bucket/key.txt"

    def __truediv__(self, other):
        return self.children.setdefault(str(other), _AsyncRemoteDestination())


class _SyncRemoteDestination:
    protocol = "s3"

    def __init__(self, *, exists: bool = False, checksum: str = "remote"):
        self.parent = type("Parent", (), {"mkdir": Mock()})()
        self.exists = Mock(return_value=exists)
        self.is_dir = Mock(return_value=False)
        self.checksums = Mock(return_value={"sha256": checksum})
        self.read_bytes = Mock(return_value=b"remote")
        self.write_bytes = Mock(return_value=7)
        self.children: dict[str, _SyncRemoteDestination] = {}

    def __str__(self):
        return "s3://bucket/key.txt"

    def __truediv__(self, other):
        return self.children.setdefault(str(other), _SyncRemoteDestination())


async def test_async_copy_requires_force_for_different_existing_file(tmp_path):
    src = AsyncPath(tmp_path / "src.txt")
    dst = AsyncPath(tmp_path / "dst.txt")
    await src.write_bytes(b"source")
    await dst.write_bytes(b"destination")

    with pytest.raises(FileExistsError):
        await src.copy(dst)
    assert await dst.read_bytes() == b"destination"
    await src.copy(dst, force=True)
    assert await dst.read_bytes() == b"source"


def test_sync_copy_skips_identical_existing_file(tmp_path):
    src = SyncPath(tmp_path / "src.txt")
    dst = SyncPath(tmp_path / "dst.txt")
    src.write_bytes(b"same")
    dst.write_bytes(b"same")

    assert src.copy(dst) == dst
    assert dst.read_bytes() == b"same"


def test_sync_copy_atomic_preserves_existing_destination_on_commit_failure(tmp_path, monkeypatch):
    src = SyncPath(tmp_path / "src.txt")
    dst = SyncPath(tmp_path / "dst.txt")
    src.write_bytes(b"new")
    dst.write_bytes(b"old")

    def fail_replace(self, target):
        raise OSError("commit failed")

    monkeypatch.setattr(Path, "replace", fail_replace)

    with pytest.raises(OSError, match="commit failed"):
        src.copy(dst, atomic=True, force=True)

    assert dst.read_bytes() == b"old"
    assert not list(tmp_path.glob(".dst.txt.tmp-*"))


def test_sync_copy_atomic_no_overwrite_on_race(tmp_path, monkeypatch):
    src = SyncPath(tmp_path / "src.txt")
    dst = SyncPath(tmp_path / "dst.txt")
    src.write_bytes(b"new")

    def race_link(self, target):
        Path(target).write_bytes(b"race")
        raise FileExistsError

    monkeypatch.setattr(os, "link", race_link)

    with pytest.raises(FileExistsError):
        src.copy(dst, atomic=True)

    assert dst.read_bytes() == b"race"
    assert not list(tmp_path.glob(".dst.txt.tmp-*"))


def test_sync_copy_chunk_size_streams_local_file(tmp_path):
    src = SyncPath(tmp_path / "src.txt")
    dst = SyncPath(tmp_path / "dst.txt")
    src.write_bytes(b"abcdef")

    src.copy(dst, chunk_size=2)

    assert dst.read_bytes() == b"abcdef"


def test_sync_copy_chunk_size_none_streams_not_copy2(tmp_path, monkeypatch):
    from asanypath import local as local_mod

    src = SyncPath(tmp_path / "src.txt")
    dst = SyncPath(tmp_path / "dst.txt")
    src.write_bytes(b"abcdef")

    def fail_copy2(*args, **kwargs):
        raise AssertionError("shutil.copy2 must not run for chunk_size=None (auto streaming)")

    monkeypatch.setattr(local_mod.shutil, "copy2", fail_copy2)

    src.copy(dst)

    assert dst.read_bytes() == b"abcdef"


def test_sync_copy_chunk_size_zero_disables_streaming(tmp_path, monkeypatch):
    from asanypath import local as local_mod

    src = SyncPath(tmp_path / "src.txt")
    dst = SyncPath(tmp_path / "dst.txt")
    src.write_bytes(b"abcdef")

    calls: list[tuple] = []
    real_copy2 = local_mod.shutil.copy2

    def spy_copy2(a, b, *args, **kwargs):
        calls.append((a, b))
        return real_copy2(a, b, *args, **kwargs)

    monkeypatch.setattr(local_mod.shutil, "copy2", spy_copy2)

    src.copy(dst, chunk_size=0)

    assert len(calls) == 1
    assert dst.read_bytes() == b"abcdef"


def test_sync_recursive_self_move_does_not_delete_source(tmp_path):
    src = SyncPath(tmp_path / "source")
    src.mkdir()
    (src / "file.txt").write_bytes(b"content")

    src.copy(src, recursive=True, remove_src=True)

    assert (src / "file.txt").read_bytes() == b"content"


async def test_async_copy_to_cloud_destination_uses_destination_backend(tmp_path):
    src = AsyncPath(tmp_path / "src.txt")
    await src.write_bytes(b"payload")
    dst = _AsyncRemoteDestination()

    copied = await src.copy(dst)

    assert copied is dst
    dst.write_bytes.assert_awaited_once_with(b"payload")
    assert not (tmp_path / "s3:").exists()


async def test_async_copy_to_identical_cloud_destination_skips_write(tmp_path):
    src = AsyncPath(tmp_path / "src.txt")
    await src.write_bytes(b"payload")
    dst = _AsyncRemoteDestination(
        exists=True,
        checksum=(await src.checksums())["sha256"],
    )

    copied = await src.copy(dst)

    assert copied is dst
    dst.write_bytes.assert_not_awaited()


async def test_async_copy_to_different_cloud_destination_requires_force(tmp_path):
    src = AsyncPath(tmp_path / "src.txt")
    await src.write_bytes(b"payload")
    dst = _AsyncRemoteDestination(exists=True, checksum="different")

    with pytest.raises(FileExistsError):
        await src.copy(dst)

    dst.write_bytes.assert_not_awaited()
    await src.copy(dst, force=True)
    dst.write_bytes.assert_awaited_once_with(b"payload")


async def test_async_copy_atomic_preserves_existing_destination_on_commit_failure(
    tmp_path, monkeypatch
):
    src = AsyncPath(tmp_path / "src.txt")
    dst = AsyncPath(tmp_path / "dst.txt")
    await src.write_bytes(b"new")
    await dst.write_bytes(b"old")

    def fail_replace(self, target):
        raise OSError("commit failed")

    monkeypatch.setattr(Path, "replace", fail_replace)

    with pytest.raises(OSError, match="commit failed"):
        await src.copy(dst, atomic=True, force=True)

    assert await dst.read_bytes() == b"old"
    assert not list(tmp_path.glob(".dst.txt.tmp-*"))


async def test_async_copy_negative_chunk_size_raises(tmp_path):
    src = AsyncPath(tmp_path / "src.txt")
    dst = AsyncPath(tmp_path / "dst.txt")
    await src.write_bytes(b"payload")

    with pytest.raises(ValueError, match="chunk_size"):
        await src.copy(dst, chunk_size=-1)


async def test_async_copy_atomic_to_cloud_destination_is_unsupported(tmp_path):
    src = AsyncPath(tmp_path / "src.txt")
    await src.write_bytes(b"payload")
    dst = _AsyncRemoteDestination()

    with pytest.raises(NotImplementedError, match="atomic=True"):
        await src.copy(dst, atomic=True)


def test_sync_copy_chunk_size_to_cloud_is_ignored(tmp_path):
    src = SyncPath(tmp_path / "src.txt")
    src.write_bytes(b"payload")
    dst = _SyncRemoteDestination()

    # chunk_size is a local streaming hint; for cloud destinations it is ignored,
    # not an error (a single PUT has no chunk concept).
    src.copy(dst, chunk_size=1024)

    dst.write_bytes.assert_called_once_with(b"payload")


async def test_async_copy_chunk_size_to_cloud_is_ignored(tmp_path):
    src = AsyncPath(tmp_path / "src.txt")
    await src.write_bytes(b"payload")
    dst = _AsyncRemoteDestination()

    await src.copy(dst, chunk_size=1024)

    dst.write_bytes.assert_awaited_once_with(b"payload")


async def test_async_copy_directory_to_cloud_requires_recursive(tmp_path):
    src = AsyncPath(tmp_path / "src")
    await src.mkdir()
    dst = _AsyncRemoteDestination()

    with pytest.raises(IsADirectoryError):
        await src.copy(dst)


async def test_async_recursive_copy_to_cloud_skips_identical_child(tmp_path):
    src = AsyncPath(tmp_path / "src")
    await src.mkdir()
    child = src / "same.txt"
    await child.write_bytes(b"same")
    checksum = (await child.checksums())["sha256"]
    dst = _AsyncRemoteDestination()
    dst.children["same.txt"] = _AsyncRemoteDestination(exists=True, checksum=checksum)

    await src.copy(dst, recursive=True)

    dst.children["same.txt"].write_bytes.assert_not_awaited()


async def test_async_recursive_copy_to_cloud_existing_child_requires_force(tmp_path):
    src = AsyncPath(tmp_path / "src")
    await src.mkdir()
    await (src / "different.txt").write_bytes(b"different")
    dst = _AsyncRemoteDestination()
    dst.children["different.txt"] = _AsyncRemoteDestination(exists=True, checksum="old")

    with pytest.raises(FileExistsError):
        await src.copy(dst, recursive=True)

    dst.children["different.txt"].write_bytes.assert_not_awaited()


def test_sync_copy_to_identical_cloud_destination_skips_write(tmp_path):
    src = SyncPath(tmp_path / "src.txt")
    src.write_bytes(b"payload")
    dst = _SyncRemoteDestination(exists=True, checksum=src.checksums()["sha256"])

    copied = src.copy(dst)

    assert copied is dst
    dst.write_bytes.assert_not_called()


def test_sync_copy_to_different_cloud_destination_requires_force(tmp_path):
    src = SyncPath(tmp_path / "src.txt")
    src.write_bytes(b"payload")
    dst = _SyncRemoteDestination(exists=True, checksum="different")

    with pytest.raises(FileExistsError):
        src.copy(dst)

    dst.write_bytes.assert_not_called()
    src.copy(dst, force=True)
    dst.write_bytes.assert_called_once_with(b"payload")


def test_sync_copy_directory_to_cloud_requires_recursive(tmp_path):
    src = SyncPath(tmp_path / "src")
    src.mkdir()
    dst = _SyncRemoteDestination()

    with pytest.raises(IsADirectoryError):
        src.copy(dst)


def test_sync_recursive_copy_to_cloud_destination_copies_children(tmp_path):
    src = SyncPath(tmp_path / "src")
    nested = src / "nested"
    nested.mkdir(parents=True)
    (src / "a.txt").write_bytes(b"a")
    (nested / "b.txt").write_bytes(b"b")
    dst = _SyncRemoteDestination()

    copied = src.copy(dst, recursive=True)

    assert copied is dst
    dst.children["a.txt"].write_bytes.assert_called_once_with(b"a")
    dst.children["nested/b.txt"].write_bytes.assert_called_once_with(b"b")


def test_sync_recursive_copy_to_cloud_skips_identical_child(tmp_path):
    src = SyncPath(tmp_path / "src")
    src.mkdir()
    child = src / "same.txt"
    child.write_bytes(b"same")
    dst = _SyncRemoteDestination()
    dst.children["same.txt"] = _SyncRemoteDestination(
        exists=True,
        checksum=child.checksums()["sha256"],
    )

    src.copy(dst, recursive=True)

    dst.children["same.txt"].write_bytes.assert_not_called()


async def test_async_recursive_copy_to_cloud_destination_copies_children(tmp_path):
    src = AsyncPath(tmp_path / "src")
    nested = src / "nested"
    await nested.mkdir(parents=True)
    await (src / "a.txt").write_bytes(b"a")
    await (nested / "b.txt").write_bytes(b"b")
    dst = _AsyncRemoteDestination()

    copied = await src.copy(dst, recursive=True)

    assert copied is dst
    dst.children["a.txt"].write_bytes.assert_awaited_once_with(b"a")
    dst.children["nested/b.txt"].write_bytes.assert_awaited_once_with(b"b")


async def test_async_copy_to_cloud_uri_resolves_destination_backend(tmp_path):
    src = AsyncPath(tmp_path / "src.txt")
    await src.write_bytes(b"payload")
    dst = _AsyncRemoteDestination()

    with patch("asanypath.AsAnyPath", return_value=dst) as make_path:
        copied = await src.copy("s3://bucket/key.txt")

    make_path.assert_called_once_with("s3://bucket/key.txt")
    assert copied is dst
    dst.write_bytes.assert_awaited_once_with(b"payload")
    assert not (tmp_path / "s3:").exists()


async def test_async_rename_to_cloud_destination_moves_via_backend(tmp_path):
    src = AsyncPath(tmp_path / "src.txt")
    await src.write_bytes(b"payload")
    dst = _AsyncRemoteDestination()

    moved = await src.rename(dst)

    assert moved is dst
    dst.write_bytes.assert_awaited_once_with(b"payload")
    assert not await src.exists()


def test_sync_copy_to_cloud_uri_resolves_destination_backend(tmp_path):
    src = SyncPath(tmp_path / "src.txt")
    src.write_bytes(b"payload")
    dst = _SyncRemoteDestination()

    with patch("asanypath.sync.AsAnyPath", return_value=dst) as make_path:
        copied = src.copy("s3://bucket/key.txt")

    make_path.assert_called_once_with("s3://bucket/key.txt")
    assert copied is dst
    dst.write_bytes.assert_called_once_with(b"payload")
    assert not (tmp_path / "s3:").exists()


def test_sync_copy_to_async_cloud_destination_awaits_backend(tmp_path):
    src = SyncPath(tmp_path / "src.txt")
    src.write_bytes(b"payload")
    dst = _AsyncRemoteDestination()

    copied = src.copy(dst)

    assert copied is dst
    dst.write_bytes.assert_awaited_once_with(b"payload")
    assert not (tmp_path / "s3:").exists()


def test_sync_move_to_cloud_uri_moves_via_destination_backend(tmp_path):
    src = SyncPath(tmp_path / "src.txt")
    src.write_bytes(b"payload")
    dst = _SyncRemoteDestination()

    with patch("asanypath.sync.AsAnyPath", return_value=dst) as make_path:
        moved = src.move("s3://bucket/key.txt")

    make_path.assert_called_once_with("s3://bucket/key.txt")
    assert moved is dst
    dst.write_bytes.assert_called_once_with(b"payload")
    assert not src.exists()


class TestAsyncPath(testbase):
    __test__ = True

    @pytest.fixture
    def p(self):
        return self._base("testdir/nested/file.txt")

    @pytest.fixture
    def tmp(self, tmp_path):
        return self._base(tmp_path)

    def test___bytes__(self, p):
        assert p.__bytes__() == b"testdir/nested/file.txt"

    def test___eq__(self, p):
        assert p == self._base("testdir/nested/file.txt")
        assert p != self._base("testdir/nested/other.txt")

    def test___fspath__(self, p):
        assert p.__fspath__() == "testdir/nested/file.txt"

    def test___repr__(self, p):
        assert repr(p) == "AsyncPath('testdir/nested/file.txt')"

    def test___rtruediv__(self, p):
        result = "base" / p
        assert result == self._base("base/testdir/nested/file.txt")
        assert isinstance(result, self._base)
        result2 = self._base("base") / p
        assert result2 == self._base("base/testdir/nested/file.txt")
        assert isinstance(result2, self._base)

    def test___str__(self, p):
        assert str(p) == "testdir/nested/file.txt"

    def test___truediv__(self, p):
        result = p / "sub"
        assert result == self._base("testdir/nested/file.txt/sub")
        assert isinstance(result, self._base)
        result2 = p / self._base("other")
        assert result2 == self._base("testdir/nested/file.txt/other")
        assert isinstance(result2, self._base)

    def test___weakref__(self, p):
        import weakref

        ref = weakref.ref(p)
        assert ref() is not None

    def test_absolute(self, p):

        result = p.absolute()
        assert str(result) == str(Path.cwd() / "testdir/nested/file.txt")
        assert isinstance(result, self._base)

    def test_anchor(self, p):
        assert p.anchor == ""
        assert self._base("/absolute/path").anchor == "/"

    def test_as_posix(self, p):
        assert p.as_posix() == "testdir/nested/file.txt"

    def test_as_uri(self, p):
        assert p.as_uri() == p.protocol + "://" + str(p)

    async def test_checksums(self, tmp):
        from hashlib import md5, sha1, sha256

        f = tmp / "checksum_test.txt"
        data = b"hello checksums"
        await f.write_bytes(data)
        result = await f.checksums()
        assert result == {
            "md5": md5(data).hexdigest(),
            "sha1": sha1(data).hexdigest(),
            "sha256": sha256(data).hexdigest(),
        }

    async def test_chmod(self, tmp):
        import stat

        f = tmp / "file.txt"
        await f.touch()
        await f.chmod(0o644)
        s = await f.stat()
        assert stat.S_IMODE(s.st_mode) == 0o644

    async def test_get_access_policy(self, tmp):
        policy = await tmp.get_access_policy()
        assert policy.owner

    async def test_update_access_policy(self, tmp):
        path = tmp / "policy.txt"
        await path.write_text("content")
        await path.update_access_policy(AccessPolicyPatch())

    async def test_cwd(self):
        from pathlib import Path

        result = await self._base.cwd()
        assert str(result) == str(Path.cwd())
        assert isinstance(result, self._base)

    def test_drive(self, p):
        assert p.drive == ""

    async def test_exists(self, tmp):
        f = tmp / "file.txt"
        assert await f.exists() is False
        await f.touch()
        assert await f.exists() is True

    async def test_expanduser(self):
        from pathlib import Path

        p = self._base("~/testfile")
        result = await p.expanduser()
        assert str(result) == str(Path.home() / "testfile")
        assert isinstance(result, self._base)

    async def test_glob(self, tmp):
        await (tmp / "a.txt").touch()
        await (tmp / "b.txt").touch()
        items = [x async for x in tmp.glob("*.txt")]
        assert len(items) == 2
        assert all(str(x).endswith(".txt") for x in items)
        assert all(isinstance(x, self._base) for x in items)

    async def test_group(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        try:
            group = await f.group()
        except KeyError as exc:
            pytest.skip(str(exc))
        assert isinstance(group, str)

    async def test_hardlink_to(self, tmp):
        src = tmp / "src.txt"
        hard = tmp / "hard.txt"
        await src.write_text("content")
        await hard.hardlink_to(src)
        assert await hard.exists()
        assert await hard.read_text() == "content"

    async def test_home(self):
        result = await self._base.home()
        assert str(result) == str(Path.home())
        assert isinstance(result, self._base)

    def test_is_absolute(self, p):
        assert p.is_absolute() is False
        assert self._base("/absolute/path").is_absolute() is True

    async def test_is_block_device(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        assert await f.is_block_device() is False

    async def test_is_char_device(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        assert await f.is_char_device() is False

    async def test_is_dir(self, tmp):
        assert await tmp.is_dir() is True
        f = tmp / "file.txt"
        await f.touch()
        assert await f.is_dir() is False

    async def test_is_fifo(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        assert await f.is_fifo() is False

    async def test_is_file(self, tmp):
        f = tmp / "file.txt"
        assert await f.is_file() is False
        await f.touch()
        assert await f.is_file() is True

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="is_junction requires Python 3.12+")
    async def test_is_junction(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        assert await f.is_junction() is False

    async def test_is_mount(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        assert await f.is_mount() is False

    def test_is_relative_to(self, p):
        assert p.is_relative_to("testdir") is True
        assert p.is_relative_to("other") is False

    def test_is_reserved(self, p):
        import sys

        if sys.version_info >= (3, 13):
            pytest.skip("pathlib.PurePath.is_reserved() deprecated in 3.13+")
        else:
            assert p.is_reserved() is False

    async def test_is_socket(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        assert await f.is_socket() is False

    async def test_is_symlink(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        link = tmp / "link.txt"
        await link.symlink_to(f)
        assert await f.is_symlink() is False
        assert await link.is_symlink() is True

    async def test_iterdir(self, tmp):
        await (tmp / "a.txt").touch()
        await (tmp / "b.txt").touch()
        items = [x async for x in tmp.iterdir()]
        assert len(items) == 2
        assert all(isinstance(x, self._base) for x in items)

    def test_joinpath(self, p):
        result = p.joinpath("extra.txt")
        assert result == self._base("testdir/nested/file.txt/extra.txt")
        assert isinstance(result, self._base)

    @pytest.mark.skip(reason="lchmod on symlinks is not supported on this platform")
    async def test_lchmod(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        link = tmp / "link.txt"
        await link.symlink_to(f)
        await link.lchmod(0o644)  # should not raise

    async def test_lstat(self, tmp):
        f = tmp / "file.txt"
        await f.write_text("hello")
        s = await f.lstat()
        assert s.st_size == 5

    def test_match(self, p):
        assert p.match("**/*.txt") is True
        assert p.match("**/*.md") is False

    async def test_mkdir(self, tmp):
        d = tmp / "newdir"
        assert await d.exists() is False
        await d.mkdir()
        assert await d.is_dir() is True
        nested = tmp / "a" / "b"
        await nested.mkdir(parents=True, exist_ok=True)
        assert await nested.is_dir() is True

    def test_name(self, p):
        assert p.name == "file.txt"

    async def test_open(self, tmp):
        f = tmp / "file.txt"
        await f.write_text("hello")
        async with await f.open("r") as fh:
            content = await fh.read()
        assert content == "hello"

    async def test_owner(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        try:
            owner = await f.owner()
        except KeyError as exc:
            pytest.skip(str(exc))
        assert isinstance(owner, str)

    def test_parent(self, p):
        assert str(p.parent) == "testdir/nested"
        assert isinstance(p.parent, self._base)

    def test_parents(self, p):
        assert [str(x) for x in p.parents] == ["testdir/nested", "testdir"]
        assert all(isinstance(x, self._base) for x in p.parents)

    def test_parts(self, p):
        assert p.parts == ("testdir", "nested", "file.txt")

    async def test_read_bytes(self, tmp):
        f = tmp / "file.txt"
        await f.write_bytes(b"hello bytes")
        assert await f.read_bytes() == b"hello bytes"

    async def test_iter_bytes(self, tmp):
        f = tmp / "file.txt"
        await f.write_bytes(b"abcdefghij")
        chunks = [chunk async for chunk in f.iter_bytes(4)]
        assert chunks == [b"abcd", b"efgh", b"ij"]

    async def test_iter_bytes_default_chunk_size(self, tmp):
        f = tmp / "file.txt"
        await f.write_bytes(b"abc")
        assert [chunk async for chunk in f.iter_bytes()] == [b"abc"]

    async def test_iter_bytes_invalid_chunk_size(self, tmp):
        f = tmp / "file.txt"
        await f.write_bytes(b"abc")
        with pytest.raises(ValueError, match="chunk_size"):
            _ = [chunk async for chunk in f.iter_bytes(0)]

    async def test_read_text(self, tmp):
        f = tmp / "file.txt"
        await f.write_text("hello text")
        assert await f.read_text() == "hello text"

    async def test_readlink(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        link = tmp / "link.txt"
        await link.symlink_to(f)
        target = await link.readlink()
        assert str(target) == str(f)
        assert isinstance(target, self._base)

    def test_relative_to(self, p):
        result = p.relative_to("testdir")
        assert result == self._base("nested/file.txt")
        assert isinstance(result, self._base)

    async def test_rename(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        dest = tmp / "renamed.txt"
        result = await f.rename(dest)
        assert await dest.exists()
        assert str(result) == str(dest)
        assert isinstance(result, self._base)

    async def test_move(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        dest = tmp / "moved.txt"
        result = await f.move(dest)
        assert await dest.exists()
        assert str(result) == str(dest)
        assert isinstance(result, self._base)

    async def test_replace(self, tmp):
        f = tmp / "file.txt"
        await f.write_text("original")
        dest = tmp / "dest.txt"
        await dest.write_text("old")
        result = await f.replace(dest)
        assert await dest.read_text() == "original"
        assert str(result) == str(dest)
        assert isinstance(result, self._base)

    async def test_resolve(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        result = await f.resolve()
        assert await result.exists()
        assert isinstance(result, self._base)

    async def test_rglob(self, tmp):
        subdir = tmp / "subdir"
        await subdir.mkdir()
        await (subdir / "nested.txt").touch()
        items = [x async for x in tmp.rglob("*.txt")]
        assert len(items) == 1
        assert str(items[0]).endswith("nested.txt")
        assert all(isinstance(x, self._base) for x in items)

    async def test_rmdir(self, tmp):
        d = tmp / "emptydir"
        await d.mkdir()
        await d.rmdir()
        assert await d.exists() is False

    def test_root(self, p):
        assert p.root == ""
        assert self._base("/absolute").root == "/"

    async def test_samefile(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        assert await f.samefile(f) is True
        g = tmp / "other.txt"
        await g.touch()
        assert await f.samefile(g) is False

    async def test_stat(self, tmp):
        f = tmp / "file.txt"
        await f.write_text("hello")
        s = await f.stat()
        assert s.st_size == 5

    def test_stem(self, p):
        assert p.stem == "file"

    def test_suffix(self, p):
        assert p.suffix == ".txt"

    def test_suffixes(self, p):
        assert p.suffixes == [".txt"]

    async def test_symlink_to(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        link = tmp / "link.txt"
        await link.symlink_to(f)
        assert await link.is_symlink() is True

    async def test_touch(self, tmp):
        f = tmp / "newfile.txt"
        assert await f.exists() is False
        await f.touch()
        assert await f.exists() is True

    async def test_unlink(self, tmp):
        f = tmp / "file.txt"
        await f.touch()
        await f.unlink()
        assert await f.exists() is False
        await f.unlink(missing_ok=True)  # should not raise

    async def test_walk(self, tmp):
        subdir = tmp / "subdir"
        await subdir.mkdir()
        await (subdir / "nested.txt").touch()
        entries = [(root, dirs, files) async for root, dirs, files in tmp.walk()]
        assert len(entries) == 2
        roots = [str(r) for r, _, _ in entries]
        assert str(tmp) in roots
        assert str(subdir) in roots
        assert all(isinstance(r, self._base) for r, _, _ in entries)

    def test_with_name(self, p):
        result = p.with_name("other.md")
        assert result == self._base("testdir/nested/other.md")
        assert isinstance(result, self._base)

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="with_segments requires Python 3.12+")
    def test_with_segments(self, p):
        result = p.with_segments("other/path.txt")
        assert result == self._base("other/path.txt")
        assert isinstance(result, self._base)

    def test_with_stem(self, p):
        result = p.with_stem("other")
        assert result == self._base("testdir/nested/other.txt")
        assert isinstance(result, self._base)

    def test_with_suffix(self, p):
        result = p.with_suffix(".md")
        assert result == self._base("testdir/nested/file.md")
        assert isinstance(result, self._base)

    async def test_write_bytes(self, tmp):
        f = tmp / "file.txt"
        n = await f.write_bytes(b"binary data")
        assert n == len(b"binary data")
        assert await f.read_bytes() == b"binary data"

    async def test_write_text(self, tmp):
        f = tmp / "file.txt"
        n = await f.write_text("text data")
        assert n == len("text data")
        assert await f.read_text() == "text data"


class TestLocalCopyAndRmdir:
    async def test_async_copy_file_and_remove_src(self, tmp_path):
        src = AsyncPath(tmp_path / "src.txt")
        dst = AsyncPath(tmp_path / "out" / "dst.txt")
        await src.write_text("hello")
        copied = await src.copy(dst, remove_src=True)
        assert str(copied).endswith("out/dst.txt")
        assert await dst.read_text() == "hello"
        assert not await src.exists()

    async def test_async_copy_callbacks(self, tmp_path):
        src = AsyncPath(tmp_path / "src.txt")
        dst = AsyncPath(tmp_path / "dst.txt")
        await src.write_bytes(b"abcdef")

        progress_calls: list[tuple[int, int]] = []
        done_calls: list[tuple[int, bool, str | None]] = []

        async def on_progress(
            delta: int,
            transferred: int,
            src_path: AsyncPath,
            dst_path: AsyncPath,
        ):
            assert src_path == src
            assert dst_path == dst
            progress_calls.append((delta, transferred))

        def on_done(
            src_path: AsyncPath,
            dst_path: AsyncPath,
            transferred: int,
            ok: bool,
            error: BaseException | None,
        ) -> None:
            assert src_path == src
            assert dst_path == dst
            done_calls.append((transferred, ok, None if error is None else type(error).__name__))

        await src.copy(dst, on_progress=on_progress, on_done=on_done)

        assert progress_calls[-1] == (6, 6)
        assert done_calls == [(6, True, None)]

    async def test_async_copy_dir_recursive_and_remove_src(self, tmp_path):
        src_dir = AsyncPath(tmp_path / "srcdir")
        await src_dir.mkdir()
        file_path = src_dir / "a.txt"
        await file_path.write_text("x")
        dst_dir = AsyncPath(tmp_path / "dstdir")
        copied = await src_dir.copy(dst_dir, recursive=True, remove_src=True)
        assert str(copied).endswith("dstdir")
        assert await (dst_dir / "a.txt").read_text() == "x"
        assert not await src_dir.exists()

    async def test_async_copy_dir_without_recursive_raises(self, tmp_path):
        src_dir = AsyncPath(tmp_path / "srcdir")
        await src_dir.mkdir()
        with pytest.raises(IsADirectoryError):
            await src_dir.copy(AsyncPath(tmp_path / "dstdir"))

    async def test_async_rmdir_recursive(self, tmp_path):
        root = AsyncPath(tmp_path / "root")
        await root.mkdir()
        child = root / "a.txt"
        await child.write_text("x")
        await root.rmdir(recursive=True)
        assert not await root.exists()

    def test_sync_copy_and_rmdir_recursive(self, tmp_path):
        src = SyncPath(tmp_path / "src")
        src.mkdir()
        (src / "a.txt").write_text("sync")
        dst = SyncPath(tmp_path / "dst")
        src.copy(dst, recursive=True, remove_src=True)
        assert (dst / "a.txt").read_text() == "sync"
        assert not src.exists()

    def test_sync_copy_callbacks(self, tmp_path):
        src = SyncPath(tmp_path / "src.txt")
        src.write_bytes(b"1234")
        dst = SyncPath(tmp_path / "dst.txt")

        progress_calls: list[tuple[int, int]] = []
        done_calls: list[tuple[int, bool, str | None]] = []

        def on_progress(
            delta: int,
            transferred: int,
            src_path: SyncPath,
            dst_path: SyncPath,
        ) -> None:
            assert src_path == src
            assert dst_path == dst
            progress_calls.append((delta, transferred))

        def on_done(
            src_path: SyncPath,
            dst_path: SyncPath,
            transferred: int,
            ok: bool,
            error: BaseException | None,
        ) -> None:
            assert src_path == src
            assert dst_path == dst
            done_calls.append((transferred, ok, None if error is None else type(error).__name__))

        src.copy(dst, on_progress=on_progress, on_done=on_done)

        assert progress_calls[-1] == (4, 4)
        assert done_calls == [(4, True, None)]

    def test_sync_copy_dir_without_recursive_raises(self, tmp_path):
        src = SyncPath(tmp_path / "src")
        src.mkdir()
        with pytest.raises(IsADirectoryError):
            src.copy(SyncPath(tmp_path / "dst"))

    def test_sync_move_alias(self, tmp_path):
        src = SyncPath(tmp_path / "src.txt")
        src.write_text("x")
        dst = SyncPath(tmp_path / "dst.txt")
        out = src.move(dst)
        assert out == dst
        assert dst.exists()
        assert not src.exists()

    def test_syncpath_equality_same_path(self, tmp_path):
        """SyncPath instances with same path are equal."""
        p1 = SyncPath(tmp_path / "file.txt")
        p2 = SyncPath(tmp_path / "file.txt")
        assert p1 == p2

    def test_syncpath_equality_different_paths(self, tmp_path):
        """SyncPath instances with different paths are not equal."""
        p1 = SyncPath(tmp_path / "file1.txt")
        p2 = SyncPath(tmp_path / "file2.txt")
        assert p1 != p2

    def test_syncpath_equality_with_string(self, tmp_path):
        """SyncPath can be compared with string path."""
        path = tmp_path / "file.txt"
        p = SyncPath(path)
        assert p == str(path)
        assert p != "/other/path.txt"

    def test_syncpath_equality_with_other_types(self, tmp_path):
        """SyncPath.__eq__ returns False for non-path types."""
        p = SyncPath(tmp_path / "file.txt")
        assert (p == 123) is False
        assert (p == []) is False
        # Also test with object() to ensure no unexpected behavior
        assert (p == object()) is False

    def test_syncpath_hash_consistency(self, tmp_path):
        """Equal SyncPath instances have same hash."""
        p1 = SyncPath(tmp_path / "file.txt")
        p2 = SyncPath(tmp_path / "file.txt")
        assert hash(p1) == hash(p2)

    def test_syncpath_in_set(self, tmp_path):
        """SyncPath instances can be used in sets (hash works)."""
        p1 = SyncPath(tmp_path / "file.txt")
        p2 = SyncPath(tmp_path / "file.txt")
        p3 = SyncPath(tmp_path / "other.txt")
        s = {p1, p2, p3}
        assert len(s) == 2  # p1 and p2 are equal, so only 2 unique

    def test_syncpath_as_dict_key(self, tmp_path):
        """SyncPath instances can be used as dict keys."""
        p1 = SyncPath(tmp_path / "file.txt")
        p2 = SyncPath(tmp_path / "file.txt")
        d = {p1: "value1"}
        d[p2] = "value2"  # Should overwrite since p1 == p2
        assert len(d) == 1
        assert d[p1] == "value2"
