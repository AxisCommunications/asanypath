# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from pathlib import Path

import pytest

from asanypath import AccessPolicyPatch, SyncPath
from tests.conftest import classtest_factory

testbase = classtest_factory("_TestSyncPath", SyncPath)


class TestSyncPath(testbase):
    __test__ = True

    @pytest.fixture
    def p(self):
        return SyncPath("testdir/nested/file.txt")

    @pytest.fixture
    def tmp(self, tmp_path):
        return SyncPath(tmp_path)

    def test___bytes__(self, p):
        assert p.__bytes__() == b"testdir/nested/file.txt"

    def test___eq__(self, p):
        assert p == SyncPath("testdir/nested/file.txt")
        assert p != SyncPath("testdir/nested/other.txt")

    def test___fspath__(self, p):
        assert p.__fspath__() == "testdir/nested/file.txt"

    def test___repr__(self, p):
        assert repr(p) == "SyncPath('testdir/nested/file.txt')"

    def test___rtruediv__(self, p):
        result = "base" / p
        assert result == SyncPath("base/testdir/nested/file.txt")
        assert isinstance(result, SyncPath)

    def test___str__(self, p):
        assert str(p) == "testdir/nested/file.txt"

    def test___truediv__(self, p):
        result = p / "sub"
        assert result == SyncPath("testdir/nested/file.txt/sub")
        assert isinstance(result, SyncPath)
        result2 = p / SyncPath("other")
        assert result2 == SyncPath("testdir/nested/file.txt/other")
        assert isinstance(result2, SyncPath)

    def test_absolute(self, p):
        result = p.absolute()
        assert str(result) == str(Path.cwd() / "testdir/nested/file.txt")
        assert isinstance(result, SyncPath)

    def test_anchor(self, p):
        assert p.anchor == ""
        assert SyncPath("/absolute/path").anchor == "/"

    def test_as_posix(self, p):
        assert p.as_posix() == "testdir/nested/file.txt"

    def test_as_uri(self, p):
        assert p.as_uri() == "file://" + str(p)

    def test_checksums(self, tmp):
        from hashlib import md5, sha1, sha256

        f = tmp / "checksum_test.txt"
        data = b"hello checksums"
        f.write_bytes(data)
        result = f.checksums()
        assert result == {
            "md5": md5(data).hexdigest(),
            "sha1": sha1(data).hexdigest(),
            "sha256": sha256(data).hexdigest(),
        }

    def test_chmod(self, tmp):
        import stat

        f = tmp / "file.txt"
        f.touch()
        f.chmod(0o644)
        s = f.stat()
        assert stat.S_IMODE(s.st_mode) == 0o644

    def test_get_access_policy(self, tmp):
        policy = tmp.get_access_policy()
        assert policy.owner

    def test_update_access_policy(self, tmp):
        path = tmp / "policy.txt"
        path.write_text("content")
        path.update_access_policy(AccessPolicyPatch())

    def test_drive(self, p):
        assert p.drive == ""

    def test_exists(self, tmp):
        f = tmp / "file.txt"
        assert f.exists() is False
        f.touch()
        assert f.exists() is True

    def test_expanduser(self):
        p = SyncPath("~/testfile")
        result = p.expanduser()
        assert str(result) == str(Path.home() / "testfile")
        assert isinstance(result, SyncPath)

    def test_glob(self, tmp):
        (tmp / "a.txt").touch()
        (tmp / "b.txt").touch()
        items = list(tmp.glob("*.txt"))
        assert len(items) == 2
        assert all(str(x).endswith(".txt") for x in items)
        assert all(isinstance(x, SyncPath) for x in items)

    def test_is_absolute(self, p):
        assert p.is_absolute() is False
        assert SyncPath("/absolute/path").is_absolute() is True

    def test_is_dir(self, tmp):
        assert tmp.is_dir() is True
        f = tmp / "file.txt"
        f.touch()
        assert f.is_dir() is False

    def test_is_file(self, tmp):
        f = tmp / "file.txt"
        assert f.is_file() is False
        f.touch()
        assert f.is_file() is True

    def test_is_relative_to(self, p):
        assert p.is_relative_to("testdir") is True
        assert p.is_relative_to("other") is False

    def test_is_symlink(self, tmp):
        f = tmp / "file.txt"
        f.touch()
        link = tmp / "link.txt"
        link.symlink_to(f)
        assert f.is_symlink() is False
        assert link.is_symlink() is True

    def test_iterdir(self, tmp):
        (tmp / "a.txt").touch()
        (tmp / "b.txt").touch()
        items = list(tmp.iterdir())
        assert len(items) == 2
        assert all(isinstance(x, SyncPath) for x in items)

    def test_joinpath(self, p):
        result = p.joinpath("extra.txt")
        assert result == SyncPath("testdir/nested/file.txt/extra.txt")
        assert isinstance(result, SyncPath)

    def test_match(self, p):
        assert p.match("**/*.txt") is True
        assert p.match("**/*.md") is False

    def test_mkdir(self, tmp):
        d = tmp / "newdir"
        assert d.exists() is False
        d.mkdir()
        assert d.is_dir() is True
        nested = tmp / "a" / "b"
        nested.mkdir(parents=True, exist_ok=True)
        assert nested.is_dir() is True

    def test_name(self, p):
        assert p.name == "file.txt"

    def test_parent(self, p):
        assert str(p.parent) == "testdir/nested"
        assert isinstance(p.parent, SyncPath)

    def test_parents(self, p):
        assert [str(x) for x in p.parents] == ["testdir/nested", "testdir"]
        assert all(isinstance(x, SyncPath) for x in p.parents)

    def test_parts(self, p):
        assert p.parts == ("testdir", "nested", "file.txt")

    def test_read_bytes(self, tmp):
        f = tmp / "file.txt"
        f.write_bytes(b"hello bytes")
        assert f.read_bytes() == b"hello bytes"

    def test_iter_bytes(self, tmp):
        f = tmp / "file.txt"
        f.write_bytes(b"abcdefghij")
        assert list(f.iter_bytes(4)) == [b"abcd", b"efgh", b"ij"]

    def test_iter_bytes_default_chunk_size(self, tmp):
        f = tmp / "file.txt"
        f.write_bytes(b"abc")
        assert list(f.iter_bytes()) == [b"abc"]

    def test_iter_bytes_invalid_chunk_size(self, tmp):
        f = tmp / "file.txt"
        f.write_bytes(b"abc")
        with pytest.raises(ValueError, match="chunk_size"):
            list(f.iter_bytes(0))

    def test_read_text(self, tmp):
        f = tmp / "file.txt"
        f.write_text("hello text")
        assert f.read_text() == "hello text"

    def test_readlink(self, tmp):
        f = tmp / "file.txt"
        f.touch()
        link = tmp / "link.txt"
        link.symlink_to(f)
        target = link.readlink()
        assert str(target) == str(f)
        assert isinstance(target, SyncPath)

    def test_relative_to(self, p):
        result = p.relative_to("testdir")
        assert result == SyncPath("nested/file.txt")
        assert isinstance(result, SyncPath)

    def test_rename(self, tmp):
        f = tmp / "file.txt"
        f.touch()
        dest = tmp / "renamed.txt"
        result = f.rename(dest)
        assert dest.exists()
        assert str(result) == str(dest)
        assert isinstance(result, SyncPath)

    def test_replace(self, tmp):
        f = tmp / "file.txt"
        f.write_text("original")
        dest = tmp / "dest.txt"
        dest.write_text("old")
        result = f.replace(dest)
        assert dest.read_text() == "original"
        assert str(result) == str(dest)
        assert isinstance(result, SyncPath)

    def test_resolve(self, tmp):
        f = tmp / "file.txt"
        f.touch()
        result = f.resolve()
        assert result.exists()
        assert isinstance(result, SyncPath)

    def test_rglob(self, tmp):
        subdir = tmp / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").touch()
        items = list(tmp.rglob("*.txt"))
        assert len(items) == 1
        assert str(items[0]).endswith("nested.txt")
        assert all(isinstance(x, SyncPath) for x in items)

    def test_rmdir(self, tmp):
        d = tmp / "emptydir"
        d.mkdir()
        d.rmdir()
        assert d.exists() is False

    def test_root(self, p):
        assert p.root == ""
        assert SyncPath("/absolute").root == "/"

    def test_stat(self, tmp):
        f = tmp / "file.txt"
        f.write_text("hello")
        s = f.stat()
        assert s.st_size == 5

    def test_stem(self, p):
        assert p.stem == "file"

    def test_suffix(self, p):
        assert p.suffix == ".txt"

    def test_suffixes(self, p):
        assert p.suffixes == [".txt"]

    def test_symlink_to(self, tmp):
        f = tmp / "file.txt"
        f.touch()
        link = tmp / "link.txt"
        link.symlink_to(f)
        assert link.is_symlink() is True

    def test_touch(self, tmp):
        f = tmp / "newfile.txt"
        assert f.exists() is False
        f.touch()
        assert f.exists() is True

    def test_unlink(self, tmp):
        f = tmp / "file.txt"
        f.touch()
        f.unlink()
        assert f.exists() is False
        f.unlink(missing_ok=True)  # should not raise

    def test_walk(self, tmp):
        subdir = tmp / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").touch()
        entries = list(tmp.walk())
        assert len(entries) == 2
        roots = [str(r) for r, _, _ in entries]
        assert str(tmp) in roots
        assert str(subdir) in roots
        assert all(isinstance(r, SyncPath) for r, _, _ in entries)

    def test_with_name(self, p):
        result = p.with_name("other.md")
        assert result == SyncPath("testdir/nested/other.md")
        assert isinstance(result, SyncPath)

    def test_with_stem(self, p):
        result = p.with_stem("other")
        assert result == SyncPath("testdir/nested/other.txt")
        assert isinstance(result, SyncPath)

    def test_with_suffix(self, p):
        result = p.with_suffix(".md")
        assert result == SyncPath("testdir/nested/file.md")
        assert isinstance(result, SyncPath)

    def test_write_bytes(self, tmp):
        f = tmp / "file.txt"
        n = f.write_bytes(b"binary data")
        assert n == len(b"binary data")
        assert f.read_bytes() == b"binary data"

    def test_write_text(self, tmp):
        f = tmp / "file.txt"
        n = f.write_text("text data")
        assert n == len("text data")
        assert f.read_text() == "text data"
