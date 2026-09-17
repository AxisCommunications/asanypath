# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import pytest

from asanypath import AsAnyPath, UnsupportedProtocolPath
from asanypath.exceptions import UnsupportedProtocolError


def test_dynamic_protocol_is_preserved():
    path = AsAnyPath("abc://host/dir/file.txt")
    assert isinstance(path, UnsupportedProtocolPath)
    assert path.protocol == "abc"
    assert str(path) == "abc://host/dir/file.txt"
    assert bytes(path) == b"abc://host/dir/file.txt"


def test_pure_path_operations_work():
    path = AsAnyPath("abc://host/dir/file.txt")
    assert path.name == "file.txt"
    assert path.parent == "abc://host/dir"
    assert (path.parent / "next.txt") == "abc://host/dir/next.txt"


@pytest.mark.parametrize(
    "method_name,args,kwargs",
    [
        ("__fspath__", (), {}),
        ("chmod", (0o644,), {}),
        ("exists", (), {}),
        ("is_file", (), {}),
        ("is_dir", (), {}),
        ("is_fifo", (), {}),
        ("is_mount", (), {}),
        ("is_socket", (), {}),
        ("is_symlink", (), {}),
        ("is_block_device", (), {}),
        ("is_char_device", (), {}),
        ("is_junction", (), {}),
        ("group", (), {}),
        ("owner", (), {}),
        ("read_bytes", (), {}),
        ("read_text", (), {}),
        ("iter_bytes", (), {}),
        ("write_bytes", (b"data",), {}),
        ("write_text", ("data",), {}),
        ("mkdir", (), {}),
        ("rmdir", (), {}),
        ("touch", (), {}),
        ("unlink", (), {}),
        ("iterdir", (), {}),
        ("glob", ("*.txt",), {}),
        ("rglob", ("*.txt",), {}),
        ("stat", (), {}),
        ("lstat", (), {}),
        ("samefile", ("abc://host/other",), {}),
        ("rename", ("abc://host/new.txt",), {}),
        ("replace", ("abc://host/new.txt",), {}),
        ("resolve", (), {}),
        ("readlink", (), {}),
        ("checksums", (), {}),
        ("walk", (), {}),
        ("lchmod", (0o644,), {}),
        ("hardlink_to", ("abc://host/t",), {}),
        ("symlink_to", ("abc://host/t",), {}),
        ("expanduser", (), {}),
        ("open", (), {}),
    ],
)
def test_io_methods_raise_unsupported(method_name, args, kwargs):
    path = AsAnyPath("abc://host/dir/file.txt")
    with pytest.raises(UnsupportedProtocolError, match="abc"):
        getattr(path, method_name)(*args, **kwargs)


def test_classmethods_raise_unsupported():
    with pytest.raises(UnsupportedProtocolError, match="cwd"):
        UnsupportedProtocolPath.cwd()
    with pytest.raises(UnsupportedProtocolError, match="home"):
        UnsupportedProtocolPath.home()
