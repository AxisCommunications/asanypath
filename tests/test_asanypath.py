# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
import re
from urllib.parse import urlparse

import pytest

from asanypath import PROTOCOL_MAP, AsAnyPath, UnsupportedProtocolPath
from asanypath.exceptions import InvalidPathError, UnsupportedProtocolError
from asanypath.s3 import S3Path


def _id_from_path(path: str | tuple[str]) -> str:
    path_tuple = tuple()
    if isinstance(path, tuple):
        path_tuple = path
        path = path[0]
    scheme, *_ = urlparse(path)
    protocol = scheme.lower()
    if scheme in ("", "file"):
        scheme = "local"
    func_id = (
        f"{scheme}_{'absolute_' if path.startswith('/') else ''}path_{protocol or 'no'}_protocol"
    )
    if path_tuple:
        func_id += "_from_parts"
    return func_id


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/file.txt",
        "file:///tmp/file.txt",
        ("/tmp", "subdir", "file.txt"),
        ("file:", "/tmp", "subdir", "file.txt"),
        ("s3:", "bucket", "subdir", "file.txt"),
        "s3://bucket/key",
        "gs://bucket/blob",
        "az://container/blob",
        "http://remote:8080/file",
        "https://remote:8443/file",
    ],
    ids=_id_from_path,
)
async def test_asanypath_factory(path):
    """Test that paths without protocol default to AsyncPath."""
    if isinstance(path, str):
        path = [path]
    elif isinstance(path, tuple) and len(path) > 1:
        path = list(path)
        if path[0].endswith(":") and path[1].startswith("/"):
            path[0] += "/"
    protocol, *_ = urlparse(path[0])

    apath = AsAnyPath(*path)
    pathstr = re.sub(
        rf"^({protocol}:/{{,2}})?", f"{protocol or 'file'}://", "/".join(path), flags=re.IGNORECASE
    ).replace("file://", "", 1)
    assert pathstr == str(apath)
    assert isinstance(apath, AsAnyPath)
    assert isinstance(apath, PROTOCOL_MAP.get(protocol.lower() or "file"))
    assert apath.protocol == (protocol or "file")


def test_no_argument_path_is_current_directory():
    assert str(AsAnyPath()) == "."


def test_none_path_raises_invalid_path_type_error():
    assert issubclass(InvalidPathError, TypeError)
    with pytest.raises(InvalidPathError, match="expected str, bytes or os.PathLike object"):
        AsAnyPath(None)


def test_unsupported_protocol_returns_placeholder():
    """Unknown protocols should return UnsupportedProtocolPath by default."""
    path = AsAnyPath("nope://server/file.txt")
    assert isinstance(path, UnsupportedProtocolPath)
    assert path.protocol == "nope"
    assert not isinstance(path, AsAnyPath)
    assert not issubclass(type(path), AsAnyPath)


def test_unsupported_protocol_raises_on_use():
    """Unknown protocol placeholder should raise on backend operations."""
    path = AsAnyPath("nope://server/file.txt")
    with pytest.raises(UnsupportedProtocolError, match="nope"):
        path.exists()


def test_missing_ssh_handler_falls_back_to_placeholder(monkeypatch):
    """If SSH support is unavailable, ssh:// should still construct safely."""
    monkeypatch.delitem(PROTOCOL_MAP, "ssh", raising=False)
    monkeypatch.setitem(PROTOCOL_MAP._pending, "ssh", ("asanypath._missing_backend", "SSHPath"))
    path = AsAnyPath("ssh://server/file.txt")
    assert isinstance(path, UnsupportedProtocolPath)
    with pytest.raises(UnsupportedProtocolError, match="ssh"):
        path.exists()


def test_case_insensitive_protocol():
    """Test that protocol detection is case-insensitive."""
    path1 = AsAnyPath("S3://bucket/key")
    path2 = AsAnyPath("s3://bucket/key")
    assert type(path1) is type(path2)
    assert isinstance(path1, S3Path)


@pytest.mark.parametrize(
    "env_var,env_val,input_path,expected_str",
    [
        (
            "ARTIFACTORY_BASE_URL",
            "artifactory.example.com/artifactory/",
            "art://cta-models/smiley/models/tf/saved_models/fruxo",
            "art://artifactory.example.com/artifactory/cta-models/smiley/models/tf/saved_models/fruxo",
        ),
        (
            "ARTIFACTORY_BASE_URL",
            "artifactory.example.com/artifactory/",
            "art://artifactory.example.com/artifactory/cta-models/smiley",
            "art://artifactory.example.com/artifactory/cta-models/smiley",
        ),
        (
            "ART_BASE_URL",
            "artifactory/artifactory",
            "art://artifactory/artifactory/cta-models/smiley",
            "art://artifactory/artifactory/cta-models/smiley",
        ),
    ],
    ids=["expands_shorthand", "does_not_override_fqdn", "does_not_duplicate_existing_base"],
)
def test_artifactory_base_url(monkeypatch, env_var, env_val, input_path, expected_str):
    """Base URL expansion: shorthand expands, FQDN stays, duplicates avoided."""
    monkeypatch.setenv(env_var, env_val)
    monkeypatch.setenv("ARTIFACTORY_IDENTITY_TOKEN", "test-token")
    path = AsAnyPath(input_path)
    assert str(path) == expected_str


@pytest.mark.parametrize(
    "env_vars,input_url,expected_type,expected_str",
    [
        (
            {
                "ARTIFACTORY_BASE_URL": "artifactory.example.com/artifactory",
                "ARTIFACTORY_IDENTITY_TOKEN": "test-token",
            },
            "https://artifactory.example.com/artifactory/cta-models/smiley",
            "ArtifactoryPath",
            "art://artifactory.example.com/artifactory/cta-models/smiley",
        ),
        (
            {},
            "https://example.com/some/file.txt",
            "HTTPSPath",
            None,
        ),
        (
            {"ART_BASE_URL": "art.example.com/repo", "ARTIFACTORY_IDENTITY_TOKEN": "test-token"},
            "https://art.example.com/repo/my-lib/file.jar",
            "ArtifactoryPath",
            "art://art.example.com/repo/my-lib/file.jar",
        ),
    ],
    ids=["routes_to_artifactory", "stays_https_no_match", "routes_with_alt_alias"],
)
def test_https_url_routing(monkeypatch, env_vars, input_url, expected_type, expected_str):
    """HTTPS URLs route to ArtifactoryPath when BASE_URL matches, else HTTPSPath."""
    from asanypath.artifactory import ArtifactoryPath
    from asanypath.http import HTTPSPath

    monkeypatch.delenv("ARTIFACTORY_BASE_URL", raising=False)
    monkeypatch.delenv("ART_BASE_URL", raising=False)
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)

    path = AsAnyPath(input_url)
    type_map = {"ArtifactoryPath": ArtifactoryPath, "HTTPSPath": HTTPSPath}
    assert isinstance(path, type_map[expected_type])
    if expected_type == "ArtifactoryPath":
        assert not isinstance(path, HTTPSPath)
    if expected_str:
        assert str(path) == expected_str


def _run_clean(code: str) -> str:
    import subprocess
    import sys

    return subprocess.run(
        [sys.executable, "-c", code], check=True, capture_output=True, text=True
    ).stdout.strip()


def test_local_path_does_not_import_cloud_backends():
    out = _run_clean(
        "import sys, asanypath; asanypath.AsAnyPath('.');"
        "print([m for m in ('asanypath.s3', 'asanypath.cloud', 'asanypath.artifactory')"
        " if m in sys.modules])"
    )
    assert out == "[]"


def test_register_before_lookup_keeps_builtins():
    out = _run_clean(
        "from asanypath import AsyncPath, register_protocol, list_protocols, PROTOCOL_MAP;"
        "register_protocol('mine', AsyncPath);"
        "print('s3' in list_protocols(), PROTOCOL_MAP['mine'] is AsyncPath)"
    )
    assert out == "True True"


def test_register_overrides_builtin():
    out = _run_clean(
        "from asanypath import AsyncPath, register_protocol, PROTOCOL_MAP;"
        "register_protocol('s3', AsyncPath);"
        "print(PROTOCOL_MAP['s3'] is AsyncPath)"
    )
    assert out == "True"


def test_concurrent_https_and_local_dispatch_during_lazy_import(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    import asanypath.asanypath as factory
    from asanypath.http import HTTPSPath
    from asanypath.local import AsyncPath

    registry = factory._LazyDict()
    monkeypatch.setattr(factory, "PROTOCOL_MAP", registry)
    original_import = factory.import_module
    importing = Event()
    resume = Event()

    def blocked_import(name):
        if name == "asanypath.local" and not importing.is_set():
            importing.set()
            if not resume.wait(5):
                raise TimeoutError("local backend import was not released")
        return original_import(name)

    monkeypatch.setattr(factory, "import_module", blocked_import)
    with ThreadPoolExecutor(max_workers=1) as executor:
        https = executor.submit(AsAnyPath, "https://example.com/file")
        try:
            assert importing.wait(5)
            local = AsAnyPath("/tmp/cfg.yaml")
        finally:
            resume.set()
        assert isinstance(https.result(timeout=10), HTTPSPath)
    assert isinstance(local, AsyncPath)
    assert str(local) == "/tmp/cfg.yaml"


@pytest.mark.parametrize("import_fails", [False, True])
def test_register_while_lazy_import_is_in_progress(monkeypatch, import_fails):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from types import SimpleNamespace

    import asanypath.asanypath as factory

    class Builtin:
        pass

    class Override:
        pass

    registry = factory._LazyDict()
    registry._pending = {"file": ("backend", "Builtin")}
    importing = Event()
    resume = Event()

    def blocked_import(name):
        importing.set()
        if not resume.wait(5):
            raise TimeoutError("backend import was not released")
        if import_fails:
            raise ImportError("optional backend unavailable")
        return SimpleNamespace(Builtin=Builtin)

    monkeypatch.setattr(factory, "import_module", blocked_import)
    with ThreadPoolExecutor(max_workers=1) as executor:
        lookup = executor.submit(registry.__getitem__, "file")
        try:
            assert importing.wait(5)
            registry["file"] = Override
        finally:
            resume.set()
        assert lookup.result(timeout=5) is Override
    assert registry["file"] is Override


def test_failed_import_does_not_hide_another_import_in_progress(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from types import SimpleNamespace

    import asanypath.asanypath as factory

    class Builtin:
        pass

    registry = factory._LazyDict()
    registry._pending = {"file": ("backend", "Builtin")}
    first_started, second_started = Event(), Event()
    fail_first, finish_second = Event(), Event()

    def blocked_import(name):
        if not first_started.is_set():
            first_started.set()
            if not fail_first.wait(5):
                raise TimeoutError("first import was not released")
            raise ImportError("temporarily unavailable")
        if not second_started.is_set():
            second_started.set()
            if not finish_second.wait(5):
                raise TimeoutError("second import was not released")
        return SimpleNamespace(Builtin=Builtin)

    monkeypatch.setattr(factory, "import_module", blocked_import)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(registry.__getitem__, "file")
        try:
            assert first_started.wait(5)
            second = executor.submit(registry.__getitem__, "file")
            assert second_started.wait(5)
            fail_first.set()
            assert first.result(timeout=5) is UnsupportedProtocolPath
            assert registry["file"] is Builtin
        finally:
            fail_first.set()
            finish_second.set()
        assert second.result(timeout=5) is Builtin


def test_missing_backend_is_not_reimported(monkeypatch):
    import asanypath.asanypath as factory

    registry = factory._LazyDict()
    registry._pending = {"file": ("backend", "Builtin")}
    attempts = []

    def missing_import(name):
        attempts.append(name)
        raise ImportError("backend unavailable")

    monkeypatch.setattr(factory, "import_module", missing_import)
    assert registry["file"] is UnsupportedProtocolPath
    assert registry["file"] is UnsupportedProtocolPath
    assert attempts == ["backend"]


def test_in_progress_import_can_finish_after_fork_reset(monkeypatch):
    from types import SimpleNamespace

    import asanypath.asanypath as factory

    class Builtin:
        pass

    registry = factory._LazyDict()
    registry._pending = {"file": ("backend", "Builtin")}

    def import_after_fork(name):
        registry._after_fork()
        return SimpleNamespace(Builtin=Builtin)

    monkeypatch.setattr(factory, "import_module", import_after_fork)
    assert registry["file"] is Builtin
    assert registry["file"] is Builtin


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork")
def test_registry_lock_is_reinitialized_after_fork():
    import multiprocessing
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    import asanypath.asanypath as factory
    from asanypath.local import AsyncPath

    ctx = multiprocessing.get_context("fork")
    parent, child = ctx.Pipe(duplex=False)
    locked, release = Event(), Event()

    def hold_lock():
        with factory.PROTOCOL_MAP._lock:
            locked.set()
            release.wait(10)

    def child_lookup():
        child.send(factory.PROTOCOL_MAP.get("file") is AsyncPath)
        child.close()

    with ThreadPoolExecutor(max_workers=1) as executor:
        holder = executor.submit(hold_lock)
        process = ctx.Process(target=child_lookup)
        try:
            assert locked.wait(5)
            process.start()
            assert parent.poll(5), "child blocked on the inherited registry lock"
            assert parent.recv()
            process.join(5)
            assert process.exitcode == 0
        finally:
            release.set()
            if process.pid is not None and process.is_alive():
                process.terminate()
                process.join()
            holder.result(timeout=5)
            parent.close()
            child.close()


def test_lazy_exports_and_dir():
    import asanypath

    assert asanypath.S3Path is S3Path
    assert {"S3Path", "SyncPath", "DirEntry"} <= set(dir(asanypath))
    with pytest.raises(AttributeError, match="nope"):
        asanypath.nope  # noqa: B018
