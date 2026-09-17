# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

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
    monkeypatch.setattr(PROTOCOL_MAP, "_ensure", lambda: None)
    monkeypatch.delitem(PROTOCOL_MAP, "ssh", raising=False)
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
