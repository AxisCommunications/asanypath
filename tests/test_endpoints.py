# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from unittest.mock import patch

import pytest

from asanypath import ArtifactoryPath, AzurePath, GCSPath, S3Path
from asanypath.cloud import ServiceEndpointMixin
from asanypath.sync import AsAnyPath as SyncAsAnyPath


@pytest.mark.parametrize(
    "cls,uri,kwargs,expected",
    [
        (
            S3Path,
            "s3://bucket/key",
            {"aws_region": "eu-west-1"},
            "https://s3.eu-west-1.amazonaws.com",
        ),
        (GCSPath, "gs://bucket/key", {}, "https://storage.googleapis.com"),
        (
            AzurePath,
            "az://container/key",
            {"account_name": "example"},
            "https://example.blob.core.windows.net",
        ),
        (
            ArtifactoryPath,
            "art://example.com/artifactory/repo/key",
            {"token": "test"},
            "https://example.com/artifactory",
        ),
        (ArtifactoryPath, "art://example.com/repo/key", {"token": "test"}, "https://example.com"),
        (
            ArtifactoryPath,
            "art://example.com/artifactory",
            {"token": "test"},
            "https://example.com/artifactory",
        ),
    ],
)
def test_endpoint_property_and_derived_paths(cls, uri, kwargs, expected):
    with patch("asanypath._credstore.save"):
        path = cls(uri, **kwargs)
        assert path.endpoint_url == expected
        assert (path / "child").endpoint_url == expected
        if str(path).rstrip("/") != "art://example.com/artifactory":
            assert path.parent.endpoint_url == expected
        assert SyncAsAnyPath(uri, **kwargs).endpoint_url == expected
        assert cls.endpoint_url is ServiceEndpointMixin.endpoint_url
        with pytest.raises(AttributeError):
            path.endpoint_url = "https://other.example.com"


@pytest.mark.parametrize(
    "cls,uri",
    [(S3Path, "s3://bucket/key"), (GCSPath, "gs://bucket/key"), (AzurePath, "az://container/key")],
)
def test_custom_endpoint(cls, uri):
    path = cls(uri, endpoint_url="http://localhost:9000/service")
    assert path.endpoint_url == "http://localhost:9000/service"
    assert path._native_kwargs["endpoint"] == path.endpoint_url


def test_artifactory_preserves_credential_identity():
    with patch("asanypath._credstore.save") as save:
        path = ArtifactoryPath("art://example.com/artifactory/repo/key", token="test")
    save.assert_called_once_with("art:example.com/artifactory", "test")
    assert path._native_kwargs["base_url"] == path.endpoint_url
    assert not hasattr(path, "_base_url")


def test_artifactory_root_parent_has_host_endpoint():
    with patch("asanypath._credstore.save"):
        path = ArtifactoryPath("art://example.com/artifactory", token="test")
    assert path.parent.endpoint_url == "https://example.com"


def test_artifactory_uses_existing_cached_credentials(monkeypatch):
    monkeypatch.delenv("ARTIFACTORY_IDENTITY_TOKEN", raising=False)
    with (
        patch("asanypath._credstore.load", return_value="cached") as load,
        patch("asanypath._credstore.save") as save,
    ):
        path = ArtifactoryPath("art://example.com/artifactory/repo/key")
    load.assert_called_once_with("art:example.com/artifactory")
    save.assert_not_called()
    assert path.endpoint_url == "https://example.com/artifactory"
