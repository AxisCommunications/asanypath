# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Credentials, caches and listings stay scoped to the path that owns them."""

from __future__ import annotations

import asyncio

import pytest

from asanypath import _credstore
from asanypath import cache as cache_module
from asanypath.artifactory import ArtifactoryPath
from asanypath.azure import AzurePath
from asanypath.batcher import MicroBatcher
from asanypath.cache import DownloadCache
from asanypath.cloud import CloudPathMixin
from asanypath.ftp import FTPPath
from asanypath.gcs import GCSPath
from asanypath.http import HTTPPath
from asanypath.s3 import S3Path
from asanypath.ssh import SSHPath

_AMBIENT = (
    "ARTIFACTORY_IDENTITY_TOKEN",
    "ARTIFACTORY_BASE_URL",
    "ART_BASE_URL",
    "SSH_PASSWORD",
    "GCS_ACCESS_TOKEN",
    "STORAGE_EMULATOR_HOST",
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_STORAGE_KEY",
    "AZURE_STORAGE_SAS_TOKEN",
    "AZURE_STORAGE_ENDPOINT",
    "AZURE_STORAGE_CONNECTION_STRING",
)

TRUSTED = "art://trusted.example/artifactory/repo"
OTHER = "art://other.example/artifactory/repo"


@pytest.fixture(autouse=True)
def _no_ambient_credentials(monkeypatch):
    for name in _AMBIENT:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(_credstore, "load", lambda key, **_: None)
    monkeypatch.setattr(_credstore, "save", lambda key, value: None)
    monkeypatch.setattr("asanypath.artifactory._jfrog_conf_token", lambda base_url: None)
    monkeypatch.setattr(CloudPathMixin, "_batcher_instances", {})


def _s3(uri: str = "s3://bucket/dir", *, secret: str = "admin-secret", **kwargs) -> S3Path:
    options = dict(
        endpoint_url="https://s3.internal.example",
        aws_region="eu-west-1",
        aws_access_key_id="AKIDSHARED",
        aws_secret_access_key=secret,
    )
    options.update(kwargs)
    return S3Path(uri, **options)


# ---------------------------------------------------------------------------
# Micro-batching
# ---------------------------------------------------------------------------


async def test_batcher_only_groups_identical_kwargs():
    flushed: list[tuple[str, list[str]]] = []

    async def flush(items, futures, use_h2, **kwargs):
        flushed.append((kwargs["token"], items))
        for fut in futures:
            fut.set_result(kwargs["token"])

    batcher = MicroBatcher()
    submit = batcher.register("get", flush_fn=flush, key_fn=lambda **_: "same-group")

    results = await asyncio.gather(
        submit(item="a", token="admin"), submit(item="b", token="reader")
    )

    assert results == ["admin", "reader"]
    assert sorted(flushed) == [("admin", ["a"]), ("reader", ["b"])]


async def test_artifactory_tokens_sharing_a_prefix_are_not_batched(monkeypatch):
    async def fake_get(paths, use_h2, base_url, token):
        return [token.encode() for _ in paths]

    monkeypatch.setitem(
        ArtifactoryPath._batcher_ops,
        "get",
        {"batch_fn": fake_get, "items_key": "paths", "wrap": bytes},
    )
    admin = ArtifactoryPath(f"{TRUSTED}/a", token="abcdefgh-admin")
    reader = ArtifactoryPath(f"{TRUSTED}/b", token="abcdefgh-reader")

    results = await asyncio.gather(admin.read_bytes(), reader.read_bytes())

    assert results == [b"abcdefgh-admin", b"abcdefgh-reader"]


async def test_s3_secrets_sharing_an_access_key_are_not_batched(monkeypatch):
    async def fake_get(keys, use_h2, secret_key, **_):
        return [secret_key.encode() for _ in keys]

    monkeypatch.setitem(
        S3Path._batcher_ops, "get", {"batch_fn": fake_get, "items_key": "keys", "wrap": bytes}
    )
    admin = _s3("s3://bucket/a", secret="admin-secret")
    guess = _s3("s3://bucket/b", secret="wrong-secret")

    results = await asyncio.gather(admin.read_bytes(), guess.read_bytes())

    assert results == [b"admin-secret", b"wrong-secret"]


# ---------------------------------------------------------------------------
# Credential scoping
# ---------------------------------------------------------------------------


def test_artifactory_token_is_not_sent_to_another_service():
    ArtifactoryPath(f"{TRUSTED}/file", token="trusted-token")

    with pytest.raises(ValueError, match="token required"):
        ArtifactoryPath(f"{OTHER}/file")


def test_artifactory_derived_paths_keep_their_own_token():
    path = ArtifactoryPath(f"{TRUSTED}/dir/file", token="first")
    ArtifactoryPath(f"{TRUSTED}/dir/file", token="second")

    derived = [
        path / "child",
        path.parent,
        path.with_name("other"),
        path.with_segments(f"{TRUSTED}/elsewhere"),
    ]

    assert [p._token for p in derived] == ["first"] * len(derived)


def test_artifactory_token_does_not_follow_a_derived_path_to_another_service():
    path = ArtifactoryPath(f"{TRUSTED}/file", token="trusted-token")

    with pytest.raises(ValueError, match="token required"):
        path.with_segments(f"{OTHER}/file")


_CLOUD_CASES = [
    pytest.param(
        S3Path,
        "s3://bucket/dir/file",
        dict(
            endpoint_url="https://s3.internal.example",
            aws_region="eu-west-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        ),
        lambda p: (p._endpoint_url, p._access_key, p._secret_key),
        id="s3",
    ),
    pytest.param(
        GCSPath,
        "gs://bucket/dir/file",
        dict(endpoint_url="https://gcs.internal.example", access_token="gcs-token"),
        lambda p: (p._endpoint_url, p._access_token),
        id="gcs",
    ),
    pytest.param(
        AzurePath,
        "az://container/dir/file",
        dict(
            endpoint_url="https://az.internal.example",
            account_name="account",
            account_key="account-key",
        ),
        lambda p: (p._endpoint_url, p._account_name, p._account_key),
        id="azure",
    ),
]


@pytest.mark.parametrize(("cls", "uri", "options", "identity"), _CLOUD_CASES)
def test_explicit_cloud_credentials_are_not_shared_class_wide(cls, uri, options, identity):
    explicit = cls(uri, **options)

    independent = cls(uri)

    assert identity(independent) != identity(explicit)


@pytest.mark.parametrize(("cls", "uri", "options", "identity"), _CLOUD_CASES)
def test_derived_cloud_paths_inherit_explicit_credentials(cls, uri, options, identity):
    path = cls(uri, **options)
    other_bucket = f"{path.protocol}://other-bucket/key"

    derived = [
        path / "child",
        path.parent,
        path.with_name("other"),
        path.with_segments(other_bucket),
        path._spawn(other_bucket),
    ]

    assert [identity(p) for p in derived] == [identity(path)] * len(derived)


def test_ssh_password_stays_with_its_host():
    path = SSHPath("ssh://a.example/dir/file", username="alice", password="pw")

    assert SSHPath("ssh://b.example/file")._password != "pw"
    assert (path / "child")._password == "pw"
    assert path.parent._password == "pw"
    assert path.with_segments("ssh://b.example/file")._password != "pw"


def test_ftp_credentials_stay_with_their_host():
    path = FTPPath("ftp://a.example/dir/file", username="alice", password="pw")

    assert FTPPath("ftp://b.example/file")._user != "alice"
    assert ((path / "child")._user, (path / "child")._password) == ("alice", "pw")
    assert path.with_segments("ftp://b.example/file")._password != "pw"


# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------


async def test_listing_cache_is_scoped_to_credentials(monkeypatch):
    async def fake_fetch(self):
        return [f"{self}/{self._secret_key}.txt"]

    monkeypatch.setattr(S3Path, "_fetch_listing", fake_fetch)
    admin = _s3(secret="admin-secret")
    reader = _s3(secret="reader-secret")

    assert [c.name async for c in admin.iterdir()] == ["admin-secret.txt"]
    assert [c.name async for c in reader.iterdir()] == ["reader-secret.txt"]


async def test_download_cache_is_scoped_to_credentials(monkeypatch, tmp_path):
    monkeypatch.setattr(cache_module, "_directory", tmp_path / "cache")
    monkeypatch.setattr(cache_module, "_cache", None)
    keys: list[str] = []
    original = DownloadCache.get_or_fetch

    async def spy(self, key, fetch):
        keys.append(key)
        return await original(self, key, fetch)

    async def fake_read(self):
        return self._secret_key.encode()

    monkeypatch.setattr(DownloadCache, "get_or_fetch", spy)
    monkeypatch.setattr(S3Path, "_read_bytes_uncached", fake_read)
    try:
        assert await _s3("s3://bucket/obj", secret="admin-secret").read_bytes(use_cache=True) == (
            b"admin-secret"
        )
        assert await _s3("s3://bucket/obj", secret="reader-secret").read_bytes(use_cache=True) == (
            b"reader-secret"
        )
    finally:
        cache_module.get_cache().close()

    assert len(set(keys)) == 2
    assert not any("secret" in key for key in keys)


def test_http_cache_identity_changes_with_auth_env(monkeypatch):
    path = HTTPPath("https://example.invalid/data")
    monkeypatch.setenv("HTTP_AUTH_TOKEN", "admin-token")
    admin_key = path._identity_fingerprint()
    monkeypatch.setenv("HTTP_AUTH_TOKEN", "reader-token")
    reader_key = path._identity_fingerprint()

    assert admin_key != reader_key


def test_artifactory_cache_identity_changes_with_implicit_token(monkeypatch):
    uri = f"{TRUSTED}/file.txt"
    tokens = iter(["admin-token", "reader-token"])
    monkeypatch.setattr(_credstore, "load", lambda key, **_: next(tokens))
    monkeypatch.setattr("asanypath.artifactory._jfrog_conf_token", lambda base_url: None)
    ArtifactoryPath._service_tokens.clear()
    admin = ArtifactoryPath(uri)
    ArtifactoryPath._service_tokens.clear()
    reader = ArtifactoryPath(uri)

    assert admin._identity_fingerprint() != reader._identity_fingerprint()


# ---------------------------------------------------------------------------
# Listing confinement
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "entry",
    ["../outside.txt", "%2E%2E/outside.txt", "sub/../../outside.txt", "a%2Fb", "", "sub?x=/../"],
)
async def test_iterdir_skips_entries_outside_the_listed_prefix(entry, monkeypatch, caplog):
    async def fake_fetch(self):
        return [
            f"s3://bucket/allowed/{entry}",
            "s3://bucket/allowed/ok.txt",
            "s3://bucket/allowed/sub/",
        ]

    monkeypatch.setattr(S3Path, "_fetch_listing", fake_fetch)

    children = [str(c) async for c in _s3("s3://bucket/allowed").iterdir()]

    assert children == ["s3://bucket/allowed/ok.txt", "s3://bucket/allowed/sub/"]
    assert "not a direct child" in caplog.text


async def test_recursive_delete_stays_inside_the_prefix(monkeypatch):
    deleted: list[str] = []

    async def fake_fetch(self):
        if str(self) == "s3://bucket/allowed":
            return ["s3://bucket/allowed/../outside.txt", "s3://bucket/allowed/inside.txt"]
        return []

    async def fake_unlink(self, missing_ok=False):
        deleted.append(str(self))

    async def not_dir(self):
        return False

    monkeypatch.setattr(S3Path, "_fetch_listing", fake_fetch)
    monkeypatch.setattr(S3Path, "unlink", fake_unlink)
    monkeypatch.setattr(S3Path, "is_dir", not_dir)

    await _s3("s3://bucket/allowed").rmdir(recursive=True)

    assert deleted == ["s3://bucket/allowed/inside.txt"]


async def test_artifactory_listing_skips_dot_segment_names():
    path = ArtifactoryPath(f"{TRUSTED}/dir", token="token")

    class _Batcher:
        async def list(self, **_):
            return [(f"{TRUSTED}/dir/..", True), (f"{TRUSTED}/dir/ok", False)]

    path._get_batcher = lambda: _Batcher()  # type: ignore[method-assign]

    assert [str(c) async for c in path.iterdir()] == [f"{TRUSTED}/dir/ok"]


async def test_http_listing_skips_links_outside_the_listed_directory(monkeypatch):
    async def fake_scrape(url, selector, **_):
        return [
            "http://host.example/dir/ok.txt",
            "http://host.example/dir/sub?x=/../",
            "http://host.example/other/file.txt",
            "http://elsewhere.example/dir/file.txt",
        ]

    monkeypatch.setattr("asanypath.http.http_scrape_links", fake_scrape)

    children = [str(c) async for c in HTTPPath("http://host.example/dir/").iterdir()]

    assert children == ["http://host.example/dir/ok.txt"]
