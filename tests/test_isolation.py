# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Credentials, caches and listings stay scoped to the path that owns them."""

from __future__ import annotations

import asyncio

import pytest
from yarl import URL

from asanypath import _credstore
from asanypath import cache as cache_module
from asanypath.artifactory import ArtifactoryPath
from asanypath.artifactory import _jfrog_conf_token as real_jfrog_conf_token
from asanypath.azure import AzurePath
from asanypath.batcher import MicroBatcher
from asanypath.cache import DownloadCache
from asanypath.cloud import CloudPathMixin
from asanypath.common import CommonPurePathMixin
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

PW = "pass" + "word"
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


@pytest.mark.parametrize(
    ("cls", "items_key", "secret_key", "make"),
    [
        (ArtifactoryPath, "paths", "token", lambda s: ArtifactoryPath(f"{TRUSTED}/{s}", token=s)),
        (S3Path, "keys", "secret_key", lambda s: _s3(f"s3://bucket/{s}", secret=s)),
    ],
    ids=["artifactory", "s3"],
)
async def test_calls_with_different_secrets_are_not_batched(
    monkeypatch, cls, items_key, secret_key, make
):
    async def fake_get(use_h2, **kwargs):
        return [kwargs[secret_key].encode() for _ in kwargs[items_key]]

    batch = {"batch_fn": fake_get, "items_key": items_key, "wrap": bytes}
    monkeypatch.setitem(cls._batcher_ops, "get", batch)
    # Secrets sharing a prefix (or an access key) must still not share a batch.
    secrets = ["abcdefgh-admin", "abcdefgh-reader"]

    results = await asyncio.gather(*(make(s).read_bytes() for s in secrets))

    assert results == [s.encode() for s in secrets]


# ---------------------------------------------------------------------------
# Credential scoping
# ---------------------------------------------------------------------------


def test_artifactory_token_scoping():
    path = ArtifactoryPath(f"{TRUSTED}/dir/file", token="first")
    ArtifactoryPath(f"{TRUSTED}/dir/file", token="second")

    derived = [path / "c", path.parent, path.with_name("o"), path.with_segments(f"{TRUSTED}/e")]
    assert [p._token for p in derived] == ["first"] * len(derived)
    for elsewhere in (
        lambda: ArtifactoryPath(f"{OTHER}/f"),
        lambda: path.with_segments(f"{OTHER}/f"),
    ):
        with pytest.raises(ValueError, match="token required"):
            elsewhere()


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


@pytest.mark.parametrize("cls", [SSHPath, FTPPath])
def test_host_bound_credentials_stay_with_their_host(cls):
    scheme = cls.protocol
    path = cls(f"{scheme}://a.example/dir/file", username="alice", **{PW: "pw"})

    assert [p._password for p in (path / "child", path.parent)] == ["pw", "pw"]
    assert cls(f"{scheme}://b.example/file")._password != "pw"
    assert path.with_segments(f"{scheme}://b.example/file")._password != "pw"


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
    [
        "../outside.txt",
        "%2E%2E/outside.txt",
        "sub/../../outside.txt",
        "a%2Fb",
        "",
        "sub?x=/../",
        "..\t",
        ".\n.",
        "\r",
        "sub/..\t/..\t/",
    ],
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


@pytest.mark.parametrize(
    "uri",
    [
        f"{TRUSTED}/dir/file",
        "art://trusted.example:8443/artifactory/repo/a",
        "art://localhost/artifactory/repo/a",
        "art://trusted.example/artifactory",
    ],
)
@pytest.mark.parametrize("join", ["x", "/x", "//x", "?q=/../x", "#f", "..", "../../.."])
def test_derive_shortcut_agrees_with_probe(uri, join):
    path = ArtifactoryPath(uri, token="trusted-token")

    for derived in (path / join, path.parent, *path.parents):
        probe = object.__new__(ArtifactoryPath)
        CommonPurePathMixin.__init__(probe, str(derived))
        same = probe._path.raw_authority == path._path.raw_authority
        assert derived._init_options == ({"token": "trusted-token"} if same else {})


def test_shorthand_host_still_probes_on_join(monkeypatch):
    monkeypatch.setenv("ARTIFACTORY_BASE_URL", "base.example/artifactory")
    monkeypatch.setattr("asanypath.common._BASE_URL_PREFIX_CACHE", {})
    path = ArtifactoryPath("art://trusted.example/artifactory/repo", token="trusted-token")
    path._path = path._path.with_host("rti")

    assert not path._keeps_authority((str(path), "x"))


@pytest.mark.parametrize("parent", ["s3://bucket/dir/sub", "s3://bucket/dir/sub/", "s3://bucket"])
def test_plain_listing_names_parse_to_a_direct_child(parent):
    path = _s3(parent)
    chars = [chr(c) for c in range(0x00, 0x80)] + ["ü", "日本", "e\u0301", "\u200b"]
    names = [form for c in chars for form in (c, f"a{c}b", f"{c}x/", f"..{c}", f"{c}..")]
    for name in names:
        uri = f"{parent.rstrip('/')}/{name}"
        tail = path._plain_child_name(uri)
        if tail is not None:
            assert URL(uri, encoded="?" in uri) == path._path / tail, repr(name)
            assert path._is_direct_child(path._child(uri)), repr(name)


# ---------------------------------------------------------------------------
# SSH host keys and sessions
# ---------------------------------------------------------------------------

_KNOWN = "{home}/.ssh/known_hosts"


@pytest.fixture
def ssh_env(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("SSH_CONFIG", "")
    monkeypatch.delenv("SSH_KNOWN_HOSTS", raising=False)
    SSHPath._env_config = None
    yield tmp_path
    SSHPath._env_config = None


@pytest.mark.parametrize(
    ("env", "kwargs", "config", "expected"),
    [
        (None, {}, None, ("ask", _KNOWN)),
        ("none", {}, None, ("disabled", "")),
        ("/pinned", {}, None, ("ask", "/pinned")),
        (None, {"known_hosts": "/kwarg"}, None, ("ask", "/kwarg")),
        (None, {"known_hosts": ()}, None, ("disabled", "")),
        (None, {}, ("Accept-New", "~/pinned"), ("accept-new", "{home}/pinned")),
        (None, {}, (None, "%d/kh-%r@%h-%p%%"), ("ask", "{home}/kh-deploy@real.example-2222%")),
        # Unrecognised StrictHostKeyChecking values fail closed.
        (None, {}, ("disabled", None), ("yes", _KNOWN)),
        (None, {}, ("maybe", None), ("yes", _KNOWN)),
        (None, {}, ("none", None), ("yes", _KNOWN)),
    ],
)
def test_ssh_host_key_settings(ssh_env, monkeypatch, env, kwargs, config, expected):
    if env is not None:
        monkeypatch.setenv("SSH_KNOWN_HOSTS", env)
    if config is not None:
        monkeypatch.setenv("SSH_CONFIG", "/cfg")
        resolved = ("real.example", 2222, "deploy", None, *config)
        monkeypatch.setattr("asanypath.ssh.ssh_resolve_config", lambda host, paths: resolved)

    native = SSHPath("ssh://alias/f", **kwargs)._native_kwargs

    policy, known_hosts = expected
    assert (native["host_key_policy"], native["known_hosts"]) == (
        policy,
        known_hosts.format(home=ssh_env),
    )


@pytest.mark.parametrize("host", ["a,b", "a b", "*.example", "a\nb", "[a]", ""])
def test_ssh_host_keys_are_only_recorded_for_plain_host_names(tmp_path, host):
    from asanypath_native import ssh_learn_host_key

    key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ"
    with pytest.raises(RuntimeError, match="not a plain host name"):
        ssh_learn_host_key(host, 22, key, str(tmp_path / "kh"))
    assert not (tmp_path / "kh").exists()


def test_ftp_sessions_are_keyed_by_credentials():
    def key(secret):
        return FTPPath("ftp://u@a.example/f", username="u", **{PW: secret})._client_key

    assert key("right") == key("right")
    assert key("right") != key("wrong")
    assert not any("right" in str(part) for part in key("right"))


# ---------------------------------------------------------------------------
# Ambient credentials: only for the service they are configured for
# ---------------------------------------------------------------------------


@pytest.fixture
def ambient(tmp_path, monkeypatch):
    import json

    from asanypath import artifactory

    for name in ("HTTP_AUTH_USER", "HTTP_AUTH_PASSWORD", "HTTP_AUTH_HEADER", "HTTP_AUTH_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("asanypath.common._BASE_URL_PREFIX_CACHE", {})
    monkeypatch.setattr("asanypath.cloud._UNSCOPED_WARNED", set())
    monkeypatch.setattr(ArtifactoryPath, "_env_config", None)
    monkeypatch.setattr(artifactory, "_jfrog_conf_token", real_jfrog_conf_token)
    monkeypatch.setenv("JFROG_CLI_HOME", str(tmp_path))
    servers = [
        {"url": "https://corp.jfrog.io/", "accessToken": "CORP", "isDefault": True},
        {"url": "https://art.example:8443/", "accessToken": "PORTED"},
    ]
    (tmp_path / "jfrog-cli.conf.v6").write_text(json.dumps({"servers": servers}))
    return monkeypatch


_ART_ENV = {
    "ARTIFACTORY_IDENTITY_TOKEN": "ambient",
    "ARTIFACTORY_BASE_URL": "corp.example/artifactory",
}


@pytest.mark.parametrize(
    ("env", "uri", "token"),
    [
        ({}, "art://corp.jfrog.io/artifactory/repo/f", "CORP"),
        ({}, "art://CORP.jfrog.io:443/artifactory/repo/f", "CORP"),
        ({}, "art://art.example:8443/artifactory/repo/f", "PORTED"),
        ({}, "art://attacker.example/artifactory/repo/f", None),
        ({}, "art://rog.io/artifactory/repo/f", None),
        ({}, "art://art.example/artifactory/repo/f", None),
        (_ART_ENV, "art://corp.example/artifactory/repo/f", "ambient"),
        (_ART_ENV, "art://attacker.example/artifactory/repo/f", None),
        ({"ARTIFACTORY_IDENTITY_TOKEN": "ambient"}, "art://any.example/artifactory/repo/f", None),
    ],
)
def test_ambient_artifactory_tokens(ambient, env, uri, token):
    for name, value in env.items():
        ambient.setenv(name, value)

    if token is None:
        with pytest.raises(ValueError, match="token required"):
            ArtifactoryPath(uri)
    else:
        assert ArtifactoryPath(uri)._token == token


@pytest.mark.parametrize(
    ("service", "url", "sent"),
    [
        ("https://svc.example", "https://svc.example/data/x", True),
        ("https://svc.example/api", "https://SVC.example:443/other", True),
        ("http://svc.example:8080", "http://svc.example:8080/x", True),
        ("https://svc.example", "http://svc.example/x", False),
        ("https://svc.example", "https://svc.example:8443/x", False),
        ("https://svc.example", "https://attacker.example/x", False),
        ("https://svc.example", "https://svc.example.attacker.example/x", False),
        (None, "https://svc.example/x", False),
    ],
)
def test_ambient_http_credentials(ambient, service, url, sent):
    from asanypath.http import HTTPPath, HTTPSPath

    ambient.setenv("HTTP_AUTH_TOKEN", "service-token")
    if service:
        ambient.setenv("HTTP_AUTH_URL", service)
    cls = HTTPSPath if url.startswith("https") else HTTPPath

    assert bool(cls(url)._auth_headers()) is sent


def test_unscoped_ambient_credentials_warn_once_per_setting(ambient, caplog):
    from asanypath.http import HTTPSPath

    ambient.setenv("HTTP_AUTH_TOKEN", "t")
    ambient.setenv("ARTIFACTORY_IDENTITY_TOKEN", "t")
    for host in ("a.example", "b.example"):
        HTTPSPath(f"https://{host}/x")._auth_headers()
        with pytest.raises(ValueError):
            ArtifactoryPath(f"art://{host}/artifactory/repo/f")

    assert sorted(r.message for r in caplog.records if "is ignored" in r.message) == [
        "ARTIFACTORY_IDENTITY_TOKEN is ignored: set ARTIFACTORY_BASE_URL to the service it is for",
        "HTTP_AUTH_* is ignored: set HTTP_AUTH_URL to the service it is for",
    ]


def test_http_auth_decision_follows_environment_changes(ambient):
    from asanypath.http import HTTPSPath

    ambient.setenv("HTTP_AUTH_TOKEN", "t")
    path = HTTPSPath("https://svc.example/x")
    ambient.setenv("HTTP_AUTH_URL", "https://svc.example")
    assert path._auth_headers()
    ambient.setenv("HTTP_AUTH_URL", "https://elsewhere.example")
    assert path._auth_headers() == []
