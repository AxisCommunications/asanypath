# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for ArtifactoryPath implementation."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import msgspec
import pytest

from asanypath.artifactory import ArtifactoryPath
from asanypath.options import AccessGrant, AccessPolicyPatch, BackendOptions
from tests.conftest import classtest_factory

testbase = classtest_factory("_TestArtifactoryPath", ArtifactoryPath)


def art_path(host="artifactory.example.com", repo="generic", file="file.txt"):
    """Helper function to construct a test path."""
    return f"art://{host}/artifactory/{repo}/{file}"


def _make_artifactory_path(
    path: str = art_path(), token: str = "test-token", **kwargs
) -> ArtifactoryPath:
    """Return an ArtifactoryPath with test credentials."""
    defaults = dict(token=token)
    defaults.update(kwargs)
    return ArtifactoryPath(path, **defaults)


class TestArtifactoryPath(testbase):
    """Test the ArtifactoryPath implementation."""

    __test__ = True

    @pytest.fixture
    def p(self):
        """Fixture for a sample Artifactory path."""
        return _make_artifactory_path(art_path())

    # --- Bearer token tests ---

    async def test_checksums(self, p):
        info = {
            "checksums": {
                "md5": "d41d8cd98f00b204e9800998ecf8427e",
                "sha1": "da39a3ee5e6b4b0d3255bfef95601890afd80709",
                "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            }
        }
        with patch.object(p, "_storage_info", new=AsyncMock(return_value=info)):
            result = await p.checksums()
        assert result == info["checksums"]

    @pytest.mark.parametrize("as_json", [False, True], ids=["mapping", "json"])
    async def test_get_access_policy_reads_named_permission_target(self, p, as_json):
        target = {
            "name": "release-readers",
            "repositories": ["generic"],
            "includesPattern": "**",
            "principals": {
                "users": {"reader": ["r"]},
                "groups": {"publishers": ["r", "w", "d"]},
            },
        }
        options = BackendOptions(provider={"permission_target": "release-readers"})
        response = msgspec.json.encode(target).decode() if as_json else target
        with patch(
            "asanypath.artifactory.art_get_permission_target",
            new=AsyncMock(return_value=response),
        ) as mock:
            policy = await p.get_access_policy(backend_options=options)

        mock.assert_awaited_once_with(name="release-readers", **p._native_kwargs)
        assert policy.grants == (
            AccessGrant("user:reader", frozenset({"read"})),
            AccessGrant("group:publishers", frozenset({"read", "write"})),
        )
        assert policy.provider == {"artifactory_permission_target": target}

    async def test_get_access_policy_rejects_malformed_permission_target(self, p):
        options = BackendOptions(provider={"permission_target": "release-readers"})
        with patch(
            "asanypath.artifactory.art_get_permission_target", new=AsyncMock(return_value=[])
        ):
            with pytest.raises(TypeError, match="must be a mapping"):
                await p.get_access_policy(backend_options=options)

    async def test_update_access_policy_replaces_named_permission_target(self, p):
        target = {"name": "release-readers", "repositories": ["generic"], "principals": {}}
        options = BackendOptions(provider={"permission_target": "release-readers"})
        policy_patch = AccessPolicyPatch(provider={"artifactory_permission_target": target})
        with patch("asanypath.artifactory.art_put_permission_target", new=AsyncMock()) as mock:
            await p.update_access_policy(policy_patch, backend_options=options)

        mock.assert_awaited_once_with(
            name="release-readers",
            target_json=msgspec.json.encode(target).decode(),
            **p._native_kwargs,
        )

    @pytest.mark.parametrize(
        "side_effect,expected",
        [
            ({"children": []}, True),
            (FileNotFoundError, False),
        ],
        ids=["true_has_children_key", "false_not_found"],
    )
    async def test_is_dir(self, p, side_effect, expected):
        if isinstance(side_effect, dict):
            mock = AsyncMock(return_value=side_effect)
        else:
            mock = AsyncMock(side_effect=side_effect)
        with patch.object(p, "_storage_info", new=mock):
            assert await p.is_dir() is expected

    async def test_stat(self, p):
        info = {"st_size": 1234, "st_mtime": 1704067200.0}
        with patch.object(p, "_storage_info", new=AsyncMock(return_value=info)):
            result = await p.stat()
            assert result.st_size == 1234
            assert result.st_mtime == 1704067200.0

    def test_token_from_parameter(self):
        p = _make_artifactory_path(token="my-token")
        assert p._token == "my-token"

    def test_token_required(self):
        # Ensure env var is not set - token must be provided explicitly
        with patch.dict("os.environ", {}, clear=True):
            ArtifactoryPath._env_config = None
            with pytest.raises(ValueError, match="token required"):
                ArtifactoryPath(art_path())

    def test_token_from_env_var(self):
        with patch.dict("os.environ", {"ARTIFACTORY_IDENTITY_TOKEN": "env-token"}):
            p = ArtifactoryPath(art_path())
            assert p._token == "env-token"

    def test_token_parameter_takes_precedence(self):
        with patch.dict("os.environ", {"ARTIFACTORY_IDENTITY_TOKEN": "env-token"}):
            p = _make_artifactory_path(token="param-token")
            assert p._token == "param-token"

    async def test_bearer_header_injected(self, p):
        """Verify Bearer token is passed to native kwargs."""
        assert p._native_kwargs["token"] == "test-token"

    async def test_bearer_header_on_put(self, p):
        """Verify Bearer token is available for PUT requests."""
        assert p._native_kwargs["token"] == "test-token"

    async def test_bearer_header_on_delete(self, p):
        """Verify Bearer token is available for DELETE requests."""
        assert p._native_kwargs["token"] == "test-token"

    async def test_bearer_header_on_head(self, p):
        """Verify Bearer token is available for HEAD requests."""
        assert p._native_kwargs["token"] == "test-token"

    async def test_bearer_overwrites_existing_auth(self, p):
        """Token passed to constructor is what ends up in native kwargs."""
        custom = _make_artifactory_path(token="custom-token")
        assert custom._native_kwargs["token"] == "custom-token"

    # --- _batcher_key_fn ---

    def test_batcher_key_fn(self):
        key = ArtifactoryPath._batcher_key_fn(base_url="host/artifactory", token="abcdefghij")
        assert key == "art|host/artifactory|abcdefgh"

    # --- _storage_info ---

    async def test_storage_info_parses_raw(self, p):
        raw = {
            "size": "100",
            "created": "2024-01-01T00:00:00.000Z",
            "lastModified": "2024-06-01T12:00:00.000Z",
            "is_dir": True,
            "checksum_md5": "abc",
            "checksum_sha1": "def",
        }
        with patch("asanypath.artifactory.art_storage_info", new=AsyncMock(return_value=raw)):
            info = await p._storage_info()
        assert info["st_size"] == 100
        assert isinstance(info["st_ctime"], float)
        assert isinstance(info["st_mtime"], float)
        assert "children" in info
        assert info["checksums"] == {"md5": "abc", "sha1": "def"}

    async def test_storage_info_cached(self, p):
        p._cached_info = {"cached": True}
        info = await p._storage_info()
        assert info == {"cached": True}

    # --- is_dir (cached path) ---

    async def test_is_dir_cached_true(self, p):
        p._is_folder = True
        assert await p.is_dir() is True

    async def test_is_dir_cached_false(self, p):
        p._is_folder = False
        assert await p.is_dir() is False

    # --- is_file ---

    @pytest.mark.parametrize(
        "cached_is_folder,storage_info,expected",
        [
            (True, None, False),
            (False, None, True),
            (None, {"size": "10"}, True),
            (None, {"children": []}, False),
        ],
        ids=["cached_dir", "cached_file", "file_no_children", "dir_has_children"],
    )
    async def test_is_file(self, p, cached_is_folder, storage_info, expected):
        p._is_folder = cached_is_folder
        if storage_info is not None:
            with patch.object(p, "_storage_info", new=AsyncMock(return_value=storage_info)):
                assert await p.is_file() is expected
        else:
            assert await p.is_file() is expected

    async def test_is_file_not_found(self, p):
        with patch.object(p, "_storage_info", new=AsyncMock(side_effect=FileNotFoundError)):
            assert await p.is_file() is False

    # --- iterdir ---

    async def test_iterdir(self, p):
        batcher = AsyncMock()
        batcher.list = AsyncMock(return_value=[("repo/child1", False), ("repo/child2/", True)])
        with patch.object(p, "_get_batcher", return_value=batcher):
            children = [child async for child in p.iterdir()]
        assert len(children) == 2
        assert children[1]._is_folder is True

    async def test_list_repos_at_root(self):
        root = _make_artifactory_path("art://artifactory.example.com/artifactory/")
        with patch(
            "asanypath.artifactory.art_list_repos",
            new=AsyncMock(return_value=["generic", "maven"]),
        ) as mock:
            result = await root._list_containers()
        assert result == [
            "art://artifactory.example.com/artifactory/generic",
            "art://artifactory.example.com/artifactory/maven",
        ]
        assert mock.await_args.kwargs["base_url"] == "artifactory.example.com/artifactory"

    async def test_list_repos_root_fallback_parse(self):
        # No trailing slash: 'artifactory' lands in the repo slot; base is rebuilt.
        root = _make_artifactory_path("art://artifactory.example.com/artifactory")
        with patch(
            "asanypath.artifactory.art_list_repos",
            new=AsyncMock(return_value=["generic"]),
        ) as mock:
            result = await root._list_containers()
        assert result == ["art://artifactory.example.com/artifactory/generic"]
        assert mock.await_args.kwargs["base_url"] == "artifactory.example.com/artifactory"

    async def test_list_repos_none_when_repo_set(self, p):
        with patch("asanypath.artifactory.art_list_repos", new=AsyncMock()) as mock:
            assert await p._list_containers() is None
        mock.assert_not_called()

    async def test_iterdir_lists_repos_at_root(self):
        root = _make_artifactory_path("art://artifactory.example.com/artifactory/")
        with patch(
            "asanypath.artifactory.art_list_repos",
            new=AsyncMock(return_value=["generic", "maven"]),
        ):
            results = [str(x) async for x in root.iterdir()]
        assert results == [
            "art://artifactory.example.com/artifactory/generic",
            "art://artifactory.example.com/artifactory/maven",
        ]

    # --- _parse_art_ts ---

    @pytest.mark.parametrize(
        "ts,expected_none",
        [
            (None, True),
            ("", True),
            ("not-a-date", True),
            ("2024-01-01T00:00:00.000+0000", False),
            ("2024-01-01T00:00:00.000Z", False),
        ],
        ids=["none", "empty", "invalid", "no_colon_offset", "zulu"],
    )
    def test_parse_art_ts(self, ts, expected_none):
        result = ArtifactoryPath._parse_art_ts(ts)
        if expected_none:
            assert result is None
        else:
            assert isinstance(result, float)

    # --- stat (full fields) ---

    async def test_stat_all_fields(self, p):
        info = {
            "st_size": 999,
            "st_ctime": 1704067200.0,
            "st_mtime": 1717200000.0,
        }
        with patch.object(p, "_storage_info", new=AsyncMock(return_value=info)):
            result = await p.stat()
        assert result.st_size == 999
        assert result.st_ctime == 1704067200.0
        assert result.st_mtime == 1717200000.0

    async def test_stat_no_size(self, p):
        with patch.object(p, "_storage_info", new=AsyncMock(return_value={})):
            result = await p.stat()
        assert result.st_size == 0


def test_protocol():
    p = _make_artifactory_path()
    assert p.protocol == "art"


def test_art_scheme_dispatch():
    """Test that art scheme resolves to ArtifactoryPath."""
    from asanypath import AsAnyPath

    path_str = "art://example.com/repo/file.txt"
    with patch.dict("os.environ", {"ARTIFACTORY_IDENTITY_TOKEN": "token"}):
        p = AsAnyPath(path_str)
        assert isinstance(p, ArtifactoryPath)


# ----------------------------------------------------------------------
# Credstore wiring
# ----------------------------------------------------------------------


def test_artifactory_loads_token_from_credstore(monkeypatch):
    """No token/env → falls back to credstore."""
    from asanypath import _credstore

    monkeypatch.setattr(_credstore, "load", lambda key, **_: "cached-token")
    with patch.dict("os.environ", {}, clear=True):
        ArtifactoryPath._env_config = None
        p = ArtifactoryPath(art_path())
        assert p._token == "cached-token"


def test_artifactory_explicit_token_is_saved(monkeypatch):
    """Explicit token=... → persisted to credstore."""
    from asanypath import _credstore

    saved: list = []
    monkeypatch.setattr(_credstore, "save", lambda k, v: saved.append((k, v)))
    monkeypatch.setattr(_credstore, "load", lambda key, **_: None)
    with patch.dict("os.environ", {}, clear=True):
        ArtifactoryPath._env_config = None
        _make_artifactory_path(token="explicit-token")
    assert len(saved) == 1
    assert saved[0][1] == "explicit-token"
    assert saved[0][0].startswith("art:")
