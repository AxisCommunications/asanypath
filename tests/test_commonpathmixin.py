# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Test CommonPurePathMixin shared path operations across all backends."""

from unittest.mock import patch

import pytest

from asanypath.artifactory import ArtifactoryPath
from asanypath.azure import AzurePath
from asanypath.common import _get_base_url_prefix
from asanypath.exceptions import InvalidPathError, UnsupportedProtocolError
from asanypath.gcs import GCSPath
from asanypath.s3 import S3Path

# ---------------------------------------------------------------------------
# Parametrized factory per backend
# ---------------------------------------------------------------------------


def _make_s3(p):
    return S3Path(
        p,
        endpoint_url="https://s3.amazonaws.com",
        aws_region="us-east-1",
        aws_access_key_id="AKID",
        aws_secret_access_key="SECRET",
    )


def _make_gcs(p):
    return GCSPath(p, endpoint_url="https://storage.googleapis.com", access_token="tok")


def _make_az(p):
    return AzurePath(
        p,
        account_name="dev",
        account_key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        endpoint_url="https://dev.blob.core.windows.net",
    )


def _make_art(p):
    return ArtifactoryPath(p, token="tok")


_CASES = [
    pytest.param((_make_s3, "s3://bucket/nested/file.txt"), id="s3"),
    pytest.param((_make_gcs, "gs://bucket/nested/file.txt"), id="gcs"),
    pytest.param((_make_az, "az://container/nested/file.txt"), id="azure"),
    pytest.param((_make_art, "art://host/artifactory/repo/file.txt"), id="artifactory"),
]


@pytest.fixture(params=_CASES)
def path_and_factory(request):
    factory, uri = request.param
    return factory(uri), factory


class TestCommonPurePath:
    """Test shared pure-path operations across all cloud backends."""

    def test_str(self, path_and_factory):
        p, _ = path_and_factory
        assert str(p).startswith(p.protocol + "://")

    def test_eq(self, path_and_factory):
        p, factory = path_and_factory
        assert p == factory(str(p))
        assert p == str(p)
        assert p != 123

    def test_hash(self, path_and_factory):
        p, factory = path_and_factory
        q = factory(str(p))
        assert hash(p) == hash(q)
        assert {p, q} == {p}
        # Usable as dict key
        d = {p: "value"}
        assert d[q] == "value"

    def test_ordering(self, path_and_factory):
        p, factory = path_and_factory
        proto = p.protocol
        a = factory(f"{proto}://bucket/a.txt")
        b = factory(f"{proto}://bucket/b.txt")
        assert a < b
        assert a <= b
        assert b > a
        assert b >= a
        assert a <= factory(f"{proto}://bucket/a.txt")

    def test_sorted(self, path_and_factory):
        p, factory = path_and_factory
        proto = p.protocol
        paths = [
            factory(f"{proto}://bucket/c.txt"),
            factory(f"{proto}://bucket/a.txt"),
            factory(f"{proto}://bucket/b.txt"),
        ]
        assert [str(q) for q in sorted(paths)] == sorted(str(q) for q in paths)

    def test_ordering_notimplemented(self, path_and_factory):
        p, _ = path_and_factory
        with pytest.raises(TypeError):
            _ = p < 123

    def test_repr(self, path_and_factory):
        p, _ = path_and_factory
        assert type(p).__name__ in repr(p)
        assert str(p) in repr(p)

    def test_truediv(self, path_and_factory):
        p, _ = path_and_factory
        child = p / "child.txt"
        assert child.name == "child.txt"
        assert isinstance(child, type(p))

    def test_rtruediv(self, path_and_factory):
        p, factory = path_and_factory
        root = f"{p.protocol}://root"
        result = root / p
        assert isinstance(result, type(p))

    def test_name(self, path_and_factory):
        p, _ = path_and_factory
        assert p.name == "file.txt"

    def test_stem(self, path_and_factory):
        p, _ = path_and_factory
        assert p.stem == "file"

    def test_suffix(self, path_and_factory):
        p, _ = path_and_factory
        assert p.suffix == ".txt"

    def test_suffixes(self, path_and_factory):
        p, _ = path_and_factory
        assert p.suffixes == [".txt"]

    def test_parent(self, path_and_factory):
        p, _ = path_and_factory
        assert isinstance(p.parent, type(p))
        assert p.name not in str(p.parent)

    def test_parents(self, path_and_factory):
        p, _ = path_and_factory
        parents = p.parents
        assert len(parents) >= 1
        assert all(isinstance(par, type(p)) for par in parents)

    def test_parts(self, path_and_factory):
        p, _ = path_and_factory
        parts = p.parts
        assert parts[0] == p.drive
        assert parts[-1] == p.name

    def test_drive(self, path_and_factory):
        p, _ = path_and_factory
        assert p.drive == f"{p.protocol}://"

    def test_root(self, path_and_factory):
        p, _ = path_and_factory
        assert p.root  # non-empty

    def test_anchor(self, path_and_factory):
        p, _ = path_and_factory
        assert p.anchor == p.drive + p.root

    def test_as_posix(self, path_and_factory):
        p, _ = path_and_factory
        posix = p.as_posix()
        assert posix.startswith("/")
        assert p.name in posix

    def test_as_uri(self, path_and_factory):
        p, _ = path_and_factory
        assert p.as_uri() == str(p)

    def test_is_absolute(self, path_and_factory):
        p, _ = path_and_factory
        assert p.is_absolute() is True

    def test_joinpath(self, path_and_factory):
        p, _ = path_and_factory
        joined = p.joinpath("extra")
        assert "extra" in str(joined)
        assert isinstance(joined, type(p))

    def test_match(self, path_and_factory):
        p, _ = path_and_factory
        assert p.match("*.txt")
        assert not p.match("*.md")

    def test_with_name(self, path_and_factory):
        p, _ = path_and_factory
        renamed = p.with_name("other.md")
        assert renamed.name == "other.md"
        assert isinstance(renamed, type(p))

    def test_with_parent(self, path_and_factory):
        p, factory = path_and_factory
        replacement_parent = factory(f"{p.protocol}://bucket/replacement")

        result = p.with_parent(replacement_parent)

        assert result.parent == replacement_parent
        assert result.name == p.name

    def test_with_stem(self, path_and_factory):
        p, _ = path_and_factory
        renamed = p.with_stem("newfile")
        assert renamed.stem == "newfile"
        assert renamed.suffix == ".txt"

    def test_with_suffix(self, path_and_factory):
        p, _ = path_and_factory
        renamed = p.with_suffix(".md")
        assert renamed.suffix == ".md"
        assert renamed.stem == "file"

    def test_with_segments(self, path_and_factory):
        p, factory = path_and_factory
        new_uri = f"{p.protocol}://other/path.txt"
        result = p.with_segments(new_uri)
        assert result.name == "path.txt"
        assert isinstance(result, type(p))

    def test_with_segments_accepts_same_backend_path(self, path_and_factory):
        p, factory = path_and_factory
        replacement = factory(f"{p.protocol}://bucket/replaced/file.md")

        result = p.with_segments(replacement)

        assert result == replacement

    def test_full_match(self, path_and_factory):
        p, _ = path_and_factory
        assert p.full_match("**/*.txt")
        assert not p.full_match("**/*.md")

    def test_full_match_empty_raises(self, path_and_factory):
        p, _ = path_and_factory
        with pytest.raises(ValueError, match="empty pattern"):
            p.full_match("")

    def test_match_empty_raises(self, path_and_factory):
        p, _ = path_and_factory
        with pytest.raises(ValueError, match="empty pattern"):
            p.match("")

    def test_suffix_no_extension(self, path_and_factory):
        p, factory = path_and_factory
        no_ext = p.with_name("Makefile")
        assert no_ext.suffix == ""

    def test_is_relative_to(self, path_and_factory):
        p, factory = path_and_factory
        parent_uri = str(p.parent)
        assert p.is_relative_to(parent_uri)

    def test_is_relative_to_false(self, path_and_factory):
        p, factory = path_and_factory
        assert not p.is_relative_to(f"{p.protocol}://totally/different")

    def test_relative_to(self, path_and_factory):
        p, factory = path_and_factory
        parent = p.parent
        rel = p.relative_to(str(parent))
        assert p.name in str(rel)

    def test_relative_to_raises(self, path_and_factory):
        p, _ = path_and_factory
        with pytest.raises(ValueError, match="is not in the subpath"):
            p.relative_to(f"{p.protocol}://completely/different")

    def test_truediv_cross_protocol_raises(self, path_and_factory):
        p, _ = path_and_factory
        other_scheme = "zz" if p.protocol != "zz" else "yy"
        with pytest.raises(UnsupportedProtocolError):
            p / f"{other_scheme}://other/path"

    def test_truediv_unsupported_type_raises(self, path_and_factory):
        p, _ = path_and_factory
        with pytest.raises(UnsupportedProtocolError):
            p / 123


# ---------------------------------------------------------------------------
# Non-parametrized edge-case tests
# ---------------------------------------------------------------------------


def test_init_none_raises():
    with pytest.raises(InvalidPathError):
        S3Path(
            None,
            endpoint_url="https://s3.amazonaws.com",
            aws_region="us-east-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
        )


def test_get_base_url_prefix_no_scheme():
    """_get_base_url_prefix adds protocol:// when missing from env value."""
    from asanypath.common import _BASE_URL_PREFIX_CACHE

    _BASE_URL_PREFIX_CACHE.clear()
    with patch.dict("os.environ", {"S3_BASE_URL": "my-bucket.s3.amazonaws.com/prefix"}):
        result = _get_base_url_prefix("s3", {})
    assert result == "my-bucket.s3.amazonaws.com/prefix"
    _BASE_URL_PREFIX_CACHE.clear()


def test_get_base_url_prefix_no_netloc():
    """_get_base_url_prefix returns None when parsed URL has no netloc."""
    from asanypath.common import _BASE_URL_PREFIX_CACHE

    _BASE_URL_PREFIX_CACHE.clear()
    with patch.dict("os.environ", {"S3_BASE_URL": "/just/a/path"}):
        result = _get_base_url_prefix("s3", {})
    assert result is None
    _BASE_URL_PREFIX_CACHE.clear()


def test_expand_base_url_http_conversion():
    """_expand_protocol_base_url converts https:// URL matching base to art:// scheme."""
    from asanypath.common import _BASE_URL_PREFIX_CACHE

    _BASE_URL_PREFIX_CACHE.clear()
    with patch.dict("os.environ", {"ARTIFACTORY_BASE_URL": "artifacts.example.com/artifactory"}):
        p = ArtifactoryPath("https://artifacts.example.com/artifactory/repo/file.txt", token="tok")
        assert str(p).startswith("art://")
    _BASE_URL_PREFIX_CACHE.clear()


def test_expand_base_url_shorthand():
    """_expand_protocol_base_url expands short host to full base URL."""
    from asanypath.common import _BASE_URL_PREFIX_CACHE

    _BASE_URL_PREFIX_CACHE.clear()
    with patch.dict("os.environ", {"ARTIFACTORY_BASE_URL": "artifacts.example.com/artifactory"}):
        p = ArtifactoryPath("art://repo/file.txt", token="tok")
        assert str(p) == "art://artifacts.example.com/artifactory/repo/file.txt"
    _BASE_URL_PREFIX_CACHE.clear()


def test_expand_base_url_with_query_fragment():
    """_expand_protocol_base_url preserves query and fragment."""
    from asanypath.common import _BASE_URL_PREFIX_CACHE

    _BASE_URL_PREFIX_CACHE.clear()
    with patch.dict("os.environ", {"ARTIFACTORY_BASE_URL": "artifacts.example.com/artifactory"}):
        p = ArtifactoryPath("art://repo/file.txt?v=1#section", token="tok")
        s = str(p)
        assert "v=1" in s
        assert "section" in s
    _BASE_URL_PREFIX_CACHE.clear()


def test_joinpath(self=None):
    """joinpath delegates to __truediv__."""
    p = _make_s3("s3://bucket/dir")
    joined = p.joinpath("child")
    assert "child" in str(joined)
