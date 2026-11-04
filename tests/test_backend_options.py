"""Tests for CLI backend options (-o scheme.key=value)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from asanypath._backend_opts import kwargs_for, option_keys, parse_options
from asanypath.cli import cli
from asanypath.s3 import S3Path
from asanypath.ssh import SSHPath


def test_option_keys_follow_constructor_signature():
    keys = option_keys(S3Path)
    assert {"profile", "aws_region", "endpoint_url", "aws_access_key_id"} <= set(keys)
    assert "args" not in keys and "kwargs" not in keys
    assert option_keys(SSHPath)["port"] is int


@pytest.mark.parametrize(
    ("specs", "expected"),
    [
        (["s3.profile=p"], {"s3": {"profile": "p"}}),
        (["s3.profile=p", "s3.profile=q"], {"s3": {"profile": "q"}}),
        (
            ["http.headers=X-Tag: one", "http.headers=X-Tag: two"],
            {"http": {"headers": [("X-Tag", "one"), ("X-Tag", "two")]}},
        ),
        (
            ["headers=X-Tag: one", "headers=X-Tag: two"],
            {"*": {"headers": [("X-Tag", "one"), ("X-Tag", "two")]}},
        ),
        (
            ["S3.aws_region=eu-west-1", "ssh.port=2222"],
            {"s3": {"aws_region": "eu-west-1"}, "ssh": {"port": 2222}},
        ),
        (["s3.endpoint_url=http://h/?a=b"], {"s3": {"endpoint_url": "http://h/?a=b"}}),
    ],
)
def test_parse_options(specs, expected):
    assert parse_options(specs) == expected


@pytest.mark.parametrize(
    "spec",
    [
        "not-an-option=value",
        "s3.profile",
        "s3.=x",
        "nope.key=v",
        "s3.bogus=v",
        "ssh.port=abc",
        "file.x=y",
        "http.headers=missing-colon",
    ],
)
def test_parse_options_rejects(spec):
    with pytest.raises(ValueError):
        parse_options([spec])


def test_kwargs_for_matches_backend_only():
    opts = parse_options(["s3.profile=p", "gs.project_id=proj"])
    assert kwargs_for("s3://b/k", opts) == {"profile": "p"}
    assert kwargs_for("gs://b/k", opts) == {"project_id": "proj"}
    assert kwargs_for("/tmp/x", opts) == {}
    assert kwargs_for("s3://b/k", {}) == {}


def test_kwargs_for_keeps_http_and_https_headers_scheme_scoped():
    opts = parse_options(["http.headers=X-One: 1", "https.headers=X-Two: 2"])
    assert kwargs_for("http://example.com/file", opts) == {"headers": [("X-One", "1")]}
    assert kwargs_for("https://example.com/file", opts) == {"headers": [("X-Two", "2")]}


def test_kwargs_for_infers_backend_for_unqualified_options():
    opts = parse_options(["profile=example", "headers=X-Client: cli", "port=2222"])
    assert kwargs_for("s3://bucket/key", opts) == {"profile": "example"}
    assert kwargs_for("https://example.com/file", opts) == {"headers": [("X-Client", "cli")]}
    assert kwargs_for("ssh://host/path", opts) == {"port": 2222}


def _invoke(args):
    with patch("asanypath.cli.AsAnyPath") as mock_cls:
        mock_cls.return_value.exists = _async_true
        result = CliRunner().invoke(cli, args)
    return result, mock_cls


async def _async_true():
    return True


def test_cli_option_reaches_path_constructor():
    result, mock_cls = _invoke(["exists", "s3://b/k", "-o", "s3.profile=prof"])
    assert result.exit_code == 0, result.output
    mock_cls.assert_called_once_with("s3://b/k", profile="prof")


def test_cli_unqualified_option_is_inferred_from_path():
    result, mock_cls = _invoke(["exists", "s3://b/k", "-o", "profile=prof"])
    assert result.exit_code == 0, result.output
    mock_cls.assert_called_once_with("s3://b/k", profile="prof")


def test_cli_unqualified_headers_are_inferred_from_https_path():
    result, mock_cls = _invoke(
        ["exists", "https://example.com/data", "-o", "headers=X-Client: cli"]
    )
    assert result.exit_code == 0, result.output
    mock_cls.assert_called_once_with("https://example.com/data", headers=[("X-Client", "cli")])


def test_cli_option_other_backend_is_ignored():
    result, mock_cls = _invoke(["exists", "s3://b/k", "-o", "gs.project_id=x"])
    assert result.exit_code == 0, result.output
    mock_cls.assert_called_once_with("s3://b/k")


def test_cli_invalid_option_is_usage_error():
    result, _ = _invoke(["exists", "s3://b/k", "-o", "s3.bogus=1"])
    assert result.exit_code == 2
    assert "bogus" in result.output


def test_cli_src_and_dst_options_are_scoped():
    calls = []

    def fake(path, **kwargs):
        calls.append((path, kwargs))
        return MagicMock()

    with patch("asanypath.cli.AsAnyPath", side_effect=fake):
        CliRunner().invoke(
            cli,
            [
                "cp",
                "s3://a/k",
                "s3://b/k",
                "-o",
                "s3.aws_region=r0",
                "--src-option",
                "s3.profile=src",
                "--dst-option",
                "s3.profile=dst",
            ],
        )
    by_path = dict(calls)
    assert by_path["s3://a/k"] == {"aws_region": "r0", "profile": "src"}
    assert by_path["s3://b/k"] == {"aws_region": "r0", "profile": "dst"}


def test_cli_http_headers_are_repeatable_and_scoped():
    calls = []

    def fake(path, **kwargs):
        calls.append((path, kwargs))
        result = MagicMock()
        result.exists = AsyncMock(return_value=True)
        result.is_dir = AsyncMock(return_value=False)
        result.copy = AsyncMock()
        return result

    with patch("asanypath.cli.AsAnyPath", side_effect=fake):
        result = CliRunner().invoke(
            cli,
            [
                "cp",
                "http://a/file",
                "http://b/file",
                "-o",
                "http.headers=X-Common: all",
                "--src-option",
                "http.headers=X-Side: source",
                "--dst-option",
                "http.headers=X-Side: destination",
            ],
        )

    assert result.exit_code == 0, result.output
    by_path = dict(calls)
    assert by_path["http://a/file"] == {"headers": [("X-Common", "all"), ("X-Side", "source")]}
    assert by_path["http://b/file"] == {"headers": [("X-Common", "all"), ("X-Side", "destination")]}
