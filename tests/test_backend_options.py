"""Tests for CLI backend options (-o scheme.key=value)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
    ["profile=p", "s3.profile", "s3.=x", "nope.key=v", "s3.bogus=v", "ssh.port=abc", "file.x=y"],
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
