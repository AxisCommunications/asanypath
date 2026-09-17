# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import pytest

from asanypath import AsAnyPath
from asanypath.cloud import CloudPathMixin


@pytest.fixture(autouse=True)
def _clear_listing_cache():
    """Clear cloud listing cache between tests to avoid cross-test contamination."""
    CloudPathMixin._listing_cache.clear()
    yield
    CloudPathMixin._listing_cache.clear()


@pytest.fixture(autouse=True)
def _clear_env_caches(monkeypatch):
    """Clear per-backend env config caches between tests."""
    from asanypath.artifactory import ArtifactoryPath
    from asanypath.azure import AzurePath
    from asanypath.common import _BASE_URL_PREFIX_CACHE
    from asanypath.gcs import GCSPath
    from asanypath.s3 import S3Path

    try:
        from asanypath.ssh import SSHPath  # noqa: N806
    except ImportError:
        SSHPath = None  # type: ignore[assignment,misc]  # noqa: N806

    try:
        from asanypath.ftp import FTPPath, FTPSPath  # noqa: N806
    except ImportError:
        FTPPath = FTPSPath = None  # type: ignore[assignment,misc]  # noqa: N806

    # Deterministic defaults so tests don't pick up the dev's real
    # ~/.ssh/config or ~/.netrc unless they opt in explicitly.
    monkeypatch.setenv("SSH_CONFIG", "")
    monkeypatch.setenv("NETRC", "/nonexistent/netrc")

    GCSPath._env_config = None
    AzurePath._env_config = None
    ArtifactoryPath._env_config = None
    S3Path._env_config = None
    if SSHPath is not None:
        SSHPath._env_config = None
    if FTPPath is not None:
        FTPPath._env_config = None
        FTPSPath._env_config = None
    _BASE_URL_PREFIX_CACHE.clear()
    yield
    GCSPath._env_config = None
    AzurePath._env_config = None
    ArtifactoryPath._env_config = None
    S3Path._env_config = None
    if SSHPath is not None:
        SSHPath._env_config = None
    if FTPPath is not None:
        FTPPath._env_config = None
        FTPSPath._env_config = None
    _BASE_URL_PREFIX_CACHE.clear()


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "asyncio: mark test as an asyncio test")


def _fail_test(*args, **kwargs):
    raise AssertionError(
        "This test should not be run directly. It is meant to be inherited by other test classes."
    )


def classtest_factory(name: str, basecls: type) -> type:
    """Factory function to create test classes with default failing tests for abstract methods."""
    return type(
        name,
        (),
        {
            "__test__": False,
            "_base": basecls,
            **{
                f"test_{m}": _fail_test
                for m in sorted(
                    (
                        ({*dir(basecls)} - {*dir(basecls.mro()[1])})
                        if len(basecls.mro()) > 1
                        else {*dir(basecls)}
                    )
                    & {*AsAnyPath.__abstractmethods__}
                )
            },
        },
    )
