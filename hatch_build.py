# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from hatchling.metadata.plugin.interface import MetadataHookInterface


class PinNativeHook(MetadataHookInterface):
    """Provide dependencies, pinning asanypath-native to asanypath's minor version."""

    def update(self, metadata: dict) -> None:
        major, minor, *_ = metadata["version"].split(".")
        metadata["dependencies"] = [
            "anyio>=4",
            f"asanypath-native~={major}.{minor}.0",
            "msgspec>=0.21.1",
            "yarl>=1.2",
        ]
