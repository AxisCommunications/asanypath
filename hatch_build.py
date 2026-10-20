# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import re

from hatchling.metadata.plugin.interface import MetadataHookInterface


class PinNativeHook(MetadataHookInterface):
    """Provide dependencies, pinning asanypath-native to asanypath's release.

    ``~=X.Y.Z`` requires at least the same patch release (fixes and APIs the
    Python code relies on) while allowing later patches of the same minor.
    """

    def update(self, metadata: dict) -> None:
        major, minor, patch = re.match(r"(\d+)\.(\d+)\.(\d+)", metadata["version"]).groups()
        metadata["dependencies"] = [
            "anyio>=4",
            f"asanypath-native~={major}.{minor}.{patch}",
            "msgspec>=0.22.0",
            "yarl>=1.2",
        ]
