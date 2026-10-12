# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Sidecar state for resumable downloads.

A partial download lives in ``<dst>.part`` with a ``<dst>.part.meta`` sidecar
recording the remote object's identity (ETag/Last-Modified) and total size. On
resume the identity is re-checked so a changed upstream object restarts cleanly
rather than stitching mismatched bytes. State is plain files — no dependency on
the optional download cache.
"""

from __future__ import annotations

from pathlib import Path

import msgspec


class ResumeMeta(msgspec.Struct, frozen=True):
    """Identity and size of the object a ``.part`` file is downloading."""

    identity: str | None
    size: int


_encoder = msgspec.json.Encoder()
_decoder = msgspec.json.Decoder(ResumeMeta)


def read_meta(path: Path) -> ResumeMeta | None:
    """Return the sidecar meta at *path*, or ``None`` if missing/corrupt."""
    try:
        return _decoder.decode(path.read_bytes())
    except (OSError, msgspec.DecodeError):
        return None


def write_meta(path: Path, meta: ResumeMeta) -> None:
    path.write_bytes(_encoder.encode(meta))
