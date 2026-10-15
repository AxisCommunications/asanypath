"""CLI backend options: ``scheme.key=value`` mapped onto path constructor kwargs."""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Iterable
from typing import Any

from asanypath.asanypath import PROTOCOL_MAP, _match_base_url_flavor

Options = dict[str, dict[str, Any]]


def option_keys(cls: type) -> dict[str, Callable[[str], Any]]:
    """Return ``{key: coercer}`` for the keyword arguments a path class accepts."""
    keys: dict[str, Callable[[str], Any]] = {}
    for name, param in inspect.signature(cls.__init__).parameters.items():
        if name == "self" or param.kind in (
            param.VAR_POSITIONAL,
            param.VAR_KEYWORD,
            param.POSITIONAL_ONLY,
        ):
            continue
        ann = str(param.annotation)
        keys[name] = int if re.search(r"\bint\b", ann) and "str" not in ann else str
    return keys


def parse_options(specs: Iterable[str]) -> Options:
    """Parse ``scheme.key=value`` specs; raise ValueError on anything unsupported."""
    parsed: Options = {}
    for spec in specs:
        lhs, eq, value = spec.partition("=")
        scheme, dot, key = lhs.partition(".")
        if not eq or not dot or not key:
            raise ValueError(f"invalid option {spec!r}; expected SCHEME.KEY=VALUE")
        scheme = scheme.lower()
        if scheme not in PROTOCOL_MAP:
            raise ValueError(f"unknown protocol {scheme!r} in option {spec!r}")
        keys = option_keys(PROTOCOL_MAP[scheme])
        if key not in keys:
            valid = ", ".join(sorted(keys)) or "none"
            raise ValueError(f"{scheme} does not support option {key!r} (valid: {valid})")
        try:
            parsed.setdefault(scheme, {})[key] = keys[key](value)
        except ValueError:
            raise ValueError(f"invalid value {value!r} for {scheme}.{key}") from None
    return parsed


def kwargs_for(path: str, options: Options) -> dict[str, Any]:
    """Return the options that apply to the backend serving ``path``."""
    if not options:
        return {}
    flavor = _match_base_url_flavor(path)
    merged: dict[str, Any] = {}
    for scheme, values in options.items():
        if PROTOCOL_MAP[scheme] is flavor:
            merged.update(values)
    return merged
