"""CLI backend options: ``scheme.key=value`` mapped onto path constructor kwargs."""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Iterable
from typing import Any

from asanypath.asanypath import PROTOCOL_MAP, _match_base_url_flavor

Options = dict[str, dict[str, Any]]


def _header_option(value: str) -> tuple[str, str]:
    name, separator, header_value = value.partition(":")
    name = name.strip()
    if not separator or not name:
        raise ValueError("expected HEADER: VALUE")
    if "\r" in name or "\n" in name or "\r" in header_value or "\n" in header_value:
        raise ValueError("header names and values cannot contain newlines")
    return name, header_value.lstrip()


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
        if name == "headers":
            keys[name] = _header_option
            continue
        ann = str(param.annotation)
        keys[name] = int if re.search(r"\bint\b", ann) and "str" not in ann else str
    return keys


def parse_options(specs: Iterable[str]) -> Options:
    """Parse ``key=value`` or ``scheme.key=value`` backend options."""
    parsed: Options = {}
    for spec in specs:
        lhs, eq, value = spec.partition("=")
        scheme, dot, key = lhs.partition(".")
        if not eq or not lhs:
            raise ValueError(f"invalid option {spec!r}; expected KEY=VALUE")
        if not dot:
            key = lhs
            if not any(key in option_keys(cls) for cls in PROTOCOL_MAP.values()):
                raise ValueError(f"unknown backend option {key!r}")
            scheme = "*"
        elif not key:
            raise ValueError(f"invalid option {spec!r}; expected SCHEME.KEY=VALUE")
        scheme = scheme.lower()
        if scheme != "*" and scheme not in PROTOCOL_MAP:
            raise ValueError(f"unknown protocol {scheme!r} in option {spec!r}")
        keys = option_keys(PROTOCOL_MAP[scheme]) if scheme != "*" else None
        if keys is not None and key not in keys:
            valid = ", ".join(sorted(keys)) or "none"
            raise ValueError(f"{scheme} does not support option {key!r} (valid: {valid})")
        if key == "headers":
            try:
                converted = _header_option(value)
            except ValueError:
                label = f"{scheme}.{key}" if scheme != "*" else key
                raise ValueError(f"invalid value {value!r} for {label}") from None
        elif scheme == "*":
            converted = value
        else:
            try:
                converted = keys[key](value)
            except ValueError:
                raise ValueError(f"invalid value {value!r} for {scheme}.{key}") from None
        scheme_options = parsed.setdefault(scheme, {})
        if key == "headers":
            scheme_options.setdefault(key, []).append(converted)
        else:
            scheme_options[key] = converted
    return parsed


def kwargs_for(path: str, options: Options) -> dict[str, Any]:
    """Return the options that apply to the backend serving ``path``."""
    if not options:
        return {}
    flavor = _match_base_url_flavor(path)
    if flavor is None:
        return {}
    merged: dict[str, Any] = {}

    def apply(values: dict[str, Any], *, coerce: bool) -> None:
        keys = option_keys(flavor)
        for key, value in values.items():
            if key not in keys:
                continue
            if coerce and key != "headers":
                try:
                    value = keys[key](value)
                except ValueError:
                    raise ValueError(f"invalid value {value!r} for {key}") from None
            if key == "headers":
                merged.setdefault(key, []).extend(value)
            else:
                merged[key] = value

    if "*" in options:
        apply(options["*"], coerce=True)
    for scheme, values in options.items():
        if scheme != "*" and PROTOCOL_MAP[scheme] is flavor:
            apply(values, coerce=False)
    return merged
