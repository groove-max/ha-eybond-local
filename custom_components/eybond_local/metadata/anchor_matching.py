"""The single semantic for matching anchor conditions against evidence.

Anchor evaluation used to exist three times (value-based engine, decision
tree signature matchers, and a drifted probe-planner copy). The planner and
the value engine are gone; this module is where the remaining comparison
semantics live so the tree's signature matchers and the family-fallback
contradiction checks can never drift apart again.

Comparison rules:

* equals / one_of compare through ``_stable_signature_value`` — catalog
  anchors say ``equals: 220`` while parsed telemetry arrives as ``220.0`` or
  ``"220"``, and all three must match.
* known_enum is STRICT: the value must be a decoded enum label, and
  ``"Unknown (7)"`` placeholders do not count. (The tree historically
  accepted any non-None value here, which would have validated garbage.)
"""

from __future__ import annotations

from .detection_descriptor_loader import (
    DetectionAnchorCondition,
    _stable_signature_value,
)


def equals_matches(expected: object, value: object) -> bool:
    return _stable_signature_value(value) == _stable_signature_value(expected)


def one_of_matches(options: tuple[object, ...], value: object) -> bool:
    normalized = _stable_signature_value(value)
    return any(normalized == _stable_signature_value(option) for option in options)


def range_matches(
    min_value: float | int | None,
    max_value: float | int | None,
    value: object,
) -> bool:
    try:
        observed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    if min_value is not None and observed < float(min_value):
        return False
    if max_value is not None and observed > float(max_value):
        return False
    return True


def contains_any_matches(tokens: tuple[str, ...], value: object) -> bool:
    observed = str(value).lower()
    return any(token and token in observed for token in tokens)


def known_enum_matches(value: object) -> bool:
    return isinstance(value, str) and not value.startswith("Unknown")


def anchor_condition_matches(anchor: DetectionAnchorCondition, value: object) -> bool:
    """Return whether an OBSERVED value satisfies one anchor condition."""

    if anchor.equals is not None and not equals_matches(anchor.equals, value):
        return False
    if anchor.one_of and not one_of_matches(anchor.one_of, value):
        return False
    if anchor.contains_any and not contains_any_matches(anchor.contains_any, value):
        return False
    if anchor.known_enum and not known_enum_matches(value):
        return False
    if (anchor.min_value is not None or anchor.max_value is not None) and not range_matches(
        anchor.min_value, anchor.max_value, value
    ):
        return False
    return True
