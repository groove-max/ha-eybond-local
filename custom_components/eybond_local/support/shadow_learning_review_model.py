"""Deterministic review model and conservative risk classifier for learned controls.

EYB-REF-042. Normalizes discovered (shadow-learned) controls into a review model
with user-facing labels, default enable decisions, risk levels, and machine-readable
exclusion reasons.

This module is intentionally pure and side-effect free so the classifier is
deterministic and testable in isolation. It does not change runtime exposure and
does not activate any control: risky or uncertain controls default to disabled so
that nothing becomes writable purely because SmartESS exposed it. ``enabled_by_default``
here is a review-screen recommendation only; the learned overlay capabilities stay
inactive (``enabled_default: False``, ``experimental: True``) until a later, explicitly
scoped activation task.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .shadow_learning import coerce_optional_int as _to_int


RISK_NORMAL = "normal"
RISK_HIGH = "high"
RISK_UNCERTAIN = "uncertain"

REVIEW_MODEL_KIND = "learned_control_review_model"
REVIEW_MODEL_SCHEMA_VERSION = 1

# Confidence labels accepted as a real correlation. Anything else (missing, low,
# unknown) is treated as weak/missing correlation and defaults to disabled.
_ACCEPTED_CONFIDENCE: frozenset[str] = frozenset({"high", "medium"})

# Value kinds the classifier understands well enough to consider default-enabling.
_KNOWN_VALUE_KINDS: frozenset[str] = frozenset(
    {"bool", "enum", "action", "u16", "u32_high_first", "u32_low_first"}
)
_NUMERIC_VALUE_KINDS: frozenset[str] = frozenset(
    {"u16", "u32_high_first", "u32_low_first"}
)


def classify_learned_control_risk(capability: dict[str, Any]) -> dict[str, Any]:
    """Classify one discovered control's risk deterministically.

    Returns a dict with:
      - ``risk_level``: one of ``normal`` / ``high`` / ``uncertain``
      - ``reasons``: sorted, de-duplicated machine-readable reason codes
      - ``enabled_by_default``: True only for normal-risk controls

    The classifier reads only the control's own evidence fields, so it can be reused
    on any learned-capability-shaped payload.
    """

    if not isinstance(capability, dict):
        return {
            "risk_level": RISK_UNCERTAIN,
            "reasons": ["unknown_value_kind"],
            "enabled_by_default": False,
        }

    provenance = capability.get("learned_provenance")
    provenance = provenance if isinstance(provenance, dict) else {}

    value_kind = str(capability.get("value_kind") or "").strip()
    confidence = str(provenance.get("confidence") or "").strip().lower()
    safety_class = str(provenance.get("safety_class") or "").strip().lower()

    high_reasons: list[str] = []
    uncertain_reasons: list[str] = []

    # High-risk = ONLY destructive one-shot actions (clear / reset / factory / erase / delete --
    # captured by safety_class=="destructive_action" on action controls). Everything else --
    # ordinary switches, selects, numbers, and non-destructive actions like "Forced EQ
    # Charging"/"Exit Fault Mode" -- is enabled by default, so the review screen pre-checks them.
    # A scary NAME alone is deliberately NOT high-risk (see
    # test_name_keyword_alone_no_longer_disables_control): the review screen pre-checks it and
    # the user still confirms every write at runtime.
    if safety_class == "destructive_action":
        high_reasons.append("destructive_action")

    register = _to_int(capability.get("register"))
    if register is None or register <= 0:
        uncertain_reasons.append("missing_register")

    if value_kind not in _KNOWN_VALUE_KINDS:
        uncertain_reasons.append("unknown_value_kind")

    if value_kind in _NUMERIC_VALUE_KINDS and not _has_bounded_range(capability):
        uncertain_reasons.append("numeric_without_bounded_range")

    if value_kind == "enum" and not _has_clear_enum_labels(capability):
        uncertain_reasons.append("enum_without_labels")

    if confidence not in _ACCEPTED_CONFIDENCE:
        uncertain_reasons.append("weak_correlation")

    reasons = sorted(set(high_reasons) | set(uncertain_reasons))
    if high_reasons:
        risk_level = RISK_HIGH
    elif uncertain_reasons:
        risk_level = RISK_UNCERTAIN
    else:
        risk_level = RISK_NORMAL

    return {
        "risk_level": risk_level,
        "reasons": reasons,
        # Pre-check everything except high-risk (destructive actions). Uncertain controls (weak
        # correlation, unlabelled enum, ...) are still surfaced but enabled by default.
        "enabled_by_default": risk_level != RISK_HIGH,
    }


def default_learned_control_label(
    *,
    field_name: str = "",
    field_id: str = "",
    register: int | None = None,
) -> str:
    """Return a user-facing default label, preferring the SmartESS title."""

    title = " ".join(str(field_name or "").split()).strip()
    if title:
        return title
    humanized = " ".join(str(field_id or "").replace("_", " ").split()).strip()
    if humanized:
        return humanized
    register_int = _to_int(register)
    if register_int is not None and register_int > 0:
        return f"Discovered control {register_int}"
    return "Discovered control"


def build_learned_control_review_entry(capability: dict[str, Any]) -> dict[str, Any]:
    """Normalize one learned capability into a review entry with risk + label."""

    provenance = capability.get("learned_provenance")
    provenance = provenance if isinstance(provenance, dict) else {}
    risk = classify_learned_control_risk(capability)
    register = _to_int(capability.get("register")) or 0
    field_id = str(provenance.get("cloud_field_id") or "")
    field_name = str(capability.get("title") or "")
    enabled = bool(risk["enabled_by_default"])
    reasons = list(risk["reasons"])
    return {
        "key": str(capability.get("key") or ""),
        "register": register,
        "field_id": field_id,
        "field_name": field_name,
        "default_label": default_learned_control_label(
            field_name=field_name, field_id=field_id, register=register
        ),
        "value_kind": str(capability.get("value_kind") or ""),
        "risk_level": str(risk["risk_level"]),
        "risk_reasons": reasons,
        "enabled_by_default": enabled,
        "exclusion_reasons": [] if enabled else list(reasons),
        "confidence": str(provenance.get("confidence") or ""),
        "safety_class": str(provenance.get("safety_class") or ""),
        "evidence_hash": str(provenance.get("evidence_hash") or ""),
    }


def build_learned_control_review_model(
    capabilities: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Build a deterministic review model from generated learned capabilities.

    ``learned_all`` preserves every discovered control (enabled and disabled) so the
    support package and the future review screen keep full evidence. ``enabled_by_default``
    lists the keys of normal-risk controls; ``excluded_by_policy`` lists the disabled
    controls with their machine-readable reasons.
    """

    learned_all: list[dict[str, Any]] = []
    enabled_by_default: list[str] = []
    excluded_by_policy: list[dict[str, Any]] = []

    for capability in list(capabilities or []):
        if not isinstance(capability, dict):
            continue
        entry = build_learned_control_review_entry(capability)
        learned_all.append(entry)
        if entry["enabled_by_default"]:
            enabled_by_default.append(entry["key"])
        else:
            excluded_by_policy.append(
                {
                    "key": entry["key"],
                    "register": entry["register"],
                    "risk_level": entry["risk_level"],
                    "reasons": list(entry["exclusion_reasons"]),
                }
            )

    return {
        "kind": REVIEW_MODEL_KIND,
        "schema_version": REVIEW_MODEL_SCHEMA_VERSION,
        "counts": {
            "learned_all": len(learned_all),
            "enabled_by_default": len(enabled_by_default),
            "excluded_by_policy": len(excluded_by_policy),
            "learned_read_all": 0,
            "read_enabled_by_default": 0,
            "read_excluded_by_policy": 0,
        },
        "learned_all": learned_all,
        "enabled_by_default": enabled_by_default,
        "excluded_by_policy": excluded_by_policy,
        "learned_read_all": [],
        "read_enabled_by_default": [],
        "read_excluded_by_policy": [],
    }


def build_learned_read_review_entry(sensor: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize one generated learned read sensor for review/activation."""

    key = str(sensor.get("key") or "").strip()
    register = _to_int(sensor.get("register")) or 0
    title = " ".join(str(sensor.get("title") or "").split()).strip()
    kind = str(sensor.get("kind") or "").strip()
    spec_set = str(sensor.get("spec_set") or "").strip()
    return {
        "key": key,
        "register": register,
        "field_name": title,
        "default_label": title or f"Discovered sensor {register}",
        "kind": kind,
        "spec_set": spec_set,
        "enabled_by_default": True,
        "exclusion_reasons": [],
    }


def attach_learned_read_review_model(
    review_model: Mapping[str, Any] | None,
    *,
    learned_read_sensors: list[dict[str, Any]] | None = None,
    skipped_read_sensors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return ``review_model`` extended with read-sensor review evidence.

    Read sensors generated by shadow learning are safe read-only additions, so the
    default review decision is enabled. Skipped reads remain visible as policy
    exclusions because they duplicate an existing register/title or otherwise could
    not be materialized into the overlay schema.
    """

    model = dict(review_model or {})
    learned_read_all: list[dict[str, Any]] = []
    read_enabled_by_default: list[str] = []
    for sensor in list(learned_read_sensors or []):
        if not isinstance(sensor, Mapping):
            continue
        entry = build_learned_read_review_entry(sensor)
        if not entry["key"]:
            continue
        learned_read_all.append(entry)
        read_enabled_by_default.append(entry["key"])

    read_excluded_by_policy: list[dict[str, Any]] = []
    for sensor in list(skipped_read_sensors or []):
        if not isinstance(sensor, Mapping):
            continue
        register = _to_int(sensor.get("register")) or 0
        title = " ".join(str(sensor.get("title") or "").split()).strip()
        reason = str(sensor.get("reason") or "skipped").strip() or "skipped"
        read_excluded_by_policy.append(
            {
                "register": register,
                "field_name": title,
                "default_label": title or f"Discovered sensor {register}",
                "kind": str(sensor.get("kind") or ""),
                "reason": reason,
                "reasons": [reason],
            }
        )

    counts = dict(model.get("counts") if isinstance(model.get("counts"), Mapping) else {})
    counts.update(
        {
            "learned_read_all": len(learned_read_all),
            "read_enabled_by_default": len(read_enabled_by_default),
            "read_excluded_by_policy": len(read_excluded_by_policy),
        }
    )
    model["counts"] = counts
    model["learned_read_all"] = learned_read_all
    model["read_enabled_by_default"] = read_enabled_by_default
    model["read_excluded_by_policy"] = read_excluded_by_policy
    return model


def build_activation_selection(
    *,
    review_model: Mapping[str, Any] | None = None,
    selections: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the activation-manifest selection block from review evidence + user choices.

    EYB-REF-044. Combines the deterministic review model (EYB-REF-042 ``learned_all``)
    with the user's per-control choices (EYB-REF-043 ``review_selections``) into the
    selection block stored on a device-scoped overlay activation. It produces three
    faithful views for one activation:

      - ``selected_controls``: the controls the user chose to expose, each with its
        user-facing label and basic evidence (key, register, value_kind, risk_level).
      - ``excluded_controls``: every discovered control the user did not enable, with
        retained exclusion/risk reasons preserved for support evidence.
      - ``selected_control_keys``: the sorted, de-duplicated keys runtime should expose.

    The review model's ``learned_all`` evidence is read-only here: this never mutates it,
    so the full discovered set (including disabled controls and developer field names)
    stays intact for the support package. When no review model is available it degrades
    to the user selections alone; when neither is present it returns empty views.
    """

    model = review_model if isinstance(review_model, Mapping) else {}
    selection = selections if isinstance(selections, Mapping) else {}
    control_choices = selection.get("controls")
    control_choices = control_choices if isinstance(control_choices, Mapping) else {}
    read_choices = selection.get("read_sensors")
    read_choices = read_choices if isinstance(read_choices, Mapping) else {}

    learned_all = model.get("learned_all")
    entries: list[Mapping[str, Any]]
    if isinstance(learned_all, list) and learned_all:
        entries = [entry for entry in learned_all if isinstance(entry, Mapping)]
    else:
        # No review model evidence: fall back to the user's own per-control choices so
        # an explicit selection is still honored (keys/labels only, no risk evidence).
        entries = [
            choice for choice in control_choices.values() if isinstance(choice, Mapping)
        ]

    selected_controls: list[dict[str, Any]] = []
    excluded_controls: list[dict[str, Any]] = []
    for entry in entries:
        key = str(entry.get("key") or "").strip()
        if not key:
            continue
        choice = control_choices.get(key)
        choice = choice if isinstance(choice, Mapping) else None

        enabled_by_default = bool(entry.get("enabled_by_default"))
        if choice is not None and "enabled" in choice:
            enabled = bool(choice.get("enabled"))
        else:
            enabled = enabled_by_default

        label = ""
        if choice is not None:
            label = str(choice.get("label") or "").strip()
        if not label:
            label = _entry_default_label(entry)

        base = {
            "key": key,
            "label": label,
            "register": _to_int(entry.get("register")) or 0,
            "value_kind": str(entry.get("value_kind") or ""),
            "risk_level": str(entry.get("risk_level") or ""),
        }
        if enabled:
            selected_controls.append(base)
        else:
            excluded_controls.append({**base, "reasons": _excluded_reasons(entry, choice)})

    selected_control_keys = sorted(
        dict.fromkeys(control["key"] for control in selected_controls)
    )

    read_entries = model.get("learned_read_all")
    if isinstance(read_entries, list) and read_entries:
        read_review_entries = [
            entry for entry in read_entries if isinstance(entry, Mapping)
        ]
    else:
        read_review_entries = [
            choice for choice in read_choices.values() if isinstance(choice, Mapping)
        ]

    selected_read_sensors: list[dict[str, Any]] = []
    excluded_read_sensors: list[dict[str, Any]] = []
    for entry in read_review_entries:
        key = str(entry.get("key") or "").strip()
        if not key:
            continue
        choice = read_choices.get(key)
        choice = choice if isinstance(choice, Mapping) else None
        enabled_by_default = bool(entry.get("enabled_by_default"))
        if choice is not None and "enabled" in choice:
            enabled = bool(choice.get("enabled"))
        else:
            enabled = enabled_by_default

        label = ""
        if choice is not None:
            label = str(choice.get("label") or "").strip()
        if not label:
            label = _entry_default_label(entry)

        base = {
            "key": key,
            "label": label,
            "register": _to_int(entry.get("register")) or 0,
            "kind": str(entry.get("kind") or ""),
            "spec_set": str(entry.get("spec_set") or ""),
        }
        if enabled:
            selected_read_sensors.append(base)
        else:
            excluded_read_sensors.append(
                {**base, "reasons": _excluded_read_reasons(entry, choice)}
            )

    selected_read_sensor_keys = sorted(
        dict.fromkeys(sensor["key"] for sensor in selected_read_sensors)
    )
    return {
        "selected_controls": selected_controls,
        "excluded_controls": excluded_controls,
        "selected_control_keys": selected_control_keys,
        "selected_read_sensors": selected_read_sensors,
        "excluded_read_sensors": excluded_read_sensors,
        "selected_read_sensor_keys": selected_read_sensor_keys,
    }


def normalize_activation_selection(
    selection: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Coerce an activation selection block into a stable, storable shape.

    Defensive companion to :func:`build_activation_selection` used when persisting an
    activation: it accepts either the output of that function or a hand-built mapping
    and returns ``selected_controls`` / ``excluded_controls`` / ``selected_control_keys``
    with normalized types. ``selected_control_keys`` is derived from ``selected_controls``
    when not supplied so the runtime exposure filter always has an authoritative key set.
    """

    selection = selection if isinstance(selection, Mapping) else {}
    selected_controls = [
        control
        for control in (
            _coerce_activation_control(raw, include_reasons=False)
            for raw in _as_list(selection.get("selected_controls"))
        )
        if control is not None
    ]
    excluded_controls = [
        control
        for control in (
            _coerce_activation_control(raw, include_reasons=True)
            for raw in _as_list(selection.get("excluded_controls"))
        )
        if control is not None
    ]
    selected_read_sensors = [
        sensor
        for sensor in (
            _coerce_activation_read_sensor(raw, include_reasons=False)
            for raw in _as_list(selection.get("selected_read_sensors"))
        )
        if sensor is not None
    ]
    excluded_read_sensors = [
        sensor
        for sensor in (
            _coerce_activation_read_sensor(raw, include_reasons=True)
            for raw in _as_list(selection.get("excluded_read_sensors"))
        )
        if sensor is not None
    ]

    raw_keys = selection.get("selected_control_keys")
    if isinstance(raw_keys, (list, tuple, set, frozenset)):
        keys = [str(key).strip() for key in raw_keys if str(key or "").strip()]
    else:
        keys = [control["key"] for control in selected_controls]
    selected_control_keys = sorted(dict.fromkeys(keys))
    raw_read_keys = selection.get("selected_read_sensor_keys")
    if isinstance(raw_read_keys, (list, tuple, set, frozenset)):
        read_keys = [
            str(key).strip() for key in raw_read_keys if str(key or "").strip()
        ]
    else:
        read_keys = [sensor["key"] for sensor in selected_read_sensors]
    selected_read_sensor_keys = sorted(dict.fromkeys(read_keys))
    return {
        "selected_controls": selected_controls,
        "excluded_controls": excluded_controls,
        "selected_control_keys": selected_control_keys,
        "selected_read_sensors": selected_read_sensors,
        "excluded_read_sensors": excluded_read_sensors,
        "selected_read_sensor_keys": selected_read_sensor_keys,
    }


CONTROL_DISCOVERY_EVIDENCE_KIND = "control_discovery_evidence"
CONTROL_DISCOVERY_EVIDENCE_SCHEMA_VERSION = 1


def build_control_discovery_evidence(
    *,
    review_model: Mapping[str, Any] | None = None,
    activation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Consolidate full control-discovery evidence for the support package.

    EYB-REF-045. Brings the otherwise scattered discovery evidence into one
    developer-facing document: every discovered control (the review model's
    ``learned_all``, with labels and risk reasons), the selected and excluded
    subsets (with user labels and retained risk/exclusion reasons), and a summary of
    activation state.

    Selection precedence: a recorded user selection (EYB-REF-043/044
    ``selected_controls`` / ``excluded_controls`` / ``selected_control_keys``) wins;
    otherwise the deterministic policy-default split from the review model is used so
    a package captured before user review still shows which controls policy would
    enable versus withhold (with reasons). This is a pure, read-only transform: it
    never mutates the review model or the activation manifest, so ``learned_all`` and
    developer field names stay intact.
    """

    model = review_model if isinstance(review_model, Mapping) else {}
    activation_map = activation if isinstance(activation, Mapping) else {}

    learned_all = model.get("learned_all")
    discovered_controls = [
        _discovered_control_evidence(entry)
        for entry in (learned_all if isinstance(learned_all, list) else [])
        if isinstance(entry, Mapping)
    ]
    learned_read_all = model.get("learned_read_all")
    discovered_read_sensors = [
        _discovered_read_sensor_evidence(entry)
        for entry in (learned_read_all if isinstance(learned_read_all, list) else [])
        if isinstance(entry, Mapping)
    ]

    has_user_selection = bool(
        _as_list(activation_map.get("selected_controls"))
        or _as_list(activation_map.get("excluded_controls"))
        or _as_list(activation_map.get("selected_control_keys"))
        or _as_list(activation_map.get("selected_read_sensors"))
        or _as_list(activation_map.get("excluded_read_sensors"))
        or _as_list(activation_map.get("selected_read_sensor_keys"))
    )
    if has_user_selection:
        normalized = normalize_activation_selection(activation_map)
        selected_controls = normalized["selected_controls"]
        excluded_controls = normalized["excluded_controls"]
        selected_read_sensors = normalized["selected_read_sensors"]
        excluded_read_sensors = normalized["excluded_read_sensors"]
        activation_status = str(activation_map.get("status") or "").strip()
        activation_active = bool(activation_map.get("active")) or activation_status == "activated"
        selection_source = "user_activation" if activation_active else "user_review"
    else:
        selected_controls, excluded_controls = _policy_default_split(discovered_controls)
        selected_read_sensors, excluded_read_sensors = _read_policy_default_split(
            discovered_read_sensors
        )
        selection_source = "policy_default"

    selected_control_keys = sorted(
        dict.fromkeys(
            str(control.get("key") or "").strip()
            for control in selected_controls
            if str(control.get("key") or "").strip()
        )
    )
    selected_read_sensor_keys = sorted(
        dict.fromkeys(
            str(sensor.get("key") or "").strip()
            for sensor in selected_read_sensors
            if str(sensor.get("key") or "").strip()
        )
    )

    return {
        "kind": CONTROL_DISCOVERY_EVIDENCE_KIND,
        "schema_version": CONTROL_DISCOVERY_EVIDENCE_SCHEMA_VERSION,
        "selection_source": selection_source,
        "counts": {
            "discovered": len(discovered_controls),
            "selected": len(selected_controls),
            "excluded": len(excluded_controls),
            "discovered_read_sensors": len(discovered_read_sensors),
            "selected_read_sensors": len(selected_read_sensors),
            "excluded_read_sensors": len(excluded_read_sensors),
        },
        "discovered_controls": discovered_controls,
        "selected_controls": selected_controls,
        "excluded_controls": excluded_controls,
        "discovered_read_sensors": discovered_read_sensors,
        "selected_read_sensors": selected_read_sensors,
        "excluded_read_sensors": excluded_read_sensors,
        "activation": {
            "present": bool(activation_map),
            "has_user_selection": has_user_selection,
            "status": str(activation_map.get("status") or ""),
            "scope": str(activation_map.get("scope") or ""),
            "selected_control_keys": selected_control_keys,
            "selected_read_sensor_keys": selected_read_sensor_keys,
        },
    }


def _discovered_control_evidence(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Return a faithful, full-evidence view of one discovered control.

    Preserves the discovery-time label and both risk and exclusion reasons so a
    developer can review every learned control, including the ones policy disabled.
    """

    return {
        "key": str(entry.get("key") or "").strip(),
        "register": _to_int(entry.get("register")) or 0,
        "field_id": str(entry.get("field_id") or ""),
        "field_name": str(entry.get("field_name") or ""),
        "default_label": _entry_default_label(entry),
        "value_kind": str(entry.get("value_kind") or ""),
        "risk_level": str(entry.get("risk_level") or ""),
        "risk_reasons": _string_list(entry.get("risk_reasons")),
        "exclusion_reasons": _string_list(entry.get("exclusion_reasons")),
        "enabled_by_default": bool(entry.get("enabled_by_default")),
        "confidence": str(entry.get("confidence") or ""),
        "safety_class": str(entry.get("safety_class") or ""),
    }


def _policy_default_split(
    discovered_controls: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split discovered controls into the default enabled/excluded subsets.

    Used only when no user activation selection has been recorded yet, so the support
    package still distinguishes which controls policy would enable from those it
    withholds (with reasons). This never activates anything; it only describes the
    default review-screen recommendation.
    """

    selected: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for control in discovered_controls:
        base = {
            "key": control["key"],
            "label": control["default_label"],
            "register": control["register"],
            "value_kind": control["value_kind"],
            "risk_level": control["risk_level"],
        }
        if control["enabled_by_default"]:
            selected.append(base)
        else:
            reasons = (
                control["exclusion_reasons"]
                or control["risk_reasons"]
                or ["excluded"]
            )
            excluded.append({**base, "reasons": list(dict.fromkeys(reasons))})
    return selected, excluded


def _discovered_read_sensor_evidence(entry: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "key": str(entry.get("key") or "").strip(),
        "register": _to_int(entry.get("register")) or 0,
        "field_name": str(entry.get("field_name") or ""),
        "default_label": _entry_default_label(entry),
        "kind": str(entry.get("kind") or ""),
        "spec_set": str(entry.get("spec_set") or ""),
        "enabled_by_default": bool(entry.get("enabled_by_default")),
        "exclusion_reasons": _string_list(entry.get("exclusion_reasons")),
    }


def _read_policy_default_split(
    discovered_read_sensors: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    selected: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for sensor in discovered_read_sensors:
        base = {
            "key": sensor["key"],
            "label": sensor["default_label"],
            "register": sensor["register"],
            "kind": sensor["kind"],
            "spec_set": sensor["spec_set"],
        }
        if sensor["enabled_by_default"]:
            selected.append(base)
        else:
            reasons = sensor["exclusion_reasons"] or ["excluded"]
            excluded.append({**base, "reasons": list(dict.fromkeys(reasons))})
    return selected, excluded


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]


def _entry_default_label(entry: Mapping[str, Any]) -> str:
    default_label = str(entry.get("default_label") or "").strip()
    if default_label:
        return default_label
    return default_learned_control_label(
        field_name=str(entry.get("field_name") or ""),
        field_id=str(entry.get("field_id") or ""),
        register=_to_int(entry.get("register")),
    )


def _excluded_reasons(
    entry: Mapping[str, Any],
    choice: Mapping[str, Any] | None,
) -> list[str]:
    """Preserve why one discovered control was not exposed.

    Risk/policy reasons take precedence (a risky control keeps its risk codes). A
    normal-risk control the user actively turned off records ``user_excluded`` so the
    support package can distinguish a user choice from a policy exclusion.
    """

    reasons: list[str] = []
    raw_reasons = entry.get("exclusion_reasons")
    if not isinstance(raw_reasons, (list, tuple)) or not raw_reasons:
        raw_reasons = entry.get("risk_reasons")
    if isinstance(raw_reasons, (list, tuple)):
        reasons = [str(reason).strip() for reason in raw_reasons if str(reason or "").strip()]

    user_disabled = choice is not None and not bool(choice.get("enabled"))
    if not reasons and user_disabled:
        reasons = ["user_excluded"]
    if not reasons:
        reasons = ["excluded"]
    return list(dict.fromkeys(reasons))


def _excluded_read_reasons(
    entry: Mapping[str, Any],
    choice: Mapping[str, Any] | None,
) -> list[str]:
    reasons = _string_list(entry.get("exclusion_reasons"))
    user_disabled = choice is not None and not bool(choice.get("enabled"))
    if not reasons and user_disabled:
        reasons = ["user_excluded"]
    if not reasons:
        reasons = ["excluded"]
    return list(dict.fromkeys(reasons))


def _coerce_activation_control(
    raw: Any,
    *,
    include_reasons: bool,
) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    key = str(raw.get("key") or "").strip()
    if not key:
        return None
    control: dict[str, Any] = {
        "key": key,
        "label": str(raw.get("label") or "").strip(),
        "register": _to_int(raw.get("register")) or 0,
        "value_kind": str(raw.get("value_kind") or ""),
        "risk_level": str(raw.get("risk_level") or ""),
    }
    if include_reasons:
        raw_reasons = raw.get("reasons")
        control["reasons"] = (
            [str(reason).strip() for reason in raw_reasons if str(reason or "").strip()]
            if isinstance(raw_reasons, (list, tuple))
            else []
        )
    return control


def _coerce_activation_read_sensor(
    raw: Any,
    *,
    include_reasons: bool,
) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    key = str(raw.get("key") or "").strip()
    if not key:
        return None
    sensor: dict[str, Any] = {
        "key": key,
        "label": str(raw.get("label") or "").strip(),
        "register": _to_int(raw.get("register")) or 0,
        "kind": str(raw.get("kind") or ""),
        "spec_set": str(raw.get("spec_set") or ""),
    }
    if include_reasons:
        raw_reasons = raw.get("reasons")
        sensor["reasons"] = (
            [str(reason).strip() for reason in raw_reasons if str(reason or "").strip()]
            if isinstance(raw_reasons, (list, tuple))
            else []
        )
    return sensor


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, (list, tuple)) else []


def _has_bounded_range(capability: dict[str, Any]) -> bool:
    minimum = _to_int(capability.get("minimum"))
    maximum = _to_int(capability.get("maximum"))
    if minimum is None or maximum is None:
        return False
    return maximum > minimum


def _has_clear_enum_labels(capability: dict[str, Any]) -> bool:
    enum_map = capability.get("enum_map")
    if not isinstance(enum_map, dict) or len(enum_map) < 2:
        return False
    for key, value in enum_map.items():
        label = str(value or "").strip()
        if not label or label == str(key).strip():
            # An empty label, or one that is just the numeric value, is not meaningful.
            return False
    return True



__all__ = [
    "RISK_NORMAL",
    "RISK_HIGH",
    "RISK_UNCERTAIN",
    "REVIEW_MODEL_KIND",
    "REVIEW_MODEL_SCHEMA_VERSION",
    "CONTROL_DISCOVERY_EVIDENCE_KIND",
    "CONTROL_DISCOVERY_EVIDENCE_SCHEMA_VERSION",
    "classify_learned_control_risk",
    "default_learned_control_label",
    "build_learned_control_review_entry",
    "build_learned_control_review_model",
    "build_learned_read_review_entry",
    "attach_learned_read_review_model",
    "build_activation_selection",
    "normalize_activation_selection",
    "build_control_discovery_evidence",
]
