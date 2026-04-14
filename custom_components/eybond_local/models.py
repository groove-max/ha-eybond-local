"""Shared models used by the integration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .link_models import EybondLinkRoute


def key_to_title(key: str) -> str:
    """Convert an internal capability key into a user-facing title."""

    return key.replace("_", " ").title()


def decimals_for_divisor(divisor: int) -> int:
    """Infer native decimal places from a decimal power divisor."""

    decimals = 0
    current = divisor
    while current > 1 and current % 10 == 0:
        current //= 10
        decimals += 1
    return decimals


@dataclass(frozen=True, slots=True)
class ProbeTarget:
    """Transport parameters required to reach an inverter payload."""

    devcode: int
    collector_addr: int
    device_addr: int

    @property
    def link_route(self) -> EybondLinkRoute:
        """Return the link-level route for the current EyeBond tunnel."""

        return EybondLinkRoute(
            devcode=self.devcode,
            collector_addr=self.collector_addr,
        )

    @property
    def payload_address(self) -> int:
        """Return the payload-level device address on the tunneled protocol."""

        return self.device_addr


@dataclass(frozen=True, slots=True)
class RegisterValueSpec:
    """Describes how to decode one logical value from Modbus registers."""

    key: str
    register: int
    word_count: int = 1
    signed: bool = False
    combine: str = "u16"
    divisor: int | None = None
    decimals: int | None = None
    enum_map: dict[int | str, str] | None = None


@dataclass(frozen=True, slots=True)
class CapabilityChoice:
    """One structured enum choice for a writable capability."""

    value: int
    label: str
    description: str = ""
    order: int = 1000
    advanced: bool = False


@dataclass(frozen=True, slots=True)
class CapabilityRecommendation:
    """One declarative recommendation for a capability value."""

    value: Any
    reason: str
    conditions: tuple["CapabilityCondition", ...] = ()
    label: str = ""
    priority: int = 1000


@dataclass(frozen=True, slots=True)
class CapabilityPresetItem:
    """One target value inside a named multi-setting preset."""

    capability_key: str
    value: Any
    reason: str = ""
    order: int = 1000


@dataclass(frozen=True, slots=True)
class CapabilityPreset:
    """Declarative multi-setting preset assembled from capability values."""

    key: str
    title: str
    description: str
    items: tuple[CapabilityPresetItem, ...]
    conditions: tuple["CapabilityCondition", ...] = ()
    group: str = "recommended"
    order: int = 1000
    icon: str | None = None
    advanced: bool = False
    requires_confirm: bool = True

    def runtime_state(
        self,
        inverter: "DetectedInverter",
        values: Mapping[str, Any],
    ) -> "CapabilityPresetRuntimeState":
        """Evaluate whether the preset is currently visible and applicable."""

        visible, reasons = _evaluate_conditions(self.conditions, values)
        applicable = visible
        matches_current = True
        warnings: list[str] = []

        for item in sorted(self.items, key=lambda item: (item.order, item.capability_key)):
            try:
                capability = inverter.get_capability(item.capability_key)
            except KeyError:
                applicable = False
                matches_current = False
                warnings.append(
                    f"Capability {item.capability_key!r} is not supported by the detected inverter."
                )
                continue

            runtime_state = capability.runtime_state(values)
            current_value = values.get(capability.value_key)
            target_label = _recommendation_label(capability, item.value)
            item_matches = current_value == item.value or current_value == target_label
            matches_current = matches_current and item_matches

            if not runtime_state.editable:
                applicable = False
                if runtime_state.reasons:
                    warnings.extend(
                        f"{capability.display_name}: {reason}"
                        for reason in runtime_state.reasons
                    )
                else:
                    warnings.append(
                        f"{capability.display_name}: capability is not editable right now."
                    )

            warnings.extend(
                f"{capability.display_name}: {warning}"
                for warning in runtime_state.warnings
            )

        return CapabilityPresetRuntimeState(
            visible=visible,
            applicable=applicable,
            reasons=_dedupe_texts(reasons),
            warnings=_dedupe_texts(warnings),
            matches_current=matches_current,
        )


@dataclass(frozen=True, slots=True)
class WriteCapability:
    """Declarative schema for one writable inverter capability."""

    key: str
    register: int
    value_kind: str
    note: str
    tested: bool = False
    support_tier: str = ""
    support_notes: str = ""
    action_value: int | None = None
    divisor: int | None = None
    minimum: int | None = None
    maximum: int | None = None
    enum_map: dict[int, str] | None = None
    choices: tuple[CapabilityChoice, ...] = ()
    recommendations: tuple[CapabilityRecommendation, ...] = ()
    title: str = ""
    group: str = "config"
    order: int = 1000
    unit: str | None = None
    device_class: str | None = None
    step: float | None = None
    enabled_default: bool = False
    advanced: bool = False
    requires_confirm: bool = False
    reboot_required: bool = False
    read_key: str = ""
    depends_on: tuple[str, ...] = ()
    affects: tuple[str, ...] = ()
    exclusive_with: tuple[str, ...] = ()
    change_summary: str = ""
    unsafe_while_running: bool = False
    safe_operating_modes: tuple[str, ...] = ("Power On", "Standby", "Fault")
    visible_if: tuple["CapabilityCondition", ...] = ()
    editable_if: tuple["CapabilityCondition", ...] = ()

    @property
    def value_key(self) -> str:
        """Runtime value key used to read the current native value."""

        return self.read_key or self.key

    @property
    def display_name(self) -> str:
        """User-facing capability name."""

        return self.title or key_to_title(self.key)

    @property
    def native_minimum(self) -> int | float | None:
        """Return the minimum value in native units."""

        return self._to_native(self.minimum)

    @property
    def native_maximum(self) -> int | float | None:
        """Return the maximum value in native units."""

        return self._to_native(self.maximum)

    @property
    def native_step(self) -> float:
        """Return the native UI step."""

        if self.step is not None:
            return self.step
        if self.divisor:
            return 1 / self.divisor
        return 1.0

    @property
    def validation_state(self) -> str:
        """Return the capability validation state used for support reporting."""

        return "tested" if self.tested else "untested"

    @property
    def resolved_support_tier(self) -> str:
        """Return the effective support tier for runtime/docs export."""

        if self.support_tier:
            return self.support_tier
        if self.visible_if or self.editable_if or self.unsafe_while_running:
            return "conditional"
        return "standard"

    @property
    def enum_options(self) -> list[str]:
        """Return sorted user-facing enum labels."""

        return [choice.label for choice in self.enum_choices]

    @property
    def enum_choices(self) -> tuple[CapabilityChoice, ...]:
        """Return structured enum choices for this capability."""

        if self.choices:
            return tuple(sorted(self.choices, key=lambda choice: (choice.order, choice.value)))
        if not self.enum_map:
            return ()
        return tuple(
            CapabilityChoice(value=value, label=label, order=value)
            for value, label in sorted(self.enum_map.items())
        )

    @property
    def enum_value_map(self) -> dict[int, str]:
        """Return the effective enum value -> label mapping."""

        if self.enum_map:
            return self.enum_map
        return {choice.value: choice.label for choice in self.enum_choices}

    def _to_native(self, raw: int | None) -> int | float | None:
        if raw is None:
            return None
        if self.divisor:
            return round(raw / self.divisor, decimals_for_divisor(self.divisor))
        return raw

    def runtime_state(self, values: Mapping[str, Any]) -> "CapabilityRuntimeState":
        """Evaluate visibility/editability rules against runtime values."""

        visible, visible_reasons = _evaluate_conditions(self.visible_if, values)
        editable, editable_reasons = _evaluate_conditions(self.editable_if, values)
        runtime_reasons = [*visible_reasons, *editable_reasons]

        blocked_reason = values.get(f"capability_block_reason_{self.key}")
        blocked_action = values.get(f"capability_block_action_{self.key}")
        if blocked_reason:
            editable = False
            runtime_reasons.append(str(blocked_reason))
            if blocked_action:
                runtime_reasons.append(f"Suggested action: {blocked_action}")

        warnings = _build_runtime_warnings(self, values)
        recommendations = _evaluate_recommendations(self, values)
        if not visible:
            editable = False
        return CapabilityRuntimeState(
            visible=visible,
            editable=editable,
            reasons=_dedupe_texts(runtime_reasons),
            warnings=_dedupe_texts(warnings),
            recommendations=recommendations,
        )


@dataclass(frozen=True, slots=True)
class CapabilityCondition:
    """One declarative condition used to control capability visibility/editability."""

    key: str
    operator: str = "eq"
    value: Any = True
    reason: str = ""


@dataclass(frozen=True, slots=True)
class CapabilityRuntimeState:
    """Evaluated runtime state for one capability."""

    visible: bool = True
    editable: bool = True
    reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    recommendations: tuple["ResolvedCapabilityRecommendation", ...] = ()


@dataclass(frozen=True, slots=True)
class CapabilityPresetRuntimeState:
    """Evaluated runtime state for one preset."""

    visible: bool = True
    applicable: bool = True
    reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    matches_current: bool = False


@dataclass(frozen=True, slots=True)
class CapabilityBlocker:
    """Runtime block applied after the inverter rejects one capability write."""

    code: str
    reason: str
    suggested_action: str = ""
    exception_code: int | None = None
    clear_on: str = "mode_change"


@dataclass(frozen=True, slots=True)
class ResolvedCapabilityRecommendation:
    """A recommendation after its runtime conditions have been evaluated."""

    value: Any
    label: str
    reason: str
    priority: int = 1000
    matches_current: bool = False


@dataclass(frozen=True, slots=True)
class CapabilityGroup:
    """Declarative UI grouping for related writable capabilities."""

    key: str
    title: str
    order: int = 1000
    description: str = ""
    icon: str | None = None
    advanced: bool = False


@dataclass(slots=True)
class CollectorInfo:
    """Runtime metadata for the connected collector."""

    remote_ip: str = ""
    remote_port: int | None = None
    collector_pn: str = ""
    last_devcode: int | None = None
    heartbeat_devcode: int | None = None
    heartbeat_payload_hex: str = ""
    last_udp_reply: str = ""
    last_udp_reply_from: str = ""
    profile_key: str = ""
    profile_name: str = ""
    heartbeat_ascii: str = ""
    heartbeat_payload_len: int | None = None
    heartbeat_format_key: str = ""
    heartbeat_suffix_ascii: str = ""
    heartbeat_suffix_kind: str = ""
    heartbeat_suffix_uint: int | None = None
    devcode_major: int | None = None
    devcode_minor: int | None = None
    collector_pn_prefix: str = ""
    collector_pn_digits: str = ""
    heartbeat_age_seconds: float | None = None
    heartbeat_fresh: bool | None = None


@dataclass(slots=True)
class CollectorCandidate:
    """One collector candidate found during onboarding discovery."""

    target_ip: str
    source: str
    ip: str = ""
    udp_reply: str = ""
    udp_reply_from: str = ""
    connected: bool = False
    collector: CollectorInfo | None = None


@dataclass(slots=True)
class DriverMatch:
    """One matched inverter identity produced by driver probing."""

    driver_key: str
    protocol_family: str
    model_name: str
    serial_number: str
    probe_target: ProbeTarget
    variant_key: str = "default"
    confidence: str = "high"
    reasons: tuple[str, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DetectedInverter:
    """Result of a successful driver probe."""

    driver_key: str
    protocol_family: str
    model_name: str
    serial_number: str
    probe_target: ProbeTarget
    variant_key: str = "default"
    details: dict[str, Any] = field(default_factory=dict)
    profile_name: str = ""
    register_schema_name: str = ""
    capability_groups: tuple[CapabilityGroup, ...] = ()
    capabilities: tuple[WriteCapability, ...] = ()
    capability_presets: tuple[CapabilityPreset, ...] = ()

    def get_capability(self, capability_key: str) -> WriteCapability:
        """Return the declared capability by key."""

        for capability in self.capabilities:
            if capability.key == capability_key:
                return capability
        raise KeyError(capability_key)

    def get_capability_preset(self, preset_key: str) -> CapabilityPreset:
        """Return the declared preset by key."""

        for preset in self.capability_presets:
            if preset.key == preset_key:
                return preset
        raise KeyError(preset_key)


@dataclass(frozen=True, slots=True)
class OnboardingResult:
    """Aggregated result of one onboarding detection attempt."""

    collector: CollectorCandidate | None = None
    match: DriverMatch | None = None
    connection_type: str = "eybond"
    connection_mode: str = ""
    warnings: tuple[str, ...] = ()
    next_action: str = ""
    last_error: str | None = None

    @property
    def confidence(self) -> str:
        """Return the effective overall confidence for this result."""

        if self.match is not None:
            return self.match.confidence
        if self.collector is not None and self.collector.connected:
            return "low"
        return "none"


@dataclass(frozen=True, slots=True)
class MeasurementDescription:
    """Home Assistant sensor metadata for a parsed value."""

    key: str
    name: str
    unit: str | None = None
    device_class: str | None = None
    state_class: str | None = None
    icon: str | None = None
    diagnostic: bool = False
    enabled_default: bool = True
    live: bool = True
    suggested_display_precision: int | None = None


@dataclass(frozen=True, slots=True)
class BinarySensorDescription:
    """Home Assistant binary sensor metadata for one boolean runtime value."""

    key: str
    name: str
    device_class: str | None = None
    icon: str | None = None
    diagnostic: bool = False
    enabled_default: bool = True
    live: bool = True


@dataclass(slots=True)
class RuntimeSnapshot:
    """Snapshot returned by the coordinator on every refresh cycle."""

    connected: bool = False
    collector: CollectorInfo | None = None
    inverter: DetectedInverter | None = None
    values: dict[str, Any] = field(default_factory=dict)
    last_error: str | None = None


def _evaluate_conditions(
    conditions: tuple[CapabilityCondition, ...],
    values: Mapping[str, Any],
) -> tuple[bool, tuple[str, ...]]:
    reasons: list[str] = []
    for condition in conditions:
        actual = values.get(condition.key)
        if _match_condition(condition, actual):
            continue
        reasons.append(condition.reason or _default_condition_reason(condition, actual))
    return (not reasons, tuple(reasons))


def _match_condition(condition: CapabilityCondition, actual: Any) -> bool:
    if condition.operator == "eq":
        return actual == condition.value
    if condition.operator == "ne":
        return actual != condition.value
    if condition.operator == "in":
        return actual in condition.value
    if condition.operator == "not_in":
        return actual not in condition.value
    if condition.operator == "truthy":
        return bool(actual)
    if condition.operator == "falsy":
        return not bool(actual)
    raise ValueError(f"unsupported_condition_operator:{condition.operator}")


def _default_condition_reason(condition: CapabilityCondition, actual: Any) -> str:
    return (
        f"Condition not met for {condition.key}: "
        f"operator={condition.operator} expected={condition.value!r} actual={actual!r}"
    )


def _build_runtime_warnings(
    capability: WriteCapability,
    values: Mapping[str, Any],
) -> tuple[str, ...]:
    warnings: list[str] = []
    if capability.unsafe_while_running:
        operating_mode = values.get("operating_mode")
        if operating_mode and operating_mode not in capability.safe_operating_modes:
            warnings.append(
                f"Changing this setting while inverter mode is {operating_mode!r} may be unsafe."
            )
    return tuple(warnings)


def _evaluate_recommendations(
    capability: WriteCapability,
    values: Mapping[str, Any],
) -> tuple[ResolvedCapabilityRecommendation, ...]:
    current_value = values.get(capability.value_key)
    resolved: list[ResolvedCapabilityRecommendation] = []

    for recommendation in sorted(
        capability.recommendations,
        key=lambda recommendation: recommendation.priority,
    ):
        matched, _ = _evaluate_conditions(recommendation.conditions, values)
        if not matched:
            continue

        label = recommendation.label or _recommendation_label(capability, recommendation.value)
        resolved.append(
            ResolvedCapabilityRecommendation(
                value=recommendation.value,
                label=label,
                reason=recommendation.reason,
                priority=recommendation.priority,
                matches_current=current_value == label or current_value == recommendation.value,
            )
        )

    return tuple(resolved)


def _recommendation_label(capability: WriteCapability, value: Any) -> str:
    enum_map = capability.enum_value_map
    if enum_map and value in enum_map:
        return enum_map[value]
    return str(value)


def _dedupe_texts(items: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    """Preserve order while removing duplicate messages."""

    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return tuple(deduped)
