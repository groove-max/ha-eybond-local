"""Strict full-route evidence for active shadow-learned read sensors.

Cloud read labels may only become local entities when the shadow session
observed the exact local route that the runtime will poll.  A Modbus register
number by itself is not an address: FC3/FC4 are separate spaces and the same
register may exist behind another EyeBond route or Modbus unit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...collector_identity import pn_is_same_identity, validated_collector_pn
from ...models import ProbeTarget


def _bounded_int(
    value: object,
    *,
    minimum: int,
    maximum: int,
    reason: str,
) -> int:
    if type(value) is not int:
        raise TypeError(reason)
    if value < minimum or value > maximum:
        raise ValueError(reason)
    return value


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


@dataclass(frozen=True, slots=True, order=True)
class ShadowReadRoute:
    """One exact EyeBond-wrapper and payload-unit route."""

    devcode: int
    collector_addr: int
    device_addr: int

    def __post_init__(self) -> None:
        _bounded_int(
            self.devcode,
            minimum=1,
            maximum=0xFFFF,
            reason="shadow_read_route_devcode_invalid",
        )
        _bounded_int(
            self.collector_addr,
            minimum=0,
            maximum=0xFF,
            reason="shadow_read_route_collector_addr_invalid",
        )
        _bounded_int(
            self.device_addr,
            minimum=1,
            maximum=0xF7,
            reason="shadow_read_route_device_addr_invalid",
        )

    def to_record(self) -> dict[str, int]:
        return {
            "devcode": self.devcode,
            "collector_addr": self.collector_addr,
            "device_addr": self.device_addr,
        }

    @classmethod
    def from_record(cls, record: object) -> "ShadowReadRoute | None":
        if type(record) is not dict or set(record) != {
            "devcode",
            "collector_addr",
            "device_addr",
        }:
            return None
        try:
            route = cls(
                devcode=record["devcode"],
                collector_addr=record["collector_addr"],
                device_addr=record["device_addr"],
            )
        except (TypeError, ValueError):
            return None
        return route if route.to_record() == record else None

    @classmethod
    def for_probe_target(cls, target: ProbeTarget) -> "ShadowReadRoute":
        if type(target) is not ProbeTarget:
            raise TypeError("shadow_read_probe_target_invalid")
        return cls(
            devcode=target.devcode,
            collector_addr=target.collector_addr,
            device_addr=target.device_addr,
        )


@dataclass(frozen=True, slots=True, order=True)
class ShadowReadRegisterEvidence:
    """One register sample set pinned to its complete read address."""

    route: ShadowReadRoute
    function: int
    register: int
    samples: tuple[int, ...]

    def __post_init__(self) -> None:
        if type(self.route) is not ShadowReadRoute:
            raise TypeError("shadow_read_evidence_route_invalid")
        if type(self.function) is not int:
            raise TypeError("shadow_read_evidence_function_invalid")
        if self.function not in {3, 4}:
            raise ValueError("shadow_read_evidence_function_invalid")
        _bounded_int(
            self.register,
            minimum=0,
            maximum=0xFFFF,
            reason="shadow_read_evidence_register_invalid",
        )
        if type(self.samples) is not tuple or not self.samples:
            raise TypeError("shadow_read_evidence_samples_invalid")
        if len(self.samples) > 8 or len(set(self.samples)) != len(self.samples):
            raise ValueError("shadow_read_evidence_samples_invalid")
        for sample in self.samples:
            _bounded_int(
                sample,
                minimum=0,
                maximum=0xFFFF,
                reason="shadow_read_evidence_sample_invalid",
            )

    def to_record(self) -> dict[str, Any]:
        return {
            **self.route.to_record(),
            "function": self.function,
            "register": self.register,
            "samples": list(self.samples),
        }

    @classmethod
    def from_record(cls, record: object) -> "ShadowReadRegisterEvidence | None":
        if type(record) is not dict or set(record) != {
            "devcode",
            "collector_addr",
            "device_addr",
            "function",
            "register",
            "samples",
        }:
            return None
        if type(record["samples"]) is not list:
            return None
        route = ShadowReadRoute.from_record(
            {
                "devcode": record["devcode"],
                "collector_addr": record["collector_addr"],
                "device_addr": record["device_addr"],
            }
        )
        if route is None:
            return None
        try:
            evidence = cls(
                route=route,
                function=record["function"],
                register=record["register"],
                samples=tuple(record["samples"]),
            )
        except (TypeError, ValueError):
            return None
        return evidence if evidence.to_record() == record else None


@dataclass(frozen=True, slots=True)
class LearnedReadActivationContext:
    """Exact runtime context authorized to receive learned read sensors."""

    collector_pn: str
    driver_key: str
    register_schema_name: str
    route: ShadowReadRoute

    def __post_init__(self) -> None:
        if type(self.collector_pn) is not str:
            raise TypeError("learned_read_context_collector_pn_invalid")
        if validated_collector_pn(self.collector_pn) != self.collector_pn:
            raise ValueError("learned_read_context_collector_pn_invalid")
        _required_token(self.driver_key, "learned_read_context_driver_invalid")
        _required_token(
            self.register_schema_name,
            "learned_read_context_schema_invalid",
        )
        if type(self.route) is not ShadowReadRoute:
            raise TypeError("learned_read_context_route_invalid")

    def to_record(self) -> dict[str, Any]:
        return {
            "collector_pn": self.collector_pn,
            "driver_key": self.driver_key,
            "register_schema_name": self.register_schema_name,
            "probe_target": self.route.to_record(),
        }

    @classmethod
    def from_record(cls, record: object) -> "LearnedReadActivationContext | None":
        if type(record) is not dict or set(record) != {
            "collector_pn",
            "driver_key",
            "register_schema_name",
            "probe_target",
        }:
            return None
        route = ShadowReadRoute.from_record(record["probe_target"])
        if route is None:
            return None
        try:
            context = cls(
                collector_pn=record["collector_pn"],
                driver_key=record["driver_key"],
                register_schema_name=record["register_schema_name"],
                route=route,
            )
        except (TypeError, ValueError):
            return None
        return context if context.to_record() == record else None


def read_register_evidence_from_map(
    read_map: object,
) -> tuple[ShadowReadRegisterEvidence, ...]:
    """Parse one exact route-aware evidence set; malformed input fails closed."""

    if type(read_map) is not dict:
        return ()
    records = read_map.get("register_series")
    if type(records) is not list or not records:
        return ()
    evidence: list[ShadowReadRegisterEvidence] = []
    for record in records:
        item = ShadowReadRegisterEvidence.from_record(record)
        if item is None:
            return ()
        evidence.append(item)
    ordered = tuple(sorted(evidence))
    if len(set(ordered)) != len(ordered):
        return ()
    return ordered


def validate_learned_read_activation(
    *,
    manifest: object,
    register_schema_record: object,
    profile_name: str,
    register_schema_name: str,
    selected_read_keys: tuple[str, ...],
    current_context: LearnedReadActivationContext,
) -> LearnedReadActivationContext:
    """Validate one selected-read activation before any persistence mutation."""

    _required_token(profile_name, "learned_read_activation_profile_invalid")
    _required_token(
        register_schema_name,
        "learned_read_activation_schema_invalid",
    )
    if type(selected_read_keys) is not tuple or not selected_read_keys:
        raise TypeError("learned_read_activation_selection_invalid")
    for key in selected_read_keys:
        _required_token(key, "learned_read_activation_selection_invalid")
    if selected_read_keys != tuple(sorted(set(selected_read_keys))):
        raise ValueError("learned_read_activation_selection_invalid")
    if type(current_context) is not LearnedReadActivationContext:
        raise TypeError("learned_read_activation_current_context_invalid")
    if type(manifest) is not dict:
        raise ValueError("learned_read_overlay_manifest_invalid")
    output = manifest.get("output")
    if (
        type(output) is not dict
        or output.get("profile_name") != profile_name
        or output.get("schema_name") != register_schema_name
        or manifest.get("source_schema_name")
        != current_context.register_schema_name
    ):
        raise ValueError("learned_read_overlay_output_mismatch")
    generated_reads = manifest.get("learned_read_sensors")
    if type(generated_reads) is not list:
        raise ValueError("learned_read_overlay_evidence_invalid")
    generated_locations: dict[str, tuple[int, int, str]] = {}
    for item in generated_reads:
        if type(item) is not dict:
            raise ValueError("learned_read_overlay_evidence_invalid")
        key = _required_token(
            item.get("key"),
            "learned_read_overlay_evidence_invalid",
        )
        function = item.get("function")
        register = item.get("register")
        spec_set = _required_token(
            item.get("spec_set"),
            "learned_read_overlay_evidence_invalid",
        )
        if (
            type(function) is not int
            or function not in {3, 4}
            or type(register) is not int
            or register < 0
            or register > 0xFFFF
            or key in generated_locations
        ):
            raise ValueError("learned_read_overlay_evidence_invalid")
        generated_locations[key] = (function, register, spec_set)
    if not set(selected_read_keys).issubset(generated_locations):
        raise ValueError("learned_read_selection_unproven")

    learned_context = LearnedReadActivationContext.from_record(
        manifest.get("learned_read_context")
    )
    if (
        learned_context is None
        or learned_context.driver_key != current_context.driver_key
        or learned_context.register_schema_name != current_context.register_schema_name
        or learned_context.route != current_context.route
        or not pn_is_same_identity(
            learned_context.collector_pn,
            current_context.collector_pn,
        )
    ):
        raise ValueError("learned_read_runtime_context_mismatch")

    if (
        type(register_schema_record) is not dict
        or register_schema_record.get("shadow_learning_overlay") != manifest
        or register_schema_record.get("driver_key") != learned_context.driver_key
        or register_schema_record.get("draft_of")
        not in {
            learned_context.register_schema_name,
            f"builtin:{learned_context.register_schema_name}",
        }
    ):
        raise ValueError("learned_read_schema_context_mismatch")
    spec_sets = register_schema_record.get("spec_sets")
    measurements = register_schema_record.get("measurement_descriptions")
    learned_locations = register_schema_record.get("learned_read_locations")
    if (
        type(spec_sets) is not dict
        or type(measurements) is not list
        or type(learned_locations) is not list
    ):
        raise ValueError("learned_read_schema_evidence_invalid")

    for key in selected_read_keys:
        function, register, spec_set = generated_locations[key]
        matching_specs = []
        for candidate_set, specs in spec_sets.items():
            if type(candidate_set) is not str or type(specs) is not list:
                raise ValueError("learned_read_schema_evidence_invalid")
            for spec in specs:
                if type(spec) is not dict:
                    raise ValueError("learned_read_schema_evidence_invalid")
                if spec.get("key") == key:
                    matching_specs.append((candidate_set, spec))
        if len(matching_specs) != 1:
            raise ValueError("learned_read_schema_selection_mismatch")
        candidate_set, spec = matching_specs[0]
        if (
            candidate_set != spec_set
            or spec.get("function") != function
            or type(spec.get("function")) is not int
            or spec.get("register") != register
            or type(spec.get("register")) is not int
        ):
            raise ValueError("learned_read_schema_selection_mismatch")

        matching_measurements = [
            item
            for item in measurements
            if type(item) is dict and item.get("key") == key
        ]
        if (
            len(matching_measurements) != 1
            or matching_measurements[0].get("learned") is not True
        ):
            raise ValueError("learned_read_schema_selection_mismatch")
        if learned_locations.count([function, register]) != 1:
            raise ValueError("learned_read_schema_selection_mismatch")
    return learned_context


__all__ = [
    "LearnedReadActivationContext",
    "ShadowReadRegisterEvidence",
    "ShadowReadRoute",
    "read_register_evidence_from_map",
    "validate_learned_read_activation",
]
