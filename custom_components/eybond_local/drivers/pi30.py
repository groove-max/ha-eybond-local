"""PI30-family inverter driver over EyeBond transport."""

from __future__ import annotations

import time

from ..poll_policy import PollPolicy


from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from ..models import (
    DetectedInverter,
    ProbeTarget,
    WriteCapability,
    decimals_for_divisor,
)
from ..payload.pi30 import (
    Pi30Error,
    Pi30Session,
    build_request as build_pi30_request,
    parse_energy_counter,
    parse_firmware_version,
    parse_model_number,
    parse_protocol_id,
    parse_q1,
    parse_qflag,
    parse_qmod,
    parse_qpigs,
    parse_qpiri,
    parse_qpiws,
    parse_qt_clock,
    parse_serial_number,
    q1_output_keys,
    qpiws_output_keys,
)
from ..metadata.compiled_detection_catalog import (
    RESOLUTION_COMPATIBLE_GROUP,
    RESOLUTION_EXACT,
    RESOLUTION_FAMILY,
    load_compiled_detection_catalog,
)
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from .base import InverterDriver
from .read_result import DriverReadMode, DriverReadResult
from .support_probe import SupportProbeRequest
from .command_support import (
    apply_unsupported_diagnostics,
    command_skipped_as_unsupported as _command_skipped_as_unsupported,
    commit_cycle_failures as _commit_cycle_failures,
    record_command_failure as _record_command_failure,
    record_command_success as _record_command_success,
    unsupported_commands as _unsupported_commands,
    unsupported_diagnostics_removed_keys as _unsupported_diagnostics_removed_keys,
)
from .catalog_probe import (
    async_probe_ascii_catalog,
    async_probe_ascii_catalog_signature,
    catalog_model_name,
    evidence_providers_from_transport,
)


PI30_POLL_POLICY = PollPolicy(
    # A normal PI30 refresh is one compact ASCII exchange, routinely
    # completed in ~1.5s even through a remote reverse-TCP bridge. Keep one
    # integer-second scheduling step plus the normal safety factor rather
    # than the conservative floor for broad ASCII polling.
    min_auto_interval=2.0,
    max_auto_interval=120.0,
    min_manual_interval=2.0,
)


@dataclass(frozen=True, slots=True)
class Pi30CommandSpec:
    """One PI30 command and its parser."""

    command: str
    parser: Callable[[str], dict[str, Any]]
    optional: bool = False


# One PI30 runtime poll executes this fixed sequence in a single pass: the
# required live metrics, then the optional QPIWS / Q1 (auto-skipped when finally
# unsupported), then the reachable energy chain. There is NO fast/medium/slow
# grouping and NO empty cycle -- every poll does one honest, fully-reachable
# sequential pass, so its wall-clock is the real full-cycle cost the neutral auto
# poll policy sees.
_RUNTIME_COMMAND_SPECS: tuple[Pi30CommandSpec, ...] = (
    Pi30CommandSpec(command="QPIGS", parser=parse_qpigs),
    Pi30CommandSpec(command="QMOD", parser=parse_qmod),
    Pi30CommandSpec(command="QPIWS", parser=parse_qpiws, optional=True),
    Pi30CommandSpec(command="Q1", parser=parse_q1, optional=True),
)

# A minimal subset read only during onboarding to enrich the confirmation UI --
# not a runtime cadence.
_ONBOARDING_RUNTIME_COMMAND_SPECS: tuple[Pi30CommandSpec, ...] = (
    Pi30CommandSpec(command="QPIGS", parser=parse_qpigs),
    Pi30CommandSpec(command="QMOD", parser=parse_qmod),
)

_PI30_BOOL_COMMANDS: dict[str, str] = {
    "buzzer_enabled": "A",
    "overload_bypass_enabled": "B",
    "power_saving_enabled": "J",
    "lcd_reset_to_default_enabled": "K",
    "overload_restart_enabled": "U",
    "over_temperature_restart_enabled": "V",
    "lcd_backlight_enabled": "X",
    "primary_source_interrupt_alarm_enabled": "Y",
    "record_fault_code_enabled": "Z",
}

_PI30_ENUM_COMMANDS: dict[str, str] = {
    "output_source_priority": "POP",
    "charger_source_priority": "PCP",
    "input_voltage_range": "PGR",
    "battery_type": "PBT",
}

_PI30_NUMERIC_COMMANDS: dict[str, str] = {
    "battery_recharge_voltage": "PBCV",
    "battery_redischarge_voltage": "PBDV",
    "battery_under_voltage": "PSDV",
    "battery_bulk_voltage": "PCVV",
    "battery_float_voltage": "PBFT",
}

_CATALOG_PARSERS = {
    "pi30.protocol_id": parse_protocol_id,
    "pi30.serial_number": parse_serial_number,
    "pi30.qpiri": parse_qpiri,
    "pi30.model_number": parse_model_number,
    "pi30.qflag": parse_qflag,
    "pi30.qmod": parse_qmod,
    "pi30.qpiws": parse_qpiws,
    "pi30.qpigs": parse_qpigs,
    "pi30.main_firmware": lambda payload: parse_firmware_version(
        payload,
        key="main_cpu_firmware_version",
    ),
    "pi30.secondary_firmware": lambda payload: parse_firmware_version(
        payload,
        key="secondary_cpu_firmware_version",
    ),
    "pi30.tertiary_firmware": lambda payload: parse_firmware_version(
        payload,
        key="tertiary_cpu_firmware_version",
    ),
}


# The bounded read-only ASCII support-probe commands PI30 owns. Order and set
# are fixed here (moved out of the runtime hub); do not expand the sweep.
_PI30_SUPPORT_PROBE_COMMANDS: tuple[str, ...] = ("QPI", "QMOD", "QPIGS", "QPIRI", "QID")


class Pi30Driver(InverterDriver):
    """PI30 probe, runtime reader, and command-based controller."""

    key = "pi30"
    poll_policy = PI30_POLL_POLICY
    name = "PI30 / ASCII"

    def support_probe_plan(self) -> tuple[SupportProbeRequest, ...]:
        """Return the fixed PI30 read-only ASCII support-probe requests."""

        return tuple(
            SupportProbeRequest(
                payload_family="pi30_ascii",
                command=command,
                request=build_pi30_request(command),
            )
            for command in _PI30_SUPPORT_PROBE_COMMANDS
        )

    @property
    def signature_timeout(self) -> float:
        return load_compiled_detection_catalog().protocols[self.key].signature_timeout

    @property
    def probe_timeout(self) -> float:
        return load_compiled_detection_catalog().protocols[self.key].probe_timeout

    @property
    def probe_targets(self) -> tuple[ProbeTarget, ...]:
        return tuple(
            ProbeTarget(
                devcode=devcode,
                collector_addr=collector_addr,
                device_addr=device_addr,
            )
            for devcode, collector_addr, device_addr
            in load_compiled_detection_catalog().protocols[self.key].probe_targets
        )

    @property
    def profile_name(self) -> str:
        return _pi30_default_binding().profile_name

    @property
    def register_schema_name(self) -> str:
        return _pi30_default_binding().register_schema_name

    @property
    def measurements(self):
        schema = self.register_schema_metadata
        return schema.measurement_descriptions if schema is not None else ()

    @property
    def binary_sensors(self):
        schema = self.register_schema_metadata
        return schema.binary_sensor_descriptions if schema is not None else ()

    @property
    def capability_groups(self):
        profile = self.profile_metadata
        return profile.groups if profile is not None else ()

    @property
    def write_capabilities(self):
        profile = self.profile_metadata
        return profile.capabilities if profile is not None else ()

    async def async_probe_signature(self, transport, target: ProbeTarget) -> bool:
        session = self._session(transport, target)
        return await async_probe_ascii_catalog_signature(
            protocol_key=self.key,
            session=session,
            parsers=_CATALOG_PARSERS,
        )

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        session = self._session(transport, target)
        try:
            probe = await async_probe_ascii_catalog(
                protocol_key="pi30",
                session=session,
                parsers=_CATALOG_PARSERS,
                collector=getattr(transport, "collector_info", None),
                evidence_providers=evidence_providers_from_transport(transport),
            )
        except (Pi30Error, RuntimeError):
            return None
        if probe.resolution.resolution not in {
            RESOLUTION_EXACT,
            RESOLUTION_COMPATIBLE_GROUP,
            RESOLUTION_FAMILY,
        }:
            return None
        surface = load_compiled_detection_catalog().surfaces.get(
            probe.resolution.surface_key or ""
        )
        if surface is None:
            return None
        config_values = probe.values
        config_values["catalog_detection"] = probe.as_details()
        model_name = catalog_model_name(
            protocol_key="pi30",
            resolution=probe.resolution,
            values=config_values,
        )
        profile_name = surface.profile_name
        schema_name = surface.register_schema_name
        profile = load_driver_profile(profile_name)
        schema = load_register_schema(schema_name)
        config_values.update(_translate_config_enums(config_values, schema))
        serial_number = config_values.get("serial_number", "")
        if len(serial_number) < 6:
            return None

        return DetectedInverter(
            driver_key=self.key,
            protocol_family="pi30",
            model_name=model_name,
            variant_key=surface.variant_key,
            serial_number=serial_number,
            probe_target=target,
            details=config_values,
            profile_name=profile_name,
            register_schema_name=schema_name,
            capability_groups=profile.groups,
            capabilities=_build_pi30_capabilities(config_values, profile.capabilities),
        )

    async def async_read_values(
        self,
        transport,
        inverter: DetectedInverter,
        *,
        runtime_state: dict[str, Any] | None = None,
        poll_interval: float | None = None,
        now_monotonic: float | None = None,
    ) -> DriverReadResult:
        session = self._session(transport, inverter.probe_target)
        values, removed_keys, diagnostics = await _async_collect_runtime_values(
            session,
            runtime_state=runtime_state,
            poll_interval=poll_interval,
            now_monotonic=now_monotonic,
        )
        values.update(
            _translate_runtime_metadata(
                values,
                _schema_for_inverter(inverter, self.register_schema_name),
            )
        )
        # Always DELTA. PI30 runs a single sequential cycle of all reachable
        # commands, but the result is never a *proven* complete snapshot: optional
        # commands (QPIWS/Q1) may fail transiently, known-unsupported commands are
        # skipped, the energy chain early-exits, and any command's request+parse
        # can fail -- so PI30 does not claim FULL. Omitted values are simply not in
        # this cycle (never stale detection duplicates); a final unsupported
        # verdict invalidates a command's values via the explicit ``removed_keys``
        # contract. The first cycle fills the empty cache with every value it
        # successfully read; later cycles overlay onto last-good.
        return DriverReadResult(
            values=values,
            mode=DriverReadMode.DELTA,
            removed_keys=removed_keys,
            diagnostics=diagnostics,
        )

    async def async_read_onboarding_values(
        self,
        transport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        session = self._session(transport, inverter.probe_target)
        values = await _async_collect_values(session, _ONBOARDING_RUNTIME_COMMAND_SPECS)
        values.update(
            _translate_runtime_metadata(
                values,
                _schema_for_inverter(inverter, self.register_schema_name),
            )
        )

        for key in ("rated_power", "output_rating_active_power"):
            value = inverter.details.get(key)
            if value not in (None, ""):
                values.setdefault(key, value)

        battery_percent = values.get("battery_percent")
        battery_voltage = values.get("battery_voltage")
        if isinstance(battery_percent, int) and 0 <= battery_percent <= 100:
            values.setdefault("battery_connected", True)
            values.setdefault("battery_connection_state", "Connected")
        elif isinstance(battery_voltage, (int, float)) and float(battery_voltage) > 0:
            values.setdefault("battery_connected", True)
            values.setdefault("battery_connection_state", "Connected")

        return values

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
    ) -> Any:
        capability = _find_capability(capability_key, inverter.capabilities or self.write_capabilities)
        raw_value = _encode_capability_value(capability, value)
        command = _build_write_command(capability, raw_value)

        session = self._session(transport, inverter.probe_target)
        response = await session.request(command)
        if response != "ACK":
            raise RuntimeError(f"unexpected_write_response:{capability.key}:{response}")

        written_value = _decode_capability_value(capability, raw_value)
        inverter.details[capability.value_key] = written_value
        if capability.key != capability.value_key:
            inverter.details[capability.key] = written_value
        return written_value

    async def async_capture_support_evidence(self, transport, inverter: DetectedInverter) -> dict[str, Any]:
        session = self._session(transport, inverter.probe_target)
        responses: dict[str, str] = {}
        failures: dict[str, str] = {}

        for command in _support_commands():
            try:
                responses[command] = await session.request(command)
            except Exception as exc:
                failures[command] = str(exc)

        await _async_capture_energy_support(session, responses, failures)

        return {
            "capture_kind": "pi30_ascii_dump",
            "driver_key": self.key,
            "model_name": inverter.model_name,
            "serial_number": inverter.serial_number,
            "responses": responses,
            "failures": failures,
        }

    @staticmethod
    def _session(transport, target: ProbeTarget) -> Pi30Session:
        return Pi30Session(
            transport,
            route=target.link_route,
        )


def _schema_for_inverter(
    inverter: DetectedInverter | None,
    fallback_schema_name: str,
):
    schema_name = fallback_schema_name
    if inverter is not None and inverter.register_schema_name:
        schema_name = inverter.register_schema_name
    return load_register_schema(schema_name)


def _pi30_default_binding():
    surfaces = tuple(
        surface
        for surface in load_compiled_detection_catalog().surfaces.values()
        if surface.driver_key == "pi30" and surface.default_for_driver
    )
    if len(surfaces) != 1:
        raise RuntimeError("missing_default_surface:pi30")
    return surfaces[0]


def _support_commands() -> tuple[str, ...]:
    protocol = load_compiled_detection_catalog().protocols["pi30"]
    return tuple(
        dict.fromkeys(
            [
                *(
                    action.command
                    for action in protocol.probe_actions
                    if action.kind == "ascii_command" and action.command
                ),
                *(spec.command for spec in _RUNTIME_COMMAND_SPECS),
            ]
        )
    )


# The full universe of PI30 runtime commands (by STABLE cache key). It bounds
# the full-cycle-cost estimate and completeness accounting. A single cycle runs
# every reachable, not-unsupported command; a command may still be absent from a
# given cycle because it is unsupported or unreachable behind an unsupported
# prerequisite. Dynamic energy commands embed the date on the wire but are
# counted under these stable keys.
_PI30_FULL_CYCLE_COMMAND_KEYS: tuple[str, ...] = (
    "QPIGS",
    "QMOD",
    "QPIWS",
    "Q1",
    "QET",
    "QLT",
    "QT",
    "QEY",
    "QEM",
    "QED",
    "QLY",
    "QLM",
    "QLD",
)

# Dynamic energy commands (year/month/day) depend on QT's clock token; the whole
# energy chain depends on QET (its early exit stops QLT / QT / dynamic). These
# gate the reachability used to invalidate energy values below.
_PI30_ENERGY_DYNAMIC_COMMANDS: tuple[str, ...] = ("QEY", "QEM", "QED", "QLY", "QLM", "QLD")
_PI30_ENERGY_ALL_COMMANDS: tuple[str, ...] = ("QET", "QLT", "QT", *_PI30_ENERGY_DYNAMIC_COMMANDS)


def _pi30_command_output_keys() -> dict[str, frozenset[str]]:
    """Command (stable cache key) -> the runtime value keys it owns.

    Ownership lives here next to the specs/parsers (never in the hub): it maps
    each optional command to the direct parser keys AND any value derived from
    them, so a command that reaches a final unsupported verdict can invalidate
    exactly the keys it produced. Q1/QPIWS key sets are derived from the payload
    layouts so they cannot drift from the parsers.
    """

    return {
        "QPIWS": qpiws_output_keys() | frozenset({"alarm_status"}),
        "Q1": q1_output_keys() | frozenset({"inverter_charge_state"}),
        "QET": frozenset({"pv_generation_sum"}),
        "QLT": frozenset({"ac_in_generation_sum"}),
        "QT": frozenset(),  # no direct value; gates the dynamic commands
        "QEY": frozenset({"pv_generation_year"}),
        "QEM": frozenset({"pv_generation_month"}),
        "QED": frozenset({"pv_generation_day"}),
        "QLY": frozenset({"ac_in_generation_year"}),
        "QLM": frozenset({"ac_in_generation_month"}),
        "QLD": frozenset({"ac_in_generation_day"}),
    }


def _pi30_removed_keys_for_unsupported(unsupported: frozenset[str]) -> frozenset[str]:
    """Return the value keys to invalidate given the final-unsupported set.

    A command finally verdicted unsupported no longer produces its values, so a
    DELTA must drop them. Energy-chain reachability (current sequence, unchanged)
    is honoured: a final-unsupported QET makes the WHOLE energy chain unreachable;
    a final-unsupported QT makes only the dynamic year/month/day values
    unreachable while QET/QLT totals stay valid.
    """

    output = _pi30_command_output_keys()
    removed: set[str] = set()
    for command in unsupported:
        removed |= output.get(command, frozenset())
    if "QET" in unsupported:
        for command in _PI30_ENERGY_ALL_COMMANDS:
            removed |= output.get(command, frozenset())
    elif "QT" in unsupported:
        for command in _PI30_ENERGY_DYNAMIC_COMMANDS:
            removed |= output.get(command, frozenset())
    return frozenset(removed)


def _record_pi30_command_stat(
    runtime_state: dict[str, Any] | None,
    cache_key: str,
    wire_command: str,
    duration_ms: int,
    outcome: str,
    command_timings: list[tuple[str, str, int, str]] | None,
) -> None:
    """Record one command's real outcome for truthful runtime diagnostics.

    ``cache_key`` is the STABLE key (e.g. ``QEY``); ``wire_command`` is what was
    actually sent (e.g. ``QEY2026``). A skipped (known-unsupported) command is
    NOT counted as attempted. ``last_duration_ms`` tracks the last REAL attempt
    (ok / timeout / error), so a full-cycle estimate reflects timeouts too. This
    is diagnostics only and never influences scheduling.
    """

    if command_timings is not None:
        command_timings.append((cache_key, wire_command, duration_ms, outcome))
    if runtime_state is None:
        return
    stats = runtime_state.setdefault("pi30_command_stats", {})
    entry = stats.get(cache_key)
    if entry is None:
        entry = {
            "attempted": 0,
            "succeeded": 0,
            "timeout": 0,
            "error": 0,
            "skipped": 0,
            "last_duration_ms": None,
            "max_duration_ms": 0,
            "last_outcome": "",
            "last_wire_command": "",
        }
        stats[cache_key] = entry
    entry["last_wire_command"] = wire_command
    entry["last_outcome"] = outcome
    if outcome == "skipped":
        entry["skipped"] += 1
        return
    entry["attempted"] += 1
    if outcome == "ok":
        entry["succeeded"] += 1
    elif outcome == "timeout":
        entry["timeout"] += 1
    elif outcome == "error":
        entry["error"] += 1
    entry["last_duration_ms"] = duration_ms
    entry["max_duration_ms"] = max(int(entry["max_duration_ms"]), duration_ms)


def _command_outcome(exc: BaseException) -> str:
    return "timeout" if "timeout" in str(exc).lower() else "error"


async def _async_collect_values(
    session: Pi30Session,
    specs: tuple[Pi30CommandSpec, ...],
    *,
    runtime_state: dict[str, Any] | None = None,
    now_monotonic: float | None = None,
    command_timings: list[tuple[str, str, int, str]] | None = None,
) -> dict[str, Any]:
    values: dict[str, Any] = {}

    for spec in specs:
        if spec.optional and _command_skipped_as_unsupported(runtime_state, spec.command):
            _record_pi30_command_stat(
                runtime_state, spec.command, spec.command, 0, "skipped", command_timings
            )
            continue
        started = time.monotonic()
        try:
            payload = await session.request(spec.command)
            values.update(spec.parser(payload))
        except Pi30Error as exc:
            duration_ms = int(round((time.monotonic() - started) * 1000.0))
            _record_pi30_command_stat(
                runtime_state, spec.command, spec.command, duration_ms, _command_outcome(exc), command_timings
            )
            if not spec.optional:
                raise
            _record_command_failure(runtime_state, spec.command)
        else:
            duration_ms = int(round((time.monotonic() - started) * 1000.0))
            _record_pi30_command_stat(
                runtime_state, spec.command, spec.command, duration_ms, "ok", command_timings
            )
            _record_command_success(runtime_state, spec.command)

    return values


def _pi30_unreachable_commands(unsupported: frozenset[str]) -> frozenset[str]:
    """Return the commands made unreachable by a finally-unsupported prerequisite.

    Mirrors the energy-chain gating exactly (unchanged sequence): a final QET
    verdict makes QLT / QT and every dynamic command unreachable; a final QT
    verdict makes only the clock-token dynamic commands unreachable. A command
    that is itself unsupported is reported as unsupported, not unreachable.
    """

    unreachable: set[str] = set()
    if "QET" in unsupported:
        unreachable |= {"QLT", "QT", *_PI30_ENERGY_DYNAMIC_COMMANDS}
    elif "QT" in unsupported:
        unreachable |= set(_PI30_ENERGY_DYNAMIC_COMMANDS)
    return frozenset(unreachable - unsupported)


def _build_pi30_read_meta(
    *,
    runtime_state: dict[str, Any] | None,
    command_timings: list[tuple[str, str, int, str]],
    cycle_wall_ms: int,
) -> dict[str, Any]:
    """Return truthful structured diagnostics for one single-cycle PI30 poll.

    ``command_timings`` entries are ``(cache_key, wire_command, duration_ms,
    outcome)`` in the ACTUAL executed order. ``cycle_wall_ms`` is the wall-clock
    of the whole single sequential cycle. The read mode is always DELTA; the
    full-cycle estimate is reachability-aware (see :func:`_pi30_unreachable_commands`).
    """

    attempted = [t for t in command_timings if t[3] != "skipped"]
    skipped = [t for t in command_timings if t[3] == "skipped"]

    diagnostics: dict[str, Any] = {
        "pi30_poll_mode": DriverReadMode.DELTA.value,
        # Actual wire commands executed this cycle, in order (dynamic energy names
        # included).
        "pi30_poll_commands": ", ".join(wire for _k, wire, _ms, _o in attempted),
        # Known-unsupported commands skipped this cycle, kept separate.
        "pi30_poll_skipped_commands": ", ".join(cache_key for cache_key, _w, _ms, _o in skipped),
        "pi30_poll_command_durations_ms": ", ".join(
            f"{wire}={ms}:{outcome}" for _k, wire, ms, outcome in attempted
        ),
        # Wall-clock of the whole single cycle (not a sum of pieces).
        "pi30_poll_total_ms": cycle_wall_ms,
        "pi30_poll_attempted": len(attempted),  # only actually-sent commands
        "pi30_poll_skipped": len(skipped),
        "pi30_poll_succeeded": sum(1 for _k, _w, _ms, o in attempted if o == "ok"),
        "pi30_poll_timeout": sum(1 for _k, _w, _ms, o in attempted if o == "timeout"),
        "pi30_poll_error": sum(1 for _k, _w, _ms, o in attempted if o == "error"),
    }

    if runtime_state is None:
        return diagnostics

    stats = runtime_state.get("pi30_command_stats", {})
    if stats:
        diagnostics["pi30_command_cumulative"] = ", ".join(
            f"{key}:att{e['attempted']}/ok{e['succeeded']}/to{e['timeout']}"
            f"/err{e['error']}/sk{e['skipped']}"
            f"/last{e['last_duration_ms'] if e['last_duration_ms'] is not None else '-'}ms"
            f"/max{e['max_duration_ms']}ms/{e['last_outcome'] or '-'}"
            for key, e in sorted(stats.items())
        )

    # Reachability-aware full-cycle estimate. A command's LAST REAL duration
    # (ok / timeout / error) feeds the estimate; NO duration is invented for a
    # command that was skipped-unsupported or is unreachable behind a
    # finally-unsupported prerequisite. "Expected" is the set of reachable,
    # not-unsupported commands, so the estimate is complete once every reachable
    # command has a measured duration -- unreachable commands are reported
    # separately and never counted as unmeasured.
    unsupported = frozenset(_unsupported_commands(runtime_state))
    unreachable = _pi30_unreachable_commands(unsupported)

    def _measured(key: str) -> bool:
        entry = stats.get(key)
        return isinstance(entry, dict) and entry["last_duration_ms"] is not None

    reachable = [
        key
        for key in _PI30_FULL_CYCLE_COMMAND_KEYS
        if key not in unsupported and key not in unreachable
    ]
    measured_reachable = [key for key in reachable if _measured(key)]
    unmeasured = [key for key in reachable if not _measured(key)]

    diagnostics["pi30_estimated_full_cycle_ms"] = sum(
        int(stats[key]["last_duration_ms"]) for key in measured_reachable
    )
    diagnostics["pi30_estimated_full_cycle_measured_commands"] = len(measured_reachable)
    diagnostics["pi30_full_cycle_expected_commands"] = len(reachable)
    diagnostics["pi30_full_cycle_unmeasured_commands"] = ", ".join(unmeasured)
    diagnostics["pi30_full_cycle_unreachable_commands"] = ", ".join(sorted(unreachable))
    diagnostics["pi30_full_cycle_estimate_complete"] = not unmeasured
    diagnostics["pi30_known_unsupported_commands"] = ", ".join(sorted(unsupported))

    return diagnostics


async def _async_collect_runtime_values(
    session: Pi30Session,
    *,
    runtime_state: dict[str, Any] | None,
    poll_interval: float | None,
    now_monotonic: float | None,
) -> tuple[dict[str, Any], frozenset[str], dict[str, Any]]:
    """Run ONE PI30 runtime poll as a single sequential command cycle.

    There is no fast/medium/slow scheduling and no empty cycle: every poll
    executes QPIGS, QMOD, then the optional QPIWS / Q1 (auto-skipped when finally
    unsupported), then the reachable energy chain (existing order + early-exit),
    back to back with no artificial pause. The result is always a DELTA (see
    :meth:`Pi30Driver.async_read_values`): a transient failure keeps last-good and
    a final unsupported verdict invalidates the command's values via
    ``removed_keys``. ``poll_interval`` / ``now_monotonic`` are accepted for the
    driver contract but no longer gate anything -- the whole-cycle wall-clock is
    the honest cost the neutral auto poll policy consumes.
    """

    command_timings: list[tuple[str, str, int, str]] = []
    cycle_started = time.monotonic()

    values = await _async_collect_values(
        session, _RUNTIME_COMMAND_SPECS, runtime_state=runtime_state, command_timings=command_timings
    )
    values.update(
        await _async_collect_energy_values(
            session, runtime_state=runtime_state, command_timings=command_timings
        )
    )

    cycle_wall_ms = int(round((time.monotonic() - cycle_started) * 1000.0))

    removed_keys: frozenset[str] = frozenset()
    if runtime_state is not None:
        _commit_cycle_failures(runtime_state)
        apply_unsupported_diagnostics(values, runtime_state)
        # Invalidate values owned by any command that reached a FINAL unsupported
        # verdict (incl. energy-chain reachability) plus the ephemeral unsupported
        # diagnostics key when the set is empty. Never remove a value read THIS cycle.
        removed_keys = frozenset(
            (
                _pi30_removed_keys_for_unsupported(
                    frozenset(_unsupported_commands(runtime_state))
                )
                | _unsupported_diagnostics_removed_keys(runtime_state)
            )
            - set(values)
        )

    diagnostics = _build_pi30_read_meta(
        runtime_state=runtime_state,
        command_timings=command_timings,
        cycle_wall_ms=cycle_wall_ms,
    )
    return values, removed_keys, diagnostics


async def _async_collect_energy_values(
    session: Pi30Session,
    *,
    runtime_state: dict[str, Any] | None = None,
    now_monotonic: float | None = None,
    command_timings: list[tuple[str, str, int, str]] | None = None,
) -> dict[str, Any]:
    values: dict[str, Any] = {}

    async def _atomic(
        command: str,
        cache_key: str,
        parse: Callable[[str], dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Run request+parse as ONE command operation.

        The command counts as ``ok`` only after BOTH the transport response and
        a successful parse; a transport failure OR a parser failure is recorded
        as timeout/error (never ``ok``) and staged as a command failure. The
        measured duration covers the whole request+parse operation. Dynamic
        commands embed the date on the wire but are tracked under the stable
        ``cache_key``.
        """

        if _command_skipped_as_unsupported(runtime_state, cache_key):
            _record_pi30_command_stat(
                runtime_state, cache_key, command, 0, "skipped", command_timings
            )
            return None
        started = time.monotonic()
        try:
            payload = await session.request(command)
            parsed = parse(payload)
        except Pi30Error as exc:
            duration_ms = int(round((time.monotonic() - started) * 1000.0))
            _record_pi30_command_stat(
                runtime_state, cache_key, command, duration_ms, _command_outcome(exc), command_timings
            )
            _record_command_failure(runtime_state, cache_key)
            return None
        duration_ms = int(round((time.monotonic() - started) * 1000.0))
        _record_pi30_command_stat(
            runtime_state, cache_key, command, duration_ms, "ok", command_timings
        )
        _record_command_success(runtime_state, cache_key)
        return parsed

    # QET gates the whole chain: a transient failure (transport OR malformed
    # payload) stops this cycle and keeps last-good values.
    parsed = await _atomic("QET", "QET", lambda p: parse_energy_counter(p, key="pv_generation_sum"))
    if parsed is None:
        return values
    values.update(parsed)

    # QLT is independent of QT/dynamic: its failure must NOT block them.
    parsed = await _atomic("QLT", "QLT", lambda p: parse_energy_counter(p, key="ac_in_generation_sum"))
    if parsed is not None:
        values.update(parsed)

    # QT gates only the clock-token dynamic commands: its failure stops just them.
    parsed = await _atomic("QT", "QT", parse_qt_clock)
    if parsed is None:
        return values
    clock_token = parsed["clock_token"]

    dynamic_specs = (
        (f"QEY{clock_token[:4]}", "QEY", "pv_generation_year"),
        (f"QEM{clock_token[:6]}", "QEM", "pv_generation_month"),
        (f"QED{clock_token[:8]}", "QED", "pv_generation_day"),
        (f"QLY{clock_token[:4]}", "QLY", "ac_in_generation_year"),
        (f"QLM{clock_token[:6]}", "QLM", "ac_in_generation_month"),
        (f"QLD{clock_token[:8]}", "QLD", "ac_in_generation_day"),
    )
    for command, cache_key, key in dynamic_specs:
        # A malformed single dynamic command fails only itself; the loop
        # continues so independent dynamic commands still run.
        parsed = await _atomic(
            command, cache_key, lambda p, key=key: parse_energy_counter(p, key=key)
        )
        if parsed is not None:
            values.update(parsed)

    return values


async def _async_capture_energy_support(
    session: Pi30Session,
    responses: dict[str, str],
    failures: dict[str, str],
) -> None:
    for command in ("QET", "QLT", "QT"):
        try:
            responses[command] = await session.request(command)
        except Exception as exc:
            failures[command] = str(exc)

    qt_payload = responses.get("QT")
    if not qt_payload:
        return

    try:
        clock_token = parse_qt_clock(qt_payload)["clock_token"]
    except Pi30Error as exc:
        failures["QT"] = str(exc)
        return

    for command in (
        f"QEY{clock_token[:4]}",
        f"QEM{clock_token[:6]}",
        f"QED{clock_token[:8]}",
        f"QLY{clock_token[:4]}",
        f"QLM{clock_token[:6]}",
        f"QLD{clock_token[:8]}",
    ):
        try:
            responses[command] = await session.request(command)
        except Exception as exc:
            failures[command] = str(exc)


def _translate_config_enums(values: dict[str, Any], schema) -> dict[str, Any]:
    translated: dict[str, Any] = {}

    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="battery_type",
        code_key="battery_type_code",
        enum_table="battery_type_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="input_voltage_range",
        code_key="input_voltage_range_code",
        enum_table="input_voltage_range_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="output_source_priority",
        code_key="output_source_priority_code",
        enum_table="output_source_priority_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="charger_source_priority",
        code_key="charger_source_priority_code",
        enum_table="charger_source_priority_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="machine_type",
        code_key="machine_type_code",
        enum_table="machine_type_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="topology",
        code_key="topology_code",
        enum_table="topology_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="output_mode",
        code_key="output_mode_code",
        enum_table="output_mode_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="operation_logic",
        code_key="operation_logic_code",
        enum_table="operation_logic_names",
    )

    return translated


def _translate_config_enum(
    translated: dict[str, Any],
    values: dict[str, Any],
    schema,
    *,
    value_key: str,
    code_key: str,
    enum_table: str,
) -> None:
    raw_code = values.get(code_key)
    if not isinstance(raw_code, int) or schema is None:
        return

    translated[value_key] = schema.enum_map_for(enum_table).get(raw_code, f"Unknown ({raw_code})")


def _translate_runtime_metadata(values: dict[str, Any], schema) -> dict[str, Any]:
    translated: dict[str, Any] = {}

    operating_mode_code = values.get("operating_mode_code")
    if isinstance(operating_mode_code, str) and schema is not None:
        translated["operating_mode"] = schema.enum_map_for("operating_mode_names").get(
            operating_mode_code,
            f"Unknown ({operating_mode_code})",
        )

    alarm_bits = values.get("alarm_bits_raw")
    if isinstance(alarm_bits, str):
        translated["alarm_status"] = _format_alarm_status(alarm_bits, schema)

    charge_state_code = values.get("inverter_charge_state_code")
    if isinstance(charge_state_code, int) and schema is not None:
        translated["inverter_charge_state"] = schema.enum_map_for("inverter_charge_state_names").get(
            charge_state_code,
            f"Unknown ({charge_state_code})",
        )

    return translated


def _format_alarm_status(alarm_bits: str, schema) -> str:
    if not any(bit == "1" for bit in alarm_bits):
        return "Ok"
    if schema is None:
        return "Unknown"

    labels = schema.bit_labels_for("alarm_status_names")
    active_labels = [
        label
        for index, label in labels.items()
        if index < len(alarm_bits) and alarm_bits[index] == "1"
    ]
    active_labels.extend(
        f"Unknown alarm bit {index}"
        for index, bit in enumerate(alarm_bits)
        if bit == "1" and index not in labels
    )
    return "; ".join(active_labels) if active_labels else "Ok"


def _build_pi30_capabilities(
    values: dict[str, Any],
    capabilities: tuple[WriteCapability, ...],
) -> tuple[WriteCapability, ...]:
    battery_rating_voltage = values.get("battery_rating_voltage")
    if not isinstance(battery_rating_voltage, (int, float)) or battery_rating_voltage <= 0:
        return capabilities

    scale = float(battery_rating_voltage) / 48.0
    scaled: list[WriteCapability] = []
    for capability in capabilities:
        if capability.key == "battery_recharge_voltage":
            scaled.append(
                _replace_voltage_range(capability, scale=scale, minimum=440, maximum=510)
            )
            continue
        if capability.key == "battery_redischarge_voltage":
            scaled.append(
                _replace_voltage_range(capability, scale=scale, minimum=0, maximum=580)
            )
            continue
        if capability.key == "battery_under_voltage":
            scaled.append(
                _replace_voltage_range(capability, scale=scale, minimum=400, maximum=480)
            )
            continue
        if capability.key in {"battery_bulk_voltage", "battery_float_voltage"}:
            scaled.append(
                _replace_voltage_range(capability, scale=scale, minimum=480, maximum=584)
            )
            continue
        scaled.append(capability)
    return tuple(scaled)


def _replace_voltage_range(
    capability: WriteCapability,
    *,
    scale: float,
    minimum: int,
    maximum: int,
) -> WriteCapability:
    # Copy ONLY the two changed fields via dataclasses.replace. The previous
    # field-by-field rebuild silently dropped every field it forgot to list
    # (word_count, combine, bitmask, provenance, experimental, metadata_scope) —
    # resetting them to defaults, which e.g. reset provenance to 'inferred' and
    # quietly changed the runtime write-gating. replace() preserves everything
    # and is immune to new WriteCapability fields.
    return replace(
        capability,
        minimum=int(round(minimum * scale)),
        maximum=int(round(maximum * scale)),
    )


def _find_capability(
    capability_key: str,
    capabilities: tuple[WriteCapability, ...],
) -> WriteCapability:
    for capability in capabilities:
        if capability.key == capability_key:
            return capability
    raise ValueError(f"unsupported_capability:{capability_key}")


def _encode_capability_value(capability: WriteCapability, value: Any) -> int:
    if capability.value_kind == "bool":
        return _encode_bool_value(capability, value)
    if capability.value_kind == "enum":
        return _encode_enum_value(capability, value)
    if capability.value_kind == "scaled_u16":
        return _encode_scaled_u16_value(capability, value)
    if capability.value_kind == "u16":
        return _encode_u16_value(capability, value)
    raise ValueError(f"unsupported_value_kind:{capability.value_kind}")


def _decode_capability_value(capability: WriteCapability, raw_value: int) -> Any:
    enum_map = capability.enum_value_map
    if capability.value_kind == "bool":
        if enum_map:
            return enum_map.get(raw_value, bool(raw_value))
        return bool(raw_value)
    if enum_map:
        return enum_map.get(raw_value, f"Unknown ({raw_value})")
    if capability.divisor:
        return round(raw_value / capability.divisor, decimals_for_divisor(capability.divisor))
    return raw_value


def _encode_enum_value(capability: WriteCapability, value: Any) -> int:
    enum_map = capability.enum_value_map
    if isinstance(value, int):
        raw_value = value
    else:
        text = str(value).strip()
        if text.isdigit():
            raw_value = int(text)
        else:
            reverse_map = {label: key for key, label in enum_map.items()}
            if text not in reverse_map:
                raise ValueError(f"unsupported_enum_value:{capability.key}:{text}")
            raw_value = reverse_map[text]

    if raw_value not in enum_map:
        raise ValueError(f"unsupported_enum_raw:{capability.key}:{raw_value}")
    return raw_value


def _encode_bool_value(capability: WriteCapability, value: Any) -> int:
    if isinstance(value, bool):
        raw_value = 1 if value else 0
    elif isinstance(value, int):
        raw_value = value
    else:
        text = str(value).strip().lower()
        truthy = {"1", "true", "on", "yes", "enable", "enabled"}
        falsy = {"0", "false", "off", "no", "disable", "disabled"}
        if text in truthy:
            raw_value = 1
        elif text in falsy:
            raw_value = 0
        else:
            raise ValueError(f"unsupported_bool_value:{capability.key}:{value}")

    if raw_value not in {0, 1}:
        raise ValueError(f"unsupported_bool_raw:{capability.key}:{raw_value}")
    return raw_value


def _encode_scaled_u16_value(capability: WriteCapability, value: Any) -> int:
    if capability.divisor is None:
        raise ValueError(f"missing_divisor:{capability.key}")

    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_numeric_value:{capability.key}:{value}") from exc

    raw_value = int(round(numeric * capability.divisor))
    _validate_range(capability, raw_value)
    return raw_value


def _encode_u16_value(capability: WriteCapability, value: Any) -> int:
    try:
        raw_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_integer_value:{capability.key}:{value}") from exc

    _validate_range(capability, raw_value)
    return raw_value


def _validate_range(capability: WriteCapability, raw_value: int) -> None:
    if capability.minimum is not None and raw_value < capability.minimum:
        raise ValueError(f"value_below_minimum:{capability.key}:{raw_value}")
    if capability.maximum is not None and raw_value > capability.maximum:
        raise ValueError(f"value_above_maximum:{capability.key}:{raw_value}")


def _build_write_command(capability: WriteCapability, raw_value: int) -> str:
    if capability.key in _PI30_BOOL_COMMANDS:
        prefix = "PE" if raw_value else "PD"
        return f"{prefix}{_PI30_BOOL_COMMANDS[capability.key]}"
    if capability.key in _PI30_ENUM_COMMANDS:
        return f"{_PI30_ENUM_COMMANDS[capability.key]}{raw_value:02d}"
    if capability.key in _PI30_NUMERIC_COMMANDS:
        return f"{_PI30_NUMERIC_COMMANDS[capability.key]}{_format_scaled_value(capability, raw_value)}"
    raise ValueError(f"unsupported_write_command:{capability.key}")


def _format_scaled_value(capability: WriteCapability, raw_value: int) -> str:
    if capability.divisor is None:
        return str(raw_value)
    return f"{raw_value / capability.divisor:.{decimals_for_divisor(capability.divisor)}f}"
