"""SMG-family inverter driver over EyeBond transport + Modbus RTU."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ..models import (
    DetectedInverter,
    ProbeTarget,
    RegisterValueSpec,
    WriteCapability,
    decimals_for_divisor,
)
from ..payload.modbus import (
    ModbusError,
    ModbusSession,
    merge_register_field,
    to_signed_16,
)
from ..payload.register_decode import decode_block as shared_decode_block
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from ..metadata.detection_evidence import (
    anchor_evidence_from_catalog_identity_probe,
    build_descriptor_decision_report,
)
from ..metadata.compiled_detection_catalog import (
    RESOLUTION_FAMILY,
    load_compiled_detection_catalog,
)
from ..metadata.device_catalog_loader import (
    CatalogBinding,
    RuntimeProbePolicy,
    SupportCapturePolicy,
    load_device_catalog,
    resolve_catalog_surface_binding,
    resolve_runtime_probe_policy,
    resolve_support_capture_policy,
)
from .base import InverterDriver
from .capability_codec import (
    decode_capability_value as _decode_capability_value,
    encode_capability_words as _encode_capability_words,
    find_capability as _find_capability,
)
from .catalog_identity import (
    CatalogIdentityProbe,
    InverterIdentityNoDataError,
    async_probe_catalog_identity,
    attach_catalog_match_details,
    catalog_match_from_resolution,
    probe_indicates_link_down,
)


_SMG_FAMILY_FALLBACK_VARIANT = "family_fallback"


class CapabilityPreWriteReadError(RuntimeError):
    """A masked write's read-modify-write failed on its PRE-WRITE read.

    No write was attempted, so the hub must NOT classify it as a write
    rejection (which would record a persistent 'unsupported_or_locked' blocker).
    It carries no Modbus exception code, so the hub's write-error classifier
    skips it and the failure surfaces as a plain transient error.
    """


def _attach_descriptor_decision_shadow_details(
    detected: DetectedInverter,
    catalog_probe: CatalogIdentityProbe | None,
    *,
    selection_source: str,
) -> None:
    """Attach the backward-compatible descriptor decision diagnostic alias."""

    catalog_match_kind = ""
    catalog_entry_key = ""
    if catalog_probe is not None:
        catalog_match_kind = str(catalog_probe.match.kind or "")
        if catalog_probe.match.entry is not None:
            catalog_entry_key = catalog_probe.match.entry.entry_key

    evidence = anchor_evidence_from_catalog_identity_probe(catalog_probe)
    rated_power = detected.details.get("rated_power")
    if rated_power is not None:
        evidence["fingerprint.rated_power"] = rated_power
    report = build_descriptor_decision_report(
        protocol_family=detected.protocol_family,
        evidence=evidence,
        catalog_match_kind=catalog_match_kind,
        catalog_entry_key=catalog_entry_key,
    )
    report["selection"] = {
        "source": selection_source,
        "safe_switch_active": True,
        "runtime_variant_key": detected.variant_key,
        "runtime_profile_name": detected.profile_name,
        "runtime_register_schema_name": detected.register_schema_name,
    }
    detected.details["descriptor_decision_shadow"] = report
    device_catalog = detected.details.get("device_catalog")
    if isinstance(device_catalog, dict):
        device_catalog["descriptor_decision"] = report


class SmgModbusDriver(InverterDriver):
    """Bench-safe SMG probe and runtime reader."""

    key = "modbus_smg"
    name = "SMG / Modbus"

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
        return _smg_default_binding().profile_name

    @property
    def register_schema_name(self) -> str:
        return _smg_default_binding().register_schema_name

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

    @property
    def capability_presets(self):
        profile = self.profile_metadata
        return profile.presets if profile is not None else ()

    async def async_probe(
        self,
        transport,
        target: ProbeTarget,
    ) -> DetectedInverter | None:
        session = self._session(transport, target)

        # The offline device catalog is the identification authority. An
        # identity region that READS as zeros means the collector has no
        # inverter link right now — probing deeper would only misreport an
        # unsupported device (the 2026-06-08 false negative class).
        try:
            catalog_probe = await async_probe_catalog_identity(session)
        except Exception:
            catalog_probe = None
        if probe_indicates_link_down(catalog_probe):
            raise InverterIdentityNoDataError()

        if catalog_probe is not None:
            return await self._async_probe_with_catalog(session, target, catalog_probe)

        # No catalog opinion: descriptor-driven SMG detection abstains instead
        # of guessing through legacy variant scoring.
        return None

    async def _async_probe_with_catalog(
        self,
        session: ModbusSession,
        target: ProbeTarget,
        catalog_probe: CatalogIdentityProbe,
    ) -> DetectedInverter | None:
        resolution = catalog_probe.compiled_resolution
        if resolution is None or not resolution.resolved:
            return None

        detected = await self._async_probe_resolution(
            session,
            target,
            catalog_probe,
            resolution=resolution,
            selection_source=f"compiled_catalog_{resolution.resolution}",
        )
        if detected is not None:
            return detected
        if resolution.resolution == RESOLUTION_FAMILY:
            return None

        family_resolution = load_compiled_detection_catalog().resolve_family(
            protocol_key=self.key,
            evidence=anchor_evidence_from_catalog_identity_probe(catalog_probe),
        )
        if not family_resolution.resolved:
            return None
        family_probe = CatalogIdentityProbe(
            layout_code=catalog_probe.layout_code,
            model_code=catalog_probe.model_code,
            rated_power=catalog_probe.rated_power,
            serial_ascii=catalog_probe.serial_ascii,
            match=catalog_match_from_resolution(
                resolution=family_resolution,
                layout_code=catalog_probe.layout_code or 0,
                rated_power=catalog_probe.rated_power,
                serial_ascii=catalog_probe.serial_ascii,
            ),
            compiled_resolution=family_resolution,
            probe_action_keys=catalog_probe.probe_action_keys,
            failed_probe_action_keys=catalog_probe.failed_probe_action_keys,
        )
        return await self._async_probe_resolution(
            session,
            target,
            family_probe,
            resolution=family_resolution,
            selection_source="compiled_catalog_runtime_fallback",
        )

    async def _async_probe_resolution(
        self,
        session: ModbusSession,
        target: ProbeTarget,
        catalog_probe: CatalogIdentityProbe,
        *,
        resolution,
        selection_source: str,
    ) -> DetectedInverter | None:
        catalog = load_compiled_detection_catalog()
        surface = catalog.surfaces.get(resolution.surface_key or "")
        if surface is None:
            return None
        descriptors = tuple(
            catalog.devices[key]
            for key in resolution.candidate_keys
            if key in catalog.devices
        )
        model_names = {descriptor.model_name for descriptor in descriptors}
        binding = CatalogBinding(
            driver_key=surface.driver_key,
            variant_key=surface.variant_key,
            profile_name=surface.profile_name,
            register_schema_name=surface.register_schema_name,
        )
        detected = await self._async_probe_binding(
            session,
            target,
            binding,
            runtime_probe=_runtime_probe_policy_for_resolution(
                resolution.candidate_keys,
                resolution.surface_key or "",
                binding,
            ),
            model_name=next(iter(model_names)) if len(model_names) == 1 else "SMG",
        )
        if detected is None:
            return None
        attach_catalog_match_details(detected, catalog_probe)
        _attach_descriptor_decision_shadow_details(
            detected,
            catalog_probe,
            selection_source=selection_source,
        )
        return detected

    async def _async_probe_binding(
        self,
        session: ModbusSession,
        target: ProbeTarget,
        binding,
        *,
        runtime_probe: RuntimeProbePolicy | None = None,
        model_name: str = "",
    ) -> DetectedInverter | None:
        try:
            schema = load_register_schema(binding.register_schema_name)
        except Exception:
            return None

        try:
            profile = load_driver_profile(binding.profile_name) if binding.profile_name else None
        except Exception:
            return None

        try:
            serial_block = await session.read_holding(
                schema.block("serial").start,
                schema.block("serial").count,
            )
            # Read the serial for display/diagnostics only. The SmartESS server does NOT bind
            # identity to the inverter serial, and neither do we: a short or blank serial must
            # not block detection of an otherwise-identified inverter.
            serial_number = _decode_ascii_words(serial_block)

            live_block = await session.read_holding(
                schema.block("live").start,
                schema.block("live").count,
            )
            live_values = _decode_block(
                schema.block("live").start,
                live_block,
                _specs_for_block(
                    schema.spec_set("live"),
                    schema.block("live").start,
                    schema.block("live").count,
                ),
            )

            config_block = await session.read_holding(
                schema.block("config").start,
                schema.block("config").count,
            )
            config_values = _decode_block(
                schema.block("config").start,
                config_block,
                _specs_for_block(
                    schema.spec_set("config"),
                    schema.block("config").start,
                    schema.block("config").count,
                ),
            )
            config_values.update(await _read_optional_specs(session, schema.spec_set("aux_config")))
            rated_power = await _read_rated_power(session, schema)
        except Exception:
            return None

        probe_policy = runtime_probe or _runtime_probe_policy_for_binding(binding)
        if not _is_valid_smg_probe(config_values, probe_policy, rated_power=rated_power):
            return None

        details = dict(config_values)
        details.update(
            await _read_optional_specs(
                session,
                _optional_probe_specs_for_policy(probe_policy),
            )
        )
        details.update(
            await _read_optional_ascii_ranges(
                session,
                _optional_ascii_probe_ranges_for_policy(probe_policy),
            )
        )
        if rated_power:
            details["rated_power"] = rated_power

        return DetectedInverter(
            driver_key=self.key,
            protocol_family="modbus_smg",
            model_name=_smg_model_name(model_name, rated_power),
            serial_number=serial_number,
            probe_target=target,
            variant_key=binding.variant_key,
            details=details,
            profile_name=binding.profile_name,
            register_schema_name=binding.register_schema_name,
            capability_groups=profile.groups if profile is not None else (),
            capabilities=profile.capabilities if profile is not None else (),
            capability_presets=profile.presets if profile is not None else (),
        )

    async def async_read_values(
        self,
        transport,
        inverter: DetectedInverter,
        *,
        runtime_state: dict[str, Any] | None = None,
        poll_interval: float | None = None,
        now_monotonic: float | None = None,
    ) -> dict[str, Any]:
        schema = _schema_for_inverter(inverter, self.register_schema_name)
        if schema is None:
            return {}
        status_block_start = schema.block("status").start
        status_block_count = schema.block("status").count
        live_block_start = schema.block("live").start
        live_block_count = schema.block("live").count
        config_block_start = schema.block("config").start
        config_block_count = schema.block("config").count
        status_fields = _specs_for_block(schema.spec_set("status"), status_block_start, status_block_count)
        live_fields = _specs_for_block(schema.spec_set("live"), live_block_start, live_block_count)
        config_fields = _specs_for_block(schema.spec_set("config"), config_block_start, config_block_count)
        aux_config_fields = schema.spec_set("aux_config")
        fault_code_names = schema.bit_labels_for("fault_code_names")
        warning_code_names = schema.bit_labels_for("warning_code_names")

        session = self._session(transport, inverter.probe_target)

        status_block = await session.read_holding(status_block_start, status_block_count)
        status_values = _decode_block(status_block_start, status_block, status_fields)

        live_block = await session.read_holding(live_block_start, live_block_count)
        values = _decode_block(live_block_start, live_block, live_fields)
        values.update(status_values)

        config_block = await session.read_holding(config_block_start, config_block_count)
        values.update(_decode_block(config_block_start, config_block, config_fields))
        values.update(await _read_optional_specs(session, aux_config_fields))
        missing_probe_details = await _read_missing_optional_probe_details(session, inverter)
        if missing_probe_details:
            inverter.details.update(missing_probe_details)
            values.update(missing_probe_details)
        values.update(_derive_inverter_clock(values))
        values.update(_derive_pv_channel_summary(values))

        fault_descriptions = _decode_named_bits(
            values.get("fault_code"),
            fault_code_names,
            one_based=True,
            include_unknown=True,
            unknown_label_prefix="Unknown Fault Bit",
        )
        warning_descriptions = _decode_named_bits(
            values.get("warning_code"),
            warning_code_names,
            include_unknown=True,
            unknown_label_prefix="Unknown Warning Bit",
        )
        values["fault_count"] = len(fault_descriptions)
        values["warning_count"] = len(warning_descriptions)
        values["fault_descriptions"] = ", ".join(fault_descriptions) if fault_descriptions else "None"
        values["warning_descriptions"] = ", ".join(warning_descriptions) if warning_descriptions else "None"

        warning_code = values.get("warning_code")
        battery_present = not (isinstance(warning_code, int) and bool(warning_code & (1 << 9)))
        values["battery_connected"] = battery_present
        values["battery_connection_state"] = "Connected" if battery_present else "Not Connected"
        if not battery_present:
            values.pop("battery_voltage", None)
            values.pop("battery_percent", None)
            values.pop("battery_current", None)
            values.pop("battery_average_current", None)
            values.pop("battery_average_power", None)

        battery_percent = values.get("battery_percent")
        if isinstance(battery_percent, int) and not (0 <= battery_percent <= 100):
            values.pop("battery_percent", None)

        battery_voltage = values.get("battery_voltage")
        if isinstance(battery_voltage, (int, float)) and battery_voltage <= 0:
            values.pop("battery_voltage", None)

        values.update(_derive_runtime_states(values))
        # Read-back for controls that have a register but no decode spec -- notably
        # learned-overlay controls, which otherwise toggle but report no current state. This
        # never changes the schema/scope, so the write-exposure proof is unaffected.
        polled_blocks = (
            (status_block_start, status_block),
            (live_block_start, live_block),
            (config_block_start, config_block),
        )
        # A few learned controls (e.g. Boot method reg 406, Output control reg 420) sit OUTSIDE
        # the polled blocks; read those single registers directly so they too report state.
        # The aux_config registers were already fetched above, so exclude them from the budget.
        aux_registers = frozenset(
            spec.register + offset
            for spec in aux_config_fields
            for offset in range(max(int(getattr(spec, "word_count", 1) or 1), 1))
        )
        extra_blocks = await _read_out_of_block_capability_registers(
            session, inverter.capabilities, polled_blocks, already_read=aux_registers
        )
        _apply_capability_read_back(values, inverter.capabilities, polled_blocks + extra_blocks)
        return values

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
    ) -> Any:
        capability = _find_capability(capability_key, inverter.capabilities or self.write_capabilities)
        raw_words = _encode_capability_words(capability, value)

        session = self._session(transport, inverter.probe_target)
        if capability.bitmask:
            # The capability owns only some bits of a shared register: read the
            # current word first and rewrite it with ONLY the masked field
            # changed. A blind write would clobber the other bits, whose
            # meanings may be unknown (e.g. OP2 enable is bit 0 of reg 354).
            shift = capability.bitmask_shift
            field = (int(raw_words[0]) << shift) & 0xFFFF
            if field & ~capability.bitmask:
                raise ValueError(f"value_exceeds_bitmask:{capability.key}:{raw_words[0]}")
            try:
                current = await session.read_holding(capability.register, 1)
            except ModbusError as exc:
                # A Modbus exception on the PRE-WRITE read is NOT a write
                # rejection (no write happened): re-raise without a Modbus code
                # so the hub does not lock the control. A ConnectionError here
                # propagates unchanged so the hub's retry-after-reconnect still
                # applies.
                raise CapabilityPreWriteReadError(
                    f"bitmask_pre_write_read_failed:{capability.key}:{exc}"
                ) from exc
            if not current:
                raise CapabilityPreWriteReadError(
                    f"bitmask_read_back_empty:{capability.key}"
                )
            merged = merge_register_field(int(current[0]), capability.bitmask, field)
            await session.write_holding(capability.register, [merged])
        else:
            await session.write_holding(capability.register, raw_words)

        native_value = _decode_capability_value(capability, raw_words)
        inverter.details[capability.key] = native_value
        return native_value

    async def async_capture_support_evidence(
        self,
        transport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        """Capture raw SMG register evidence for support packages."""

        session = self._session(transport, inverter.probe_target)
        schema_name = inverter.register_schema_name or self.register_schema_name
        ranges = _support_capture_ranges(schema_name)
        captured_ranges: list[dict[str, Any]] = []
        fixture_ranges: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []

        for start, count in ranges:
            try:
                values = await session.read_holding(start, count)
            except Exception as exc:
                failures.append(
                    {
                        "start": start,
                        "count": count,
                        "error": str(exc),
                    }
                )
                continue

            formatted = _format_support_range(start, values)
            captured_ranges.append(formatted)
            fixture_ranges.append(
                {
                    "start": start,
                    "count": count,
                    "values": list(values),
                }
            )

        return {
            "capture_kind": "modbus_register_dump",
            "driver_key": self.key,
            "model_name": inverter.model_name,
            "serial_number": inverter.serial_number,
            "capture_notes": list(_support_capture_notes(schema_name)),
            "planned_ranges": [
                {"start": start, "count": count}
                for start, count in ranges
            ],
            "captured_ranges": captured_ranges,
            "range_failures": failures,
            "fixture_ranges": fixture_ranges,
        }

    @staticmethod
    def _session(transport, target: ProbeTarget) -> ModbusSession:
        return ModbusSession(
            transport,
            route=target.link_route,
            slave_id=target.payload_address,
        )


def _decode_ascii_words(registers: list[int]) -> str:
    chars: list[str] = []
    for value in registers:
        for byte in ((value >> 8) & 0xFF, value & 0xFF):
            if byte in (0x00, 0xFF):
                continue
            char = chr(byte)
            if char.isalnum() or char in " -_/.":
                chars.append(char)
    return "".join(chars)


def _is_meaningful_optional_ascii_text(text: str) -> bool:
    normalized = "".join(char for char in text if char not in " -_/.")
    if not normalized:
        return False
    return any(char != "0" for char in normalized)


def _specs_for_block(
    specs: tuple[RegisterValueSpec, ...],
    start_register: int,
    register_count: int,
) -> tuple[RegisterValueSpec, ...]:
    block_end = start_register + register_count
    return tuple(
        spec
        for spec in specs
        if start_register <= spec.register and (spec.register + spec.word_count) <= block_end
    )


async def _read_out_of_block_capability_registers(
    session: ModbusSession,
    capabilities,
    polled_blocks: tuple[tuple[int, list[int]], ...],
    *,
    already_read: frozenset[int] = frozenset(),
) -> tuple[tuple[int, list[int]], ...]:
    """Read writable-capability registers that fall OUTSIDE the polled blocks.

    Most controls live in the status/live/config blocks the driver already reads, but a few
    learned-overlay controls (e.g. Boot method 406, Output control 420) do not. Read those single
    registers directly so their selects/numbers report current state too. De-duplicated and
    bounded (at most 16 extra reads per refresh); ``action`` controls have no readable state and
    are skipped; individual read failures are skipped silently.

    ``already_read`` lists registers the caller fetched through other paths (the
    aux_config / optional-spec reads). Excluding them stops the 16-read budget
    from being spent re-reading registers already in hand -- which on variants
    with many aux registers (anenji_anj_11kw: 677-693) would otherwise crowd out
    the genuinely out-of-block controls this helper exists to read.
    """

    polled: set[int] = set(already_read)
    for start, block in polled_blocks:
        polled.update(range(start, start + len(block)))

    needed: set[int] = set()
    for capability in capabilities:
        if str(getattr(capability, "value_kind", "") or "") == "action":
            continue
        register = int(getattr(capability, "register", 0) or 0)
        if register <= 0:
            continue
        for offset in range(max(int(getattr(capability, "word_count", 1) or 1), 1)):
            if register + offset not in polled:
                needed.add(register + offset)

    extra: list[tuple[int, list[int]]] = []
    for register in sorted(needed)[:16]:
        try:
            raw = await session.read_holding(register, 1)
        except ModbusError:
            continue
        if raw:
            extra.append((register, [int(raw[0])]))
    return tuple(extra)


def _apply_capability_read_back(
    values: dict[str, Any],
    capabilities,
    register_blocks: tuple[tuple[int, list[int]], ...],
) -> None:
    """Fill ``values`` for writable capabilities that have a register but no decode spec.

    Built-in controls are decoded from the schema spec-sets, so their value is already
    present. Learned-overlay controls carry a register but no schema spec, so without this they
    would toggle yet report no current state (``is_on`` reads an absent ``value_key``). We read
    the raw register straight from the blocks the driver already polled -- crucially WITHOUT
    changing the inverter's ``register_schema_name`` (which would flip the metadata scope and
    break write-exposure for every control). Registers outside every polled block are skipped.
    """

    register_map: dict[int, int] = {}
    for start, block in register_blocks:
        for index, raw in enumerate(block):
            register_map[start + index] = raw

    for capability in capabilities:
        value_key = getattr(capability, "value_key", "") or getattr(capability, "key", "")
        if not value_key or value_key in values:
            continue
        register = int(getattr(capability, "register", 0) or 0)
        if register <= 0 or register not in register_map:
            continue
        word_count = int(getattr(capability, "word_count", 1) or 1)
        if word_count >= 2:
            high = register_map.get(register)
            low = register_map.get(register + 1)
            if high is None or low is None:
                continue
            if str(getattr(capability, "combine", "") or "") == "u32_high_first":
                raw_value = (high << 16) | low
            else:
                raw_value = (low << 16) | high
        else:
            raw_value = register_map[register]
        bitmask = int(getattr(capability, "bitmask", 0) or 0)
        if bitmask:
            # Masked capability: only its own bits carry the value (the rest of
            # the register belongs to other settings).
            shift = (bitmask & -bitmask).bit_length() - 1
            raw_value = (raw_value & bitmask) >> shift
        value_kind = str(getattr(capability, "value_kind", "") or "")
        divisor = int(getattr(capability, "divisor", 0) or 0)
        if value_kind == "enum":
            # A select reads the decoded LABEL (a string), like built-in enums. Map the raw
            # register value to its label via the capability's enum map; without this the select
            # gets a bare int and shows "unknown".
            enum_map = getattr(capability, "enum_value_map", None) or {}
            values[value_key] = enum_map.get(raw_value, f"Unknown ({raw_value})")
        elif divisor > 1:
            # Scaled number: store the NATIVE (display) value, since the number entity reads
            # value_key as-is and its native_min/max + the write encode use the same divisor.
            values[value_key] = round(raw_value / divisor, decimals_for_divisor(divisor))
        else:
            values[value_key] = raw_value


def _decode_block(
    start_register: int,
    values: list[int],
    specs: tuple[RegisterValueSpec, ...],
) -> dict[str, Any]:
    """Decode one SMG block via the shared decoder (all-ones sentinel on).

    Kept as a module symbol: tests exercise the sentinel semantics through
    this name. Behavioral equivalence with the historical private copy was
    verified against the schema corpus: every multi-word SMG spec is
    u32_high_first/unsigned, and no SMG spec uses multiplier or ascii
    combines.
    """

    return shared_decode_block(
        start_register,
        [int(value) for value in values],
        specs,
        all_ones_unavailable=True,
    )


def _group_optional_specs(
    specs: tuple[RegisterValueSpec, ...],
) -> tuple[tuple[int, int, tuple[RegisterValueSpec, ...]], ...]:
    grouped: list[tuple[int, int, tuple[RegisterValueSpec, ...]]] = []
    current_specs: list[RegisterValueSpec] = []
    current_start = 0
    current_end = -1

    for spec in sorted(specs, key=lambda item: (item.register, item.word_count, item.key)):
        spec_end = spec.register + spec.word_count - 1
        if not current_specs or spec.register > current_end + 1:
            if current_specs:
                grouped.append(
                    (current_start, current_end - current_start + 1, tuple(current_specs))
                )
            current_specs = [spec]
            current_start = spec.register
            current_end = spec_end
            continue

        current_specs.append(spec)
        current_end = max(current_end, spec_end)

    if current_specs:
        grouped.append((current_start, current_end - current_start + 1, tuple(current_specs)))

    return tuple(grouped)


async def _read_optional_specs(
    session: ModbusSession,
    specs: tuple[RegisterValueSpec, ...],
) -> dict[str, Any]:
    decoded: dict[str, Any] = {}

    for start_register, register_count, grouped_specs in _group_optional_specs(specs):
        try:
            raw_values = await session.read_holding(start_register, register_count)
        except Exception as exc:
            if not _is_optional_spec_error(exc):
                raise
            for spec in grouped_specs:
                try:
                    raw_values = await session.read_holding(spec.register, spec.word_count)
                except Exception as fallback_exc:
                    if not _is_optional_spec_error(fallback_exc):
                        raise
                    continue
                decoded.update(_decode_block(spec.register, raw_values, (spec,)))
            continue

        decoded.update(_decode_block(start_register, raw_values, grouped_specs))

    return decoded


async def _read_optional_ascii_ranges(
    session: ModbusSession,
    ranges: tuple[tuple[str, int, int], ...],
) -> dict[str, str]:
    decoded: dict[str, str] = {}
    for key, register, word_count in ranges:
        try:
            raw_values = await session.read_holding(register, word_count)
        except Exception as exc:
            if not _is_optional_spec_error(exc):
                raise
            continue
        text = _decode_ascii_words(raw_values).strip()
        if _is_meaningful_optional_ascii_text(text):
            decoded[key] = text
    return decoded


async def _read_missing_optional_probe_details(
    session: ModbusSession,
    inverter: DetectedInverter,
) -> dict[str, Any]:
    probe_policy = _runtime_probe_policy_for_inverter(inverter)
    missing_specs = tuple(
        spec
        for spec in _optional_probe_specs_for_policy(probe_policy)
        if spec.key not in inverter.details
    )
    missing_ascii_ranges = tuple(
        item
        for item in _optional_ascii_probe_ranges_for_policy(probe_policy)
        if item[0] not in inverter.details
    )
    if not missing_specs and not missing_ascii_ranges:
        return {}

    decoded: dict[str, Any] = {}
    decoded.update(await _read_optional_specs(session, missing_specs))
    decoded.update(await _read_optional_ascii_ranges(session, missing_ascii_ranges))
    return decoded


def _is_optional_spec_error(exc: Exception) -> bool:
    """Return whether one optional-register read failure should be ignored."""

    if isinstance(exc, ModbusError):
        return True
    return str(exc).startswith("missing_register:")


def _decode_named_bits(
    raw_value: Any,
    names_by_bit: dict[int, str],
    *,
    one_based: bool = False,
    include_unknown: bool = False,
    unknown_label_prefix: str = "Unknown Bit",
) -> tuple[str, ...]:
    if not isinstance(raw_value, int):
        return ()

    active: list[str] = []
    if raw_value <= 0:
        return ()

    for bit_index in range(raw_value.bit_length()):
        if not raw_value & (1 << bit_index):
            continue

        lookup_bit = bit_index + 1 if one_based else bit_index
        label = names_by_bit.get(lookup_bit)
        if label is not None:
            active.append(label)
            continue
        if include_unknown:
            active.append(f"{unknown_label_prefix} {lookup_bit}")
    return tuple(active)


def _decode_power_flow_status(raw_value: Any) -> dict[str, Any]:
    if not isinstance(raw_value, int) or raw_value < 0:
        return {}

    mains_charging = bool((raw_value >> 8) & 0x1)
    pv_charging = bool((raw_value >> 9) & 0x1)
    return {
        "power_flow_pv_connection_state": _power_flow_connection_state(raw_value & 0x3),
        "power_flow_utility_connection_state": _power_flow_connection_state((raw_value >> 2) & 0x3),
        "power_flow_battery_state": _power_flow_battery_state((raw_value >> 4) & 0x3),
        "power_flow_load_state": _power_flow_load_state((raw_value >> 6) & 0x3),
        "power_flow_charge_source_state": _power_flow_charge_source_state(
            mains_charging=mains_charging,
            pv_charging=pv_charging,
        ),
    }


def _power_flow_connection_state(value: int) -> str:
    if value == 0:
        return "Disconnected"
    if value == 1:
        return "Connected"
    return f"Unknown ({value})"


def _power_flow_battery_state(value: int) -> str:
    if value == 0:
        return "Idle"
    if value == 1:
        return "Charging"
    if value == 2:
        return "Discharging"
    return f"Unknown ({value})"


def _power_flow_load_state(value: int) -> str:
    if value == 0:
        return "Inactive"
    if value == 1:
        return "Active"
    return f"Unknown ({value})"


def _power_flow_charge_source_state(*, mains_charging: bool, pv_charging: bool) -> str:
    if mains_charging and pv_charging:
        return "PV + Utility"
    if mains_charging:
        return "Utility"
    if pv_charging:
        return "PV"
    return "Idle"


def _derive_runtime_states(values: dict[str, Any]) -> dict[str, Any]:
    """Derive human-readable runtime states from already decoded SMG values."""

    derived: dict[str, Any] = {}
    operating_mode = values.get("operating_mode")
    charge_source_priority = values.get("charge_source_priority")
    battery_connected = bool(values.get("battery_connected"))
    warning_descriptions = values.get("warning_descriptions")
    fault_descriptions = values.get("fault_descriptions")
    battery_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("battery",),
    )
    grid_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("mains", "grid"),
    )
    pv_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("pv",),
    )
    thermal_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("temperature", "fan"),
    )
    load_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("overload", "output short circuit", "output power derating"),
    )
    derived["configuration_safe_mode"] = values.get("operating_mode") in {
        "Power On",
        "Standby",
        "Fault",
    }
    derived["battery_equalization_enabled"] = values.get("battery_equalization_mode") == "On"
    utility_charging_allowed = values.get("charge_source_priority") in {
        "Utility Priority",
        "PV Priority",
        "PV and Utility",
        "PV Priority With Load Reserve",
    }
    derived["utility_charging_allowed"] = utility_charging_allowed
    derived["ac_charging_allowed"] = utility_charging_allowed
    derived["pv_only_charging"] = values.get("charge_source_priority") == "PV Only"
    derived["remote_control_enabled"] = values.get("turn_on_mode") in {
        "Local and Remote",
        "Remote Only",
    }
    derived["fault_active"] = bool(values.get("fault_count"))
    derived["warning_active"] = bool(values.get("warning_count"))
    derived["battery_protection_active"] = battery_alarm_active
    derived["grid_warning_active"] = grid_alarm_active
    derived["pv_warning_active"] = pv_alarm_active
    derived["thermal_warning_active"] = thermal_alarm_active
    derived["load_protection_active"] = load_alarm_active
    derived["grid_power_direction"] = _direction_from_power(
        values.get("grid_power"),
        positive_label="Importing",
        negative_label="Exporting",
    )
    derived["pv_producing"] = _is_active_power(values.get("pv_power"))
    derived["load_active"] = _is_active_power(values.get("output_power"))
    derived["battery_charging"] = _is_active_power(values.get("battery_average_power"))
    derived["battery_discharging"] = _is_active_power(values.get("battery_average_power"), negative=True)
    derived["battery_power_direction"] = _direction_from_power(
        values.get("battery_average_power"),
        positive_label="Charging",
        negative_label="Discharging",
    )
    derived["output_state"] = _active_state_from_power(
        values.get("output_power"),
        active_label="Supplying Load",
    )
    derived["pv_state"] = _pv_state(
        power=values.get("pv_power"),
        voltage=values.get("pv_voltage"),
    )
    derived["charging_source_state"] = _charging_source_state(
        pv_charging_power=values.get("pv_charging_power"),
        inverter_charging_power=values.get("inverter_charging_power"),
    )
    documented_power_flow = _decode_power_flow_status(values.get("power_flow_status"))
    if documented_power_flow:
        derived.update(documented_power_flow)
    derived["charging_active"] = bool(
        _is_active_power(values.get("pv_charging_power"))
        or _is_active_power(values.get("inverter_charging_power"))
    )
    derived["charging_inactive"] = not derived["charging_active"]
    documented_charge_source_state = derived.get("power_flow_charge_source_state")
    if (
        isinstance(documented_charge_source_state, str)
        and documented_charge_source_state not in {"Idle", "Unknown"}
        and derived["charging_source_state"] in {"Idle", "Unknown"}
    ):
        derived["charging_source_state"] = documented_charge_source_state
        derived["charging_active"] = True
        derived["charging_inactive"] = False
    derived["charge_source_policy_state"] = charge_source_priority or "Unknown"
    derived["charging_settings_state"] = _charging_settings_state(
        charging_active=derived["charging_active"],
        charge_source_priority=charge_source_priority,
        utility_charging_allowed=derived["utility_charging_allowed"],
    )
    derived["battery_settings_state"] = _battery_settings_state(
        battery_connected=battery_connected,
        charging_active=derived["charging_active"],
        battery_equalization_enabled=derived["battery_equalization_enabled"],
    )
    derived["output_settings_state"] = _output_settings_state(
        configuration_safe_mode=derived["configuration_safe_mode"],
        operating_mode=operating_mode,
    )
    derived["operational_state"] = _operational_state(
        operating_mode=operating_mode,
        output_state=derived["output_state"],
        warning_active=derived["warning_active"],
        fault_active=derived["fault_active"],
    )
    derived["protection_state"] = _protection_state(
        warning_descriptions=warning_descriptions,
        fault_descriptions=fault_descriptions,
        warning_active=derived["warning_active"],
        fault_active=derived["fault_active"],
        battery_alarm_active=battery_alarm_active,
        grid_alarm_active=grid_alarm_active,
        pv_alarm_active=pv_alarm_active,
        thermal_alarm_active=thermal_alarm_active,
        load_alarm_active=load_alarm_active,
    )
    derived["alarm_context_state"] = _alarm_context_state(
        warning_active=derived["warning_active"],
        fault_active=derived["fault_active"],
        battery_alarm_active=battery_alarm_active,
        grid_alarm_active=grid_alarm_active,
        pv_alarm_active=pv_alarm_active,
        thermal_alarm_active=thermal_alarm_active,
        load_alarm_active=load_alarm_active,
    )
    derived["grid_assist_state"] = _grid_assist_state(
        operating_mode=operating_mode,
        grid_power_direction=derived["grid_power_direction"],
        charging_source_state=derived["charging_source_state"],
    )
    derived["load_supply_state"] = _load_supply_state(
        operating_mode=operating_mode,
        output_state=derived["output_state"],
        grid_power_direction=derived["grid_power_direction"],
        battery_discharging=derived["battery_discharging"],
        pv_producing=derived["pv_producing"],
    )
    derived["battery_role_state"] = _battery_role_state(
        battery_connected=battery_connected,
        battery_charging=derived["battery_charging"],
        battery_discharging=derived["battery_discharging"],
        charging_source_state=derived["charging_source_state"],
        load_active=derived["load_active"],
    )
    derived["pv_role_state"] = _pv_role_state(
        pv_state=derived["pv_state"],
        pv_producing=derived["pv_producing"],
        charging_source_state=derived["charging_source_state"],
        load_supply_state=derived["load_supply_state"],
    )
    derived["utility_role_state"] = _utility_role_state(
        operating_mode=operating_mode,
        grid_power_direction=derived["grid_power_direction"],
        charging_source_state=derived["charging_source_state"],
    )
    derived["site_mode_state"] = _site_mode_state(
        operating_mode=operating_mode,
        load_supply_state=derived["load_supply_state"],
        pv_role_state=derived["pv_role_state"],
        utility_role_state=derived["utility_role_state"],
        warning_active=derived["warning_active"],
        fault_active=derived["fault_active"],
    )
    derived["power_flow_summary"] = _power_flow_summary(
        load_supply_state=derived["load_supply_state"],
        battery_role_state=derived["battery_role_state"],
        pv_role_state=derived["pv_role_state"],
        utility_role_state=derived["utility_role_state"],
    )
    return derived


def _derive_inverter_clock(values: dict[str, Any]) -> dict[str, Any]:
    """Derive human-readable inverter date/time strings from optional clock registers."""

    raw_parts = {
        "year": values.pop("inverter_clock_year", None),
        "month": values.pop("inverter_clock_month", None),
        "day": values.pop("inverter_clock_day", None),
        "hour": values.pop("inverter_clock_hour", None),
        "minute": values.pop("inverter_clock_minute", None),
        "second": values.pop("inverter_clock_second", None),
    }
    if not all(isinstance(part, int) for part in raw_parts.values()):
        return {}

    year = raw_parts["year"]
    month = raw_parts["month"]
    day = raw_parts["day"]
    hour = raw_parts["hour"]
    minute = raw_parts["minute"]
    second = raw_parts["second"]

    if not (2000 <= year <= 2100):
        return {}

    derived: dict[str, Any] = {}
    try:
        datetime(year, month, day)
    except ValueError:
        pass
    else:
        derived["inverter_date"] = f"{year:04d}-{month:02d}-{day:02d}"

    if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
        derived["inverter_time"] = f"{hour:02d}:{minute:02d}:{second:02d}"

    return derived


def _derive_pv_channel_summary(values: dict[str, Any]) -> dict[str, Any]:
    """Backfill aggregate PV values when one variant only exposes per-string channels."""

    derived: dict[str, Any] = {}

    pv_power = values.get("pv_power")
    if not isinstance(pv_power, (int, float)) or pv_power <= 0:
        powers = [
            power
            for power in (values.get("pv1_power"), values.get("pv2_power"))
            if isinstance(power, (int, float))
        ]
        if powers:
            derived["pv_power"] = int(round(sum(powers)))

    pv_voltage = values.get("pv_voltage")
    if not isinstance(pv_voltage, (int, float)) or pv_voltage <= 0:
        voltages = [
            voltage
            for voltage in (values.get("pv1_voltage"), values.get("pv2_voltage"))
            if isinstance(voltage, (int, float))
        ]
        if voltages:
            active_voltages = [voltage for voltage in voltages if voltage > 0]
            derived["pv_voltage"] = round(max(active_voltages or voltages), 1)

    pv_current = values.get("pv_current")
    if not isinstance(pv_current, (int, float)) or pv_current <= 0:
        currents = [
            current
            for current in (values.get("pv1_current"), values.get("pv2_current"))
            if isinstance(current, (int, float))
        ]
        if currents:
            derived["pv_current"] = round(sum(currents), 1)

    return derived


def _is_active_power(
    raw_value: Any,
    *,
    threshold: int = 20,
    negative: bool = False,
) -> bool:
    """Return whether one power reading indicates active flow."""

    if not isinstance(raw_value, (int, float)):
        return False
    if negative:
        return raw_value <= -threshold
    return raw_value >= threshold


def _direction_from_power(
    raw_value: Any,
    *,
    positive_label: str,
    negative_label: str,
    idle_label: str = "Idle",
    threshold: int = 20,
) -> str:
    """Return a direction label for a signed power value."""

    if not isinstance(raw_value, (int, float)):
        return "Unknown"
    if raw_value >= threshold:
        return positive_label
    if raw_value <= -threshold:
        return negative_label
    return idle_label


def _active_state_from_power(
    raw_value: Any,
    *,
    active_label: str,
    idle_label: str = "Idle",
    threshold: int = 20,
) -> str:
    """Return an activity label for one non-negative power reading."""

    if not isinstance(raw_value, (int, float)):
        return "Unknown"
    if raw_value >= threshold:
        return active_label
    return idle_label


def _pv_state(
    *,
    power: Any,
    voltage: Any,
) -> str:
    """Return a coarse PV state based on live SMG PV readings."""

    if isinstance(power, (int, float)) and power >= 20:
        return "Producing"
    if isinstance(voltage, (int, float)) and voltage >= 15:
        return "Available"
    if isinstance(voltage, (int, float)):
        return "Inactive"
    return "Unknown"


def _charging_source_state(
    *,
    pv_charging_power: Any,
    inverter_charging_power: Any,
    threshold: int = 20,
) -> str:
    """Return a human-readable charging source summary."""

    pv_active = isinstance(pv_charging_power, (int, float)) and pv_charging_power >= threshold
    inverter_active = (
        isinstance(inverter_charging_power, (int, float))
        and inverter_charging_power >= threshold
    )
    if pv_active and inverter_active:
        return "PV + Utility"
    if pv_active:
        return "PV"
    if inverter_active:
        return "Utility"
    if pv_charging_power is None and inverter_charging_power is None:
        return "Unknown"
    return "Idle"


def _charging_settings_state(
    *,
    charging_active: bool,
    charge_source_priority: Any,
    utility_charging_allowed: bool,
) -> str:
    """Summarize whether charging-related settings are currently practical to edit."""

    if charging_active:
        return "Locked While Charging"
    if charge_source_priority == "PV Only":
        return "PV-Only Policy Active"
    if utility_charging_allowed:
        return "Utility Charging Allowed"
    return "Idle"


def _battery_settings_state(
    *,
    battery_connected: bool,
    charging_active: bool,
    battery_equalization_enabled: bool,
) -> str:
    """Summarize the current editing context for battery settings."""

    if not battery_connected:
        return "Battery Disconnected"
    if charging_active:
        return "Locked While Charging"
    if not battery_equalization_enabled:
        return "Equalization Disabled"
    return "Editable"


def _output_settings_state(
    *,
    configuration_safe_mode: bool,
    operating_mode: Any,
) -> str:
    """Summarize whether output/system configuration changes are currently safe."""

    if configuration_safe_mode:
        return "Editable"
    if isinstance(operating_mode, str) and operating_mode:
        return f"Locked In {operating_mode}"
    return "Safe Mode Required"


def _operational_state(
    *,
    operating_mode: Any,
    output_state: Any,
    warning_active: bool,
    fault_active: bool,
) -> str:
    """Summarize the inverter's overall runtime state."""

    if fault_active:
        return "Fault"
    if warning_active:
        if operating_mode in {"Mains", "Off-Grid", "Bypass", "Charging"}:
            return "Running with Warnings"
        if isinstance(operating_mode, str) and operating_mode:
            return f"{operating_mode} with Warnings"
        return "Warnings Active"
    if output_state == "Supplying Load":
        return "Supplying Load"
    if isinstance(operating_mode, str) and operating_mode:
        return operating_mode
    return "Unknown"


def _protection_state(
    *,
    warning_descriptions: Any,
    fault_descriptions: Any,
    warning_active: bool,
    fault_active: bool,
    battery_alarm_active: bool,
    grid_alarm_active: bool,
    pv_alarm_active: bool,
    thermal_alarm_active: bool,
    load_alarm_active: bool,
) -> str:
    """Summarize alarm/protection context from decoded warning and fault states."""

    warnings_text = warning_descriptions if isinstance(warning_descriptions, str) else ""
    faults_text = fault_descriptions if isinstance(fault_descriptions, str) else ""
    if fault_active:
        if battery_alarm_active or "Battery" in faults_text:
            return "Battery Fault Protection"
        if pv_alarm_active:
            return "PV Fault Protection"
        if thermal_alarm_active:
            return "Thermal Fault Protection"
        if load_alarm_active:
            return "Load Fault Protection"
        return "Fault Protection Active"
    if battery_alarm_active or "Battery Low Voltage" in warnings_text or "Battery Discharged Below Recovery Point" in warnings_text:
        return "Battery Protection Active"
    if thermal_alarm_active or "Over Temperature" in warnings_text:
        return "Thermal Warning"
    if load_alarm_active or "Overload" in warnings_text or "Output Power Derating" in warnings_text:
        return "Load Protection Active"
    if pv_alarm_active:
        return "PV Input Warning"
    if grid_alarm_active:
        return "Grid Warning"
    if warning_active:
        return "Warning Active"
    return "Normal"


def _alarm_context_state(
    *,
    warning_active: bool,
    fault_active: bool,
    battery_alarm_active: bool,
    grid_alarm_active: bool,
    pv_alarm_active: bool,
    thermal_alarm_active: bool,
    load_alarm_active: bool,
) -> str:
    """Return one compact alarm category label for automation/UI use."""

    prefix = "Fault" if fault_active else "Warning" if warning_active else ""
    if not prefix:
        return "Normal"
    if battery_alarm_active:
        return f"Battery {prefix}"
    if grid_alarm_active:
        return f"Grid {prefix}"
    if pv_alarm_active:
        return f"PV {prefix}"
    if thermal_alarm_active:
        return f"Thermal {prefix}"
    if load_alarm_active:
        return f"Load {prefix}"
    return prefix


def _grid_assist_state(
    *,
    operating_mode: Any,
    grid_power_direction: Any,
    charging_source_state: Any,
) -> str:
    """Summarize the current role of the grid/utility path."""

    if operating_mode == "Bypass":
        return "Bypass Active"
    if charging_source_state in {"Utility", "PV + Utility"}:
        return "Utility Charging Active"
    if grid_power_direction == "Importing" or operating_mode == "Mains":
        return "Grid Assisting Load"
    if grid_power_direction == "Exporting":
        return "Exporting To Grid"
    if operating_mode == "Off-Grid":
        return "Grid Independent"
    if operating_mode == "Standby":
        return "Standby"
    return "Idle"


def _load_supply_state(
    *,
    operating_mode: Any,
    output_state: Any,
    grid_power_direction: Any,
    battery_discharging: bool,
    pv_producing: bool,
) -> str:
    """Summarize which sources appear to be feeding the load."""

    if output_state != "Supplying Load":
        return "Idle"
    if operating_mode == "Bypass":
        return "Utility Bypass"
    if operating_mode == "Mains":
        if pv_producing and battery_discharging:
            return "Utility + PV + Battery"
        if pv_producing:
            return "Utility + PV"
        if battery_discharging:
            return "Utility + Battery"
        return "Utility"
    if operating_mode == "Off-Grid":
        if pv_producing and battery_discharging:
            return "PV + Battery"
        if pv_producing:
            return "PV"
        if battery_discharging:
            return "Battery"
        return "Inverter"
    if grid_power_direction == "Importing":
        return "Utility"
    if pv_producing and battery_discharging:
        return "PV + Battery"
    if pv_producing:
        return "PV"
    if battery_discharging:
        return "Battery"
    return "Unknown"


def _battery_role_state(
    *,
    battery_connected: bool,
    battery_charging: bool,
    battery_discharging: bool,
    charging_source_state: Any,
    load_active: bool,
) -> str:
    """Summarize the current role of the battery pack."""

    if not battery_connected:
        return "Disconnected"
    if battery_charging:
        if charging_source_state == "PV":
            return "Charging from PV"
        if charging_source_state == "Utility":
            return "Charging from Utility"
        if charging_source_state == "PV + Utility":
            return "Charging from PV + Utility"
        return "Charging"
    if battery_discharging:
        if load_active:
            return "Supplying Load"
        return "Discharging"
    return "Idle"


def _pv_role_state(
    *,
    pv_state: Any,
    pv_producing: bool,
    charging_source_state: Any,
    load_supply_state: Any,
) -> str:
    """Summarize the current role of the PV input."""

    if pv_producing:
        supplies_load = isinstance(load_supply_state, str) and "PV" in load_supply_state
        if charging_source_state == "PV + Utility":
            return "Charging Battery with Utility Assist"
        if charging_source_state == "PV":
            if supplies_load:
                return "Supplying Load + Charging Battery"
            return "Charging Battery"
        if supplies_load:
            return "Supplying Load"
        return "Producing"
    if pv_state in {"Available", "Inactive"}:
        return pv_state
    return "Unknown"


def _utility_role_state(
    *,
    operating_mode: Any,
    grid_power_direction: Any,
    charging_source_state: Any,
) -> str:
    """Summarize the current role of the utility/grid path."""

    if operating_mode == "Bypass":
        return "Bypass Active"
    if charging_source_state in {"Utility", "PV + Utility"}:
        if grid_power_direction == "Importing":
            return "Supplying Load + Charging Battery"
        return "Charging Battery"
    if grid_power_direction == "Importing" or operating_mode == "Mains":
        return "Supplying Load"
    if grid_power_direction == "Exporting":
        return "Exporting"
    if operating_mode == "Off-Grid":
        return "Standby"
    if operating_mode == "Standby":
        return "Standby"
    return "Idle"


def _site_mode_state(
    *,
    operating_mode: Any,
    load_supply_state: Any,
    pv_role_state: Any,
    utility_role_state: Any,
    warning_active: bool,
    fault_active: bool,
) -> str:
    """Return one concise site-wide operating mode summary."""

    if fault_active:
        return "Fault"
    if operating_mode == "Bypass":
        return "Utility Bypass"
    if operating_mode == "Off-Grid":
        summary = "Off-Grid"
        if isinstance(load_supply_state, str) and load_supply_state not in {"Idle", "Unknown"}:
            summary += f" on {load_supply_state}"
        if pv_role_state == "Available":
            summary += ", PV Available"
        if warning_active:
            summary += " with Warnings"
        return summary
    if operating_mode == "Mains":
        summary = "Grid-Connected"
        if isinstance(load_supply_state, str) and load_supply_state not in {"Idle", "Unknown"}:
            summary += f" via {load_supply_state}"
        if utility_role_state == "Charging Battery":
            summary += ", Utility Charging"
        if warning_active:
            summary += " with Warnings"
        return summary
    if operating_mode == "Standby":
        return "Standby"
    if isinstance(operating_mode, str) and operating_mode:
        if warning_active:
            return f"{operating_mode} with Warnings"
        return operating_mode
    return "Unknown"


def _power_flow_summary(
    *,
    load_supply_state: Any,
    battery_role_state: Any,
    pv_role_state: Any,
    utility_role_state: Any,
) -> str:
    """Return one compact cross-source power-flow summary."""

    parts: list[str] = []
    if isinstance(load_supply_state, str) and load_supply_state not in {"Idle", "Unknown"}:
        parts.append(f"Load: {load_supply_state}")
    if isinstance(battery_role_state, str) and battery_role_state not in {"Idle", "Unknown", "Disconnected"}:
        parts.append(f"Battery: {battery_role_state}")
    if isinstance(pv_role_state, str) and pv_role_state not in {"Idle", "Unknown", "Inactive"}:
        parts.append(f"PV: {pv_role_state}")
    if isinstance(utility_role_state, str) and utility_role_state not in {"Idle", "Standby", "Unknown"}:
        parts.append(f"Utility: {utility_role_state}")
    if not parts:
        return "Idle"
    return " | ".join(parts)


def _alarm_matches(
    warning_descriptions: Any,
    fault_descriptions: Any,
    keywords: tuple[str, ...],
) -> bool:
    """Return whether any alarm text contains one of the given keywords."""

    haystacks = []
    if isinstance(warning_descriptions, str) and warning_descriptions != "None":
        haystacks.append(warning_descriptions.lower())
    if isinstance(fault_descriptions, str) and fault_descriptions != "None":
        haystacks.append(fault_descriptions.lower())
    if not haystacks:
        return False
    return any(keyword in haystack for keyword in keywords for haystack in haystacks)


# Capability value encoding/decoding lives in capability_codec (shared with
# the generic catalog driver); the aliases keep this module's call sites.


def _support_capture_notes(schema_name: str | None = None) -> tuple[str, ...]:
    return _support_capture_policy(schema_name).notes


def _support_capture_ranges(schema_name: str | None = None) -> tuple[tuple[int, int], ...]:
    default_binding = _smg_default_binding()
    schema = load_register_schema(schema_name or default_binding.register_schema_name)
    planned: list[tuple[int, int]] = [
        (schema.block("status").start, schema.block("status").count),
        (schema.block("serial").start, schema.block("serial").count),
        (schema.block("live").start, schema.block("live").count),
        (schema.block("config").start, schema.block("config").count),
    ]
    planned.extend(
        (spec.register, spec.word_count)
        for spec in schema.spec_set("aux_config")
    )
    planned.extend(
        (register, 1)
        for register in sorted({value for value in schema.scalar_registers.values() if value > 0})
    )
    planned.extend(_support_capture_policy(schema_name).ranges)
    return _merge_capture_ranges(planned)


def _support_capture_policy(
    schema_name: str | None = None,
) -> SupportCapturePolicy:
    default_binding = _smg_default_binding()
    resolved_schema_name = schema_name or default_binding.register_schema_name
    policy = resolve_support_capture_policy(
        driver_key="modbus_smg",
        register_schema_name=resolved_schema_name,
    )
    if policy.ranges or policy.notes:
        return policy
    return resolve_support_capture_policy(
        driver_key=default_binding.driver_key,
        variant_key=default_binding.variant_key,
        profile_name=default_binding.profile_name,
        register_schema_name=default_binding.register_schema_name,
    )


def _schema_for_inverter(
    inverter: DetectedInverter | None,
    fallback_schema_name: str,
):
    schema_name = fallback_schema_name
    if inverter is not None and inverter.register_schema_name:
        schema_name = inverter.register_schema_name
    if not schema_name:
        return None
    return load_register_schema(schema_name)


def _runtime_probe_policy_for_binding(binding) -> RuntimeProbePolicy:
    return resolve_runtime_probe_policy(
        driver_key=str(getattr(binding, "driver_key", "modbus_smg") or "modbus_smg"),
        variant_key=str(getattr(binding, "variant_key", "") or ""),
        profile_name=str(getattr(binding, "profile_name", "") or ""),
        register_schema_name=str(getattr(binding, "register_schema_name", "") or ""),
    )


def _runtime_probe_policy_for_resolution(
    candidate_keys: tuple[str, ...],
    surface_key: str,
    binding,
) -> RuntimeProbePolicy:
    source = load_device_catalog()
    if len(candidate_keys) == 1:
        entry = next(
            (item for item in source.devices if item.entry_key == candidate_keys[0]),
            None,
        )
        if entry is not None:
            return entry.runtime_probe
    family_default = next(
        (item for item in source.family_defaults if item.surface_key == surface_key),
        None,
    )
    if family_default is not None:
        return family_default.runtime_probe
    return _runtime_probe_policy_for_binding(binding)


def _runtime_probe_policy_for_inverter(inverter: DetectedInverter) -> RuntimeProbePolicy:
    return resolve_runtime_probe_policy(
        driver_key=inverter.driver_key,
        variant_key=inverter.variant_key,
        profile_name=inverter.profile_name,
        register_schema_name=inverter.register_schema_name,
    )


def _optional_probe_specs_for_policy(
    policy: RuntimeProbePolicy,
) -> tuple[RegisterValueSpec, ...]:
    return tuple(
        RegisterValueSpec(
            key=item.key,
            register=item.register,
            word_count=item.word_count,
            signed=item.signed,
            combine=item.combine,
            divisor=item.divisor,
        )
        for item in policy.optional_registers
    )


def _optional_ascii_probe_ranges_for_policy(
    policy: RuntimeProbePolicy,
) -> tuple[tuple[str, int, int], ...]:
    return tuple(
        (item.key, item.register, item.word_count)
        for item in policy.optional_ascii
    )


def _is_valid_smg_probe(
    config_values: dict[str, Any],
    policy: RuntimeProbePolicy,
    *,
    rated_power: int = 0,
) -> bool:
    values = dict(config_values)
    values["rated_power"] = rated_power
    for rule in policy.validation:
        value = values.get(rule.key)
        if rule.equals is not None and value != rule.equals:
            return False
        if rule.one_of and value not in rule.one_of:
            return False
        if rule.known_enum and not _is_known_enum_value(value):
            return False
        if rule.min_value is not None:
            if not isinstance(value, (int, float)) or value < rule.min_value:
                return False
        if rule.max_value is not None:
            if not isinstance(value, (int, float)) or value > rule.max_value:
                return False
    return True


def _is_known_enum_value(value: Any) -> bool:
    return isinstance(value, str) and not value.startswith("Unknown")


async def _read_rated_power(session: ModbusSession, schema) -> int:
    rated_power_register = schema.scalar_registers.get("rated_power_register", 0)
    if rated_power_register <= 0:
        return 0
    try:
        return (await session.read_holding(rated_power_register, 1))[0]
    except ModbusError:
        return 0


def _smg_model_name(catalog_model_name: str, rated_power: int) -> str:
    if catalog_model_name:
        return catalog_model_name
    return f"SMG {rated_power}" if rated_power else "SMG"


def _smg_default_binding():
    binding = resolve_catalog_surface_binding("modbus_smg")
    if binding is None:
        raise RuntimeError("missing_default_surface:modbus_smg")
    return binding


def _merge_capture_ranges(
    ranges: list[tuple[int, int]] | tuple[tuple[int, int], ...],
) -> tuple[tuple[int, int], ...]:
    normalized = sorted(
        (
            (int(start), int(count))
            for start, count in ranges
            if count > 0
        ),
        key=lambda item: item[0],
    )
    if not normalized:
        return ()

    merged: list[tuple[int, int]] = []
    current_start, current_count = normalized[0]
    current_end = current_start + current_count

    for start, count in normalized[1:]:
        end = start + count
        if start <= current_end:
            current_end = max(current_end, end)
            current_count = current_end - current_start
            continue
        merged.append((current_start, current_count))
        current_start = start
        current_count = count
        current_end = end

    merged.append((current_start, current_count))
    return tuple(merged)


def _format_support_range(start: int, values: list[int]) -> dict[str, Any]:
    entries = []
    for offset, value in enumerate(values):
        entries.append(
            {
                "register": start + offset,
                "u16": value,
                "s16": to_signed_16(value),
                "hex": f"0x{value:04X}",
            }
        )
    return {
        "start": start,
        "count": len(values),
        "ascii": _decode_ascii_words(values),
        "words": list(values),
        "values": entries,
    }
