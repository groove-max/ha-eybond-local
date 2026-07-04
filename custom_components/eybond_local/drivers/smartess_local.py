"""SmartESS local Modbus runtime driver."""

from __future__ import annotations

import time
import logging
from typing import Any

from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from ..models import (
    DetectedInverter,
    ProbeTarget,
    RegisterValueSpec,
    WriteCapability,
    decimals_for_divisor,
)
from ..payload.modbus import ModbusSession, to_signed_16
from ..payload.register_decode import decode_block as shared_decode_block
from .base import InverterDriver
from .command_support import (
    apply_unsupported_diagnostics,
    command_skipped_as_unsupported,
    commit_cycle_failures,
    record_command_failure,
    record_command_success,
)


SMARTESS_LOCAL_0925_PROFILE_NAME = "smartess_local/models/0925.json"
SMARTESS_LOCAL_0925_SCHEMA_NAME = "smartess_local/models/0925.json"
SMARTESS_LOCAL_0925_VARIANT = "smartess_0925"

_SMARTESS_0925_PROBE_TARGETS: tuple[ProbeTarget, ...] = (
    ProbeTarget(devcode=0x0001, collector_addr=0x05, device_addr=0x05),
    ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x05),
    ProbeTarget(devcode=0x0005, collector_addr=0x05, device_addr=0x05),
)

_FAST_BLOCKS = frozenset({"live"})
_MEDIUM_BLOCKS = frozenset({"config_state"})
_SLOW_BLOCKS = frozenset({"rating", "status", "serial", "energy", "config", "time_settings"})
_SMARTESS_0925_DATA_COLLECTOR_ADDR = 0xFF
_SMARTESS_0925_CONFIG_COLLECTOR_ADDR = 0x05
_SMARTESS_0925_CONFIG_BLOCKS = frozenset({"config", "time_settings"})
_SMARTESS_0925_CONFIG_STATE_REGISTER_RANGE = range(4535, 4553)
_LOGGER = logging.getLogger(__name__)


class SmartEssLocalDriver(InverterDriver):
    """Runtime driver for SmartESS 0925 Modbus RTU over EyeBond binary tunnel."""

    key = "smartess_local"
    name = "SmartESS Local / Modbus"
    profile_name = SMARTESS_LOCAL_0925_PROFILE_NAME
    register_schema_name = SMARTESS_LOCAL_0925_SCHEMA_NAME
    probe_timeout = 12.0
    signature_timeout = 4.0
    probe_targets = _SMARTESS_0925_PROBE_TARGETS

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
        if profile is None:
            return ()
        return profile.capabilities

    @property
    def capability_presets(self):
        return ()

    async def async_probe_signature(self, transport, target: ProbeTarget) -> bool:
        session = self._session(
            transport,
            _target_with_collector_addr(target, _SMARTESS_0925_CONFIG_COLLECTOR_ADDR),
        )
        try:
            raw = await session.read_holding(5004, 1)
        except Exception:
            return False
        return bool(raw) and int(raw[0]) in {0, 1}

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        schema = load_register_schema(self.register_schema_name)
        profile = load_driver_profile(self.profile_name)
        data_target = _target_with_collector_addr(
            target,
            _SMARTESS_0925_DATA_COLLECTOR_ADDR,
        )
        config_target = _target_with_collector_addr(
            target,
            _SMARTESS_0925_CONFIG_COLLECTOR_ADDR,
        )
        data_session = self._session(transport, data_target)
        config_session = self._session(transport, config_target)
        try:
            live_block = schema.block("live")
            live_words = _normalize_block_words(
                live_block.key,
                await data_session.read_holding(live_block.start, live_block.count),
            )
            lcd_backlight = await config_session.read_holding(5004, 1)
        except Exception as exc:
            _LOGGER.debug(
                "SmartESS 0925 probe failed target devcode=0x%04X base_addr=0x%02X "
                "data_addr=0x%02X config_addr=0x%02X slave=%d: %s",
                target.devcode,
                target.collector_addr,
                data_target.collector_addr,
                config_target.collector_addr,
                target.payload_address,
                exc,
            )
            return None

        if not _looks_like_smartess_0925(live_words, lcd_backlight):
            _LOGGER.debug(
                "SmartESS 0925 probe sanity rejected target devcode=0x%04X base_addr=0x%02X "
                "data_addr=0x%02X config_addr=0x%02X slave=%d live=%s lcd=%s",
                target.devcode,
                target.collector_addr,
                data_target.collector_addr,
                config_target.collector_addr,
                target.payload_address,
                list(live_words),
                list(lcd_backlight),
            )
            return None

        values = _decode_block(live_block.start, live_words, schema.spec_set("live"))
        try:
            rating_block = schema.block("rating")
            rating_words = _normalize_block_words(
                rating_block.key,
                await data_session.read_holding(rating_block.start, rating_block.count),
            )
            values.update(_decode_block(rating_block.start, rating_words, schema.spec_set("rating")))
        except Exception:
            pass
        values["lcd_backlight_enabled"] = bool(int(lcd_backlight[0]))
        values.update(
            {
                "protocol_id": "SMARTESS_0925_MODBUS",
                "smartess_protocol_asset_id": "0925",
                "smartess_profile_key": "smartess_0925",
                "smartess_device_address": target.payload_address,
                "smartess_data_collector_addr": data_target.collector_addr,
                "smartess_config_collector_addr": config_target.collector_addr,
                "catalog_detection": {
                    "resolution": "exact",
                    "surface_key": SMARTESS_LOCAL_0925_VARIANT,
                    "evidence": {
                        "protocol.protocol_id": "SMARTESS_0925_MODBUS",
                        "smartess.protocol_asset_id": "0925",
                        "smartess.device_address": target.payload_address,
                    },
                },
            }
        )

        return DetectedInverter(
            driver_key=self.key,
            protocol_family="smartess_local",
            model_name=_model_name(values),
            serial_number="",
            probe_target=target,
            variant_key=SMARTESS_LOCAL_0925_VARIANT,
            details=values,
            profile_name=self.profile_name,
            register_schema_name=self.register_schema_name,
            capability_groups=profile.groups,
            capabilities=self.write_capabilities,
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
        schema = load_register_schema(inverter.register_schema_name or self.register_schema_name)
        now = time.monotonic() if now_monotonic is None else float(now_monotonic)
        cache = _runtime_cache(runtime_state)
        values_cache: dict[str, Any] = cache.setdefault("values", {})
        register_cache: dict[int, int] = cache.setdefault("registers", {})
        last_read: dict[str, float] = cache.setdefault("last_read", {})

        request_timings: list[tuple[str, int, str]] = []

        async def _timed_read(session_obj, label: str, start: int, count: int):
            request_started = time.monotonic()
            try:
                result = await session_obj.read_holding(start, count)
            except Exception:
                request_timings.append(
                    (label, int(round((time.monotonic() - request_started) * 1000.0)), "error")
                )
                raise
            request_timings.append(
                (label, int(round((time.monotonic() - request_started) * 1000.0)), "ok")
            )
            return result

        read_blocks: list[tuple[int, list[int]]] = []
        for block in schema.blocks:
            if block.count <= 0:
                continue
            if not _is_block_due(
                block.key,
                last_read,
                now=now,
                poll_interval=poll_interval,
            ):
                continue
            block_cache_key = f"block:{block.key}"
            if command_skipped_as_unsupported(runtime_state, block_cache_key):
                # The bulk read is known-rejected for this device; keep the
                # cheap per-register capability fallbacks (they have their own
                # negative cache) instead of burning a timeout every cycle.
                await _read_capability_register_fallbacks(
                    self,
                    transport,
                    inverter.probe_target,
                    self.write_capabilities,
                    block_start=block.start,
                    block_count=block.count,
                    register_cache=register_cache,
                    runtime_state=runtime_state,
                )
                continue
            session = self._session(
                transport,
                _target_for_block(inverter.probe_target, block.key),
            )
            try:
                words = _normalize_block_words(
                    block.key,
                    await _timed_read(session, block_cache_key, block.start, block.count),
                )
            except Exception:
                record_command_failure(runtime_state, block_cache_key)
                await _read_capability_register_fallbacks(
                    self,
                    transport,
                    inverter.probe_target,
                    self.write_capabilities,
                    block_start=block.start,
                    block_count=block.count,
                    register_cache=register_cache,
                    runtime_state=runtime_state,
                )
                continue
            record_command_success(runtime_state, block_cache_key)
            last_read[block.key] = now
            read_blocks.append((block.start, words))
            for index, raw in enumerate(words):
                register_cache[block.start + index] = int(raw)
            values_cache.update(_decode_block(block.start, words, schema.spec_sets.get(block.key, ())))

        if read_blocks or "protocol_id" not in values_cache:
            values_cache.setdefault("protocol_id", "SMARTESS_0925_MODBUS")
            values_cache.setdefault("smartess_protocol_asset_id", "0925")
            values_cache.setdefault("smartess_profile_key", "smartess_0925")
            values_cache.setdefault("smartess_device_address", inverter.probe_target.payload_address)
            values_cache.setdefault("smartess_data_collector_addr", _SMARTESS_0925_DATA_COLLECTOR_ADDR)
            values_cache.setdefault("smartess_config_collector_addr", _SMARTESS_0925_CONFIG_COLLECTOR_ADDR)

        _apply_capability_read_back(values_cache, self.write_capabilities, register_cache)
        commit_cycle_failures(runtime_state)
        result = dict(values_cache)
        apply_unsupported_diagnostics(result, runtime_state)
        if request_timings:
            slowest = sorted(request_timings, key=lambda item: -item[1])[:5]
            result["driver_slow_requests"] = ", ".join(
                f"{label}={elapsed_ms}ms:{outcome}" for label, elapsed_ms, outcome in slowest
            )
            _LOGGER.debug(
                "SmartESS 0925 cycle requests: %s",
                ", ".join(
                    f"{label}={elapsed_ms}ms:{outcome}"
                    for label, elapsed_ms, outcome in request_timings
                ),
            )
        return result

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
    ) -> Any:
        capability = _find_capability(capability_key, self.write_capabilities)
        raw_value = _encode_capability_value(capability, value)
        session = self._session(
            transport,
            _target_for_register(inverter.probe_target, capability.register),
        )
        await session.write_single_holding(capability.register, raw_value)
        readback = await session.read_holding(capability.register, 1)
        readback = _normalize_register_words(capability.register, readback)
        if not readback:
            raise RuntimeError(f"missing_write_readback:{capability.key}")
        if int(readback[0]) != raw_value:
            raise RuntimeError(
                f"unexpected_write_readback:{capability.key}:{readback[0]}:expected={raw_value}"
            )
        decoded = _decode_capability_value(capability, [int(readback[0])])
        inverter.details[capability.value_key] = decoded
        if capability.key != capability.value_key:
            inverter.details[capability.key] = decoded
        return decoded

    async def async_capture_support_evidence(
        self,
        transport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        schema = load_register_schema(inverter.register_schema_name or self.register_schema_name)
        captured_ranges: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for block in schema.blocks:
            if block.count <= 0:
                continue
            if block.key in _SMARTESS_0925_CONFIG_BLOCKS:
                captured, failed = await _capture_individual_control_registers(
                    self,
                    transport,
                    inverter.probe_target,
                    self.write_capabilities,
                    block_start=block.start,
                    block_count=block.count,
                    block_key=block.key,
                )
                captured_ranges.extend(captured)
                failures.extend(failed)
                continue
            target = _target_for_block(inverter.probe_target, block.key)
            session = self._session(transport, target)
            try:
                values = _normalize_block_words(
                    block.key,
                    await session.read_holding(block.start, block.count),
                )
            except Exception as exc:
                failures.append(
                    {
                        "start": block.start,
                        "count": block.count,
                        "block": block.key,
                        "collector_addr": target.collector_addr,
                        "error": str(exc),
                    }
                )
                continue
            captured_ranges.append(
                {
                    "start": block.start,
                    "count": block.count,
                    "block": block.key,
                    "collector_addr": target.collector_addr,
                    "words": list(values),
                }
            )

        return {
            "capture_kind": "smartess_0925_modbus_register_dump",
            "driver_key": self.key,
            "model_name": inverter.model_name,
            "serial_number": inverter.serial_number,
            "probe_target": {
                "devcode": inverter.probe_target.devcode,
                "collector_addr": inverter.probe_target.collector_addr,
                "device_addr": inverter.probe_target.device_addr,
            },
            "route_policy": {
                "data_collector_addr": _SMARTESS_0925_DATA_COLLECTOR_ADDR,
                "config_collector_addr": _SMARTESS_0925_CONFIG_COLLECTOR_ADDR,
            },
            "capture_notes": [
                "SmartESS 0925 uses Modbus RTU frames routed through the legacy EyeBond binary tunnel.",
                "Write controls are exposed from the SmartESS 0925 profile for this runtime.",
                "Configuration-only 500x registers are captured individually because some 0925 devices reject wide config block reads.",
            ],
            "planned_ranges": [
                {
                    "start": block.start,
                    "count": block.count,
                    "block": block.key,
                    "read_strategy": (
                        "individual_non_action_control_registers"
                        if block.key in _SMARTESS_0925_CONFIG_BLOCKS
                        else "bulk_read_holding"
                    ),
                }
                for block in schema.blocks
                if block.count > 0
            ],
            "captured_ranges": captured_ranges,
            "range_failures": failures,
            "fixture_ranges": [
                {
                    "start": item["start"],
                    "count": item["count"],
                    "values": list(item["words"]),
                }
                for item in captured_ranges
            ],
        }

    @staticmethod
    def _session(transport, target: ProbeTarget) -> ModbusSession:
        return ModbusSession(
            transport,
            route=target.link_route,
            slave_id=target.payload_address,
        )


def _runtime_cache(runtime_state: dict[str, Any] | None) -> dict[str, Any]:
    if runtime_state is None:
        return {"values": {}, "registers": {}, "last_read": {}}
    cache = runtime_state.setdefault("smartess_local", {})
    if not isinstance(cache, dict):
        runtime_state["smartess_local"] = {}
        cache = runtime_state["smartess_local"]
    return cache


def _target_for_block(target: ProbeTarget, block_key: str) -> ProbeTarget:
    collector_addr = (
        _SMARTESS_0925_CONFIG_COLLECTOR_ADDR
        if block_key in _SMARTESS_0925_CONFIG_BLOCKS
        else _SMARTESS_0925_DATA_COLLECTOR_ADDR
    )
    return _target_with_collector_addr(target, collector_addr)


def _target_for_register(target: ProbeTarget, register: int) -> ProbeTarget:
    collector_addr = (
        _SMARTESS_0925_DATA_COLLECTOR_ADDR
        if int(register) in _SMARTESS_0925_CONFIG_STATE_REGISTER_RANGE
        else _SMARTESS_0925_CONFIG_COLLECTOR_ADDR
    )
    return _target_with_collector_addr(target, collector_addr)


def _target_with_collector_addr(target: ProbeTarget, collector_addr: int) -> ProbeTarget:
    if target.collector_addr == collector_addr:
        return target
    return ProbeTarget(
        devcode=target.devcode,
        collector_addr=collector_addr,
        device_addr=target.device_addr,
    )


def _is_block_due(
    block_key: str,
    last_read: dict[str, float],
    *,
    now: float,
    poll_interval: float | None,
) -> bool:
    if block_key not in last_read:
        return True
    if block_key in _FAST_BLOCKS:
        return True
    base_interval = max(float(poll_interval or 10.0), 1.0)
    if block_key in _MEDIUM_BLOCKS:
        return now - last_read[block_key] >= max(30.0, base_interval * 3.0)
    if block_key in _SLOW_BLOCKS:
        return now - last_read[block_key] >= max(60.0, base_interval * 6.0)
    return now - last_read[block_key] >= base_interval


async def _read_capability_register_fallbacks(
    driver: SmartEssLocalDriver,
    transport,
    target: ProbeTarget,
    capabilities: tuple[WriteCapability, ...],
    *,
    block_start: int,
    block_count: int,
    register_cache: dict[int, int],
    runtime_state: dict[str, Any] | None = None,
) -> None:
    """Read individual capability registers when a bulk metadata block is rejected."""

    block_end = block_start + block_count
    for capability in capabilities:
        if capability.value_kind == "action":
            continue
        register = int(capability.register)
        if register in register_cache:
            continue
        if not block_start <= register < block_end:
            continue
        register_cache_key = f"register:{register}"
        if command_skipped_as_unsupported(runtime_state, register_cache_key):
            continue
        session = driver._session(transport, _target_for_register(target, register))
        try:
            words = _normalize_register_words(register, await session.read_holding(register, 1))
        except Exception:
            record_command_failure(runtime_state, register_cache_key)
            continue
        record_command_success(runtime_state, register_cache_key)
        if words:
            register_cache[register] = int(words[0])


async def _capture_individual_control_registers(
    driver: SmartEssLocalDriver,
    transport,
    target: ProbeTarget,
    capabilities: tuple[WriteCapability, ...],
    *,
    block_start: int,
    block_count: int,
    block_key: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Capture readable control registers without relying on unsupported bulk reads."""

    captured: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    block_end = block_start + block_count
    for capability in capabilities:
        if capability.value_kind == "action":
            continue
        register = int(capability.register)
        if not block_start <= register < block_end:
            continue
        target_for_register = _target_for_register(target, register)
        session = driver._session(transport, target_for_register)
        try:
            words = _normalize_register_words(
                register,
                await session.read_holding(register, 1),
            )
        except Exception as exc:
            failures.append(
                {
                    "start": register,
                    "count": 1,
                    "block": block_key,
                    "capability": capability.key,
                    "collector_addr": target_for_register.collector_addr,
                    "error": str(exc),
                }
            )
            continue
        captured.append(
            {
                "start": register,
                "count": 1,
                "block": block_key,
                "capability": capability.key,
                "collector_addr": target_for_register.collector_addr,
                "words": list(words),
            }
        )
    return captured, failures


def _looks_like_smartess_0925(live_words: list[int], lcd_backlight: list[int]) -> bool:
    if len(live_words) < 14 or not lcd_backlight:
        return False
    checks = 0
    if int(lcd_backlight[0]) in {0, 1}:
        checks += 1
    if 0 <= int(live_words[0]) <= 20:
        checks += 1
    if _is_plausible_voltage(int(live_words[1]) / 10.0, allow_zero=True, maximum=300.0):
        checks += 1
    if _is_plausible_voltage(int(live_words[5]) / 10.0, allow_zero=False, maximum=120.0):
        checks += 1
    if _is_plausible_voltage(int(live_words[9]) / 10.0, allow_zero=True, maximum=300.0):
        checks += 1
    output_frequency = int(live_words[10]) / 10.0
    if output_frequency == 0.0 or 45.0 <= output_frequency <= 65.0:
        checks += 1
    if 0 <= int(live_words[13]) <= 200:
        checks += 1
    return checks >= 5


def _normalize_block_words(block_key: str, words: list[int]) -> list[int]:
    """Return schema-normalized SmartESS 0925 register words."""

    if block_key in _SMARTESS_0925_CONFIG_BLOCKS:
        return [int(word) for word in words]
    return _swap_words(words)


def _normalize_register_words(register: int, words: list[int]) -> list[int]:
    if int(register) in _SMARTESS_0925_CONFIG_STATE_REGISTER_RANGE:
        return _swap_words(words)
    return [int(word) for word in words]


def _swap_words(words: list[int]) -> list[int]:
    """Swap bytes inside each 16-bit SmartESS 0925 data-area register word."""

    return [_swap_word(int(word)) for word in words]


def _swap_word(word: int) -> int:
    value = int(word) & 0xFFFF
    return ((value & 0x00FF) << 8) | ((value & 0xFF00) >> 8)


def _is_plausible_voltage(value: float, *, allow_zero: bool, maximum: float) -> bool:
    if allow_zero and value == 0.0:
        return True
    return 1.0 <= value <= maximum


def _decode_block(
    start_register: int,
    words: list[int],
    specs: tuple[RegisterValueSpec, ...],
) -> dict[str, Any]:
    """Decode one SmartESS block via the shared decoder.

    The historical private copy was a near-verbatim clone; equivalence was
    verified against the schema corpus (all multi-word specs are
    u32_high_first/unsigned, no multipliers).
    """

    return shared_decode_block(start_register, [int(value) for value in words], specs)






def _apply_capability_read_back(
    values: dict[str, Any],
    capabilities: tuple[WriteCapability, ...],
    register_cache: dict[int, int],
) -> None:
    for capability in capabilities:
        if capability.value_kind == "action":
            continue
        value_key = capability.value_key
        if not value_key:
            continue
        if capability.register not in register_cache:
            continue
        raw_value = register_cache[capability.register]
        values[value_key] = _decode_capability_value(capability, [raw_value])


def _find_capability(
    capability_key: str,
    capabilities: tuple[WriteCapability, ...],
) -> WriteCapability:
    for capability in capabilities:
        if capability.key == capability_key:
            return capability
    raise ValueError(f"unsupported_capability:{capability_key}")


def _encode_capability_value(capability: WriteCapability, value: Any) -> int:
    if capability.value_kind == "action":
        return _encode_action_value(capability, value)
    if capability.value_kind == "bool":
        return _encode_bool_value(capability, value)
    if capability.value_kind == "enum":
        return _encode_enum_value(capability, value)
    if capability.value_kind == "scaled_u16":
        return _encode_scaled_u16_value(capability, value)
    if capability.value_kind == "u16":
        return _encode_u16_value(capability, value)
    raise ValueError(f"unsupported_value_kind:{capability.value_kind}")


def _encode_bool_value(capability: WriteCapability, value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        raw = int(value)
    else:
        text = str(value).strip().lower()
        if text in {"1", "true", "on", "yes", "enable", "enabled"}:
            raw = 1
        elif text in {"0", "false", "off", "no", "disable", "disabled"}:
            raw = 0
        else:
            raise ValueError(f"unsupported_bool_value:{capability.key}:{value}")
    if raw not in {0, 1}:
        raise ValueError(f"unsupported_bool_raw:{capability.key}:{raw}")
    return raw


def _encode_action_value(capability: WriteCapability, value: Any) -> int:
    if value is None:
        if capability.action_value is None:
            raise ValueError(f"missing_action_value:{capability.key}")
        return capability.action_value
    raw = int(value)
    if capability.action_value is not None and raw != capability.action_value:
        raise ValueError(f"unsupported_action_value:{capability.key}:{raw}")
    return raw


def _encode_enum_value(capability: WriteCapability, value: Any) -> int:
    enum_map = capability.enum_value_map
    if isinstance(value, int):
        raw = int(value)
    else:
        text = str(value).strip()
        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            raw = int(text)
        else:
            reverse_map = {label: key for key, label in enum_map.items()}
            if text not in reverse_map:
                raise ValueError(f"unsupported_enum_value:{capability.key}:{text}")
            raw = reverse_map[text]
    if raw not in enum_map:
        raise ValueError(f"unsupported_enum_raw:{capability.key}:{raw}")
    return raw


def _encode_scaled_u16_value(capability: WriteCapability, value: Any) -> int:
    if capability.divisor is None:
        raise ValueError(f"missing_divisor:{capability.key}")
    raw = int(round(float(value) * capability.divisor))
    _validate_u16(capability, raw)
    return raw


def _encode_u16_value(capability: WriteCapability, value: Any) -> int:
    raw = int(value)
    _validate_u16(capability, raw)
    return raw


def _validate_u16(capability: WriteCapability, raw: int) -> None:
    if raw < 0 or raw > 0xFFFF:
        raise ValueError(f"u16_out_of_range:{capability.key}:{raw}")
    if capability.minimum is not None and raw < capability.minimum:
        raise ValueError(f"value_below_minimum:{capability.key}:{raw}:{capability.minimum}")
    if capability.maximum is not None and raw > capability.maximum:
        raise ValueError(f"value_above_maximum:{capability.key}:{raw}:{capability.maximum}")


def _decode_capability_value(capability: WriteCapability, raw_words: list[int]) -> Any:
    if not raw_words:
        raise ValueError(f"missing_raw_words:{capability.key}")
    if capability.value_kind == "bool":
        return bool(int(raw_words[0]))
    if capability.value_kind == "enum":
        raw = int(raw_words[0])
        return capability.enum_value_map.get(raw, f"Unknown ({raw})")
    if capability.value_kind == "scaled_u16" and capability.divisor:
        return round(int(raw_words[0]) / capability.divisor, decimals_for_divisor(capability.divisor))
    return int(raw_words[0])


def _model_name(values: dict[str, Any]) -> str:
    rated_power = values.get("nominal_output_active_power") or values.get("nominal_output_apparent_power")
    try:
        rated_power_int = int(rated_power)
    except (TypeError, ValueError):
        rated_power_int = 0
    if rated_power_int > 0:
        power_kw = rated_power_int / 1000.0
        power_label = f"{power_kw:g}kW"
        return f"PowMr {power_label} / VMII-NXPW5KW (SmartESS 0925)"
    return "PowMr 4.2kW / VMII-NXPW5KW (SmartESS 0925)"
