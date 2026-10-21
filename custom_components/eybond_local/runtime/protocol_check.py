"""Read-only alternate-profile checks; never change the running driver."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping

from ..drivers.registry import get_driver, iter_drivers
from ..drivers.local_register_evidence import LocalRegisterReadPlan
from ..link_models import EybondLinkRoute
from ..link_transport import async_send_payload
from ..models import ProbeTarget
from ..payload.modbus import decode_read_request
from ..support.shadow_learning.protocol_mismatch import CloudModbusRead
from ..support.shadow_learning.protocol_evidence import CloudProtocolEvidence, read_protocols, protocol_sample
from ..payload.modbus import build_read_request


PROBE_ROUTE_OPTION = "inverter_probe_route"
COMPARISON_KEYS = ("battery_voltage", "grid_voltage", "ac_output_voltage",
                   "output_voltage", "output_power", "pv_power")


def selected_probe_target(raw: object, driver_hint: str) -> ProbeTarget | None:
    """A saved user selection is scoped to ONE driver, not detected identity."""
    if raw is None or driver_hint == "auto":
        return None
    try:
        driver = get_driver(driver_hint)
    except KeyError:
        return None
    if not driver.cloud_probe_codecs:
        return None
    if not isinstance(raw, Mapping):
        raise ValueError("invalid_inverter_probe_route")
    if raw.get("driver_key") != driver_hint:
        return None
    codec = raw.get("codec", "modbus_rtu")
    values = [raw.get(key) for key in ("devcode", "collector_addr", "device_addr")]
    bounds = ((0, 65535), (0, 255), (1, 247) if codec == "modbus_rtu" else (0, 255))
    if (codec not in driver.cloud_probe_codecs or any(
            type(value) is not int or not low <= value <= high
            for value, (low, high) in zip(values, bounds))):
        # Corrupt intent must not silently redirect polling/writes to slave 1.
        raise ValueError("invalid_inverter_probe_route")
    return ProbeTarget(*values)


class ReadOnlyProtocolTransport:
    """Allow only validated reads in one candidate codec/observed route."""

    def __init__(self, transport, target: ProbeTarget, *,
                 extra_reads: tuple[LocalRegisterReadPlan, ...] = (),
                 codec: str = "modbus_rtu") -> None:
        self._transport = transport
        self._target = target
        self._extra_reads = extra_reads
        self._codec = codec
        self.samples: list[dict] = []

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        request = decode_read_request(payload)
        codecs = read_protocols(payload)
        if (not isinstance(route, EybondLinkRoute)
                or self._codec not in codecs
                or route.devcode != self._target.devcode
                or codecs[self._codec] != self._target.device_addr):
            raise ValueError("protocol_check_read_only_route_required")
        if route.collector_addr != self._target.collector_addr and not (
            request is not None and self._codec == "modbus_rtu" and any(
            plan.devcode == route.devcode
            and plan.collector_addr == route.collector_addr
            and plan.device_addr == request.slave_id
            and plan.function == request.function_code
            and plan.start == request.address and plan.count == request.count
            for plan in self._extra_reads
        )):
            raise ValueError("protocol_check_read_only_route_required")
        sample = {"request": protocol_sample(payload, direction="cloud_to_collector", carrier="profile_check"),
                  "devcode": route.devcode, "collector_addr": route.collector_addr}
        if len(self.samples) < 32:
            self.samples.append(sample)
        try:
            response = await async_send_payload(
                self._transport, payload, route=route,
                request_timeout=min(float(request_timeout or 3), 3),
            )
        except Exception as exc:
            sample["error_type"] = type(exc).__name__
            raise
        sample["response"] = protocol_sample(response, direction="collector_to_cloud", carrier="profile_check")
        return response


def _numeric(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


class ReadOnlyModbusTransport(ReadOnlyProtocolTransport):
    """Compatibility name for existing Modbus callers."""


def protocol_evidence(raw: object) -> CloudProtocolEvidence | None:
    """Accept current evidence or a retained old Modbus-only flow result."""
    evidence = CloudProtocolEvidence.from_mapping(raw)
    if evidence is not None:
        return evidence
    read = CloudModbusRead.from_mapping(raw)
    if read is not None:
        return CloudProtocolEvidence(read.devcode, read.collector_addr,
            build_read_request(read.device_addr, read.address, read.count, function=read.function))
    return None


async def check_modbus_profiles(transport, read: CloudModbusRead, local_values: dict) -> dict:
    return await check_protocol_profiles(transport, protocol_evidence(read.to_mapping()), local_values)


async def check_protocol_profiles(transport, evidence: CloudProtocolEvidence, local_values: dict) -> dict:
    """Require catalog identity AND comparable live readings, not a read ACK.

    Probe the observed unit/devcode. Split-route drivers may declare exact
    additional catalog reads, never a scan or unrestricted alternate route.
    Do not change UART, reinterpret PI30, attach overlays or forward writes.
    """
    candidates = []
    attempts = []
    read_diagnostics = {}
    for driver in iter_drivers("auto"):
        key = driver.key
        codecs = set(driver.cloud_probe_codecs).intersection(evidence.protocols)
        if not codecs:
            continue
        if len(codecs) != 1:
            attempts.append({"driver_key": key, "status": "ambiguous_driver_codec"})
            continue
        codec = next(iter(codecs))
        target = evidence.target(codec)
        try:
            guarded = ReadOnlyProtocolTransport(
                transport, target, codec=codec, extra_reads=driver.probe_read_plans(target),
            )
            read_diagnostics[key] = guarded.samples
            async with asyncio.timeout(15):
                inverter = await driver.async_probe(guarded, target)
                if inverter is None or inverter.probe_target != target:
                    attempts.append({"driver_key": key, "status": "no_match"})
                    continue
                if not inverter.profile_name or not inverter.register_schema_name:
                    attempts.append({"driver_key": key, "status": "no_profile"})
                    continue
                if (inverter.driver_key != key or driver.support_marker(
                        variant_key=inverter.variant_key, profile_name=inverter.profile_name,
                ) is not None):
                    attempts.append({"driver_key": key, "status": "unverified_profile"})
                    continue
                # Probe details are fresh reads from this operation, not the
                # running PI30 cache. Avoid a full slow settings poll when the
                # identity probe already returned comparable measurements.
                values = inverter.details
                if not any(_numeric(local_values.get(field)) and _numeric(values.get(field))
                           for field in COMPARISON_KEYS):
                    values = await driver.async_read_onboarding_values(guarded, inverter)
                comparisons = [
                    {"key": field, "local": local_values[field], "modbus": values[field]}
                    for field in COMPARISON_KEYS
                    if _numeric(local_values.get(field)) and _numeric(values.get(field))
                ]
                if not comparisons:
                    attempts.append({"driver_key": key, "status": "no_comparable_readings"})
                    continue
                candidates.append({
                    "driver_key": key, "model_name": inverter.model_name,
                    "codec": codec, "learning_supported": driver.cloud_learning_supported,
                    "profile_name": inverter.profile_name,
                    "register_schema_name": inverter.register_schema_name,
                    "comparisons": comparisons,
                    "route": {"driver_key": key, "codec": codec, "devcode": target.devcode,
                              "collector_addr": target.collector_addr,
                              "device_addr": target.device_addr},
                })
                attempts.append({"driver_key": key, "status": "matched"})
        except TimeoutError:
            attempts.append({"driver_key": key, "status": "timeout"})
        except Exception:
            # The support trace retains wire diagnostics; no arbitrary exception
            # text or secrets are exposed by this UI-facing result.
            attempts.append({"driver_key": key, "status": "unconfirmed"})
    return {"candidates": candidates, "attempts": attempts, "read_diagnostics": read_diagnostics}
