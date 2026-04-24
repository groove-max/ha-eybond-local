"""Hub that orchestrates runtime links, payload drivers, and polling."""

from __future__ import annotations

import asyncio
import logging
from time import monotonic
from typing import Any

from ..canonical_telemetry import (
    apply_canonical_measurements,
    canonical_measurements_for_driver,
)
from ..const import (
    CONNECTION_TYPE_EYBOND,
    DRIVER_HINT_AUTO,
)
from ..connection.models import EybondConnectionSpec
from ..drivers.base import InverterDriver
from ..drivers.registry import iter_drivers
from ..onboarding.driver_detection import async_detect_inverter
from ..models import CapabilityBlocker, DetectedInverter, RuntimeSnapshot, WriteCapability
from ..payload.modbus import ModbusError, ModbusSession, to_signed_16
from ..runtime_labels import runtime_path_label
from .link import EybondRuntimeLinkManager, resolve_server_ip

logger = logging.getLogger(__name__)


def _error_code(exc: BaseException) -> str:
    return str(exc)


def _is_retryable_collector_error(exc: BaseException) -> bool:
    """Return whether one transport error is worth retrying after reconnect."""

    return isinstance(exc, ConnectionError) and _error_code(exc) in {
        "collector_disconnected",
        "collector_not_connected",
        "collector_heartbeat_timeout",
        "collector_write_timeout",
    }


def _should_mark_snapshot_disconnected(exc: BaseException) -> bool:
    """Return whether one refresh error should make live sensors unavailable."""

    return _error_code(exc) in {
        "request_timeout",
        "collector_disconnected",
        "collector_not_connected",
        "collector_heartbeat_timeout",
        "collector_write_timeout",
    }


def _should_force_reconnect(exc: BaseException) -> bool:
    """Return whether one refresh error warrants a forced collector reconnect."""

    return _error_code(exc) in {
        "request_timeout",
        "collector_write_timeout",
    }


def _modbus_exception_code(exc: BaseException) -> int | None:
    """Parse one Modbus exception code from an error string."""

    if not isinstance(exc, ModbusError):
        return None

    text = str(exc)
    if not text.startswith("exception_code:"):
        return None
    try:
        return int(text.split(":", 1)[1])
    except ValueError:
        return None


def _blocker_from_write_error(
    capability: WriteCapability,
    exc: BaseException,
    *,
    operating_mode: object,
) -> CapabilityBlocker | None:
    """Return one structured runtime blocker for a write failure, if applicable."""

    exception_code = _modbus_exception_code(exc)
    if exception_code is None:
        return None

    capability_name = capability.display_name
    safe_modes = ", ".join(capability.safe_operating_modes)

    if exception_code == 1:
        return CapabilityBlocker(
            code="illegal_function",
            reason=(
                f"The inverter does not expose writable access for {capability_name!r} "
                "through this protocol."
            ),
            suggested_action=(
                "Leave this control disabled for the current firmware, or retry after "
                "updating the driver/profile."
            ),
            exception_code=exception_code,
            clear_on="redetect",
        )
    if exception_code == 2:
        return CapabilityBlocker(
            code="illegal_data_address",
            reason=(
                f"The inverter reported register {capability.register} for "
                f"{capability_name!r} as unavailable."
            ),
            suggested_action=(
                "This register is likely absent on the current model or firmware. "
                "Leave it disabled unless a later probe confirms support."
            ),
            exception_code=exception_code,
            clear_on="redetect",
        )
    if exception_code == 7:
        if (
            capability.unsafe_while_running
            and operating_mode
            and operating_mode not in capability.safe_operating_modes
        ):
            return CapabilityBlocker(
                code="mode_restricted",
                reason=(
                    f"The inverter rejected writes to {capability_name!r} while "
                    f"operating mode is {operating_mode!r}."
                ),
                suggested_action=(
                    "Retry after switching the inverter into a safe mode for this setting: "
                    f"{safe_modes}."
                ),
                exception_code=exception_code,
                clear_on="mode_change",
            )
        return CapabilityBlocker(
            code="unsupported_or_locked",
            reason=(
                f"The inverter rejected writes to {capability_name!r}. "
                "This register appears locked or unsupported by the current firmware."
            ),
            suggested_action=(
                "Keep this control disabled for now, or retry after a firmware/profile update."
            ),
            exception_code=exception_code,
            clear_on="redetect",
        )
    return None


def _friendly_write_error(
    capability: WriteCapability,
    exc: BaseException,
) -> ValueError | None:
    """Return one user-facing write error that should not persist as a blocker."""

    exception_code = _modbus_exception_code(exc)
    if exception_code != 3:
        return None

    native_minimum = capability.native_minimum
    native_maximum = capability.native_maximum
    if native_minimum is not None and native_maximum is not None:
        allowed_range = f"Allowed profile range: {native_minimum} to {native_maximum}."
    elif native_minimum is not None:
        allowed_range = f"Allowed profile minimum: {native_minimum}."
    elif native_maximum is not None:
        allowed_range = f"Allowed profile maximum: {native_maximum}."
    else:
        allowed_range = "The inverter may enforce a narrower range than the current profile metadata."

    return ValueError(
        f"illegal_data_value:{capability.key}:"
        f"The inverter rejected {capability.display_name!r} as out of range. "
        f"{allowed_range}"
    )


def _should_confirm_write(capability: WriteCapability) -> bool:
    """Return whether a write should be verified by immediate readback."""

    return capability.value_kind != "action"


def _write_readback_matches(
    capability: WriteCapability,
    *,
    requested_value: object,
    written_value: object,
    readback_value: object,
) -> bool:
    """Return whether one refreshed value confirms the requested write."""

    if readback_value == written_value or readback_value == requested_value:
        return True

    if capability.enum_value_map and isinstance(requested_value, int):
        expected_label = capability.enum_value_map.get(requested_value)
        if expected_label is not None and readback_value == expected_label:
            return True

    return False


def _write_not_confirmed_error(
    capability: WriteCapability,
    *,
    written_value: object,
    readback_value: object,
    refresh_error: str,
) -> RuntimeError:
    """Return one explicit error for a write that did not confirm by readback."""

    readback_text = "unavailable" if readback_value is None else repr(readback_value)
    message = (
        f"Command accepted, but {capability.display_name!r} did not confirm by readback. "
        f"Expected {written_value!r}, got {readback_text}."
    )
    if refresh_error:
        message = f"{message} Refresh reported {refresh_error}."
    return RuntimeError(f"write_not_confirmed:{capability.key}:{message}")


class EybondHub:
    """Coordinates runtime link connectivity, driver probing and polling."""

    def __init__(
        self,
        *,
        connection: EybondConnectionSpec,
        driver_hint: str = DRIVER_HINT_AUTO,
        connection_mode: str = "",
    ) -> None:
        self._driver_hint = driver_hint
        self._connection = connection
        self._connection_mode = connection_mode
        self._link_manager = EybondRuntimeLinkManager(
            server_ip=connection.server_ip,
            advertised_server_ip=connection.advertised_server_ip,
            collector_ip=connection.collector_ip,
            tcp_port=connection.tcp_port,
            advertised_tcp_port=connection.advertised_tcp_port,
            udp_port=connection.udp_port,
            discovery_target=connection.discovery_target,
            discovery_interval=connection.discovery_interval,
            heartbeat_interval=connection.heartbeat_interval,
        )
        self._driver: InverterDriver | None = None
        self._inverter: DetectedInverter | None = None
        self._last_snapshot = RuntimeSnapshot()
        self._runtime_read_state: dict[str, Any] = {}
        self._write_blockers: dict[str, CapabilityBlocker] = {}
        self._last_operating_mode: object | None = None
        self._last_success_monotonic: float | None = None
        self._recovery_backoff_until_monotonic = 0.0
        self._recovery_streak = 0
        self._reconnect_count = 0
        self._last_recovery_reason = ""

    async def async_start(self) -> None:
        """Start the underlying runtime link and discovery loop."""

        await self._link_manager.async_start()

    async def async_stop(self) -> None:
        """Stop discovery and the active runtime link."""

        await self._link_manager.async_stop()

    async def async_refresh(self, *, poll_interval: float | None = None) -> RuntimeSnapshot:
        """Refresh the current runtime snapshot."""

        if not self._link_manager.connected:
            self._runtime_read_state.clear()
            ok = await self._link_manager.async_try_connect(timeout=0.75)
            if not ok:
                snapshot = self._build_snapshot(last_error="waiting_for_collector")
                self._last_snapshot = snapshot
                return snapshot

        ok = await self._link_manager.async_try_connect(timeout=1.5, require_heartbeat=True)
        if not ok:
            self._runtime_read_state.clear()
            snapshot = self._build_snapshot(
                last_error="collector_heartbeat_timeout" if self._link_manager.connected else "waiting_for_collector",
                connected=False,
            )
            self._last_snapshot = snapshot
            return snapshot

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                logger.warning("Driver detection failed: %s", detect_error)
                snapshot = self._build_snapshot(last_error=detect_error)
                self._last_snapshot = snapshot
                return snapshot

        remaining_backoff = self._recovery_backoff_remaining()
        if remaining_backoff > 0:
            logger.warning(
                "Runtime refresh backoff active after %s; skipping refresh for %.1fs",
                self._last_recovery_reason or "runtime_error",
                remaining_backoff,
            )
            snapshot = self._build_snapshot(
                last_error=self._last_recovery_reason or self._last_snapshot.last_error or "request_timeout",
                connected=False,
            )
            self._last_snapshot = snapshot
            return snapshot

        try:
            runtime_values = await self._driver.async_read_values(
                self._link_manager.transport,
                self._inverter,
                runtime_state=self._runtime_read_state,
                poll_interval=poll_interval,
                now_monotonic=asyncio.get_running_loop().time() if poll_interval is not None else None,
            )
        except Exception as exc:
            if _is_retryable_collector_error(exc):
                logger.warning("Runtime refresh failed: %s; retrying after collector reconnect", exc)
                try:
                    self._record_recovery_attempt(reason=_error_code(exc))
                    await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                    self._runtime_read_state.clear()
                    runtime_values = await self._driver.async_read_values(
                        self._link_manager.transport,
                        self._inverter,
                        runtime_state=self._runtime_read_state,
                        poll_interval=poll_interval,
                        now_monotonic=asyncio.get_running_loop().time() if poll_interval is not None else None,
                    )
                except Exception as retry_exc:
                    logger.warning("Runtime refresh failed after retry: %s", retry_exc)
                    self._runtime_read_state.clear()
                    self._record_recovery_failure(reason=_error_code(retry_exc))
                    snapshot = self._build_snapshot(
                        last_error=str(retry_exc),
                        connected=False if _should_mark_snapshot_disconnected(retry_exc) else None,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            elif _should_force_reconnect(exc):
                logger.warning(
                    "Runtime refresh failed: %s; forcing collector reconnect and retry",
                    exc,
                )
                try:
                    self._record_recovery_attempt(reason=_error_code(exc))
                    await self._link_manager.async_reset_connection(reason=str(exc))
                    await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                    self._runtime_read_state.clear()
                    runtime_values = await self._driver.async_read_values(
                        self._link_manager.transport,
                        self._inverter,
                        runtime_state=self._runtime_read_state,
                        poll_interval=poll_interval,
                        now_monotonic=asyncio.get_running_loop().time() if poll_interval is not None else None,
                    )
                except Exception as retry_exc:
                    logger.warning("Runtime refresh failed after forced reconnect: %s", retry_exc)
                    self._runtime_read_state.clear()
                    self._record_recovery_failure(reason=_error_code(retry_exc))
                    snapshot = self._build_snapshot(
                        last_error=str(retry_exc),
                        connected=False if _should_mark_snapshot_disconnected(retry_exc) else None,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            else:
                logger.warning("Runtime refresh failed: %s", exc)
                self._runtime_read_state.clear()
                snapshot = self._build_snapshot(
                    last_error=str(exc),
                    connected=False if _should_mark_snapshot_disconnected(exc) else None,
                )
                self._last_snapshot = snapshot
                return snapshot

            self._record_refresh_success()
        snapshot = self._build_snapshot(extra_values=runtime_values)
        self._last_snapshot = snapshot
        return snapshot

    async def async_write_capability(
        self,
        capability_key: str,
        value: object,
    ) -> object:
        """Write one validated capability through the active driver."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                raise RuntimeError(detect_error or "no_supported_driver_matched")

        snapshot = await self.async_refresh()
        capability = self._inverter.get_capability(capability_key)
        runtime_state = capability.runtime_state(snapshot.values)
        if not runtime_state.editable:
            reasons = "; ".join(runtime_state.reasons) or "capability_not_editable"
            raise ValueError(f"capability_not_editable:{capability_key}:{reasons}")

        written_value: object | None = None
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                written_value = await self._driver.async_write_capability(
                    self._link_manager.transport,
                    self._inverter,
                    capability_key,
                    value,
                )
                self._write_blockers.pop(capability_key, None)
                break
            except Exception as exc:
                last_error = exc
                if attempt == 0 and _is_retryable_collector_error(exc):
                    logger.warning(
                        "Write %s failed: %s; retrying once after collector reconnect",
                        capability_key,
                        exc,
                    )
                    await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                    continue
                friendly_error = _friendly_write_error(capability, exc)
                if friendly_error is not None:
                    raise friendly_error from exc
                blocker = _blocker_from_write_error(
                    capability,
                    exc,
                    operating_mode=snapshot.values.get("operating_mode"),
                )
                if blocker:
                    logger.warning(
                        "Blocking capability %s after write failure: %s (%s)",
                        capability_key,
                        blocker.reason,
                        blocker.code,
                    )
                    self._write_blockers[capability_key] = blocker
                raise

        if written_value is None:
            raise last_error or RuntimeError(f"write_failed:{capability_key}")

        snapshot = await self.async_refresh()
        if snapshot.last_error in {"collector_disconnected", "collector_not_connected", "waiting_for_collector"}:
            logger.warning(
                "Refresh after write reported: %s; retrying once after collector reconnect",
                snapshot.last_error,
            )
            await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
            snapshot = await self.async_refresh()
        if snapshot.last_error:
            logger.warning("Refresh after write reported: %s", snapshot.last_error)

        if _should_confirm_write(capability):
            readback_value = snapshot.values.get(capability.value_key)
            if not _write_readback_matches(
                capability,
                requested_value=value,
                written_value=written_value,
                readback_value=readback_value,
            ):
                logger.warning(
                    "Write %s was accepted but did not confirm by readback; expected=%r readback=%r refresh_error=%s",
                    capability_key,
                    written_value,
                    readback_value,
                    snapshot.last_error or "",
                )
                raise _write_not_confirmed_error(
                    capability,
                    written_value=written_value,
                    readback_value=readback_value,
                    refresh_error=snapshot.last_error,
                )
        return written_value

    async def async_apply_preset(self, preset_key: str) -> dict[str, object]:
        """Apply one declarative preset through sequential capability writes."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                raise RuntimeError(detect_error or "no_supported_driver_matched")

        snapshot = await self.async_refresh()
        preset = self._inverter.get_capability_preset(preset_key)
        runtime_state = preset.runtime_state(self._inverter, snapshot.values)
        if not runtime_state.visible:
            reasons = "; ".join(runtime_state.reasons) or "preset_not_visible"
            raise ValueError(f"preset_not_visible:{preset_key}:{reasons}")
        if not runtime_state.applicable:
            reasons = "; ".join(runtime_state.reasons or runtime_state.warnings) or "preset_not_applicable"
            raise ValueError(f"preset_not_applicable:{preset_key}:{reasons}")

        results: list[dict[str, object]] = []
        for item in sorted(preset.items, key=lambda item: (item.order, item.capability_key)):
            capability = self._inverter.get_capability(item.capability_key)
            current_value = snapshot.values.get(capability.value_key)
            target_label = capability.enum_value_map.get(item.value, item.value)
            if current_value == item.value or current_value == target_label:
                results.append(
                    {
                        "key": capability.key,
                        "status": "unchanged",
                        "current_value": current_value,
                        "target_value": target_label,
                    }
                )
                continue

            written_value = await self.async_write_capability(capability.key, item.value)
            snapshot = self._last_snapshot
            results.append(
                {
                    "key": capability.key,
                    "status": "written",
                    "current_value": current_value,
                    "target_value": target_label,
                    "written_value": written_value,
                }
            )

        return {
            "preset_key": preset.key,
            "title": preset.title,
            "results": results,
            "warnings": list(runtime_state.warnings),
        }

    async def async_capture_support_evidence(self) -> dict[str, object]:
        """Capture matched-driver or generic raw evidence for one support archive."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        detect_error = ""
        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                return await self._async_capture_generic_support_evidence(detect_error)

        try:
            evidence = await self._driver.async_capture_support_evidence(
                self._link_manager.transport,
                self._inverter,
            )
        except Exception as exc:
            if _is_retryable_collector_error(exc):
                logger.warning(
                    "Support evidence capture failed: %s; retrying after collector reconnect",
                    exc,
                )
                await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                evidence = await self._driver.async_capture_support_evidence(
                    self._link_manager.transport,
                    self._inverter,
                )
            else:
                raise

        return evidence

    async def _async_capture_generic_support_evidence(
        self,
        detect_error: str,
    ) -> dict[str, object]:
        """Capture generic register evidence when no built-in driver matches."""

        captures: list[dict[str, Any]] = []

        for driver in iter_drivers(self._driver_hint):
            schema = getattr(driver, "register_schema_metadata", None)
            probe_targets = getattr(driver, "probe_targets", ())
            if schema is None or not probe_targets:
                continue

            target = probe_targets[0]
            ranges = _capture_ranges_from_schema(schema)
            if not ranges:
                continue

            session = ModbusSession(
                self._link_manager.transport,
                route=target.link_route,
                slave_id=target.payload_address,
            )

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

                captured_ranges.append(_format_support_range(start, values))
                fixture_ranges.append(
                    {
                        "start": start,
                        "count": count,
                        "values": list(values),
                    }
                )

            captures.append(
                {
                    "driver_key": driver.key,
                    "driver_name": runtime_path_label(driver.key),
                    "driver_implementation_name": driver.name,
                    "runtime_path_name": runtime_path_label(driver.key),
                    "profile_name": getattr(driver, "profile_name", ""),
                    "register_schema_name": getattr(driver, "register_schema_name", ""),
                    "probe_target": {
                        "devcode": target.devcode,
                        "collector_addr": target.collector_addr,
                        "device_addr": target.device_addr,
                    },
                    "planned_ranges": [
                        {"start": start, "count": count}
                        for start, count in ranges
                    ],
                    "captured_ranges": captured_ranges,
                    "range_failures": failures,
                    "fixture_ranges": fixture_ranges,
                }
            )

        return {
            "capture_kind": "generic_register_dump",
            "driver_hint": self._driver_hint,
            "connection_mode": self._connection_mode,
            "detection_error": detect_error or "no_supported_driver_matched",
            "captures": captures,
        }

    async def _async_ensure_connected(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> None:
        """Ensure there is an active collector connection, retrying discovery if needed."""

        await self._link_manager.async_ensure_connected(
            timeout=timeout,
            require_heartbeat=require_heartbeat,
        )

    async def _async_detect_driver(self) -> str:
        try:
            context = await async_detect_inverter(
                self._link_manager.transport,
                driver_hint=self._driver_hint,
            )
        except RuntimeError as exc:
            return str(exc)

        self._driver = context.driver
        self._inverter = context.inverter
        self._runtime_read_state.clear()
        self._write_blockers.clear()
        logger.info(
            "Detected inverter driver=%s protocol=%s serial=%s confidence=%s",
            context.inverter.driver_key,
            context.inverter.protocol_family,
            context.inverter.serial_number,
            context.match.confidence,
        )
        return ""

    def _recovery_backoff_delay(self) -> float:
        base = max(2.0, float(self._connection.request_timeout))
        return min(60.0, base * (2 ** max(self._recovery_streak - 1, 0)))

    def _recovery_backoff_remaining(self) -> float:
        if self._recovery_backoff_until_monotonic <= 0.0:
            return 0.0
        return max(0.0, self._recovery_backoff_until_monotonic - monotonic())

    def _record_recovery_attempt(self, *, reason: str) -> None:
        self._reconnect_count += 1
        self._last_recovery_reason = reason

    def _record_recovery_failure(self, *, reason: str) -> None:
        self._recovery_streak += 1
        self._last_recovery_reason = reason
        self._recovery_backoff_until_monotonic = monotonic() + self._recovery_backoff_delay()

    def _record_refresh_success(self) -> None:
        self._last_success_monotonic = monotonic()
        self._recovery_streak = 0
        self._recovery_backoff_until_monotonic = 0.0
        self._last_recovery_reason = ""

    def _build_snapshot(
        self,
        *,
        extra_values: dict[str, object] | None = None,
        last_error: str | None = None,
        connected: bool | None = None,
    ) -> RuntimeSnapshot:
        generated_canonical_keys: set[str] = set()
        if self._inverter is not None:
            generated_canonical_keys = {
                description.key
                for description in canonical_measurements_for_driver(self._inverter.driver_key)
            }

        values = {
            key: value
            for key, value in self._last_snapshot.values.items()
            if not key.startswith("capability_block_") and key not in generated_canonical_keys
        }
        collector = self._link_manager.collector_info

        if collector.remote_ip:
            values["collector_remote_ip"] = collector.remote_ip
        values["collector_connection_count"] = collector.connection_count
        values["collector_connection_replace_count"] = collector.connection_replace_count
        values["collector_disconnect_count"] = collector.disconnect_count
        values["collector_pending_request_drop_count"] = collector.pending_request_drop_count
        values["collector_discovery_restart_count"] = collector.discovery_restart_count
        if collector.collector_pn:
            values["collector_pn"] = collector.collector_pn
        if collector.profile_name:
            values["collector_profile"] = collector.profile_name
        if collector.profile_key:
            values["collector_profile_key"] = collector.profile_key
        if collector.last_disconnect_reason:
            values["collector_last_disconnect_reason"] = collector.last_disconnect_reason
        else:
            values.pop("collector_last_disconnect_reason", None)
        if collector.last_discovery_reason:
            values["collector_last_discovery_reason"] = collector.last_discovery_reason
        else:
            values.pop("collector_last_discovery_reason", None)
        if collector.heartbeat_devcode is not None:
            values["collector_heartbeat_devcode"] = f"0x{collector.heartbeat_devcode:04X}"
        if collector.heartbeat_payload_hex:
            values["collector_heartbeat_payload"] = collector.heartbeat_payload_hex
        if collector.heartbeat_age_seconds is not None:
            values["collector_heartbeat_age_seconds"] = round(collector.heartbeat_age_seconds, 1)
        else:
            values.pop("collector_heartbeat_age_seconds", None)
        if collector.heartbeat_ascii:
            values["collector_heartbeat_ascii"] = collector.heartbeat_ascii
        if collector.heartbeat_payload_len is not None:
            values["collector_heartbeat_payload_len"] = collector.heartbeat_payload_len
        if collector.heartbeat_format_key:
            values["collector_heartbeat_format"] = collector.heartbeat_format_key
        if collector.heartbeat_suffix_ascii:
            values["collector_heartbeat_suffix"] = collector.heartbeat_suffix_ascii
        if collector.heartbeat_suffix_kind:
            values["collector_heartbeat_suffix_kind"] = collector.heartbeat_suffix_kind
        if collector.heartbeat_suffix_uint is not None:
            values["collector_heartbeat_suffix_uint"] = collector.heartbeat_suffix_uint
        if collector.devcode_major is not None:
            values["collector_devcode_major"] = collector.devcode_major
        if collector.devcode_minor is not None:
            values["collector_devcode_minor"] = collector.devcode_minor
        if collector.collector_pn_prefix:
            values["collector_pn_prefix"] = collector.collector_pn_prefix
        if collector.collector_pn_digits:
            values["collector_pn_digits"] = collector.collector_pn_digits
        values["connection_type"] = CONNECTION_TYPE_EYBOND
        if self._connection_mode:
            values["connection_mode"] = self._connection_mode
        if self._connection.collector_ip:
            values["configured_collector_ip"] = self._connection.collector_ip
        if collector.last_devcode is not None:
            values["collector_devcode"] = f"0x{collector.last_devcode:04X}"
        if collector.last_udp_reply:
            values["collector_udp_reply"] = collector.last_udp_reply
        if collector.last_udp_reply_from:
            values["collector_udp_reply_from"] = collector.last_udp_reply_from

        if self._inverter is not None:
            values["driver_key"] = self._inverter.driver_key
            values["protocol_family"] = self._inverter.protocol_family
            if self._inverter.variant_key:
                values["variant_key"] = self._inverter.variant_key
            if self._inverter.profile_name:
                values["profile_name"] = self._inverter.profile_name
            if self._inverter.register_schema_name:
                values["register_schema_name"] = self._inverter.register_schema_name
            values["model_name"] = self._inverter.model_name
            values["serial_number"] = self._inverter.serial_number
            if self._inverter.capabilities:
                values["write_capabilities"] = ", ".join(
                    capability.key for capability in self._inverter.capabilities
                )
            values.update(self._inverter.details)

        if extra_values:
            values.update(extra_values)

        if self._inverter is not None:
            apply_canonical_measurements(self._inverter.driver_key, values)

        values["runtime_recovery_streak"] = self._recovery_streak
        values["runtime_reconnect_count"] = self._reconnect_count
        values["runtime_backoff_seconds"] = round(self._recovery_backoff_remaining(), 1)
        if self._last_success_monotonic is not None:
            values["runtime_last_success_age_seconds"] = round(
                max(0.0, monotonic() - self._last_success_monotonic),
                1,
            )
        else:
            values.pop("runtime_last_success_age_seconds", None)
        if self._last_recovery_reason:
            values["runtime_last_recovery_reason"] = self._last_recovery_reason
        else:
            values.pop("runtime_last_recovery_reason", None)

        operating_mode = values.get("operating_mode")
        if operating_mode != self._last_operating_mode:
            clearable = [
                capability_key
                for capability_key, blocker in self._write_blockers.items()
                if blocker.clear_on == "mode_change"
            ]
            if self._last_operating_mode is not None and clearable:
                logger.info(
                    "Operating mode changed from %r to %r; clearing %d capability write blockers",
                    self._last_operating_mode,
                    operating_mode,
                    len(clearable),
                )
            for capability_key in clearable:
                self._write_blockers.pop(capability_key, None)
            self._last_operating_mode = operating_mode

        for capability_key, blocker in sorted(self._write_blockers.items()):
            values[f"capability_block_reason_{capability_key}"] = blocker.reason
            values[f"capability_block_code_{capability_key}"] = blocker.code
            if blocker.suggested_action:
                values[f"capability_block_action_{capability_key}"] = blocker.suggested_action
            if blocker.exception_code is not None:
                values[f"capability_block_exception_{capability_key}"] = blocker.exception_code
        if self._write_blockers:
            values["blocked_write_capabilities"] = ", ".join(sorted(self._write_blockers))
            values["blocked_write_count"] = len(self._write_blockers)
            values["blocked_write_summary"] = "; ".join(
                f"{capability_key}: {blocker.code}"
                for capability_key, blocker in sorted(self._write_blockers.items())
            )
        else:
            values.pop("blocked_write_capabilities", None)
            values.pop("blocked_write_count", None)
            values.pop("blocked_write_summary", None)

        if last_error:
            values["last_error"] = last_error
        else:
            values.pop("last_error", None)

        return RuntimeSnapshot(
            connected=self._link_manager.connected if connected is None else connected,
            collector=collector,
            inverter=self._inverter,
            values=values,
            last_error=last_error,
        )


def _capture_ranges_from_schema(schema: Any) -> tuple[tuple[int, int], ...]:
    """Build one generic support-capture plan from register schema metadata."""

    planned: list[tuple[int, int]] = []
    for block_key in ("status", "serial", "live", "config"):
        try:
            block = schema.block(block_key)
        except KeyError:
            continue
        planned.append((block.start, block.count))

    try:
        planned.extend(
            (spec.register, spec.word_count)
            for spec in schema.spec_set("aux_config")
        )
    except KeyError:
        pass

    scalar_registers = getattr(schema, "scalar_registers", {})
    planned.extend(
        (register, 1)
        for register in sorted(set(scalar_registers.values()))
    )
    return _merge_capture_ranges(planned)


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
