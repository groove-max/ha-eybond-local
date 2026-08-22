"""HubSupportMixin ownership slice for the runtime hub."""

from __future__ import annotations

from ...drivers.local_register_evidence import LocalRegisterSnapshot
from ...collector_identity import validated_collector_pn

from .common import (
    Any,
    DRIVER_HINT_AUTO,
    EybondLinkRoute,
    ModbusSession,
    _AT_TEXT_ASCII_PROBE_TIMEOUT,
    _capture_ranges_from_schema,
    _format_support_range,
    _is_retryable_collector_error,
    async_send_payload,
    asyncio,
    collector_capability_profile_from_runtime,
    iter_drivers,
    logger,
    runtime_path_label,
    select_payload_route,
)


class HubSupportMixin:
    """Methods owned by HubSupportMixin."""

    async def async_capture_support_evidence(self) -> dict[str, object]:
        """Capture matched-driver or generic raw evidence for one support archive."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        detect_error = ""
        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                if (
                    self._collector_capabilities().virtual_bridge
                    and "probe_timeout" in str(detect_error or "")
                ):
                    return self._collector_only_support_evidence(detect_error)
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

    async def async_capture_local_register_snapshot(
        self,
    ) -> LocalRegisterSnapshot | None:
        """Capture exact driver-owned register observations from the live link."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
        if self._driver is None or self._inverter is None:
            await self._async_detect_driver()
        if self._driver is None or self._inverter is None:
            return None
        collector = self._link_manager.collector_info
        collector_pn = getattr(collector, "collector_pn", "")
        if (
            type(collector_pn) is not str
            or not collector_pn
            or validated_collector_pn(collector_pn) != collector_pn
        ):
            return None
        snapshot = await self._driver.async_capture_local_register_snapshot(
            self._link_manager.transport,
            self._inverter,
            collector_pn=collector_pn,
        )
        if snapshot is not None and type(snapshot) is not LocalRegisterSnapshot:
            raise TypeError("driver_local_register_snapshot_invalid")
        return snapshot

    def _collector_capabilities(self):
        """Return collector capabilities from current hub runtime evidence."""

        return collector_capability_profile_from_runtime(
            collector=getattr(self._link_manager, "collector_info", None),
            values=self._combined_collector_runtime_values(),
        )

    def _collector_only_support_evidence(self, detect_error: str) -> dict[str, object]:
        """Return bounded support evidence when a local bridge has no inverter link."""

        return {
            "capture_kind": "collector_only",
            "driver_hint": self._driver_hint,
            "connection_mode": self._connection_mode,
            "detection_error": detect_error or "collector_link_probe_timeout",
            "captures": [],
            "range_failures": [],
            "note": (
                "Local bridge was detected, but no downstream inverter replied during "
                "driver detection; generic register scans were skipped."
            ),
        }

    async def _async_capture_at_text_ascii_probe(self) -> dict[str, object] | None:
        """Capture a raw ASCII probe trace over an at_text collector callback.

        Generic register dumps only cover Modbus drivers, but at_text
        collectors bridge raw-serial ASCII inverters (PI30 family, G-ASCII).
        When detection fails there, the register scans abort on the route
        guard and the archive carries no wire evidence at all — this bounded
        read-only sweep records what each query actually got back.
        """

        # This probe is available only when PN-bound live evidence configured an
        # AT session. Cloud family alone never reaches this branch.
        configured_session_protocol = str(
            self._connection.collector_configured_session_protocol or ""
        ).strip().lower()
        if configured_session_protocol != "at_text":
            return None

        transport = self._link_manager.transport
        attempts: list[dict[str, object]] = []
        # Detection may have failed, so ``self._driver`` may be unbound: gather
        # read-only probe plans from every driver via the registry. The commands
        # are protocol policy owned by each driver; the hub only executes them.
        for driver in iter_drivers(DRIVER_HINT_AUTO):
            for probe in driver.support_probe_plan():
                route = select_payload_route(
                    transport,
                    EybondLinkRoute(devcode=1, collector_addr=255),
                    payload_family=probe.payload_family,
                )
                attempt: dict[str, object] = {
                    # The owning driver is authoritative here (registry
                    # iteration), so record its key as probe provenance rather
                    # than trusting a duplicated field on the descriptor.
                    "driver_key": driver.key,
                    "payload_family": probe.payload_family,
                    "command": probe.command,
                    "request_hex": probe.request.hex(),
                }
                try:
                    response = await async_send_payload(
                        transport,
                        probe.request,
                        route=route,
                        request_timeout=_AT_TEXT_ASCII_PROBE_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    attempt["error"] = "request_timeout"
                except Exception as exc:
                    attempt["error"] = str(exc)
                else:
                    attempt["response_hex"] = response.hex()
                    attempt["response_ascii"] = response.decode(
                        "ascii", errors="replace"
                    )
                attempts.append(attempt)

        collector = self._link_manager.collector_info
        return {
            "session_protocol": configured_session_protocol,
            "raw_passthrough_frame_format": collector.raw_last_frame_format,
            "raw_request_count": collector.raw_request_count,
            "raw_response_count": collector.raw_response_count,
            "raw_timeout_count": collector.raw_timeout_count,
            "raw_unhandled_line_count": collector.raw_unhandled_line_count,
            "attempts": attempts,
        }

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
            ranges = _capture_ranges_from_schema(
                schema,
                driver_key=driver.key,
            )
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

        evidence: dict[str, object] = {
            "capture_kind": "generic_register_dump",
            "driver_hint": self._driver_hint,
            "connection_mode": self._connection_mode,
            "detection_error": detect_error or "no_supported_driver_matched",
            "captures": captures,
        }
        ascii_probe = await self._async_capture_at_text_ascii_probe()
        if ascii_probe is not None:
            evidence["at_text_ascii_probe"] = ascii_probe
        return evidence
