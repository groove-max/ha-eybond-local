"""Hub that orchestrates runtime links, payload drivers, and polling."""

from __future__ import annotations

import asyncio
import logging
from time import monotonic
from typing import Any, Callable

from ..canonical_telemetry import (
    apply_canonical_measurements,
    canonical_measurements_for_driver,
)
from ..const import (
    CONNECTION_TYPE_EYBOND,
    DRIVER_HINT_AUTO,
)
from ..connection.models import EybondConnectionSpec
from ..collector.at_runtime import parse_collector_vdtu, query_runtime_collector_at_values
from ..collector_endpoint import (
    DEFAULT_COLLECTOR_SERVER_PORT,
    inspect_collector_server_endpoint,
    normalize_collector_server_endpoint as normalize_runtime_collector_server_endpoint,
)
from ..collector.parameter_registry import query_runtime_collector_values
from ..collector.smartess_local import (
    QUERY_REBOOT_REQUIRED,
    SET_REBOOT_OR_APPLY,
    SET_SERVER_ENDPOINT,
    SmartEssLocalSession,
)
from ..drivers.base import InverterDriver
from ..drivers.registry import iter_drivers
from ..onboarding.driver_detection import async_detect_inverter
from ..models import CapabilityBlocker, DetectedInverter, RuntimeSnapshot, WriteCapability
from ..payload.modbus import ModbusError, ModbusSession, to_signed_16
from ..runtime_labels import runtime_path_label
from .link import EybondRuntimeLinkManager, resolve_server_ip

logger = logging.getLogger(__name__)


def _prefer_more_complete_collector_pn(current: object, candidate: object) -> str:
    normalized_current = str(current or "").strip()
    normalized_candidate = str(candidate or "").strip()
    if not normalized_candidate:
        return normalized_current
    if not normalized_current:
        return normalized_candidate
    if normalized_candidate == normalized_current:
        return normalized_candidate
    if normalized_candidate.startswith(normalized_current):
        return normalized_candidate
    if normalized_current.startswith(normalized_candidate):
        return normalized_current
    return normalized_candidate


def _split_collector_endpoint(endpoint: object) -> tuple[str, int | None, str]:
    raw = str(endpoint or "").strip()
    if not raw:
        return "", None, ""
    try:
        parsed = inspect_collector_server_endpoint(
            raw,
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return raw, None, ""
    return parsed.host, parsed.port, parsed.protocol


_DEFAULT_PROXY_CAPTURE_PORT = DEFAULT_COLLECTOR_SERVER_PORT


def _is_home_assistant_callback_endpoint(
    endpoint: object,
    *,
    server_ip: str,
    advertised_server_ip: str,
    advertised_tcp_port: int,
) -> bool:
    host, port, protocol = _split_collector_endpoint(endpoint)
    normalized_host = host.lower()
    allowed_hosts = {
        str(server_ip or "").strip().lower(),
        str(advertised_server_ip or "").strip().lower(),
    }
    allowed_hosts.discard("")
    return (
        bool(normalized_host)
        and normalized_host in allowed_hosts
        and port in {int(advertised_tcp_port or 0), _DEFAULT_PROXY_CAPTURE_PORT}
        and protocol.upper() == "TCP"
    )


def _callback_owner_label(
    endpoint: object,
    *,
    server_ip: str,
    advertised_server_ip: str,
    advertised_tcp_port: int,
) -> str:
    host, _port, _protocol = _split_collector_endpoint(endpoint)
    normalized_host = host.lower()
    if _is_home_assistant_callback_endpoint(
        endpoint,
        server_ip=server_ip,
        advertised_server_ip=advertised_server_ip,
        advertised_tcp_port=advertised_tcp_port,
    ):
        return "Home Assistant"
    if "eybond" in normalized_host or "smartess" in normalized_host:
        return "SmartESS cloud"
    if normalized_host:
        return "Custom endpoint"
    return "Unknown"


def _collector_signal_quality(signal_strength: object) -> str:
    try:
        value = int(signal_strength)
    except (TypeError, ValueError):
        return "unknown"
    if value >= -70:
        return "excellent"
    if value >= -85:
        return "good"
    if value >= -100:
        return "fair"
    return "weak"


def _collector_signal_source_label(source: object) -> str:
    normalized = str(source or "").strip().lower()
    if normalized == "wifi_rssi":
        return "Wi-Fi RSSI"
    if normalized == "gprs_csq":
        return "GPRS CSQ"
    return "Unknown"


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


def _normalize_collector_server_endpoint(endpoint: str) -> str:
    return normalize_runtime_collector_server_endpoint(
        endpoint,
        require_explicit_port=False,
        require_explicit_protocol=False,
        preserve_shape=True,
    )


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

    @property
    def collector_server_endpoint_rollback_target(self) -> str:
        """Return the rollback endpoint remembered during the active runtime session."""

        if self._collector_last_server_endpoint_before_change:
            return self._collector_last_server_endpoint_before_change
        return ""

    @property
    def effective_server_ip(self) -> str:
        """Return the collector-facing local host selected by the link manager."""

        return self._link_manager.effective_server_ip

    @property
    def effective_advertised_server_ip(self) -> str:
        """Return the callback host advertised to the collector."""

        return self._link_manager.effective_advertised_server_ip

    def diagnostic_link_transport(self):
        """Return the shared payload transport for read-only diagnostic command runs.

        Exposes the active collector link so the diagnostic command runner can
        reuse the existing connection instead of opening its own socket. Returns
        ``None`` when no link manager/transport is available.
        """

        link_manager = getattr(self, "_link_manager", None)
        if link_manager is None:
            return None
        return getattr(link_manager, "transport", None)

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
            collector_pn=connection.collector_pn,
            tcp_port=connection.tcp_port,
            advertised_tcp_port=connection.advertised_tcp_port,
            udp_port=connection.udp_port,
            discovery_target=connection.discovery_target,
            discovery_interval=connection.discovery_interval,
            heartbeat_interval=connection.heartbeat_interval,
        )
        self._driver: InverterDriver | None = None
        self._inverter: DetectedInverter | None = None
        self._inverter_overlay_applier: (
            Callable[[DetectedInverter, Any], DetectedInverter] | None
        ) = None
        self._last_snapshot = RuntimeSnapshot()
        self._runtime_read_state: dict[str, Any] = {}
        self._collector_runtime_values: dict[str, object] = {}
        self._collector_runtime_last_refresh_monotonic = 0.0
        self._collector_at_runtime_values: dict[str, object] = {}
        self._collector_at_runtime_last_refresh_monotonic = 0.0
        self._collector_last_server_endpoint_before_change = ""
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

    async def async_reconcile_network(self, *, reason: str = "network_change") -> bool:
        """Re-resolve listener network state after HA/network readiness changes."""

        return await self._link_manager.async_reconcile_network(reason=reason)

    def set_inverter_overlay_applier(
        self, applier: Callable[[DetectedInverter, Any], DetectedInverter] | None
    ) -> None:
        """Install a hook that post-processes the detected inverter.

        The coordinator uses this to merge activated device-scoped learned controls into
        the detected inverter (whose capabilities otherwise reflect only built-in
        detection), so the learned controls become entities and are writable.
        """

        self._inverter_overlay_applier = applier

    def set_reverse_discovery_enabled(self, enabled: bool) -> None:
        """Pass reverse-discovery policy changes through to the runtime link layer."""

        self._link_manager.set_reverse_discovery_enabled(enabled)

    async def async_ensure_callback_listener(self, port: int) -> None:
        """Ensure one auxiliary callback listener is available for collector redirects."""

        await self._link_manager.async_ensure_callback_listener(port)

    async def async_trigger_reverse_discovery(
        self,
        *,
        port: int = 0,
        timeout: float = 0.75,
    ) -> dict[str, object]:
        """Send one explicit UDP bootstrap redirect through the runtime link layer."""

        return await self._link_manager.async_trigger_reverse_discovery(
            port=port,
            timeout=timeout,
        )

    async def async_start_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        masked_endpoint: str = "",
        restore_trigger_path=None,
    ) -> None:
        """Start one in-process proxy capture route on the active runtime link."""

        route_kwargs = {
            "collector_ip": collector_ip,
            "listen_port": listen_port,
            "upstream_host": upstream_host,
            "upstream_port": upstream_port,
            "output_path": output_path,
            "masked_endpoint": masked_endpoint,
            "restore_trigger_path": restore_trigger_path,
        }
        if owner_id:
            route_kwargs["owner_id"] = owner_id
        if entry_id:
            route_kwargs["entry_id"] = entry_id
        await self._link_manager.async_start_proxy_capture_route(
            **route_kwargs,
        )

    async def async_stop_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process proxy capture route."""

        if owner_id or force:
            await self._link_manager.async_stop_proxy_capture_route(
                owner_id=owner_id,
                force=force,
            )
        else:
            await self._link_manager.async_stop_proxy_capture_route()

    def proxy_capture_route_running(self) -> bool:
        """Return whether the runtime link currently owns one proxy route."""

        return self._link_manager.proxy_capture_route_running()

    async def async_start_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        seed,
    ) -> None:
        """Start one in-process shadow-learning route on the active runtime link."""

        route_kwargs = {
            "collector_ip": collector_ip,
            "listen_port": listen_port,
            "upstream_host": upstream_host,
            "upstream_port": upstream_port,
            "output_path": output_path,
            "seed": seed,
        }
        if owner_id:
            route_kwargs["owner_id"] = owner_id
        if entry_id:
            route_kwargs["entry_id"] = entry_id
        await self._link_manager.async_start_shadow_learning_route(**route_kwargs)

    async def async_stop_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process shadow-learning route."""

        if owner_id or force:
            await self._link_manager.async_stop_shadow_learning_route(
                owner_id=owner_id,
                force=force,
            )
        else:
            await self._link_manager.async_stop_shadow_learning_route()

    def shadow_learning_route_running(self) -> bool:
        """Return whether the runtime link currently owns one shadow route."""

        return self._link_manager.shadow_learning_route_running()

    def shadow_learning_route_ready(self) -> bool:
        """Return whether the active shadow route is ready for cloud control learning."""

        return self._link_manager.shadow_learning_route_ready()

    def shadow_learning_route_status(self) -> dict[str, object]:
        """Return detailed status for the active shadow route."""

        return self._link_manager.shadow_learning_route_status()

    async def async_disconnect_collector_connections(self, *, reason: str = "") -> None:
        """Drop active collector sockets without changing collector settings."""

        await self._link_manager.async_disconnect_collector_connections(reason=reason)

    async def async_refresh(self, *, poll_interval: float | None = None) -> RuntimeSnapshot:
        """Refresh the current runtime snapshot."""

        if not self._link_manager.connected:
            self._runtime_read_state.clear()
            ok = await self._link_manager.async_try_connect(timeout=0.75)
            if not ok:
                collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
                snapshot = self._build_snapshot(
                    extra_values=collector_values,
                    last_error="waiting_for_collector",
                    connected=False,
                )
                self._last_snapshot = snapshot
                return snapshot

        ok = await self._link_manager.async_try_connect(timeout=1.5, require_heartbeat=True)
        if not ok:
            self._runtime_read_state.clear()
            if self._link_manager.connected:
                logger.warning(
                    "Collector heartbeat timed out; resetting stale runtime connection"
                )
                try:
                    await self._async_recover_heartbeat_timeout(timeout=5.0)
                    ok = True
                except Exception as exc:
                    logger.warning("Collector heartbeat recovery failed: %s", exc)
                    self._record_recovery_failure(reason="collector_heartbeat_timeout")
                    collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
                    snapshot = self._build_snapshot(
                        extra_values=collector_values,
                        last_error="collector_heartbeat_timeout",
                        connected=False,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            else:
                collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
                snapshot = self._build_snapshot(
                    extra_values=collector_values,
                    last_error="waiting_for_collector",
                    connected=False,
                )
                self._last_snapshot = snapshot
                return snapshot

        if not ok:
            collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
            snapshot = self._build_snapshot(
                extra_values=collector_values,
                last_error="collector_heartbeat_timeout",
                connected=False,
            )
            self._last_snapshot = snapshot
            return snapshot

        collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                logger.warning("Driver detection failed: %s", detect_error)
                snapshot = self._build_snapshot(extra_values=collector_values, last_error=detect_error)
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
                extra_values=collector_values,
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
                        extra_values=collector_values,
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
                        extra_values=collector_values,
                        last_error=str(retry_exc),
                        connected=False if _should_mark_snapshot_disconnected(retry_exc) else None,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            else:
                logger.warning("Runtime refresh failed: %s", exc)
                self._runtime_read_state.clear()
                snapshot = self._build_snapshot(
                    extra_values=collector_values,
                    last_error=str(exc),
                    connected=False if _should_mark_snapshot_disconnected(exc) else None,
                )
                self._last_snapshot = snapshot
                return snapshot

        self._record_refresh_success()
        snapshot = self._build_snapshot(extra_values={**collector_values, **runtime_values})
        self._last_snapshot = snapshot
        return snapshot

    async def _async_read_collector_runtime_values(
        self,
        *,
        poll_interval: float | None,
    ) -> dict[str, object]:
        """Best-effort collector-side metadata refresh over FC=2 and plain AT helpers."""

        missing = object()
        active_transport = getattr(self._link_manager, "active_transport", missing)
        if active_transport is missing:
            transport = self._link_manager.transport if self._link_manager.connected else None
        else:
            transport = active_transport

        active_at_transport = getattr(self._link_manager, "active_collector_at_transport", missing)
        if active_at_transport is missing:
            at_transport = getattr(self._link_manager, "collector_at_transport", None)
        else:
            at_transport = active_at_transport
        allow_disconnected_at_query = active_at_transport is missing and not self._link_manager.connected

        now_monotonic = asyncio.get_running_loop().time()
        refresh_interval = max(float(poll_interval or 0.0) * 3.0, 30.0)
        if transport is not None and hasattr(transport, "async_send_collector") and (
            not self._collector_runtime_values
            or now_monotonic - self._collector_runtime_last_refresh_monotonic >= refresh_interval
        ):
            try:
                values = await query_runtime_collector_values(SmartEssLocalSession(transport))
            except Exception as exc:
                logger.debug("Collector runtime FC query failed: %s", exc)
            else:
                if values:
                    self._collector_runtime_values = dict(values)
                    self._collector_runtime_last_refresh_monotonic = now_monotonic

        if at_transport is not None and (
            getattr(at_transport, "connected", False)
            or allow_disconnected_at_query
        ) and (
            not self._collector_at_runtime_values
            or now_monotonic - self._collector_at_runtime_last_refresh_monotonic >= refresh_interval
        ):
            try:
                values = await query_runtime_collector_at_values(at_transport)
            except Exception as exc:
                logger.debug("Collector runtime AT query failed: %s", exc)
            else:
                if values:
                    self._collector_at_runtime_values = dict(values)
                    self._collector_at_runtime_last_refresh_monotonic = now_monotonic

        return self._combined_collector_runtime_values()

    def _combined_collector_runtime_values(self) -> dict[str, object]:
        values = dict(self._collector_runtime_values)
        values.update(self._collector_at_runtime_values)
        return values

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

    async def async_set_collector_server_endpoint(
        self,
        endpoint: str,
        *,
        apply_changes: bool = True,
    ) -> dict[str, object]:
        """Stage or apply collector parameter 21 on the local SmartESS management path."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        transport = self._link_manager.transport
        if not hasattr(transport, "async_send_collector"):
            raise RuntimeError("collector_local_management_not_supported")

        normalized_endpoint = _normalize_collector_server_endpoint(endpoint)
        session = SmartEssLocalSession(transport)
        previous_endpoint = await self._async_query_collector_text(session, SET_SERVER_ENDPOINT)
        if previous_endpoint and previous_endpoint != normalized_endpoint:
            self._collector_last_server_endpoint_before_change = previous_endpoint

        set_response = await session.set_collector(SET_SERVER_ENDPOINT, normalized_endpoint)
        if set_response.status != 0 or set_response.parameter != SET_SERVER_ENDPOINT:
            raise RuntimeError(
                f"collector_set_failed:parameter={SET_SERVER_ENDPOINT}:status={set_response.status}"
            )

        readback_endpoint = await self._async_query_collector_text(session, SET_SERVER_ENDPOINT)
        reboot_required = await self._async_query_collector_text(session, QUERY_REBOOT_REQUIRED)

        effective_endpoint = readback_endpoint or normalized_endpoint
        self._collector_runtime_values["collector_server_endpoint"] = effective_endpoint
        if reboot_required:
            self._collector_runtime_values["collector_reboot_required"] = reboot_required
        self._collector_runtime_last_refresh_monotonic = asyncio.get_running_loop().time()

        result: dict[str, object] = {
            "status": "staged",
            "requested_endpoint": normalized_endpoint,
            "readback_endpoint": effective_endpoint,
            "apply_changes": apply_changes,
        }
        if previous_endpoint:
            result["previous_endpoint"] = previous_endpoint
        if reboot_required:
            result["reboot_required"] = reboot_required

        if not apply_changes:
            return result

        apply_response = await session.set_collector(SET_REBOOT_OR_APPLY, "1")
        if apply_response.status != 0 or apply_response.parameter != SET_REBOOT_OR_APPLY:
            raise RuntimeError(
                f"collector_set_failed:parameter={SET_REBOOT_OR_APPLY}:status={apply_response.status}"
            )

        result["status"] = "applied"
        result["warning"] = "collector redirect apply accepted; the current session may disconnect before the next refresh"
        return result

    async def async_apply_collector_changes(self) -> dict[str, object]:
        """Trigger collector apply on parameter 29 without changing parameter 21."""

        return await self._async_execute_collector_system_action(action="apply")

    async def async_reboot_collector(self) -> dict[str, object]:
        """Trigger collector reboot-intent on parameter 29."""

        return await self._async_execute_collector_system_action(action="reboot")

    async def async_rollback_collector_server_endpoint(
        self,
        *,
        apply_changes: bool = True,
    ) -> dict[str, object]:
        """Rollback parameter 21 to the cached endpoint remembered in this runtime session."""

        rollback_endpoint = self.collector_server_endpoint_rollback_target
        if not rollback_endpoint:
            raise RuntimeError("collector_rollback_endpoint_unavailable")

        result = await self.async_set_collector_server_endpoint(
            rollback_endpoint,
            apply_changes=apply_changes,
        )
        result["status"] = "rollback_applied" if apply_changes else "rollback_staged"
        result["rollback_source"] = "session_cached_previous_endpoint"
        result["rollback_endpoint"] = rollback_endpoint
        return result

    async def async_get_collector_server_endpoint_state(self) -> dict[str, object]:
        """Return the live collector endpoint and reboot-required flag from local management."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        transport = self._link_manager.transport
        if not hasattr(transport, "async_send_collector"):
            raise RuntimeError("collector_local_management_not_supported")

        session = SmartEssLocalSession(transport)
        current_endpoint = await self._async_query_collector_text(session, SET_SERVER_ENDPOINT)
        reboot_required = await self._async_query_collector_text(session, QUERY_REBOOT_REQUIRED)

        if current_endpoint:
            self._collector_runtime_values["collector_server_endpoint"] = current_endpoint
        if reboot_required:
            self._collector_runtime_values["collector_reboot_required"] = reboot_required
        self._collector_runtime_last_refresh_monotonic = asyncio.get_running_loop().time()
        return {
            "current_endpoint": current_endpoint,
            "reboot_required": reboot_required,
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

    async def _async_query_collector_text(
        self,
        session: SmartEssLocalSession,
        parameter: int,
    ) -> str:
        response = await session.query_collector(parameter)
        if response.code != 0:
            return ""
        return str(response.text or "").strip().strip("\x00")

    async def _async_execute_collector_system_action(self, *, action: str) -> dict[str, object]:
        """Run collector parameter 29 for apply/reboot intent without changing endpoint state."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        transport = self._link_manager.transport
        if not hasattr(transport, "async_send_collector"):
            raise RuntimeError("collector_local_management_not_supported")

        session = SmartEssLocalSession(transport)
        current_endpoint = await self._async_query_collector_text(session, SET_SERVER_ENDPOINT)
        reboot_required = await self._async_query_collector_text(session, QUERY_REBOOT_REQUIRED)

        apply_response = await session.set_collector(SET_REBOOT_OR_APPLY, "1")
        if apply_response.status != 0 or apply_response.parameter != SET_REBOOT_OR_APPLY:
            raise RuntimeError(
                f"collector_set_failed:parameter={SET_REBOOT_OR_APPLY}:status={apply_response.status}"
            )

        if current_endpoint:
            self._collector_runtime_values["collector_server_endpoint"] = current_endpoint
        self._collector_runtime_values["collector_reboot_required"] = "0"
        self._collector_runtime_last_refresh_monotonic = asyncio.get_running_loop().time()
        return {
            "status": "applied" if action == "apply" else "reboot_triggered",
            "action": action,
            "current_endpoint": current_endpoint,
            "reboot_required_before": reboot_required,
            "warning": "collector system action accepted; the current session may disconnect before the next refresh",
        }

    async def _async_ensure_connected(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> None:
        """Ensure there is an active collector connection, retrying discovery if needed."""

        try:
            await self._link_manager.async_ensure_connected(
                timeout=timeout,
                require_heartbeat=require_heartbeat,
            )
        except ConnectionError as exc:
            if require_heartbeat and _error_code(exc) == "collector_heartbeat_timeout":
                await self._async_recover_heartbeat_timeout(timeout=timeout)
                return
            raise

    async def _async_recover_heartbeat_timeout(self, *, timeout: float) -> None:
        """Drop a stale connected socket and wait for a fresh heartbeat."""

        self._record_recovery_attempt(reason="collector_heartbeat_timeout")
        await self._link_manager.async_reset_connection(reason="collector_heartbeat_timeout")
        await self._link_manager.async_ensure_connected(
            timeout=timeout,
            require_heartbeat=True,
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
        # The overlay merge is applied in _build_snapshot (every refresh, once the
        # collector identity is populated), not here -- at detection the collector is
        # not yet identified, so the device-scope match would fail and never retry.
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

        collector_field_overrides = extra_values or {}
        if collector_field_overrides:
            merged_collector_pn = _prefer_more_complete_collector_pn(
                collector.collector_pn,
                collector_field_overrides.get("collector_pn"),
            )
            if merged_collector_pn and merged_collector_pn != collector.collector_pn:
                collector.collector_pn = merged_collector_pn
                collector.collector_pn_prefix = merged_collector_pn[:1]
                collector.collector_pn_digits = merged_collector_pn[1:]
            collector.smartess_collector_version = str(
                collector_field_overrides.get("smartess_collector_version", collector.smartess_collector_version) or ""
            )
            collector.smartess_protocol_raw_id = str(
                collector_field_overrides.get("smartess_protocol_raw_id", collector.smartess_protocol_raw_id) or ""
            )
            collector.smartess_protocol_asset_id = str(
                collector_field_overrides.get("smartess_protocol_asset_id", collector.smartess_protocol_asset_id) or ""
            )
            collector.smartess_protocol_asset_name = str(
                collector_field_overrides.get("smartess_protocol_asset_name", collector.smartess_protocol_asset_name) or ""
            )
            collector.smartess_protocol_suffix = str(
                collector_field_overrides.get("smartess_protocol_suffix", collector.smartess_protocol_suffix) or ""
            )
            collector.smartess_protocol_profile_key = str(
                collector_field_overrides.get("smartess_protocol_profile_key", collector.smartess_protocol_profile_key) or ""
            )
            collector.smartess_protocol_name = str(
                collector_field_overrides.get("smartess_protocol_name", collector.smartess_protocol_name) or ""
            )
            if collector_field_overrides.get("smartess_device_address") is not None:
                collector.smartess_device_address = int(collector_field_overrides["smartess_device_address"])
            if "collector_vdtu_raw" in collector_field_overrides:
                bridge = parse_collector_vdtu(collector_field_overrides.get("collector_vdtu_raw"))
                collector.collector_virtual_bridge = bridge.is_virtual_bridge
                collector.collector_bridge_kind = bridge.kind
                collector.collector_bridge_version = bridge.version
                collector.collector_bridge_features = bridge.features
                collector.collector_bridge_attributes = bridge.attributes

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
        listener_diagnostics = getattr(self._link_manager, "listener_diagnostics", None)
        if listener_diagnostics is not None:
            values.update(listener_diagnostics())
            if not values.get("collector_listener_last_error"):
                values.pop("collector_listener_last_error", None)
        if collector.last_devcode is not None:
            values["collector_devcode"] = f"0x{collector.last_devcode:04X}"
        if collector.last_udp_reply:
            values["collector_udp_reply"] = collector.last_udp_reply
        if collector.last_udp_reply_from:
            values["collector_udp_reply_from"] = collector.last_udp_reply_from
        if collector.smartess_collector_version:
            values["smartess_collector_version"] = collector.smartess_collector_version
        if collector.smartess_protocol_raw_id:
            values["smartess_protocol_raw_id"] = collector.smartess_protocol_raw_id
        if collector.smartess_protocol_asset_id:
            values["smartess_protocol_asset_id"] = collector.smartess_protocol_asset_id
        if collector.smartess_protocol_asset_name:
            values["smartess_protocol_asset_name"] = collector.smartess_protocol_asset_name
        if collector.smartess_protocol_suffix:
            values["smartess_protocol_suffix"] = collector.smartess_protocol_suffix
        if collector.smartess_protocol_profile_key:
            values["smartess_protocol_profile_key"] = collector.smartess_protocol_profile_key
        if collector.smartess_protocol_name:
            values["smartess_protocol_name"] = collector.smartess_protocol_name
        if collector.smartess_device_address is not None:
            values["smartess_device_address"] = collector.smartess_device_address
        if collector.collector_virtual_bridge:
            values["collector_virtual_bridge"] = True
            if collector.collector_bridge_kind:
                values["collector_bridge_kind"] = collector.collector_bridge_kind
            if collector.collector_bridge_version:
                values["collector_bridge_version"] = collector.collector_bridge_version
            if collector.collector_bridge_features:
                values["collector_bridge_features"] = ", ".join(collector.collector_bridge_features)
            if collector.collector_bridge_attributes:
                values["collector_bridge_attributes"] = ", ".join(
                    f"{key}={value}" for key, value in collector.collector_bridge_attributes
                )
                bridge_attributes = dict(collector.collector_bridge_attributes)
                for key, value_key in (
                    ("uart", "collector_bridge_uart"),
                    ("spacing_ms", "collector_bridge_spacing_ms"),
                    ("queue", "collector_bridge_queue"),
                ):
                    if bridge_attributes.get(key):
                        values[value_key] = bridge_attributes[key]

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

        callback_endpoint = values.get("collector_server_endpoint")
        if callback_endpoint:
            values["collector_callback_owner"] = _callback_owner_label(
                callback_endpoint,
                server_ip=self._connection.server_ip,
                advertised_server_ip=self._connection.effective_advertised_server_ip,
                advertised_tcp_port=self._connection.effective_advertised_tcp_port,
            )
        else:
            values.pop("collector_callback_owner", None)

        signal_strength = values.get("collector_signal_strength")
        if signal_strength is not None:
            values["collector_signal_quality"] = _collector_signal_quality(signal_strength)
        else:
            values.pop("collector_signal_quality", None)

        signal_source = values.get("collector_signal_strength_source")
        if signal_source:
            values["collector_signal_strength_source"] = _collector_signal_source_label(
                signal_source
            )
        else:
            values.pop("collector_signal_strength_source", None)

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

        if self._inverter_overlay_applier is not None and self._inverter is not None:
            # Merge activated device-scoped learned controls into the inverter on every
            # snapshot. This is idempotent (it only appends not-yet-present learned
            # capabilities). It must run here, not only at detection: detection completes
            # before the collector identity is fully populated, so a detection-time scope
            # match can fail and never retry; here the collector is ready and the merge
            # converges, so the learned controls reliably become entities and are writable.
            self._inverter = self._inverter_overlay_applier(self._inverter, collector)

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
