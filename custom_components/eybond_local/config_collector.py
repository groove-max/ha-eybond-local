"""Selected collector capability, session, and endpoint operations."""

from __future__ import annotations

import logging
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any

from .collector.at_runtime import query_runtime_collector_at_values
from .collector.callback_endpoint import home_assistant_callback_endpoint
from .collector.capabilities import (
    parse_esp_collector_hardware_token,
)
from .collector.cloud_family import collector_cloud_family_observation_from_endpoint
from .collector.discovery import async_send_callback_trigger
from .collector.parameter_registry import (
    COLLECTOR_PARAMETER_DEFINITION_BY_ID,
    query_runtime_collector_values,
)
from .collector.smartess_local import (
    QUERY_REBOOT_REQUIRED,
    SET_REBOOT_OR_APPLY,
    SET_SERVER_ENDPOINT,
    SmartEssLocalSession,
)
from .collector.transport import (
    SharedCollectorAtTransport,
    SharedEybondTransport,
)
from .collector_endpoint import (
    inspect_collector_server_endpoint,
)
from .collector_identity import (
    pn_is_same_identity,
    reconcile_pn,
)
from .config_common import (
    _async_timeout,
)
from .config_result_model import (
    _result_collector_capabilities,
    _result_is_virtual_bridge,
    _smartess_collector_firmware_version_for_result,
)
from .connection.admission import ObservedCollectorSession
from .connection.admission_transaction import (
    CollectorAdmissionTransaction,
)
from .connection.recovery.verification import (
    CallbackRecoveryRoute,
)
from .connection.spec_factory import (
    build_connection_spec_from_values,
)
from .const import (
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_DRIVER_HINT,
    CONF_SMARTESS_COLLECTOR_VERSION,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_TCP_PORT,
    DRIVER_HINT_AUTO,
)
from .models import (
    CollectorInfo,
    OnboardingResult,
)
from .support.collector_registry import remember_collector_original_endpoint

logger = logging.getLogger(__name__)


class SelectedCollectorFlowMixin:
    """Selected collector capability, session, and endpoint operations."""

    def _selected_result_is_virtual_bridge(self) -> bool:
        """Return True when the selected onboarding result is a detected bridge.

        Reads the bridge verdict carried from the onboarding hardware token (see
        ``onboarding/eybond.py``). Positive-only and fail-safe: a factory
        collector or an unread token leaves no verdict, so the confirm step
        behaves exactly as before — the runtime path still corrects identity and
        menu gating after the entry runs.
        """

        return _result_is_virtual_bridge(self._selected_result)

    def _reset_scan_progress(self) -> None:
        """Reset scan-progress bookkeeping before one new scan attempt starts."""

        self._scan_task = None
        self._scan_started_monotonic = None
        self._scan_progress_stage = "preparing"
        self._scan_progress_visible = False
        self._ble_last_error = ""
        self._smartess_cloud_assist_mode = ""
        self._smartess_cloud_assist_last_error = ""
        self._smartess_cloud_assist_last_error_code = ""

    def _set_selected_result(self, result: OnboardingResult | None) -> None:
        """Persist the selected onboarding result and reset lazy confirm refresh state."""

        self._selected_result = result
        self._selected_result_collector_capabilities_attempted = False

    def _selected_poll_policy_driver_key(self) -> str:
        """Return the detected driver that owns onboarding poll limits."""

        result = self._selected_result
        if result is not None and result.match is not None:
            return str(result.match.driver_key or DRIVER_HINT_AUTO)
        return str(self._auto_config.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO))

    def _selected_poll_policy_match(self):
        """Return the detected match (model identity) for the poll policy, if any.

        A catalog driver may pick a model-specific policy from it; ``None`` when
        no match is selected yet.
        """

        result = self._selected_result
        return result.match if result is not None else None

    def _selected_collector_ip(self) -> str:
        result = self._selected_result
        if result is None or result.collector is None:
            return ""
        verified_route = self._verified_callback_route_for_result(result)
        if verified_route is not None:
            return verified_route.trigger_target_ip
        return str(result.collector.ip or result.collector.target_ip or "").strip()

    def _verified_callback_route_for_result(
        self, result: OnboardingResult
    ) -> CallbackRecoveryRoute | None:
        """Return the callback route earned by this result's terminal proof.

        The selected scan result still describes the physical TCP observation,
        whose peer may be a router/NAT address. It must never become the
        operational callback target merely because verification completed.
        Conversely, an offered/selected route is only a hint until the same
        transaction returns a strict callback proof for that exact route.
        """

        transaction = self._admission_transaction
        if (
            result is not self._selected_result
            or type(transaction) is not CollectorAdmissionTransaction
        ):
            return None
        route = transaction.request.callback_route
        if type(route) is not CallbackRecoveryRoute or route.invalid_reason():
            return None
        terminal = transaction.terminal_input
        proof = terminal.callback_proof
        if proof is None:
            return None
        if (
            not pn_is_same_identity(terminal.collector_pn, transaction.expected_pn)
            or not pn_is_same_identity(proof.collector_pn, transaction.expected_pn)
            or proof.trigger_target != route.trigger_target
            or proof.advertised_ha_endpoint != route.advertised_ha_endpoint
            or proof.listener_port != route.listener_port
        ):
            return None
        return route

    def _selected_result_is_passive_callback(self) -> bool:
        result = self._selected_result
        if result is None:
            return False
        return self._result_is_passive_callback(result)

    @staticmethod
    def _result_is_passive_callback(result: OnboardingResult) -> bool:
        collector = result.collector
        return bool(
            result.connection_mode == "callback_listener"
            or (collector is not None and collector.source == "callback_listener")
        )

    async def _async_refresh_selected_result_collector_capabilities(self) -> None:
        """Fetch missing collector capability evidence before rendering confirm."""

        selected_result = self._selected_result
        if selected_result is None or selected_result.collector is None:
            return
        if self._selected_result_is_passive_callback():
            return
        if self._selected_result_collector_capabilities_attempted:
            return
        if not self._autodetect_results or selected_result is self._manual_result:
            return
        if _result_collector_capabilities(selected_result).virtual_bridge:
            return

        collector_ip = self._selected_collector_ip()
        if not collector_ip:
            return

        self._selected_result_collector_capabilities_attempted = True

        values = dict(self._auto_connection_defaults(), **self._auto_config)
        spec = build_connection_spec_from_values(
            self._current_connection_type(), values
        )
        collector_pn = self._collector_pn_for_result(selected_result)
        details: dict[str, object] = {}
        payload_transport = SharedEybondTransport(
            host="0.0.0.0",
            port=getattr(spec, "tcp_port", DEFAULT_TCP_PORT),
            request_timeout=min(
                float(getattr(spec, "request_timeout", DEFAULT_REQUEST_TIMEOUT)), 3.0
            ),
            heartbeat_interval=float(
                getattr(spec, "heartbeat_interval", DEFAULT_HEARTBEAT_INTERVAL)
            ),
            collector_ip=collector_ip,
            collector_pn=collector_pn,
        )
        at_transport = SharedCollectorAtTransport(
            host="0.0.0.0",
            port=getattr(spec, "tcp_port", DEFAULT_TCP_PORT),
            request_timeout=min(
                float(getattr(spec, "request_timeout", DEFAULT_REQUEST_TIMEOUT)), 3.0
            ),
            collector_ip=collector_ip,
            collector_pn=collector_pn,
        )
        try:
            await payload_transport.start()
            await at_transport.start()
        except Exception as exc:
            logger.debug(
                "Selected-result collector capability transport unavailable collector_ip=%s error=%s",
                collector_ip,
                exc,
            )
            with suppress(Exception):
                await at_transport.stop()
            with suppress(Exception):
                await payload_transport.stop()
            return

        try:
            async with _async_timeout(4.0):
                collector_parameters = tuple(
                    COLLECTOR_PARAMETER_DEFINITION_BY_ID[parameter]
                    for parameter in (6, 21)
                    if parameter in COLLECTOR_PARAMETER_DEFINITION_BY_ID
                )
                details.update(
                    await query_runtime_collector_values(
                        SmartEssLocalSession(payload_transport),
                        parameters=collector_parameters,
                    )
                )
        except TimeoutError:
            logger.debug(
                "Selected-result collector FC capability refresh timed out collector_ip=%s",
                collector_ip,
            )
        except Exception as exc:
            logger.debug(
                "Selected-result collector FC capability refresh failed collector_ip=%s error=%s",
                collector_ip,
                exc,
            )

        try:
            async with _async_timeout(4.0):
                details.update(await query_runtime_collector_at_values(at_transport))
        except TimeoutError:
            logger.debug(
                "Selected-result collector AT capability refresh timed out collector_ip=%s",
                collector_ip,
            )
        except Exception as exc:
            logger.debug(
                "Selected-result collector AT capability refresh failed collector_ip=%s error=%s",
                collector_ip,
                exc,
            )
        finally:
            with suppress(Exception):
                await at_transport.stop()
            with suppress(Exception):
                await payload_transport.stop()

        hardware_token = parse_esp_collector_hardware_token(
            details.get("collector_hardware_version")
        )
        if not hardware_token.is_bridge:
            return

        collector = selected_result.collector
        collector_info = collector.collector
        if collector_info is None:
            collector_info = CollectorInfo(
                remote_ip=collector.ip or collector.target_ip
            )
            collector.collector = collector_info
        collector_info.collector_virtual_bridge = True
        collector_info.collector_bridge_kind = "esp-collector"
        if hardware_token.version:
            collector_info.collector_bridge_version = hardware_token.version
        endpoint = str(details.get("collector_server_endpoint") or "").strip()
        if endpoint:
            collector_info.collector_server_endpoint = endpoint
        for detail_key, attr_name in (
            ("collector_cloud_family", "collector_cloud_family"),
            ("collector_cloud_family_source", "collector_cloud_family_source"),
            ("collector_cloud_family_confidence", "collector_cloud_family_confidence"),
        ):
            value = str(details.get(detail_key) or "").strip()
            if value:
                setattr(collector_info, attr_name, value)

    def _collector_callback_target_endpoint(self) -> str:
        values = dict(self._auto_connection_defaults(), **self._auto_config)
        spec = build_connection_spec_from_values(
            self._current_connection_type(), values
        )
        template_endpoint = str(
            self._collector_current_server_endpoint
            or self._collector_original_server_endpoint
            or ""
        ).strip()
        return home_assistant_callback_endpoint(
            server_host=spec.effective_advertised_server_ip,
            listener_port=int(
                getattr(spec, "effective_advertised_tcp_port", 0)
                or getattr(spec, "tcp_port", 0)
                or 0
            ),
            template_endpoint=template_endpoint,
        )

    def _collector_original_endpoint_options(self, endpoint: str) -> dict[str, str]:
        """Return sticky option fields for a preserved original collector endpoint."""

        normalized_endpoint = str(endpoint or "").strip()
        if not normalized_endpoint:
            return {}

        profile_key = ""
        try:
            parsed = inspect_collector_server_endpoint(
                normalized_endpoint,
                require_explicit_port=False,
                require_explicit_protocol=False,
            )
        except ValueError:
            parsed = None
        if parsed is not None:
            profile_key = collector_cloud_family_observation_from_endpoint(
                normalized_endpoint
            ).family
            if profile_key == "unknown":
                profile_key = ""

        return {
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: normalized_endpoint,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY: profile_key,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: "config_flow_pre_bind",
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT: datetime.now(
                timezone.utc
            ).isoformat(),
        }

    async def _async_remember_collector_original_endpoint_in_registry(
        self,
        *,
        collector_pn: str,
        endpoint: str,
        options: dict[str, Any],
    ) -> None:
        """Persist the original collector endpoint outside the config entry."""

        if self._selected_result_is_virtual_bridge():
            return
        normalized_pn = str(collector_pn or "").strip()
        normalized_endpoint = str(endpoint or "").strip()
        if not normalized_pn or not normalized_endpoint:
            return
        config_dir = self._config_dir_path()
        await self.hass.async_add_executor_job(
            lambda: remember_collector_original_endpoint(
                config_dir=config_dir,
                collector_pn=normalized_pn,
                original_endpoint_raw=normalized_endpoint,
                cloud_profile_key=str(
                    options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY)
                    or ""
                ),
                source=str(
                    options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE) or ""
                ),
                observed_at=str(
                    options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT)
                    or ""
                ),
                last_seen_ip=self._selected_collector_ip(),
            )
        )

    async def _async_with_selected_collector_session(self):
        collector_ip = self._selected_collector_ip()
        if not collector_ip:
            raise RuntimeError("collector_ip_unavailable")

        values = dict(self._auto_connection_defaults(), **self._auto_config)
        spec = build_connection_spec_from_values(
            self._current_connection_type(), values
        )
        transport = SharedEybondTransport(
            host=spec.server_ip,
            port=spec.tcp_port,
            request_timeout=DEFAULT_REQUEST_TIMEOUT,
            heartbeat_interval=float(spec.heartbeat_interval),
            collector_ip=collector_ip,
        )
        await transport.start()
        try:
            with suppress(Exception):
                await async_send_callback_trigger(
                    bind_ip=spec.server_ip,
                    advertised_server_ip=spec.effective_advertised_server_ip,
                    advertised_server_port=spec.effective_advertised_tcp_port,
                    target_ip=collector_ip,
                    udp_port=spec.udp_port,
                    timeout=1.0,
                    source="config_flow_management_probe",
                )
            connected = await transport.wait_until_connected(timeout=5.0)
            if not connected:
                raise ConnectionError("collector_not_connected")
            await transport.wait_until_heartbeat(timeout=1.5)
            return transport, SmartEssLocalSession(transport)
        except Exception:
            await transport.stop()
            raise

    async def _async_query_selected_collector_text(self, parameter: int) -> str:
        transport, session = await self._async_with_selected_collector_session()
        try:
            response = await session.query_collector(parameter)
            if response.code != 0:
                raise RuntimeError(
                    f"collector_query_failed:parameter={parameter}:code={response.code}"
                )
            return str(response.text or "").strip().strip("\x00")
        finally:
            await transport.stop()

    async def _async_read_selected_collector_server_endpoint(self) -> str:
        endpoint = await self._async_query_selected_collector_text(SET_SERVER_ENDPOINT)
        self._collector_current_server_endpoint = endpoint
        if endpoint and not self._collector_original_server_endpoint:
            self._collector_original_server_endpoint = endpoint
        self._collector_target_server_endpoint = (
            self._collector_callback_target_endpoint()
        )
        return endpoint

    async def _async_bind_selected_collector_to_home_assistant(
        self,
        *,
        allow_refused_endpoint_write: bool = False,
    ) -> None:
        target_endpoint = self._collector_callback_target_endpoint()
        current_endpoint = (
            self._collector_current_server_endpoint
            or await self._async_read_selected_collector_server_endpoint()
        )
        self._collector_target_server_endpoint = target_endpoint
        if current_endpoint == target_endpoint:
            return

        transport, session = await self._async_with_selected_collector_session()
        try:
            set_response = await session.set_collector(
                SET_SERVER_ENDPOINT, target_endpoint
            )
            if (
                set_response.status != 0
                or set_response.parameter != SET_SERVER_ENDPOINT
            ):
                # Modern virtual-bridge firmware accepts the FC=3 param-21
                # endpoint write. Older bridge firmware may refuse it; for a
                # detected bridge that refusal is non-fatal because the mode is
                # HA-only regardless. A factory collector keeps the original hard
                # failure.
                if allow_refused_endpoint_write:
                    logger.debug(
                        "Collector endpoint write refused by a detected bridge "
                        "(parameter=%s status=%s); treating as applied and continuing.",
                        SET_SERVER_ENDPOINT,
                        set_response.status,
                    )
                    return
                raise RuntimeError(
                    f"collector_set_failed:parameter={SET_SERVER_ENDPOINT}:status={set_response.status}"
                )
            readback = await session.query_collector(SET_SERVER_ENDPOINT)
            if readback.code == 0 and str(readback.text or "").strip().strip("\x00"):
                self._collector_current_server_endpoint = (
                    str(readback.text or "").strip().strip("\x00")
                )
            with suppress(Exception):
                await session.query_collector(QUERY_REBOOT_REQUIRED)
            try:
                apply_response = await session.set_collector(SET_REBOOT_OR_APPLY, "1")
            except Exception as exc:
                # Applying a staged endpoint makes the collector drop this TCP
                # session (it reconnects to the new endpoint / reboots), and
                # bridge firmware before the deferred-apply fix closes the
                # socket before the FC=3 ack is flushed. The endpoint write
                # and readback already succeeded above, so for a bridge the
                # lost ack means "applying", not "failed".
                if allow_refused_endpoint_write:
                    logger.debug(
                        "Collector endpoint apply dropped the session on a "
                        "detected bridge (%s); treating as applied.",
                        exc,
                    )
                    return
                raise
            if (
                apply_response.status != 0
                or apply_response.parameter != SET_REBOOT_OR_APPLY
            ):
                if allow_refused_endpoint_write:
                    logger.debug(
                        "Collector endpoint apply refused by a detected bridge "
                        "(parameter=%s status=%s); treating as applied.",
                        SET_REBOOT_OR_APPLY,
                        apply_response.status,
                    )
                    return
                raise RuntimeError(
                    f"collector_set_failed:parameter={SET_REBOOT_OR_APPLY}:status={apply_response.status}"
                )
        finally:
            await transport.stop()

    def _reset_collector_endpoint_binding_state(self) -> None:
        self._collector_original_server_endpoint = ""
        self._collector_current_server_endpoint = ""
        self._collector_endpoint_error = ""
        self._collector_endpoint_bind_applied = False

    @staticmethod
    def _collector_identity_projection(
        result: OnboardingResult | None,
    ) -> tuple[str, bool]:
        """Return the reconciled PN and whether the result contains a conflict.

        ``ObservedCollectorSession`` is the typed admission evidence carried by
        a silent callback scan.  It may be the only place where the exact-session
        FC=2 probe produced a PN, so the older ``CollectorInfo`` projection must
        not make that result disappear from the UI.  Conversely, no source may
        silently override another identity: all non-empty facts must denote the
        same collector before short/full reconciliation is allowed.
        """

        if result is None:
            return "", False

        def _exact_pn(value: object) -> str:
            if type(value) is not str or not value or value != value.strip():
                return ""
            return value

        identities: list[str] = []

        collector_info = (
            result.collector.collector if result.collector is not None else None
        )
        if collector_info is not None:
            collector_pn = _exact_pn(collector_info.collector_pn)
            if collector_pn:
                identities.append(collector_pn)

        observed = result.observed_session
        if type(observed) is ObservedCollectorSession:
            identities.append(observed.collector_pn)

        match_details = result.match.details if result.match is not None else {}
        match_pn = _exact_pn(match_details.get("collector_pn"))
        if match_pn:
            identities.append(match_pn)

        for index, identity in enumerate(identities):
            if any(
                not pn_is_same_identity(identity, candidate)
                for candidate in identities[index + 1 :]
            ):
                return "", True

        collector_pn = ""
        for identity in identities:
            collector_pn = reconcile_pn(collector_pn, identity)
        return collector_pn, False

    def _collector_pn_for_result(self, result: OnboardingResult | None) -> str:
        collector_pn, conflict = self._collector_identity_projection(result)
        return "" if conflict else collector_pn

    def _known_smartess_ble_firmware_version(self, ble_address: str) -> str:
        cached_fw_version = str(
            self._ble_fw_version_by_address.get(ble_address, "") or ""
        ).strip()
        if cached_fw_version:
            return cached_fw_version
        for result in (self._selected_result, self._manual_result):
            fw_version = _smartess_collector_firmware_version_for_result(result)
            if fw_version:
                return fw_version
        return str(
            self._auto_config.get(CONF_SMARTESS_COLLECTOR_VERSION)
            or self._manual_config.get(CONF_SMARTESS_COLLECTOR_VERSION)
            or ""
        ).strip()

    def _smartess_detected_hint_values(
        self, result: OnboardingResult | None
    ) -> tuple[str, str]:
        if result is None:
            return "", ""

        collector_info = (
            result.collector.collector if result.collector is not None else None
        )
        match_details = result.match.details if result.match is not None else {}
        asset_id = str(
            match_details.get("smartess_protocol_asset_id")
            or getattr(collector_info, "smartess_protocol_asset_id", "")
            or ""
        ).strip()
        profile_key = str(
            match_details.get("smartess_profile_key")
            or getattr(collector_info, "smartess_protocol_profile_key", "")
            or ""
        ).strip()
        return asset_id, profile_key
