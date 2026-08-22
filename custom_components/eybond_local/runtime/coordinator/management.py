"""Inverter writes and collector-management actions for the coordinator."""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from typing import Any

from homeassistant.util import dt as dt_util

from ...const import (
    CONF_ENDPOINT_CONTROL_POLICY,
    CONF_ENDPOINT_WRITTEN_AT,
    CONF_ENDPOINT_WRITTEN_VALUE,
    CONTROL_MODE_FULL,
    ENDPOINT_CONTROL_EXTERNAL,
    ENDPOINT_CONTROL_INTEGRATION_MANAGED,
)
from .endpoint_projection import (
    format_collector_server_endpoint as _format_collector_server_endpoint,
    normalize_preserved_collector_server_endpoint as _normalize_preserved_collector_server_endpoint,
)


class CoordinatorManagementMixin:
    """Serialize writes through the coordinator's existing runtime authorities."""

    async def async_write_capability(self, capability_key: str, value: Any) -> Any:
        """Write one inverter capability and refresh coordinator state."""

        inverter = self.data.inverter
        if inverter is None:
            raise RuntimeError("inverter_not_detected")
        capability = inverter.get_capability(capability_key)
        if not self.can_expose_capability(capability):
            raise PermissionError(
                f"capability_control_disabled:{capability.key}:{self.controls_reason}"
            )
        try:
            # Serialize the control write against the polling loop: both take
            # _runtime_operation_lock so a write and a refresh never interleave
            # Modbus frames on the shared transport (which could cross-correlate
            # the write read-back with a poll response on a safety-critical
            # write). The follow-up refresh is only scheduled here, so it takes
            # the lock later in its own task — no re-entrancy.
            async with self._runtime_operation_lock:
                written_value = await self._runtime.async_write_capability(capability_key, value)
        except Exception:
            await self.async_request_refresh()
            raise
        await self.async_request_refresh()
        return written_value

    async def async_apply_preset(self, preset_key: str) -> dict[str, object]:
        """Apply one declarative preset and refresh coordinator state."""

        inverter = self.data.inverter
        if inverter is None:
            raise RuntimeError("inverter_not_detected")
        preset = inverter.get_capability_preset(preset_key)
        if not self.can_expose_preset(preset):
            raise PermissionError(
                f"preset_control_disabled:{preset.key}:{self.controls_reason}"
            )
        try:
            # Serialize against the polling loop on the shared transport (see
            # async_write_capability for the rationale).
            async with self._runtime_operation_lock:
                result = await self._runtime.async_apply_preset(preset_key)
        except Exception:
            await self.async_request_refresh()
            raise
        await self.async_request_refresh()
        return result

    async def async_sync_inverter_clock(self) -> dict[str, str]:
        """Write the current Home Assistant local date/time into the inverter clock."""

        now = dt_util.now().replace(microsecond=0)
        date_value = now.strftime("%Y-%m-%d")
        time_value = now.strftime("%H:%M:%S")

        await self.async_write_capability("inverter_date_write", date_value)
        await self.async_write_capability("inverter_time_write", time_value)

        return {
            "inverter_date": date_value,
            "inverter_time": time_value,
        }

    @contextlib.asynccontextmanager
    async def _collector_endpoint_operation(self, operation_kind: str):
        """Own THE per-entry endpoint-operation authority for one transient write.

        Acquired fail-closed (no wait) BEFORE the first wire/persistence side
        effect; on a busy entry a neutral ``collector_endpoint_operation_busy`` is
        raised BEFORE anything is written, and the active owner keeps running. The
        token is released on EXACT match in ``finally`` (cancellation included),
        never dropping another owner.
        """

        from ...connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY,
            COLLECTOR_ENDPOINT_OPERATION_BUSY,
        )

        entry_id = self.config_entry.entry_id
        outcome = COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.acquire(entry_id, operation_kind)
        if not outcome.acquired:
            raise RuntimeError(COLLECTOR_ENDPOINT_OPERATION_BUSY)
        try:
            yield outcome.token
        finally:
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.release(entry_id, outcome.token)

    async def async_set_collector_server_endpoint(
        self,
        *,
        server_host: str,
        server_port: int,
        server_protocol: str = "TCP",
        apply_changes: bool = True,
        confirm_redirect: bool = False,
    ) -> dict[str, object]:
        """Stage or apply collector parameter 21 behind an explicit full-control gate."""

        if self.control_mode != CONTROL_MODE_FULL:
            raise PermissionError(
                f"collector_control_disabled:{self.control_mode}:{self.controls_reason}"
            )
        if not confirm_redirect:
            raise ValueError("collector_server_reconfig_requires_confirmation")

        endpoint = _format_collector_server_endpoint(
            server_host=server_host,
            server_port=server_port,
            server_protocol=server_protocol,
        )
        return await self.async_set_raw_collector_server_endpoint(
            endpoint=endpoint,
            apply_changes=apply_changes,
            confirm_redirect=True,
        )

    async def async_set_raw_collector_server_endpoint(
        self,
        *,
        endpoint: str,
        apply_changes: bool = True,
        confirm_redirect: bool = False,
    ) -> dict[str, object]:
        """Stage or apply collector parameter 21 using the caller's raw endpoint shape."""

        if self.control_mode != CONTROL_MODE_FULL:
            raise PermissionError(
                f"collector_control_disabled:{self.control_mode}:{self.controls_reason}"
            )
        if not confirm_redirect:
            raise ValueError("collector_server_reconfig_requires_confirmation")
        from ...connection.collector_endpoint_operation import (
            OPERATION_MANUAL_ENDPOINT_WRITE,
        )

        async with self._collector_endpoint_operation(OPERATION_MANUAL_ENDPOINT_WRITE):
            lock_code = self.collector_configuration_lock_code()
            if lock_code is not None:
                raise RuntimeError(lock_code)

            normalized_endpoint = _normalize_preserved_collector_server_endpoint(endpoint)
            await self._async_prepare_home_assistant_callback_listener(normalized_endpoint)
            result = await self._runtime.async_set_collector_server_endpoint(
                normalized_endpoint,
                apply_changes=apply_changes,
            )
            if not apply_changes:
                self._publish_snapshot_values(
                    collector_callback_endpoint_pending=normalized_endpoint,
                    collector_callback_endpoint_pending_apply_required=True,
                )
                await self.async_request_refresh()
            else:
                self._publish_snapshot_values(
                    collector_callback_endpoint_pending=None,
                    collector_callback_endpoint_pending_apply_required=None,
                )
            return result

    async def async_bind_collector_to_home_assistant(
        self,
        *,
        confirm_redirect: bool = False,
    ) -> dict[str, object]:
        """Move the collector callback endpoint back to this Home Assistant listener."""

        self._raise_if_high_level_collector_actions_disabled()
        if not confirm_redirect:
            raise ValueError("collector_bind_home_assistant_requires_confirmation")
        from ...connection.collector_endpoint_operation import OPERATION_ENDPOINT_BIND

        async with self._collector_endpoint_operation(OPERATION_ENDPOINT_BIND):
            target_endpoint = self.collector_callback_target_endpoint
            current_endpoint = self.data.collector_server_endpoint
            if current_endpoint == target_endpoint:
                # Already pointing at Home Assistant and no write happens: nothing
                # was earned, so NO axis changes of any kind. Since Batch 8 the
                # bind action records only endpoint-write FACTS; the connection
                # strategy changes exclusively through the verified transition
                # authority (a bind is not a reconnect proof).
                self._publish_snapshot_values(
                    collector_callback_endpoint_pending=None,
                    collector_callback_endpoint_pending_apply_required=None,
                )
                return {
                    "status": "already_bound",
                    "requested_endpoint": target_endpoint,
                    "readback_endpoint": target_endpoint,
                    "target_role": "home_assistant",
                }

            await self._async_prepare_home_assistant_callback_listener(target_endpoint)
            result = await self._runtime.async_set_collector_server_endpoint(
                target_endpoint,
                apply_changes=True,
            )
            result["target_role"] = "home_assistant"
            # A successful apply means the integration wrote the endpoint: it now
            # manages it, with recorded write provenance. That is a FACT about the
            # write — the connection strategy is deliberately NOT touched here:
            # only the verified transition authority may change it, after a real
            # reconnect proof.
            written_value = str(
                result.get("readback_endpoint")
                or result.get("requested_endpoint")
                or target_endpoint
            )
            self._persist_connection_axes(
                {
                    CONF_ENDPOINT_CONTROL_POLICY: ENDPOINT_CONTROL_INTEGRATION_MANAGED,
                    CONF_ENDPOINT_WRITTEN_VALUE: written_value,
                    CONF_ENDPOINT_WRITTEN_AT: datetime.now(timezone.utc).isoformat(),
                }
            )
            self._publish_snapshot_values(
                collector_callback_endpoint_pending=None,
                collector_callback_endpoint_pending_apply_required=None,
            )
            return result

    async def async_apply_collector_changes(
        self,
        *,
        confirm_restart: bool = False,
    ) -> dict[str, object]:
        """Apply staged collector-side config changes behind an explicit full-control gate."""

        self._raise_if_high_level_collector_actions_disabled()
        if not confirm_restart:
            raise ValueError("collector_apply_requires_confirmation")
        from ...connection.collector_endpoint_operation import (
            OPERATION_COLLECTOR_SYSTEM_ACTION,
        )

        # CP2C: a public apply is a route-affecting collector action -> typed
        # refuse BEFORE the apply when another endpoint operation owns the entry.
        async with self._collector_endpoint_operation(OPERATION_COLLECTOR_SYSTEM_ACTION):
            result = await self._runtime.async_apply_collector_changes()
            self._publish_snapshot_values(
                collector_callback_endpoint_pending=None,
                collector_callback_endpoint_pending_apply_required=None,
            )
            return result

    async def async_trigger_collector_rediscovery(self) -> dict[str, object]:
        """Send one explicit bootstrap discovery probe to recover collector connectivity."""

        lock_code = self.collector_configuration_lock_code()
        if lock_code in {
            "collector_configuration_proxy_transition_active",
            "collector_configuration_proxy_session_active",
        }:
            raise RuntimeError(lock_code)
        from ...connection.collector_endpoint_operation import (
            OPERATION_COLLECTOR_SYSTEM_ACTION,
        )

        # CP2C: rediscovery sends a UDP set>server trigger -> typed refuse BEFORE
        # the UDP when another endpoint operation owns the entry.
        async with self._collector_endpoint_operation(OPERATION_COLLECTOR_SYSTEM_ACTION):
            target_endpoint = self.collector_callback_target_endpoint
            if target_endpoint:
                await self._async_prepare_home_assistant_callback_listener(target_endpoint)

            result = await self._runtime.async_trigger_reverse_discovery()
            result.setdefault("target_role", "bootstrap")
            result["collector_callback_target_endpoint"] = target_endpoint
            await self.async_request_refresh()
            return result

    async def async_reboot_collector(
        self,
        *,
        confirm_restart: bool = False,
    ) -> dict[str, object]:
        """Trigger one collector reboot-intent action behind an explicit full-control gate."""

        self._raise_if_high_level_collector_actions_disabled()
        if not confirm_restart:
            raise ValueError("collector_reboot_requires_confirmation")
        from ...connection.collector_endpoint_operation import (
            OPERATION_COLLECTOR_SYSTEM_ACTION,
        )

        # CP2C: a public reboot is route-affecting -> typed refuse BEFORE the
        # reboot when another endpoint operation owns the entry.
        async with self._collector_endpoint_operation(OPERATION_COLLECTOR_SYSTEM_ACTION):
            return await self._runtime.async_reboot_collector()

    async def async_query_collector_parameters(
        self,
        parameters: tuple[int, ...],
    ) -> dict[int, str]:
        """Read collector settings through the runtime-owned exact session."""

        query = getattr(self._runtime, "async_query_collector_parameters", None)
        if not callable(query):
            raise RuntimeError("collector_local_management_not_supported")
        return await query(parameters)

    async def async_set_collector_wifi_credentials(
        self,
        *,
        ssid: str,
        password: str,
        ssid_parameter: int,
        password_parameter: int,
    ) -> str:
        """Apply Wi-Fi settings under the shared collector-operation authority."""

        self._raise_if_high_level_collector_actions_disabled()
        writer = getattr(self._runtime, "async_set_collector_wifi_credentials", None)
        if not callable(writer):
            raise RuntimeError("collector_local_management_not_supported")
        from ...connection.collector_endpoint_operation import (
            OPERATION_COLLECTOR_SYSTEM_ACTION,
        )

        async with self._collector_endpoint_operation(
            OPERATION_COLLECTOR_SYSTEM_ACTION
        ):
            return await writer(
                ssid=ssid,
                password=password,
                ssid_parameter=ssid_parameter,
                password_parameter=password_parameter,
            )

    async def async_set_collector_uart_baudrate(self, baudrate: str) -> str:
        """Apply UART speed under the shared collector-operation authority."""

        self._raise_if_high_level_collector_actions_disabled()
        writer = getattr(self._runtime, "async_set_collector_uart_baudrate", None)
        if not callable(writer):
            raise RuntimeError("collector_local_management_not_supported")
        from ...connection.collector_endpoint_operation import (
            OPERATION_COLLECTOR_SYSTEM_ACTION,
        )

        async with self._collector_endpoint_operation(
            OPERATION_COLLECTOR_SYSTEM_ACTION
        ):
            return await writer(baudrate)

    async def async_rollback_collector_server_endpoint(
        self,
        *,
        apply_changes: bool = True,
        confirm_redirect: bool = False,
    ) -> dict[str, object]:
        """Rollback collector parameter 21 to the remembered original external endpoint."""

        self._raise_if_high_level_collector_actions_disabled()
        if not confirm_redirect:
            raise ValueError("collector_rollback_requires_confirmation")
        from ...connection.collector_endpoint_operation import OPERATION_ENDPOINT_ROLLBACK

        async with self._collector_endpoint_operation(OPERATION_ENDPOINT_ROLLBACK):
            rollback_endpoint = self.collector_server_endpoint_rollback_target
            if not rollback_endpoint:
                raise RuntimeError("collector_rollback_endpoint_unavailable")

            runtime_target = str(
                getattr(self._runtime, "collector_server_endpoint_rollback_target", "") or ""
            ).strip()
            rollback_source = (
                "session_cached_previous_endpoint"
                if runtime_target and runtime_target == rollback_endpoint
                else "remembered_original_endpoint"
            )

            result = await self._runtime.async_set_collector_server_endpoint(
                rollback_endpoint,
                apply_changes=apply_changes,
            )
            result["status"] = "rollback_applied" if apply_changes else "rollback_staged"
            result["rollback_source"] = rollback_source
            result["rollback_endpoint"] = rollback_endpoint
            result.setdefault("target_role", "smartess")
            if not apply_changes:
                # Staging must not change durable axes: nothing was written yet.
                self._publish_snapshot_values(
                    collector_callback_endpoint_pending=rollback_endpoint,
                    collector_callback_endpoint_pending_apply_required=True,
                )
                await self.async_request_refresh()
            else:
                # A successful rollback hands endpoint control back to the external
                # target and clears the write provenance. That is the endpoint-write
                # FACT only — the connection strategy is deliberately NOT touched
                # (Batch 8): a rollback is not a callback proof, and only the
                # verified transition authority may change the strategy.
                self._persist_connection_axes(
                    {CONF_ENDPOINT_CONTROL_POLICY: ENDPOINT_CONTROL_EXTERNAL},
                    clear=(CONF_ENDPOINT_WRITTEN_VALUE, CONF_ENDPOINT_WRITTEN_AT),
                )
                self._publish_snapshot_values(
                    collector_callback_endpoint_pending=None,
                    collector_callback_endpoint_pending_apply_required=None,
                )
            return result



__all__ = ["CoordinatorManagementMixin"]
