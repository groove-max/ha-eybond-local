"""Diagnostic command execution lifecycle for the runtime coordinator."""

from __future__ import annotations

from pathlib import Path

from ...const import CONF_DRIVER_HINT, DRIVER_HINT_AUTO
from ...support.diagnostic_export import export_diagnostic_run
from ...support.diagnostic_projection import build_runtime_transport_debug
from ...support.diagnostic_runner import DiagnosticRuntimeContext, run_scenario


class CoordinatorDiagnosticsMixin:
    """Serialize diagnostics against the coordinator runtime lock."""

    async def _async_cancel_diagnostic_run(self) -> None:
        """Cancel any in-flight diagnostic command run (called on unload)."""

        await self._diagnostic_flight.cancel()

    async def async_run_diagnostic_commands(
        self,
        *,
        commands: str,
        stop_on_error: bool = True,
        operation_timeout: float | None = None,
        integration_version: str = "",
        confirm_write: bool = False,
        publish_download_copy: bool = False,
    ) -> dict:
        """Run one diagnostic command scenario against the shared collector link.

        Only one scenario runs per config entry at a time; normal polling is
        quiesced while the run holds the transport. Permanent config-entry
        settings (driver hint, probe target, detection snapshot) are never
        modified. Scenarios that write to the device require ``confirm_write``.
        """

        async def _factory() -> dict:
            context = self._build_diagnostic_context(
                stop_on_error=stop_on_error,
                operation_timeout=operation_timeout,
                integration_version=integration_version,
                confirm_write=confirm_write,
            )
            return await self._async_execute_diagnostic(
                commands,
                context,
                publish_download_copy=publish_download_copy,
            )

        return await self._diagnostic_flight.run(
            _factory,
            on_start=self._mark_diagnostic_active,
            on_finish=self._mark_diagnostic_idle,
        )

    def _mark_diagnostic_active(self) -> None:
        self._diagnostic_active = True

    def _mark_diagnostic_idle(self) -> None:
        self._diagnostic_active = False

    @property
    def support_package_export_running(self) -> bool:
        """Return whether this entry is currently building a support archive."""

        return self._support_package_active or self._support_package_flight.running

    def _mark_support_package_active(self) -> None:
        self._support_package_active = True
        self._publish_tooling_values(
            support_package_export_running=True,
            support_package_export_status="running",
            local_metadata_status="Support archive export running",
        )

    def _mark_support_package_idle(self) -> None:
        self._support_package_active = False
        self._publish_tooling_values(
            support_package_export_running=False,
            support_package_export_status="idle",
        )

    def _build_diagnostic_context(
        self,
        *,
        stop_on_error: bool,
        operation_timeout: float | None,
        integration_version: str,
        confirm_write: bool = False,
    ) -> DiagnosticRuntimeContext:
        snapshot = self.data
        inverter = snapshot.inverter if snapshot is not None else None
        transport = self._diagnostic_link_transport()
        driver_hint = self.config_entry.options.get(
            CONF_DRIVER_HINT,
            self.config_entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
        )

        def _is_connected() -> bool:
            return bool(transport is not None and getattr(transport, "connected", False))

        return DiagnosticRuntimeContext(
            transport=transport,
            active_driver_key=inverter.driver_key if inverter is not None else None,
            active_probe_target=inverter.probe_target if inverter is not None else None,
            configured_driver_hint=driver_hint,
            driver_default_probe_target=self._diagnostic_default_probe_target,
            is_connected=_is_connected,
            entry_id=self.config_entry.entry_id,
            integration_version=integration_version,
            catalog_detection=self._diagnostic_catalog_detection(),
            runtime_debug=build_runtime_transport_debug(transport),
            default_stop_on_error=stop_on_error,
            default_operation_timeout=operation_timeout,
            confirm_write=confirm_write,
        )

    def _diagnostic_link_transport(self):
        accessor = getattr(self._runtime, "diagnostic_link_transport", None)
        if callable(accessor):
            return accessor()
        return None

    @staticmethod
    def _diagnostic_default_probe_target(driver_key: str):
        try:
            from ...drivers.registry import get_driver

            driver = get_driver(driver_key)
        except KeyError:
            return None
        targets = getattr(driver, "probe_targets", ())
        return targets[0] if targets else None

    def _diagnostic_catalog_detection(self) -> dict:
        try:
            snapshot = self.effective_metadata_snapshot
            if snapshot is None:
                return {}
            return {
                "candidate_keys": list(getattr(snapshot, "candidate_keys", ()) or ()),
                "surface_key": getattr(snapshot, "surface_key", "") or "",
                "evidence_fingerprint": getattr(snapshot, "evidence_fingerprint", "")
                or "",
            }
        except Exception:  # noqa: BLE001 - diagnostic context must never block a run
            return {}

    async def _async_execute_diagnostic(
        self,
        commands: str,
        context: DiagnosticRuntimeContext,
        *,
        publish_download_copy: bool = False,
    ) -> dict:
        async with self._runtime_operation_lock:
            result = await run_scenario(commands, context)
        result.context["runtime_debug_after"] = build_runtime_transport_debug(
            getattr(context, "transport", None)
        )
        config_dir = Path(self.hass.config.config_dir)
        entry_id = self.config_entry.entry_id
        export = await self.hass.async_add_executor_job(
            lambda: export_diagnostic_run(
                config_dir=config_dir,
                entry_id=entry_id,
                result=result,
                publish_download_copy=publish_download_copy,
            )
        )
        return {
            "success": result.success,
            "output": result.output,
            "results": result.results,
            "context": result.context,
            "started_at": result.started_at,
            "finished_at": result.finished_at,
            "result_path": str(export.result_path),
            "download_url": export.download_url,
        }



__all__ = ["CoordinatorDiagnosticsMixin"]
