"""Startup snapshot and persisted inverter-identity bootstrap."""

from __future__ import annotations

from types import SimpleNamespace

from ...collector.capabilities import collector_capability_profile_from_runtime
from ...collector_endpoint import (
    DEFAULT_COLLECTOR_SERVER_PORT,
    DEFAULT_COLLECTOR_SERVER_PROTOCOL,
    format_collector_server_endpoint as format_runtime_collector_server_endpoint,
)
from ...const import (
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_CONNECTION_MODE,
    CONF_CONNECTION_TYPE,
    CONF_DETECTED_DRIVER,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DRIVER_HINT,
    DRIVER_HINT_AUTO,
)
from ...drivers.registry import get_driver
from ...metadata.compiled_detection_catalog import resolve_unique_persisted_model_surface
from ...metadata.profile_loader import load_driver_profile
from ...metadata.register_schema_loader import load_register_schema
from ...models import DetectedInverter, ProbeTarget, RuntimeSnapshot
from .poll_projection import (
    COLLECTOR_POLL_CONTEXT_DETECTION as _COLLECTOR_POLL_CONTEXT_DETECTION,
    RUNTIME_DRIVER_STATE_DRIVER_BOUND as _RUNTIME_DRIVER_STATE_DRIVER_BOUND,
    RUNTIME_DRIVER_STATE_DRIVER_UNBOUND as _RUNTIME_DRIVER_STATE_DRIVER_UNBOUND,
)


class CoordinatorStartupIdentityMixin:
    """Prime startup state without opening a second runtime identity authority."""

    def prime_startup_snapshot(self) -> bool:
        """Seed coordinator data from persisted entry metadata without network I/O.

        Home Assistant setup should not wait for a full inverter detection pass
        just to create collector/runtime entities. This lightweight snapshot
        provides stable collector identity and an explicit detection-pending
        state; the ordinary background refresh will replace it with live data.
        """

        existing_snapshot = self.data if isinstance(self.data, RuntimeSnapshot) else None
        inverter = self._prime_startup_inverter_from_persisted_metadata()
        if (
            existing_snapshot is not None
            and existing_snapshot.values
            and (existing_snapshot.inverter is not None or inverter is None)
        ):
            return False

        connection = self._connection_spec
        persisted_capabilities = collector_capability_profile_from_runtime(
            data=dict(self.config_entry.data or {}),
            options=dict(self.config_entry.options or {}),
        )
        persisted_bridge_version = str(
            self.config_entry.options.get(
                "collector_bridge_version",
                self.config_entry.data.get("collector_bridge_version", ""),
            )
            or ""
        ).strip()
        collector = SimpleNamespace(
            remote_ip=str(getattr(connection, "collector_ip", "") or ""),
            collector_pn=str(getattr(connection, "collector_pn", "") or ""),
            profile_name="",
            smartess_protocol_name="",
            smartess_protocol_asset_name="",
            smartess_collector_version="",
            collector_cloud_family=str(
                getattr(connection, "collector_cloud_family", "") or ""
            ),
            collector_virtual_bridge=persisted_capabilities.virtual_bridge,
            collector_bridge_kind=(
                "esp-collector" if persisted_capabilities.virtual_bridge else ""
            ),
            collector_bridge_version=persisted_bridge_version,
        )
        values: dict[str, object] = dict(getattr(existing_snapshot, "values", {}) or {})
        persisted_detection_status = str(
            getattr(inverter, "details", {}).get("runtime_detection_status", "")
            if inverter is not None
            else ""
        ).strip()
        values.update({
            "connection_type": self.config_entry.data.get(
                CONF_CONNECTION_TYPE,
                "eybond",
            ),
            "collector_operation_mode": self.collector_operation_mode,
            "control_mode": self.control_mode,
            "detection_confidence": self.detection_confidence,
            "runtime_driver_state": (
                _RUNTIME_DRIVER_STATE_DRIVER_BOUND
                if inverter is not None
                else _RUNTIME_DRIVER_STATE_DRIVER_UNBOUND
            ),
            "collector_identity_binding_required": self._identity_binding_required_flag(),
            "runtime_detection_status": (
                persisted_detection_status or "detecting_inverter"
            ),
            "collector_poll_context": _COLLECTOR_POLL_CONTEXT_DETECTION,
            "collector_poll_mode": self._configured_poll_mode(),
            "collector_poll_current_interval_seconds": self._configured_poll_interval_seconds(),
            "collector_poll_interval_configured_seconds": self._configured_poll_interval_seconds(),
            "collector_poll_manual_interval_seconds": self._configured_poll_interval_seconds(),
            "collector_poll_duration_ms": 0,
            "collector_poll_utilization_percent": 0,
            "collector_poll_recommended_min_interval_seconds": self._configured_poll_interval_seconds(),
            "last_error": "startup_detection_pending",
        })
        if inverter is not None:
            values["effective_variant_key"] = inverter.variant_key
            values["effective_profile_name"] = inverter.profile_name
            values["effective_register_schema_name"] = inverter.register_schema_name
            values["effective_inverter_capability_count"] = len(inverter.capabilities)
        connection_mode = self.config_entry.data.get(CONF_CONNECTION_MODE, "")
        if connection_mode:
            values["connection_mode"] = connection_mode
        if collector.remote_ip:
            values["collector_remote_ip"] = collector.remote_ip
            values["configured_collector_ip"] = collector.remote_ip
        if collector.collector_pn:
            values["collector_pn"] = collector.collector_pn
        if collector.collector_cloud_family:
            values["collector_cloud_family"] = collector.collector_cloud_family
        if collector.collector_virtual_bridge:
            values["collector_virtual_bridge"] = True
            values["collector_bridge_kind"] = "esp-collector"
            if collector.collector_bridge_version:
                values["collector_bridge_version"] = collector.collector_bridge_version

        endpoint = str(
            self.config_entry.options.get(
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
                self.config_entry.data.get("collector_server_endpoint", ""),
            )
            or ""
        ).strip()
        if not endpoint and getattr(connection, "server_ip", ""):
            endpoint = format_runtime_collector_server_endpoint(
                server_host=getattr(connection, "effective_advertised_server_ip", "")
                or getattr(connection, "server_ip", ""),
                server_port=getattr(connection, "effective_advertised_tcp_port", 0)
                or getattr(connection, "tcp_port", DEFAULT_COLLECTOR_SERVER_PORT),
                server_protocol=DEFAULT_COLLECTOR_SERVER_PROTOCOL,
            )
        if endpoint:
            values["collector_server_endpoint"] = endpoint

        snapshot = RuntimeSnapshot(
            connected=True,
            collector=collector,
            inverter=inverter,
            values=values,
        )
        if endpoint:
            snapshot.set_collector_server_endpoint(endpoint)
        try:
            snapshot.last_error = "startup_detection_pending"
        except Exception:
            pass
        self.data = snapshot
        self._cached_effective_metadata = None
        return True

    def _seed_runtime_from_persisted_inverter_metadata(self) -> None:
        """Pass a confirmed persisted inverter binding into the runtime manager."""

        inverter = self._prime_startup_inverter_from_persisted_metadata()
        if inverter is None:
            return
        driver_key = str(getattr(inverter, "driver_key", "") or "").strip()
        if not driver_key:
            return
        try:
            driver = get_driver(driver_key)
        except KeyError:
            return
        setter = getattr(self._runtime, "set_initial_inverter_binding", None)
        if callable(setter):
            setter(driver, inverter)

    def _on_runtime_inverter_detected(
        self,
        _driver: object,
        inverter: DetectedInverter,
    ) -> None:
        """Persist a runtime-confirmed inverter before the first value read.

        Detection and runtime reading are separate phases. Some links can identify
        the inverter and then time out on the first full poll; without persisting
        the detected identity immediately, the reload requested for late identity
        can recreate the entry as collector-only.
        """

        self.hass.async_create_task(
            self._async_remember_detected_inverter_identity(inverter)
        )

    async def _async_remember_detected_inverter_identity(
        self,
        inverter: DetectedInverter,
    ) -> None:
        collector = (
            self.data.collector if isinstance(self.data, RuntimeSnapshot) else None
        )
        snapshot = RuntimeSnapshot(
            connected=True,
            collector=collector,
            inverter=inverter,
            values={
                "runtime_detection_status": "autodetected_high_confidence",
            },
        )
        await self._async_remember_runtime_identity(snapshot)

    def _prime_startup_inverter_from_persisted_metadata(self) -> DetectedInverter | None:
        """Build a lightweight inverter identity from persisted metadata, if available.

        Entity platforms are constructed once during setup. When startup uses a
        collector-only primed snapshot, writable capability entities would be
        skipped until a later entry reload. If the entry already carries a
        confirmed inverter identity/effective metadata from an earlier runtime
        detection, expose that metadata immediately without waiting for network I/O.
        The live refresh replaces this lightweight object with the real probe
        result.
        """

        detected_model = str(
            self.config_entry.data.get(CONF_DETECTED_MODEL) or ""
        ).strip()
        detected_serial = str(
            self.config_entry.data.get(CONF_DETECTED_SERIAL) or ""
        ).strip()
        if not (detected_model or detected_serial):
            return None

        snapshot = self.effective_metadata_snapshot
        profile_name = str(getattr(snapshot, "profile_name", "") or "").strip()
        register_schema_name = str(
            getattr(snapshot, "register_schema_name", "") or ""
        ).strip()
        variant_key = str(getattr(snapshot, "variant_key", "") or "default").strip()

        catalog_identity_source = ""
        catalog_surface = None
        driver_key = str(
            self.config_entry.options.get(
                CONF_DRIVER_HINT,
                self.config_entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
            )
            or ""
        ).strip()
        if not driver_key or driver_key == DRIVER_HINT_AUTO:
            driver_key = str(
                self.config_entry.data.get(CONF_DETECTED_DRIVER) or ""
            ).strip()
        if not getattr(snapshot, "is_valid", False):
            if self.detection_confidence != "high" or not detected_model:
                return None
            catalog_resolution = resolve_unique_persisted_model_surface(detected_model)
            if catalog_resolution is None:
                return None
            _descriptor, surface = catalog_resolution
            if (
                driver_key
                and driver_key != DRIVER_HINT_AUTO
                and driver_key != surface.driver_key
            ):
                return None
            driver_key = surface.driver_key
            profile_name = surface.profile_name
            register_schema_name = surface.register_schema_name
            variant_key = surface.variant_key
            catalog_identity_source = "persisted_detected_model"
            catalog_surface = surface

        driver = None
        if driver_key and driver_key != DRIVER_HINT_AUTO:
            try:
                driver = get_driver(driver_key)
            except KeyError:
                driver = None
        if driver is None and profile_name:
            try:
                profile = load_driver_profile(profile_name)
            except Exception:
                profile = None
            if profile is not None:
                driver_key = str(getattr(profile, "driver_key", "") or "").strip()
                try:
                    driver = get_driver(driver_key) if driver_key else None
                except KeyError:
                    driver = None
        if driver is None:
            return None

        if not profile_name:
            profile_name = str(getattr(driver, "profile_name", "") or "").strip()
        if not register_schema_name:
            register_schema_name = str(
                getattr(driver, "register_schema_name", "") or ""
            ).strip()

        profile = None
        if profile_name:
            try:
                profile = load_driver_profile(profile_name)
            except Exception:
                return None
        elif catalog_surface is None or not catalog_surface.read_only:
            return None
        if catalog_identity_source:
            try:
                register_schema = load_register_schema(register_schema_name)
            except Exception:
                return None
            if (
                (
                    profile is not None
                    and str(getattr(profile, "driver_key", "") or "").strip()
                    != driver_key
                )
                or str(getattr(register_schema, "driver_key", "") or "").strip()
                != driver_key
            ):
                return None

        probe_targets = tuple(getattr(driver, "probe_targets", ()) or ())
        probe_target = (
            probe_targets[0]
            if probe_targets
            else ProbeTarget(devcode=0, collector_addr=0, device_addr=0)
        )
        return DetectedInverter(
            driver_key=str(getattr(driver, "key", "") or driver_key),
            protocol_family=str(
                getattr(profile, "protocol_family", "")
                or getattr(driver, "key", "")
                or driver_key
            ),
            model_name=detected_model,
            serial_number=detected_serial,
            probe_target=probe_target,
            variant_key=variant_key or "default",
            details={
                "runtime_detection_status": (
                    "persisted_model_probe_degraded"
                    if catalog_identity_source
                    else "startup_persisted_identity"
                ),
                "detection_confidence": self.detection_confidence,
                **(
                    {"identity_source": catalog_identity_source}
                    if catalog_identity_source
                    else {}
                ),
            },
            profile_name=profile_name,
            register_schema_name=register_schema_name,
            capability_groups=tuple(getattr(profile, "groups", ()) or ()),
            capabilities=tuple(getattr(profile, "capabilities", ()) or ()),
            capability_presets=tuple(getattr(profile, "presets", ()) or ()),
        )



__all__ = ["CoordinatorStartupIdentityMixin"]
