"""Config-entry persistence and runtime identity synchronization."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any

from ..const import (
    COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE,
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_MODE,
    CONF_CONTROL_MODE,
    CONF_DETECTED_DRIVER,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DETECTION_CONFIDENCE,
    CONF_SERVER_IP,
    CONF_SMARTESS_COLLECTOR_VERSION,
    CONF_SMARTESS_DEVICE_ADDRESS,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
    CONTROL_MODE_READ_ONLY,
    DEFAULT_CONTROL_MODE,
)
from ..drivers.registry import serial_is_stable
from ..metadata.effective_metadata_snapshot import effective_metadata_snapshot_from_dict
from ..models import RuntimeSnapshot
from ..naming import installation_title, legacy_installation_titles
from ..support.collector_registry import (
    get_collector_registry_record,
    get_collector_registry_record_by_last_seen_ip,
    remember_collector_original_endpoint,
)
from .coordinator_endpoint_projection import (
    collector_cloud_family_from_endpoint_shape as _collector_cloud_family_from_endpoint_shape,
    collector_original_endpoint_source_options as _collector_original_endpoint_source_options,
    known_collector_cloud_family as _known_collector_cloud_family,
    normalize_preserved_collector_server_endpoint as _normalize_preserved_collector_server_endpoint,
    parse_collector_server_endpoint as _parse_collector_server_endpoint,
    resolve_collector_server_endpoint as _resolve_collector_server_endpoint,
    same_ipv4_24 as _same_ipv4_24,
)

logger = logging.getLogger(__name__)

_EFFECTIVE_METADATA_SNAPSHOT_OPTION_KEY = "effective_metadata_snapshot"
_CONF_COLLECTOR_CLOUD_PROFILE_KEY = "collector_cloud_profile_key"
_CONF_COLLECTOR_CLOUD_PROFILE_LABEL = "collector_cloud_profile_label"
_CONF_COLLECTOR_CLOUD_PROFILE_SOURCE = "collector_cloud_profile_source"
_CONF_COLLECTOR_CLOUD_PROFILE_CONFIDENCE = "collector_cloud_profile_confidence"


class CoordinatorPersistenceMixin:
    """Persist coordinator-owned facts without triggering an implicit reload."""

    def consume_entry_reload_suppression(self) -> bool:
        """Return whether the next config-entry update listener should skip reload."""

        if getattr(self, "_suppress_entry_reload_count", 0) <= 0:
            return False
        self._suppress_entry_reload_count -= 1
        return True

    def _async_update_entry_without_reload(self, **update_kwargs: Any) -> None:
        """Persist runtime metadata without reloading the entry we are actively running."""

        # During initial setup the coordinator may persist discovered metadata
        # before async_setup_entry registers its update listener. Such an update
        # cannot schedule a reload, so it must not leave a suppression token
        # behind: that stale token would swallow the user's next genuine options
        # change. Only arm suppression when a listener actually exists and can
        # consume it.
        update_listeners = getattr(self.config_entry, "update_listeners", ())
        suppression_armed = bool(update_listeners)
        if suppression_armed:
            self._suppress_entry_reload_count = (
                getattr(self, "_suppress_entry_reload_count", 0) + 1
            )
        changed = False
        try:
            changed = bool(
                self.hass.config_entries.async_update_entry(
                    self.config_entry,
                    **update_kwargs,
                )
            )
        finally:
            if suppression_armed and not changed:
                # A no-op update fires no update listener, so nothing would
                # ever consume the suppression - and the NEXT genuine options
                # change would have its reload silently swallowed.
                self._suppress_entry_reload_count = max(
                    self._suppress_entry_reload_count - 1, 0
                )

    def _persist_confirmed_session_protocol_from_runtime(self) -> None:
        """Persist the confirmed live wire as durable ``live_session`` evidence.

        Written ONLY from the runtime's confirmed wire binding (a trusted live
        SessionHandle), and ONLY when the durable PN matches this entry. This is
        the write side of the fail-closed confirmed-protocol bootstrap: a later
        same-PN restart seeds it; cloud family / endpoint / driver key / peer IP
        can never produce it. No-op when unchanged.
        """

        if getattr(self, "hass", None) is None or getattr(self, "config_entry", None) is None:
            return
        evidence = getattr(self._runtime, "confirmed_session_protocol_evidence", None)
        if not callable(evidence):
            return
        try:
            protocol, pn = evidence()
        except Exception:  # pragma: no cover - defensive
            return
        if not protocol or not pn:
            return
        from ..collector_identity import pn_is_same_identity, reconcile_pn

        entry_pn = str(self.config_entry.data.get(CONF_COLLECTOR_PN, "") or "").strip()
        if not entry_pn or not pn_is_same_identity(entry_pn, pn):
            return
        stored_pn = str(
            self.config_entry.data.get(CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN, "")
            or ""
        ).strip()
        if (
            self.config_entry.data.get(CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL) == protocol
            and self.config_entry.data.get(
                CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE
            )
            == COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE
            and stored_pn
            and pn_is_same_identity(stored_pn, pn)
        ):
            return
        # Reaching here means the confirmed protocol/source/PN genuinely changed
        # (the no-op guard above returned otherwise), so this is NEW live
        # evidence. Stamp the observation time ONCE here -- it is never rewritten
        # on an unchanged poll, so it records when the wire was first confirmed.
        self._persist_connection_axes(
            updates={
                CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL: protocol,
                CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE: (
                    COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE
                ),
                CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN: reconcile_pn(entry_pn, pn),
                CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT: (
                    datetime.now(timezone.utc).isoformat()
                ),
            }
        )

    def _persist_connection_axes(
        self,
        updates: dict[str, Any] | None = None,
        *,
        clear: tuple[str, ...] = (),
    ) -> None:
        """Persist explicit connection-architecture axis fields into entry data.

        The three axes are durable, opaque entry state. Explicit endpoint
        actions set them here so runtime never has to re-derive transport
        ownership from the endpoint hostname.
        """

        if getattr(self, "hass", None) is None or getattr(self, "config_entry", None) is None:
            return
        data = dict(self.config_entry.data)
        changed = False
        for key, value in (updates or {}).items():
            if data.get(key) != value:
                data[key] = value
                changed = True
        for key in clear:
            if key in data:
                del data[key]
                changed = True
        if changed:
            self._async_update_entry_without_reload(data=data)

    def _normalized_remembered_collector_server_endpoint(self) -> str:
        endpoint = str(
            getattr(self, "_remembered_collector_server_endpoint", "") or ""
        ).strip()
        if not endpoint:
            return ""
        try:
            normalized_endpoint = _normalize_preserved_collector_server_endpoint(endpoint)
            host, _port, _protocol = _parse_collector_server_endpoint(normalized_endpoint)
        except ValueError:
            return ""
        if host == self._effective_callback_server_host:
            return ""
        if self._endpoint_looks_like_local_collector_callback(normalized_endpoint):
            return ""
        return normalized_endpoint

    @property
    def _effective_callback_server_host(self) -> str:
        runtime_host = str(
            getattr(self._runtime, "effective_advertised_server_ip", "") or ""
        ).strip()
        if runtime_host:
            return runtime_host
        return str(
            getattr(self._connection_spec, "effective_advertised_server_ip", "") or ""
        ).strip()

    async def _async_prepare_home_assistant_callback_listener(self, endpoint: str) -> None:
        ensure_listener = getattr(self._runtime, "async_ensure_callback_listener", None)
        if ensure_listener is None:
            return

        callback_host, callback_port, _callback_protocol = _resolve_collector_server_endpoint(
            endpoint,
            cloud_family=self.collector_cloud_family,
        )
        if callback_host != self._effective_callback_server_host:
            return

        await ensure_listener(callback_port)

    def _endpoint_looks_like_local_collector_callback(self, endpoint: str) -> bool:
        # Endpoint-provenance safety only -- NOT transport ownership (that lives
        # in the CallbackSessionRegistry / SessionHandle). This exists so the
        # integration never records Home Assistant's own address as the "original
        # external endpoint" it could later restore to. It never decides which
        # collector owns a socket or which wire to use.
        try:
            host, _port, _protocol = _parse_collector_server_endpoint(endpoint)
        except ValueError:
            return False
        if host == self._effective_callback_server_host:
            return True
        config_entry = getattr(self, "config_entry", None)
        config_data = getattr(config_entry, "data", {}) if config_entry is not None else {}
        collector_ip = str(config_data.get(CONF_COLLECTOR_IP) or "").strip()
        return bool(collector_ip and _same_ipv4_24(host, collector_ip))

    async def _async_remember_collector_server_endpoint(self, snapshot: RuntimeSnapshot) -> None:
        current_endpoint = snapshot.collector_server_endpoint
        if not current_endpoint:
            return
        try:
            normalized_endpoint = _normalize_preserved_collector_server_endpoint(current_endpoint)
            host, _port, _protocol = _parse_collector_server_endpoint(normalized_endpoint)
        except ValueError:
            return
        # Whether to record an observed endpoint as the original external
        # endpoint is a provenance decision driven by the endpoint SHAPE, not by
        # the legacy collector_operation_mode. Recording provenance is not
        # endpoint mutation -- it is what lets an explicit rollback restore the
        # real cloud endpoint later. The two shape guards below (Home Assistant's
        # own callback host, and any local-callback-shaped address) are the only
        # things that must never be remembered as an "external" endpoint.
        if host == self._effective_callback_server_host:
            return
        if self._endpoint_looks_like_local_collector_callback(normalized_endpoint):
            return

        remembered_endpoint = self._normalized_remembered_collector_server_endpoint()
        if remembered_endpoint and normalized_endpoint != remembered_endpoint:
            return
        if normalized_endpoint == remembered_endpoint:
            return

        profile_key = (
            _collector_cloud_family_from_endpoint_shape(normalized_endpoint)
            or _known_collector_cloud_family(snapshot.values.get("collector_cloud_family"))
            or self.collector_cloud_family
        )
        self._remembered_collector_server_endpoint = normalized_endpoint
        options = dict(self.config_entry.options)
        options.update(
            _collector_original_endpoint_source_options(
                endpoint=normalized_endpoint,
                profile_key=profile_key,
                source="runtime_observed",
            )
        )
        self._async_update_entry_without_reload(options=options)
        await self._async_remember_collector_original_endpoint_in_registry(
            snapshot=snapshot,
            endpoint=normalized_endpoint,
            profile_key=profile_key,
            source="runtime_observed",
            observed_at=str(
                options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT) or ""
            ),
        )

    async def _async_restore_collector_original_endpoint_from_registry(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        """Restore preserved original endpoint options from the PN registry when absent."""

        if self.collector_capabilities.virtual_bridge:
            return
        if self._normalized_remembered_collector_server_endpoint():
            return
        collector_pn = self._preferred_collector_pn(snapshot)
        collector = getattr(snapshot, "collector", None)
        collector_ip = (
            str(getattr(collector, "remote_ip", "") or "").strip()
            or str(self.config_entry.data.get(CONF_COLLECTOR_IP, "") or "").strip()
        )
        if not collector_pn and not collector_ip:
            return

        config_dir = Path(self.hass.config.config_dir)
        try:
            record = await self.hass.async_add_executor_job(
                lambda: (
                    get_collector_registry_record(
                        config_dir=config_dir,
                        collector_pn=collector_pn,
                    )
                    if collector_pn
                    else None
                )
            )
            if record is None and collector_ip:
                record = await self.hass.async_add_executor_job(
                    lambda: get_collector_registry_record_by_last_seen_ip(
                        config_dir=config_dir,
                        last_seen_ip=collector_ip,
                    )
                )
        except Exception as exc:
            logger.debug("Could not read collector registry: %s", exc)
            return
        if record is None or not record.original_endpoint_raw:
            return
        try:
            normalized_endpoint = _normalize_preserved_collector_server_endpoint(
                record.original_endpoint_raw
            )
        except ValueError:
            return
        if self._endpoint_looks_like_local_collector_callback(normalized_endpoint):
            return

        self._remembered_collector_server_endpoint = normalized_endpoint
        options = dict(self.config_entry.options)
        options.update(
            _collector_original_endpoint_source_options(
                endpoint=normalized_endpoint,
                profile_key=record.cloud_profile_key,
                source=record.source or "collector_registry",
                observed_at=record.observed_at,
            )
        )
        self._async_update_entry_without_reload(options=options)

    async def _async_remember_collector_original_endpoint_in_registry(
        self,
        *,
        snapshot: RuntimeSnapshot,
        endpoint: str,
        profile_key: str,
        source: str,
        observed_at: str = "",
    ) -> None:
        """Persist the original endpoint in the collector PN registry when possible."""

        if self.collector_capabilities.virtual_bridge:
            return
        collector_pn = self._preferred_collector_pn(snapshot)
        if not collector_pn:
            return
        try:
            normalized_endpoint = _normalize_preserved_collector_server_endpoint(endpoint)
        except ValueError:
            return
        if self._endpoint_looks_like_local_collector_callback(normalized_endpoint):
            return

        collector = getattr(snapshot, "collector", None)
        last_seen_ip = str(getattr(collector, "remote_ip", "") or "").strip()
        config_dir = Path(self.hass.config.config_dir)
        try:
            await self.hass.async_add_executor_job(
                lambda: remember_collector_original_endpoint(
                    config_dir=config_dir,
                    collector_pn=collector_pn,
                    original_endpoint_raw=normalized_endpoint,
                    cloud_profile_key=profile_key,
                    source=source,
                    observed_at=observed_at,
                    last_seen_ip=last_seen_ip,
                )
            )
        except Exception as exc:
            logger.debug("Could not update collector registry: %s", exc)

    async def _async_remember_runtime_identity(self, snapshot: RuntimeSnapshot) -> None:
        """Persist stronger collector/inverter identity once runtime detection succeeds."""

        current_data = dict(self.config_entry.data)
        updated_data = dict(current_data)
        current_options = dict(self.config_entry.options)
        updated_options = dict(current_options)
        had_inverter_identity = bool(
            str(current_data.get(CONF_DETECTED_MODEL) or "").strip()
            or str(current_data.get(CONF_DETECTED_SERIAL) or "").strip()
        )

        def _set_data_if_value(key: str, value: object) -> None:
            if value is None:
                return
            normalized = value if isinstance(value, int) else str(value).strip()
            if normalized == "":
                return
            if updated_data.get(key) != normalized:
                updated_data[key] = normalized

        collector_pn = self._preferred_collector_pn(snapshot)
        if collector_pn and updated_data.get(CONF_COLLECTOR_PN) != collector_pn:
            updated_data[CONF_COLLECTOR_PN] = collector_pn

        collector = snapshot.collector
        collector_ip = str(getattr(collector, "remote_ip", "") or "").strip()
        connection_mode = str(current_data.get(CONF_CONNECTION_MODE) or "").strip()
        if (
            collector_ip
            and connection_mode != "callback_listener"
            and not str(updated_data.get(CONF_COLLECTOR_IP) or "").strip()
        ):
            updated_data[CONF_COLLECTOR_IP] = collector_ip

        collector_cloud_family = _known_collector_cloud_family(
            snapshot.values.get("collector_cloud_family")
        )
        if not collector_cloud_family:
            collector_cloud_family = self.collector_cloud_family
        if collector_cloud_family and updated_data.get(CONF_COLLECTOR_CLOUD_FAMILY) != collector_cloud_family:
            updated_data[CONF_COLLECTOR_CLOUD_FAMILY] = collector_cloud_family

        if collector is not None:
            _set_data_if_value(
                CONF_SMARTESS_COLLECTOR_VERSION,
                getattr(collector, "smartess_collector_version", "")
                or snapshot.values.get("smartess_collector_version"),
            )
            _set_data_if_value(
                CONF_SMARTESS_PROTOCOL_ASSET_ID,
                getattr(collector, "smartess_protocol_asset_id", "")
                or snapshot.values.get("smartess_protocol_asset_id"),
            )
            _set_data_if_value(
                CONF_SMARTESS_PROFILE_KEY,
                getattr(collector, "smartess_protocol_profile_key", "")
                or snapshot.values.get("smartess_protocol_profile_key")
                or snapshot.values.get("smartess_profile_key"),
            )
            _set_data_if_value(
                CONF_SMARTESS_DEVICE_ADDRESS,
                getattr(collector, "smartess_device_address", None)
                if getattr(collector, "smartess_device_address", None) is not None
                else snapshot.values.get("smartess_device_address"),
            )
            collector_cloud_profile = snapshot.collector_cloud_profile
            _set_data_if_value(
                _CONF_COLLECTOR_CLOUD_PROFILE_KEY,
                collector_cloud_profile.key,
            )
            _set_data_if_value(
                _CONF_COLLECTOR_CLOUD_PROFILE_LABEL,
                collector_cloud_profile.label,
            )
            _set_data_if_value(
                _CONF_COLLECTOR_CLOUD_PROFILE_SOURCE,
                collector_cloud_profile.source,
            )
            _set_data_if_value(
                _CONF_COLLECTOR_CLOUD_PROFILE_CONFIDENCE,
                collector_cloud_profile.confidence,
            )

        inverter = snapshot.inverter
        detected_serial = str(getattr(inverter, "serial_number", "") or "").strip()
        persisted_serial = str(current_data.get(CONF_DETECTED_SERIAL) or "").strip()
        # Defensive identity-conflict guard at the persistence boundary: a
        # confirmed durable serial must not be silently overwritten by a
        # different confirmed serial. The hub already keeps the durable identity
        # bound on conflict (so this rarely triggers), but persisting must never
        # be the path that swaps a confirmed inverter identity.
        inverter_identity_conflict = bool(
            inverter is not None
            and detected_serial
            and persisted_serial
            and detected_serial != persisted_serial
        )
        if inverter_identity_conflict:
            logger.warning(
                "Persisted inverter identity conflict: durable serial=%s live serial=%s; keeping durable identity",
                persisted_serial,
                detected_serial,
            )
        if inverter is not None and not inverter_identity_conflict:
            detected_model = str(inverter.model_name or "").strip()
            driver_key = str(getattr(inverter, "driver_key", "") or "").strip()
            variant_key = str(getattr(inverter, "variant_key", "") or "").strip()
            if detected_model and updated_data.get(CONF_DETECTED_MODEL) != detected_model:
                updated_data[CONF_DETECTED_MODEL] = detected_model
            if detected_serial and updated_data.get(CONF_DETECTED_SERIAL) != detected_serial:
                updated_data[CONF_DETECTED_SERIAL] = detected_serial
            if (
                not detected_serial
                and not serial_is_stable(driver_key, inverter)
                and str(updated_data.get(CONF_DETECTED_SERIAL) or "").strip()
            ):
                updated_data[CONF_DETECTED_SERIAL] = ""
            if str(updated_data.get(CONF_DETECTION_CONFIDENCE) or "").strip() in {
                "",
                "none",
                "low",
                "medium",
            }:
                updated_data[CONF_DETECTION_CONFIDENCE] = "high"
            if updated_data.get(CONF_CONTROL_MODE) == CONTROL_MODE_READ_ONLY:
                updated_data[CONF_CONTROL_MODE] = DEFAULT_CONTROL_MODE
            if updated_options.get(CONF_CONTROL_MODE) == CONTROL_MODE_READ_ONLY:
                updated_options[CONF_CONTROL_MODE] = DEFAULT_CONTROL_MODE
            if driver_key:
                # ``driver_hint`` is user intent (auto or an explicit choice).
                # Runtime detection is a separate fact and must never silently
                # turn automatic mode into a forced protocol selection.
                updated_data[CONF_DETECTED_DRIVER] = driver_key

        current_effective_snapshot = effective_metadata_snapshot_from_dict(
            current_options.get(_EFFECTIVE_METADATA_SNAPSHOT_OPTION_KEY)
        )
        updated_effective_snapshot = self._build_runtime_effective_metadata_snapshot(
            snapshot,
            entry_data=updated_data,
            current_snapshot=current_effective_snapshot,
        )
        if updated_effective_snapshot is not None:
            updated_snapshot_data = updated_effective_snapshot.as_dict()
            if updated_snapshot_data != current_effective_snapshot.as_dict():
                updated_options[_EFFECTIVE_METADATA_SNAPSHOT_OPTION_KEY] = (
                    updated_snapshot_data
                )
            self._request_entry_reload_for_metadata_drift(
                setup_signature=getattr(
                    self,
                    "_platform_loaded_effective_metadata_signature",
                    ("", "", ""),
                ),
                runtime_signature=self._effective_metadata_reload_signature_from_snapshot(
                    updated_effective_snapshot
                ),
            )

        current_unique_id = str(getattr(self.config_entry, "unique_id", "") or "").strip()
        updated_unique_id = ""
        if collector_pn and current_unique_id.startswith("collector:"):
            current_unique_pn = current_unique_id.split(":", 1)[1]
            if current_unique_pn != collector_pn:
                updated_unique_id = f"collector:{collector_pn}"

        if (
            updated_data == current_data
            and updated_options == current_options
            and not updated_unique_id
        ):
            return

        current_title = str(self.config_entry.title or "").strip()
        previous_preferred_title = installation_title(
            collector_pn=current_data.get(CONF_COLLECTOR_PN, ""),
            collector_ip=current_data.get(CONF_COLLECTOR_IP, ""),
            detected_model=current_data.get(CONF_DETECTED_MODEL, ""),
            detected_serial=current_data.get(CONF_DETECTED_SERIAL, ""),
        )
        updated_title = installation_title(
            collector_pn=updated_data.get(CONF_COLLECTOR_PN, ""),
            collector_ip=updated_data.get(CONF_COLLECTOR_IP, ""),
            detected_model=updated_data.get(CONF_DETECTED_MODEL, ""),
            detected_serial=updated_data.get(CONF_DETECTED_SERIAL, ""),
        )
        legacy_titles = legacy_installation_titles(
            detected_model=current_data.get(CONF_DETECTED_MODEL, ""),
            detected_serial=current_data.get(CONF_DETECTED_SERIAL, ""),
            collector_ip=current_data.get(CONF_COLLECTOR_IP, ""),
            server_ip=current_data.get(CONF_SERVER_IP, ""),
        )

        update_kwargs: dict[str, Any] = {}
        if updated_data != current_data:
            update_kwargs["data"] = updated_data
        if updated_options != current_options:
            update_kwargs["options"] = updated_options
        if updated_unique_id:
            update_kwargs["unique_id"] = updated_unique_id
        if (
            updated_title
            and updated_title != current_title
            and current_title in {previous_preferred_title, *legacy_titles}
        ):
            update_kwargs["title"] = updated_title

        self._async_update_entry_without_reload(**update_kwargs)
        gained_inverter_identity = bool(
            str(updated_data.get(CONF_DETECTED_MODEL) or "").strip()
            or str(updated_data.get(CONF_DETECTED_SERIAL) or "").strip()
        )
        platforms_need_identity_reload = bool(
            getattr(self, "_entity_platforms_initialized", False)
            and not getattr(self, "_entity_platforms_loaded_with_inverter_identity", False)
        )
        if gained_inverter_identity and (not had_inverter_identity or platforms_need_identity_reload):
            self._request_entry_reload_for_late_identity()



__all__ = ["CoordinatorPersistenceMixin"]
