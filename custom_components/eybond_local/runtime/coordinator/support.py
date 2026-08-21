"""Support export, cloud evidence, and local-metadata tooling lifecycle."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any

from homeassistant.components import persistent_notification

from ...const import (
    CONF_COLLECTOR_PN,
    CONF_SMARTESS_DEVICE_ADDRESS,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
    DOMAIN,
)
from ...fixtures.utils import anonymize_fixture_json
from ...metadata.effective_metadata import resolve_effective_metadata_selection
from ...metadata.local_metadata import (
    clear_local_metadata_loader_caches,
    create_local_profile_draft,
    create_local_schema_draft,
    rollback_local_metadata_overrides,
)
from ...metadata.profile_loader import builtin_base_profile_name
from ...metadata.register_schema_loader import builtin_base_schema_name
from ...metadata.smartess_draft import SmartEssKnownFamilyDraftPlan
from ...metadata.smartess_smg_bridge import SmartEssSmgBridgePlan
from ...support.bundle import export_support_bundle
from ...support.cloud_evidence_providers import DRAFT_KIND_KNOWN_FAMILY, DRAFT_KIND_SMG_BRIDGE
from ...support.collector_registry import get_collector_registry_record
from ...support.download import sign_support_package_download_url
from ...support.package import export_support_package
from ...support.runtime_projection import (
    build_collector_support_payload,
    build_support_fixture,
    metadata_source_payload,
)
from ...support.shadow_learning_review_model import normalize_activation_selection
from .tooling_projection import (
    integration_build_runtime_values as _integration_build_runtime_values,
    localized_runtime_text as _localized_runtime_text,
)

logger = logging.getLogger(__name__)

_DEVICE_SCOPED_OVERLAY_ACTIVATION_OPTION_KEY = "device_scoped_overlay_activation"


class CoordinatorSupportMixin:
    """Coordinate support artifacts without owning runtime transport semantics."""

    @property
    def smartess_known_family_draft_plan(self) -> SmartEssKnownFamilyDraftPlan | None:
        """Return one safe SmartESS known-family draft plan when available.

        Backward-compatible SmartESS-named surface; the plan is RESOLVED behind
        the provider contract (which owns the SmartESS asset-field gathering).
        """

        candidate = self._cloud_evidence_draft_candidate(DRAFT_KIND_KNOWN_FAMILY)
        return candidate.plan if candidate is not None else None

    @property
    def smartess_smg_bridge_plan(self) -> SmartEssSmgBridgePlan | None:
        """Return one safe SmartESS-backed SMG bridge plan when available.

        The SMG/model decision lives entirely in the SmartESS provider +
        ``metadata/smartess_smg_bridge``; the coordinator only reads the result.
        """

        candidate = self._cloud_evidence_draft_candidate(DRAFT_KIND_SMG_BRIDGE)
        return candidate.plan if candidate is not None else None

    async def async_export_cloud_evidence(
        self,
        *,
        username: str,
        password: str,
    ) -> str:
        """Fetch and persist one provider-specific cloud-evidence bundle for this entry.

        The coordinator owns only HA orchestration (executor job, cache, notify);
        WHICH provider and HOW it fetches live entirely behind the neutral
        contract. Credentials are ephemeral arguments -- never cached or logged.
        """

        context = self._cloud_evidence_context()
        if not context.collector_pn:
            raise RuntimeError("cloud_evidence_collector_pn_not_available")
        # Capture the provider impl + its id + context BEFORE the await: the
        # active cloud family may change while the executor job runs, so the
        # cache must be stamped with the FETCHING provider (never a re-read
        # dynamic value). If the active provider changed by the time this
        # completes, ``_latest_..._record`` sees the mismatch and the result
        # stays invisible to the new provider (and to support bundles).
        provider = self._cloud_evidence_provider_impl()
        provider_id = provider.provider_id
        if not provider.export_available(context):
            raise RuntimeError(
                f"cloud_evidence_provider_not_supported:{self.cloud_evidence_provider or 'unknown'}"
            )

        record = await self.hass.async_add_executor_job(
            lambda: provider.export(context, username=username, password=password)
        )
        self._cached_smartess_cloud_evidence_record = record
        self._cached_cloud_evidence_provider = provider_id
        self._cached_smartess_cloud_evidence_warmed = True
        # The export belongs to the provider captured before the await.  If
        # live detection changed provider in the meantime, retain the correctly
        # stamped (and therefore hidden) cache but do not publish its path/status
        # as tooling state for the newly active provider.
        if self.cloud_evidence_provider == provider_id:
            self._publish_tooling_values(
                cloud_evidence_path=str(record.path),
                local_metadata_status=provider.export_status_label,
            )
        return str(record.path)

    async def async_export_support_bundle(self) -> str:
        """Export one JSON support bundle for the current entry."""

        await self._async_refresh_before_support_export()
        integration_build_values = await self.hass.async_add_executor_job(
            _integration_build_runtime_values
        )
        collector_registry_lookup = await self._async_collector_registry_lookup()
        support_bundle_payload = self._build_support_bundle_payload(
            integration_build_values=integration_build_values,
            collector_registry_lookup=collector_registry_lookup,
        )
        path = await self.hass.async_add_executor_job(
            lambda: export_support_bundle(
                config_dir=Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                entry_title=self._support_context_title(),
                connected=support_bundle_payload["runtime"]["connected"],
                collector=support_bundle_payload["runtime"]["collector"],
                inverter=support_bundle_payload["runtime"]["inverter"],
                values=support_bundle_payload["runtime"]["values"],
                telemetry=self.data.telemetry,
                data=support_bundle_payload["entry"]["data"],
                options=support_bundle_payload["entry"]["options"],
                profile_name=support_bundle_payload["source_metadata"]["profile_name"],
                register_schema_name=support_bundle_payload["source_metadata"]["register_schema_name"],
                cloud_evidence=support_bundle_payload["evidence"]["cloud"],
            )
        )
        self._publish_tooling_values(
            cloud_evidence_path=str(
                support_bundle_payload["runtime"]["values"].get("cloud_evidence_path") or ""
            ),
            support_bundle_path=str(path),
            local_metadata_status="Support bundle exported",
        )
        return str(path)

    async def async_export_support_package(self) -> str:
        """Export one combined support archive with raw capture and replay fixture."""

        return await self.async_export_support_package_with_cloud_refresh()

    async def async_export_support_package_with_cloud_refresh(
        self,
        *,
        smartess_username: str = "",
        smartess_password: str = "",
        wants_refresh: bool | None = None,
    ) -> str:
        """Export one support archive, optionally refreshing SmartESS cloud evidence first.

        ``wants_refresh`` lets the caller override the legacy "refresh when any
        credential field is non-empty" inference so that ``USE_SAVED`` mode can
        be honored even when credentials are pre-filled in the form. The legacy
        behavior is preserved when the parameter is left unset.
        """

        async def _factory() -> str:
            try:
                return await self._async_export_support_package_with_cloud_refresh_unlocked(
                    smartess_username=smartess_username,
                    smartess_password=smartess_password,
                    wants_refresh=wants_refresh,
                )
            except Exception:
                self._publish_tooling_values(
                    local_metadata_status="Support archive export failed"
                )
                raise

        return await self._support_package_flight.run(
            _factory,
            on_start=self._mark_support_package_active,
            on_finish=self._mark_support_package_idle,
        )

    async def _async_export_support_package_with_cloud_refresh_unlocked(
        self,
        *,
        smartess_username: str = "",
        smartess_password: str = "",
        wants_refresh: bool | None = None,
    ) -> str:
        """Export one support archive after the single-flight guard is acquired."""

        if wants_refresh is None:
            wants_refresh = bool(smartess_username or smartess_password)
        if wants_refresh:
            if not smartess_username or not smartess_password:
                raise RuntimeError("cloud_credentials_required")
            try:
                await self.async_export_cloud_evidence(
                    username=smartess_username,
                    password=smartess_password,
                )
            except Exception as exc:
                if self._cached_smartess_cloud_evidence_record is None:
                    raise
                logger.warning(
                    "Cloud evidence refresh failed; building archive with last saved evidence: %s",
                    exc,
                )
                self._publish_tooling_values(
                    local_metadata_status=(
                        "Cloud evidence refresh failed; using last saved evidence"
                    ),
                )

        integration_build_values = await self.hass.async_add_executor_job(
            _integration_build_runtime_values
        )
        support_bundle_payload, raw_capture = await self._async_build_support_package_payloads(
            integration_build_values=integration_build_values,
        )
        collector_payload = build_collector_support_payload(
            self.data.collector,
            self.collector_cloud_profile,
        )
        fixture = build_support_fixture(
            raw_capture,
            inverter=self.data.inverter,
            collector_payload=collector_payload,
        )
        anonymized_fixture = anonymize_fixture_json(fixture) if fixture is not None else None
        profile_metadata = self.effective_profile_metadata
        register_schema_metadata = self.effective_register_schema_metadata

        export_result = await self.hass.async_add_executor_job(
            lambda: export_support_package(
                config_dir=Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                entry_title=self._support_context_title(),
                support_bundle=support_bundle_payload,
                raw_capture=raw_capture,
                fixture=fixture,
                anonymized_fixture=anonymized_fixture,
                profile_source=metadata_source_payload(profile_metadata),
                register_schema_source=metadata_source_payload(register_schema_metadata),
            )
        )
        path = export_result.path
        if export_result.download_url:
            relative_download_url = str(export_result.download_url)
        else:
            # Use a short-lived signed HA API path for browser navigation. A
            # plain authenticated API URL returns 401 when opened from markdown,
            # because the browser does not attach the HA bearer token to a
            # normal link click. Keep it relative so the browser uses the same
            # HA origin through which the frontend was opened (LAN, WireGuard,
            # reverse proxy, or external URL). HA signs the path and query, not
            # the origin.
            relative_download_url = sign_support_package_download_url(
                self.hass,
                self.config_entry.entry_id,
            )
        download_url = relative_download_url
        self._publish_tooling_values(
            cloud_evidence_path=str(
                support_bundle_payload["runtime"]["values"].get("cloud_evidence_path") or ""
            ),
            support_package_path=str(path),
            support_package_download_path=str(export_result.download_path or ""),
            support_package_download_url=download_url,
            support_package_download_relative_url=relative_download_url,
            local_metadata_status="Support archive exported",
        )
        if download_url:
            persistent_notification.async_create(
                self.hass,
                _localized_runtime_text(
                    self.hass,
                    "support_archive_notification_body",
                    download_url=download_url,
                ),
                title=_localized_runtime_text(self.hass, "support_archive_notification_title"),
                notification_id=f"{DOMAIN}_support_package_{self.config_entry.entry_id}",
            )
        return str(path)

    async def _async_refresh_before_support_export(self) -> None:
        """Best-effort refresh so support archives reflect self-healed runtime state."""

        try:
            snapshot = await self._async_update_data()
        except Exception as exc:  # noqa: BLE001 - support export must remain available
            logger.warning(
                "Support archive pre-refresh failed for entry %s: %s",
                self.config_entry.entry_id,
                exc,
            )
            return
        if snapshot is not None:
            self.data = snapshot

    async def _async_build_support_package_payloads(
        self,
        *,
        integration_build_values: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Build support bundle payload and raw capture under one runtime operation lock."""

        async with self._runtime_operation_lock:
            try:
                snapshot = await self._async_update_data_with_runtime_lock()
            except Exception as exc:  # noqa: BLE001 - support export must remain available
                logger.warning(
                    "Support archive pre-refresh failed for entry %s: %s",
                    self.config_entry.entry_id,
                    exc,
                )
            else:
                if snapshot is not None:
                    self.data = snapshot

            collector_registry_lookup = await self._async_collector_registry_lookup()
            support_bundle_payload = self._build_support_bundle_payload(
                integration_build_values=integration_build_values,
                collector_registry_lookup=collector_registry_lookup,
            )
            try:
                raw_capture = await self._runtime.async_capture_support_evidence()
            except Exception as exc:
                raw_capture = {
                    "capture_kind": "unsupported_or_failed",
                    "error": str(exc),
                    "captured_ranges": [],
                    "range_failures": [],
                }

        return support_bundle_payload, raw_capture

    async def _async_collector_registry_lookup(self) -> tuple[str, Any | None]:
        """Read the collector registry record without blocking the Home Assistant loop."""

        collector_pn = self._preferred_collector_pn(self.data)
        if not collector_pn:
            return "unavailable", None

        try:
            record = await self.hass.async_add_executor_job(
                lambda: get_collector_registry_record(
                    config_dir=Path(self.hass.config.path()),
                    collector_pn=collector_pn,
                )
            )
        except Exception as exc:  # pragma: no cover - defensive diagnostics only
            return f"error:{type(exc).__name__}", None

        return ("found" if record is not None else "missing"), record

    async def async_create_local_profile_draft(self) -> str:
        """Create or refresh one local experimental profile draft."""

        return await self.async_create_local_profile_draft_named()

    async def async_create_local_profile_draft_named(
        self,
        output_profile_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> str:
        """Create or refresh one local experimental profile draft."""

        source_profile_name = self.effective_profile_name
        if not source_profile_name:
            raise RuntimeError("driver_profile_not_available")
        path = await self.hass.async_add_executor_job(
            lambda: create_local_profile_draft(
                config_dir=Path(self.hass.config.config_dir),
                source_profile_name=source_profile_name,
                output_profile_name=output_profile_name,
                overwrite=overwrite,
            )
        )
        self._publish_tooling_values(
            local_profile_draft_path=str(path),
            local_metadata_status="Local profile draft created",
        )
        return str(path)

    async def async_create_local_schema_draft(self) -> str:
        """Create or refresh one local experimental register schema draft."""

        return await self.async_create_local_schema_draft_named()

    async def async_create_local_schema_draft_named(
        self,
        output_schema_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> str:
        """Create or refresh one local experimental register schema draft."""

        source_schema_name = self.effective_register_schema_name
        if not source_schema_name:
            raise RuntimeError("driver_register_schema_not_available")
        path = await self.hass.async_add_executor_job(
            lambda: create_local_schema_draft(
                config_dir=Path(self.hass.config.config_dir),
                source_schema_name=source_schema_name,
                output_schema_name=output_schema_name,
                overwrite=overwrite,
            )
        )
        self._publish_tooling_values(
            local_schema_draft_path=str(path),
            local_metadata_status="Local register schema draft created",
        )
        return str(path)

    async def async_reload_local_metadata(self) -> None:
        """Reload the current config entry after local metadata changes."""

        self._cached_effective_metadata = None
        clear_local_metadata_loader_caches()
        self._publish_tooling_values(local_metadata_status="Reloading local metadata")
        await self.hass.config_entries.async_reload(self.config_entry.entry_id)

    async def async_activate_device_scoped_overlay(
        self,
        *,
        profile_name: str,
        register_schema_name: str,
        selection: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Persist one explicit device-scoped learned overlay activation and reload.

        ``selection`` carries the user's control choices for this device (built from the
        review screen via ``build_activation_selection``). When provided, the activation
        records ``selected_controls`` (with user labels), ``excluded_controls`` (with
        retained reasons), and ``selected_control_keys`` so runtime exposes only the
        selected learned controls. When omitted, the activation declares no selection and
        runtime keeps exposing every learned control (legacy behavior).
        """

        normalized_profile_name = str(profile_name or "").strip()
        normalized_schema_name = str(register_schema_name or "").strip()
        if not normalized_profile_name or not normalized_schema_name:
            raise ValueError("device_scoped_overlay_activation_requires_profile_and_schema")

        collector = self.data.collector
        inverter = self.identified_inverter
        write_context = self._write_exposure_context()
        activation_scope: dict[str, Any] = {
            "effective_owner_key": str(self.effective_owner_key or "").strip(),
            # Rebase to the built-in base: when activating an overlay while another is
            # already active, ``effective_*_name`` is the *previous* overlay's learned
            # name. Storing that raw would poison the device-scope match on reload (the
            # runtime base resolves to the built-in name), silently suppressing the
            # activation. The scope matcher also rebases defensively, so old activations
            # self-heal; this keeps newly written activations clean at the source.
            "base_profile_name": str(
                builtin_base_profile_name(self.effective_profile_name or "")
            ).strip(),
            "base_register_schema_name": str(
                builtin_base_schema_name(self.effective_register_schema_name or "")
            ).strip(),
            "variant_key": str(write_context.get("variant_key") or "").strip(),
            "collector_pn": str(
                getattr(collector, "collector_pn", "")
                or self.config_entry.data.get(CONF_COLLECTOR_PN, "")
                or ""
            ).strip(),
            "smartess_protocol_asset_id": str(
                getattr(collector, "smartess_protocol_asset_id", "")
                or self.config_entry.data.get(CONF_SMARTESS_PROTOCOL_ASSET_ID, "")
                or ""
            ).strip(),
            "smartess_protocol_profile_key": str(
                getattr(collector, "smartess_protocol_profile_key", "")
                or self.config_entry.data.get(CONF_SMARTESS_PROFILE_KEY, "")
                or ""
            ).strip(),
            "smartess_device_address": (
                getattr(collector, "smartess_device_address", None)
                if getattr(collector, "smartess_device_address", None) is not None
                else self.config_entry.data.get(CONF_SMARTESS_DEVICE_ADDRESS)
            ),
            "inverter_model": str(getattr(inverter, "model_name", "") or "").strip(),
            "inverter_serial": str(getattr(inverter, "serial_number", "") or "").strip(),
        }
        activation = {
            "profile_name": normalized_profile_name,
            "register_schema_name": normalized_schema_name,
            "scope": "device",
            "activated_at": datetime.now(timezone.utc).isoformat(),
            "activation_scope": activation_scope,
        }
        if selection is not None:
            activation.update(normalize_activation_selection(selection))

        options = dict(self.config_entry.options)
        options[_DEVICE_SCOPED_OVERLAY_ACTIVATION_OPTION_KEY] = activation
        self._async_update_entry_without_reload(options=options)

        self._cached_effective_metadata = None
        clear_local_metadata_loader_caches()
        self._publish_tooling_values(
            local_metadata_status="Device-scoped learned overlay activated; reloading local metadata",
        )
        await self.hass.config_entries.async_reload(self.config_entry.entry_id)
        return activation

    async def async_rollback_local_metadata(self) -> tuple[str, ...]:
        """Remove active managed local overrides and reload the entry."""

        removed_paths = await self.hass.async_add_executor_job(
            lambda: rollback_local_metadata_overrides(
                config_dir=Path(self.hass.config.config_dir),
                profile_name=self.effective_profile_name or None,
                schema_name=self.effective_register_schema_name or None,
                profile_metadata=self.effective_profile_metadata,
                schema_metadata=self.effective_register_schema_metadata,
            )
        )
        clear_local_metadata_loader_caches()
        self._cached_effective_metadata = None
        self._publish_tooling_values(local_metadata_status="Rolling back local metadata")
        await self.hass.config_entries.async_reload(self.config_entry.entry_id)
        return tuple(str(path) for path in removed_paths)

    async def _async_create_cloud_evidence_draft(
        self,
        kind: str,
        output_profile_name: str | None,
        output_schema_name: str | None,
        *,
        overwrite: bool,
        missing_plan_error: str,
        status: str,
    ) -> tuple[str, str]:
        """Create one local draft pair from the active provider's candidate.

        The coordinator owns only HA orchestration (executor + tooling values);
        the draft DECISION (known-family / SMG bridge / model rules) lives behind
        the provider contract.
        """

        record = self._latest_smartess_cloud_evidence_record()
        if record is None:
            raise RuntimeError("smartess_cloud_evidence_not_available")
        provider = self._cloud_evidence_provider_impl()
        context = self._cloud_evidence_context()
        candidate = provider.draft_candidate(context, record, kind)
        if candidate is None:
            raise RuntimeError(missing_plan_error)

        profile_path, schema_path = await self.hass.async_add_executor_job(
            lambda: provider.create_draft(
                context,
                record,
                candidate,
                output_profile_name=output_profile_name,
                output_schema_name=output_schema_name,
                overwrite=overwrite,
            )
        )
        self._publish_tooling_values(
            cloud_evidence_path=str(record.path),
            local_profile_draft_path=str(profile_path),
            local_schema_draft_path=str(schema_path),
            local_metadata_status=status,
        )
        return str(profile_path), str(schema_path)

    async def async_create_smartess_known_family_draft_named(
        self,
        output_profile_name: str | None = None,
        output_schema_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> tuple[str, str]:
        """Create local profile/schema drafts from latest SmartESS known-family evidence."""

        return await self._async_create_cloud_evidence_draft(
            DRAFT_KIND_KNOWN_FAMILY,
            output_profile_name,
            output_schema_name,
            overwrite=overwrite,
            missing_plan_error="smartess_known_family_not_resolved",
            status="SmartESS local draft created",
        )

    async def async_create_smartess_smg_bridge_named(
        self,
        output_profile_name: str | None = None,
        output_schema_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> tuple[str, str]:
        """Create one SmartESS-backed SMG bridge draft pair."""

        return await self._async_create_cloud_evidence_draft(
            DRAFT_KIND_SMG_BRIDGE,
            output_profile_name,
            output_schema_name,
            overwrite=overwrite,
            missing_plan_error="smartess_smg_bridge_not_resolved",
            status="SmartESS SMG bridge created",
        )

    def _latest_smartess_cloud_evidence_record(self):
        """Return the cached cloud-evidence record OWNED BY THE ACTIVE PROVIDER.

        Reads from the in-memory cache populated by the warm/export helpers. If
        the active provider changed since the cache was populated, the cached
        record belongs to the previous provider and is ignored (a fresh warm will
        repopulate it), so one provider's evidence never leaks to another.
        """

        if self._cached_cloud_evidence_provider != self.cloud_evidence_provider:
            return None
        return self._cached_smartess_cloud_evidence_record

    async def _async_warm_smartess_cloud_evidence_cache(self) -> None:
        """Refresh the cached cloud-evidence record from disk via the provider.

        The provider impl + id + context are captured BEFORE the executor await so
        the record that is cached is the one THIS provider loaded, stamped with
        THIS provider's id -- a mid-flight active-provider change cannot make it
        visible to the new provider.
        """

        provider = self._cloud_evidence_provider_impl()
        provider_id = provider.provider_id
        context = self._cloud_evidence_context()
        record = await self.hass.async_add_executor_job(
            lambda: provider.load_latest(context)
        )
        self._cached_smartess_cloud_evidence_record = record
        self._cached_cloud_evidence_provider = provider_id
        self._cached_smartess_cloud_evidence_warmed = True

    def _warm_effective_metadata_cache_blocking(self):
        """Resolve effective metadata and force profile/schema cache population."""

        metadata = resolve_effective_metadata_selection(
            inverter=self.identified_inverter,
            driver=self.current_driver,
            collector=self.data.collector,
            entry_data=self.config_entry.data,
            entry_options=self.config_entry.options,
            persisted_snapshot=self.effective_metadata_snapshot,
        )
        # Access the lazy fields in the executor thread. The sync properties are used
        # later by HA runtime/UI code, so their JSON files must already be cached there.
        _ = metadata.profile_metadata
        _ = metadata.register_schema_metadata
        return metadata

    async def _async_warm_effective_metadata_cache(self) -> None:
        """Warm profile/schema loaders outside the event loop."""

        try:
            self._cached_effective_metadata = await self.hass.async_add_executor_job(
                self._warm_effective_metadata_cache_blocking
            )
        except Exception as exc:
            self._cached_effective_metadata = None
            logger.debug("Effective metadata cache warm-up failed: %s", exc)



__all__ = ["CoordinatorSupportMixin"]
