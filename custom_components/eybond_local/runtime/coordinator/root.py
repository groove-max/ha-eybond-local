"""Home Assistant coordinator for the EyeBond Local integration."""

from __future__ import annotations

import asyncio
from datetime import timedelta
import logging
from typing import Any

from homeassistant.components import persistent_notification
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from ...connection.spec_factory import build_connection_spec
from ...const import (
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_CONNECTION_MODE,
    CONF_DETECTED_DRIVER,
    CONF_DRIVER_DETECTION_STRATEGY,
    CONF_DRIVER_HINT,
    CONF_POLL_INTERVAL,
    DEFAULT_DRIVER_DETECTION_STRATEGY,
    DEFAULT_POLL_INTERVAL,
    DOMAIN,
    DRIVER_DETECTION_STRATEGIES,
    DRIVER_HINT_AUTO,
)
from ...drivers.registry import get_driver, poll_policy_for_driver_key
from ...models import ProbeTarget, RuntimeSnapshot
from ...support.diagnostic_runner import DiagnosticSingleFlight
from ...support.local_register_collection import LocalRegisterCollectionManager
from ...timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY
from .cloud_tools import CoordinatorCloudToolsMixin
from .collector_profile import CoordinatorCollectorProfileMixin
from .control_projection import CoordinatorControlProjectionMixin
from .device_registry import CoordinatorDeviceRegistryMixin
from .diagnostics import CoordinatorDiagnosticsMixin
from .entity_reload import CoordinatorEntityReloadMixin
from .inverter_profile import CoordinatorInverterProfileMixin
from .lifecycle import CoordinatorLifecycleMixin
from .management import CoordinatorManagementMixin
from .management_projection import CoordinatorManagementProjectionMixin
from .network import CoordinatorNetworkReconcileMixin
from .operating_profile import CoordinatorOperatingProfileMixin
from .persistence import CoordinatorPersistenceMixin
from .poll_projection import (
    is_clean_runtime_poll_cycle as _is_clean_runtime_poll_cycle,
    poll_recommended_interval_seconds as _poll_recommended_interval_seconds,
)
from .polling import CoordinatorPollingMixin
from .runtime_profile import CoordinatorRuntimeProfileMixin
from .snapshot_projection import CoordinatorSnapshotProjectionMixin
from .startup import CoordinatorStartupIdentityMixin
from .strategy import CoordinatorStrategyTransitionMixin
from .support import CoordinatorSupportMixin
from .tooling_projection import (
    integration_build_runtime_values as _integration_build_runtime_values,
    localized_runtime_text as _localized_runtime_text,
    proxy_capture_notification_id as _proxy_capture_notification_id,
)
from ..factory import create_runtime_manager
from ..manager import RuntimeManager
from ..poll_scheduler import PollDecision, PollScheduler

logger = logging.getLogger(__name__)

_UNSUPPORTED_COMMANDS_OPTION_KEY = "driver_unsupported_commands"
_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY = "driver_unsupported_commands_version"
_UNSUPPORTED_COMMANDS_OPTION_VERSION = 2
_METADATA_DEAD_CHANNELS_OPTION_KEY = "collector_metadata_dead_channels"
_METADATA_DEAD_CHANNELS_OPTION_VERSION_KEY = "collector_metadata_dead_channels_version"
_METADATA_DEAD_CHANNELS_OPTION_VERSION = 1
_LEGACY_METADATA_CHANNEL_PREFIX = "collector:"


class EybondLocalCoordinator(
    CoordinatorLifecycleMixin,
    CoordinatorDiagnosticsMixin,
    CoordinatorStartupIdentityMixin,
    CoordinatorCloudToolsMixin,
    CoordinatorSnapshotProjectionMixin,
    CoordinatorSupportMixin,
    CoordinatorStrategyTransitionMixin,
    CoordinatorManagementMixin,
    CoordinatorManagementProjectionMixin,
    CoordinatorNetworkReconcileMixin,
    CoordinatorEntityReloadMixin,
    CoordinatorOperatingProfileMixin,
    CoordinatorPersistenceMixin,
    CoordinatorRuntimeProfileMixin,
    CoordinatorPollingMixin,
    CoordinatorCollectorProfileMixin,
    CoordinatorControlProjectionMixin,
    CoordinatorInverterProfileMixin,
    CoordinatorDeviceRegistryMixin,
    DataUpdateCoordinator[RuntimeSnapshot],
):
    """Owns the hub and exposes its snapshots to Home Assistant entities."""

    config_entry: ConfigEntry

    def __init__(self, hass, entry: ConfigEntry) -> None:
        self.config_entry = entry
        connection_spec = build_connection_spec(entry.data, entry.options)
        self._connection_spec = connection_spec
        driver_intent = str(
            entry.options.get(
                CONF_DRIVER_HINT,
                entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
            )
            or DRIVER_HINT_AUTO
        ).strip()
        detected_driver = str(entry.data.get(CONF_DETECTED_DRIVER) or "").strip()
        runtime_driver_key = (
            driver_intent
            if driver_intent != DRIVER_HINT_AUTO
            else detected_driver or DRIVER_HINT_AUTO
        )
        driver_detection_strategy = entry.options.get(
            CONF_DRIVER_DETECTION_STRATEGY,
            entry.data.get(
                CONF_DRIVER_DETECTION_STRATEGY,
                DEFAULT_DRIVER_DETECTION_STRATEGY,
            ),
        )
        if (
            type(driver_detection_strategy) is not str
            or driver_detection_strategy not in DRIVER_DETECTION_STRATEGIES
        ):
            driver_detection_strategy = DEFAULT_DRIVER_DETECTION_STRATEGY
        self._runtime: RuntimeManager = create_runtime_manager(
            connection_spec,
            driver_hint=runtime_driver_key,
            driver_detection_strategy=driver_detection_strategy,
            connection_mode=entry.data.get(CONF_CONNECTION_MODE, ""),
        )
        # The runtime inverter is built from built-in detection and never carries the
        # learned overlay capabilities on its own. Give the runtime a hook so that, once
        # a device-scoped overlay is active, the activated learned controls are merged
        # into the detected inverter -- otherwise they exist only in effective metadata
        # and never become entities (or writable) because every entity/write path reads
        # the runtime inverter's capabilities.
        self._device_overlay_merge_status = ""
        set_overlay_applier = getattr(self._runtime, "set_inverter_overlay_applier", None)
        if callable(set_overlay_applier):
            set_overlay_applier(self._apply_device_overlay_to_inverter)
        set_detection_observer = getattr(
            self._runtime,
            "set_inverter_detection_observer",
            None,
        )
        if callable(set_detection_observer):
            set_detection_observer(self._on_runtime_inverter_detected)
        set_snapshot_observer = getattr(
            self._runtime,
            "set_runtime_snapshot_observer",
            None,
        )
        if callable(set_snapshot_observer):
            set_snapshot_observer(self._publish_runtime_intermediate_snapshot)
        set_connection_watcher = getattr(
            self._runtime,
            "set_collector_connection_watcher",
            None,
        )
        if callable(set_connection_watcher):
            set_connection_watcher(self._on_collector_connection_established)
        persisted_unsupported = entry.options.get(_UNSUPPORTED_COMMANDS_OPTION_KEY)
        persisted_unsupported_version = entry.options.get(
            _UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY
        )
        set_unsupported = getattr(
            self._runtime,
            "set_persistent_unsupported_commands",
            None,
        )
        if (
            callable(set_unsupported)
            and persisted_unsupported_version == _UNSUPPORTED_COMMANDS_OPTION_VERSION
            and isinstance(persisted_unsupported, (list, tuple))
        ):
            # The runtime filters ``collector:`` keys out of the driver table.
            set_unsupported(tuple(persisted_unsupported))
        # Metadata dead channels seed from the dedicated option AND (one-time
        # migration) any legacy ``collector:`` keys still riding the driver
        # option. The option REWRITE that strips them from the driver table +
        # writes the dedicated option happens on the first persist cycle
        # (``_maybe_persist_metadata_dead_channels``) so no entry write races the
        # coordinator constructor.
        set_dead_channels = getattr(
            self._runtime,
            "set_persistent_metadata_dead_channels",
            None,
        )
        if callable(set_dead_channels):
            dead_channels: set[str] = set()
            persisted_dead_channels = entry.options.get(_METADATA_DEAD_CHANNELS_OPTION_KEY)
            persisted_dead_channels_version = entry.options.get(
                _METADATA_DEAD_CHANNELS_OPTION_VERSION_KEY
            )
            if (
                persisted_dead_channels_version == _METADATA_DEAD_CHANNELS_OPTION_VERSION
                and isinstance(persisted_dead_channels, (list, tuple))
            ):
                dead_channels.update(
                    str(channel).strip()
                    for channel in persisted_dead_channels
                    if str(channel).strip()
                )
            if (
                persisted_unsupported_version == _UNSUPPORTED_COMMANDS_OPTION_VERSION
                and isinstance(persisted_unsupported, (list, tuple))
            ):
                dead_channels.update(
                    str(channel).strip()
                    for channel in persisted_unsupported
                    if str(channel).strip().startswith(_LEGACY_METADATA_CHANNEL_PREFIX)
                )
            if dead_channels:
                set_dead_channels(tuple(sorted(dead_channels)))
        super().__init__(
            hass,
            logger,
            name=DOMAIN,
            update_interval=timedelta(
                seconds=entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
            ),
        )
        self.data = RuntimeSnapshot()
        self._remembered_collector_server_endpoint = str(
            entry.options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT) or ""
        ).strip()
        self._last_synced_device_meta: tuple[str, str, str, str, str] = (
            "",
            "",
            "",
            "",
            "",
        )
        self._last_synced_collector_device_meta: tuple[str, str, str, str, str] = (
            "",
            "",
            "",
            "",
            "",
        )
        self._tooling_values: dict[str, Any] = {}
        self._cached_smartess_cloud_evidence_record = None
        self._cached_smartess_cloud_evidence_warmed = False
        # The provider the cached record belongs to; a runtime provider change
        # makes the cache stale so it is never reused for the new provider.
        self._cached_cloud_evidence_provider = ""
        self._cached_effective_metadata = None
        self._cached_proxy_capture_session_state = None
        self._cached_shadow_learning_session_state = None
        # Once True, _cached_shadow_learning_session_state is authoritative and
        # the per-refresh disk read is skipped. The save/clear paths keep the
        # cache in sync (this coordinator is the only writer of the file), so the
        # steady-state cost is zero when learning is never used.
        self._shadow_learning_session_state_loaded = False
        self._proxy_trace_download_manifest_path = ""
        self._proxy_trace_download_details: tuple[str, str] = ("", "")
        self._proxy_capture_deadline_refresh_handle = None
        self._suppress_entry_reload_count = 0
        if (
            persisted_unsupported is not None
            and persisted_unsupported_version != _UNSUPPORTED_COMMANDS_OPTION_VERSION
        ):
            options = dict(self.config_entry.options)
            options.pop(_UNSUPPORTED_COMMANDS_OPTION_KEY, None)
            options.pop(_UNSUPPORTED_COMMANDS_OPTION_VERSION_KEY, None)
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                options=options,
            )
            logger.info(
                "Discarded stale unsupported inverter command cache for this device; "
                "commands will be rechecked on the current transport."
            )
        self._ha_primary_reconcile_last_signature: tuple[str, str] = ("", "")
        self._ha_primary_reconcile_last_attempt_monotonic = 0.0
        self._collector_operation_pending_target_endpoint = ""
        self._entity_platforms_initialized = False
        self._entity_platform_reload_requested = False
        self._entity_platform_reload_dispatched = False
        self._entry_loaded_reload_unsub = None
        self._component_loaded_reload_unsub = None
        self._entity_platforms_loaded_with_inverter_identity = False
        self._entity_platforms_loaded_with_driver_fallback = False
        self._platform_loaded_effective_metadata_signature: tuple[str, str, str] = (
            "",
            "",
            "",
        )
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_complete = False
        # Diagnostic command runner: at most one scenario per config entry, with
        # normal polling quiesced while it holds the shared transport.
        self._diagnostic_active = False
        self._diagnostic_flight = DiagnosticSingleFlight()
        self._support_package_active = False
        self._support_package_flight = DiagnosticSingleFlight(
            busy_error="support_package_export_in_progress"
        )
        self._runtime_operation_lock = asyncio.Lock()
        # Repeated local-register evidence outlives the options-flow dialog that
        # starts it, but never outlives this coordinator/runtime transport.  The
        # manager owns only the retained read task; cloud credentials, history,
        # correlation, entities, and writes stay outside this boundary.
        self._local_register_collection = LocalRegisterCollectionManager(
            capture_snapshot=self.async_capture_local_register_snapshot,
            create_task=getattr(self.hass, "async_create_task", asyncio.create_task),
            on_update=self._publish_local_register_collection_update,
        )
        # Proxy capture and shadow learning share one endpoint-mutation
        # authority.  Their terminal paths must also be single-flight per
        # coordinator: two same-owner stop/recovery calls may legitimately
        # adopt the same durable token, but must never run two restore writes or
        # two callback verification windows concurrently.
        self._collector_endpoint_terminalization_lock = asyncio.Lock()
        # One coordinator-lifetime receipt that a payload snapshot was truly
        # connected.  Strategy activation waits for this instead of treating
        # ConfigEntryState.LOADED or a registry-only claim as runtime success.
        self._runtime_connected_event = asyncio.Event()
        self._poll_duration_ewma_seconds = 0.0
        self._poll_duration_max_seconds = 0.0
        self._poll_recent_durations_seconds: list[float] = []
        self._poll_last_cycle_started_monotonic = 0.0
        self._collector_poll_overrun_streak = 0
        self._collector_poll_high_utilization_streak = 0
        self._poll_normal_utilization_streak = 0
        self._poll_notification_active = False
        self._inverter_protocol_notification_active = False
        self._poll_last_notification_monotonic = 0.0
        self._poll_non_runtime_retry_interval_seconds = 0
        self._poll_scheduler_driver_key = str(
            runtime_driver_key or DRIVER_HINT_AUTO
        )
        self._poll_scheduler = PollScheduler(
            policy=poll_policy_for_driver_key(self._poll_scheduler_driver_key),
            mode=self._configured_poll_mode(),
            manual_interval=self._configured_poll_interval_seconds(),
        )
        self._seed_runtime_from_persisted_inverter_metadata()
