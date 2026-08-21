"""Collector operating-profile and callback-ownership projections."""

from __future__ import annotations

from ...collector.capabilities import (
    COLLECTOR_KIND_UNKNOWN,
    CollectorCapabilityProfile,
    collector_capability_profile_from_runtime,
    collector_profile_entry_fields,
)
from ...connection.connection_policy import (
    may_auto_manage_endpoint,
    may_run_steady_reverse_discovery,
    resolve_connection_strategy,
    resolve_endpoint_control_policy,
)
from ...connection.operating_profile import (
    CollectorOperatingProfile,
    collector_operating_profile_from_entry,
)
from ...const import CONNECTION_STRATEGY_INBOUND
from ...models import RuntimeSnapshot


class CoordinatorOperatingProfileMixin:
    """Project collector operating axes and configure existing runtime owners."""

    @property
    def collector_operation_mode(self) -> str:
        """Return the compatibility sensor value for the operating profile.

        The old writable operation mode is retired.  Keep this property only so
        the existing entity unique ID remains stable; its value now comes from
        the typed read-only profile and can honestly report ``custom`` when the
        architecture axes do not describe either normal product state.
        """

        return self.collector_operating_profile.profile

    @property
    def collector_operating_profile(self) -> CollectorOperatingProfile:
        """Return the current read-only user-facing operating profile."""

        return collector_operating_profile_from_entry(
            dict(self.config_entry.data),
            dict(self.config_entry.options),
            ha_only_required=self.collector_capabilities.ha_only_required,
        )

    @property
    def collector_uses_home_assistant_route(self) -> bool:
        """Return whether the canonical strategy routes the collector to HA.

        ``resolve_connection_strategy`` owns all pre-schema compatibility
        fallback. Runtime code must not read ``collector_operation_mode`` again:
        doing so would create a second strategy resolver with different answers.
        """

        if self.collector_capabilities.ha_only_required:
            return True
        return self.connection_strategy == CONNECTION_STRATEGY_INBOUND

    @property
    def collector_cloud_tools_allowed(self) -> bool:
        """Return whether a new temporary cloud-traffic operation may start."""

        return self.collector_operating_profile.cloud_tools_allowed

    @property
    def collector_callback_listener_required(self) -> bool:
        """Return whether this entry must keep the callback listener prepared.

        This is driven by the explicit architecture axes, not by the legacy
        collector operation mode:

        - inbound entries receive collector-initiated sessions;
        - integration-managed endpoints are kept pointed at Home Assistant.
        """

        return (
            self.connection_strategy == CONNECTION_STRATEGY_INBOUND
            or may_auto_manage_endpoint(self.endpoint_control_policy)
        )

    @property
    def connection_strategy(self) -> str:
        """Return the explicit connection strategy for this entry.

        Opaque, hostname-free: ``inbound`` (the collector dials Home Assistant
        by itself) or ``callback_on_demand`` (Home Assistant must trigger a
        callback). This is the single top-level branch for transport ownership.
        """

        return resolve_connection_strategy(
            self.config_entry.data,
            self.config_entry.options,
        )

    @property
    def endpoint_control_policy(self) -> str:
        """Return whether the integration may manage the collector endpoint.

        ``external`` (never write/restore/auto-heal) or ``integration_managed``
        (the integration wrote the endpoint through an explicit action and may
        keep it aligned).
        """

        return resolve_endpoint_control_policy(
            self.config_entry.data,
            self.config_entry.options,
        )

    def _sync_collector_capability_profile(self) -> None:
        """Persist runtime-proven collector profile metadata.

        ``collector_operation_mode`` is deliberately not written here.  The
        HA-only requirement is already represented by ``collector_capabilities``
        and the public mode is a read-only projection of that capability plus
        the canonical connection strategy.
        """

        capabilities = self.collector_capabilities
        if capabilities.collector_kind == COLLECTOR_KIND_UNKNOWN:
            return

        data = dict(self.config_entry.data)
        options = dict(self.config_entry.options)
        changed = False
        hardware_version = str(
            self.data.values.get("collector_hardware_version") or ""
        ).strip()
        profile_fields = collector_profile_entry_fields(
            capabilities,
            hardware_version=hardware_version,
        )
        if capabilities.virtual_bridge:
            bridge_version = str(
                getattr(self.data.collector, "collector_bridge_version", "")
                or self.data.values.get("collector_bridge_version")
                or profile_fields.get("collector_bridge_version")
                or ""
            ).strip()
            if bridge_version:
                profile_fields["collector_bridge_version"] = bridge_version
        for key, value in profile_fields.items():
            if data.get(key) != value:
                data[key] = value
                changed = True
            if options.get(key) != value:
                options[key] = value
                changed = True
        if changed:
            self._async_update_entry_without_reload(data=data, options=options)
            if capabilities.virtual_bridge:
                self._request_entry_reload_for_collector_capability_change()

    def _collector_is_virtual_bridge(self) -> bool:
        """Return True when the running collector is a detected virtual bridge.

        Positive-only: reads the runtime snapshot's parsed hardware-version token and
        defaults to False when the snapshot is unavailable, so a factory
        collector behaves exactly as before.
        """

        return self.collector_capabilities.virtual_bridge

    @property
    def collector_capabilities(self) -> CollectorCapabilityProfile:
        """Return collector kind/capability profile for the current runtime."""

        snapshot = getattr(self, "data", RuntimeSnapshot())
        collector = getattr(snapshot, "collector", None)
        values = getattr(snapshot, "values", None)
        config_entry = getattr(self, "config_entry", None)
        return collector_capability_profile_from_runtime(
            collector=collector,
            values=values if isinstance(values, dict) else {},
            data=dict(getattr(config_entry, "data", {}) or {}),
            options=dict(getattr(config_entry, "options", {}) or {}),
        )

    def _configure_reverse_discovery_mode(self) -> None:
        """Enable steady reverse discovery only for the callback_on_demand strategy.

        Reverse discovery is the UDP ``set>server`` callback trigger. It is now
        gated purely on the explicit ``connection_strategy`` axis, not on the
        operation mode, the endpoint hostname, or the collector type:

        - ``inbound``: the collector dials Home Assistant by itself. Runtime must
          never send a UDP callback probe -- it claims or waits for the inbound
          session. Reverse discovery is disabled.
        - ``callback_on_demand``: Home Assistant may ask the collector to dial
          back. Reverse discovery is enabled.
        """

        set_reverse_discovery_enabled = getattr(
            self._runtime,
            "set_reverse_discovery_enabled",
            None,
        )
        if set_reverse_discovery_enabled is None:
            return
        set_reverse_discovery_enabled(
            may_run_steady_reverse_discovery(self.connection_strategy)
        )

    def _configure_callback_ownership(self) -> None:
        """Give the runtime the domain callback-session registry + this entry id.

        The domain registry is the production ownership authority: runtime uses
        the real entry claim to resolve the live session id, listener port, and
        negotiated wire without reading listener internals or using peer IP as
        identity. It also classifies claimed-by-other callback outcomes.
        """

        set_ownership = getattr(self._runtime, "set_callback_ownership", None)
        if not callable(set_ownership):
            return
        try:
            from ...passive_discovery import get_callback_session_registry

            registry = get_callback_session_registry(self.hass)
        except Exception:
            registry = None
        entry_id = str(getattr(self.config_entry, "entry_id", "") or "")
        set_ownership(registry, entry_id)



__all__ = ["CoordinatorOperatingProfileMixin"]
