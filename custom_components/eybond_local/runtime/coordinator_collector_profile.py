"""Read-only collector route, transport, and cloud-profile projections."""

from __future__ import annotations

from collections.abc import Mapping
import logging
from pathlib import Path

from ..collector.callback_endpoint import home_assistant_callback_endpoint
from ..collector_endpoint import (
    CollectorEndpointWriteShape,
    resolve_collector_endpoint_write_shape,
)
from ..collector.transport_profile import (
    apply_observed_collector_session_protocol,
    collector_cloud_family_from_entry_context,
    collector_session_protocol_from_inventory_state,
    normalize_collector_session_protocol,
    resolve_collector_transport_profile,
)
from ..connection.confirmed_session_protocol import ConfirmedSessionProtocolEvidence
from ..connection.connection_policy import resolve_connection_strategy
from ..const import (
    CONF_ADVERTISED_SERVER_IP,
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_COLLECTOR_PN,
    CONF_CONTROL_MODE,
    CONF_DETECTION_CONFIDENCE,
    CONF_DRIVER_HINT,
    CONF_SMARTESS_PROFILE_KEY,
    DEFAULT_CONTROL_MODE,
    DRIVER_HINT_AUTO,
)
from ..models import CollectorCloudProfile
from ..support.collector_registry import get_collector_registry_record
from .coordinator_endpoint_projection import (
    collector_cloud_family_from_endpoint_shape as _collector_cloud_family_from_endpoint_shape,
    default_cloud_upstream_endpoint as _default_cloud_upstream_endpoint,
    format_home_assistant_collector_endpoint as _format_home_assistant_collector_endpoint,
    known_collector_cloud_family as _known_collector_cloud_family,
    known_collector_cloud_profile_value as _known_collector_cloud_profile_value,
    normalize_preserved_collector_server_endpoint as _normalize_preserved_collector_server_endpoint,
    parse_collector_server_endpoint as _parse_collector_server_endpoint,
)

logger = logging.getLogger(__name__)

_CONF_COLLECTOR_CLOUD_PROFILE_KEY = "collector_cloud_profile_key"
_CONF_COLLECTOR_CLOUD_PROFILE_LABEL = "collector_cloud_profile_label"
_CONF_COLLECTOR_CLOUD_PROFILE_SOURCE = "collector_cloud_profile_source"
_CONF_COLLECTOR_CLOUD_PROFILE_CONFIDENCE = "collector_cloud_profile_confidence"


class CoordinatorCollectorProfileMixin:
    """Expose collector profile facts without creating a second authority."""

    @property
    def collector_server_endpoint_rollback_target(self) -> str:
        """Return the remembered collector callback endpoint for rollback/proxy restore."""

        runtime = getattr(self, "_runtime", None)
        runtime_target = str(
            getattr(runtime, "collector_server_endpoint_rollback_target", "") or ""
        ).strip()
        if runtime_target:
            try:
                runtime_target = _normalize_preserved_collector_server_endpoint(runtime_target)
            except ValueError:
                runtime_target = ""
            else:
                if self._endpoint_looks_like_local_collector_callback(runtime_target):
                    runtime_target = ""
        if runtime_target:
            return runtime_target
        return self._normalized_remembered_collector_server_endpoint()

    async def collector_cloud_rollback_context(self):
        """Return the typed, READ-ONLY cloud rollback endpoint for the transition UX.

        CP2B.1 convergence: this gathers already-persisted/observed facts through
        the EXISTING APIs and hands them to the neutral
        ``resolve_cloud_rollback_endpoint`` resolver. It performs NO network I/O,
        NO writes to the entry/registry/runtime, NO cloud-family fallback and
        reads NO private link fields -- it is a pre-run read model the strategy
        transition form presents, never a second endpoint authority or a proof.

        Sources gathered (the resolver owns the priority + fail-closed rules):

        * the durable original cloud endpoint, read as ONE whole record
          (``entry.data`` owns it when any original-endpoint field is present
          there; ``entry.options`` is only a legacy whole-record fallback and its
          fields are never mixed with data's);
        * the PN-bound collector-registry endpoint, via the existing read-only
          ``get_collector_registry_record`` (executor, no write);
        * the confirmed current endpoint from the live snapshot.
        """

        from ..connection.strategy_transition_context import (
            resolve_confirmed_ha_endpoint,
            resolve_cloud_rollback_endpoint,
        )
        from ..connection.recovery_contract import RecoveryContract

        data = self.config_entry.data
        options = self.config_entry.options
        original_fields = (
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
        )
        # Whole-record precedence: data owns the record when ANY original field is
        # present there (a partial data record yields a missing endpoint -> the
        # resolver fails closed, never mixing in an options field). Only a total
        # absence in data falls back to the whole options record.
        if any(field in data for field in original_fields):
            durable_original = data.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT)
        elif any(field in options for field in original_fields):
            durable_original = options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT)
        else:
            durable_original = ""

        # Durable identity only.  Do not stringify a malformed entry value or
        # substitute a transient live PN at this read-model trust boundary.
        raw_entry_pn = data.get(CONF_COLLECTOR_PN)
        entry_pn = (
            raw_entry_pn
            if type(raw_entry_pn) is str
            and raw_entry_pn
            and raw_entry_pn == raw_entry_pn.strip()
            else ""
        )
        registry_endpoint = ""
        registry_pn = ""
        if entry_pn and not self.collector_capabilities.virtual_bridge:
            try:
                config_dir = Path(self.hass.config.config_dir)
                record = await self.hass.async_add_executor_job(
                    lambda: get_collector_registry_record(
                        config_dir=config_dir,
                        collector_pn=entry_pn,
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive read
                logger.debug("Cloud rollback context registry read failed: %s", exc)
                record = None
            if record is not None and record.original_endpoint_raw:
                registry_endpoint = record.original_endpoint_raw
                registry_pn = record.collector_pn

        # The complete HA endpoint must be earned and PN-bound.  Runtime/local
        # server fallbacks are intentionally absent: behind NAT they are not the
        # address the collector was proven to dial.
        recovery_contract = RecoveryContract.from_entry_data(data)
        confirmed_ha_endpoint = resolve_confirmed_ha_endpoint(
            current_strategy=resolve_connection_strategy(data, options),
            entry_pn=raw_entry_pn,
            advertised_host=data.get(CONF_ADVERTISED_SERVER_IP, ""),
            advertised_port=data.get(CONF_ADVERTISED_TCP_PORT, 0),
            recovery_contract=recovery_contract,
        )
        # Pass the raw observed value.  A non-string/duck value must fail closed
        # in the neutral resolver, never become an endpoint via ``str()``.
        observed_current = self.data.collector_server_endpoint

        return resolve_cloud_rollback_endpoint(
            # CP2B.1 offers NO explicit user choice yet (reserved for CP2B.2).
            explicit_user_endpoint="",
            durable_original_endpoint=durable_original,
            registry_endpoint=registry_endpoint,
            registry_pn=registry_pn,
            entry_pn=entry_pn,
            observed_current_endpoint=observed_current,
            confirmed_ha_endpoint=confirmed_ha_endpoint,
        )

    @property
    def collector_callback_target_endpoint(self) -> str:
        """Return the effective callback endpoint configured for this entry."""

        template_endpoint = str(
            self.data.collector_server_endpoint
            or self.collector_server_endpoint_rollback_target
            or ""
        ).strip()
        return home_assistant_callback_endpoint(
            server_host=self._effective_callback_server_host,
            listener_port=int(
                getattr(self._connection_spec, "effective_advertised_tcp_port", 0)
                or getattr(self._connection_spec, "tcp_port", 0)
                or 0
            ),
            template_endpoint=template_endpoint,
            cloud_family=self.collector_cloud_family,
        )

    @property
    def collector_endpoint_write_shape(self) -> CollectorEndpointWriteShape:
        """Return the catalog-backed endpoint shape used by this collector.

        This is a read-only projection.  It does not decide identity, choose an
        address or mint transition evidence; it only explains how the already
        selected family/template serializes the endpoint and, for a host-only
        format, which implicit TCP port that value means on the wire.
        """

        template_endpoint = (
            self.data.collector_server_endpoint
            or self.collector_server_endpoint_rollback_target
            or getattr(self, "_remembered_collector_server_endpoint", "")
            or ""
        )
        return resolve_collector_endpoint_write_shape(
            cloud_family=self.collector_cloud_family,
            template_endpoint=template_endpoint,
        )

    @property
    def proxy_capture_target_endpoint(self) -> str:
        """Return the dedicated callback endpoint reserved for proxy capture sessions."""

        upstream_endpoint = self.proxy_capture_upstream_endpoint
        return _format_home_assistant_collector_endpoint(
            server_host=self._effective_callback_server_host,
            template_endpoint=upstream_endpoint,
            cloud_family=self.collector_cloud_family,
        )

    @property
    def proxy_capture_upstream_endpoint(self) -> str:
        """Return the endpoint that the proxy should forward collector traffic to."""

        rollback_target = self.collector_server_endpoint_rollback_target
        if rollback_target:
            try:
                _parse_collector_server_endpoint(rollback_target)
            except ValueError:
                rollback_target = ""

        current_endpoint = self.data.collector_server_endpoint
        if current_endpoint:
            try:
                current_endpoint = _normalize_preserved_collector_server_endpoint(current_endpoint)
                current_host, _current_port, _current_protocol = _parse_collector_server_endpoint(current_endpoint)
            except ValueError:
                current_host = ""
            if (
                current_host != self._effective_callback_server_host
                and not self._endpoint_looks_like_local_collector_callback(current_endpoint)
            ):
                return current_endpoint

        if rollback_target:
            return rollback_target

        return _default_cloud_upstream_endpoint(
            cloud_family=self.collector_cloud_family,
            template_endpoint=current_endpoint,
        )

    @property
    def collector_cloud_family(self) -> str:
        """Return the best available collector cloud family known to the coordinator."""

        collector = getattr(self.data, "collector", None)
        family = _known_collector_cloud_family(
            getattr(collector, "collector_cloud_family", "")
        )
        if family:
            return family
        family = _known_collector_cloud_family(
            self.data.values.get("collector_cloud_family")
        )
        if family:
            return family
        config_entry = getattr(self, "config_entry", None)
        config_data = getattr(config_entry, "data", {}) if config_entry is not None else {}
        config_options = getattr(config_entry, "options", {}) if config_entry is not None else {}

        endpoint_candidates = (
            self.data.collector_server_endpoint,
            self.collector_server_endpoint_rollback_target,
            getattr(self, "_remembered_collector_server_endpoint", ""),
        )
        family = collector_cloud_family_from_entry_context(
            config_data,
            config_options,
            extra_endpoints=endpoint_candidates,
        )
        if family:
            return family

        for endpoint in (*endpoint_candidates, config_options.get(CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT, "")):
            family = _collector_cloud_family_from_endpoint_shape(endpoint)
            if family:
                return family
        return ""

    @property
    def collector_session_protocol(self) -> str:
        """Return the callback protocol selected by confirmed wire evidence."""

        return self.collector_transport_profile.session_protocol

    @property
    def collector_identity_strategy(self) -> str:
        """Return the identity strategy for the confirmed callback wire."""

        return self.collector_transport_profile.identity_strategy

    @property
    def collector_raw_passthrough_bootstrap(self) -> str:
        """Return forwarding setup for an already-confirmed AT wire."""

        return self.collector_transport_profile.raw_passthrough_bootstrap

    @property
    def collector_raw_passthrough_frame_format(self) -> str:
        """Return forwarding framing for an already-confirmed AT wire."""

        return self.collector_transport_profile.raw_passthrough_frame_format

    @property
    def collector_raw_passthrough_min_interval_ms(self) -> int:
        """Return minimum interval between raw passthrough requests."""

        return self.collector_transport_profile.raw_passthrough_min_interval_ms

    @property
    def collector_transport_profile(self):
        """Return the resolved callback transport profile for this runtime."""

        resolved = resolve_collector_transport_profile(
            cloud_family=self.collector_cloud_family,
            runtime_owner_key=self._collector_runtime_owner_key(),
            virtual_bridge=self._collector_is_virtual_bridge(),
        )
        # Only a live/PN-bound protocol can turn the neutral profile into a wire
        # profile. The protocol map is owned by the transport-profile authority.
        return apply_observed_collector_session_protocol(
            resolved, self._observed_collector_session_protocol()
        )

    def _observed_collector_session_protocol(self) -> str:
        """Return a positive callback-session protocol observed from this entry.

        The same ESP bridge can transport either framed FC traffic or raw
        AT/ASCII passthrough, so only live or persisted PN-bound evidence counts.
        """

        runtime = getattr(self, "_runtime", None)
        link_diagnostics = getattr(runtime, "listener_diagnostics", None)
        if callable(link_diagnostics):
            try:
                diagnostics = link_diagnostics()
            except Exception:  # pragma: no cover - defensive runtime inspection
                diagnostics = {}
            protocol = normalize_collector_session_protocol(
                diagnostics.get("collector_callback_observed_session_protocol")
            )
            if not protocol and not str(
                self.config_entry.data.get(CONF_COLLECTOR_PN, "") or ""
            ).strip():
                protocol = self._observed_collector_session_protocol_from_diagnostics(
                    diagnostics
                )
            if protocol:
                return protocol

        values = getattr(self.data, "values", {})
        if isinstance(values, Mapping):
            # Runtime-owned live observation only. Generic configured protocol
            # values are intentionally not trusted here.
            protocol = normalize_collector_session_protocol(
                values.get("collector_runtime_link_session_protocol")
            )
            if protocol:
                return protocol
        evidence = ConfirmedSessionProtocolEvidence.from_entry(
            self.config_entry.data,
            self.config_entry.options,
            entry_pn=self.config_entry.data.get(CONF_COLLECTOR_PN),
        )
        if evidence is not None:
            return evidence.protocol
        return ""

    @staticmethod
    def _observed_collector_session_protocol_from_diagnostics(
        diagnostics: Mapping[str, object],
    ) -> str:
        """Return observed protocol from live listener inventory diagnostics.

        ``collector_configured_session_protocol`` is the link manager's configured
        protocol, not a byte-shape observation. The actual observation lives in
        ``collector_callback_session_inventory``.  Prefer it so a newly-added
        inbound collector can switch from a stale/persisted profile to the
        protocol proven by the current TCP session before runtime claims it.
        """

        sessions = diagnostics.get("collector_callback_session_inventory")
        if not isinstance(sessions, (list, tuple)):
            return ""
        protocols: set[str] = set()
        for session in sessions:
            if not isinstance(session, Mapping):
                continue
            protocol = collector_session_protocol_from_inventory_state(
                state=session.get("state"),
                protocol_shape=session.get("protocol_shape"),
            )
            if protocol:
                protocols.add(protocol)
        if len(protocols) == 1:
            return next(iter(protocols))
        return ""

    def _collector_runtime_owner_key(self) -> str:
        """Return the best known inverter owner for profile diagnostics."""

        for source in (self.config_entry.options, self.config_entry.data):
            driver_hint = str(
                source.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO) or DRIVER_HINT_AUTO
            ).strip().lower()
            if driver_hint and driver_hint != DRIVER_HINT_AUTO:
                return driver_hint

        snapshot = self.effective_metadata_snapshot
        if isinstance(snapshot, Mapping):
            owner_key = str(snapshot.get("effective_owner_key") or "").strip().lower()
        else:
            owner_key = str(getattr(snapshot, "effective_owner_key", "") or "").strip().lower()
        if owner_key:
            return owner_key
        try:
            return str(self.effective_owner_key or "").strip().lower()
        except Exception:
            return ""

    @property
    def collector_cloud_profile(self) -> CollectorCloudProfile:
        """Return one coherent runtime or persisted collector cloud profile."""

        runtime_profile = getattr(self.data, "collector_cloud_profile", None)
        if type(runtime_profile) is CollectorCloudProfile and runtime_profile.known:
            return runtime_profile
        config_entry = getattr(self, "config_entry", None)
        config_data = getattr(config_entry, "data", {}) if config_entry is not None else {}
        key = _known_collector_cloud_profile_value(
            config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_KEY)
        ) or _known_collector_cloud_profile_value(
            config_data.get(CONF_SMARTESS_PROFILE_KEY)
        )
        if not key:
            return CollectorCloudProfile()
        return CollectorCloudProfile(
            key=key,
            label=_known_collector_cloud_profile_value(
                config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_LABEL)
            ),
            source=(
                _known_collector_cloud_profile_value(
                    config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_SOURCE)
                )
                or "entry_persisted"
            ),
            confidence=(
                _known_collector_cloud_profile_value(
                    config_data.get(_CONF_COLLECTOR_CLOUD_PROFILE_CONFIDENCE)
                )
                or "low"
            ),
        )

    @property
    def collector_cloud_profile_key(self) -> str:
        """Return the selected collector cloud profile key."""

        return self.collector_cloud_profile.key

    @property
    def collector_cloud_profile_label(self) -> str:
        """Return the selected collector cloud profile label."""

        return self.collector_cloud_profile.label

    @property
    def collector_cloud_profile_source(self) -> str:
        """Return the selected collector cloud profile provenance."""

        return self.collector_cloud_profile.source

    @property
    def collector_cloud_profile_confidence(self) -> str:
        """Return confidence for the selected collector cloud profile."""

        return self.collector_cloud_profile.confidence

    @property
    def detection_confidence(self) -> str:
        """Return the saved detection confidence for this entry."""

        return self.config_entry.data.get(CONF_DETECTION_CONFIDENCE, "none")

    @property
    def control_mode(self) -> str:
        """Return the configured control mode override."""

        return self.config_entry.options.get(
            CONF_CONTROL_MODE,
            self.config_entry.data.get(CONF_CONTROL_MODE, DEFAULT_CONTROL_MODE),
        )

    def format_home_assistant_callback_endpoint(self, host: str, port: int) -> str:
        """Format ONE user-confirmed host/port as this entry's callback endpoint.

        Wraps the single endpoint-formatting rule (protocol/template shaping
        from the collector's currently reported endpoint) around a
        caller-supplied address: the host/port come from the user verbatim —
        behind NAT they may be a public address — and are never replaced by a
        local interface guess.
        """

        template_endpoint = str(
            self.data.collector_server_endpoint
            or self.collector_server_endpoint_rollback_target
            or ""
        ).strip()
        return home_assistant_callback_endpoint(
            server_host=str(host or "").strip(),
            listener_port=int(port or 0),
            template_endpoint=template_endpoint,
            cloud_family=self.collector_cloud_family,
        )



__all__ = ["CoordinatorCollectorProfileMixin"]
