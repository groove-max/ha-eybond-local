"""Passive callback discovery for collectors that already dial Home Assistant."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
import inspect
import logging
import time
from typing import TYPE_CHECKING, Any

from .collector.transport import _acquire_shared_listener, _release_shared_listener
from .collector.transport_profile import collector_session_protocol_from_inventory_state
from .connection.session_registry import (
    CallbackSessionRegistry,
    identity_source_is_strong,
    pn_is_same_identity,
)
from .collector_endpoint import (
    DEFAULT_COLLECTOR_SERVER_PORT,
    LEGACY_BINARY_COLLECTOR_SERVER_PORT,
)
from .const import (
    COLLECTOR_OPERATION_HA_ONLY,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_MODE,
    CONF_CONNECTION_TYPE,
    CONF_TCP_PORT,
    CONNECTION_TYPE_EYBOND,
    DEFAULT_TCP_PORT,
    DOMAIN,
)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

logger = logging.getLogger(__name__)

_DATA_KEY = "passive_callback_discovery"
_REGISTRY_DATA_KEY = "callback_session_registry"
_LISTENER_HOST = "0.0.0.0"
_POLL_INTERVAL_SECONDS = 2.0
_WEAK_IDENTITY_SETTLE_SECONDS = 6.0
_STOP_LISTENER_RELEASE_TIMEOUT_SECONDS = 4.0


@dataclass(frozen=True)
class PassiveDiscoveryRefreshResult:
    """Summary of one user-requested passive-discovery refresh."""

    connected_unclaimed_count: int
    suppressed_candidate_count: int


try:
    from homeassistant.const import EVENT_HOMEASSISTANT_STOP
except ModuleNotFoundError:  # Local tooling imports the package without Home Assistant.
    EVENT_HOMEASSISTANT_STOP = "homeassistant_stop"


def _collector_identity_matches(left: str, right: str) -> bool:
    normalized_left = str(left or "").strip()
    normalized_right = str(right or "").strip()
    return bool(normalized_left and normalized_right and normalized_left == normalized_right)


def _collector_prefix_matches(left: str, right: str) -> bool:
    # Short/full PN identity reconciliation lives in the registry; the transient
    # discovery-flow dedup below defers to the same rule instead of duplicating
    # the prefix logic.
    return pn_is_same_identity(left, right)


def _session_identity_source(session: dict[str, object]) -> str:
    return str(session.get("collector_identity_source") or "").strip()


def _session_has_strong_identity(session: dict[str, object]) -> bool:
    return identity_source_is_strong(_session_identity_source(session))


def _session_id(session: dict[str, object]) -> str:
    return str(session.get("session_id") or "").strip()


def _session_inventory_key(session: Mapping[str, object]) -> str:
    """Return one listener-scoped transient socket identity."""

    session_id = str(session.get("session_id") or "").strip()
    if not session_id:
        return ""
    return f"{int(session.get('listener_port') or 0)}:{session_id}"


class PassiveCallbackDiscovery:
    """Keep shared listeners open and publish unclaimed callback sessions to HA."""

    def __init__(self, hass: HomeAssistant) -> None:
        self._hass = hass
        self._listeners: dict[int, Any] = {}
        self._task: asyncio.Task | None = None
        self._notified: set[str] = set()
        self._weak_identity_first_seen: dict[str, float] = {}
        self._logged_session_signatures: set[str] = set()
        # Active onboarding scans and passive discovery intentionally share the
        # same listener.  A socket caused by the scan's UDP callback trigger is
        # a result of that flow, not an independent passive-discovery edge.
        # Track only transient session ids accepted while a scan scope is open;
        # PN and peer IP are deliberately not used as causal evidence.
        self._active_probe_scopes: dict[str, set[str]] = {}
        self._probe_suppressed_sessions: set[str] = set()
        # The single ownership + short/full PN reconciliation authority. Passive
        # discovery reads coalesced, unclaimed candidates from it; the listener
        # inventory is only its raw source.
        self._registry = CallbackSessionRegistry(
            sessions_source=self.iter_observed_sessions,
        )

    @property
    def registry(self) -> CallbackSessionRegistry:
        """Return the callback session ownership registry backing discovery."""

        return self._registry

    def begin_active_probe_scope(self, scope_id: str) -> None:
        """Start attributing newly accepted sessions to one active scan."""

        token = str(scope_id or "").strip()
        if not token:
            return
        self._active_probe_scopes[token] = self._observed_session_keys()

    def end_active_probe_scope(
        self,
        scope_id: str,
        *,
        retain_session_ids: set[str] | None = None,
    ) -> None:
        """Finish one probe scope and retain only its attributable sockets.

        ``None`` preserves the broad scan behavior: every socket accepted during
        the scope remains suppressed.  A concrete set is used by one-target
        callback attempts: unrelated sessions are suppressed only while the
        matcher is deciding, then become discoverable again.
        """

        token = str(scope_id or "").strip()
        baseline = self._active_probe_scopes.pop(token, None)
        if baseline is None:
            return
        current = self._observed_session_keys()
        new_keys = current - baseline
        if retain_session_ids is None:
            self._probe_suppressed_sessions.update(new_keys)
            return

        retained_ids = {
            str(session_id or "").strip()
            for session_id in retain_session_ids
            if str(session_id or "").strip()
        }
        retained_keys = {
            _session_inventory_key(session)
            for session in self.iter_observed_sessions()
            if _session_id(session) in retained_ids
        }
        # Do not release a socket while another overlapping probe scope is still
        # evaluating it.  That scope will make its own retain/release decision.
        other_scope_keys: set[str] = set()
        for other_baseline in self._active_probe_scopes.values():
            other_scope_keys.update(current - other_baseline)
        self._probe_suppressed_sessions.difference_update(
            new_keys - retained_keys - other_scope_keys
        )
        self._probe_suppressed_sessions.update(retained_keys)

    def retire_entry_sessions(self, entry_id: str) -> None:
        """Prevent an unloaded entry's current sockets becoming new discovery edges.

        Removing or reloading a config entry releases its registry ownership,
        but that lifecycle event does not make the already-known TCP socket a
        newly discovered device.  Retire the exact listener/session ids before
        the claim is released; a later physical reconnect receives a new
        session id and remains discoverable.
        """

        owner = str(entry_id or "").strip()
        if not owner:
            return
        for candidate in self._registry.observed_sessions_per_socket():
            if candidate.owner_entry_id != owner:
                continue
            key = _session_inventory_key(candidate.raw)
            if key:
                self._probe_suppressed_sessions.add(key)

    async def async_show_discovered_devices_again(
        self,
    ) -> PassiveDiscoveryRefreshResult:
        """Forget temporary publication suppression and immediately re-poll.

        This intentionally resets only the in-memory, edge-triggered discovery
        history. Durable config entries, registry ownership, active config
        flows, and active scan scopes remain authoritative, so configured
        collectors cannot be duplicated and an in-progress scan keeps owning
        the sockets it caused.
        """

        unclaimed = tuple(self._registry.list_unclaimed_sessions())
        suppressed_candidate_count = 0
        for candidate in unclaimed:
            session = dict(candidate.raw)
            collector_pn = str(session.get("collector_pn") or "").strip()
            port = int(session.get("listener_port") or 0)
            if (
                _session_inventory_key(session) in self._probe_suppressed_sessions
                or (
                    collector_pn
                    and self._already_notified(port, collector_pn, session=session)
                )
            ):
                suppressed_candidate_count += 1

        self._notified.clear()
        self._probe_suppressed_sessions.clear()
        await self._async_poll_once()
        return PassiveDiscoveryRefreshResult(
            connected_unclaimed_count=len(unclaimed),
            suppressed_candidate_count=suppressed_candidate_count,
        )

    def _observed_session_keys(self) -> set[str]:
        return {
            key
            for session in self.iter_observed_sessions()
            if (key := _session_inventory_key(session))
        }

    def _capture_active_probe_sessions(self, observed: object) -> None:
        """Suppress only sockets accepted after an active scan began."""

        current = {
            key
            for candidate in tuple(observed or ())
            if (key := _session_inventory_key(candidate.raw))
        }
        if self._active_probe_scopes:
            for baseline in self._active_probe_scopes.values():
                self._probe_suppressed_sessions.update(current - baseline)
        # A later TCP connection is a new edge and must remain discoverable.
        self._probe_suppressed_sessions.intersection_update(current)

    async def async_start(self) -> None:
        """Start passive discovery on known collector callback ports."""

        if self._task is not None:
            return
        for port in sorted(
            {
                LEGACY_BINARY_COLLECTOR_SERVER_PORT,
                DEFAULT_TCP_PORT,
                DEFAULT_COLLECTOR_SERVER_PORT,
            }
        ):
            try:
                self._listeners[port] = await _acquire_shared_listener(
                    _LISTENER_HOST,
                    port,
                )
            except Exception as exc:
                # A configured entry may bind later after network state changes;
                # passive discovery is opportunistic and must not block startup.
                logger.info(
                    "Passive EyeBond callback discovery is unavailable on %s:%s: %s",
                    _LISTENER_HOST,
                    port,
                    exc,
                )
        background_task = getattr(self._hass, "async_create_background_task", None)
        if callable(background_task):
            kwargs = {}
            with suppress(ValueError, TypeError):
                signature = inspect.signature(background_task)
                if "eager_start" in signature.parameters:
                    kwargs["eager_start"] = False
            try:
                self._task = background_task(
                    self._async_run(),
                    "EyeBond passive callback discovery",
                    **kwargs,
                )
            except TypeError:
                self._task = background_task(
                    self._async_run(),
                    "EyeBond passive callback discovery",
                )
        else:
            self._task = self._hass.async_create_task(self._async_run())

    def iter_observed_sessions(self) -> tuple[dict[str, object], ...]:
        """Return raw inbound sessions across all passive listeners.

        Each session dict is the shared listener's ``discovered_collector_sessions``
        shape, augmented with the listener port and the confirmed session
        protocol. This is the source the callback session registry reads.
        """

        sessions: list[dict[str, object]] = []
        for port, listener in tuple(self._listeners.items()):
            inventory_provider = getattr(listener, "discovered_collector_sessions", None)
            if not callable(inventory_provider):
                continue
            try:
                observed = inventory_provider() or ()
            except Exception:
                continue
            for session in observed:
                if not isinstance(session, dict):
                    continue
                enriched = dict(session)
                enriched.setdefault("listener_port", int(port))
                enriched.setdefault(
                    "session_protocol",
                    collector_session_protocol_from_inventory_state(
                        state=session.get("state"),
                        protocol_shape=session.get("protocol_shape"),
                    ),
                )
                sessions.append(enriched)
        return tuple(sessions)

    async def async_stop(self) -> None:
        """Stop passive discovery and release listener references."""

        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        for listener in tuple(self._listeners.values()):
            await self._async_release_listener_bounded(listener)
        self._listeners.clear()

    async def _async_release_listener_bounded(self, listener: Any) -> None:
        """Release one passive listener without blocking Home Assistant shutdown.

        Runtime entries still use the strict shared-listener release path.  The
        domain-level passive discovery service is opportunistic; during HA stop
        it must not hold the Core shutdown on a parked collector socket or a
        slow TCP close.
        """

        release_task = asyncio.create_task(
            _release_shared_listener(listener),
            name=f"eybond_passive_discovery_release_{getattr(listener, '_port', 'unknown')}",
        )
        try:
            await asyncio.wait_for(
                asyncio.shield(release_task),
                timeout=_STOP_LISTENER_RELEASE_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.debug(
                "Timed out releasing passive callback listener on port %s; "
                "cleanup will continue in background",
                getattr(listener, "_port", "unknown"),
            )
        except Exception:
            logger.debug("Failed to release passive callback listener", exc_info=True)

    async def _async_run(self) -> None:
        while True:
            try:
                await self._async_poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("Passive EyeBond callback discovery poll failed", exc_info=True)
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    async def _async_poll_once(self) -> None:
        flow_manager = getattr(getattr(self._hass, "config_entries", None), "flow", None)
        async_init = getattr(flow_manager, "async_init", None)
        if not callable(async_init):
            return

        # The registry is the single coalescing + ownership authority: it reads
        # the listener inventory (via ``iter_observed_sessions``), collapses
        # short/full PN duplicates, and marks sessions owned by a config entry.
        observed = self._registry.observed_sessions()
        self._capture_active_probe_sessions(observed)
        self._prune_notified_for_observed_sessions(observed)

        # 1) Repair/upgrade existing entries from their currently-live session.
        for candidate in observed:
            session = dict(candidate.raw)
            collector_pn = str(session.get("collector_pn") or "").strip()
            peer_ip = str(session.get("peer_ip") or "").strip()
            if not collector_pn or not peer_ip:
                continue
            port = int(session.get("listener_port") or 0)
            self._log_session_once(port, session)
            existing_entry = self._entry_for_collector_pn(collector_pn)
            if existing_entry is not None:
                await self._async_abort_configured_discovery_flows(
                    port=port,
                    session=session,
                )
                self._maybe_upgrade_existing_entry_from_session(
                    existing_entry,
                    port=port,
                    session=session,
                )

        # 2) Publish unclaimed candidates -- collectors that already dial Home
        # Assistant but no entry owns yet. list_unclaimed_sessions() already
        # excludes registry-claimed identities.
        for candidate in self._registry.list_unclaimed_sessions():
            session = dict(candidate.raw)
            if _session_inventory_key(session) in self._probe_suppressed_sessions:
                logger.debug(
                    "Suppressing passive discovery for already-handled session port=%s session=%s pn=%s",
                    session.get("listener_port"),
                    session.get("session_id"),
                    session.get("collector_pn"),
                )
                continue
            collector_pn = str(session.get("collector_pn") or "").strip()
            peer_ip = str(session.get("peer_ip") or "").strip()
            if not collector_pn or not peer_ip:
                continue
            if self._entry_for_collector_pn(collector_pn) is not None:
                # Handled by the upgrade pass above.
                continue
            port = int(session.get("listener_port") or 0)
            if self._weak_identity_is_still_settling(port, session):
                continue
            session_protocol = collector_session_protocol_from_inventory_state(
                state=session.get("state"),
                protocol_shape=session.get("protocol_shape"),
            )
            # Never replace an existing short-PN flow when this same identity
            # later yields its full PN.  The user may already be interacting
            # with that flow; aborting it makes the frontend poll a removed
            # flow_id ("Invalid flow specified").  The flow's verification
            # path owns short->full enrichment and updates its unique identity
            # in place.
            if self._active_discovery_flow_exists(port=port, session=session):
                logger.debug(
                    "Passive callback discovery flow already active port=%s pn=%s peer=%s",
                    port,
                    collector_pn,
                    peer_ip,
                )
                continue
            if self._already_notified(port, collector_pn, session=session):
                logger.debug(
                    "Passive callback discovery already published port=%s pn=%s peer=%s",
                    port,
                    collector_pn,
                    peer_ip,
                )
                continue
            logger.info(
                "Publishing passive EyeBond callback discovery port=%s pn=%s peer=%s source=%s state=%s",
                port,
                collector_pn,
                peer_ip,
                session.get("collector_identity_source"),
                session.get("state"),
            )
            await async_init(
                DOMAIN,
                context={
                    "source": "integration_discovery",
                    "title_placeholders": {
                        "name": _discovery_title(collector_pn, peer_ip)
                    },
                    # FlowManager.async_progress() does not expose the original
                    # discovery data on real Home Assistant. Keep the durable
                    # identity needed for active-flow dedup in public context
                    # instead of relying on test-only ``flow['data']``.
                    "eybond_discovery": {
                        CONF_COLLECTOR_PN: collector_pn,
                        CONF_TCP_PORT: port,
                        "peer_ip": peer_ip,
                        "session_id": str(session.get("session_id") or ""),
                        "collector_identity_source": str(
                            session.get("collector_identity_source") or ""
                        ),
                    },
                },
                data={
                    CONF_CONNECTION_TYPE: CONNECTION_TYPE_EYBOND,
                    CONF_TCP_PORT: port,
                    CONF_COLLECTOR_PN: collector_pn,
                    "peer_ip": peer_ip,
                    "session_id": str(session.get("session_id") or ""),
                    "protocol_shape": str(session.get("protocol_shape") or ""),
                    "collector_session_protocol": session_protocol,
                    "collector_identity_source": str(
                        session.get("collector_identity_source") or ""
                    ),
                },
            )
            self._remember_notified(port, collector_pn, session=session)

    def _log_session_once(self, port: int, session: dict[str, object]) -> None:
        collector_pn = str(session.get("collector_pn") or "").strip()
        peer_ip = str(session.get("peer_ip") or "").strip()
        session_id = str(session.get("session_id") or "").strip()
        identity_source = str(session.get("collector_identity_source") or "").strip()
        state = str(session.get("state") or "").strip()
        signature = f"{int(port)}:{session_id}:{peer_ip}:{collector_pn}:{identity_source}:{state}"
        if signature in self._logged_session_signatures:
            return
        self._logged_session_signatures.add(signature)
        if len(self._logged_session_signatures) > 200:
            self._logged_session_signatures = set(
                tuple(self._logged_session_signatures)[-100:]
            )
        logger.info(
            "Observed passive EyeBond callback session port=%s pn=%s peer=%s session=%s source=%s state=%s protocol_shape=%s",
            port,
            collector_pn,
            peer_ip,
            session_id,
            identity_source,
            state,
            session.get("protocol_shape"),
        )

        self._prune_weak_identity_seen()

    async def _async_abort_configured_discovery_flows(
        self,
        *,
        port: int,
        session: dict[str, object],
    ) -> None:
        """Abort passive discovery flows once the collector has a config entry."""

        flow_manager = getattr(getattr(self._hass, "config_entries", None), "flow", None)
        async_progress = getattr(flow_manager, "async_progress", None)
        async_abort = getattr(flow_manager, "async_abort", None)
        if not callable(async_progress) or not callable(async_abort):
            return

        collector_pn = str(session.get("collector_pn") or "").strip()
        peer_ip = str(session.get("peer_ip") or "").strip()
        session_id = _session_id(session)
        if not collector_pn:
            return

        try:
            flows = async_progress(include_uninitialized=True)
        except TypeError:
            flows = async_progress()
        except Exception:
            logger.debug("Failed to inspect active passive discovery flows", exc_info=True)
            return

        for flow in tuple(flows or ()):
            flow_data = self._flow_discovery_data(flow)
            if flow_data is None:
                continue
            flow_pn = str(flow_data.get(CONF_COLLECTOR_PN) or "").strip()
            flow_peer_ip = str(flow_data.get("peer_ip") or "").strip()
            flow_port = int(flow_data.get(CONF_TCP_PORT) or 0)
            flow_session_id = str(flow_data.get("session_id") or "").strip()
            same_identity = _collector_prefix_matches(flow_pn, collector_pn)
            if flow_port and flow_port != int(port) and not same_identity:
                continue
            same_session = bool(session_id and flow_session_id and session_id == flow_session_id)
            legacy_same_peer = (
                not flow_session_id
                and bool(peer_ip)
                and flow_peer_ip == peer_ip
            )
            if not (same_identity or same_session or legacy_same_peer):
                continue

            flow_id = str(flow.get("flow_id") or "").strip()
            if not flow_id:
                continue
            try:
                result = async_abort(flow_id)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.debug(
                    "Failed to abort configured passive discovery flow for collector %s",
                    flow_pn,
                    exc_info=True,
                )
                continue
            self._forget_notified(port, flow_pn or collector_pn)
            if flow_session_id:
                self._notified.discard(f"session:{int(port)}:{flow_session_id}")

    def _notification_keys(
        self,
        port: int,
        collector_pn: str,
        *,
        session: dict[str, object] | None = None,
    ) -> tuple[str, ...]:
        normalized_port = str(int(port))
        keys = [f"pn:{normalized_port}:{collector_pn}"]
        if session is not None:
            session_id = _session_id(session)
            if session_id:
                keys.append(f"session:{normalized_port}:{session_id}")
        return tuple(keys)

    def _already_notified(
        self,
        port: int,
        collector_pn: str,
        *,
        session: dict[str, object] | None = None,
    ) -> bool:
        if any(
            key in self._notified
            for key in self._notification_keys(port, collector_pn, session=session)
        ):
            return True
        # Compatibility with keys stored by older in-memory service instances.
        for notified in self._notified:
            if notified.startswith("pn:") or notified.startswith("session:"):
                continue
            notified_port, _, notified_pn = notified.partition(":")
            if notified_port != str(int(port)):
                continue
            if _collector_identity_matches(notified_pn, collector_pn):
                return True
        return False

    def _remember_notified(
        self,
        port: int,
        collector_pn: str,
        *,
        session: dict[str, object] | None = None,
    ) -> None:
        normalized_port = str(int(port))
        self._notified = {
            notified
            for notified in self._notified
            if not (
                (
                    notified.startswith("pn:")
                    and notified.partition(":")[2].partition(":")[0] == normalized_port
                    and _collector_identity_matches(
                        notified.rsplit(":", 1)[1],
                        collector_pn,
                    )
                )
                or (
                    not notified.startswith(("pn:", "session:"))
                    and notified.partition(":")[0] == normalized_port
                    and _collector_identity_matches(
                        notified.partition(":")[2],
                        collector_pn,
                    )
                )
            )
        }
        self._notified.update(
            self._notification_keys(port, collector_pn, session=session)
        )

    def _prune_notified_for_observed_sessions(self, observed: object) -> None:
        """Forget published candidates whose live callback session disappeared.

        Passive discovery is edge-triggered: one live session/identity should
        publish one HA discovery flow.  If the user dismisses that flow while
        the same socket is still connected, we should not immediately recreate
        it every poll.  Once the socket disappears, a later callback is a new
        edge and may be published again.
        """

        if not self._notified:
            return

        alive_sessions: set[str] = set()
        alive_pns: list[tuple[str, str]] = []
        for candidate in tuple(observed or ()):
            raw = getattr(candidate, "raw", None)
            if not isinstance(raw, Mapping):
                continue
            port = str(int(raw.get("listener_port") or 0))
            collector_pn = str(raw.get("collector_pn") or "").strip()
            session_id = _session_id(raw)
            if session_id:
                alive_sessions.add(f"session:{port}:{session_id}")
            if collector_pn:
                alive_pns.append((port, collector_pn))

        pruned: set[str] = set()
        for notified in self._notified:
            if notified.startswith("session:"):
                if notified in alive_sessions:
                    pruned.add(notified)
                continue
            if notified.startswith("pn:"):
                _prefix, _sep, rest = notified.partition(":")
                port, _sep, pn = rest.partition(":")
            else:
                port, _sep, pn = notified.partition(":")
            if any(
                port == alive_port and _collector_identity_matches(pn, alive_pn)
                for alive_port, alive_pn in alive_pns
            ):
                pruned.add(notified)
        self._notified = pruned

    def _active_discovery_flow_exists(
        self,
        *,
        port: int,
        session: dict[str, object],
    ) -> bool:
        flow_manager = getattr(getattr(self._hass, "config_entries", None), "flow", None)
        async_progress = getattr(flow_manager, "async_progress", None)
        if not callable(async_progress):
            return False

        collector_pn = str(session.get("collector_pn") or "").strip()
        peer_ip = str(session.get("peer_ip") or "").strip()
        if not collector_pn or not peer_ip:
            return False

        try:
            flows = async_progress(include_uninitialized=True)
        except TypeError:
            flows = async_progress()
        except Exception:
            logger.debug("Failed to inspect active passive discovery flows", exc_info=True)
            return False

        current_strong = _session_has_strong_identity(session)
        current_session_id = _session_id(session)
        for flow in tuple(flows or ()):
            flow_data = self._flow_discovery_data(flow)
            if flow_data is None:
                continue
            flow_pn = str(flow_data.get(CONF_COLLECTOR_PN) or "").strip()
            flow_peer_ip = str(flow_data.get("peer_ip") or "").strip()
            flow_port = int(flow_data.get(CONF_TCP_PORT) or 0)
            flow_session_id = str(flow_data.get("session_id") or "").strip()
            # Durable identity is independent of listener port. This also
            # suppresses a late short heartbeat PN while the same collector's
            # full-PN flow is already open on another/reconnected session.
            if flow_pn and _collector_prefix_matches(flow_pn, collector_pn):
                return True
            if flow_port and flow_port != int(port):
                continue
            if (
                not current_strong
                and _session_has_strong_identity(flow_data)
                and _collector_prefix_matches(flow_pn, collector_pn)
            ):
                return True
            if (
                current_session_id
                and flow_session_id
                and current_session_id == flow_session_id
            ):
                # The same callback socket/session is already represented by
                # an active discovery flow. Keep that flow stable; its own
                # verification path adopts a later full PN in place.
                return True
            if flow_peer_ip != peer_ip or flow_port != int(port):
                continue
            if _collector_identity_matches(flow_pn, collector_pn):
                return True
        return False

    @staticmethod
    def _flow_discovery_data(flow: object) -> dict[str, object] | None:
        if not isinstance(flow, dict):
            return None
        if str(flow.get("handler") or "") != DOMAIN:
            return None
        context = flow.get("context") if isinstance(flow.get("context"), dict) else {}
        if str(context.get("source") or "") != "integration_discovery":
            return None
        data = dict(flow.get("data")) if isinstance(flow.get("data"), dict) else {}
        discovery = context.get("eybond_discovery")
        if isinstance(discovery, dict):
            for key, value in discovery.items():
                data.setdefault(str(key), value)
        # Existing flows created before the context metadata was introduced
        # still expose their durable identity as ConfigFlow unique_id.
        unique_id = str(context.get("unique_id") or "").strip()
        if unique_id.startswith("collector:"):
            data.setdefault(CONF_COLLECTOR_PN, unique_id.split(":", 1)[1])
        return data

    def _forget_notified(self, port: int, collector_pn: str) -> None:
        normalized_port = str(int(port))
        self._notified = {
            notified
            for notified in self._notified
            if not (
                (
                    notified.startswith("pn:")
                    and notified.partition(":")[2].partition(":")[0] == normalized_port
                    and _collector_identity_matches(
                        notified.rsplit(":", 1)[1],
                        collector_pn,
                    )
                )
                or (
                    not notified.startswith(("pn:", "session:"))
                    and notified.partition(":")[0] == normalized_port
                    and _collector_identity_matches(
                        notified.partition(":")[2],
                        collector_pn,
                    )
                )
            )
        }

    def _weak_identity_is_still_settling(self, port: int, session: dict[str, object]) -> bool:
        collector_pn = str(session.get("collector_pn") or "").strip()
        identity_source = str(session.get("collector_identity_source") or "").strip()
        if (
            not collector_pn
            or not identity_source
            or identity_source_is_strong(identity_source)
        ):
            return False
        session_id = str(session.get("session_id") or "").strip()
        peer_ip = str(session.get("peer_ip") or "").strip()
        key = f"{int(port)}:{session_id or peer_ip}:{collector_pn}"
        now = time.monotonic()
        first_seen = self._weak_identity_first_seen.setdefault(key, now)
        return (now - first_seen) < _WEAK_IDENTITY_SETTLE_SECONDS

    def _prune_weak_identity_seen(self) -> None:
        if not self._weak_identity_first_seen:
            return
        cutoff = time.monotonic() - (_WEAK_IDENTITY_SETTLE_SECONDS * 10)
        self._weak_identity_first_seen = {
            key: first_seen
            for key, first_seen in self._weak_identity_first_seen.items()
            if first_seen >= cutoff
        }

    def _entry_for_collector_pn(self, collector_pn: str):
        config_entries = getattr(self._hass, "config_entries", None)
        async_entries = getattr(config_entries, "async_entries", None)
        if not callable(async_entries):
            return None
        for entry in async_entries(DOMAIN):
            entry_data = getattr(entry, "data", {}) or {}
            entry_pn = str(entry_data.get(CONF_COLLECTOR_PN) or "")
            if _collector_identity_matches(entry_pn, collector_pn):
                return entry
            is_callback_listener = (
                str(entry_data.get(CONF_CONNECTION_MODE) or "").strip()
                == "callback_listener"
            )
            if is_callback_listener and _collector_prefix_matches(entry_pn, collector_pn):
                return entry
            unique_id = str(getattr(entry, "unique_id", "") or "")
            if unique_id.startswith("collector:") and _collector_identity_matches(
                unique_id.split(":", 1)[1],
                collector_pn,
            ):
                return entry
            if (
                is_callback_listener
                and unique_id.startswith("collector:")
                and _collector_prefix_matches(unique_id.split(":", 1)[1], collector_pn)
            ):
                return entry
        return None

    def _entry_exists_for_collector_pn(self, collector_pn: str) -> bool:
        return self._entry_for_collector_pn(collector_pn) is not None

    def _maybe_upgrade_existing_entry_from_session(
        self,
        entry,
        *,
        port: int,
        session: dict[str, object],
    ) -> None:
        config_entries = getattr(self._hass, "config_entries", None)
        async_update_entry = getattr(config_entries, "async_update_entry", None)
        if not callable(async_update_entry):
            return

        collector_pn = str(session.get("collector_pn") or "").strip()
        peer_ip = str(session.get("peer_ip") or "").strip()
        if not collector_pn:
            return
        session_protocol = collector_session_protocol_from_inventory_state(
            state=session.get("state"),
            protocol_shape=session.get("protocol_shape"),
        )

        current_data = dict(getattr(entry, "data", {}) or {})
        current_pn = str(current_data.get(CONF_COLLECTOR_PN) or "").strip()
        data = dict(current_data)
        changed = False

        if (
            collector_pn
            and collector_pn != current_pn
            and (
                not current_pn
                or _collector_identity_matches(current_pn, collector_pn)
                or _collector_prefix_matches(current_pn, collector_pn)
            )
        ):
            data[CONF_COLLECTOR_PN] = collector_pn
            changed = True

        # The live listener port belongs to the transient SessionHandle in the
        # callback registry.  Do not rewrite a configured entry every time the
        # same collector moves between 8899/18899: that causes config churn,
        # reloads, and a second avoidable reconnect.  Only backfill truly old
        # entries which have no callback port at all.
        if int(data.get(CONF_TCP_PORT) or 0) <= 0 and int(port) > 0:
            data[CONF_TCP_PORT] = int(port)
            changed = True

        if session_protocol and str(data.get("collector_session_protocol") or "") != session_protocol:
            data["collector_session_protocol"] = session_protocol
            changed = True

        # If this entry is already backed by a live callback session, preserve
        # that topology in the durable entry data.  This repairs older entries
        # created before passive callback provenance was persisted.
        if str(data.get(CONF_CONNECTION_MODE) or "").strip() != "callback_listener":
            data[CONF_CONNECTION_MODE] = "callback_listener"
            changed = True
        if (
            str(data.get(CONF_COLLECTOR_OPERATION_MODE) or "").strip()
            == COLLECTOR_OPERATION_HA_ONLY
        ):
            data[CONF_COLLECTOR_OPERATION_MODE] = COLLECTOR_OPERATION_HA_ONLY

        if not changed:
            return

        update_kwargs: dict[str, object] = {"data": data}
        if collector_pn:
            update_kwargs["title"] = _discovery_title(collector_pn, peer_ip)
            current_unique_id = str(getattr(entry, "unique_id", "") or "")
            if current_unique_id.startswith("collector:"):
                update_kwargs["unique_id"] = f"collector:{collector_pn}"
        try:
            async_update_entry(entry, **update_kwargs)
        except Exception:
            logger.debug(
                "Failed to upgrade passive callback entry from live session",
                exc_info=True,
            )


def _discovery_title(collector_pn: str, peer_ip: str) -> str:
    collector_pn = str(collector_pn or "").strip()
    peer_ip = str(peer_ip or "").strip()
    if collector_pn:
        return f"Collector PN {collector_pn}"
    if peer_ip:
        return f"Collector {peer_ip}"
    return "EyeBond collector"


def get_callback_session_registry(hass: HomeAssistant) -> CallbackSessionRegistry | None:
    """Return the domain-level callback session ownership registry, if started."""

    data = getattr(hass, "data", None)
    if not isinstance(data, dict):
        return None
    domain_data = data.get(DOMAIN)
    if not isinstance(domain_data, dict):
        return None
    registry = domain_data.get(_REGISTRY_DATA_KEY)
    return registry if isinstance(registry, CallbackSessionRegistry) else None


def get_passive_callback_discovery(
    hass: HomeAssistant,
) -> PassiveCallbackDiscovery | None:
    """Return the domain passive-discovery service, if it is running."""

    data = getattr(hass, "data", None)
    if not isinstance(data, dict):
        return None
    domain_data = data.get(DOMAIN)
    if not isinstance(domain_data, dict):
        return None
    service = domain_data.get(_DATA_KEY)
    return service if isinstance(service, PassiveCallbackDiscovery) else None


@contextmanager
def active_callback_probe_scope(hass: HomeAssistant, scope_id: str):
    """Attribute sockets created by one active probe to that probe only.

    Active onboarding and passive discovery intentionally share listeners.  A
    callback session caused by a manual or pending one-shot trigger belongs to
    that attempt and must not also create an unrelated discovery card.  The
    service records exact transient session ids; PN and peer IP are never used
    as causal evidence.
    """

    retained_session_ids: set[str] = set()
    service = get_passive_callback_discovery(hass)
    if service is not None:
        service.begin_active_probe_scope(scope_id)
    try:
        yield retained_session_ids
    finally:
        if service is not None:
            service.end_active_probe_scope(
                scope_id,
                retain_session_ids=retained_session_ids,
            )


async def async_start_passive_callback_discovery(hass: HomeAssistant) -> None:
    """Start the domain-level passive callback discovery service."""

    data = hass.data.setdefault(DOMAIN, {})
    service = data.get(_DATA_KEY)
    if isinstance(service, PassiveCallbackDiscovery):
        return
    service = PassiveCallbackDiscovery(hass)
    data[_DATA_KEY] = service
    # Expose the service's own registry so config entries claim/release against
    # the same ownership authority passive discovery publishes from.
    data[_REGISTRY_DATA_KEY] = service.registry
    await service.async_start()

    async def _async_stop_passive_discovery(_event) -> None:
        await service.async_stop()

    bus = getattr(hass, "bus", None)
    async_listen_once = getattr(bus, "async_listen_once", None)
    if callable(async_listen_once):
        async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_stop_passive_discovery)
