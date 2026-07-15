"""Behavioral connection-strategy verification for passive-discovery onboarding.

A collector observed on the passive callback listener is NOT proof of a
permanent inbound configuration: a factory EyeBond collector may only be
connected because an earlier UDP callback trigger made it dial Home Assistant
temporarily, and that link disappears on the collector's next restart. The
strategy therefore must never be inferred from the endpoint/hostname, the cloud
family, the collector type, the peer IP, private/public address shape, or the
mere presence of a TCP session.

This module verifies the strategy through observable device behavior instead:

    observed_session
      -> restart_requested
      -> waiting_for_disconnect
      -> waiting_for_inbound_reconnect
           -> inbound_verified
           -> inbound_not_verified   (config flow continues on the existing
                                      manual callback step)

``inbound`` is confirmed only when the full sequence was observed: the restart
command was confirmed, the old TCP session really disconnected, a NEW session
(new ``session_id``) belonging to the SAME full collector PN appeared, and no
UDP callback trigger was sent by the flow in between. Anything else returns
``inbound_not_verified`` with a typed reason -- it is NOT automatically
classified as ``callback_on_demand``; that classification requires its own
behavioral proof (the existing one-shot manual callback step).

Identity rules follow the session registry: the full collector PN is durable
identity; a short/prefix PN observation alone can never confirm verification
once the full PN is known. Peer IP is never consulted here at all.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Protocol

from ..collector.smartess_local import CollectorManagementUnsupportedError
from ..connection.session_registry import pn_is_same_identity
from ..const import (
    CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
    CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT,
    CONNECTION_STRATEGY_EVIDENCE_USER_CONFIRMED_SESSION,
)

logger = logging.getLogger(__name__)

# --- states -------------------------------------------------------------------
STATE_OBSERVED_SESSION = "observed_session"
STATE_WAITING_FOR_STRONG_IDENTITY = "waiting_for_strong_identity"
STATE_RESTART_REQUESTED = "restart_requested"
STATE_WAITING_FOR_DISCONNECT = "waiting_for_disconnect"
STATE_WAITING_FOR_INBOUND_RECONNECT = "waiting_for_inbound_reconnect"
STATE_INBOUND_VERIFIED = "inbound_verified"
STATE_INBOUND_NOT_VERIFIED = "inbound_not_verified"

# --- strategy / evidence values -----------------------------------------------
STRATEGY_INBOUND = "inbound"
STRATEGY_CALLBACK_ON_DEMAND = "callback_on_demand"
STRATEGY_UNKNOWN = "unknown"

# Evidence values live in the shared const layer so the connection policy never
# has to compare an onboarding literal.
EVIDENCE_REBOOT_RECONNECT = CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT
EVIDENCE_CALLBACK_TRIGGER = CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER
# The user explicitly bound an observed, unclaimed strong-PN session. Honest
# provenance for inbound, but NOT a restart/reconnect proof -- this verifier is
# the only thing allowed to record EVIDENCE_REBOOT_RECONNECT.
EVIDENCE_USER_CONFIRMED_SESSION = CONNECTION_STRATEGY_EVIDENCE_USER_CONFIRMED_SESSION

# --- typed failure reasons ------------------------------------------------------
FAILURE_STRONG_IDENTITY_TIMEOUT = "strong_identity_timeout"
FAILURE_RESTART_NOT_SUPPORTED = "restart_not_supported"
FAILURE_RESTART_NOT_CONFIRMED = "restart_not_confirmed"
FAILURE_DISCONNECT_NOT_OBSERVED = "disconnect_not_observed"
FAILURE_RECONNECT_TIMEOUT = "inbound_reconnect_timeout"
FAILURE_UDP_TRIGGER_OBSERVED = "udp_trigger_during_verification"
FAILURE_SESSION_UNAVAILABLE = "collector_session_unavailable"
FAILURE_SESSION_CLAIMED = "session_claimed_by_other_owner"


class SessionUnavailableError(RuntimeError):
    """The observed session could not be claimed/activated for the restart."""


# Old factory collectors can take well over a minute to boot and re-open their
# outbound link; keep the wait bounded but generous, and poll the in-memory
# session inventory instead of touching the network (no UDP loops).
INBOUND_RECONNECT_TIMEOUT_SECONDS = 180.0
RESTART_DISCONNECT_TIMEOUT_SECONDS = 45.0
STRONG_IDENTITY_TIMEOUT_SECONDS = 30.0
_DEFAULT_POLL_INTERVAL_SECONDS = 0.5

# Listener inventory states that mean the session is gone for good.
_CLOSED_STATE_PREFIXES = ("closed",)
# Inventory states that must never confirm a new inbound session.
_UNTRUSTED_SESSION_STATES = frozenset({"route_identity_mismatch"})


class RestartChannel(Protocol):
    """Transport-side contract for restarting the observed collector session."""

    async def async_send_restart(self) -> None:
        """Send the restart command over the observed session; raise on failure."""

    async def async_probe_identity(self) -> str:
        """Read the authoritative collector PN over the claimed session."""

    def is_connected(self) -> bool:
        """Return whether the observed (old) session is still connected."""

    async def async_close(self) -> None:
        """Release any transport resources (idempotent)."""


@dataclass(slots=True)
class StrategyVerificationResult:
    """Outcome of one behavioral connection-strategy verification."""

    strategy: str = STRATEGY_UNKNOWN
    evidence: str = ""
    failure_reason: str = ""
    new_session_id: str = ""
    collector_pn: str = ""
    state: str = STATE_OBSERVED_SESSION
    transitions: tuple[str, ...] = ()

    @property
    def inbound_verified(self) -> bool:
        return self.strategy == STRATEGY_INBOUND and self.state == STATE_INBOUND_VERIFIED


def _session_state(session: Mapping[str, Any]) -> str:
    return str(session.get("state") or "").strip().lower()


def _session_is_closed(session: Mapping[str, Any]) -> bool:
    state = _session_state(session)
    return any(state.startswith(prefix) for prefix in _CLOSED_STATE_PREFIXES)


def _session_has_strong_identity(session: Mapping[str, Any]) -> bool:
    """Return the registry-provided strong-identity verdict for one session dict.

    The strong/weak decision is centralized in the CallbackSessionRegistry
    (``CallbackSession.has_strong_identity``); sessions_source projections carry
    it as a plain bool. No local identity-source allowlist, no length heuristics.
    """

    return bool(session.get("has_strong_identity"))


class InboundStrategyVerifier:
    """One-shot behavioral verifier: restart the collector, watch it come back.

    All IO is injected: ``restart_channel`` owns the transport used to send the
    restart over the already-observed session, and ``sessions_source`` is the
    public listener-inventory facade (session dicts with ``session_id``,
    ``collector_pn``, ``state``). The verifier itself has no way to send UDP --
    ``udp_trigger_count`` is sampled before/after purely to prove that nothing
    else did either.
    """

    def __init__(
        self,
        *,
        collector_pn: str,
        session_id: str,
        restart_channel: RestartChannel,
        sessions_source: Callable[[], Iterable[Mapping[str, Any]]],
        callback_trigger_generation: Callable[[], int] | None = None,
        promote_claim: Callable[[str], None] | None = None,
        probe_reconnected_identity: Callable[[str], Any] | None = None,
        disconnect_timeout: float = RESTART_DISCONNECT_TIMEOUT_SECONDS,
        reconnect_timeout: float = INBOUND_RECONNECT_TIMEOUT_SECONDS,
        identity_timeout: float = STRONG_IDENTITY_TIMEOUT_SECONDS,
        poll_interval: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        self._collector_pn = str(collector_pn or "").strip()
        self._session_id = str(session_id or "").strip()
        self._restart_channel = restart_channel
        self._sessions_source = sessions_source
        self._callback_trigger_generation = callback_trigger_generation or (lambda: 0)
        # Ownership promotion hook: called with the strong FULL PN right after
        # the identity phase, BEFORE baseline/restart, so the transient
        # session-id claim becomes the durable full-PN claim in the registry.
        # Raising ValueError means another owner holds the identity.
        self._promote_claim = promote_claim
        self._probe_reconnected_identity = probe_reconnected_identity
        self._disconnect_timeout = max(0.0, float(disconnect_timeout))
        self._reconnect_timeout = max(0.0, float(reconnect_timeout))
        self._identity_timeout = max(0.0, float(identity_timeout))
        self._poll_interval = max(0.01, float(poll_interval))
        self._baseline_session_ids: frozenset[str] = frozenset()
        self._transitions: list[str] = [STATE_OBSERVED_SESSION]

    def _enter(self, state: str) -> None:
        self._transitions.append(state)

    def _result(
        self,
        *,
        strategy: str,
        evidence: str = "",
        failure_reason: str = "",
        new_session_id: str = "",
    ) -> StrategyVerificationResult:
        return StrategyVerificationResult(
            strategy=strategy,
            evidence=evidence,
            failure_reason=failure_reason,
            new_session_id=new_session_id,
            collector_pn=self._collector_pn,
            state=self._transitions[-1],
            transitions=tuple(self._transitions),
        )

    def _fail(self, reason: str) -> StrategyVerificationResult:
        self._enter(STATE_INBOUND_NOT_VERIFIED)
        return self._result(strategy=STRATEGY_UNKNOWN, failure_reason=reason)

    def _sessions(self) -> tuple[Mapping[str, Any], ...]:
        try:
            return tuple(self._sessions_source() or ())
        except Exception:
            logger.debug("Strategy verification sessions source failed", exc_info=True)
            return ()

    def _old_session_live(self) -> bool:
        for session in self._sessions():
            if str(session.get("session_id") or "").strip() != self._session_id:
                continue
            return not _session_is_closed(session)
        return False

    def _observed_session_entry(self) -> Mapping[str, Any] | None:
        for session in self._sessions():
            if str(session.get("session_id") or "").strip() == self._session_id:
                return session
        return None

    def _capture_baseline(self) -> None:
        """Record EVERY session id visible before the restart.

        A collector can hold several parallel sessions of the same durable PN.
        Only a session whose id was absent from the WHOLE pre-restart baseline
        can prove the post-reboot dial-in; comparing against the single selected
        old session id is not enough.
        """

        self._baseline_session_ids = frozenset(
            str(session.get("session_id") or "").strip()
            for session in self._sessions()
            if str(session.get("session_id") or "").strip()
        ) | {self._session_id}

    def _find_new_inbound_session(self) -> str:
        """Return the session_id of a NEW live session of the same full PN, or ""."""

        for session in self._sessions():
            session_id = str(session.get("session_id") or "").strip()
            if not session_id or session_id in self._baseline_session_ids:
                # Any pre-restart socket (or its re-listing) can never confirm
                # inbound -- including parallel baseline sessions of the same PN.
                continue
            if _session_is_closed(session):
                continue
            if _session_state(session) in _UNTRUSTED_SESSION_STATES:
                continue
            if not _session_has_strong_identity(session):
                # Only a strong (registry-certified) identity can prove the
                # post-reboot dial-in; weak observations keep waiting.
                continue
            if str(session.get("collector_pn") or "").strip() != self._collector_pn:
                # After strong promotion the durable identity is FINAL: only the
                # exact full PN confirms -- no prefix/length matching, and a
                # different collector behind the same peer IP never matches.
                continue
            return session_id
        return ""

    def _find_new_weak_identity_candidate(self) -> str:
        """Return one new trusted socket needing authoritative PN enrichment."""

        for session in self._sessions():
            session_id = str(session.get("session_id") or "").strip()
            if not session_id or session_id in self._baseline_session_ids:
                continue
            if _session_is_closed(session) or _session_state(session) in _UNTRUSTED_SESSION_STATES:
                continue
            if _session_has_strong_identity(session):
                continue
            session_pn = str(session.get("collector_pn") or "").strip()
            if session_pn and pn_is_same_identity(self._collector_pn, session_pn):
                return session_id
        return ""

    async def async_verify(self) -> StrategyVerificationResult:
        if not self._collector_pn or not self._session_id:
            return self._fail(FAILURE_SESSION_UNAVAILABLE)

        loop = asyncio.get_running_loop()
        generation_before = self._safe_trigger_generation()

        # observed_session -> waiting_for_strong_identity: never restart a
        # collector whose durable identity is not yet strong. Two matching short
        # PNs do not prove identity; the registry is the strong/weak authority.
        self._enter(STATE_WAITING_FOR_STRONG_IDENTITY)
        observed = self._observed_session_entry()
        if observed is None or not _session_has_strong_identity(observed):
            # A passive heartbeat commonly carries only a short PN. Once the
            # user consented, issue one safe read-only FC2 identity query over
            # the exact transient-claimed socket. Its response is recorded in
            # the registry as strong evidence before this coroutine resumes.
            probe_identity = getattr(self._restart_channel, "async_probe_identity", None)
            if callable(probe_identity):
                try:
                    await probe_identity()
                except Exception as exc:
                    logger.info(
                        "Inbound verification: collector identity probe did not complete: %s",
                        exc,
                    )
            observed = self._observed_session_entry()
        deadline = loop.time() + self._identity_timeout
        while True:
            if observed is not None and _session_has_strong_identity(observed):
                strong_pn = str(observed.get("collector_pn") or "").strip()
                if strong_pn and (
                    not self._collector_pn
                    or pn_is_same_identity(self._collector_pn, strong_pn)
                ):
                    if len(strong_pn) >= len(self._collector_pn):
                        # Adopt the enriched full PN as the durable identity.
                        self._collector_pn = strong_pn
                    break
            if loop.time() >= deadline:
                await self._close_channel()
                return self._fail(FAILURE_STRONG_IDENTITY_TIMEOUT)
            await asyncio.sleep(self._poll_interval)
            observed = self._observed_session_entry()

        # Promote the transient session-id claim to the now-final full durable
        # PN BEFORE baseline/restart. A conflict means another owner holds the
        # identity: stop without touching the collector.
        if self._promote_claim is not None:
            try:
                self._promote_claim(self._collector_pn)
            except ValueError as exc:
                logger.info(
                    "Inbound verification: identity %s already claimed during promotion: %s",
                    self._collector_pn,
                    exc,
                )
                await self._close_channel()
                return self._fail(FAILURE_SESSION_CLAIMED)

        # Baseline of ALL currently-visible sessions, captured before restart.
        self._capture_baseline()

        # waiting_for_strong_identity -> restart_requested
        self._enter(STATE_RESTART_REQUESTED)
        try:
            await self._restart_channel.async_send_restart()
        except CollectorManagementUnsupportedError as exc:
            logger.info(
                "Inbound verification: collector %s management unsupported: %s",
                self._collector_pn,
                exc,
            )
            await self._close_channel()
            return self._fail(FAILURE_RESTART_NOT_SUPPORTED)
        except SessionUnavailableError as exc:
            logger.info(
                "Inbound verification: collector %s session unavailable: %s",
                self._collector_pn,
                exc,
            )
            await self._close_channel()
            return self._fail(FAILURE_SESSION_UNAVAILABLE)
        except Exception as exc:
            logger.info(
                "Inbound verification: collector %s restart not confirmed: %s",
                self._collector_pn,
                exc,
            )
            await self._close_channel()
            return self._fail(FAILURE_RESTART_NOT_CONFIRMED)

        try:
            # restart_requested -> waiting_for_disconnect: the collector itself
            # must drop the old TCP session (we never close it ourselves before
            # observing the disconnect, so the EOF is genuine device behavior).
            self._enter(STATE_WAITING_FOR_DISCONNECT)
            deadline = loop.time() + self._disconnect_timeout
            # The registry's physical session_id is the sole disconnect truth.
            # ``RestartChannel`` wraps a reusable transport facade which may
            # immediately attach to a successor socket and remain connected;
            # consulting it here would turn a successful reboot/reconnect into
            # a false ``disconnect_not_observed`` result.
            while self._old_session_live():
                if loop.time() >= deadline:
                    return self._fail(FAILURE_DISCONNECT_NOT_OBSERVED)
                await asyncio.sleep(self._poll_interval)
        finally:
            # Release the (now dead or failed) claimed socket before watching
            # for the collector's fresh dial-in.
            await self._close_channel()

        # waiting_for_disconnect -> waiting_for_inbound_reconnect
        self._enter(STATE_WAITING_FOR_INBOUND_RECONNECT)
        deadline = loop.time() + self._reconnect_timeout
        identity_probe_attempted: set[str] = set()
        while True:
            new_session_id = self._find_new_inbound_session()
            if new_session_id:
                break
            weak_session_id = self._find_new_weak_identity_candidate()
            if (
                weak_session_id
                and weak_session_id not in identity_probe_attempted
                and self._probe_reconnected_identity is not None
            ):
                identity_probe_attempted.add(weak_session_id)
                try:
                    result = self._probe_reconnected_identity(weak_session_id)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    logger.info(
                        "Inbound verification: reconnect identity probe failed for %s: %s",
                        weak_session_id,
                        exc,
                    )
            if loop.time() >= deadline:
                return self._fail(FAILURE_RECONNECT_TIMEOUT)
            await asyncio.sleep(self._poll_interval)

        # The whole point: the collector dialed back IN on its own. If ANY
        # callback trigger was recorded anywhere in the integration meanwhile
        # (this flow, another flow, another entry's runtime), the reconnect
        # proves nothing -- conservatively refuse to certify inbound. A false
        # refusal is safe (manual callback follows); a false inbound is not.
        if self._safe_trigger_generation() != generation_before:
            return self._fail(FAILURE_UDP_TRIGGER_OBSERVED)

        self._enter(STATE_INBOUND_VERIFIED)
        return self._result(
            strategy=STRATEGY_INBOUND,
            evidence=EVIDENCE_REBOOT_RECONNECT,
            new_session_id=new_session_id,
        )

    def _safe_trigger_generation(self) -> int:
        try:
            return int(self._callback_trigger_generation() or 0)
        except Exception:
            return 0

    async def _close_channel(self) -> None:
        with suppress(Exception):
            await self._restart_channel.async_close()


class ObservedSessionRestartChannel:
    """Restart channel over an already-observed passive listener session.

    Claims exactly the observed ``session_id`` through the shared-transport
    claimed-session mechanism (identity-routed by the full collector PN plus the
    registry session id -- never by peer IP) and sends the single shared
    reboot/apply wire command. Sends NO UDP.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        collector_pn: str,
        session_id: str,
        session_id_provider: Callable[[], str] | None = None,
        request_timeout: float = 5.0,
        heartbeat_interval: float = 60.0,
        claim_timeout: float = 5.0,
    ) -> None:
        self._host = str(host or "0.0.0.0")
        self._port = int(port)
        self._collector_pn = str(collector_pn or "").strip()
        self._session_id = str(session_id or "").strip()
        # Registry-owned claims are the ownership authority: when a provider is
        # given (the config flow's registry SessionHandle resolver), it decides
        # which session id the transport may claim.
        self._session_id_provider = session_id_provider
        self._request_timeout = float(request_timeout)
        self._heartbeat_interval = float(heartbeat_interval)
        self._claim_timeout = float(claim_timeout)
        self._transport: Any = None

    def _resolve_session_id(self) -> str:
        """Resolve the registry-owned session id, with NO ownership fallback.

        When a provider (the registry claim resolver) is installed, an empty
        result is an ERROR: the transport must never fall back to the initially
        observed session id, nor be allowed to pick some other socket by PN/IP.
        """

        provider = self._session_id_provider
        if provider is None:
            if not self._session_id:
                raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
            return self._session_id
        try:
            resolved = str(provider() or "").strip()
        except Exception as exc:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE) from exc
        if not resolved:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return resolved

    async def _async_ensure_transport(self):
        """Activate and return the exact registry-owned framed session."""

        if self._transport is not None:
            return self._transport

        from ..collector.transport import SharedEybondTransport

        # Resolve strictly BEFORE touching any socket; a missing registry claim
        # aborts here and no transport is created at all.
        resolved_session_id = self._resolve_session_id()
        transport = SharedEybondTransport(
            host=self._host,
            port=self._port,
            request_timeout=self._request_timeout,
            heartbeat_interval=self._heartbeat_interval,
            collector_ip="",
            collector_pn=self._collector_pn,
        )
        transport.set_claimed_session_provider(lambda: resolved_session_id)
        await transport.start()
        self._transport = transport
        connected = await transport.wait_until_connected(timeout=self._claim_timeout)
        if not connected:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return transport

    async def async_probe_identity(self) -> str:
        """Read full collector identity before deciding whether restart is safe."""

        from ..collector.smartess_local import SmartEssLocalSession

        transport = await self._async_ensure_transport()
        return await SmartEssLocalSession(transport).query_collector_pn()

    async def async_send_restart(self) -> None:
        from ..collector.smartess_local import async_send_collector_reboot_or_apply

        transport = await self._async_ensure_transport()
        await async_send_collector_reboot_or_apply(transport)

    def is_connected(self) -> bool:
        transport = self._transport
        if transport is None:
            return False
        try:
            return bool(transport.connected)
        except Exception:
            return False

    async def async_close(self) -> None:
        transport, self._transport = self._transport, None
        if transport is None:
            return
        with suppress(Exception):
            await transport.stop()
