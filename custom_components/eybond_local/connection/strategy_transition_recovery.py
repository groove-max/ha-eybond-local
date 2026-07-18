"""Typed persisted strategy-transition recovery state (Batch 8 / 8A).

When an ``inbound -> callback_on_demand`` switch has CONFIRMED the endpoint
restore to the external target but has NOT yet proven the callback strategy,
the entry is in a genuinely unsafe place: the collector's persistent endpoint
already points away from Home Assistant, yet the canonical strategy is still
``inbound``. A bare string marker cannot recover from this after an HA restart:
it holds no route.

This module is the ONE typed, self-contained snapshot that makes the degraded
state genuinely repairable. It carries EXACTLY the fields the repair reads --
the callback route (trigger target, advertised HA endpoint, local listener) and
the durable PN. It is a pure data model (no I/O), so it round-trips byte-stably
through ``entry.data`` and survives a restart.

Trust boundary (Batch 8A):

* the DIRECT constructor is strict and NEVER coerces -- exact ``type() is``
  checks (``bool`` is not ``int``, a ``str`` subclass is not ``str``), required
  strings are non-empty AND already normalized (``value == value.strip()``),
  the PN is already normalized through the ONE central rule, timestamps are
  timezone-aware ISO and byte-stable, ports are real ints in ``1..65535``;
* :meth:`from_record` NEVER raises and is fail-closed: it passes persisted
  values THROUGH to the strict constructor without coercion, so a malformed
  record (padded string, ``"1"``/``1.0``/``true`` schema version, a non-string
  host, a bytes/int/object where a string is required) yields ``None``;
* a constructible object is JSON-safe and byte-stable:
  ``from_record(o.to_record()).to_record() == o.to_record()``;
* diagnostics expose only ``kind`` / timestamps / a boolean route-completeness
  flag (and ports only when the caller's privacy policy allows) -- never the
  raw endpoint or the trigger/advertised/bind addresses.

Only the fields the repair algorithm actually reads are stored: there is no
``restored_endpoint`` and no ``identity_source`` -- neither was read by the
recovery path, and ``identity_source`` must never be persisted as if it were a
strong-identity proof (identity is re-certified live at repair time through the
registry, never trusted from a stored string).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .session_registry import normalize_pn

# The one recovery kind this batch models: an inbound -> callback_on_demand
# transition whose callback strategy was never proven. Named for the TRANSITION,
# not a "restore": the write-ahead PENDING phase persists this kind BEFORE any
# endpoint restore has happened, so a "restore_unproven" name would be dishonest
# there. (The schema is unreleased WIP -- no legacy alias is kept.)
RECOVERY_KIND_CALLBACK_TRANSITION_UNPROVEN = "callback_transition_unproven"
_RECOVERY_KINDS = frozenset({RECOVERY_KIND_CALLBACK_TRANSITION_UNPROVEN})

# Write-ahead lifecycle phases (exhaustive, typed):
#   * PENDING -- the durable intent is persisted, but the remote side effect
#     (endpoint restore or reboot) is NOT yet confirmed / its result unknown;
#   * RESTORE_CONFIRMED_UNPROVEN -- the endpoint restore was CONFIRMED and the
#     policy is now external, but the callback strategy is not yet proven.
# A successful callback proof deletes the state entirely (no third phase).
RECOVERY_PHASE_PENDING = "transition_pending"
RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN = "callback_restore_confirmed_unproven"
_RECOVERY_PHASES = frozenset(
    {RECOVERY_PHASE_PENDING, RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN}
)

_SCHEMA_VERSION = 1

# Callback_on_demand is the only target a degraded restore can recover into.
_TARGET_STRATEGY = "callback_on_demand"

# The exact string fields, split by requiredness. ``trigger_bind_host`` and
# ``listener_bind_host`` are DISTINCT transport concerns: the former is the local
# bind of the UDP ``set>server`` trigger socket; the latter is the actual bind
# host of the shared TCP callback listener (the runtime's real listener bind).
_REQUIRED_HOST_FIELDS = (
    "trigger_target_host",
    "advertised_host",
    "trigger_bind_host",
    "listener_bind_host",
)
_TIMESTAMP_FIELDS = ("created_at", "updated_at")
_PORT_FIELDS = ("trigger_udp_port", "advertised_port", "local_listener_port")


def _is_exact_str(value: object) -> bool:
    return type(value) is str


def _is_exact_int(value: object) -> bool:
    # ``type() is int`` rejects ``bool`` (its own type) -- exactly what we want.
    return type(value) is int


def _aware_iso(value: object) -> str:
    """Return a byte-stable timezone-aware ISO string, or "" otherwise.

    No reformatting: the value is validated and returned verbatim, so a
    round-trip never changes a byte.
    """

    if type(value) is not str:
        return ""
    text = value
    if text != text.strip() or not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return ""
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return ""
    return text


def _require_aware_iso(value: object) -> str:
    """Strict variant of :func:`_aware_iso` for the writer transition APIs.

    A writer that refreshes ``updated_at`` must NEVER silently keep the old
    timestamp when handed a bad ``now`` -- that would persist a state whose
    ``updated_at`` lies about when it was written. So a non-string raises
    ``TypeError`` and an empty/naive/padded/non-ISO string raises ``ValueError``;
    no object is produced on invalid input.
    """

    if type(value) is not str:
        raise TypeError("recovery_state_now_must_be_str")
    if _aware_iso(value) != value:
        raise ValueError("recovery_state_now_not_aware")
    return value


@dataclass(frozen=True, slots=True)
class StrategyTransitionRecoveryState:
    """A strict, JSON-safe, self-contained degraded-transition snapshot.

    Direct construction validates every field and raises on any malformed
    input. Parsing untrusted persisted data must go through :meth:`from_record`,
    which never raises.
    """

    collector_pn: str
    created_at: str
    updated_at: str
    trigger_target_host: str
    trigger_udp_port: int
    advertised_host: str
    advertised_port: int
    trigger_bind_host: str
    listener_bind_host: str
    local_listener_port: int
    phase: str = RECOVERY_PHASE_PENDING
    kind: str = RECOVERY_KIND_CALLBACK_TRANSITION_UNPROVEN
    target_strategy: str = _TARGET_STRATEGY
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        # schema_version: exact int (not bool), exactly the current version.
        if not _is_exact_int(self.schema_version) or self.schema_version != _SCHEMA_VERSION:
            raise ValueError("recovery_state_schema_version_invalid")
        if not _is_exact_str(self.kind) or self.kind not in _RECOVERY_KINDS:
            raise ValueError("recovery_state_kind_invalid")
        if not _is_exact_str(self.phase) or self.phase not in _RECOVERY_PHASES:
            raise ValueError("recovery_state_phase_invalid")
        if not _is_exact_str(self.target_strategy) or self.target_strategy != _TARGET_STRATEGY:
            raise ValueError("recovery_state_target_invalid")
        # collector_pn: exact str, non-empty, already normalized.
        if (
            not _is_exact_str(self.collector_pn)
            or not self.collector_pn
            or normalize_pn(self.collector_pn) != self.collector_pn
        ):
            raise ValueError("recovery_state_pn_not_normalized")
        # timestamps: exact str, NON-EMPTY, aware ISO, byte-stable.
        for field_name in _TIMESTAMP_FIELDS:
            value = getattr(self, field_name)
            if not _is_exact_str(value) or not value or _aware_iso(value) != value:
                raise ValueError(f"recovery_state_{field_name}_not_aware")
        # required hosts: exact str, non-empty, already normalized (no padding).
        for host_field in _REQUIRED_HOST_FIELDS:
            value = getattr(self, host_field)
            if not _is_exact_str(value) or not value or value != value.strip():
                raise ValueError(f"recovery_state_{host_field}_invalid")
        # ports: exact int (not bool), 1..65535.
        for port_field in _PORT_FIELDS:
            value = getattr(self, port_field)
            if not _is_exact_int(value) or not (1 <= value <= 65535):
                raise ValueError(f"recovery_state_{port_field}_invalid")

    # -- construction (the WRITER path: strict, no arbitrary coercion) -----
    @classmethod
    def create(
        cls,
        *,
        collector_pn: object,
        now: object,
        trigger_target_host: object,
        trigger_udp_port: object,
        advertised_host: object,
        advertised_port: object,
        trigger_bind_host: object,
        listener_bind_host: object,
        local_listener_port: object,
        phase: str = RECOVERY_PHASE_PENDING,
    ) -> "StrategyTransitionRecoveryState":
        """Build a fresh state from live runtime values.

        Strict on the writer side too: hosts / timestamp / PN must be REAL
        ``str`` (no ``str()`` of an arbitrary object), ports must be REAL
        ``int`` (``bool`` rejected -- no ``int()`` coercion). Malformed writer
        input raises ``TypeError`` here, never becomes a look-alike value. The
        only normalization applied is ``normalize_pn`` on an already-``str`` PN
        and ``.strip()`` on already-``str`` hosts.
        """

        def _require_str(value: object, name: str) -> str:
            if type(value) is not str:
                raise TypeError(f"recovery_state_{name}_must_be_str")
            return value

        def _require_port(value: object, name: str) -> int:
            if type(value) is not int:  # rejects bool (its own type)
                raise TypeError(f"recovery_state_{name}_must_be_int")
            return value

        stamp = _require_str(now, "now")
        return cls(
            collector_pn=normalize_pn(_require_str(collector_pn, "collector_pn")),
            created_at=stamp,
            updated_at=stamp,
            trigger_target_host=_require_str(trigger_target_host, "trigger_target_host").strip(),
            trigger_udp_port=_require_port(trigger_udp_port, "trigger_udp_port"),
            advertised_host=_require_str(advertised_host, "advertised_host").strip(),
            advertised_port=_require_port(advertised_port, "advertised_port"),
            trigger_bind_host=_require_str(trigger_bind_host, "trigger_bind_host").strip(),
            listener_bind_host=_require_str(listener_bind_host, "listener_bind_host").strip(),
            local_listener_port=_require_port(local_listener_port, "local_listener_port"),
            phase=phase,
        )

    def with_phase(self, phase: str, *, now: object) -> "StrategyTransitionRecoveryState":
        """Return a copy in ``phase`` with ``updated_at`` refreshed.

        ``now`` is validated STRICTLY first: an invalid/empty/naive/non-string
        ``now`` raises (``TypeError``/``ValueError``) and NO new object is
        created -- the phase transition never silently reuses the old timestamp.
        """

        from dataclasses import replace

        stamp = _require_aware_iso(now)
        return replace(self, phase=phase, updated_at=stamp)

    def touched(self, *, now: object) -> "StrategyTransitionRecoveryState":
        """Return a copy with ``updated_at`` refreshed (idempotent re-persist).

        Same strict ``now`` contract as :meth:`with_phase`: a bad ``now`` raises
        and produces no object.
        """

        from dataclasses import replace

        stamp = _require_aware_iso(now)
        return replace(self, updated_at=stamp)

    # -- persistence -------------------------------------------------------
    def to_record(self) -> dict[str, Any]:
        """A byte-stable, JSON-safe mapping for ``entry.data`` storage."""

        return {
            "schema_version": self.schema_version,
            "kind": self.kind,
            "target_strategy": self.target_strategy,
            "phase": self.phase,
            "collector_pn": self.collector_pn,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "trigger_target_host": self.trigger_target_host,
            "trigger_udp_port": self.trigger_udp_port,
            "advertised_host": self.advertised_host,
            "advertised_port": self.advertised_port,
            "trigger_bind_host": self.trigger_bind_host,
            "listener_bind_host": self.listener_bind_host,
            "local_listener_port": self.local_listener_port,
        }

    @classmethod
    def from_record(cls, record: object) -> "StrategyTransitionRecoveryState | None":
        """Parse an untrusted persisted record. NEVER raises; fail-closed.

        Values are passed THROUGH to the strict constructor with NO coercion:
        a padded string, a non-string host, a ``"1"``/``1.0``/``true`` schema
        version, or a missing key all raise inside ``__post_init__`` and are
        turned into ``None`` here.

        ``phase`` is a REQUIRED persisted field read from the record and passed
        through to the strict constructor -- the dataclass default is NEVER used
        when parsing a persisted record, so a missing / unknown / non-string /
        padded phase fail-closes to ``None`` instead of silently defaulting to
        ``transition_pending``.
        """

        if type(record) is not dict:
            return None
        _MISSING = object()
        try:
            return cls(
                collector_pn=record.get("collector_pn", _MISSING),
                created_at=record.get("created_at", _MISSING),
                updated_at=record.get("updated_at", _MISSING),
                trigger_target_host=record.get("trigger_target_host", _MISSING),
                trigger_udp_port=record.get("trigger_udp_port", _MISSING),
                advertised_host=record.get("advertised_host", _MISSING),
                advertised_port=record.get("advertised_port", _MISSING),
                trigger_bind_host=record.get("trigger_bind_host", _MISSING),
                listener_bind_host=record.get("listener_bind_host", _MISSING),
                local_listener_port=record.get("local_listener_port", _MISSING),
                phase=record.get("phase", _MISSING),
                kind=record.get("kind", _MISSING),
                target_strategy=record.get("target_strategy", _MISSING),
                schema_version=record.get("schema_version", _MISSING),
            )
        except (ValueError, TypeError):
            return None

    # -- diagnostics (privacy-aware) --------------------------------------
    def diagnostics(self, *, include_ports: bool = False) -> dict[str, Any]:
        """A redacted view for support diagnostics.

        Never exposes the raw endpoint or the trigger/advertised/bind
        addresses. Only the kind, timestamps, a boolean route-completeness flag
        and (optionally, when the caller's privacy policy allows) the ports.
        """

        view: dict[str, Any] = {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "target_strategy": self.target_strategy,
            "phase": self.phase,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "route_complete": self.route_is_complete(),
        }
        if include_ports:
            view["trigger_udp_port"] = self.trigger_udp_port
            view["advertised_port"] = self.advertised_port
            view["local_listener_port"] = self.local_listener_port
        return view

    # -- repair route ------------------------------------------------------
    def route_is_complete(self) -> bool:
        """Whether every field a repair needs is present and sane."""

        return bool(
            self.trigger_target_host
            and self.trigger_udp_port
            and self.advertised_host
            and self.advertised_port
            and self.trigger_bind_host
            and self.listener_bind_host
            and self.local_listener_port
        )

    def callback_route(self) -> Any:
        """Build the exact :class:`CallbackRecoveryRoute` for repair, or None.

        ``bind_ip`` is the UDP TRIGGER bind (``trigger_bind_host``) -- NOT the TCP
        listener bind. The TCP listener bind is :attr:`listener_bind_host`, used by
        the bootstrap channel / observed-listener lease / Phase-B listener.
        """

        if not self.route_is_complete():
            return None
        from ..onboarding.strategy_verification import CallbackRecoveryRoute

        return CallbackRecoveryRoute(
            bind_ip=self.trigger_bind_host,
            trigger_target_ip=self.trigger_target_host,
            trigger_udp_port=self.trigger_udp_port,
            advertised_ha_host=self.advertised_host,
            advertised_ha_port=self.advertised_port,
            listener_port=self.local_listener_port,
        )


__all__ = [
    "RECOVERY_KIND_CALLBACK_TRANSITION_UNPROVEN",
    "RECOVERY_PHASE_PENDING",
    "RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN",
    "StrategyTransitionRecoveryState",
]
