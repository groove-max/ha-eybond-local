"""Neutral shared timeout policy -- the single source of default timeout values.

The recovery execution layer (``connection/recovery``) and the onboarding scan
layer share ONE timeout policy type and ONE default object, defined here. This
module is a dependency leaf (stdlib only): it imports nothing from
``onboarding``, ``connection``, ``runtime`` or ``config_flow``, so neutral
recovery can take the policy without any back-dependency on onboarding.

Onboarding-specific deadline / scan helpers stay in ``onboarding/timeouts.py``
and import the policy from here; connection / runtime import it from here
directly.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OnboardingTimeoutPolicy:
    """Central timeout policy for onboarding flow wrappers and probe phases."""

    discovery_timeout: float = 1.5
    connect_timeout: float = 5.0
    connect_timeout_without_udp_reply: float = 0.75
    heartbeat_timeout: float = 2.0
    auto_attempts: int = 3
    auto_attempt_delay: float = 0.75
    driver_detection_attempts: int = 3
    driver_retry_delay: float = 0.35
    pi30_qpi_probe_timeout: float = 1.0
    smartess_probe_timeout: float = 3.0
    smartess_query_timeout: float = 1.5
    runtime_enrichment_timeout: float = 4.0
    collector_query_timeout: float = 1.0
    driver_onboarding_read_timeout: float = 2.0
    manual_total_timeout: float = 45.0
    # LINK budget for one callback identity transaction: how long we wait for the
    # collector to actually open a socket after our single trigger sequence. It
    # is not a detection budget -- no driver work happens inside it -- which is
    # why it is a fraction of manual_total_timeout above.
    callback_identity_session_wait: float = 20.0
    # How long one attempt may wait for the exclusive causality lease before
    # giving up. The wire carries no correlation token, so attempts are
    # serialized; this bounds the queue rather than letting a caller hang.
    callback_causality_lease_wait: float = 30.0
    # Inbound recovery verification budgets (the reboot/reconnect transaction).
    # How long the observed session may take to become strongly identified
    # after the consented read-only identity probe.
    inbound_strong_identity_timeout: float = 30.0
    # How long after a confirmed reboot the OLD socket may take to actually
    # close. E500 hardware has been observed applying the acknowledged restart
    # on its one-minute device cadence (60.1s in production). Keep a small
    # scheduler/polling margin beyond that cadence; the collector itself must
    # drop the socket and we never close it on its behalf.
    inbound_restart_disconnect_timeout: float = 65.0
    # Autonomous recovery uses the same bounded session-observation mechanism
    # as callback recovery. One minute is enough for the slow E500 reboot seen
    # in production without making every failed discovery flow block for three
    # minutes. This is an upper bound; a session ends the wait immediately.
    inbound_reconnect_timeout: float = 60.0
    # Callback recovery: after the immediate autonomous snapshot and exactly
    # ONE unicast set>server sequence, how long the collector may take to dial
    # the advertised endpoint. LINK budget only -- no detection runs here.
    #
    # E500 hardware observed in production may accept the reboot, take roughly
    # 30 seconds to drop the old socket, and then need another ~30 seconds after
    # the callback trigger before opening the replacement socket.  The disconnect
    # phase has its own budget above; this value must still cover the latter dial
    # delay instead of declaring a timeout while the proven collector is booting.
    # This is only an upper bound: a faster collector completes immediately.
    callback_recovery_session_wait: float = 60.0
    # The detector owns the manual onboarding work budget above.  A wrapper may
    # wait this little bit longer only so the detector can materialize and
    # return its partial result after its own deadline expires.  This is not
    # additional probe/driver-detection time.
    result_finalization_grace: float = 2.0
    auto_total_timeout: float = 45.0
    auto_scan_estimated_seconds: float = 12.5
    deep_scan_followup_estimated_seconds: float = 75.0
    deep_scan_batch_timeout: float = 0.35
    deep_scan_concurrency: int = 32
    deep_scan_timeout_buffer: float = 20.0
    unicast_fallback_probe_timeout: float = 0.35
    unicast_fallback_concurrency: int = 32
    # Absolute runaway guard for one deep scan. The working deadline grows as
    # connected collectors are admitted for identification; this is the wall
    # it can never grow past.
    deep_scan_hard_ceiling_seconds: float = 900.0


DEFAULT_ONBOARDING_TIMEOUT_POLICY = OnboardingTimeoutPolicy()


__all__ = ["DEFAULT_ONBOARDING_TIMEOUT_POLICY", "OnboardingTimeoutPolicy"]
