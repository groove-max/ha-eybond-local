"""Generic driver-probing helpers independent of one physical transport."""

from __future__ import annotations

import logging
import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable

from ..drivers.base import InverterDriver
from ..drivers.catalog_identity import (
    ERROR_INVERTER_LINK_DOWN,
    InverterIdentityNoDataError,
)
from ..drivers.registry import iter_drivers
from ..metadata.compiled_detection_catalog import (
    CompiledDeviceDescriptor,
    CompiledSurfaceDescriptor,
    load_compiled_detection_catalog,
)
from ..models import DetectedInverter, DriverMatch, ProbeTarget

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DetectedDriverContext:
    """The concrete matched driver plus a serializable match summary."""

    driver: InverterDriver
    inverter: DetectedInverter
    match: DriverMatch


@dataclass(slots=True)
class DriverCandidateScan:
    """All driver candidates found by one deep probe plus budget status.

    ``probe_log`` records what each driver probe actually cost and how it
    ended, so real installations produce the evidence needed to tune the
    per-driver budgets instead of guessing.
    """

    candidates: tuple[DetectedDriverContext, ...] = field(default_factory=tuple)
    budget_exhausted: bool = False
    probe_log: tuple[dict[str, object], ...] = field(default_factory=tuple)


@dataclass(slots=True)
class DriverDetectionDeadline:
    """Shared deadline for driver ordering and probing.

    Per-driver signature/probe timeouts are still authoritative upper bounds;
    this object clamps them to the caller's remaining detection budget so a
    newly added protocol cannot silently add another full timeout chain to one
    target scan.
    """

    remaining_seconds: Callable[[], float | None] | None = None

    def remaining(self) -> float | None:
        if self.remaining_seconds is None:
            return None
        value = self.remaining_seconds()
        if value is None:
            return None
        return max(0.0, float(value))

    def expired(self) -> bool:
        remaining = self.remaining()
        return remaining is not None and remaining <= 0

    def timeout(self, configured: float | None) -> float | None:
        base = float(configured or 0.0)
        remaining = self.remaining()
        if remaining is None:
            return base if base > 0 else None
        if base <= 0:
            return remaining
        return min(base, remaining)


def driver_keys_for_profile_prefixes(profile_names: Any) -> tuple[str, ...]:
    """Map catalog profile paths to registered driver keys, order-preserving.

    SmartESS collector metadata names the protocol profile (for example
    ``pi30_ascii/models/smartess_0925_compat.json``); the path prefix
    identifies the local driver that speaks it.
    """

    catalog = load_compiled_detection_catalog()
    surface_map: dict[str, str] = {}
    for surface in catalog.surfaces.values():
        prefix = str(surface.profile_name or "").split("/", 1)[0]
        if prefix:
            surface_map.setdefault(prefix, surface.driver_key)
    registered = {driver.key for driver in iter_drivers("auto")}
    keys: list[str] = []
    for name in profile_names or ():
        prefix = str(name or "").split("/", 1)[0]
        if not prefix:
            continue
        key = surface_map.get(prefix) or (prefix if prefix in registered else "")
        if key and key not in keys:
            keys.append(key)
    return tuple(keys)


async def _ordered_driver_targets(
    transport: Any,
    *,
    driver_hint: str,
    preferred_driver_keys: tuple[str, ...] = (),
    allowed_driver_keys: tuple[str, ...] = (),
    deadline: DriverDetectionDeadline | None = None,
) -> tuple[tuple[InverterDriver, tuple[ProbeTarget, ...]], ...]:
    """Return the probe order, seeded by collector metadata when available.

    A metadata hint (SmartESS protocol profile) names the driver the collector
    itself reports, so it outranks the signature pre-pass — and skipping that
    pre-pass saves its per-driver probe budget, which matters inside the
    shared deep-scan deadline.

    ``allowed_driver_keys`` must restrict the set BEFORE the signature
    pre-pass: a restricted re-sweep (the link baud walk) would otherwise
    still spend wire probes signing drivers it is never going to run.
    """

    driver_targets = tuple(
        (driver, _ordered_probe_targets(driver, transport))
        for driver in iter_drivers(driver_hint)
        if not allowed_driver_keys or driver.key in allowed_driver_keys
    )
    preferred = {key for key in preferred_driver_keys if key}
    if preferred and any(driver.key in preferred for driver, _ in driver_targets):
        logger.debug("Driver order seeded by collector metadata hint: %s", sorted(preferred))
        return tuple(
            sorted(
                driver_targets,
                key=lambda item: 0 if item[0].key in preferred else 1,
            )
        )
    return await _ordered_driver_targets_by_signature(
        driver_targets,
        transport,
        deadline=deadline,
    )


class DriverSweepNoMatch(RuntimeError):
    """No driver matched; carries a structured verdict about the failure.

    ``silent`` is computed from what the sweep actually observed — True only
    when NO probe saw an inverter response (every attempt timed out or the
    identity region read as zeros).  Consumers must prefer this attribute
    over parsing the error string: a future outcome like
    ``answered_then_probe_timeout`` would fool a string suffix check but not
    the tracked verdict.
    """

    def __init__(
        self,
        message: str,
        *,
        silent: bool,
        probe_log: tuple[dict[str, object], ...] = (),
    ) -> None:
        super().__init__(message)
        self.silent = silent
        self.probe_log = probe_log


async def async_detect_inverter(
    transport: Any,
    *,
    driver_hint: str,
    depth: str = "fast",
    preferred_driver_keys: tuple[str, ...] = (),
    remaining_seconds: Callable[[], float | None] | None = None,
) -> DetectedDriverContext:
    """Probe all drivers against one active transport and return the first match."""

    errors: list[str] = []
    probe_log: list[dict[str, object]] = []
    inverter_link_down = False
    deadline = DriverDetectionDeadline(remaining_seconds)
    logger.debug("Starting inverter driver detection depth=%s hint=%s", depth, driver_hint)
    driver_targets = await _ordered_driver_targets(
        transport,
        driver_hint=driver_hint,
        preferred_driver_keys=preferred_driver_keys,
        deadline=deadline,
    )

    loop = asyncio.get_running_loop()
    saw_any_response = False
    for index, (driver, targets) in enumerate(driver_targets):
        if deadline.expired():
            probe_log.extend(
                {"driver": pending.key, "elapsed_ms": 0, "outcome": "skipped_budget_exhausted"}
                for pending, _ in driver_targets[index:]
            )
            break
        probe_started = loop.time()
        tracked = _ResponseTrackingTransport(transport)
        try:
            inverter = await _async_probe_driver_with_budget(
                driver,
                tracked,
                targets,
                deadline=deadline,
            )
        except asyncio.TimeoutError:
            saw_any_response = saw_any_response or tracked.saw_response
            errors.append(f"{driver.key}:probe_timeout")
            _append_probe_log(
                probe_log,
                driver=driver,
                started=probe_started,
                outcome="probe_timeout",
                saw_response=tracked.saw_response,
                loop=loop,
            )
            logger.debug("Probe timed out driver=%s timeout=%s", driver.key, driver.probe_timeout)
            continue
        except InverterIdentityNoDataError:
            # The identity registers read as zeros: the collector currently has
            # no inverter link. Remaining drivers still get their chance, but a
            # fully failed detection must surface the link problem instead of
            # the misleading "no supported driver" verdict.
            errors.append(f"{driver.key}:{ERROR_INVERTER_LINK_DOWN}")
            inverter_link_down = True
            _append_probe_log(
                probe_log,
                driver=driver,
                started=probe_started,
                outcome=ERROR_INVERTER_LINK_DOWN,
                saw_response=False,
                loop=loop,
            )
            logger.debug("Identity region read as zeros driver=%s", driver.key)
            continue
        except Exception as exc:
            saw_any_response = saw_any_response or tracked.saw_response
            errors.append(f"{driver.key}:{exc}")
            _append_probe_log(
                probe_log,
                driver=driver,
                started=probe_started,
                outcome=f"error:{str(exc)[:80]}",
                saw_response=tracked.saw_response,
                loop=loop,
            )
            logger.debug("Probe failed driver=%s error=%s", driver.key, exc)
            continue

        saw_any_response = saw_any_response or tracked.saw_response
        if inverter is not None:
            elapsed_ms = _append_probe_log(
                probe_log,
                driver=driver,
                started=probe_started,
                outcome="matched",
                saw_response=True,
                loop=loop,
            )
            inverter.details["probe_elapsed_ms"] = elapsed_ms
            inverter.details["probe_log"] = list(probe_log)
            return DetectedDriverContext(
                driver=driver,
                inverter=inverter,
                match=_build_driver_match(driver, inverter),
            )
        _append_probe_log(
            probe_log,
            driver=driver,
            started=probe_started,
            outcome="no_match",
            saw_response=tracked.saw_response,
            loop=loop,
        )

    silent = not saw_any_response
    if inverter_link_down:
        raise DriverSweepNoMatch(
            ERROR_INVERTER_LINK_DOWN,
            silent=silent,
            probe_log=tuple(probe_log),
        )
    raise DriverSweepNoMatch(
        errors[-1] if errors else "no_supported_driver_matched",
        silent=silent,
        probe_log=tuple(probe_log),
    )


async def async_detect_inverter_candidates(
    transport: Any,
    *,
    driver_hint: str,
    depth: str = "deep",
    remaining_seconds: Callable[[], float | None] | None = None,
    preferred_driver_keys: tuple[str, ...] = (),
    allowed_driver_keys: tuple[str, ...] = (),
) -> DriverCandidateScan:
    """Probe all drivers and return every successful driver candidate.

    ``remaining_seconds`` exposes the shared onboarding deadline. When it runs
    out mid-scan, the candidates found so far are returned with
    ``budget_exhausted=True`` instead of being discarded by the outer timeout.
    """

    errors: list[str] = []
    candidates: list[DetectedDriverContext] = []
    probe_log: list[dict[str, object]] = []
    inverter_link_down = False
    budget_exhausted = False
    deadline = DriverDetectionDeadline(remaining_seconds)
    logger.debug("Starting multi-candidate inverter detection depth=%s hint=%s", depth, driver_hint)
    # Restricted re-sweeps (the link baud walk) probe only the drivers whose
    # protocol family is expected at the current link speed; the restriction
    # applies before the signature pre-pass inside.
    driver_targets = await _ordered_driver_targets(
        transport,
        driver_hint=driver_hint,
        preferred_driver_keys=preferred_driver_keys,
        allowed_driver_keys=allowed_driver_keys,
        deadline=deadline,
    )

    loop = asyncio.get_running_loop()

    def _log_probe(
        driver: InverterDriver,
        started: float,
        outcome: str,
        *,
        saw_response: bool = False,
    ) -> int:
        elapsed_ms = int(round(max(0.0, loop.time() - started) * 1000.0))
        probe_log.append(
            {
                "driver": driver.key,
                "elapsed_ms": elapsed_ms,
                "outcome": outcome,
                "saw_response": saw_response,
            }
        )
        return elapsed_ms

    for index, (driver, targets) in enumerate(driver_targets):
        remaining = deadline.remaining()
        if remaining is not None and remaining <= 0:
            budget_exhausted = True
            logger.debug("Deep detection budget exhausted before driver=%s", driver.key)
            probe_log.extend(
                {"driver": pending.key, "elapsed_ms": 0, "outcome": "skipped_budget_exhausted"}
                for pending, _ in driver_targets[index:]
            )
            break
        probe_started = loop.time()
        tracked = _ResponseTrackingTransport(transport)
        try:
            probe = _async_probe_driver_with_budget(
                driver,
                tracked,
                targets,
                deadline=deadline,
            )
            if remaining is not None:
                inverter = await asyncio.wait_for(probe, timeout=remaining)
            else:
                inverter = await probe
        except asyncio.TimeoutError:
            errors.append(f"{driver.key}:probe_timeout")
            _log_probe(driver, probe_started, "probe_timeout", saw_response=tracked.saw_response)
            logger.debug("Probe timed out driver=%s timeout=%s", driver.key, driver.probe_timeout)
            continue
        except InverterIdentityNoDataError:
            errors.append(f"{driver.key}:{ERROR_INVERTER_LINK_DOWN}")
            inverter_link_down = True
            # The collector answered with zeros — the INVERTER did not speak;
            # this must never count as a link-level response.
            _log_probe(driver, probe_started, ERROR_INVERTER_LINK_DOWN, saw_response=False)
            logger.debug("Identity region read as zeros driver=%s", driver.key)
            continue
        except Exception as exc:
            errors.append(f"{driver.key}:{exc}")
            _log_probe(
                driver,
                probe_started,
                f"error:{str(exc)[:80]}",
                saw_response=tracked.saw_response,
            )
            logger.debug("Probe failed driver=%s error=%s", driver.key, exc)
            continue

        if inverter is not None:
            elapsed_ms = _log_probe(driver, probe_started, "matched", saw_response=True)
            # Measured identification time: the driver-choice step shows it so
            # the user can compare candidates on something real.
            inverter.details["probe_elapsed_ms"] = elapsed_ms
            candidates.append(
                DetectedDriverContext(
                    driver=driver,
                    inverter=inverter,
                    match=_build_driver_match(driver, inverter),
                )
            )
        else:
            # None from async_probe is NOT evidence of an answer: drivers
            # swallow read timeouts internally. Only the transport-observed
            # response counter may claim the inverter spoke.
            _log_probe(driver, probe_started, "no_match", saw_response=tracked.saw_response)

    if candidates or budget_exhausted:
        return DriverCandidateScan(
            candidates=tuple(candidates),
            budget_exhausted=budget_exhausted,
            probe_log=tuple(probe_log),
        )
    silent = not any(entry.get("saw_response") for entry in probe_log)
    if inverter_link_down:
        raise DriverSweepNoMatch(
            ERROR_INVERTER_LINK_DOWN, silent=silent, probe_log=tuple(probe_log)
        )
    raise DriverSweepNoMatch(
        errors[-1] if errors else "no_supported_driver_matched",
        silent=silent,
        probe_log=tuple(probe_log),
    )


async def _ordered_driver_targets_by_signature(
    driver_targets: tuple[tuple[InverterDriver, tuple[ProbeTarget, ...]], ...],
    transport: Any,
    *,
    deadline: DriverDetectionDeadline | None = None,
) -> tuple[tuple[InverterDriver, tuple[ProbeTarget, ...]], ...]:
    unsigned: list[tuple[InverterDriver, tuple[ProbeTarget, ...]]] = []

    for index, (driver, targets) in enumerate(driver_targets):
        signature_timeout = getattr(driver, "signature_timeout", None)
        if signature_timeout is None or signature_timeout <= 0:
            unsigned.append((driver, targets))
            continue
        if deadline is not None and deadline.expired():
            return tuple((*unsigned, *driver_targets[index:]))
        try:
            matched = await _async_probe_driver_signature(
                driver,
                transport,
                targets,
                deadline=deadline,
            )
        except Exception as exc:
            logger.debug("Signature probe failed driver=%s error=%s", driver.key, exc)
            matched = False
        if matched:
            remaining = driver_targets[index + 1 :]
            logger.debug("Driver signature matched driver=%s; prioritizing without probing remaining signatures", driver.key)
            return tuple(((driver, targets), *unsigned, *remaining))
        unsigned.append((driver, targets))

    return tuple(unsigned)


async def _async_probe_driver_signature(
    driver: InverterDriver,
    transport: Any,
    targets: tuple[ProbeTarget, ...],
    *,
    deadline: DriverDetectionDeadline | None = None,
) -> bool:
    configured_timeout = getattr(driver, "signature_timeout", None)
    timeout = (
        deadline.timeout(configured_timeout)
        if deadline is not None
        else configured_timeout
    )
    if timeout is None or timeout <= 0:
        return False
    try:
        return await asyncio.wait_for(
            _async_probe_driver_signature_targets(driver, transport, targets),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        logger.debug("Signature probe timed out driver=%s timeout=%s", driver.key, timeout)
        return False


async def _async_probe_driver_signature_targets(
    driver: InverterDriver,
    transport: Any,
    targets: tuple[ProbeTarget, ...],
) -> bool:
    for target in targets:
        try:
            if await driver.async_probe_signature(transport, target):
                return True
        except Exception as exc:
            logger.debug("Signature probe failed driver=%s target=%s error=%s", driver.key, target, exc)
    return False


class _ResponseTrackingTransport:
    """Delegating transport proxy that records observed payload responses.

    Drivers swallow read timeouts internally and return ``None`` from
    ``async_probe``, so "no match" alone is NOT evidence that the inverter
    ever answered.  The proxy watches the payload-level send methods: any
    non-empty response means bytes actually came back over the link during
    this probe.  This is the ground truth the silence verdict is built from.
    """

    _WATCHED = ("async_send_forward", "async_send_payload", "async_send_collector")

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.responses = 0

    @property
    def saw_response(self) -> bool:
        return self.responses > 0

    def __getattr__(self, name: str):
        attr = getattr(self._inner, name)
        if name not in self._WATCHED or not callable(attr):
            return attr

        async def _watched(*args: Any, **kwargs: Any):
            result = await attr(*args, **kwargs)
            payload = result[1] if isinstance(result, tuple) and len(result) > 1 else result
            if payload:
                self.responses += 1
            return result

        return _watched


async def _async_probe_driver_with_budget(
    driver: InverterDriver,
    transport: Any,
    targets: tuple[ProbeTarget, ...],
    *,
    deadline: DriverDetectionDeadline | None = None,
) -> DetectedInverter | None:
    configured_timeout = getattr(driver, "probe_timeout", None)
    timeout = (
        deadline.timeout(configured_timeout)
        if deadline is not None
        else configured_timeout
    )
    if timeout is None or timeout <= 0:
        return await _async_probe_driver_targets(driver, transport, targets)
    return await asyncio.wait_for(
        _async_probe_driver_targets(driver, transport, targets),
        timeout=timeout,
    )


async def _async_probe_driver_targets(
    driver: InverterDriver,
    transport: Any,
    targets: tuple[ProbeTarget, ...],
) -> DetectedInverter | None:
    for target in targets:
        try:
            inverter = await driver.async_probe(transport, target)
        except InverterIdentityNoDataError:
            raise
        except Exception as exc:
            logger.debug("Probe failed driver=%s target=%s error=%s", driver.key, target, exc)
            continue

        if inverter is not None:
            return inverter
    return None


def _ordered_probe_targets(driver: InverterDriver, transport: Any) -> tuple[ProbeTarget, ...]:
    probe_targets = tuple(getattr(driver, "probe_targets", ()))
    if not probe_targets:
        return ()
    collector_info = getattr(transport, "collector_info", None)
    preferred_devcodes: tuple[int, ...] = ()
    if collector_info is not None:
        seen_devcodes: list[int] = []
        for attribute in ("heartbeat_devcode", "last_devcode"):
            value = getattr(collector_info, attribute, None)
            if isinstance(value, int) and value not in seen_devcodes:
                seen_devcodes.append(value)
        preferred_devcodes = tuple(seen_devcodes)

    original_order = {
        (target.devcode, target.collector_addr, target.device_addr): index
        for index, target in enumerate(probe_targets)
    }

    def _sort_key(target: ProbeTarget) -> tuple[int, int, int]:
        original_index = original_order[(target.devcode, target.collector_addr, target.device_addr)]
        devcode_rank = 0
        if preferred_devcodes:
            devcode_rank = 0 if target.devcode in preferred_devcodes else 1
        return (devcode_rank, original_index, original_index)

    return tuple(sorted(probe_targets, key=_sort_key))


def _append_probe_log(
    probe_log: list[dict[str, object]],
    *,
    driver: InverterDriver,
    started: float,
    outcome: str,
    saw_response: bool,
    loop,
) -> int:
    elapsed_ms = int(round(max(0.0, loop.time() - started) * 1000.0))
    probe_log.append(
        {
            "driver": driver.key,
            "elapsed_ms": elapsed_ms,
            "outcome": outcome,
            "saw_response": bool(saw_response),
        }
    )
    return elapsed_ms


def _build_driver_match(driver: InverterDriver, inverter: DetectedInverter) -> DriverMatch:
    reasons = []
    confidence = "medium"
    if inverter.protocol_family:
        reasons.append("protocol_family_present")
    if inverter.model_name:
        reasons.append("model_name_present")
    if inverter.serial_number:
        reasons.append("serial_number_present")
    if inverter.details.get("rated_power"):
        reasons.append("rated_power_present")
    surface, candidates = _catalog_surface_context(inverter)
    if surface is not None and surface.read_only and inverter.variant_key == "family_fallback":
        reasons.append("family_fallback_variant")
    elif candidates and all(
        candidate.provenance_confidence == "rule-migrated"
        for candidate in candidates
    ):
        reasons.append("unverified_variant")
    elif (
        (surface is not None and surface.read_only)
        or str(inverter.profile_name or "").strip().endswith("/family_fallback.json")
    ):
        reasons.append("read_only_profile")
    elif inverter.protocol_family and inverter.model_name and inverter.serial_number:
        confidence = "high"

    return DriverMatch(
        driver_key=driver.key,
        protocol_family=inverter.protocol_family,
        model_name=inverter.model_name,
        variant_key=inverter.variant_key,
        serial_number=inverter.serial_number,
        probe_target=inverter.probe_target,
        confidence=confidence,
        reasons=tuple(reasons),
        details=dict(inverter.details),
    )


def _catalog_surface_context(
    inverter: DetectedInverter,
) -> tuple[
    CompiledSurfaceDescriptor | None,
    tuple[CompiledDeviceDescriptor, ...],
]:
    catalog = load_compiled_detection_catalog()
    surfaces = tuple(
        surface
        for surface in catalog.surfaces.values()
        if surface.driver_key == inverter.driver_key
        and surface.variant_key == inverter.variant_key
        and surface.profile_name == inverter.profile_name
        and (
            not inverter.register_schema_name
            or surface.register_schema_name == inverter.register_schema_name
        )
    )
    if len(surfaces) != 1:
        return None, ()
    surface = surfaces[0]
    candidates = tuple(
        catalog.devices[key]
        for key in catalog.devices_by_surface.get(surface.key, ())
        if key in catalog.devices
    )
    return surface, candidates
