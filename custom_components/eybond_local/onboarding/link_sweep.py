"""Inverter-link baud sweep for ESP bridge collectors during deep scan.

The catalog is descriptive: device packs and protocol families declare the
baud rates their inverters are usually found at (``link_hints``).  When a
deep scan finds a live collector but TOTAL SILENCE on the inverter UART, and
the collector is our own esp-eybond-collector on a platform that supports
runtime UART reconfiguration (advertised through the hardware token), the
scan may walk the small closed set of catalog baud rates: set the speed,
re-run the driver sweep, and either keep the speed that answered or restore
the original one.

The sweep is transactional and bounded:

* Only runs on explicit deep scans, never quick scans.
* Only when the flashed speed produced *silence* (``inverter_link_down`` or
  every probe timing out) — a device that answered but did not match is a
  catalog problem, not a link problem.
* Restores the original baud when nothing answered anywhere.

Original factory collectors and fixed-UART builds (bk72xx) never enter the
sweep; for them the same hints become remediation text instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Awaitable, Callable

from ..metadata.device_catalog_loader import load_device_catalog

logger = logging.getLogger(__name__)

ERROR_INVERTER_LINK_DOWN = "inverter_link_down"


def is_silent_detection_error(error: str) -> bool:
    """Return whether a failed driver sweep saw no inverter bytes at all.

    ``inverter_link_down`` is the sweep's explicit all-zero/no-response
    verdict; an error trail that ends in a probe timeout means the last (and
    by construction every) driver ran into silence too.  A ``no_match`` style
    error means something DID answer — the link is fine and switching baud
    rates would only corrupt a working conversation.
    """

    text = (error or "").strip()
    return text == ERROR_INVERTER_LINK_DOWN or text.endswith(":probe_timeout")


def catalog_link_baud_hints() -> tuple[int, ...]:
    """Return the distinct catalog-declared inverter baud rates, ascending."""

    catalog = load_device_catalog()
    hints: set[int] = set()
    for protocol in catalog.protocols.values():
        hints.update(protocol.link_baud_hints)
    return tuple(sorted(hints))


def driver_keys_for_link_baud(baud: int) -> tuple[str, ...]:
    """Return driver keys whose protocol families expect the given baud.

    The per-baud re-sweep probes only these drivers: walking the FULL driver
    registry at every candidate speed multiplies scan time for nothing — a
    protocol family that lives at 2400 cannot match at 19200.
    """

    catalog = load_device_catalog()
    return tuple(
        sorted(
            key
            for key, protocol in catalog.protocols.items()
            if baud in protocol.link_baud_hints
        )
    )


@dataclass(frozen=True, slots=True)
class LinkBaudSweepOutcome:
    """What one transactional baud sweep did and found."""

    scan: Any | None = None
    matched_baud: int | None = None
    original_baud: int | None = None
    attempted_bauds: tuple[int, ...] = field(default_factory=tuple)
    restored: bool = True

    @property
    def matched(self) -> bool:
        return self.scan is not None

    def as_details(self) -> dict[str, object]:
        return {
            "original_baud": self.original_baud,
            "attempted_bauds": list(self.attempted_bauds),
            "matched_baud": self.matched_baud,
            "restored": self.restored,
        }


async def async_run_link_baud_sweep(
    *,
    candidate_bauds: tuple[int, ...],
    read_baud: Callable[[], Awaitable[int | None]],
    set_baud: Callable[[int], Awaitable[bool]],
    run_sweep: Callable[[int], Awaitable[Any | None]],
    admit: Callable[[], bool] | None = None,
) -> LinkBaudSweepOutcome:
    """Walk candidate baud rates transactionally.

    ``run_sweep`` returns a scan whose truthy ``candidates`` means a driver
    matched at that speed; ``None``/empty means silence or failure there too.
    On a match the new speed is kept (the firmware persists it); otherwise
    the original speed is restored before returning.
    """

    original = await read_baud()
    if original is None:
        # Fail closed: without a known original speed the sweep cannot be
        # transactional — a failed walk would strand the collector on the
        # last tested rate with nothing to restore.
        logger.debug("Link baud sweep skipped: current baud is unreadable")
        return LinkBaudSweepOutcome(original_baud=None, restored=False)
    attempted: list[int] = []
    for baud in candidate_bauds:
        if baud == original:
            continue
        if admit is not None and not admit():
            logger.debug("Link baud sweep stopped by budget admission at %s", baud)
            break
        if not await set_baud(baud):
            logger.debug("Link baud sweep: collector rejected baud %s", baud)
            continue
        attempted.append(baud)
        scan = await run_sweep(baud)
        if scan is not None and getattr(scan, "candidates", ()):
            logger.info(
                "Link baud sweep matched at %s (was %s)", baud, original
            )
            return LinkBaudSweepOutcome(
                scan=scan,
                matched_baud=baud,
                original_baud=original,
                attempted_bauds=tuple(attempted),
                restored=False,
            )

    restored = True
    if attempted:
        restored = await set_baud(original)
        if not restored:
            logger.warning(
                "Link baud sweep could not restore original baud %s", original
            )
    return LinkBaudSweepOutcome(
        scan=None,
        matched_baud=None,
        original_baud=original,
        attempted_bauds=tuple(attempted),
        restored=restored,
    )


def parse_reported_baud(raw: object) -> int | None:
    """Parse the collector's param-34 answer ("9600" or "9600,8,1,NONE")."""

    text = str(raw or "").strip().strip("\x00")
    if not text:
        return None
    leading = text.split(",", 1)[0].strip()
    if not leading.isdigit():
        return None
    value = int(leading)
    return value if value > 0 else None
