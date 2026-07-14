"""Neutral driver contract for bounded read-only support probes.

When normal inverter detection may have failed, the runtime hub can still run a
small read-only diagnostic sweep so the support archive carries wire evidence.
The *commands* in that sweep are protocol policy owned by each driver; the hub
is only a neutral executor. A driver advertises its probe as a tuple of
:class:`SupportProbeRequest` descriptors that carry exactly the execution data
the hub needs -- and nothing about how to build them.

The descriptor carries no owning-driver identity: the hub already iterates the
registry drivers, so the owner is authoritatively ``driver.key`` at capture
time. Duplicating it here would let a descriptor's owner drift from the real
owner, so it is intentionally absent.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SupportProbeRequest:
    """One bounded, read-only support-probe request built by a driver.

    * ``payload_family`` -- selects the neutral payload route in the hub.
    * ``command`` -- the human-facing command/display key recorded in evidence.
    * ``request`` -- the already-encoded request bytes to send verbatim.
    """

    payload_family: str
    command: str
    request: bytes
