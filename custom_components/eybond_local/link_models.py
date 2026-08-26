"""Transport-agnostic link routing models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LinkRoute:
    """Opaque payload route understood by one concrete link transport."""

    family: str


@dataclass(frozen=True, slots=True)
class EybondLinkRoute(LinkRoute):
    """EyeBond collector tunnel route for one forwarded device payload."""

    devcode: int
    collector_addr: int

    def __init__(self, *, devcode: int, collector_addr: int) -> None:
        object.__setattr__(self, "family", "eybond")
        object.__setattr__(self, "devcode", int(devcode))
        object.__setattr__(self, "collector_addr", int(collector_addr))


@dataclass(frozen=True, slots=True)
class RawSerialLinkRoute(LinkRoute):
    """Raw serial passthrough route with no collector tunnel envelope."""

    protocol: str

    def __init__(self, *, protocol: str = "") -> None:
        object.__setattr__(self, "family", "raw_serial")
        object.__setattr__(self, "protocol", str(protocol or "").strip())


@dataclass(frozen=True, slots=True)
class AtMixedLinkRoute(LinkRoute):
    """Data-plane negotiation route for one exact AT-primary session.

    AT manages the collector, while inverter payloads may be raw bytes or an
    EyeBond FC=4 frame on the same socket. Both typed alternatives are retained
    until a correlated response proves which data plane this socket supports.
    """

    devcode: int
    collector_addr: int
    protocol: str

    def __init__(
        self,
        *,
        devcode: int,
        collector_addr: int,
        protocol: str = "",
    ) -> None:
        object.__setattr__(self, "family", "at_mixed")
        object.__setattr__(self, "devcode", int(devcode))
        object.__setattr__(self, "collector_addr", int(collector_addr))
        object.__setattr__(self, "protocol", str(protocol or "").strip())
