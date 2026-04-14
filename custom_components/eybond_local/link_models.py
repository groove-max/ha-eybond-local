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
