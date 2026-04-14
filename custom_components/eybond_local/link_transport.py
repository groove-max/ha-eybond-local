"""Transport-agnostic payload link interfaces."""

from __future__ import annotations

from typing import Any, Protocol

from .link_models import EybondLinkRoute, LinkRoute


class LinkTransport(Protocol):
    """Minimal runtime contract shared by all physical link implementations."""

    @property
    def connected(self) -> bool:
        ...

    async def wait_until_connected(self, timeout: float) -> bool:
        ...


class PayloadLinkTransport(LinkTransport, Protocol):
    """Link transport that can exchange one payload over a typed route."""

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route: LinkRoute,
    ) -> bytes:
        ...


async def async_send_payload(
    transport: Any,
    payload: bytes,
    *,
    route: LinkRoute,
) -> bytes:
    """Send one routed payload via the new or legacy transport contract."""

    sender = getattr(transport, "async_send_payload", None)
    if callable(sender):
        return await sender(payload, route=route)

    if isinstance(route, EybondLinkRoute):
        legacy_sender = getattr(transport, "async_send_forward", None)
        if callable(legacy_sender):
            return await legacy_sender(
                payload,
                devcode=route.devcode,
                collector_addr=route.collector_addr,
            )

    raise TypeError(f"unsupported_link_transport:{type(transport).__name__}:{route.family}")
