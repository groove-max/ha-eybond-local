"""Transport-agnostic payload link interfaces."""

from __future__ import annotations

import inspect
from typing import Any, Protocol

from .link_models import LinkRoute


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
        request_timeout: float | None = None,
    ) -> bytes:
        ...


async def async_send_payload(
    transport: Any,
    payload: bytes,
    *,
    route: LinkRoute,
    request_timeout: float | None = None,
) -> bytes:
    """Send one payload through the typed-route transport contract."""

    sender = getattr(transport, "async_send_payload", None)
    if callable(sender):
        if request_timeout is not None:
            try:
                signature = inspect.signature(sender)
            except (TypeError, ValueError):
                signature = None
            if signature is not None and "request_timeout" in signature.parameters:
                return await sender(
                    payload,
                    route=route,
                    request_timeout=float(request_timeout),
                )
        return await sender(payload, route=route)

    raise TypeError(f"unsupported_link_transport:{type(transport).__name__}:{route.family}")


def select_payload_route(
    transport: Any,
    route: LinkRoute,
    *,
    payload_family: str = "",
) -> LinkRoute:
    """Let a concrete transport map a logical route to its wire route.

    Driver code starts with the catalog/default route. Transports whose wire
    format differs from that default can return a more precise route type.
    """

    selector = getattr(transport, "select_payload_route", None)
    if callable(selector):
        selected = selector(route, payload_family=payload_family)
        if isinstance(selected, LinkRoute):
            return selected
    return route
