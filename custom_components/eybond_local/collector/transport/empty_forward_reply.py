"""Socket-local correlation for the observed zero-length FC4 no-result envelope.

This is NOT a relaxed binary grammar. Only an aligned eight-byte header matching
a recently transmitted FC4's full route is consumable, once. It can only fail a
request, never return telemetry or acknowledge a write. Timed-out requests retain
a short tombstone so a late no-result reply cannot poison the next transaction.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
import logging
from time import monotonic

from ..protocol import EybondHeader, FC_FORWARD_TO_DEVICE

logger = logging.getLogger(__name__)
_MAX_REQUESTS = 16
_RETENTION_SECONDS = 10.0


class EmptyForwardReplyWindow:
    """One physical socket's bounded request evidence, never shared on reconnect."""

    def __init__(self) -> None:
        self._requests: OrderedDict[int, tuple[int, int, float]] = OrderedDict()

    def forget(self, tid: int) -> None:
        self._requests.pop(tid, None)

    def record(self, header: EybondHeader) -> None:
        # Every newly allocated TID invalidates its previous generation,
        # including reuse by an FC1/FC2/FC3 request or heartbeat.
        self.forget(header.tid)
        if header.fcode != FC_FORWARD_TO_DEVICE:
            return
        self._requests[header.tid] = (
            header.devcode, header.devaddr, monotonic() + _RETENTION_SECONDS,
        )
        while len(self._requests) > _MAX_REQUESTS:
            self._requests.popitem(last=False)

    def consume(
        self, header: EybondHeader,
        pending: dict[int, asyncio.Future[tuple[EybondHeader, bytes]]],
    ) -> bool:
        if header.fcode != FC_FORWARD_TO_DEVICE or header.payload_len != -2:
            return False
        request = self._requests.get(header.tid)
        if request is None:
            return False
        devcode, address, expires = request
        if monotonic() >= expires:
            self.forget(header.tid)
            return False
        # Ordinary valid replies may rewrite route fields. This malformed-wire
        # compatibility exception deliberately requires an exact route match.
        if (header.devcode, header.devaddr) != (devcode, address):
            return False
        self.forget(header.tid)
        future = pending.get(header.tid)
        active = future is not None and not future.done()
        if active:
            future.set_exception(TimeoutError("collector_empty_forward_response"))
        logger.debug(
            "Consumed correlated empty FC4 envelope tid=%d devcode=0x%04X "
            "devaddr=0x%02X active_request=%s; no inverter result",
            header.tid, header.devcode, header.devaddr, active,
        )
        return True
