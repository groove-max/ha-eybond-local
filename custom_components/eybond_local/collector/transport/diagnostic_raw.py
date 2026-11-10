"""Socket-scoped exchange ownership shared by explicit raw diagnostics."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import math

from .request_trace import SocketRequestTrace
from .send_ownership import SocketSendOwner, finish_request_future


@dataclass(frozen=True)
class RawDiagnosticClaim:
    request: bytes
    future: asyncio.Future[bytes]
    deadline: float


class RawDiagnosticSession:
    """One pending raw exchange; the connection's normal reader decodes it.

    Subclasses validate requests and consume their own reply grammar. No extra
    reader, reconnect, bootstrap or permanent wire selection belongs here.
    """

    def __init__(self) -> None:
        self.claim: RawDiagnosticClaim | None = None
        self.closed = False
        self.received = b""
        self.failure_reason = ""

    def validate_request(self, payload: bytes) -> None:
        raise NotImplementedError

    def close(self, reason: str = "") -> None:
        self.closed = True
        if reason:
            self.failure_reason = reason
        claim = self.claim
        if claim is not None and not claim.future.done():
            claim.future.set_exception(ConnectionError("collector_disconnected"))

    async def send(
        self, connection, owner: SocketSendOwner, payload: bytes, timeout: float,
        *, trace: SocketRequestTrace | None = None,
    ) -> bytes:
        """Send once under existing locks; fence an uncertain physical socket."""
        self.validate_request(payload)
        if not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise ValueError("diagnostic_request_timeout_invalid")
        reader_task = connection._reader_task
        guard = connection._auxiliary_session
        if trace is not None:
            trace.phase = "waiting_request_lock"
        async with connection._request_lock:
            if trace is not None:
                trace.phase = "checking_reader"
            owner.check(connection)
            if self.closed:
                raise ConnectionError("collector_session_changed")
            if guard.enabled or not guard.at_boundary:
                raise ConnectionError("diagnostic_reader_busy")
            future = asyncio.get_running_loop().create_future()
            claim = RawDiagnosticClaim(bytes(payload), future,
                                       asyncio.get_running_loop().time() + timeout)
            self.received = b""
            self.failure_reason = ""
            sent = False
            try:
                if trace is not None:
                    trace.phase = "waiting_write_lock"
                async with connection._write_lock:
                    owner.check(connection)
                    if guard.enabled or not guard.at_boundary:
                        raise ConnectionError("diagnostic_reader_busy")
                    self.claim = claim
                    sent = True
                    if trace is not None:
                        trace.phase = "writing"
                        trace.write_started = True
                    owner.writer.write(payload)
                    if trace is not None:
                        trace.write_returned = True
                        trace.phase = "draining"
                    await asyncio.wait_for(owner.writer.drain(), connection._write_timeout)
                    if trace is not None:
                        trace.drain_completed = True
                        trace.phase = "waiting_response"
                response = await asyncio.wait_for(future, max(
                    0.001, claim.deadline - asyncio.get_running_loop().time(),
                ))
                owner.check_reply(connection)
                if trace is not None:
                    trace.phase = "response_received"
                    trace.response_received = True
                return response
            except BaseException:
                if sent:
                    # Never disconnect a successor through mutable shared slots.
                    self.close()
                    guard.close()
                    owner.writer.close()
                    if reader_task is not None:
                        reader_task.cancel()
                raise
            finally:
                if self.claim is claim:
                    self.claim = None
                finish_request_future(future)
