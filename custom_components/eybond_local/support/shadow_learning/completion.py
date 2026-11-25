"""Provider/codec-neutral, single-flight live command completion ledger.

Only complete, sealed evidence confirms delivery, never counts or elapsed time.
Each route instance owns one ledger. Historical traces cannot recreate it.
"""
from __future__ import annotations

import asyncio
import re
from time import monotonic

from .completion_provider import completion_provider
from .completion_wire import classify_request, validate_wire_receipt

MAX_COMMANDS = 2048
# Measured SmartESS send-failed responses: <12 s on the SMG bench. This
# is a post-result observation window, not a guessed vendor command TTL.
DISPATCH_FAILURE_QUIET_SECONDS = 30.0
_KEYS = {"request_hex", "response_hex", "action_sha256", "source", "carrier",
         "protocol", "epoch", "sequence", "terminal"}


def validate_completed_commands(commands: list, *, source: str | None = None) -> None:
    if type(commands) is not list or not 0 < len(commands) <= MAX_COMMANDS:
        raise ValueError("completion_commands_invalid")
    transactions, routes, sources = set(), set(), set()
    previous_epoch = 0
    for sequence, command in enumerate(commands, 1):
        if type(command) is not dict or set(command) != _KEYS:
            raise ValueError("completion_receipt_shape")
        for key in ("source", "protocol", "carrier", "action_sha256"):
            if type(command[key]) is not str or not 0 < len(command[key]) <= 64:
                raise ValueError("completion_receipt_encoding")
        if not re.fullmatch(r"[0-9a-f]{64}", command["action_sha256"]):
            raise ValueError("completion_action_digest")
        terminal = command["terminal"]
        failed_send = type(terminal) is dict and terminal.get("kind") == "dispatch_failed"
        if failed_send and sequence != len(commands):
            raise ValueError("completion_dispatch_after_failure")
        for key in ("request_hex", "response_hex"):
            raw = command[key]
            if failed_send and raw == "":
                continue
            if type(raw) is not str or len(raw) > 1040 or not re.fullmatch(r"(?:[0-9a-f]{2})+", raw):
                raise ValueError("completion_frame_encoding")
        if (type(command["epoch"]) is not int or not max(1, previous_epoch) <= command["epoch"] <= MAX_COMMANDS
                or type(command["sequence"]) is not int or command["sequence"] != sequence):
            raise ValueError("completion_order_invalid")
        completion_provider(command["source"]).validate_receipt(
            command["terminal"], bytes.fromhex(command["response_hex"]))
        sources.add(command["source"])
        previous_epoch = command["epoch"]
        if failed_send and not command["request_hex"]:
            if command["response_hex"] or (command["carrier"], command["protocol"]) != ("none", "none"):
                raise ValueError("completion_missing_request")
            continue
        route, transaction = validate_wire_receipt(command, allow_missing_reply=failed_send)
        if transaction is not None:
            # A framed transaction ID is local to a physical TCP connection.
            key = command["epoch"], transaction
            if key in transactions:
                raise ValueError("completion_transaction_reused")
            transactions.add(key)
        routes.add(route)
    if len(routes) > 1 or len(sources) != 1 or (source is not None and sources != {source}):
        raise ValueError("completion_scope_changed")


class CloudCommandCompletion:
    def __init__(self, *, entry_id: str, study_id: str, collector_pn: str):
        self.entry_id, self.study_id, self.collector_pn = entry_id, study_id, collector_pn
        self.commands: list[dict] = []
        self.failed = False
        self.reason = ""
        self.closing = False
        self.closed = False
        self.active = False
        self._epoch = 0
        self._connected = False
        self._action_epoch = 0
        self._wire_interrupted = False
        self._quiet_until = 0.0
        self._quiet_observed = False
        self.last_cloud_write = 0.0
        self.unexpected_write = False
        self._source = ""
        self._request: bytes | None = None
        self._response: bytes | None = None
        self._carrier = self._protocol = self._action_digest = ""

    def invalidate(self, reason: str = "completion_evidence_incomplete") -> None:
        self.failed = True
        if not self.reason:
            self.reason = reason

    def connection_closed(self) -> None:
        # The local refusal and the provider's HTTP reply travel separately.
        # A broken exchange cannot prove a wire echo. The vendor may still
        # explicitly finish that same HTTP action with a delivery failure.
        if self.active and self._response is None:
            self._wire_interrupted = True
        self._connected = False

    def connection_opened(self, epoch: int) -> None:
        self.connection_closed()
        if type(epoch) is not int or not self._epoch < epoch <= MAX_COMMANDS:
            self.invalidate("completion_connection_changed")
            return
        self._epoch = epoch
        self._connected = True

    def begin(self, source: str, action: object) -> None:
        if not self._connected:
            self.invalidate("completion_connection_changed")
        if self.active or self.closing or self._quiet_until or len(self.commands) >= MAX_COMMANDS:
            self.invalidate("completion_dispatch_overlap")
        if self._source and source != self._source:
            self.invalidate("completion_provider_changed")
        self.active = True
        self._action_epoch = self._epoch
        self._wire_interrupted = False
        self._request = self._response = None
        self._source = source
        try:
            self._action_digest = completion_provider(source).action_digest(action, self.collector_pn)
        except (ValueError, TypeError):
            self.invalidate("completion_action_or_provider_evidence_unavailable")

    def request(self, frame: bytes, *, carrier: str = "frame", protocol: str | None = None) -> bool:
        """Record before backend await; only proven reads may bypass the ledger."""
        try:
            codec = classify_request(frame, carrier, protocol)
            if codec is None:
                return False
            self.last_cloud_write = monotonic()
            if not self.active or self.closing or self._request is not None or self._wire_interrupted:
                raise ValueError("completion_unowned_or_duplicate_write")
            self._request, self._carrier, self._protocol = frame, carrier, codec
            return True
        except (ValueError, TypeError):
            self.last_cloud_write = monotonic()
            self.unexpected_write = True
            self.invalidate("completion_wire_unproven")
            return False

    def replied(self, frame: bytes | None) -> None:
        """Only after the real upstream writer's drain completed."""
        if type(frame) is not bytes or not 0 < len(frame) <= 520:
            self.invalidate("completion_response_invalid")
            return
        if self._response is not None or not self.active or self._request is None:
            self.invalidate("completion_unowned_or_duplicate_response")
        self._response = frame

    def finish(self, envelope) -> None:
        try:
            if not self.active:
                raise ValueError("completion_missing_exchange")
            provider = completion_provider(self._source)
            failure = provider.dispatch_failure(envelope)
            if failure is None:
                if not self._response or not self._request or self._wire_interrupted:
                    raise ValueError("completion_missing_exchange")
                terminal = provider.terminal_receipt(envelope, self._response)
            else:
                terminal = failure
            record = {"request_hex": (self._request or b"").hex(), "response_hex": (self._response or b"").hex(),
                      "action_sha256": self._action_digest, "source": self._source,
                      "carrier": self._carrier if self._request else "none",
                      "protocol": self._protocol if self._request else "none",
                      "epoch": self._action_epoch, "sequence": len(self.commands) + 1, "terminal": terminal}
            validate_completed_commands([*self.commands, record])
            self.commands.append(record)
            if failure is not None:
                self._quiet_until = monotonic() + DISPATCH_FAILURE_QUIET_SECONDS
        except (ValueError, TypeError):
            self.invalidate("completion_terminal_confirmation_missing")
        finally:
            self.active = False

    def start_closing(self) -> None:
        self.closing = True
        if self.active:
            self.invalidate("completion_dispatch_in_flight")

    def remaining_quiet_time(self) -> float:
        return 0.0 if self.failed else max(0.0, self._quiet_until - monotonic())

    async def observe_before_detach(self, *, uncertain_stop: bool = False) -> None:
        """The coordinator still owns the attached, filtering route here."""
        if not self.closing or self.closed:
            return
        if uncertain_stop and (self.failed or not self.commands or self._quiet_until):
            start = monotonic()
            window = DISPATCH_FAILURE_QUIET_SECONDS
            deadline = self._quiet_until if self._quiet_until and not self.failed else start + window
            limit = start + 2 * window
            while (left := min(max(deadline, self.last_cloud_write + window), limit) - monotonic()) > 0:
                await asyncio.sleep(left)
            self._quiet_observed = not self.failed
            return
        if self.failed or not self._quiet_until:
            return
        try:
            while remaining := self.remaining_quiet_time():
                await asyncio.sleep(remaining)
        except asyncio.CancelledError:
            self.invalidate("completion_observation_interrupted")
            raise
        self._quiet_observed = not self.failed

    def route_detaching(self) -> None:
        if self._quiet_until and not self._quiet_observed:
            self.invalidate("completion_observation_interrupted")

    def completed_commands(self) -> list[dict] | None:
        if (self.failed or self.active or not self.closing or not self.closed
                or (self._quiet_until and not self._quiet_observed)):
            return None
        try:
            validate_completed_commands(self.commands)
        except ValueError:
            return None
        return [dict(command, terminal=dict(command["terminal"])) for command in self.commands]
