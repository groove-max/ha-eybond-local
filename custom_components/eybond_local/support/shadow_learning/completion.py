"""Provider/codec-neutral, single-flight live command completion ledger.

Only complete, sealed evidence permits retirement, never counts or elapsed time.
Each route instance owns one ledger. Historical traces cannot recreate it.
"""
from __future__ import annotations

import re

from .completion_provider import completion_provider
from .completion_wire import classify_request, validate_wire_receipt

MAX_COMMANDS = 2048
_KEYS = {"request_hex", "response_hex", "action_sha256", "source", "carrier",
         "protocol", "epoch", "sequence", "terminal"}


def validate_completed_commands(commands: list, *, source: str | None = None) -> None:
    if type(commands) is not list or not 0 < len(commands) <= MAX_COMMANDS:
        raise ValueError("completion_commands_invalid")
    transactions, routes, sources, epochs = set(), set(), set(), set()
    for sequence, command in enumerate(commands, 1):
        if type(command) is not dict or set(command) != _KEYS:
            raise ValueError("completion_receipt_shape")
        for key in ("source", "protocol", "carrier", "action_sha256"):
            if type(command[key]) is not str or not 0 < len(command[key]) <= 64:
                raise ValueError("completion_receipt_encoding")
        if not re.fullmatch(r"[0-9a-f]{64}", command["action_sha256"]):
            raise ValueError("completion_action_digest")
        for key in ("request_hex", "response_hex"):
            raw = command[key]
            if type(raw) is not str or len(raw) > 1040 or not re.fullmatch(r"(?:[0-9a-f]{2})+", raw):
                raise ValueError("completion_frame_encoding")
        if (type(command["epoch"]) is not int or not 1 <= command["epoch"] <= MAX_COMMANDS
                or type(command["sequence"]) is not int or command["sequence"] != sequence):
            raise ValueError("completion_order_invalid")
        completion_provider(command["source"]).validate_receipt(
            command["terminal"], bytes.fromhex(command["response_hex"]))
        route, transaction = validate_wire_receipt(command)
        if transaction is not None:
            if transaction in transactions:
                raise ValueError("completion_transaction_reused")
            transactions.add(transaction)
        routes.add(route)
        sources.add(command["source"])
        epochs.add(command["epoch"])
    if len(routes) != 1 or len(epochs) != 1 or len(sources) != 1 or (source is not None and sources != {source}):
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
        self._epoch = 1
        self._source = ""
        self._request: bytes | None = None
        self._response: bytes | None = None
        self._carrier = self._protocol = self._action_digest = ""

    def invalidate(self, reason: str = "completion_evidence_incomplete") -> None:
        self.failed = True
        if not self.reason:
            self.reason = reason

    def connection_opened(self, epoch: int) -> None:
        if self.active or self.commands:
            self.invalidate("completion_connection_changed")
        self._epoch = epoch

    def begin(self, source: str, action: object) -> None:
        if self.active or self.closing or len(self.commands) >= MAX_COMMANDS:
            self.invalidate("completion_dispatch_overlap")
        if self._source and source != self._source:
            self.invalidate("completion_provider_changed")
        self.active = True
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
            if not self.active or self.closing or self._request is not None:
                raise ValueError("completion_unowned_or_duplicate_write")
            self._request, self._carrier, self._protocol = frame, carrier, codec
            return True
        except (ValueError, TypeError):
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
            if not self.active or not self._response or not self._request:
                raise ValueError("completion_missing_exchange")
            terminal = completion_provider(self._source).terminal_receipt(envelope, self._response)
            record = {"request_hex": self._request.hex(), "response_hex": self._response.hex(),
                      "action_sha256": self._action_digest, "source": self._source,
                      "carrier": self._carrier, "protocol": self._protocol,
                      "epoch": self._epoch, "sequence": len(self.commands) + 1, "terminal": terminal}
            validate_completed_commands([*self.commands, record])
            self.commands.append(record)
        except (ValueError, TypeError):
            self.invalidate("completion_terminal_confirmation_missing")
        finally:
            self.active = False

    def start_closing(self) -> None:
        self.closing = True
        if self.active:
            self.invalidate("completion_dispatch_in_flight")

    def completed_commands(self) -> list[dict] | None:
        if self.failed or self.active or not self.closing or not self.closed:
            return None
        try:
            validate_completed_commands(self.commands)
        except ValueError:
            return None
        return [dict(command, terminal=dict(command["terminal"])) for command in self.commands]
