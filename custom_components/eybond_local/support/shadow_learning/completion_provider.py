"""Provider-owned action binding and terminal evidence, never HTTP success alone.

Adapters expose a canonical, credential-free receipt to the common ledger.
ValueCloud's current API exposes acceptance but no qualified terminal wire echo;
it reports that missing evidence explicitly, not a ban on its inverter protocol.
"""
from __future__ import annotations

from hashlib import sha256
from urllib.parse import parse_qs

from ...smartess_cloud import ApiEnvelope, SmartEssActionRejectedError
from ...dessmonitor_cloud import DessMonitorApiEnvelope, build_device_control_action


class WireEchoProvider:
    def __init__(self, envelope_type):
        self.envelope_type = envelope_type

    def action_digest(self, arguments, pn):
        action = arguments.get("action") if type(arguments) is dict else arguments
        if type(action) is not str or len(action) > 4096:
            raise ValueError("completion_action_invalid")
        query = parse_qs(action.lstrip("?&"), keep_blank_values=True)
        if (query.get("action") != ["ctrlDevice"] or query.get("pn") != [pn]
                or len(query.get("id", [])) != 1 or len(query.get("val", [])) != 1):
            raise ValueError("completion_action_scope")
        return sha256(action.encode()).hexdigest()

    def terminal_receipt(self, envelope, response: bytes):
        if (type(envelope) is not self.envelope_type
                or type(envelope.err) is not int or envelope.err != 0):
            raise ValueError("completion_api_result")
        dat = envelope.dat
        if type(dat) is not dict or type(dat.get("status")) is not int or dat["status"] != 0:
            raise ValueError("completion_api_status")
        echo = dat.get("dat")
        if type(echo) is not str or len(echo) > 2080 or bytes.fromhex(echo) != response:
            raise ValueError("completion_api_echo_mismatch")
        return {"kind": "wire_echo", "response_hex": response.hex()}

    def validate_receipt(self, receipt, response: bytes):
        if type(receipt) is not dict or receipt != {"kind": "wire_echo", "response_hex": response.hex()}:
            raise ValueError("completion_terminal_receipt_invalid")


class DessMonitorCompletionProvider(WireEchoProvider):
    def action_digest(self, arguments, pn):
        if type(arguments) is not dict:
            raise ValueError("completion_action_invalid")
        action = build_device_control_action(
            identity=arguments.get("identity"), field_id=arguments.get("field_id"),
            value=arguments.get("value"),
        )
        return super().action_digest(action, pn)


class SmartEssCompletionProvider(WireEchoProvider):
    @staticmethod
    def _readonly_receipt(response):
        # Qualified on SMG6200's bare FC16 exchange: cloud parses our Modbus
        # exception instead of echoing bytes. Generic ERR_FAIL/timeout/acceptance
        # cannot stand in for this exact device-refusal result. The common wire
        # validator separately binds this reply to the single captured request.
        from ...payload.modbus import crc16_modbus

        if (len(response) != 5 or not 1 <= response[0] <= 247
                or response[1:3] != b"\x90\x01"
                or crc16_modbus(response[:-2]) != int.from_bytes(response[-2:], "little")):
            raise ValueError("completion_device_refusal_wire_mismatch")
        return {"kind": "device_refusal", "reason": "smartess_read_only_register",
                "response_hex": response.hex()}

    def terminal_receipt(self, envelope, response):
        if (type(envelope) is SmartEssActionRejectedError and envelope.err == 1
                and envelope.desc == "ERR_FAIL(Read-Only Register)"):
            return self._readonly_receipt(response)
        return super().terminal_receipt(envelope, response)

    def validate_receipt(self, receipt, response):
        if type(receipt) is dict and receipt.get("kind") == "device_refusal":
            if receipt != self._readonly_receipt(response):
                raise ValueError("completion_terminal_receipt_invalid")
            return
        super().validate_receipt(receipt, response)


class AcceptanceOnlyProvider:
    def action_digest(self, arguments, pn):
        raise ValueError("completion_terminal_evidence_unavailable")

    def terminal_receipt(self, envelope, response):
        raise ValueError("completion_terminal_evidence_unavailable")

    def validate_receipt(self, receipt, response):
        raise ValueError("completion_terminal_evidence_unavailable")


_PROVIDERS = {
    "smartess": SmartEssCompletionProvider(ApiEnvelope),
    "dessmonitor": DessMonitorCompletionProvider(DessMonitorApiEnvelope),
    "valuecloud": AcceptanceOnlyProvider(),
}


def completion_provider(source):
    adapter = _PROVIDERS.get(source) if type(source) is str else None
    if adapter is None:
        raise ValueError("completion_provider_evidence_unavailable")
    return adapter
