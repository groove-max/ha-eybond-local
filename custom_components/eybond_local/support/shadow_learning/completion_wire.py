"""Wire-specific completion proof, separate from study retirement policy.

Reuse the learning adapters that actually refused the write. A carrier provides
transaction/route identity; a codec proves the request and negative response.
Raw carriers use the ledger's single-flight sequence on one connection epoch.
"""
from __future__ import annotations

import re

from ...collector.at import CollectorAtError, parse_at_command
from ...collector.protocol import HEADER_SIZE, decode_header
from .protocol import (
    ModbusRtuShadowLearningAdapter, EybondGAsciiShadowLearningAdapter,
    resolve_shadow_learning_wire_adapter,
)
from .pi30_protocol import Pi30ShadowLearningAdapter
from .protocol_evidence import read_protocols


_CODECS = {adapter.key: adapter for adapter in (
    ModbusRtuShadowLearningAdapter(), Pi30ShadowLearningAdapter(),
    EybondGAsciiShadowLearningAdapter(),
)}


class FramedCarrier:
    def unpack(self, frame: bytes):
        if len(frame) < HEADER_SIZE:
            raise ValueError("completion_frame_short")
        header = decode_header(frame)
        if header.total_len != len(frame):
            raise ValueError("completion_frame_length")
        return frame[HEADER_SIZE:], header

    def request(self, frame: bytes):
        payload, header = self.unpack(frame)
        if header.fcode in (1, 2):
            return None
        if header.fcode != 4:
            raise ValueError("completion_frame_function")
        return payload

    def response(self, request: bytes, response: bytes):
        _, h = self.unpack(request)
        payload, r = self.unpack(response)
        if h.fcode != 4 or (h.tid, h.devcode, h.devaddr, h.fcode) != (r.tid, r.devcode, r.devaddr, r.fcode):
            raise ValueError("completion_route_mismatch")
        return payload, (h.devcode, h.devaddr), h.tid


class RawCarrier:
    def request(self, frame: bytes):
        return frame

    def response(self, request: bytes, response: bytes):
        return response, (), None


_CARRIERS = {"frame": FramedCarrier(), "modbus": RawCarrier(),
             "ascii": RawCarrier(), "pi30": RawCarrier()}


def classify_request(frame: bytes, carrier: str, protocol: str | None):
    """Return the proven codec for a write; None means a recognized read."""
    if type(frame) is not bytes or not 0 < len(frame) <= 520:
        raise ValueError("completion_wire_size")
    if carrier == "at":
        try:
            if not frame.isascii():
                raise ValueError("completion_at_encoding")
            command = parse_at_command(frame)
            if command.operation == "query":
                return None
            # These collector bootstrap settings are answered only by the
            # shadow backend; they are never forwarded or inverter controls.
            # Do not confuse their virtual ACK with a test-command receipt.
            patterns = {"SYST": r"[0-9]{14}",
                        "UART": r"[0-9]{1,7},[5-8],[12],(?:NONE|EVEN|ODD)"}
            if (command.operation == "write" and command.command in patterns
                    and re.fullmatch(patterns[command.command], command.value)):
                return None
        except CollectorAtError:
            pass
        raise ValueError("completion_at_write_unproven")
    transport = _CARRIERS.get(carrier)
    if transport is None:
        raise ValueError("completion_carrier_unsupported")
    payload = transport.request(frame)
    if payload is None:
        return None
    # Preparation may still be identifying the cloud codec. A fully decoded
    # read is not an unowned write and must not poison later completion.
    if read_protocols(payload):
        return None
    adapter = (_CODECS.get(protocol) if protocol is not None
               else resolve_shadow_learning_wire_adapter(payload))
    if adapter is None or not adapter.supported:
        raise ValueError("completion_codec_unsupported")
    if adapter.decode_read_request(payload) is not None:
        return None
    if adapter.decode_write_request(payload) is None:
        raise ValueError("completion_write_unrecognized")
    return adapter.key


def validate_wire_receipt(record: dict):
    """Return stable route and optional wire transaction identity."""
    request = bytes.fromhex(record["request_hex"])
    response = bytes.fromhex(record["response_hex"])
    protocol, carrier = record["protocol"], record["carrier"]
    if classify_request(request, carrier, protocol) != protocol:
        raise ValueError("completion_not_write")
    transport, adapter = _CARRIERS[carrier], _CODECS[protocol]
    write = adapter.decode_write_request(transport.request(request))
    payload, route, transaction = transport.response(request, response)
    if payload != adapter.build_write_exception_response(write, exception_code=1):
        raise ValueError("completion_response_not_refusal")
    return (carrier, protocol, *route, write.unit), transaction
