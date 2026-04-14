"""Offline fixture transport for driver replay without a live collector."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .utils import load_fixture_json
from ..link_models import EybondLinkRoute, LinkRoute
from ..models import ProbeTarget
from ..payload.modbus import crc16_modbus
from ..payload.pi30 import crc16_xmodem


class FixtureTransportError(Exception):
    """Raised when a fixture transport request cannot be fulfilled."""


class FixtureTransport:
    """Minimal transport that replays saved requests for offline driver validation."""

    def __init__(
        self,
        *,
        registers: dict[int, int] | None,
        command_responses: dict[tuple[int, int, str], str] | None,
        probe_target: ProbeTarget,
        name: str = "",
        collector: dict[str, Any] | None = None,
    ) -> None:
        self._registers = dict(registers or {})
        self._command_responses = dict(command_responses or {})
        self._probe_target = probe_target
        self.name = name
        self.collector = collector or {}

    async def async_send_forward(
        self,
        payload: bytes,
        *,
        devcode: int,
        collector_addr: int,
    ) -> bytes:
        """Replay one saved request against the in-memory fixture payload."""

        self._validate_target(devcode=devcode, collector_addr=collector_addr)

        if self._command_responses:
            return self._handle_command_request(payload, devcode=devcode, collector_addr=collector_addr)
        if len(payload) < 4:
            raise FixtureTransportError("request_too_short")

        slave_id = payload[0]
        function_code = payload[1]
        if slave_id != self._probe_target.device_addr:
            raise FixtureTransportError(
                f"unexpected_slave_id:{slave_id}:expected={self._probe_target.device_addr}"
            )

        if function_code == 0x03:
            return self._handle_read_holding(payload)
        if function_code == 0x10:
            return self._handle_write_multiple(payload)
        raise FixtureTransportError(f"unsupported_function:{function_code}")

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route: LinkRoute,
    ) -> bytes:
        if not isinstance(route, EybondLinkRoute):
            raise FixtureTransportError(f"unsupported_link_route:{route.family}")
        return await self.async_send_forward(
            payload,
            devcode=route.devcode,
            collector_addr=route.collector_addr,
        )

    def _validate_target(self, *, devcode: int, collector_addr: int) -> None:
        if devcode != self._probe_target.devcode:
            raise FixtureTransportError(
                f"unexpected_devcode:0x{devcode:04X}:expected=0x{self._probe_target.devcode:04X}"
            )
        if collector_addr != self._probe_target.collector_addr:
            raise FixtureTransportError(
                f"unexpected_collector_addr:0x{collector_addr:02X}:"
                f"expected=0x{self._probe_target.collector_addr:02X}"
            )

    def _handle_command_request(self, payload: bytes, *, devcode: int, collector_addr: int) -> bytes:
        if len(payload) < 4:
            raise FixtureTransportError("request_too_short")
        if payload[-1] != 0x0D:
            raise FixtureTransportError("missing_terminator")

        request_crc = payload[-3:-1]
        body = payload[:-3]
        command_family = _command_family(body)
        if request_crc != _encode_command_crc(body, family=command_family):
            raise FixtureTransportError("request_crc_mismatch")

        try:
            command = body.decode("ascii")
        except UnicodeDecodeError as exc:
            raise FixtureTransportError("request_not_ascii") from exc

        response_payload = self._command_responses.get((devcode, collector_addr, command))
        if response_payload is None:
            raise FixtureTransportError(f"missing_command:{command}")

        if command_family == "pi18":
            response_body = f"^D{len(response_payload) + 3:03d}{response_payload}".encode("ascii")
            response_crc = _encode_command_crc(response_body, family=command_family)
            return response_body + response_crc + b"\r"

        response_body = f"({response_payload}".encode("ascii")
        response_crc = _encode_command_crc(response_body, family=command_family)
        return response_body + response_crc + b"\r"

    def _handle_read_holding(self, payload: bytes) -> bytes:
        request_crc = int.from_bytes(payload[-2:], "little")
        expected_crc = crc16_modbus(payload[:-2])
        if request_crc != expected_crc:
            raise FixtureTransportError("request_crc_mismatch")

        address = int.from_bytes(payload[2:4], "big")
        count = int.from_bytes(payload[4:6], "big")
        words: list[int] = []
        for register in range(address, address + count):
            if register not in self._registers:
                raise FixtureTransportError(f"missing_register:{register}")
            words.append(self._registers[register])

        response = bytearray([self._probe_target.device_addr, 0x03, count * 2])
        for value in words:
            response.extend(value.to_bytes(2, "big", signed=False))
        response_crc = crc16_modbus(response)
        response.extend(response_crc.to_bytes(2, "little"))
        return bytes(response)

    def _handle_write_multiple(self, payload: bytes) -> bytes:
        address = int.from_bytes(payload[2:4], "big")
        register_count = int.from_bytes(payload[4:6], "big")
        byte_count = payload[6]
        expected_byte_count = register_count * 2
        if byte_count != expected_byte_count:
            raise FixtureTransportError(
                f"unexpected_byte_count:{byte_count}:expected={expected_byte_count}"
            )

        data = payload[7 : 7 + byte_count]
        if len(data) != expected_byte_count:
            raise FixtureTransportError("request_write_payload_length_mismatch")

        for offset in range(register_count):
            value = int.from_bytes(data[offset * 2 : offset * 2 + 2], "big")
            self._registers[address + offset] = value

        response = bytearray([self._probe_target.device_addr, 0x10])
        response.extend(address.to_bytes(2, "big"))
        response.extend(register_count.to_bytes(2, "big"))
        response_crc = crc16_modbus(response)
        response.extend(response_crc.to_bytes(2, "little"))
        return bytes(response)


def load_fixture(path: str | Path) -> tuple[FixtureTransport, dict[str, Any]]:
    """Load one fixture file and return the replay transport plus raw metadata."""

    fixture_path = Path(path)
    raw = load_fixture_json(fixture_path)
    return load_fixture_payload(raw, name=str(raw.get("name", fixture_path.stem)))


def load_fixture_payload(
    raw: dict[str, Any],
    *,
    name: str = "",
) -> tuple[FixtureTransport, dict[str, Any]]:
    """Load one raw fixture payload and build the replay transport."""

    if int(raw.get("fixture_version", 0)) != 1:
        raise ValueError(f"unsupported_fixture_version:{raw.get('fixture_version')}")

    probe_raw = raw["probe_target"]
    probe_target = ProbeTarget(
        devcode=int(probe_raw["devcode"]),
        collector_addr=int(probe_raw["collector_addr"]),
        device_addr=int(probe_raw["device_addr"]),
    )
    ranges = raw.get("ranges", [])
    registers: dict[int, int] = {}
    for range_item in ranges:
        start = int(range_item["start"])
        values = [int(value) for value in range_item["values"]]
        for offset, value in enumerate(values):
            registers[start + offset] = value

    command_responses_raw = raw.get("command_responses", {})
    command_responses: dict[tuple[int, int, str], str] = {}
    if isinstance(command_responses_raw, dict):
        for command, response in command_responses_raw.items():
            command_responses[(probe_target.devcode, probe_target.collector_addr, str(command))] = str(response)
    elif isinstance(command_responses_raw, list):
        for item in command_responses_raw:
            command_responses[
                (
                    int(item.get("devcode", probe_target.devcode)),
                    int(item.get("collector_addr", probe_target.collector_addr)),
                    str(item["command"]),
                )
            ] = str(item["response"])

    transport = FixtureTransport(
        registers=registers,
        command_responses=command_responses,
        probe_target=probe_target,
        name=name or str(raw.get("name", "fixture")),
        collector=dict(raw.get("collector", {})),
    )
    return transport, raw


def _command_family(body: bytes) -> str:
    return "pi18" if body.startswith(b"^") else "pi30"


def _encode_command_crc(body: bytes, *, family: str) -> bytes:
    crc = crc16_xmodem(body)
    high = (crc >> 8) & 0xFF
    low = crc & 0xFF
    if family == "pi18":
        return bytes((high, low))
    return bytes((_escape_pi30_crc_byte(high), _escape_pi30_crc_byte(low)))


def _escape_pi30_crc_byte(value: int) -> int:
    return value + 1 if value in {0x28, 0x0D, 0x0A} else value
