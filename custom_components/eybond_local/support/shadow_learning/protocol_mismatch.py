"""Cloud read evidence is a protocol hint, never an inverter/profile identity."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass

from ...collector.protocol import HEADER_SIZE, decode_header
from ...models import ProbeTarget
from ...payload.modbus import decode_read_request


@dataclass(frozen=True, slots=True)
class CloudModbusRead:
    """One bounded CRC-valid read observed on a PN-verified cloud session."""

    devcode: int
    collector_addr: int
    device_addr: int
    function: int
    address: int
    count: int

    def __post_init__(self) -> None:
        bounds = ((self.devcode, 0, 65535), (self.collector_addr, 0, 255),
                  (self.device_addr, 1, 247), (self.function, 3, 4),
                  (self.address, 0, 65535), (self.count, 1, 125))
        if any(type(value) is not int or not low <= value <= high
               for value, low, high in bounds) or self.address + self.count > 65536:
            raise ValueError("invalid_cloud_modbus_read")

    @property
    def target(self) -> ProbeTarget:
        return ProbeTarget(self.devcode, self.collector_addr, self.device_addr)

    def to_mapping(self) -> dict[str, int]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, raw: object) -> CloudModbusRead | None:
        if not isinstance(raw, Mapping):
            return None
        try:
            return cls(**{key: raw[key] for key in cls.__dataclass_fields__})
        except (KeyError, TypeError, ValueError):
            return None

    @classmethod
    def from_frame(cls, frame: bytes) -> CloudModbusRead | None:
        if len(frame) != HEADER_SIZE + 8:
            return None
        header = decode_header(frame[:HEADER_SIZE])
        if header.fcode != 4 or header.total_len != len(frame):
            return None
        request = decode_read_request(frame[HEADER_SIZE:])
        if request is None:
            return None
        try:
            return cls(header.devcode, header.devaddr, request.slave_id,
                       request.function_code, request.address, request.count)
        except ValueError:
            return None


class CloudProtocolMismatchError(RuntimeError):
    """Carry only validated read coordinates through route rollback to the UI."""

    def __init__(self, read: CloudModbusRead | None = None, *, evidence=None,
                 diagnostics=None, reason="cloud_payload_modbus_requires_register_seed") -> None:
        super().__init__("shadow_learning_protocol_preparation_failed:" + reason)
        self.cloud_read = read
        self.evidence = evidence
        self.diagnostics = diagnostics or {}
