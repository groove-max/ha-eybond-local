"""Native PI30 learning codec shared by proxy and Bridge adapters.

No sockets, cloud calls or physical writes. In particular an ACK produced here
is synthetic, not proof that a device accepted a setting. Commands retain their
exact case, padding and punctuation; a cloud register number is never translated
into an ASCII command.
"""

from __future__ import annotations

import re

from ...cloud_numeric_learning import classify_numeric_encoding, decimal_value
from ...payload.pi30 import build_request, parse_response, Pi30Error
from . import ShadowWriteObservation, utc_now_iso
from .protocol import ShadowLearningProtocolAdapter, ShadowReadRequest, ShadowWriteRequest


_COMMAND = re.compile(r"[A-Z][A-Za-z0-9.,:+-]{0,63}\Z")
_NUMBER_COMMAND = re.compile(r"([A-Za-z]+)([0-9]+(?:\.[0-9]+)?)\Z")


def decode_pi30_learning_command(frame: bytes) -> str | None:
    """Require a complete, bounded command with the PI30 escaped XMODEM CRC."""
    if not isinstance(frame, bytes) or not 4 <= len(frame) <= 67:
        return None
    try:
        command = frame[:-3].decode("ascii")
    except UnicodeDecodeError:
        return None
    if not _COMMAND.fullmatch(command) or build_request(command) != frame:
        return None
    # Collector management and inverter replies are not inverter commands.
    if command.startswith("AT+"):
        return None
    return command


def classify_pi30_numeric_command(samples: list[dict]) -> dict:
    """Prove a direct decimal suffix, including padding, from distinct probes.

    No register-map translation, inferred voltage family, offset or arbitrary
    interpolation. Cloud limits remain optional; probe values are not limits.
    The caller validates wire CRC, target and provider-field correlation first.
    """
    encoded_samples = []
    formats = set()
    for sample in samples:
        command = str(sample.get("observation", {}).get("command") or "")
        match = _NUMBER_COMMAND.fullmatch(command)
        if match is None:
            return {"rejection_reason": "pi30_numeric_encoding_not_proven"}
        prefix, suffix = match.groups()
        value = decimal_value(sample.get("requested_value"))
        precision = len(suffix.partition(".")[2])
        if value is None or value != decimal_value(suffix) or precision > 3:
            return {"rejection_reason": "pi30_numeric_encoding_not_proven"}
        formats.add((prefix, len(suffix), precision))
        raw = value * 10 ** precision
        if raw != raw.to_integral_value():
            return {"rejection_reason": "pi30_numeric_encoding_not_proven"}
        encoded_samples.append({
            **sample,
            "observation": {**sample["observation"], "values": [int(raw)]},
        })
    if len(formats) != 1:
        return {"rejection_reason": "pi30_numeric_format_inconsistent"}
    result = classify_numeric_encoding(encoded_samples)
    if result.get("rejection_reason"):
        return result
    prefix, width, precision = formats.pop()
    return {**result, "command": prefix, "command_width": width,
            "command_precision": precision}


class Pi30ShadowLearningAdapter(ShadowLearningProtocolAdapter):
    """Observe exact native commands; serve only actually captured query data."""

    def __init__(self) -> None:
        super().__init__(key="pi30_ascii", supported=True)

    def decode_read_request(self, frame: bytes) -> ShadowReadRequest | None:
        command = decode_pi30_learning_command(frame)
        if command and command.startswith("Q"):
            return ShadowReadRequest(0, 0, -1, 0, command=command)
        return None

    def decode_write_request(self, frame: bytes) -> ShadowWriteRequest | None:
        command = decode_pi30_learning_command(frame)
        if not command or command.startswith("Q"):
            return None
        # Keep the COMPLETE wire command. Splitting an unknown prefix/value
        # would guess an encoding (PEa, POP02, PCVV27.9 are different grammars).
        return ShadowWriteRequest(0, 0, -1, (), command=command)

    def write_observation(
        self, *, frame: bytes, devcode: int | None, devaddr: int | None,
        timestamp: str = "", source: str = "shadow_learning",
    ) -> ShadowWriteObservation | None:
        request = self.decode_write_request(frame)
        if request is None:
            return None
        return ShadowWriteObservation(
            register=-1, values=(), function_code=0, unit=0,
            devcode=devcode, devaddr=devaddr, raw_payload_hex=frame.hex(),
            protocol=self.key, command=request.command,
            timestamp=timestamp or utc_now_iso(), source=source,
        )

    def build_read_response(self, request: ShadowReadRequest, values: list[int]) -> bytes:
        return self.build_raw_exception(b"", exception_code=1)

    def build_seeded_read_response(
        self, request: ShadowReadRequest, values: list[int],
        command_responses: dict[str, str],
    ) -> bytes:
        value = command_responses.get(request.command)
        if (not request.command.startswith("Q") or not isinstance(value, str)
                or not value or len(value) > 4096
                or any(not 0x20 <= ord(char) <= 0x7E for char in value)):
            return self.build_raw_exception(b"", exception_code=1)
        return build_request("(" + value)

    def build_write_ack_response(self, request: ShadowWriteRequest) -> bytes:
        return build_request("(ACK")

    def build_write_exception_response(
        self, request: ShadowWriteRequest, *, exception_code: int,
    ) -> bytes:
        return self.build_raw_exception(b"", exception_code=exception_code)

    def build_raw_exception(self, frame: bytes, *, exception_code: int) -> bytes:
        return build_request("(NAK")

    def has_command_seed(self, responses: dict[str, str]) -> bool:
        """Require inverter data, not the collector PN or an error response."""
        for command in ("QPIRI", "QPIGS", "QFLAG", "QMN", "QPI"):
            reply = self.build_seeded_read_response(
                ShadowReadRequest(0, 0, -1, 0, command), [], responses,
            )
            try:
                if parse_response(reply) not in ("NAK", "NOA", "ERCRC", "ACK"):
                    return True
            except Pi30Error:
                continue
        return False
