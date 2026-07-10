"""Shared register decoding for schema-driven Modbus drivers.

The SRNE and MUST drivers (and future catalog-driven Modbus device packs)
decode register blocks the same way: a block of words is read, then each
``RegisterValueSpec`` extracts one logical value.  The only historical
difference between the driver-local copies was the ASCII character policy,
kept here as an explicit ``ascii_style`` argument:

* ``"printable"`` — any printable character (SRNE product strings).
* ``"model"`` — alphanumerics plus ``" -_/."`` (MUST model prefixes, which
  otherwise pick up stray punctuation from uninitialised registers).
"""

from __future__ import annotations

from typing import Any, Literal

from ..models import RegisterValueSpec, decimals_for_divisor
from .modbus import to_signed_16

AsciiStyle = Literal["printable", "model"]


def decode_ascii_word(value: int, *, style: AsciiStyle = "printable") -> str:
    """Decode one register word as up to two ASCII characters."""

    chars: list[str] = []
    for byte in ((int(value) >> 8) & 0xFF, int(value) & 0xFF):
        if byte in (0x00, 0xFF):
            continue
        char = chr(byte)
        if style == "model":
            if char.isalnum() or char in " -_/.":
                chars.append(char)
        elif char.isprintable():
            chars.append(char)
    return "".join(chars).strip()


def decode_ascii_low_bytes(words: list[int]) -> str:
    """Decode a word list where only the low byte of each word is a character."""

    chars: list[str] = []
    for word in words:
        byte = int(word) & 0xFF
        if byte in (0x00, 0xFF):
            continue
        char = chr(byte)
        if char.isprintable():
            chars.append(char)
    return "".join(chars).strip()


def decode_raw_value(
    registers: dict[int, int],
    spec: RegisterValueSpec,
    *,
    ascii_style: AsciiStyle = "printable",
) -> int | str:
    """Extract the raw (unscaled) value for one spec from decoded registers."""

    if spec.combine == "ascii_low_byte":
        return decode_ascii_low_bytes(
            [registers.get(spec.register + offset, 0) for offset in range(spec.word_count)]
        )
    if spec.combine == "ascii":
        chars: list[str] = []
        for offset in range(spec.word_count):
            chars.append(
                decode_ascii_word(registers.get(spec.register + offset, 0), style=ascii_style)
            )
        return "".join(chars).strip()
    if spec.word_count >= 2:
        high = registers.get(spec.register, 0)
        low = registers.get(spec.register + 1, 0)
        if spec.combine == "u32_low_first":
            raw = (low << 16) | high
        else:
            raw = (high << 16) | low
        if spec.signed and spec.word_count == 2:
            return raw - 0x1_0000_0000 if raw >= 0x8000_0000 else raw
        return raw
    raw = registers.get(spec.register, 0)
    if spec.signed:
        return to_signed_16(raw)
    return raw


def is_all_ones_unavailable(raw: object, spec: RegisterValueSpec) -> bool:
    """Return whether ``raw`` is the Modbus all-ones "not available" marker.

    An all-ones UNSIGNED register is the conventional "value not populated"
    sentinel: a variant that does not implement the register reads 0xFFFF
    (or 0xFFFFFFFF combined). Signed specs are excluded — there 0xFFFF is a
    legitimate -1.
    """

    if spec.signed or not isinstance(raw, int):
        return False
    if spec.word_count >= 2:
        return raw == 0xFFFF_FFFF
    return raw == 0xFFFF


def decode_block(
    start_register: int,
    words: list[int],
    specs: tuple[RegisterValueSpec, ...],
    *,
    ascii_style: AsciiStyle = "printable",
    all_ones_unavailable: bool = False,
) -> dict[str, Any]:
    """Decode one register block into logical values keyed by spec key.

    ``all_ones_unavailable`` opts into the SMG-style sentinel: unsigned
    all-ones raw values decode to ``None`` instead of a bogus 65535/6553.5.
    """

    registers = {start_register + index: int(value) for index, value in enumerate(words)}
    decoded: dict[str, Any] = {}
    for spec in specs:
        raw = decode_raw_value(registers, spec, ascii_style=ascii_style)
        if spec.enum_map is not None:
            decoded[spec.key] = spec.enum_map.get(raw, f"Unknown ({raw})")
            continue
        if all_ones_unavailable and is_all_ones_unavailable(raw, spec):
            decoded[spec.key] = None
            continue
        # The offset applies to the WIRE value, so it must come after the
        # all-ones sentinel check: shifting 0xFFFF first would unmask it.
        if spec.offset and isinstance(raw, (int, float)):
            raw = raw + spec.offset
        if spec.multiplier is not None and isinstance(raw, (int, float)):
            decoded[spec.key] = round(raw * spec.multiplier, spec.decimals or 0)
        elif spec.divisor and isinstance(raw, (int, float)):
            # Without explicit decimals the divisor implies the precision
            # (divisor 10 -> 1 decimal): rounding a scaled reading to an
            # integer would silently truncate real telemetry.  An explicit
            # decimals — including 0 — always wins over the implied value.
            precision = (
                spec.decimals
                if spec.decimals is not None
                else decimals_for_divisor(spec.divisor)
            )
            decoded[spec.key] = round(raw / spec.divisor, precision)
        else:
            decoded[spec.key] = raw
    return decoded


async def read_spec_set_values(
    session,
    schema,
    *,
    spec_set: str = "runtime",
    ascii_style: AsciiStyle = "printable",
) -> dict[str, Any]:
    """Read every schema block and decode the specs it covers.

    Blocks that fail to read are skipped: partially responding devices still
    produce the values they do expose, matching the historical driver
    behaviour.
    """

    values: dict[str, Any] = {}
    specs = schema.spec_set(spec_set)
    for block in schema.blocks:
        block_function = getattr(block, "function", 3)
        try:
            words = await session.read_registers(
                block.start,
                block.count,
                function=block_function,
            )
        except Exception:  # pylint: disable=broad-except
            continue
        block_specs = tuple(
            spec
            for spec in specs
            if getattr(spec, "function", 3) == block_function
            and block.start <= spec.register
            and spec.register + spec.word_count <= block.start + block.count
        )
        values.update(decode_block(block.start, words, block_specs, ascii_style=ascii_style))
    return values
