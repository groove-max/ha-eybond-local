"""Encode/decode Modbus write-capability values.

One codec for every Modbus-writing driver (SMG, generic catalog packs):
capability value handling must not fork per driver.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ..models import WriteCapability, decimals_for_divisor


class CapabilityPreWriteReadError(RuntimeError):
    """A masked write failed before any register mutation was attempted."""


class CapabilityPostWriteReadError(RuntimeError):
    """A register write completed but its mandatory wire readback failed."""


def find_capability(
    capability_key: str,
    capabilities: tuple[WriteCapability, ...],
) -> WriteCapability:
    for capability in capabilities:
        if capability.key == capability_key:
            return capability
    raise ValueError(f"unsupported_capability:{capability_key}")


def encode_capability_words(capability: WriteCapability, value: Any) -> list[int]:
    if capability.value_kind == "action":
        return [_encode_action_value(capability, value)]
    if capability.value_kind == "bool":
        return [_encode_bool_value(capability, value)]
    if capability.value_kind == "enum":
        return [_encode_enum_value(capability, value)]
    if capability.value_kind == "scaled_u16":
        return [_encode_scaled_u16_value(capability, value)]
    if capability.value_kind == "u16":
        return [_encode_u16_value(capability, value)]
    if capability.value_kind == "time_hhmm":
        return [_encode_time_hhmm_value(capability, value)]
    if capability.value_kind == "u32":
        return _encode_u32_words(capability, value)
    if capability.value_kind == "date_words":
        return _encode_date_words(capability, value)
    if capability.value_kind == "time_words":
        return _encode_time_words(capability, value)
    raise ValueError(f"unsupported_value_kind:{capability.value_kind}")


def decode_capability_value(capability: WriteCapability, raw_words: list[int]) -> Any:
    if not raw_words:
        raise ValueError(f"missing_raw_words:{capability.key}")
    raw_value = raw_words[0]
    if capability.value_kind == "action":
        return raw_value
    if capability.value_kind == "u32":
        return _decode_u32_words(capability, raw_words)
    if capability.value_kind == "date_words":
        return _decode_date_words(capability, raw_words)
    if capability.value_kind == "time_words":
        return _decode_time_words(capability, raw_words)
    if capability.value_kind == "time_hhmm":
        return _decode_time_hhmm_value(capability, raw_value)
    enum_map = capability.enum_value_map
    if capability.value_kind == "bool":
        if enum_map:
            return enum_map.get(raw_value, bool(raw_value))
        return bool(raw_value)
    if enum_map:
        return enum_map.get(raw_value, f"Unknown ({raw_value})")
    if capability.divisor:
        return round(raw_value / capability.divisor, decimals_for_divisor(capability.divisor))
    if capability.multiplier is not None:
        return raw_value * capability.multiplier
    return raw_value


def merge_capability_register_word(
    capability: WriteCapability,
    *,
    current_word: int,
    encoded_word: int,
) -> int:
    """Merge one encoded capability field into its shared register word."""

    from ..payload.modbus import merge_register_field

    if not capability.bitmask:
        raise ValueError(f"capability_has_no_bitmask:{capability.key}")
    shift = capability.bitmask_shift
    field = (int(encoded_word) << shift) & 0xFFFF
    if field & ~capability.bitmask:
        raise ValueError(f"value_exceeds_bitmask:{capability.key}:{encoded_word}")
    return merge_register_field(int(current_word), capability.bitmask, field)


def _encode_enum_value(capability: WriteCapability, value: Any) -> int:
    enum_map = capability.enum_value_map
    if isinstance(value, int):
        raw_value = value
    else:
        text = str(value).strip()
        if text.isdigit():
            raw_value = int(text)
        else:
            reverse_map = {label: key for key, label in enum_map.items()}
            if text not in reverse_map:
                raise ValueError(f"unsupported_enum_value:{capability.key}:{text}")
            raw_value = reverse_map[text]

    if raw_value not in enum_map:
        raise ValueError(f"unsupported_enum_raw:{capability.key}:{raw_value}")
    return raw_value


def _encode_bool_value(capability: WriteCapability, value: Any) -> int:
    enum_map = capability.enum_value_map
    if isinstance(value, bool):
        raw_value = 1 if value else 0
    elif isinstance(value, int):
        raw_value = value
    else:
        text = str(value).strip().lower()
        truthy = {"1", "true", "on", "yes", "enable", "enabled"}
        falsy = {"0", "false", "off", "no", "disable", "disabled"}
        if text in truthy:
            raw_value = 1
        elif text in falsy:
            raw_value = 0
        else:
            reverse_map = {label.lower(): key for key, label in enum_map.items()}
            if text not in reverse_map:
                raise ValueError(f"unsupported_bool_value:{capability.key}:{value}")
            raw_value = reverse_map[text]

    if raw_value not in {0, 1}:
        raise ValueError(f"unsupported_bool_raw:{capability.key}:{raw_value}")
    return raw_value


def _encode_action_value(capability: WriteCapability, value: Any) -> int:
    if value is None:
        if capability.action_value is None:
            raise ValueError(f"missing_action_value:{capability.key}")
        return capability.action_value
    try:
        raw_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_action_value:{capability.key}:{value}") from exc
    if capability.action_value is not None and raw_value != capability.action_value:
        raise ValueError(f"unsupported_action_value:{capability.key}:{raw_value}")
    return raw_value


def _encode_scaled_u16_value(capability: WriteCapability, value: Any) -> int:
    if capability.divisor is None and capability.multiplier is None:
        raise ValueError(f"missing_scale:{capability.key}")
    if capability.divisor is not None and capability.multiplier is not None:
        raise ValueError(f"ambiguous_scale:{capability.key}")

    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_numeric_value:{capability.key}:{value}") from exc

    raw_value = (
        int(round(numeric * capability.divisor))
        if capability.divisor is not None
        else int(round(numeric / float(capability.multiplier)))
    )
    _validate_range(capability, raw_value)
    return raw_value


def _encode_time_hhmm_value(capability: WriteCapability, value: Any) -> int:
    """Encode an HH:MM string (or exact HHMM integer) into one register."""

    if type(value) is str:
        if value != value.strip():
            raise ValueError(f"invalid_time_value:{capability.key}:{value}")
        if len(value) == 5 and value[2] == ":":
            hour_text, minute_text = value.split(":", 1)
            if not (hour_text.isascii() and hour_text.isdecimal() and minute_text.isascii() and minute_text.isdecimal()):
                raise ValueError(f"invalid_time_value:{capability.key}:{value}")
            hour = int(hour_text)
            minute = int(minute_text)
            raw_value = hour * 100 + minute
        elif value.isascii() and value.isdecimal():
            raw_value = int(value)
        else:
            raise ValueError(f"invalid_time_value:{capability.key}:{value}")
    elif type(value) is int:
        raw_value = value
    else:
        raise ValueError(f"invalid_time_value:{capability.key}:{value}")

    hour, minute = divmod(raw_value, 100)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"invalid_time_value:{capability.key}:{value}")
    _validate_range(capability, raw_value)
    return raw_value


def _decode_time_hhmm_value(capability: WriteCapability, raw_value: int) -> str:
    hour, minute = divmod(int(raw_value), 100)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"invalid_time_word:{capability.key}:{raw_value}")
    return f"{hour:02d}:{minute:02d}"


def _encode_u16_value(capability: WriteCapability, value: Any) -> int:
    try:
        raw_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_integer_value:{capability.key}:{value}") from exc

    _validate_range(capability, raw_value)
    return raw_value


def _encode_u32_words(capability: WriteCapability, value: Any) -> list[int]:
    raw_value: int
    if isinstance(value, str):
        text = value.strip()
        try:
            raw_value = int(text, 0)
        except ValueError as exc:
            raise ValueError(f"invalid_integer_value:{capability.key}:{value}") from exc
    else:
        try:
            raw_value = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid_integer_value:{capability.key}:{value}") from exc

    _validate_range(capability, raw_value)
    if capability.word_count != 2:
        raise ValueError(f"unsupported_word_count:{capability.key}:{capability.word_count}")
    if capability.combine != "u32_high_first":
        raise ValueError(f"unsupported_combine:{capability.key}:{capability.combine}")
    return [(raw_value >> 16) & 0xFFFF, raw_value & 0xFFFF]


def _decode_u32_words(capability: WriteCapability, raw_words: list[int]) -> int:
    if capability.word_count != 2:
        raise ValueError(f"unsupported_word_count:{capability.key}:{capability.word_count}")
    if capability.combine != "u32_high_first":
        raise ValueError(f"unsupported_combine:{capability.key}:{capability.combine}")
    if len(raw_words) != 2:
        raise ValueError(f"unexpected_word_length:{capability.key}:{len(raw_words)}")
    return ((raw_words[0] & 0xFFFF) << 16) | (raw_words[1] & 0xFFFF)


def _encode_date_words(capability: WriteCapability, value: Any) -> list[int]:
    if capability.word_count != 3:
        raise ValueError(f"unsupported_word_count:{capability.key}:{capability.word_count}")
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            try:
                parsed = datetime.strptime(text, "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError(f"invalid_date_value:{capability.key}:{value}") from exc
    return [parsed.year, parsed.month, parsed.day]


def _decode_date_words(capability: WriteCapability, raw_words: list[int]) -> str:
    if capability.word_count != 3:
        raise ValueError(f"unsupported_word_count:{capability.key}:{capability.word_count}")
    if len(raw_words) != 3:
        raise ValueError(f"unexpected_word_length:{capability.key}:{len(raw_words)}")
    year, month, day = raw_words
    datetime(year, month, day)
    return f"{year:04d}-{month:02d}-{day:02d}"


def _encode_time_words(capability: WriteCapability, value: Any) -> list[int]:
    if capability.word_count != 3:
        raise ValueError(f"unsupported_word_count:{capability.key}:{capability.word_count}")
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        parsed = None
        for time_format in ("%H:%M:%S", "%H:%M"):
            try:
                parsed = datetime.strptime(text, time_format)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError(f"invalid_time_value:{capability.key}:{value}")
    return [parsed.hour, parsed.minute, parsed.second]


def _decode_time_words(capability: WriteCapability, raw_words: list[int]) -> str:
    if capability.word_count != 3:
        raise ValueError(f"unsupported_word_count:{capability.key}:{capability.word_count}")
    if len(raw_words) != 3:
        raise ValueError(f"unexpected_word_length:{capability.key}:{len(raw_words)}")
    hour, minute, second = raw_words
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        raise ValueError(f"invalid_time_words:{capability.key}:{raw_words}")
    return f"{hour:02d}:{minute:02d}:{second:02d}"


def _validate_range(capability: WriteCapability, raw_value: int) -> None:
    if capability.minimum is not None and raw_value < capability.minimum:
        raise ValueError(f"value_below_minimum:{capability.key}:{raw_value}")
    if capability.maximum is not None and raw_value > capability.maximum:
        raise ValueError(f"value_above_maximum:{capability.key}:{raw_value}")
