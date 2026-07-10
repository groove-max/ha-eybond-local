"""Shared identifier masking for share-safe support artifacts.

One masking semantic for everything that may leave the owner's host: a long
digit run (collector PN tails, serial numbers, MAC-derived identifiers) keeps
its first and last four digits so separate artifacts still correlate, and the
middle is starred out. Applies to plain text and to ASCII payloads embedded in
hex blobs.

Documented limitation (per design decision): raw register words in
``decimal``/``hex`` integer arrays are NOT scrubbed — a serial sitting in
arbitrary registers can still appear there.
"""

from __future__ import annotations

import re

_NUMERIC_IDENTIFIER_RE = re.compile(r"(?<!\d)(\d{10,})(?!\d)")
_NUMERIC_IDENTIFIER_BYTES_RE = re.compile(rb"(?<!\d)(\d{10,})(?!\d)")


def mask_identifier_token(token: str) -> str:
    if len(token) <= 4:
        return "*" * len(token)
    return token[:4] + ("*" * max(len(token) - 8, 1)) + token[-4:]


def mask_numeric_identifiers(value):
    """Mask long numeric identifiers in text and ASCII-encoded hex blobs."""

    if isinstance(value, dict):
        # Keys are masked too: a PN/serial used as a mapping key would
        # otherwise leave the artifact unmasked.
        return {
            (mask_numeric_identifiers(key) if isinstance(key, str) else key): (
                mask_numeric_identifiers(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [mask_numeric_identifiers(item) for item in value]
    if not isinstance(value, str):
        return value

    # Hex blobs FIRST: an ASCII-encoded identifier inside hex is itself a
    # long run of decimal characters ("E50..." becomes "4535303030..."), so
    # text-level masking would star out the middle of the blob and corrupt
    # it without ever exposing the ASCII payload to the byte-level pass.
    normalized = "".join(value.split())
    if (
        normalized
        and len(normalized) >= 8
        and len(normalized) % 2 == 0
        and all(char in "0123456789abcdefABCDEF" for char in normalized)
    ):
        try:
            raw = bytes.fromhex(normalized)
        except ValueError:
            raw = None
        if raw is not None:
            redacted = _NUMERIC_IDENTIFIER_BYTES_RE.sub(
                lambda match: mask_identifier_token(
                    match.group(1).decode("ascii")
                ).encode("ascii"),
                raw,
            )
            if redacted != raw:
                return redacted.hex()
            # No embedded ASCII identifier — fall through to text masking:
            # a bare decimal serial ("92632500000001") is also valid hex,
            # and returning it untouched would leak it.

    return _NUMERIC_IDENTIFIER_RE.sub(
        lambda match: mask_identifier_token(match.group(1)),
        value,
    )
