"""Remove structured credentials/account data from shareable support payloads.

This is an export boundary, not a mutation of saved cloud evidence or live
sessions. It deliberately keeps protocol IDs, register words and timestamps.
Arbitrary wire data and user-written prose still require review before sharing.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
import json
from pathlib import PurePath
import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_SENSITIVE_PARTS = ("password", "secret", "token", "authorization")
_SENSITIVE_NAMES = frozenset(
    {
        "account", "accountid", "accountname", "login", "uid", "user", "userid",
        "username", "usr", "email", "emailaddress", "passwd", "pwd", "apikey",
        "accesskey", "cookie", "setcookie", "authsig",
    }
)
# A protocol/detection signature is useful evidence, not an API credential.
_URL_SIGNATURE_NAMES = frozenset({"sign", "signature"})
_URL = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)


def _sensitive_key(key: object) -> bool:
    normalized = str(key).strip().casefold()
    compact = normalized.replace("_", "").replace("-", "")
    return compact in _SENSITIVE_NAMES or any(part in compact for part in _SENSITIVE_PARTS)


def _safe_query(query: str) -> str:
    return urlencode([
        (key, value)
        for key, value in parse_qsl(query, keep_blank_values=True)
        if not _sensitive_key(key) and key.casefold() not in _URL_SIGNATURE_NAMES
    ])


def _safe_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        # Userinfo may itself contain ':' or percent-encoded credentials.
        netloc = parts.netloc.rsplit("@", 1)[-1]
        fragment = parts.fragment
        if "=" in fragment:
            fragment = _safe_query(fragment)
        return urlunsplit((parts.scheme, netloc, parts.path, _safe_query(parts.query), fragment))
    except ValueError:
        return "<redacted-invalid-url>"


def sanitize_support_payload(value: Any) -> Any:
    """Copy a payload into JSON-compatible, credential-free structured data."""

    if isinstance(value, dict):
        return {
            str(key): sanitize_support_payload(item)
            for key, item in value.items() if not _sensitive_key(key)
        }
    if isinstance(value, (list, tuple)):
        return [sanitize_support_payload(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [sanitize_support_payload(item) for item in sorted(value, key=str)]
    if isinstance(value, str):
        # Some older evidence wraps an API JSON response in a string.
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                parsed = json.loads(stripped)
            except (ValueError, RecursionError):
                pass
            else:
                return json.dumps(sanitize_support_payload(parsed), ensure_ascii=False)
        # HA download links are relative and carry a short-lived authSig.
        if stripped.startswith("/") and "?" in stripped:
            return _safe_url(stripped)
        return _URL.sub(lambda match: _safe_url(match.group()), value)
    if isinstance(value, Enum):
        return sanitize_support_payload(value.value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, PurePath):
        return str(value)
    # Do not leak a credential-bearing repr via json.dumps(default=str).
    return f"<unsupported:{type(value).__name__}>"
