"""Human-readable labels for internal runtime paths and runtime-profile reports."""

from __future__ import annotations

from typing import Final


_RUNTIME_PATH_LABELS: Final[dict[str, str]] = {
    "modbus_smg": "SMG-family runtime",
    "pi18": "PI18-family runtime",
    "pi30": "PI30-family runtime",
    "smartess_local": "SmartESS-local runtime",
}

_RUNTIME_PROFILE_LABELS: Final[dict[str, str]] = {
    "modbus_smg_base": "SMG-family base runtime profile",
    "modbus_smg_default": "SMG-family default runtime profile",
    "modbus_smg_family_base": "SMG-family shared base runtime profile",
    "modbus_smg_family_fallback": "SMG-family read-only fallback runtime profile",
    "pi30_ascii": "PI30-family runtime profile",
    "pi30_ascii_default": "PI30-family default runtime profile",
    "pi30_ascii_smartess_0925_compat": "PI30-family SmartESS 0925 compatibility runtime profile",
    "smg_modbus": "SMG-family default runtime profile",
}


def runtime_path_label(driver_key: str) -> str:
    """Return one clarifying label for one internal runtime path key."""

    normalized_key = _normalized(driver_key)
    if not normalized_key:
        return ""
    return _RUNTIME_PATH_LABELS.get(normalized_key, normalized_key)


def runtime_profile_label(
    *,
    profile_key: str = "",
    driver_key: str = "",
    title: str = "",
) -> str:
    """Return one user-facing label for one implementation-level runtime profile."""

    normalized_key = _normalized(profile_key)
    normalized_title = _normalized(title)
    if normalized_key in _RUNTIME_PROFILE_LABELS:
        return _RUNTIME_PROFILE_LABELS[normalized_key]

    if normalized_key.startswith("modbus_smg_anenji_"):
        if normalized_title:
            return f"{normalized_title} model-specific runtime profile"
        return "SMG-family Anenji model-specific runtime profile"

    rewritten_title = _rewrite_legacy_runtime_title(normalized_title)
    if rewritten_title:
        if rewritten_title == normalized_title:
            return rewritten_title
        if rewritten_title.lower().endswith("profile"):
            return rewritten_title
        return f"{rewritten_title} runtime profile"

    runtime_path = runtime_path_label(driver_key)
    if runtime_path:
        return f"{runtime_path} profile"
    return normalized_key


def _rewrite_legacy_runtime_title(title: str) -> str:
    normalized_title = _normalized(title)
    if not normalized_title:
        return ""

    rewritten = normalized_title
    rewritten = rewritten.replace("PI30 / ASCII", "PI30-family runtime")
    rewritten = rewritten.replace("SMG / Modbus", "SMG-family runtime")
    return rewritten


def _normalized(value: str) -> str:
    return str(value or "").strip()