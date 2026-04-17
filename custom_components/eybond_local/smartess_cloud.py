"""Reusable SmartESS cloud client helpers for integration runtime workflows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "https://android.shinemonitor.com/public/"
DEFAULT_LANGUAGE = "en"
DEFAULT_APP_ID = "com.eybond.smartclient.ess"
DEFAULT_APP_VERSION = "3.43.3.0"
DEFAULT_COMPANY_KEY = "bnrl_frRFjEz8Mkn"
DEFAULT_DEVICE_TYPE = 2304
DEFAULT_TIMEOUT = 15.0


_SMARTESS_0925_EXACT = "exact_0925"
_SMARTESS_0925_PROBABLE = "probable_0925"
_SMARTESS_CLOUD_ONLY = "cloud_only"


_SMARTESS_SETTING_TITLE_ALIASES: dict[str, str] = {
    "main output priority": "output priority",
    "output voltage setting": "output voltage",
    "output frequency setting": "output frequency",
    "bat_eq_time": "eq charing time",
    "bat eq time": "eq charing time",
    "maximum charging voltage": "bulk charging voltage (c.v voltage)",
    "floating charge voltage": "floating charging voltage",
    "battery overvoltage protection point": "high dc protection voltage",
    "maximum charging current": "max.charging current",
    "maximum mains charging current": "max.ac.charging current",
    "battery eq mode enable": "battery eq mode",
    "eq charging voltage": "eq charing voltage",
    "eq timeout exit": "eq timeout exit time",
    "eq charging interval": "eq interval time",
    "equalization activated immediately": "forced eq charging",
    "clean generation power": "clear record",
    "mains mode battery low voltage protection point": "low dc protection voltage in mains mode",
    "off-grid mode battery low voltage protection point": "low dc protection voltage in off-grid mode",
}


_SMARTESS_0925_SETTING_BINDINGS: dict[str, dict[str, Any]] = {
    "output priority": {
        "profile_key": "output_source_priority",
        "register_key": "output_source_priority",
        "register": 4537,
    },
    "input voltage range": {
        "profile_key": "input_voltage_range",
        "register_key": "input_voltage_range",
        "register": 4538,
    },
    "battery type": {
        "profile_key": "battery_type",
        "register_key": "battery_type",
        "register": 4539,
    },
    "high dc protection voltage": {
        "profile_key": "battery_overvoltage_protection_voltage",
        "register_key": "battery_overvoltage_protection_voltage",
    },
    "output frequency": {
        "profile_key": "configured_output_frequency",
        "register_key": "configured_output_frequency",
        "register": 4540,
    },
    "max.charging current": {
        "profile_key": "max_total_charge_current",
        "register_key": "max_total_charge_current",
        "register": 4541,
    },
    "output voltage": {
        "profile_key": "configured_output_voltage",
        "register_key": "configured_output_voltage",
        "register": 4542,
    },
    "max.ac.charging current": {
        "profile_key": "max_utility_charge_current",
        "register_key": "max_utility_charge_current",
        "register": 4543,
    },
    "recovery voltage back to mains mode": {
        "profile_key": "utility_return_voltage_sbu",
        "register_key": "utility_return_voltage_sbu",
        "register": 4544,
    },
    "soc recovery value of battery discharge in mains mode": {
        "profile_key": "battery_return_voltage_sbu",
        "register_key": "battery_return_voltage_sbu",
        "register": 4545,
    },
    "bulk charging voltage (c.v voltage)": {
        "profile_key": "bulk_charging_voltage",
        "register_key": "bulk_charging_voltage",
        "register": 4546,
    },
    "floating charging voltage": {
        "profile_key": "floating_charging_voltage",
        "register_key": "floating_charging_voltage",
        "register": 4547,
    },
    "low dc protection voltage in mains mode": {
        "profile_key": "low_battery_cutoff_voltage",
        "register_key": "low_battery_cutoff_voltage",
        "register": 4548,
    },
    "charger source priority": {
        "profile_key": "charger_source_priority",
        "register_key": "charger_source_priority",
        "register": 4536,
    },
    "eq charing voltage": {
        "profile_key": "battery_equalization_voltage",
        "register_key": "battery_equalization_voltage",
        "register": 4549,
    },
    "eq charing time": {
        "profile_key": "battery_equalized_time",
        "register_key": "battery_equalized_time",
        "register": 4550,
    },
    "eq timeout exit time": {
        "profile_key": "battery_equalized_timeout",
        "register_key": "battery_equalized_timeout",
        "register": 4551,
    },
    "eq interval time": {
        "profile_key": "battery_equalization_interval",
        "register_key": "battery_equalization_interval",
        "register": 4552,
    },
    "beeps while primary source is interrupted": {
        "profile_key": "primary_source_interrupt_alarm_enabled",
        "register_key": "primary_source_interrupt_alarm_enabled",
        "register": 5007,
    },
    "backlight control": {
        "profile_key": "lcd_backlight_enabled",
        "register_key": "lcd_backlight_enabled",
        "register": 5004,
    },
    "auto return to default display screen": {
        "profile_key": "lcd_reset_to_default_enabled",
        "register_key": "lcd_reset_to_default_enabled",
        "register": 5008,
    },
    "power saving mode": {
        "profile_key": "power_saving_enabled",
        "register_key": "power_saving_enabled",
        "register": 5003,
    },
    "auto restart when overload occurs": {
        "profile_key": "overload_restart_enabled",
        "register_key": "overload_restart_enabled",
        "register": 5005,
    },
    "auto restart when over temperature occurs": {
        "profile_key": "over_temperature_restart_enabled",
        "register_key": "over_temperature_restart_enabled",
        "register": 5006,
    },
    "overload bypass": {
        "profile_key": "overload_bypass_enabled",
        "register_key": "overload_bypass_enabled",
        "register": 5009,
    },
    "battery eq mode": {
        "profile_key": "battery_equalization_mode",
        "register_key": "battery_equalization_mode",
        "register": 5011,
    },
    "forced eq charging": {
        "profile_key": "force_battery_equalization",
        "register_key": "force_battery_equalization",
        "register": 5012,
    },
    "clear record": {
        "profile_key": "clear_power_history",
        "register_key": "clear_power_history",
        "register": 5001,
    },
    "reset user settings": {
        "profile_key": "restore_defaults",
        "register_key": "restore_defaults",
        "register": 5016,
    },
    "record fault code": {
        "profile_key": "record_fault_code_enabled",
        "register_key": "record_fault_code_enabled",
        "register": 5010,
    },
}

_SMARTESS_0925_SETTING_CLASSIFICATIONS: dict[str, dict[str, Any]] = {
    "output mode": {
        "bucket": _SMARTESS_0925_PROBABLE,
        "source": "pi30_family_surface",
        "reason": (
            "Matches the PI30-family output-mode vocabulary, but no direct 0925 write "
            "register has been proven yet."
        ),
    },
    "output priority": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4537,
        "reason": "Direct 0925 root-map match for output source priority.",
    },
    "input voltage range": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4538,
        "reason": "Direct 0925 root-map match for AC input range.",
    },
    "buzzer mode": {
        "bucket": _SMARTESS_0925_PROBABLE,
        "source": "pi30_family_surface",
        "reason": (
            "A direct 0925 local buzzer toggle exists at register 5002, but the cloud "
            "exposes a richer mode enum than the currently proven local surface."
        ),
    },
    "beeps while primary source is interrupted": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5007,
        "reason": "Direct 0925 root-map match for the primary-source interrupt beeper toggle.",
    },
    "backlight control": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5004,
        "reason": "Direct 0925 root-map match for LCD backlight control.",
    },
    "auto return to default display screen": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5008,
        "reason": "Direct 0925 root-map match for LCD auto-return behavior.",
    },
    "power saving mode": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5003,
        "reason": "Direct 0925 root-map match for power-saving mode.",
    },
    "auto restart when overload occurs": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5005,
        "reason": "Direct 0925 root-map match for overload auto-restart.",
    },
    "auto restart when over temperature occurs": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5006,
        "reason": "Direct 0925 root-map match for over-temperature auto-restart.",
    },
    "overload bypass": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5009,
        "reason": "Direct 0925 root-map match for overload bypass behavior.",
    },
    "battery eq mode": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5011,
        "reason": "Direct 0925 root-map match for battery equalization enablement.",
    },
    "output voltage": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4542,
        "reason": "Direct 0925 root-map match for configured output voltage.",
    },
    "output frequency": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4540,
        "reason": "Direct 0925 root-map match for configured output frequency.",
    },
    "soc recovery value of battery discharge in mains mode": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4545,
        "reason": "Direct 0925 root-map match for the SBU battery return threshold.",
    },
    "battery type": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4539,
        "reason": "Direct 0925 root-map match for battery type.",
    },
    "high dc protection voltage": {
        "bucket": _SMARTESS_0925_PROBABLE,
        "source": "pi30_family_surface",
        "reason": (
            "Looks like the same PI30-family battery-threshold cluster, but no exact 0925 "
            "root-map register has been confirmed."
        ),
    },
    "bulk charging voltage (c.v voltage)": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4546,
        "reason": "Direct 0925 root-map match for bulk charging voltage.",
    },
    "floating charging voltage": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4547,
        "reason": "Direct 0925 root-map match for floating charging voltage.",
    },
    "recovery voltage back to mains mode": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4544,
        "reason": "Direct 0925 root-map match for the SBU return-to-mains threshold.",
    },
    "low dc protection voltage in mains mode": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4548,
        "reason": "Direct 0925 root-map match for low-DC protection in mains mode.",
    },
    "low dc protection voltage in off-grid mode": {
        "bucket": _SMARTESS_0925_PROBABLE,
        "source": "pi30_family_surface",
        "reason": (
            "Looks like the off-grid counterpart to the proven 0925 mains-mode cut-off, but "
            "the local 0925 register has not been confirmed."
        ),
    },
    "time from c.v to floating charge": {
        "bucket": _SMARTESS_0925_PROBABLE,
        "source": "pi30_family_surface",
        "reason": (
            "Fits the PI30-family battery-charge timing surface, but there is no direct 0925 "
            "root-map proof yet."
        ),
    },
    "charger source priority": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4536,
        "reason": "Direct 0925 root-map match for charger source priority.",
    },
    "max.charging current": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4541,
        "reason": "Direct 0925 root-map match for maximum total charge current.",
    },
    "max.ac.charging current": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4543,
        "reason": "Direct 0925 root-map match for maximum AC charge current.",
    },
    "eq charing voltage": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4549,
        "reason": "Direct 0925 root-map match for equalization voltage.",
    },
    "eq charing time": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4550,
        "reason": "Direct 0925 root-map match for equalization time.",
    },
    "eq timeout exit time": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4551,
        "reason": "Direct 0925 root-map match for equalization timeout.",
    },
    "eq interval time": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 4552,
        "reason": "Direct 0925 root-map match for equalization interval.",
    },
    "forced eq charging": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5012,
        "reason": "Direct 0925 root-map match for forced one-shot equalization.",
    },
    "clear record": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5001,
        "reason": "Matches the 0925 clear-history action in the root map.",
    },
    "reset user settings": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5016,
        "reason": "Matches the 0925 restore-defaults action in the root map.",
    },
    "record fault code": {
        "bucket": _SMARTESS_0925_EXACT,
        "source": "root_map_0925",
        "asset_register": 5010,
        "reason": "Direct 0925 root-map match for the fault-code recording toggle.",
    },
}

_SMARTESS_SETTING_AREA_BY_PREFIX = {
    "bse": "output",
    "bat": "battery",
    "sys": "system",
}


class SmartEssCloudError(RuntimeError):
    """Raised when one SmartESS cloud request fails."""


@dataclass(frozen=True, slots=True)
class ApiEnvelope:
    """One parsed SmartESS cloud response envelope."""

    err: int
    desc: str
    dat: Any
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SessionCredentials:
    """Session credentials returned by SmartESS cloud authentication."""

    token: str
    secret: str
    uid: str = ""
    usr: str = ""
    role: int | None = None
    expire: int | None = None


def _sha1_lower(value: str | bytes) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha1(data).hexdigest()


def _normalize_base_url(base_url: str) -> str:
    normalized = str(base_url or DEFAULT_BASE_URL).strip()
    if not normalized:
        normalized = DEFAULT_BASE_URL
    if not normalized.endswith("/"):
        normalized += "/"
    return normalized


def _salt_millis() -> str:
    return str(int(time.time() * 1000))


def _decode_response_body(body: bytes) -> str:
    text = body.decode("utf-8", errors="replace").strip()
    if text.startswith("null(") and text.endswith(")"):
        return text[text.find("(") + 1 : -1]
    return text


def _http_get_json(url: str, *, timeout: float = DEFAULT_TIMEOUT) -> ApiEnvelope:
    request = Request(
        url,
        headers={
            "User-Agent": "okhttp/3.12.1",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = _decode_response_body(response.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace").strip()
        raise SmartEssCloudError(f"http_error:{exc.code}:{detail}") from exc
    except URLError as exc:
        raise SmartEssCloudError(f"network_error:{exc.reason}") from exc

    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SmartEssCloudError(f"invalid_json:{payload[:200]}") from exc

    if not isinstance(raw, dict):
        raise SmartEssCloudError("invalid_envelope:not_an_object")

    return ApiEnvelope(
        err=int(raw.get("err", -1)),
        desc=str(raw.get("desc", "")),
        dat=raw.get("dat"),
        raw=raw,
    )


def _maybe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_login_action(
    *,
    username: str,
    company_key: str = DEFAULT_COMPANY_KEY,
    language: str = DEFAULT_LANGUAGE,
) -> str:
    return (
        f"&action=authSource"
        f"&usr={quote(username, safe='')}"
        f"&company-key={quote(company_key, safe='')}"
        f"&source=1"
        f"&lang={quote(language, safe='')}"
    )


def build_login_url(
    *,
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
    company_key: str = DEFAULT_COMPANY_KEY,
    language: str = DEFAULT_LANGUAGE,
) -> str:
    action = _build_login_action(
        username=username,
        company_key=company_key,
        language=language,
    )
    salt = _salt_millis()
    sign = _sha1_lower(salt + _sha1_lower(password) + action)
    return f"{_normalize_base_url(base_url)}?sign={sign}&salt={salt}{action}"


def login_with_password(
    *,
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
    company_key: str = DEFAULT_COMPANY_KEY,
    language: str = DEFAULT_LANGUAGE,
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[ApiEnvelope, SessionCredentials]:
    envelope = _http_get_json(
        build_login_url(
            username=username,
            password=password,
            base_url=base_url,
            company_key=company_key,
            language=language,
        ),
        timeout=timeout,
    )
    if envelope.err != 0:
        raise SmartEssCloudError(f"login_failed:{envelope.err}:{envelope.desc}")
    if not isinstance(envelope.dat, dict):
        raise SmartEssCloudError("login_failed:missing_dat")

    session = SessionCredentials(
        token=str(envelope.dat.get("token") or "").strip(),
        secret=str(envelope.dat.get("secret") or "").strip(),
        uid=str(envelope.dat.get("uid") or "").strip(),
        usr=str(envelope.dat.get("usr") or "").strip(),
        role=_maybe_int(envelope.dat.get("role")),
        expire=_maybe_int(envelope.dat.get("expire")),
    )
    if not session.token or not session.secret:
        raise SmartEssCloudError("login_failed:missing_token_or_secret")
    return envelope, session


def _build_base_action(
    action: str,
    *,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
) -> str:
    if not action.startswith("&"):
        action = f"&{action}"
    return (
        f"{action}"
        f"&i18n={language}"
        f"&lang={language}"
        f"&source=1"
        f"&_app_client_=android"
        f"&_app_id_={app_id}"
        f"&_app_version_={app_version}"
    )


def build_signed_action_url(
    *,
    action: str,
    session: SessionCredentials,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
) -> str:
    base_action = _build_base_action(
        action,
        language=language,
        app_id=app_id,
        app_version=app_version,
    )
    salt = _salt_millis()
    sign = _sha1_lower(salt + session.secret + session.token + base_action)
    return (
        f"{_normalize_base_url(base_url)}?sign={sign}"
        f"&salt={salt}"
        f"&token={quote(session.token, safe='')}"
        f"{base_action}"
    )


def fetch_signed_action(
    *,
    action: str,
    session: SessionCredentials,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ApiEnvelope:
    envelope = _http_get_json(
        build_signed_action_url(
            action=action,
            session=session,
            base_url=base_url,
            language=language,
            app_id=app_id,
            app_version=app_version,
        ),
        timeout=timeout,
    )
    if envelope.err != 0:
        raise SmartEssCloudError(f"action_failed:{envelope.err}:{envelope.desc}")
    return envelope


def _build_action(action_name: str, parameters: list[tuple[str, Any]]) -> str:
    parts = [f"&action={quote(action_name, safe='')}"]
    for key, value in parameters:
        if value in (None, ""):
            continue
        parts.append(f"&{quote(str(key), safe='')}={quote(str(value), safe='')}")
    return "".join(parts)


def build_device_list_action(
    *,
    device_type: int = DEFAULT_DEVICE_TYPE,
    page: int = 0,
    pagesize: int = 10,
    search: str = "",
    pn: str = "",
    status: str = "",
    brand: str = "",
    order_by: str = "",
) -> str:
    parameters: list[tuple[str, Any]] = [("devtype", device_type)]
    if pn:
        parameters.append(("pn", pn))
    parameters.extend(
        [
            ("page", page),
            ("pagesize", pagesize),
            ("search", search),
            ("status", status),
            ("brand", brand),
            ("orderBy", order_by),
        ]
    )
    return _build_action("webQueryDeviceEs", parameters)


def build_device_detail_action(*, pn: str, sn: str, devcode: int, devaddr: int) -> str:
    return _build_action(
        "querySPDeviceLastData",
        [("pn", pn), ("devcode", devcode), ("devaddr", devaddr), ("sn", sn)],
    )


def build_device_settings_action(*, pn: str, sn: str, devcode: int, devaddr: int) -> str:
    return _build_action(
        "webQueryDeviceCtrlField",
        [("pn", pn), ("devcode", devcode), ("devaddr", devaddr), ("sn", sn)],
    )


def build_device_energy_flow_action(*, pn: str, sn: str, devcode: int, devaddr: int) -> str:
    return _build_action(
        "webQueryDeviceEnergyFlowEs",
        [("pn", pn), ("sn", sn), ("devaddr", devaddr), ("devcode", devcode)],
    )


def normalize_device_list(dat: Any) -> dict[str, Any] | None:
    if not isinstance(dat, dict):
        return None
    raw_devices = dat.get("device")
    if not isinstance(raw_devices, list):
        raw_devices = []

    devices: list[dict[str, Any]] = []
    for item in raw_devices:
        if not isinstance(item, dict):
            continue
        devices.append(
            {
                "pn": item.get("pn"),
                "sn": item.get("sn"),
                "devcode": item.get("devcode"),
                "devaddr": item.get("devaddr"),
                "devName": item.get("devName"),
                "devalias": item.get("devalias"),
                "status": item.get("status"),
                "brand": item.get("brand"),
                "usr": item.get("usr"),
                "uid": item.get("uid"),
                "pid": item.get("pid"),
                "devicePicture": item.get("devicePicture"),
            }
        )

    return {
        "page": dat.get("page"),
        "pagesize": dat.get("pagesize"),
        "total": dat.get("total"),
        "device_count": len(devices),
        "devices": devices,
    }


def normalize_device_detail(dat: Any) -> dict[str, Any] | None:
    if not isinstance(dat, dict):
        return None
    pars = dat.get("pars")
    if not isinstance(pars, dict):
        return None

    sections: dict[str, list[dict[str, Any]]] = {}
    section_counts: dict[str, int] = {}
    for key, value in pars.items():
        if not isinstance(value, list):
            continue
        items: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            items.append(
                {
                    "id": item.get("id"),
                    "par": item.get("par"),
                    "val": item.get("val"),
                    "unit": item.get("unit"),
                }
            )
        if not items:
            continue
        sections[key] = items
        section_counts[key] = len(items)

    return {
        "section_counts": section_counts,
        "sections": sections,
    }


def normalize_device_settings(dat: Any) -> dict[str, Any] | None:
    if not isinstance(dat, dict):
        return None

    raw_fields = dat.get("field")
    if not isinstance(raw_fields, list):
        raw_fields = []

    fields: list[dict[str, Any]] = []
    area_counts: dict[str, int] = {}
    value_kind_counts: dict[str, int] = {}
    bucket_counts: dict[str, int] = {
        _SMARTESS_0925_EXACT: 0,
        _SMARTESS_0925_PROBABLE: 0,
        _SMARTESS_CLOUD_ONLY: 0,
    }
    mapped_field_count = 0
    fields_with_current_value = 0

    for raw_field in raw_fields:
        if not isinstance(raw_field, dict):
            continue
        title = str(
            raw_field.get("name")
            or raw_field.get("field")
            or raw_field.get("title")
            or ""
        ).strip()
        if not title:
            continue

        field_id = str(raw_field.get("id") or "").strip()
        unit = _optional_text(raw_field.get("unit"))
        hint = _optional_text(raw_field.get("hint"))
        current_value, has_current_value = _extract_current_value(raw_field)
        if has_current_value:
            fields_with_current_value += 1

        choices = _normalize_setting_choices(raw_field.get("item"))
        value_kind = _infer_setting_value_kind(
            choices=choices,
            unit=unit,
            has_current_value=has_current_value,
        )
        area = _infer_setting_area(field_id)
        normalized_title = _normalize_setting_name(title)
        binding = _SMARTESS_0925_SETTING_BINDINGS.get(normalized_title)
        classification = _resolve_smartess_0925_setting_classification(normalized_title)
        if binding is not None:
            mapped_field_count += 1

        field: dict[str, Any] = {
            "cloud_id": field_id,
            "title": title,
            "area": area,
            "value_kind": value_kind,
            "unit": unit,
            "hint": hint,
            "has_current_value": has_current_value,
            "choice_count": len(choices),
            "choices": choices,
            "bucket": classification["bucket"],
            "bucket_source": classification["source"],
            "bucket_reason": classification["reason"],
        }
        if has_current_value:
            field["current_value"] = current_value
        asset_register = classification.get("asset_register")
        if isinstance(asset_register, int):
            field["asset_register"] = asset_register
        if binding is not None:
            field["binding"] = dict(binding)

        fields.append(field)
        area_counts[area] = area_counts.get(area, 0) + 1
        value_kind_counts[value_kind] = value_kind_counts.get(value_kind, 0) + 1
        bucket = str(classification["bucket"])
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

    two_tier = dat.get("two_tier")
    field_count = len(fields)
    return {
        "field_count": field_count,
        "mapped_field_count": mapped_field_count,
        "unmapped_field_count": field_count - mapped_field_count,
        "fields_with_current_value": fields_with_current_value,
        "fields_without_current_value": field_count - fields_with_current_value,
        "current_values_included": fields_with_current_value > 0,
        "area_counts": area_counts,
        "value_kind_counts": value_kind_counts,
        "bucket_counts": bucket_counts,
        "exact_0925_field_count": bucket_counts[_SMARTESS_0925_EXACT],
        "probable_0925_field_count": bucket_counts[_SMARTESS_0925_PROBABLE],
        "cloud_only_field_count": bucket_counts[_SMARTESS_CLOUD_ONLY],
        "write_action": "ctrlDevice",
        "write_action_known_from_apk_analysis": True,
        "two_tier_present": isinstance(two_tier, dict) and bool(two_tier),
        "fields": fields,
    }


def _resolve_smartess_0925_setting_classification(normalized_title: str) -> dict[str, Any]:
    classification = _SMARTESS_0925_SETTING_CLASSIFICATIONS.get(normalized_title)
    if classification is not None:
        return dict(classification)
    return {
        "bucket": _SMARTESS_CLOUD_ONLY,
        "source": "cloud_payload_only",
        "reason": "No current 0925 root-map or PI30-family local evidence is linked to this field.",
    }


def _normalize_setting_choices(raw_choices: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_choices, list):
        return []

    choices: list[dict[str, Any]] = []
    for index, raw_choice in enumerate(raw_choices):
        if not isinstance(raw_choice, dict):
            continue
        raw_key = raw_choice.get("key")
        label = raw_choice.get("val")
        if raw_key in (None, "") and label in (None, ""):
            continue
        parsed_value = _parse_scalar(raw_key)
        choice: dict[str, Any] = {
            "value": parsed_value,
            "raw_value": "" if raw_key is None else str(raw_key),
            "label": str(label or raw_key or "").strip(),
            "order": index,
        }
        choices.append(choice)
    return choices


def _extract_current_value(raw_field: dict[str, Any]) -> tuple[Any, bool]:
    for key in (
        "current_value",
        "currentValue",
        "selected_value",
        "selectedValue",
        "value",
        "val",
    ):
        if key not in raw_field:
            continue
        value = raw_field.get(key)
        if value in (None, ""):
            continue
        return _parse_scalar(value), True
    return None, False


def _infer_setting_area(field_id: str) -> str:
    prefix = str(field_id or "").split("_", 1)[0].lower()
    return _SMARTESS_SETTING_AREA_BY_PREFIX.get(prefix, "other")


def _infer_setting_value_kind(
    *,
    choices: list[dict[str, Any]],
    unit: str | None,
    has_current_value: bool,
) -> str:
    if choices:
        numeric_choices = {choice.get("value") for choice in choices}
        if numeric_choices == {0, 1}:
            return "bool"
        if len(choices) == 1 and not unit and not has_current_value:
            return "action"
        return "enum"
    if unit:
        return "number"
    if has_current_value:
        return "value"
    return "unknown"


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip() or None


def _normalize_setting_name(value: str) -> str:
    normalized = " ".join(str(value or "").strip().lower().split())
    return _SMARTESS_SETTING_TITLE_ALIASES.get(normalized, normalized)


def _parse_scalar(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return value
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        if any(char in text for char in (".", "e", "E")):
            return float(text)
        return int(text)
    except ValueError:
        return text


def _mask(value: str, *, visible: int = 4) -> str:
    text = str(value or "")
    if not text:
        return ""
    if len(text) <= visible * 2:
        return "*" * len(text)
    return f"{text[:visible]}...{text[-visible:]}"


def session_preview(session: SessionCredentials) -> dict[str, Any]:
    preview = asdict(session)
    preview["token"] = _mask(session.token)
    preview["secret"] = _mask(session.secret)
    return preview


def _build_response_block(
    *,
    action: str,
    envelope: ApiEnvelope,
    normalized: Any = None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "action": action,
        "response": {"err": envelope.err, "desc": envelope.desc},
        "dat": envelope.dat,
    }
    if normalized is not None:
        response["normalized"] = normalized
    return response


def fetch_device_bundle_for_identity(
    *,
    session: SessionCredentials,
    session_source: str,
    pn: str,
    sn: str,
    devcode: int,
    devaddr: int,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    list_action = build_device_list_action(pn=pn, pagesize=50)
    detail_action = build_device_detail_action(
        pn=pn,
        sn=sn,
        devcode=devcode,
        devaddr=devaddr,
    )
    settings_action = build_device_settings_action(
        pn=pn,
        sn=sn,
        devcode=devcode,
        devaddr=devaddr,
    )
    energy_flow_action = build_device_energy_flow_action(
        pn=pn,
        sn=sn,
        devcode=devcode,
        devaddr=devaddr,
    )

    list_envelope = fetch_signed_action(
        action=list_action,
        session=session,
        base_url=base_url,
        language=language,
        app_id=app_id,
        app_version=app_version,
        timeout=timeout,
    )
    detail_envelope = fetch_signed_action(
        action=detail_action,
        session=session,
        base_url=base_url,
        language=language,
        app_id=app_id,
        app_version=app_version,
        timeout=timeout,
    )
    settings_envelope = fetch_signed_action(
        action=settings_action,
        session=session,
        base_url=base_url,
        language=language,
        app_id=app_id,
        app_version=app_version,
        timeout=timeout,
    )
    energy_flow_envelope = fetch_signed_action(
        action=energy_flow_action,
        session=session,
        base_url=base_url,
        language=language,
        app_id=app_id,
        app_version=app_version,
        timeout=timeout,
    )

    normalized_list = normalize_device_list(list_envelope.dat)
    normalized_detail = normalize_device_detail(detail_envelope.dat)
    normalized_settings = normalize_device_settings(settings_envelope.dat)
    return {
        "request": {
            "command": "device-bundle",
            "actions": {
                "device_list": "webQueryDeviceEs",
                "device_detail": "querySPDeviceLastData",
                "device_settings": "webQueryDeviceCtrlField",
                "energy_flow": "webQueryDeviceEnergyFlowEs",
            },
            "params": {
                "device_type": DEFAULT_DEVICE_TYPE,
                "pn": pn,
                "sn": sn,
                "devcode": devcode,
                "devaddr": devaddr,
                "search": "",
                "status": "",
                "brand": "",
                "order_by": "",
                "page": 0,
                "pagesize": 50,
            },
            "session_source": session_source,
        },
        "session": session_preview(session),
        "responses": {
            "device_list": _build_response_block(
                action="webQueryDeviceEs",
                envelope=list_envelope,
                normalized=normalized_list,
            ),
            "device_detail": _build_response_block(
                action="querySPDeviceLastData",
                envelope=detail_envelope,
                normalized=normalized_detail,
            ),
            "device_settings": _build_response_block(
                action="webQueryDeviceCtrlField",
                envelope=settings_envelope,
                normalized=normalized_settings,
            ),
            "energy_flow": _build_response_block(
                action="webQueryDeviceEnergyFlowEs",
                envelope=energy_flow_envelope,
            ),
        },
        "normalized": {
            "device_list": normalized_list,
            "device_detail": normalized_detail,
            "device_settings": normalized_settings,
        },
    }


def fetch_device_bundle_for_collector(
    *,
    username: str,
    password: str,
    collector_pn: str,
    base_url: str = DEFAULT_BASE_URL,
    company_key: str = DEFAULT_COMPANY_KEY,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    _, session = login_with_password(
        username=username,
        password=password,
        base_url=base_url,
        company_key=company_key,
        language=language,
        timeout=timeout,
    )
    list_action = build_device_list_action(
        pn=collector_pn,
        pagesize=50,
    )
    list_envelope = fetch_signed_action(
        action=list_action,
        session=session,
        base_url=base_url,
        language=language,
        app_id=app_id,
        app_version=app_version,
        timeout=timeout,
    )
    normalized_list = normalize_device_list(list_envelope.dat) or {"devices": []}
    device = _resolve_bundle_device_identity(normalized_list, collector_pn=collector_pn)

    return fetch_device_bundle_for_identity(
        session=session,
        session_source="login",
        pn=str(device["pn"]),
        sn=str(device["sn"]),
        devcode=int(device["devcode"]),
        devaddr=int(device["devaddr"]),
        base_url=base_url,
        language=language,
        app_id=app_id,
        app_version=app_version,
        timeout=timeout,
    )


def _resolve_bundle_device_identity(
    normalized_list: dict[str, Any],
    *,
    collector_pn: str,
) -> dict[str, Any]:
    devices = list(normalized_list.get("devices") or [])
    if not devices:
        raise SmartEssCloudError("collector_device_identity_not_found")

    prefix_matches = [
        device
        for device in devices
        if str(device.get("pn") or "").startswith(collector_pn)
        or str(device.get("sn") or "").startswith(collector_pn)
    ]
    candidates = prefix_matches or devices
    if len(candidates) != 1:
        raise SmartEssCloudError(
            f"collector_device_identity_ambiguous:{len(candidates)}"
        )

    device = candidates[0]
    devcode = _maybe_int(device.get("devcode"))
    devaddr = _maybe_int(device.get("devaddr"))
    pn = str(device.get("pn") or "").strip()
    sn = str(device.get("sn") or "").strip()
    if not pn or not sn or devcode is None or devaddr is None:
        raise SmartEssCloudError("collector_device_identity_incomplete")
    return {
        "pn": pn,
        "sn": sn,
        "devcode": devcode,
        "devaddr": devaddr,
    }