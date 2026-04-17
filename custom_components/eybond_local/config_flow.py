"""Config flow for EyeBond Local."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
import json
import logging
from functools import lru_cache, wraps
from pathlib import Path
import socket
import subprocess
import time
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback
from homeassistant.data_entry_flow import section
from homeassistant.helpers.selector import (
    BooleanSelector,
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
)

from .connection.branch_registry import get_connection_branch, supported_connection_types
from .connection.entry import (
    build_detected_entry_settings,
    build_manual_entry_settings,
    build_runtime_option_settings,
    with_driver_hint,
)
from .connection.models import build_connection_spec_from_values
from .connection.ui import ConnectionFormField
from .const import (
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_TYPE,
    CONF_CONNECTION_MODE,
    CONF_CONTROL_MODE,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DETECTION_CONFIDENCE,
    CONF_SMARTESS_COLLECTOR_VERSION,
    CONF_SMARTESS_DEVICE_ADDRESS,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
    CONTROL_MODE_AUTO,
    CONTROL_MODE_FULL,
    CONTROL_MODE_READ_ONLY,
    CONNECTION_TYPE_EYBOND,
    DEFAULT_CONTROL_MODE,
    CONF_DISCOVERY_INTERVAL,
    CONF_DISCOVERY_TARGET,
    CONF_DRIVER_HINT,
    CONF_HEARTBEAT_INTERVAL,
    CONF_POLL_INTERVAL,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    DEFAULT_COLLECTOR_IP,
    DEFAULT_DISCOVERY_INTERVAL,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    DOMAIN,
    DRIVER_HINT_AUTO,
)
from .control_policy import control_mode_options
from .drivers.registry import driver_options
from .metadata.local_metadata import (
    draft_activates_automatically,
    local_profile_override_details,
    local_register_schema_override_details,
    resolve_local_metadata_rollback_paths,
)
from .metadata.profile_loader import load_driver_profile
from .metadata.smartess_draft import resolve_smartess_known_family_draft_plan
from .models import OnboardingResult
from .onboarding.factory import create_onboarding_manager
from .onboarding.presentation import (
    confidence_sort_score,
    has_smartess_collector_hint,
    scan_result_sort_key,
    scan_result_status_code,
)
from .support.cloud_evidence import fetch_and_export_smartess_device_bundle_cloud_evidence

CONF_RESULT_KEY = "result_key"
CONF_SETUP_MODE = "setup_mode"
CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE = "smartess_cloud_mode"
SETUP_MODE_AUTO = "auto"
SETUP_MODE_MANUAL = "manual"
MANUAL_CONFIRM_ACTION_PROBE_AGAIN = "manual_probe_again"
MANUAL_CONFIRM_ACTION_EDIT_SETTINGS = "manual_edit_settings"
MANUAL_CONFIRM_ACTION_CREATE_PENDING = "manual_create_pending"
SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED = "use_saved"
SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH = "refresh"
SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY = "archive_only"
_INT_FIELDS = {
    CONF_ADVERTISED_TCP_PORT,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    CONF_DISCOVERY_INTERVAL,
    CONF_HEARTBEAT_INTERVAL,
    CONF_POLL_INTERVAL,
}
logger = logging.getLogger(__name__)
_TRANSLATIONS_DIR = Path(__file__).with_name("translations")
_FLOW_TRANSLATIONS_DIR = Path(__file__).with_name("flow_translations")
_AUTO_SCAN_TIMEOUT = 45.0
_MANUAL_PROBE_TIMEOUT = 20.0
_SCAN_PROGRESS_BAR_WIDTH = 12


@dataclass(slots=True)
class _SmartEssCloudSettingHighlight:
    """Compact preview for one SmartESS cloud field shown during onboarding."""

    title: str
    bucket: str = ""
    current_value: str = ""
    register: int | None = None


@dataclass(slots=True)
class _SmartEssCloudAssistState:
    """One saved SmartESS cloud-assist result for the current onboarding flow."""

    collector_pn: str
    evidence_path: str = ""
    inferred_asset_id: str = ""
    inferred_profile_key: str = ""
    inferred_driver_key: str = ""
    inferred_family_label: str = ""
    inferred_reason: str = ""
    exact_field_count: int = 0
    probable_field_count: int = 0
    cloud_only_field_count: int = 0
    current_values_included: bool = False
    total_field_count: int = 0
    mapped_field_count: int = 0
    fields_with_current_value: int = 0
    device_pn: str = ""
    device_sn: str = ""
    device_name: str = ""
    device_alias: str = ""
    device_status: str = ""
    device_brand: str = ""
    device_devcode: int | None = None
    device_devaddr: int | None = None
    detail_sections: tuple[str, ...] = ()
    highlight_settings: tuple[_SmartEssCloudSettingHighlight, ...] = ()


@asynccontextmanager
async def _async_timeout(timeout_seconds: float):
    """Use asyncio.timeout when available, with a Python 3.10-compatible fallback."""

    native_timeout = getattr(asyncio, "timeout", None)
    if native_timeout is not None:
        async with native_timeout(timeout_seconds):
            yield
        return

    task = asyncio.current_task()
    if task is None:
        yield
        return

    loop = asyncio.get_running_loop()
    timed_out = False

    def _cancel_current_task() -> None:
        nonlocal timed_out
        timed_out = True
        task.cancel()

    handle = loop.call_later(timeout_seconds, _cancel_current_task)
    try:
        yield
    except asyncio.CancelledError as exc:
        if timed_out:
            raise TimeoutError from exc
        raise
    finally:
        handle.cancel()


def _translation_candidates(language: str) -> list[str]:
    candidates: list[str] = []
    normalized = (language or "").strip()
    if normalized:
        candidates.append(normalized)
        if "-" in normalized:
            candidates.append(normalized.split("-", 1)[0])
        if "_" in normalized:
            candidates.append(normalized.split("_", 1)[0])
    candidates.append("en")
    return candidates


def _load_translation_bundle_from_dir(directory: Path, language: str) -> dict[str, Any]:
    seen: set[str] = set()
    for candidate in _translation_candidates(language):
        if candidate in seen:
            continue
        seen.add(candidate)
        path = directory / f"{candidate}.json"
        if not path.exists():
            continue
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("Failed to load translation bundle: %s", path)
            break
    return {}


def _merge_translation_bundle(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in extra.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _merge_translation_bundle(existing, value)
        else:
            merged[key] = value
    return merged


@lru_cache(maxsize=16)
def _load_translation_bundle(language: str) -> dict[str, Any]:
    """Load one translation bundle for the requested language."""

    bundle = _load_translation_bundle_from_dir(_TRANSLATIONS_DIR, language)
    flow_bundle = _load_translation_bundle_from_dir(_FLOW_TRANSLATIONS_DIR, language)
    return _merge_translation_bundle(bundle, flow_bundle)


def _translation_lookup(bundle: dict[str, Any], key: str) -> Any:
    """Look up a nested translation key inside one bundle."""

    current: Any = bundle
    for part in key.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _with_translation_bundle(step):
    """Preload one flow translation bundle before rendering localized UI."""

    @wraps(step)
    async def _wrapped(self, *args, **kwargs):
        await self._async_ensure_translation_bundle()
        return await step(self, *args, **kwargs)

    return _wrapped


def _apply_smartess_detection_metadata(
    data: dict[str, Any],
    result: OnboardingResult | None,
) -> None:
    """Persist SmartESS onboarding metadata when the probe captured it."""

    if result is None:
        return

    collector_info = result.collector.collector if result.collector is not None else None
    match_details = result.match.details if result.match is not None else {}

    def _pick(detail_key: str, collector_attr: str) -> Any:
        value = match_details.get(detail_key)
        if value not in (None, ""):
            return value
        if collector_info is None:
            return None
        value = getattr(collector_info, collector_attr, None)
        if value in (None, ""):
            return None
        return value

    mapping = (
        (CONF_SMARTESS_COLLECTOR_VERSION, "smartess_collector_version", "smartess_collector_version"),
        (CONF_SMARTESS_PROTOCOL_ASSET_ID, "smartess_protocol_asset_id", "smartess_protocol_asset_id"),
        (CONF_SMARTESS_PROFILE_KEY, "smartess_profile_key", "smartess_protocol_profile_key"),
        (CONF_SMARTESS_DEVICE_ADDRESS, "smartess_device_address", "smartess_device_address"),
    )
    for config_key, detail_key, collector_attr in mapping:
        value = _pick(detail_key, collector_attr)
        if value is not None:
            data[config_key] = value


def _apply_smartess_cloud_assist_metadata(
    data: dict[str, Any],
    assist_state: _SmartEssCloudAssistState | None,
) -> None:
    """Persist SmartESS cloud-assisted metadata hints for one onboarding entry."""

    if assist_state is None:
        return

    if assist_state.inferred_asset_id:
        data[CONF_SMARTESS_PROTOCOL_ASSET_ID] = assist_state.inferred_asset_id
    if assist_state.inferred_profile_key:
        data[CONF_SMARTESS_PROFILE_KEY] = assist_state.inferred_profile_key
    if (
        assist_state.inferred_driver_key
        and str(data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO) or DRIVER_HINT_AUTO) == DRIVER_HINT_AUTO
    ):
        data[CONF_DRIVER_HINT] = assist_state.inferred_driver_key


def _smartess_cloud_bundle_payload(evidence: dict[str, Any]) -> dict[str, Any]:
    payload = evidence.get("payload") if isinstance(evidence, dict) else None
    return payload if isinstance(payload, dict) else {}


def _smartess_cloud_device_preview(evidence: dict[str, Any]) -> dict[str, Any]:
    identity = evidence.get("device_identity") if isinstance(evidence, dict) else None
    identity = identity if isinstance(identity, dict) else {}
    normalized = _smartess_cloud_bundle_payload(evidence).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    normalized_list = normalized.get("device_list")
    normalized_list = normalized_list if isinstance(normalized_list, dict) else {}
    devices = normalized_list.get("devices")
    devices = devices if isinstance(devices, list) else []

    device_preview: dict[str, Any] = {}
    identity_pn = str(identity.get("pn") or "").strip()
    identity_sn = str(identity.get("sn") or "").strip()
    for item in devices:
        if not isinstance(item, dict):
            continue
        item_pn = str(item.get("pn") or "").strip()
        item_sn = str(item.get("sn") or "").strip()
        if identity_pn and item_pn == identity_pn:
            device_preview = item
            break
        if identity_sn and item_sn == identity_sn:
            device_preview = item
            break
    if not device_preview:
        for item in devices:
            if isinstance(item, dict):
                device_preview = item
                break

    return {
        "pn": identity_pn or str(device_preview.get("pn") or "").strip(),
        "sn": identity_sn or str(device_preview.get("sn") or "").strip(),
        "devcode": identity.get("devcode") if identity.get("devcode") not in (None, "") else device_preview.get("devcode"),
        "devaddr": identity.get("devaddr") if identity.get("devaddr") not in (None, "") else device_preview.get("devaddr"),
        "name": str(device_preview.get("devName") or "").strip(),
        "alias": str(device_preview.get("devalias") or "").strip(),
        "status": str(device_preview.get("status") or "").strip(),
        "brand": str(device_preview.get("brand") or "").strip(),
    }


def _smartess_cloud_detail_sections(evidence: dict[str, Any]) -> tuple[str, ...]:
    summary = evidence.get("summary") if isinstance(evidence, dict) else None
    summary = summary if isinstance(summary, dict) else {}
    normalized = _smartess_cloud_bundle_payload(evidence).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    normalized_detail = normalized.get("device_detail")
    normalized_detail = normalized_detail if isinstance(normalized_detail, dict) else {}
    section_counts = normalized_detail.get("section_counts")
    section_counts = section_counts if isinstance(section_counts, dict) else {}

    previews: list[str] = []
    if section_counts:
        for key in sorted(section_counts):
            previews.append(f"{key} ({section_counts[key]})")
    else:
        detail_sections = summary.get("detail_sections")
        if isinstance(detail_sections, list):
            previews.extend(str(item).strip() for item in detail_sections if str(item).strip())
    return tuple(previews)


def _smartess_cloud_highlight_settings(
    evidence: dict[str, Any],
    *,
    limit: int = 5,
) -> tuple[_SmartEssCloudSettingHighlight, ...]:
    normalized = _smartess_cloud_bundle_payload(evidence).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    normalized_settings = normalized.get("device_settings")
    normalized_settings = normalized_settings if isinstance(normalized_settings, dict) else {}
    fields = normalized_settings.get("fields")
    fields = fields if isinstance(fields, list) else []

    bucket_priority = {"exact_0925": 0, "probable_0925": 1, "cloud_only": 2}

    def _register_for_field(field: dict[str, Any]) -> int | None:
        binding = field.get("binding")
        if isinstance(binding, dict):
            register = binding.get("register")
            if isinstance(register, int):
                return register
        register = field.get("asset_register")
        if isinstance(register, int):
            return register
        return None

    def _choice_label(field: dict[str, Any], value: Any) -> str:
        choices = field.get("choices")
        if not isinstance(choices, list):
            return ""
        value_text = str(value)
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            if choice.get("value") == value:
                return str(choice.get("label") or "").strip()
            if str(choice.get("raw_value") or "") == value_text:
                return str(choice.get("label") or "").strip()
        return ""

    def _current_value_preview(field: dict[str, Any]) -> str:
        if not field.get("has_current_value"):
            return ""
        current_value = field.get("current_value")
        label = _choice_label(field, current_value)
        if label:
            return label
        text = str(current_value).strip()
        if not text:
            return ""
        unit = str(field.get("unit") or "").strip()
        return f"{text} {unit}".strip()

    candidates = [field for field in fields if isinstance(field, dict) and str(field.get("title") or "").strip()]
    candidates.sort(
        key=lambda field: (
            0 if field.get("has_current_value") else 1,
            bucket_priority.get(str(field.get("bucket") or ""), 9),
            0 if _register_for_field(field) is not None else 1,
            str(field.get("title") or "").lower(),
        )
    )

    highlights: list[_SmartEssCloudSettingHighlight] = []
    for field in candidates:
        highlights.append(
            _SmartEssCloudSettingHighlight(
                title=str(field.get("title") or "").strip(),
                bucket=str(field.get("bucket") or "").strip(),
                current_value=_current_value_preview(field),
                register=_register_for_field(field),
            )
        )
        if len(highlights) >= limit:
            break
    return tuple(highlights)


class _TranslationBundleMixin:
    """Shared translation loading helpers for config and options flows."""

    def _flow_language(self) -> str:
        language = str(getattr(self, "context", {}).get("language") or "")
        if not language:
            hass = getattr(self, "hass", None)
            language = str(getattr(getattr(hass, "config", None), "language", "") or "")
        return language or "en"

    async def _async_ensure_translation_bundle(self) -> None:
        language = self._flow_language()
        if getattr(self, "_translation_bundle_language", None) == language:
            cached_bundle = getattr(self, "_translation_bundle", None)
            if isinstance(cached_bundle, dict):
                return

        self._translation_bundle = await self.hass.async_add_executor_job(
            _load_translation_bundle,
            language,
        )
        self._translation_bundle_language = language

    def _tr(
        self,
        key: str,
        default: str,
        placeholders: dict[str, Any] | None = None,
    ) -> str:
        bundle: dict[str, Any] = {}
        if getattr(self, "_translation_bundle_language", None) == self._flow_language():
            cached_bundle = getattr(self, "_translation_bundle", None)
            if isinstance(cached_bundle, dict):
                bundle = cached_bundle
        value = _translation_lookup(bundle, key)
        text = value if isinstance(value, str) and value else default
        if placeholders:
            try:
                return text.format(**placeholders)
            except (KeyError, ValueError):
                try:
                    return default.format(**placeholders)
                except (KeyError, ValueError):
                    return default
        return text

# ---------------------------------------------------------------------------
# Selector helpers
# ---------------------------------------------------------------------------

_PORT_SELECTOR = NumberSelector(
    NumberSelectorConfig(min=1, max=65535, mode=NumberSelectorMode.BOX)
)

_DISCOVERY_INTERVAL_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=1,
        max=60,
        step=1,
        unit_of_measurement="s",
        mode=NumberSelectorMode.SLIDER,
    )
)

_HEARTBEAT_INTERVAL_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=5,
        max=3600,
        step=5,
        unit_of_measurement="s",
        mode=NumberSelectorMode.BOX,
    )
)

_POLL_INTERVAL_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=2,
        max=3600,
        step=1,
        unit_of_measurement="s",
        mode=NumberSelectorMode.BOX,
    )
)

_IP_TEXT_SELECTOR = TextSelector(TextSelectorConfig())
_PASSWORD_TEXT_SELECTOR = TextSelector(TextSelectorConfig(type="password"))

_BOOLEAN_SELECTOR = BooleanSelector()


def _driver_selector() -> SelectSelector:
    options = [
        SelectOptionDict(value=opt, label=opt.replace("_", " ").title())
        for opt in driver_options()
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _control_mode_selector() -> SelectSelector:
    labels = {"auto": "Auto", "read_only": "Read only", "full": "Full control"}
    options = [
        SelectOptionDict(value=opt, label=labels.get(opt, opt))
        for opt in control_mode_options()
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _interface_selector(interface_options: list[dict[str, str]]) -> SelectSelector:
    """Return a selector for known interfaces."""

    options = [
        SelectOptionDict(value=item["ip"], label=item["label"])
        for item in interface_options
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _result_selector(result_options: dict[str, str]) -> SelectSelector:
    """Return a selector for scan results."""

    options = [
        SelectOptionDict(value=key, label=label)
        for key, label in result_options.items()
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _setup_mode_selector(auto_label: str, manual_label: str) -> SelectSelector:
    """Return a selector for choosing auto-scan vs manual setup."""

    options = [
        SelectOptionDict(value=SETUP_MODE_AUTO, label=auto_label),
        SelectOptionDict(value=SETUP_MODE_MANUAL, label=manual_label),
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


# ---------------------------------------------------------------------------
# Section helpers
# ---------------------------------------------------------------------------

def _flatten_sections(user_input: dict[str, Any]) -> dict[str, Any]:
    """Flatten section-nested user input into a flat dict."""

    flat: dict[str, Any] = {}
    for key, value in user_input.items():
        if isinstance(value, dict):
            flat.update(value)
        else:
            flat[key] = value
    for key in _INT_FIELDS:
        value = flat.get(key)
        if isinstance(value, (int, float)):
            flat[key] = int(value)
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.isdigit():
                flat[key] = int(stripped)
    return flat


# ---------------------------------------------------------------------------
# Network utilities
# ---------------------------------------------------------------------------

def _get_local_ip() -> str:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except OSError:
        return ""


def _get_ipv4_interfaces() -> list[dict[str, str]]:
    """Return active global IPv4 interfaces with human-friendly labels."""

    try:
        output = subprocess.check_output(
            ["ip", "-j", "-4", "addr", "show", "up"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        raw = json.loads(output)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        fallback_ip = _get_local_ip()
        if not fallback_ip:
            return []
        return [{"name": "default", "ip": fallback_ip, "label": fallback_ip}]

    interfaces: list[dict[str, str]] = []
    for item in raw:
        ifname = str(item.get("ifname", "")).strip()
        for addr in item.get("addr_info", []):
            ip = str(addr.get("local", "")).strip()
            if not ip:
                continue
            if addr.get("family") != "inet":
                continue
            if addr.get("scope") not in {"global", "site"}:
                continue
            if ip.startswith("127."):
                continue
            label = f"{ifname} — {ip}" if ifname else ip
            interfaces.append({"name": ifname, "ip": ip, "label": label})

    deduped: dict[str, dict[str, str]] = {}
    for interface in interfaces:
        deduped.setdefault(interface["ip"], interface)
    return list(deduped.values())


def _compute_broadcast_24(ip: str) -> str:
    parts = ip.split(".")
    if len(parts) != 4:
        return DEFAULT_DISCOVERY_TARGET
    return f"{parts[0]}.{parts[1]}.{parts[2]}.255"


def _is_ipv4(ip: str) -> bool:
    try:
        socket.inet_aton(ip)
        return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Config flow
# ---------------------------------------------------------------------------

class EybondLocalConfigFlow(_TranslationBundleMixin, ConfigFlow, domain=DOMAIN):
    """Create a config entry for an inverter behind an EyeBond collector."""

    VERSION = 1

    def __init__(self) -> None:
        self._translation_bundle: dict[str, Any] = {}
        self._translation_bundle_language = ""
        self._local_ip = ""
        self._default_broadcast = DEFAULT_DISCOVERY_TARGET
        self._interface_options: list[dict[str, str]] = []
        self._auto_config: dict[str, Any] = {}
        self._manual_defaults: dict[str, Any] = {}
        self._manual_config: dict[str, Any] = {}
        self._manual_result: OnboardingResult | None = None
        self._autodetect_results: dict[str, OnboardingResult] = {}
        self._selected_result: OnboardingResult | None = None
        self._scan_task: asyncio.Task | None = None
        self._scan_error: bool = False
        self._scan_started_monotonic: float | None = None
        self._scan_progress_stage = "preparing"
        self._scan_progress_visible = False
        self._smartess_cloud_assist: _SmartEssCloudAssistState | None = None
        self._smartess_cloud_assist_mode = ""
        self._smartess_cloud_assist_last_error = ""

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return EybondLocalOptionsFlow(config_entry)

    # ---- step: user (welcome) ----

    @_with_translation_bundle
    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()

        if user_input is not None:
            connection_type = str(
                user_input.get(
                    CONF_CONNECTION_TYPE,
                    self._auto_config.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND),
                )
            )
            self._auto_config = {CONF_CONNECTION_TYPE: connection_type}
            if len(self._interface_options) == 1:
                self._auto_config[CONF_SERVER_IP] = self._local_ip
            return await self.async_step_auto()

        data_schema = vol.Schema(
            {
                vol.Required(
                    CONF_CONNECTION_TYPE,
                    default=self._auto_config.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND),
                ): self._connection_type_selector(),
            }
        )
        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            description_placeholders=self._welcome_description_placeholders(),
        )

    # ---- step: auto (choose interface → trigger scan) ----

    @_with_translation_bundle
    async def async_step_auto(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        errors: dict[str, str] = {}

        if self._scan_error:
            errors = {"base": "cannot_autodetect"}
            self._scan_error = False

        if user_input is not None:
            setup_mode = str(user_input.get(CONF_SETUP_MODE, SETUP_MODE_AUTO) or SETUP_MODE_AUTO)
            effective = dict(user_input)
            effective.pop(CONF_SETUP_MODE, None)
            effective.setdefault(CONF_SERVER_IP, self._local_ip)
            input_errors = self._validate_connection_inputs(
                effective,
                fields=self._connection_branch().form_layout.auto_fields,
            )
            if input_errors:
                errors.update(input_errors)
            else:
                self._auto_config.update(effective)
                if setup_mode == SETUP_MODE_MANUAL:
                    self._manual_result = None
                    self._selected_result = None
                    return await self.async_step_manual()
                self._reset_scan_progress()
                return await self.async_step_scanning()

        data_schema = vol.Schema(
            {
                **self._build_connection_fields_schema(
                    self._current_connection_type(),
                    fields=self._connection_branch().form_layout.auto_fields,
                    values=self._auto_connection_defaults(),
                ),
                vol.Required(
                    CONF_SETUP_MODE,
                    default=SETUP_MODE_AUTO,
                ): self._setup_mode_selector(),
            }
        )

        return self.async_show_form(
            step_id="auto",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._auto_description_placeholders(len(self._interface_options) == 1),
        )

    # ---- step: scanning (progress indicator) ----

    @_with_translation_bundle
    async def async_step_scanning(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._scan_task is None:
            self._scan_started_monotonic = time.monotonic()
            self._scan_progress_stage = "preparing"
            self._scan_progress_visible = False
            self.async_update_progress(0.0)
            self._scan_task = self.hass.async_create_task(
                self._async_do_scan()
            )

        selected_ip = self._auto_config.get(CONF_SERVER_IP, self._local_ip)
        selected_label = next(
            (item["label"] for item in self._interface_options if item["ip"] == selected_ip),
            selected_ip or "Unknown",
        )

        if not self._scan_progress_visible:
            self._scan_progress_visible = True
            return self.async_show_progress(
                step_id="scanning",
                progress_action="scanning_network",
                progress_task=self._scan_task,
                description_placeholders=self._scan_progress_placeholders(selected_label),
            )

        if self._scan_task.done():
            self._scan_started_monotonic = None
            self._scan_progress_visible = False
            if self._scan_task.exception():
                self._scan_error = True
            elif not self._autodetect_results:
                self._scan_error = True
            return self.async_show_progress_done(next_step_id="scan_results")

        return self.async_show_progress(
            step_id="scanning",
            progress_action="scanning_network",
            progress_task=self._scan_task,
            description_placeholders=self._scan_progress_placeholders(selected_label),
        )

    async def _async_do_scan(self) -> None:
        """Run auto-detection in the background."""

        effective_input = self._auto_config
        server_ip = str(effective_input.get(CONF_SERVER_IP, self._local_ip) or self._local_ip)
        discovery_target = _compute_broadcast_24(server_ip)
        self._scan_progress_stage = "discovering"
        progress_updater = asyncio.create_task(self._async_update_scan_progress_loop())
        detector = create_onboarding_manager(
            build_connection_spec_from_values(
                self._current_connection_type(),
                dict(self._auto_connection_defaults(), **effective_input),
            ),
            driver_hint=DRIVER_HINT_AUTO,
        )
        try:
            async with _async_timeout(_AUTO_SCAN_TIMEOUT):
                results = await detector.async_auto_detect(
                    discovery_target=discovery_target,
                )
        except TimeoutError:
            logger.warning(
                "Auto-detect scan timed out after %.1fs server_ip=%s discovery_target=%s",
                _AUTO_SCAN_TIMEOUT,
                server_ip,
                discovery_target,
            )
            self._scan_progress_stage = "finalizing"
            self._autodetect_results = {}
            return
        finally:
            progress_updater.cancel()
            with suppress(asyncio.CancelledError):
                await progress_updater
        self._scan_progress_stage = "analyzing"
        self.async_update_progress(0.9)
        await asyncio.sleep(0.08)
        visible_results = self._collapse_scan_results(
            result
            for result in results
            if self._is_visible_scan_result(result)
        )

        if not visible_results:
            self._scan_progress_stage = "finalizing"
            self._autodetect_results = {}
            return

        connected_collectors = [
            result
            for result in visible_results
            if result.collector is not None and result.collector.connected
        ]
        matched = [result for result in visible_results if result.match is not None]

        self._autodetect_results = {
            str(index): result
            for index, result in enumerate(self._sort_scan_results(visible_results))
        }
        self._scan_progress_stage = "finalizing"
        self.async_update_progress(0.99)
        await asyncio.sleep(0.08)
        self._selected_result = None

        if not matched and not connected_collectors:
            best_result = visible_results[0] if visible_results else None
            self._manual_defaults = self._build_manual_defaults(effective_input, best_result)
        self.async_update_progress(1.0)
        await asyncio.sleep(0.12)

    # ---- step: scan_results ----

    @_with_translation_bundle
    async def async_step_scan_results(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        available_results = self._available_autodetect_results()
        menu_options: list[str] = []
        if available_results:
            menu_options.append("choose")
        if len(self._interface_options) > 1:
            menu_options.append("change_scan_interface")
        menu_options.extend(["refresh_scan", "manual"])
        return self.async_show_menu(
            step_id="scan_results",
            menu_options=menu_options,
            description_placeholders=self._scan_results_placeholders(),
        )

    # ---- step: change_scan_interface ----

    @_with_translation_bundle
    async def async_step_change_scan_interface(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        errors: dict[str, str] = {}

        if user_input is not None:
            effective = dict(self._auto_config)
            effective.update(user_input)
            input_errors = self._validate_connection_inputs(
                effective,
                fields=self._connection_branch().form_layout.auto_fields,
            )
            if input_errors:
                errors.update(input_errors)
            else:
                self._auto_config.update(user_input)
                self._reset_scan_progress()
                return await self.async_step_scanning()

        data_schema = vol.Schema(
            self._build_connection_fields_schema(
                self._current_connection_type(),
                fields=self._connection_branch().form_layout.auto_fields,
                values=self._auto_connection_defaults(),
            )
        )
        return self.async_show_form(
            step_id="change_scan_interface",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._auto_description_placeholders(False),
        )

    # ---- step: refresh_scan ----

    async def async_step_refresh_scan(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        if not self._auto_config:
            self._auto_config = self._auto_connection_defaults()
        self._reset_scan_progress()
        return await self.async_step_scanning()

    # ---- step: choose ----

    @_with_translation_bundle
    async def async_step_choose(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if not self._autodetect_results:
            return await self.async_step_auto()

        available_results = self._available_autodetect_results()
        if not available_results:
            return await self.async_step_scan_results()
        if user_input is None and len(available_results) == 1:
            self._selected_result = next(iter(available_results.values()))
            return await self.async_step_confirm()

        errors: dict[str, str] = {}
        if user_input is not None:
            selected_key = user_input[CONF_RESULT_KEY]
            result = available_results.get(selected_key)
            if result is None:
                errors["base"] = "invalid_selection"
            elif self._existing_entry_for_result(result) is not None:
                errors["base"] = "already_added_candidate"
            else:
                self._selected_result = result
                return await self.async_step_confirm()

        options = {
            key: self._result_label(result)
            for key, result in available_results.items()
        }
        data_schema = vol.Schema(
            {
                vol.Required(CONF_RESULT_KEY): _result_selector(options),
            }
        )
        return self.async_show_form(
            step_id="choose",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._choose_placeholders(),
        )

    # ---- step: confirm ----

    @_with_translation_bundle
    async def async_step_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if user_input is None and self._selected_result is not None and self._can_offer_smartess_cloud_assist(self._selected_result):
            return await self.async_step_smartess_cloud_assist_choice()
        return await self._async_show_confirm_form(step_id="confirm", user_input=user_input)

    @_with_translation_bundle
    async def async_step_confirm_without_cloud_assist(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        return await self._async_show_confirm_form(
            step_id="confirm_without_cloud_assist",
            user_input=user_input,
        )

    @_with_translation_bundle
    async def async_step_smartess_cloud_assist_choice(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if self._selected_result is None:
            return await self.async_step_auto()
        self._smartess_cloud_assist_mode = "auto"
        if not self._can_offer_smartess_cloud_assist(self._selected_result):
            return await self.async_step_confirm()
        return self.async_show_menu(
            step_id="smartess_cloud_assist_choice",
            menu_options=["smartess_cloud_assist", "confirm_without_cloud_assist"],
            description_placeholders=self._smartess_cloud_assist_placeholders(
                self._selected_result,
            ),
        )

    @_with_translation_bundle
    async def async_step_smartess_cloud_assist(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        result = self._smartess_cloud_assist_context_result()
        if result is None:
            if self._smartess_cloud_assist_mode == "manual":
                return await self.async_step_manual_confirm()
            return await self.async_step_auto()

        if user_input is not None:
            try:
                self._smartess_cloud_assist = await self._async_run_smartess_cloud_assist(
                    result,
                    username=str(user_input.get("username") or "").strip(),
                    password=str(user_input.get("password") or ""),
                )
                self._smartess_cloud_assist_last_error = ""
            except Exception as exc:
                self._smartess_cloud_assist_last_error = str(exc)
            else:
                return await self.async_step_smartess_cloud_assist_summary()

        return self.async_show_form(
            step_id="smartess_cloud_assist",
            data_schema=vol.Schema(
                {
                    vol.Required("username", default=""): _IP_TEXT_SELECTOR,
                    vol.Required("password", default=""): _PASSWORD_TEXT_SELECTOR,
                }
            ),
            description_placeholders=self._smartess_cloud_assist_placeholders(result),
        )

    @_with_translation_bundle
    async def async_step_smartess_cloud_assist_summary(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        result = self._smartess_cloud_assist_context_result()
        if result is None:
            if self._smartess_cloud_assist_mode == "manual":
                return await self.async_step_manual_confirm()
            return await self.async_step_confirm()

        if self._smartess_cloud_assist_state_for_result(result) is None:
            if self._smartess_cloud_assist_mode == "manual":
                return await self.async_step_manual_confirm()
            return await self.async_step_confirm()

        menu_options = ["manual_confirm"] if self._smartess_cloud_assist_mode == "manual" else ["confirm"]
        return self.async_show_menu(
            step_id="smartess_cloud_assist_summary",
            menu_options=menu_options,
            description_placeholders=self._smartess_cloud_assist_summary_placeholders(result),
        )

    async def _async_show_confirm_form(
        self,
        *,
        step_id: str,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._selected_result is None:
            return await self.async_step_auto()

        if user_input is not None:
            return await self._async_create_entry_from_result(user_input)

        description_placeholders = self._result_placeholders(self._selected_result)
        return self.async_show_form(
            step_id=step_id,
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_POLL_INTERVAL, default=DEFAULT_POLL_INTERVAL): _POLL_INTERVAL_SELECTOR,
                }
            ),
            description_placeholders=description_placeholders,
        )

    # ---- step: manual ----

    @_with_translation_bundle
    async def async_step_manual(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        errors: dict[str, str] = {}

        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            errors = self._validate_connection_inputs(
                flat_input,
                fields=self._connection_branch().form_layout.manual_fields
                + self._connection_branch().form_layout.manual_advanced_fields,
            )
            if not errors:
                self._manual_config = dict(flat_input)
                self._manual_result = await self._async_probe_manual_target(flat_input)
                if self._manual_result.match is not None and self._manual_result.confidence == "high":
                    return await self._async_create_manual_entry(flat_input, self._manual_result)
                return await self.async_step_manual_confirm()

        defaults = self._build_manual_defaults(user_input, self._selected_result)
        data_schema = vol.Schema(
            {
                **self._build_connection_fields_schema(
                    self._current_connection_type(),
                    fields=self._connection_branch().form_layout.manual_fields,
                    values=defaults,
                ),
                vol.Required("advanced_connection"): section(
                    vol.Schema(
                        self._build_connection_fields_schema(
                            self._current_connection_type(),
                            fields=self._connection_branch().form_layout.manual_advanced_fields,
                            values=defaults,
                        )
                    ),
                    {"collapsed": True},
                ),
            }
        )

        return self.async_show_form(
            step_id="manual",
            data_schema=data_schema,
            errors=errors,
        )

    # ---- step: manual_confirm ----

    @_with_translation_bundle
    async def async_step_manual_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()

        menu_options = [
            MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
            MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
        ]
        if self._can_offer_smartess_cloud_assist(self._manual_result):
            menu_options.append("manual_smartess_cloud_assist")
        menu_options.append(MANUAL_CONFIRM_ACTION_CREATE_PENDING)

        return self.async_show_menu(
            step_id="manual_confirm",
            menu_options=menu_options,
            description_placeholders=self._manual_confirm_placeholders(
                self._manual_config,
                self._manual_result,
            ),
        )

    @_with_translation_bundle
    async def async_step_manual_smartess_cloud_assist(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()
        self._smartess_cloud_assist_mode = "manual"
        return await self.async_step_smartess_cloud_assist()

    async def async_step_manual_probe_again(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()

        self._manual_result = await self._async_probe_manual_target(self._manual_config)
        if self._manual_result.match is not None and self._manual_result.confidence == "high":
            return await self._async_create_manual_entry(self._manual_config, self._manual_result)
        return await self.async_step_manual_confirm()

    async def async_step_manual_edit_settings(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()

        self._manual_defaults = dict(self._manual_config)
        self._manual_result = None
        return await self.async_step_manual()

    async def async_step_manual_create_pending(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()
        return await self._async_create_manual_entry(self._manual_config, self._manual_result)

    # ---- entry creation ----

    async def _async_create_entry_from_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._selected_result is None:
            raise RuntimeError("no_selected_result")

        result = self._selected_result
        existing_entry = self._existing_entry_for_result(result)
        if existing_entry is not None:
            return self.async_abort(reason="already_configured")
        collector_ip = result.collector.ip if result.collector is not None else ""
        collector_info = result.collector.collector if result.collector is not None else None
        collector_pn = collector_info.collector_pn if collector_info is not None else ""
        driver_hint = (
            result.match.driver_key
            if result.match is not None
            else self._auto_config.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO)
        )
        assist_state = self._smartess_cloud_assist_state_for_result(result)
        if result.match is None and driver_hint == DRIVER_HINT_AUTO and assist_state is not None and assist_state.inferred_driver_key:
            driver_hint = assist_state.inferred_driver_key

        unique_id = self._result_unique_id(result)
        await self.async_set_unique_id(unique_id)
        self._abort_if_unique_id_configured()

        title = (
            f"{result.match.model_name} ({result.match.serial_number})"
            if result.match is not None and result.match.serial_number
            else result.match.model_name
            if result.match is not None
            else f"{self._connection_display().integration_name} ({collector_ip or self._auto_config[CONF_SERVER_IP]})"
        )

        connection_type = result.connection_type or self._current_connection_type()
        connection_settings = with_driver_hint(
            build_detected_entry_settings(
                connection_type,
                server_ip=self._auto_config[CONF_SERVER_IP],
                collector_ip=collector_ip or self._auto_config.get(CONF_COLLECTOR_IP, ""),
                default_broadcast=_compute_broadcast_24(self._auto_config[CONF_SERVER_IP]),
                overrides=self._auto_config,
            ),
            driver_hint=driver_hint,
        )
        data = {
            CONF_CONNECTION_TYPE: connection_type,
            **connection_settings,
            CONF_CONNECTION_MODE: "known_ip" if collector_ip else result.connection_mode,
            CONF_CONTROL_MODE: DEFAULT_CONTROL_MODE,
            CONF_COLLECTOR_PN: collector_pn,
            CONF_DETECTION_CONFIDENCE: result.confidence,
            CONF_DETECTED_MODEL: result.match.model_name if result.match is not None else "",
            CONF_DETECTED_SERIAL: result.match.serial_number if result.match is not None else "",
        }
        _apply_smartess_detection_metadata(data, result)
        _apply_smartess_cloud_assist_metadata(data, assist_state)
        poll_interval = int((user_input or {}).get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL))
        return self.async_create_entry(
            title=title,
            data=data,
            options={CONF_POLL_INTERVAL: poll_interval},
        )

    async def _async_create_manual_entry(
        self,
        user_input: dict[str, Any],
        result: OnboardingResult | None = None,
    ) -> ConfigFlowResult:
        result = result or self._manual_result
        if result is not None:
            existing_entry = self._existing_entry_for_result(result)
            if existing_entry is not None:
                return self.async_abort(reason="already_configured")
        collector_ip = user_input.get(CONF_COLLECTOR_IP, "")
        collector_pn = ""
        detected_model = ""
        detected_serial = ""
        driver_hint = user_input.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO)
        connection_mode = "manual"

        if result is not None:
            connection_mode = result.connection_mode or connection_mode
            if result.collector is not None:
                collector_ip = result.collector.ip or collector_ip
                collector_info = result.collector.collector
                if collector_info is not None and collector_info.collector_pn:
                    collector_pn = collector_info.collector_pn
            if result.match is not None:
                detected_model = result.match.model_name
                detected_serial = result.match.serial_number
                driver_hint = result.match.driver_key or driver_hint

        assist_state = self._smartess_cloud_assist_state_for_result(result)
        if result is not None and result.match is None and driver_hint == DRIVER_HINT_AUTO and assist_state is not None and assist_state.inferred_driver_key:
            driver_hint = assist_state.inferred_driver_key

        unique_id = (
            f"collector:{collector_pn}"
            if collector_pn
            else f"inverter:{detected_serial}"
            if detected_serial
            else f"manual:{collector_ip}"
            if collector_ip
            else f"listener:{user_input[CONF_SERVER_IP]}:{user_input[CONF_TCP_PORT]}"
        )
        await self.async_set_unique_id(unique_id)
        self._abort_if_unique_id_configured()

        if detected_model:
            title = (
                f"{detected_model} ({detected_serial})"
                if detected_serial
                else detected_model
            )
        else:
            title = self._connection_display().pending_entry_title

        connection_type = result.connection_type if result is not None else self._current_connection_type()
        data = with_driver_hint(
            build_manual_entry_settings(connection_type, user_input),
            driver_hint=driver_hint,
        )
        data.setdefault(CONF_CONNECTION_TYPE, connection_type)
        data.setdefault(CONF_CONTROL_MODE, CONTROL_MODE_READ_ONLY)
        data[CONF_COLLECTOR_IP] = collector_ip
        data[CONF_DETECTION_CONFIDENCE] = result.confidence if result is not None else "none"
        data[CONF_CONNECTION_MODE] = connection_mode
        data[CONF_COLLECTOR_PN] = collector_pn
        data[CONF_DETECTED_MODEL] = detected_model
        data[CONF_DETECTED_SERIAL] = detected_serial
        _apply_smartess_detection_metadata(data, result)
        _apply_smartess_cloud_assist_metadata(data, assist_state)
        return self.async_create_entry(title=title, data=data)

    # ---- probe ----

    async def _async_probe_manual_target(
        self,
        user_input: dict[str, Any],
    ) -> OnboardingResult:
        """Run one-shot detection using the manual settings before creating an entry."""

        detector = create_onboarding_manager(
            build_connection_spec_from_values(
                self._current_connection_type(),
                build_manual_entry_settings(self._current_connection_type(), user_input),
            ),
            driver_hint=user_input[CONF_DRIVER_HINT],
        )
        collector_ip = user_input.get(CONF_COLLECTOR_IP, "")
        discovery_target = user_input.get(CONF_DISCOVERY_TARGET, "")
        try:
            async with _async_timeout(_MANUAL_PROBE_TIMEOUT):
                results = await detector.async_auto_detect(
                    collector_ip=collector_ip,
                    discovery_target=discovery_target,
                    attempts=1,
                    connect_timeout=3.5,
                    heartbeat_timeout=1.5,
                )
        except TimeoutError:
            logger.warning(
                "Manual onboarding probe timed out after %.1fs server_ip=%s collector_ip=%s discovery_target=%s driver_hint=%s",
                _MANUAL_PROBE_TIMEOUT,
                user_input.get(CONF_SERVER_IP, ""),
                collector_ip or "-",
                discovery_target or "-",
                user_input.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
            )
            return OnboardingResult(
                connection_type=self._current_connection_type(),
                connection_mode="manual",
                next_action="create_pending_entry",
                last_error="manual_probe_timeout",
            )
        if results:
            return results[0]

        return OnboardingResult(
            connection_type=self._current_connection_type(),
            connection_mode="manual",
            next_action="create_pending_entry",
            last_error="manual_target_not_confirmed",
        )

    # ---- network defaults ----

    async def _async_ensure_network_defaults(self) -> None:
        if self._local_ip:
            return
        self._interface_options = await self.hass.async_add_executor_job(_get_ipv4_interfaces)
        detected_local_ip = await self.hass.async_add_executor_job(_get_local_ip)
        if self._interface_options:
            preferred = next(
                (
                    item["ip"]
                    for item in self._interface_options
                    if item["ip"] == detected_local_ip
                ),
                self._interface_options[0]["ip"],
            )
            self._local_ip = preferred
        else:
            self._local_ip = detected_local_ip
        self._default_broadcast = (
            _compute_broadcast_24(self._local_ip)
            if self._local_ip
            else DEFAULT_DISCOVERY_TARGET
        )

    def _build_manual_defaults(
        self,
        user_input: dict[str, Any] | None,
        result: OnboardingResult | None,
    ) -> dict[str, Any]:
        collector_ip = ""
        driver_hint = DRIVER_HINT_AUTO
        if result is not None and result.collector is not None:
            collector_ip = result.collector.ip
        if result is not None and result.match is not None:
            driver_hint = result.match.driver_key
        defaults = self._connection_branch().build_manual_base_values(
            server_ip=self._local_ip,
            default_broadcast=self._default_broadcast,
            stored_defaults=self._manual_defaults,
            collector_ip=collector_ip,
            driver_hint=driver_hint,
        )
        if self._auto_config:
            defaults[CONF_SERVER_IP] = self._auto_config.get(CONF_SERVER_IP, defaults[CONF_SERVER_IP])
        if user_input is not None:
            flat = _flatten_sections(user_input)
            defaults.update(flat)
        self._manual_defaults = defaults
        return defaults

    # ---- selector helpers ----

    def _server_ip_field(self) -> SelectSelector | TextSelector:
        """Return the most user-friendly selector for choosing the local server IP."""

        if not self._interface_options:
            return _IP_TEXT_SELECTOR
        return _interface_selector(self._interface_options)

    def _connection_type_selector(self) -> SelectSelector:
        """Return a selector for supported connection branches."""

        options = [
            SelectOptionDict(
                value=connection_type,
                label=get_connection_branch(connection_type).display.integration_name,
            )
            for connection_type in supported_connection_types()
        ]
        return SelectSelector(
            SelectSelectorConfig(
                options=options,
                mode=SelectSelectorMode.DROPDOWN,
            )
        )

    def _setup_mode_selector(self) -> SelectSelector:
        """Return a selector for starting with auto-scan or manual setup."""

        return _setup_mode_selector(
            self._tr(
                "common.dynamic.setup_mode_auto",
                "Start auto-scan",
            ),
            self._tr(
                "common.dynamic.setup_mode_manual",
                "Skip to manual setup",
            ),
        )

    def _reset_scan_progress(self) -> None:
        """Reset scan-progress bookkeeping before one new scan attempt starts."""

        self._scan_task = None
        self._scan_started_monotonic = None
        self._scan_progress_stage = "preparing"
        self._scan_progress_visible = False
        self._smartess_cloud_assist_mode = ""
        self._smartess_cloud_assist_last_error = ""

    def _collector_pn_for_result(self, result: OnboardingResult | None) -> str:
        if result is None or result.collector is None or result.collector.collector is None:
            return ""
        return str(result.collector.collector.collector_pn or "").strip()

    def _smartess_detected_hint_values(self, result: OnboardingResult | None) -> tuple[str, str]:
        if result is None:
            return "", ""

        collector_info = result.collector.collector if result.collector is not None else None
        match_details = result.match.details if result.match is not None else {}
        asset_id = str(
            match_details.get("smartess_protocol_asset_id")
            or getattr(collector_info, "smartess_protocol_asset_id", "")
            or ""
        ).strip()
        profile_key = str(
            match_details.get("smartess_profile_key")
            or getattr(collector_info, "smartess_protocol_profile_key", "")
            or ""
        ).strip()
        return asset_id, profile_key

    def _smartess_cloud_assist_context_result(self) -> OnboardingResult | None:
        if self._smartess_cloud_assist_mode == "manual":
            return self._manual_result
        return self._selected_result

    def _smartess_cloud_assist_state_for_result(
        self,
        result: OnboardingResult | None,
    ) -> _SmartEssCloudAssistState | None:
        collector_pn = self._collector_pn_for_result(result)
        if not collector_pn or self._smartess_cloud_assist is None:
            return None
        if self._smartess_cloud_assist.collector_pn != collector_pn:
            return None
        return self._smartess_cloud_assist

    def _can_offer_smartess_cloud_assist(self, result: OnboardingResult | None) -> bool:
        if result is None or result.collector is None:
            return False
        if not result.collector.connected:
            return False
        if result.confidence == "high":
            return False
        if not self._collector_pn_for_result(result):
            return False
        return self._smartess_cloud_assist_state_for_result(result) is None

    def _smartess_cloud_summary(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""

        placeholders = {
            "family_label": state.inferred_family_label,
            "driver_key": state.inferred_driver_key or DRIVER_HINT_AUTO,
            "exact_count": state.exact_field_count,
            "probable_count": state.probable_field_count,
            "cloud_only_count": state.cloud_only_field_count,
        }
        if state.inferred_family_label:
            return self._tr(
                "common.dynamic.smartess_cloud_summary_known_family",
                "**SmartESS cloud:** suggests **{family_label}** and pre-fills local metadata hints for `{driver_key}`. Settings surface: exact {exact_count}, probable {probable_count}, cloud-only {cloud_only_count}. Local controls stay disabled until a high-confidence local detection is confirmed.",
                placeholders,
            )
        return self._tr(
            "common.dynamic.smartess_cloud_summary_generic",
            "**SmartESS cloud:** evidence was saved for this collector, but no safe local family mapping was resolved yet. Settings surface: exact {exact_count}, probable {probable_count}, cloud-only {cloud_only_count}.",
            placeholders,
        )

    def _smartess_cloud_offer_summary(self, result: OnboardingResult | None) -> str:
        collector_pn = self._collector_pn_for_result(result)
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is not None:
            return self._smartess_cloud_summary(result)
        return self._tr(
            "common.dynamic.smartess_cloud_offer_summary",
            "Local detection is not yet high-confidence for collector `{collector_pn}`. SmartESS cloud assist can fetch extra identity and settings evidence before the entry is created.",
            {"collector_pn": collector_pn or self._tr("common.dynamic.not_available", "Not available")},
        )

    def _smartess_cloud_identity_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""

        not_available = self._tr("common.dynamic.not_available", "Not available")
        lines = [
            self._tr("common.dynamic.smartess_cloud_identity_heading", "**Cloud identity**"),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_table_label', 'Detail')} | {self._tr('common.dynamic.smartess_cloud_table_value', 'Value')} |",
            "|---|---|",
            f"| {self._tr('common.dynamic.smartess_cloud_collector_pn_label', 'Collector PN')} | {self._collector_pn_for_result(result) or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_device_pn_label', 'Device PN')} | {state.device_pn or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_device_sn_label', 'Device SN')} | {state.device_sn or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_device_name_label', 'Device')} | {state.device_name or not_available} |",
        ]
        if state.device_alias:
            lines.append(
                f"| {self._tr('common.dynamic.smartess_cloud_device_alias_label', 'Alias')} | {state.device_alias} |"
            )
        if state.device_status:
            lines.append(
                f"| {self._tr('common.dynamic.smartess_cloud_device_status_label', 'Status')} | {state.device_status} |"
            )
        if state.device_brand:
            lines.append(
                f"| {self._tr('common.dynamic.smartess_cloud_device_brand_label', 'Brand')} | {state.device_brand} |"
            )
        address_value = self._smartess_cloud_device_address_preview(state) or not_available
        lines.append(
            f"| {self._tr('common.dynamic.smartess_cloud_device_address_label', 'Cloud address')} | {address_value} |"
        )
        return "\n".join(lines)

    def _smartess_cloud_mapping_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""

        not_available = self._tr("common.dynamic.not_available", "Not available")
        reason = state.inferred_reason or self._tr(
            "common.dynamic.smartess_cloud_mapping_reason_missing",
            "No safe local family mapping was resolved yet. The evidence is still saved for later diagnostics and support work.",
        )
        lines = [
            self._tr("common.dynamic.smartess_cloud_mapping_heading", "**Local interpretation**"),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_table_label', 'Detail')} | {self._tr('common.dynamic.smartess_cloud_table_value', 'Value')} |",
            "|---|---|",
            f"| {self._tr('common.dynamic.smartess_cloud_family_label', 'Suggested family')} | {state.inferred_family_label or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_driver_label', 'Local driver hint')} | {state.inferred_driver_key or DRIVER_HINT_AUTO} |",
            f"| {self._tr('common.dynamic.smartess_cloud_mapping_reason_label', 'Reason')} | {reason} |",
        ]
        return "\n".join(lines)

    def _smartess_cloud_detail_summary(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""
        if state.detail_sections:
            return self._tr(
                "common.dynamic.smartess_cloud_detail_sections_found",
                "**Cloud detail sections:** {sections}",
                {"sections": ", ".join(state.detail_sections)},
            )
        return self._tr(
            "common.dynamic.smartess_cloud_detail_sections_missing",
            "**Cloud detail sections:** no normalized section breakdown was captured.",
        )

    def _smartess_cloud_settings_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""
        lines = [
            self._tr("common.dynamic.smartess_cloud_settings_heading", "**Settings digest**"),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_table_label', 'Detail')} | {self._tr('common.dynamic.smartess_cloud_table_value', 'Value')} |",
            "|---|---|",
            f"| {self._tr('common.dynamic.smartess_cloud_total_fields_label', 'Total fields')} | {state.total_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_mapped_fields_label', 'Mapped local fields')} | {state.mapped_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_current_values_label', 'Fields with current value')} | {state.fields_with_current_value} |",
            f"| {self._tr('common.dynamic.smartess_cloud_exact_fields_label', 'Exact local matches')} | {state.exact_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_probable_fields_label', 'Probable local matches')} | {state.probable_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_cloud_only_fields_label', 'Cloud-only fields')} | {state.cloud_only_field_count} |",
        ]
        return "\n".join(lines)

    def _smartess_cloud_highlights_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""
        if not state.highlight_settings:
            return self._tr(
                "common.dynamic.smartess_cloud_highlights_empty",
                "**Highlighted SmartESS fields:** no compact field preview was captured.",
            )

        def _escape_cell(value: str) -> str:
            return str(value).replace("|", "\\|").replace("\n", " ")

        lines = [
            self._tr("common.dynamic.smartess_cloud_highlights_heading", "**Highlighted SmartESS fields**"),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_highlight_field_label', 'Field')} | {self._tr('common.dynamic.smartess_cloud_highlight_value_label', 'Value')} | {self._tr('common.dynamic.smartess_cloud_highlight_local_use_label', 'Local use')} |",
            "|---|---|---|",
        ]
        not_available = self._tr("common.dynamic.not_available", "Not available")
        for highlight in state.highlight_settings:
            lines.append(
                f"| {_escape_cell(highlight.title)} | {_escape_cell(highlight.current_value or not_available)} | {_escape_cell(self._smartess_cloud_local_use_preview(highlight))} |"
            )
        return "\n".join(lines)

    def _smartess_cloud_device_address_preview(
        self,
        state: _SmartEssCloudAssistState,
    ) -> str:
        if state.device_devcode in (None, "") and state.device_devaddr in (None, ""):
            return ""

        devcode = ""
        if isinstance(state.device_devcode, int):
            devcode = self._tr(
                "common.dynamic.smartess_cloud_device_devcode_value",
                "devcode {devcode} (0x{devcode_hex})",
                {"devcode": state.device_devcode, "devcode_hex": f"{state.device_devcode:04X}"},
            )
        devaddr = ""
        if isinstance(state.device_devaddr, int):
            devaddr = self._tr(
                "common.dynamic.smartess_cloud_device_devaddr_value",
                "devaddr {devaddr}",
                {"devaddr": state.device_devaddr},
            )
        return ", ".join(part for part in (devcode, devaddr) if part)

    def _smartess_cloud_bucket_label(self, bucket: str) -> str:
        if bucket == "exact_0925":
            return self._tr("common.dynamic.smartess_cloud_bucket_exact", "Exact local match")
        if bucket == "probable_0925":
            return self._tr("common.dynamic.smartess_cloud_bucket_probable", "Probable local match")
        if bucket == "cloud_only":
            return self._tr("common.dynamic.smartess_cloud_bucket_cloud_only", "Cloud-only")
        return self._tr("common.dynamic.unknown", "Unknown")

    def _smartess_cloud_local_use_preview(
        self,
        highlight: _SmartEssCloudSettingHighlight,
    ) -> str:
        bucket_label = self._smartess_cloud_bucket_label(highlight.bucket)
        if highlight.register is None:
            return bucket_label
        return self._tr(
            "common.dynamic.smartess_cloud_local_use_register",
            "{bucket_label}, reg {register}",
            {"bucket_label": bucket_label, "register": highlight.register},
        )

    def _smartess_cloud_status_line(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is not None and state.evidence_path:
            return self._tr(
                "common.dynamic.smartess_cloud_status_saved",
                "Last SmartESS cloud evidence: {path}",
                {"path": state.evidence_path},
            )
        if self._smartess_cloud_assist_last_error:
            return self._tr(
                "common.dynamic.smartess_cloud_status_failed",
                "Last SmartESS cloud assist attempt failed: {error}",
                {"error": self._smartess_cloud_assist_last_error},
            )
        return ""

    def _smartess_cloud_assist_placeholders(
        self,
        result: OnboardingResult | None,
    ) -> dict[str, str]:
        state = self._smartess_cloud_assist_state_for_result(result)
        return {
            "collector_pn": self._collector_pn_for_result(result)
            or self._tr("common.dynamic.not_available", "Not available"),
            "cloud_evidence_path": (
                state.evidence_path
                if state is not None and state.evidence_path
                else self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "smartess_cloud_offer_summary": self._smartess_cloud_offer_summary(result),
            "smartess_cloud_status_line": self._smartess_cloud_status_line(result),
        }

    def _smartess_cloud_assist_summary_placeholders(
        self,
        result: OnboardingResult | None,
    ) -> dict[str, str]:
        placeholders = self._smartess_cloud_assist_placeholders(result)
        placeholders.update(
            {
                "smartess_cloud_identity_table": self._smartess_cloud_identity_table(result),
                "smartess_cloud_mapping_table": self._smartess_cloud_mapping_table(result),
                "smartess_cloud_detail_summary": self._smartess_cloud_detail_summary(result),
                "smartess_cloud_settings_table": self._smartess_cloud_settings_table(result),
                "smartess_cloud_highlights_table": self._smartess_cloud_highlights_table(result),
            }
        )
        return placeholders

    def _config_dir_path(self) -> Path:
        config_dir = str(getattr(getattr(self.hass, "config", None), "config_dir", "") or "").strip()
        if not config_dir:
            raise RuntimeError("config_dir_not_available")
        return Path(config_dir)

    async def _async_run_smartess_cloud_assist(
        self,
        result: OnboardingResult,
        *,
        username: str,
        password: str,
    ) -> _SmartEssCloudAssistState:
        collector_pn = self._collector_pn_for_result(result)
        if not collector_pn:
            raise RuntimeError("smartess_collector_pn_not_available")

        record = await self.hass.async_add_executor_job(
            lambda: fetch_and_export_smartess_device_bundle_cloud_evidence(
                config_dir=self._config_dir_path(),
                username=username,
                password=password,
                collector_pn=collector_pn,
                source="smartess_cloud_onboarding",
            )
        )
        asset_id, profile_key = self._smartess_detected_hint_values(result)
        plan = resolve_smartess_known_family_draft_plan(
            smartess_protocol_asset_id=asset_id,
            smartess_profile_key=profile_key,
            cloud_evidence=record.payload,
        )
        inferred_driver_key = ""
        if plan is not None and plan.source_profile_name:
            try:
                inferred_driver_key = str(
                    load_driver_profile(plan.source_profile_name).driver_key or ""
                ).strip()
            except Exception:
                inferred_driver_key = ""

        summary = dict(record.payload.get("summary") or {})
        device_preview = _smartess_cloud_device_preview(record.payload)
        normalized_settings = _smartess_cloud_bundle_payload(record.payload).get("normalized")
        normalized_settings = normalized_settings if isinstance(normalized_settings, dict) else {}
        normalized_settings = normalized_settings.get("device_settings") if isinstance(normalized_settings, dict) else {}
        normalized_settings = normalized_settings if isinstance(normalized_settings, dict) else {}
        return _SmartEssCloudAssistState(
            collector_pn=collector_pn,
            evidence_path=str(record.path),
            inferred_asset_id=plan.asset_id if plan is not None else "",
            inferred_profile_key=plan.profile_key if plan is not None else "",
            inferred_driver_key=inferred_driver_key,
            inferred_family_label=plan.driver_label if plan is not None else "",
            inferred_reason=plan.reason if plan is not None else "",
            exact_field_count=int(summary.get("settings_exact_0925_field_count") or 0),
            probable_field_count=int(summary.get("settings_probable_0925_field_count") or 0),
            cloud_only_field_count=int(summary.get("settings_cloud_only_field_count") or 0),
            current_values_included=bool(summary.get("settings_current_values_included", False)),
            total_field_count=int(normalized_settings.get("field_count") or summary.get("settings_field_count") or 0),
            mapped_field_count=int(normalized_settings.get("mapped_field_count") or summary.get("settings_mapped_field_count") or 0),
            fields_with_current_value=int(normalized_settings.get("fields_with_current_value") or 0),
            device_pn=str(device_preview.get("pn") or "").strip(),
            device_sn=str(device_preview.get("sn") or "").strip(),
            device_name=str(device_preview.get("name") or "").strip(),
            device_alias=str(device_preview.get("alias") or "").strip(),
            device_status=str(device_preview.get("status") or "").strip(),
            device_brand=str(device_preview.get("brand") or "").strip(),
            device_devcode=device_preview.get("devcode") if device_preview.get("devcode") not in ("", None) else None,
            device_devaddr=device_preview.get("devaddr") if device_preview.get("devaddr") not in ("", None) else None,
            detail_sections=_smartess_cloud_detail_sections(record.payload),
            highlight_settings=_smartess_cloud_highlight_settings(record.payload),
        )

    def _current_connection_type(self) -> str:
        """Return the active connection type for the current setup branch."""

        if self._selected_result is not None and self._selected_result.connection_type:
            return self._selected_result.connection_type
        if self._manual_result is not None and self._manual_result.connection_type:
            return self._manual_result.connection_type
        return str(self._auto_config.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND) or CONNECTION_TYPE_EYBOND)

    def _connection_branch(self):
        """Return branch metadata for the active connection type."""

        return get_connection_branch(self._current_connection_type())

    def _connection_display(self):
        """Return branch-aware display metadata for the active connection type."""

        return self._connection_branch().display

    def _auto_connection_defaults(self) -> dict[str, Any]:
        """Return branch-aware defaults for the auto-scan flow."""

        server_ip = str(self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip)
        defaults = self._connection_branch().build_auto_values(
            server_ip=server_ip,
            default_broadcast=_compute_broadcast_24(server_ip) if server_ip else self._default_broadcast,
        )
        defaults.update(self._auto_config)
        return defaults

    def _scan_action_label(self, action: str, default: str) -> str:
        return self._tr(
            f"config.step.scan_results.menu_options.{action}",
            default,
        )

    def _manual_confirm_action_label(self, action: str, default: str) -> str:
        return self._tr(
            f"config.step.manual_confirm.menu_options.{action}",
            default,
        )

    async def _async_update_scan_progress_loop(self) -> None:
        """Periodically publish determinate progress updates while one scan runs."""

        await asyncio.sleep(0.35)
        while True:
            started = self._scan_started_monotonic
            now = time.monotonic()
            elapsed_seconds = max(0.0, now - started) if started is not None else 0.0
            self.async_update_progress(self._scan_progress_fraction(elapsed_seconds))
            await asyncio.sleep(0.35)

    def _scan_progress_fraction(self, elapsed_seconds: float) -> float:
        bounded_elapsed = min(max(elapsed_seconds, 0.0), _AUTO_SCAN_TIMEOUT)
        time_fraction = bounded_elapsed / _AUTO_SCAN_TIMEOUT if _AUTO_SCAN_TIMEOUT > 0 else 0.0
        if self._scan_progress_stage == "preparing":
            return 0.0
        if self._scan_progress_stage == "discovering":
            return min(0.82, 0.02 + (time_fraction * 0.8))
        if self._scan_progress_stage == "analyzing":
            return 0.9
        if self._scan_progress_stage == "finalizing":
            return 0.97
        return min(0.82, 0.02 + (time_fraction * 0.8))

    def _scan_progress_placeholders(self, selected_label: str) -> dict[str, str]:
        now = time.monotonic()
        started = self._scan_started_monotonic if self._scan_started_monotonic is not None else now
        elapsed_seconds_float = max(0.0, now - started)
        bounded_elapsed = min(elapsed_seconds_float, _AUTO_SCAN_TIMEOUT)
        elapsed_seconds = int(round(bounded_elapsed))
        remaining_seconds = max(0, int(round(_AUTO_SCAN_TIMEOUT - bounded_elapsed)))
        progress_fraction = self._scan_progress_fraction(elapsed_seconds_float)
        percent = max(0, min(99, int(round(progress_fraction * 100))))
        filled = max(0, min(_SCAN_PROGRESS_BAR_WIDTH, int(round(progress_fraction * _SCAN_PROGRESS_BAR_WIDTH))))
        progress_bar = (
            "["
            + ("#" * filled)
            + ("-" * (_SCAN_PROGRESS_BAR_WIDTH - filled))
            + f"] {percent}%"
        )
        stage_label = self._tr(
            f"common.dynamic.scan_progress_stage_{self._scan_progress_stage}",
            "Preparing scan",
        )
        return {
            "selected_scan_interface": selected_label,
            "scan_progress_phase": stage_label,
            "scan_progress_bar": progress_bar,
            "scan_progress_detail": self._tr(
                "common.dynamic.scan_progress_detail",
                "{elapsed_seconds}s elapsed, about {remaining_seconds}s remaining.",
                {
                    "elapsed_seconds": elapsed_seconds,
                    "remaining_seconds": remaining_seconds,
                },
            ),
            "scan_progress_hint": self._tr(
                "common.dynamic.scan_progress_hint",
                "Most scans finish in 5-15 seconds. This progress bar is estimated from the current phase and timeout.",
            ),
        }

    def _peer_label(self) -> str:
        return self._tr(
            "common.dynamic.peer_label",
            self._connection_display().peer_label,
        )

    def _peer_label_plural(self) -> str:
        return self._tr(
            "common.dynamic.peer_label_plural",
            self._connection_display().peer_label_plural,
        )

    def _unconfirmed_inverter_label(self) -> str:
        return self._tr(
            "common.dynamic.unconfirmed_inverter",
            self._connection_display().unconfirmed_inverter_label,
        )

    def _selector_for_connection_field(self, field: ConnectionFormField):
        """Resolve the concrete HA selector for one branch-aware connection field."""

        if field.selector_kind == "server_ip":
            return self._server_ip_field()
        if field.selector_kind == "ip":
            return _IP_TEXT_SELECTOR
        if field.selector_kind == "port":
            return _PORT_SELECTOR
        if field.selector_kind == "optional_port":
            return _IP_TEXT_SELECTOR
        if field.selector_kind == "discovery_interval":
            return _DISCOVERY_INTERVAL_SELECTOR
        if field.selector_kind == "heartbeat_interval":
            return _HEARTBEAT_INTERVAL_SELECTOR
        if field.selector_kind == "driver_hint":
            return _driver_selector()
        raise ValueError(f"unsupported_connection_selector:{field.selector_kind}")

    def _build_connection_fields_schema(
        self,
        connection_type: str,
        *,
        fields: tuple[ConnectionFormField, ...],
        values: dict[str, Any],
    ) -> dict[Any, Any]:
        """Build a voluptuous schema mapping for branch-aware connection fields."""

        get_connection_branch(connection_type)
        schema: dict[Any, Any] = {}
        for field in fields:
            marker = vol.Required if field.required else vol.Optional
            schema[marker(field.key, default=values.get(field.key, ""))] = self._selector_for_connection_field(field)
        return schema

    # ---- description placeholders ----

    def _auto_description_placeholders(self, single_interface: bool) -> dict[str, str]:
        if single_interface and self._interface_options:
            item = self._interface_options[0]
            return {
                "interface_hint": self._tr(
                    "common.dynamic.auto_interface_hint_single",
                    "Home Assistant will use **{selected_interface}** automatically.",
                    {"selected_interface": item["label"]},
                ),
            }
        return {
            "interface_hint": self._tr(
                "common.dynamic.auto_interface_hint_multi",
                "Choose which Home Assistant interface the {peer_label} should connect back to.",
                {"peer_label": self._peer_label()},
            ),
        }

    def _welcome_description_placeholders(self) -> dict[str, str]:
        display = self._connection_display()
        if len(self._interface_options) > 1:
            return {
                "welcome_hint": self._tr(
                    "common.dynamic.welcome_connection_type_multi",
                    "Choose the connection type first. On the next step you will choose which Home Assistant network interface to use, then scanning will start automatically.",
                    {
                        "integration_name": display.integration_name,
                    },
                ),
            }
        selected_ip = self._local_ip
        selected_label = next(
            (
                item["label"]
                for item in self._interface_options
                if item["ip"] == selected_ip
            ),
            selected_ip
            or self._tr(
                "common.dynamic.default_home_assistant_interface",
                "the default Home Assistant interface",
            ),
        )
        return {
            "welcome_hint": self._tr(
                "common.dynamic.welcome_connection_type_single",
                "Choose the connection type first. Scanning will then start automatically using **{selected_interface}**.",
                {
                    "integration_name": display.integration_name,
                    "selected_interface": selected_label,
                },
            ),
        }

    def _manual_confirm_placeholders(
        self,
        manual_config: dict[str, Any],
        result: OnboardingResult | None,
    ) -> dict[str, str]:
        collector_ip = ""
        collector_pn = ""
        smartess_collector_version = ""
        smartess_protocol_asset_id = ""
        model_name = self._unconfirmed_inverter_label()
        serial_number = self._tr("common.dynamic.not_available_yet", "Not available yet")

        if result is not None and result.collector is not None:
            collector_ip = result.collector.ip
            collector = result.collector.collector
            if collector is not None:
                collector_pn = collector.collector_pn or ""
                smartess_collector_version = collector.smartess_collector_version or ""
                smartess_protocol_asset_id = collector.smartess_protocol_asset_id or ""
        if not collector_ip:
            collector_ip = manual_config.get(CONF_COLLECTOR_IP) or manual_config.get(CONF_DISCOVERY_TARGET, "")

        smartess_hint_available = bool(
            smartess_collector_version or smartess_protocol_asset_id
        )

        if result is not None and result.match is not None:
            model_name = result.match.model_name
            serial_number = result.match.serial_number or serial_number

        if result is not None and result.match is not None:
            probe_summary = self._tr(
                "common.dynamic.manual_probe_confirmed",
                "{peer_label_capitalized} and inverter were confirmed with the manual settings.",
                {"peer_label_capitalized": self._peer_label().capitalize()},
            )
        elif (
            result is not None
            and result.collector is not None
            and result.collector.connected
            and smartess_hint_available
        ):
            probe_summary = self._tr(
                "common.dynamic.manual_probe_smartess_hint",
                "The {peer_label} responded and exposed SmartESS metadata, but the local inverter model is still unconfirmed.",
                {"peer_label": self._peer_label()},
            )
        elif result is not None and result.collector is not None and result.collector.connected:
            probe_summary = self._tr(
                "common.dynamic.manual_probe_unconfirmed_model",
                "The {peer_label} responded, but the inverter model is still unconfirmed.",
                {"peer_label": self._peer_label()},
            )
        else:
            probe_summary = self._tr(
                "common.dynamic.manual_probe_none",
                "No {peer_label} or inverter was confirmed yet.",
                {"peer_label": self._peer_label()},
            )

        return {
            "probe_summary": probe_summary,
            "collector_ip": collector_ip or self._tr("common.dynamic.unknown", "Unknown"),
            "collector_pn": collector_pn or self._tr("common.dynamic.unknown", "Unknown"),
            "model_name": model_name,
            "serial_number": serial_number,
            "smartess_cloud_summary": self._smartess_cloud_summary(result),
            "control_summary": self._tr(
                (
                    "common.dynamic.manual_control_summary_smartess_hint"
                    if smartess_hint_available
                    else "common.dynamic.manual_control_summary"
                ),
                (
                    "If you continue, a **read-only** pending entry will be created. Sensors may stay unavailable until a local driver match is confirmed. This local probe does not rule out SmartESS app support; the app may still use a separate cloud identity."
                    if smartess_hint_available
                    else "If you continue, a **read-only** pending entry will be created. Sensors may stay unavailable until the {peer_label} connects and detection completes."
                ),
                {"peer_label": self._peer_label()},
            ),
            "next_actions_hint": self._tr(
                "common.dynamic.manual_probe_next_actions",
                "Choose **{probe_again_action_label}** to test again, **{edit_settings_action_label}** to change the values, or **{create_pending_action_label}** to save a read-only pending entry now.",
                {
                    "probe_again_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
                        "Probe again",
                    ),
                    "edit_settings_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
                        "Edit settings",
                    ),
                    "create_pending_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_CREATE_PENDING,
                        "Create pending entry",
                    ),
                },
            ),
        }

    @staticmethod
    def _validate_connection_inputs(
        user_input: dict[str, Any],
        *,
        fields: tuple[ConnectionFormField, ...],
    ) -> dict[str, str]:
        errors: dict[str, str] = {}
        for field in fields:
            raw_value = str(user_input.get(field.key, "") or "").strip()
            if field.validation_kind == "ipv4":
                if not raw_value:
                    if field.required:
                        errors[field.key] = "invalid_ip"
                    continue
                if not _is_ipv4(raw_value):
                    errors[field.key] = "invalid_ip"
                continue
            if field.validation_kind == "port_optional":
                if not raw_value:
                    continue
                if not raw_value.isdigit() or not 1 <= int(raw_value) <= 65535:
                    errors[field.key] = "invalid_port"
        return errors

    # ---- scan result helpers ----

    def _result_label(self, result: OnboardingResult) -> str:
        match = result.match
        collector = result.collector
        collector_ip = collector.ip if collector is not None else self._tr("common.dynamic.unknown", "Unknown")
        status_label = self._result_status_label(result)
        if match is None:
            suffix = (
                self._tr(
                    "common.dynamic.suffix_smartess_hint",
                    "SmartESS metadata",
                )
                if has_smartess_collector_hint(result)
                else self._tr(
                    "common.dynamic.suffix_peer_connected",
                    "{peer_label} connected",
                    {"peer_label": self._peer_label()},
                )
                if collector is not None and collector.connected
                else self._tr(
                    "common.dynamic.suffix_peer_only",
                    "{peer_label} only",
                    {"peer_label": self._peer_label()},
                )
            )
            return self._tr(
                "common.dynamic.result_label_unmatched",
                "{status_label}: {collector_ip} ({suffix})",
                {
                    "status_label": status_label,
                    "collector_ip": collector_ip,
                    "suffix": suffix,
                },
            )
        serial = match.serial_number or self._tr("common.dynamic.unknown_serial", "unknown serial")
        return self._tr(
            "common.dynamic.result_label_matched",
            "{status_label}: {model_name} ({serial_number}) on {collector_ip} — {confidence_label}",
            {
                "status_label": status_label,
                "model_name": match.model_name,
                "serial_number": serial,
                "collector_ip": collector_ip,
                "confidence_label": self._confidence_label(result.confidence),
            },
        )

    def _result_placeholders(self, result: OnboardingResult) -> dict[str, str]:
        collector = result.collector
        match = result.match
        assist_state = self._smartess_cloud_assist_state_for_result(result)
        collector_ip = collector.ip if collector is not None else self._tr("common.dynamic.unknown", "Unknown")
        collector_pn = ""
        if collector is not None and collector.collector is not None:
            collector_pn = collector.collector.collector_pn or ""
        driver_key = match.driver_key if match is not None else DRIVER_HINT_AUTO
        if match is None and assist_state is not None and assist_state.inferred_driver_key:
            driver_key = f"{assist_state.inferred_driver_key} (cloud-assisted)"
        return {
            "model_name": match.model_name if match is not None else self._unconfirmed_inverter_label(),
            "serial_number": match.serial_number if match is not None else self._tr("common.dynamic.not_available_yet", "Not available yet"),
            "driver_key": driver_key,
            "collector_ip": collector_ip,
            "collector_pn": collector_pn or self._tr("common.dynamic.unknown", "Unknown"),
            "confidence": self._confidence_label(result.confidence),
            "smartess_cloud_summary": self._smartess_cloud_summary(result),
            "control_summary": self._default_control_summary(result.confidence),
        }

    def _confidence_label(self, confidence: str) -> str:
        return {
            "high": self._tr("common.dynamic.confidence_high", "High confidence"),
            "medium": self._tr("common.dynamic.confidence_medium", "Medium confidence"),
            "low": self._tr("common.dynamic.confidence_low", "Low confidence"),
            "none": self._tr("common.dynamic.confidence_none", "No confidence"),
        }.get(confidence, confidence)

    def _default_control_summary(self, confidence: str) -> str:
        if confidence == "high":
            return self._tr(
                "common.dynamic.control_auto",
                "Tested controls are enabled automatically.",
            )
        return self._tr(
            "common.dynamic.control_waiting",
            "Monitoring only until a high-confidence detection is confirmed.",
        )

    def _result_unique_id(self, result: OnboardingResult) -> str:
        collector_ip = result.collector.ip if result.collector is not None else ""
        collector_info = result.collector.collector if result.collector is not None else None
        collector_pn = collector_info.collector_pn if collector_info is not None else ""
        server_ip = self._auto_config.get(CONF_SERVER_IP, self._local_ip)
        return (
            f"collector:{collector_pn}"
            if collector_pn
            else f"inverter:{result.match.serial_number}"
            if result.match is not None and result.match.serial_number
            else f"collector_ip:{collector_ip}"
            if collector_ip
            else f"listener:{server_ip}:{DEFAULT_TCP_PORT}"
        )

    def _existing_entry_for_result(self, result: OnboardingResult):
        collector = result.collector
        collector_info = collector.collector if collector is not None else None
        collector_pn = collector_info.collector_pn if collector_info is not None else ""
        collector_ip = collector.ip if collector is not None else ""
        serial_number = result.match.serial_number if result.match is not None else ""
        candidate_unique_id = self._result_unique_id(result)

        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if entry.entry_id == self.context.get("entry_id"):
                continue
            if entry.unique_id and entry.unique_id == candidate_unique_id:
                return entry
            entry_collector_pn = entry.data.get(CONF_COLLECTOR_PN, "")
            entry_serial = entry.data.get(CONF_DETECTED_SERIAL, "")
            entry_collector_ip = entry.data.get(CONF_COLLECTOR_IP, "")
            if collector_pn and entry_collector_pn == collector_pn:
                return entry
            if serial_number and entry_serial == serial_number:
                return entry
            if collector_ip and entry_collector_ip == collector_ip:
                return entry
        return None

    @staticmethod
    def _is_visible_scan_result(result: OnboardingResult) -> bool:
        collector = result.collector
        if result.match is not None:
            return True
        if collector is None:
            return False
        return bool(collector.connected or collector.udp_reply)

    @staticmethod
    def _is_addable_scan_result(result: OnboardingResult) -> bool:
        collector = result.collector
        return bool(result.match is not None or (collector is not None and collector.connected))

    def _available_autodetect_results(self) -> dict[str, OnboardingResult]:
        return {
            key: result
            for key, result in self._sorted_autodetect_items()
            if self._is_addable_scan_result(result)
            if self._existing_entry_for_result(result) is None
        }

    @staticmethod
    def _scan_result_key(result: OnboardingResult) -> str:
        collector = result.collector
        collector_info = collector.collector if collector is not None else None
        collector_pn = collector_info.collector_pn if collector_info is not None else ""
        if collector_pn:
            return f"collector:{collector_pn}"
        if collector is not None and collector.ip:
            return f"ip:{collector.ip}"
        if collector is not None and collector.target_ip:
            return f"target:{collector.target_ip}"
        if result.match is not None and result.match.serial_number:
            return f"serial:{result.match.serial_number}"
        return "unknown"

    @staticmethod
    def _scan_result_priority(result: OnboardingResult) -> tuple[int, int, int, int]:
        collector = result.collector
        return (
            1 if result.match is not None else 0,
            1 if collector is not None and collector.connected else 0,
            1 if collector is not None and collector.udp_reply else 0,
            confidence_sort_score(result.confidence),
        )

    def _sorted_autodetect_items(self) -> list[tuple[str, OnboardingResult]]:
        return sorted(
            self._autodetect_results.items(),
            key=lambda item: scan_result_sort_key(
                item[1],
                already_added=self._existing_entry_for_result(item[1]) is not None,
            ),
        )

    def _sort_scan_results(self, results: list[OnboardingResult]) -> list[OnboardingResult]:
        return sorted(
            results,
            key=lambda result: scan_result_sort_key(
                result,
                already_added=self._existing_entry_for_result(result) is not None,
            ),
        )

    @staticmethod
    def _scan_result_status_code(result: OnboardingResult, already_added: bool = False) -> str:
        return scan_result_status_code(result, already_added)

    @classmethod
    def _scan_result_sort_key(
        cls,
        result: OnboardingResult,
        *,
        already_added: bool = False,
    ) -> tuple[int, int, str, str, str]:
        return scan_result_sort_key(result, already_added=already_added)

    def _collapse_scan_results(
        self,
        results: Any,
    ) -> list[OnboardingResult]:
        collapsed: dict[str, OnboardingResult] = {}
        for result in results:
            key = self._scan_result_key(result)
            current = collapsed.get(key)
            if current is None or self._scan_result_priority(result) > self._scan_result_priority(current):
                collapsed[key] = result
        return list(collapsed.values())

    def _scan_results_placeholders(self) -> dict[str, str]:
        results = self._sorted_autodetect_items()
        available_count = 0
        already_added_count = 0
        selected_ip = self._auto_config.get(CONF_SERVER_IP, self._local_ip)
        refresh_action_label = self._scan_action_label("refresh_scan", "Refresh scan results")
        manual_action_label = self._scan_action_label("manual", "Manual setup")
        selected_label = next(
            (
                item["label"]
                for item in self._interface_options
                if item["ip"] == selected_ip
            ),
            selected_ip or "Unknown",
        )
        for _, result in results:
            existing_entry = self._existing_entry_for_result(result)
            if existing_entry is not None:
                already_added_count += 1
            elif self._is_addable_scan_result(result):
                available_count += 1

        detected_count = len(results)
        ready_models = [
            result.match.model_name
            for result in self._available_autodetect_results().values()
            if result.match is not None and result.match.model_name
        ]
        candidate_list = "\n".join(
            self._scan_result_line(index, result)
            for index, (_, result) in enumerate(results, start=1)
        )
        if detected_count == 0:
            scan_summary = self._tr(
                "common.dynamic.scan_no_results_summary",
                "No reachable {peer_label_plural} or inverters were found.",
                {"peer_label_plural": self._peer_label_plural()},
            )
            next_hint = self._tr(
                "common.dynamic.scan_no_results_next",
                "Use **{refresh_action_label}** to try again, or **{manual_action_label}** to switch to manual setup.",
                {
                    "refresh_action_label": refresh_action_label,
                    "manual_action_label": manual_action_label,
                },
            )
        elif available_count == 0 and already_added_count == detected_count:
            scan_summary = self._tr(
                "common.dynamic.scan_all_added_summary",
                "Found **{detected_count}** device candidate(s), but all of them are already configured.",
                {"detected_count": detected_count},
            )
            next_hint = self._tr(
                "common.dynamic.scan_all_added_next",
                "Use **{refresh_action_label}** to look again, or **{manual_action_label}** if you intentionally need a different connection path.",
                {
                    "refresh_action_label": refresh_action_label,
                    "manual_action_label": manual_action_label,
                },
            )
        elif available_count == 0:
            scan_summary = self._tr(
                "common.dynamic.scan_none_addable_summary",
                "Found **{detected_count}** device candidate(s), but none are ready to add yet.",
                {"detected_count": detected_count},
            )
            next_hint = self._tr(
                "common.dynamic.scan_none_addable_next",
                "Use **{refresh_action_label}** to try again, or **{manual_action_label}** to override the connection settings.",
                {
                    "refresh_action_label": refresh_action_label,
                    "manual_action_label": manual_action_label,
                },
            )
        elif not ready_models:
            choose_action_label = self._scan_action_label("choose", "Add detected device")
            scan_summary = self._tr(
                "common.dynamic.scan_pending_summary",
                "Found **{detected_count}** device candidate(s). **{available_count}** collector candidate(s) can be added now, but local inverter matching is still pending.",
                {
                    "detected_count": detected_count,
                    "available_count": available_count,
                },
            )
            next_hint = self._tr(
                "common.dynamic.scan_pending_next",
                "Choose **{choose_action_label}** to save a pending entry, or use **{refresh_action_label}** or **{manual_action_label}** to retry the local match.",
                {
                    "choose_action_label": choose_action_label,
                    "refresh_action_label": refresh_action_label,
                    "manual_action_label": manual_action_label,
                },
            )
        else:
            choose_action_label = self._scan_action_label("choose", "Add detected device")
            ready_summary = (
                ", ".join(dict.fromkeys(ready_models[:5]))
                or self._tr("common.dynamic.scan_ready_fallback", "detected inverters")
            )
            scan_summary = self._tr(
                "common.dynamic.scan_ready_summary",
                "Found **{detected_count}** device candidate(s). **{available_count}** can be added now, **{already_added_count}** already configured. Ready now: {ready_summary}.",
                {
                    "detected_count": detected_count,
                    "available_count": available_count,
                    "already_added_count": already_added_count,
                    "ready_summary": ready_summary,
                },
            )
            next_hint = self._tr(
                "common.dynamic.scan_ready_next",
                "Choose **{choose_action_label}** to pick which inverter to add.",
                {"choose_action_label": choose_action_label},
            )

        return {
            "scan_summary": scan_summary,
            "scan_next_hint": next_hint,
            "selected_scan_interface": selected_label,
            "candidate_list": candidate_list,
        }

    def _choose_placeholders(self) -> dict[str, str]:
        return {
            "choose_summary": self._tr(
                "common.dynamic.choose_summary",
                "**{available_count}** detected device candidate(s) can be added right now. Already configured devices are excluded.",
                {"available_count": len(self._available_autodetect_results())},
            )
        }

    def _scan_result_line(self, index: int, result: OnboardingResult) -> str:
        collector = result.collector
        collector_info = collector.collector if collector is not None else None
        collector_ip = collector.ip if collector is not None else self._tr("common.dynamic.unknown", "Unknown")
        existing_entry = self._existing_entry_for_result(result)
        collector_pn = collector_info.collector_pn if collector_info is not None else ""
        status_label = self._result_status_label(result, existing_entry is not None)

        if result.match is not None:
            line = self._tr(
                "common.dynamic.scan_line_matched",
                "{index}. **{status_label}** — {model_name} · serial {serial_number} · {peer_label} {collector_ip} · {confidence_label}",
                {
                    "index": index,
                    "status_label": status_label,
                    "model_name": result.match.model_name,
                    "serial_number": result.match.serial_number or self._tr("common.dynamic.unknown", "Unknown"),
                    "peer_label": self._peer_label(),
                    "collector_ip": collector_ip,
                    "confidence_label": self._confidence_label(result.confidence),
                },
            )
        else:
            details = [
                self._unconfirmed_inverter_label(),
                f"{self._peer_label()} {collector_ip}",
            ]
            if collector_pn:
                details.append(f"PN {collector_pn}")
            if has_smartess_collector_hint(result):
                details.append(
                    self._tr(
                        "common.dynamic.scan_line_smartess_hint",
                        "SmartESS metadata",
                    )
                )
            if collector is not None and collector.connected:
                details.append(
                    self._tr(
                        "common.dynamic.scan_line_peer_connected",
                        "{peer_label} connected",
                        {"peer_label": self._peer_label()},
                    )
                )
            elif collector is not None and collector.udp_reply:
                details.append(
                    self._tr(
                        "common.dynamic.scan_line_peer_replied",
                        "{peer_label} replied, waiting for reverse connection",
                        {"peer_label": self._peer_label()},
                    )
                )
            line = f"{index}. **{status_label}** — " + " · ".join(details)

        if existing_entry is not None:
            line += " " + self._tr(
                "common.dynamic.scan_line_already_added",
                '*(already added as "{entry_title}")*',
                {"entry_title": existing_entry.title},
            )
        return line

    def _result_status_label(self, result: OnboardingResult, already_added: bool = False) -> str:
        status_code = scan_result_status_code(result, already_added)
        return {
            "ready": self._tr("common.dynamic.status_ready", "Ready"),
            "review": self._tr("common.dynamic.status_review", "Review"),
            "already_added": self._tr("common.dynamic.status_already_added", "Already added"),
            "smartess_hint": self._tr("common.dynamic.status_smartess_hint", "SmartESS hint"),
            "collector_only": self._tr("common.dynamic.status_collector_only", "Collector only"),
            "collector_replied": self._tr("common.dynamic.status_collector_replied", "Collector replied"),
            "unknown": self._tr("common.dynamic.status_unknown", "Unknown"),
        }.get(status_code, self._tr("common.dynamic.status_unknown", "Unknown"))


# ---------------------------------------------------------------------------
# Options flow
# ---------------------------------------------------------------------------

class EybondLocalOptionsFlow(_TranslationBundleMixin, OptionsFlow):
    """Config entry options."""

    def __init__(self, config_entry) -> None:
        self._config_entry = config_entry
        self._translation_bundle: dict[str, Any] = {}
        self._translation_bundle_language = ""
        self._interface_options: list[dict[str, str]] = []
        self._diagnostics_result: dict[str, str] = {}

    def _server_ip_field(self) -> SelectSelector | TextSelector:
        """Return the user-friendly selector for one local server IP."""

        if not self._interface_options:
            return _IP_TEXT_SELECTOR
        return _interface_selector(self._interface_options)

    def _selector_for_connection_field(self, field: ConnectionFormField):
        """Resolve one selector for branch-aware connection fields."""

        return EybondLocalConfigFlow._selector_for_connection_field(self, field)

    def _build_connection_fields_schema(
        self,
        connection_type: str,
        *,
        fields: tuple[ConnectionFormField, ...],
        values: dict[str, Any],
    ) -> dict[Any, Any]:
        """Build one schema mapping for options-flow connection sections."""

        return EybondLocalConfigFlow._build_connection_fields_schema(
            self,
            connection_type,
            fields=fields,
            values=values,
        )

    async def async_step_init(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        return self.async_show_menu(
            step_id="init",
            menu_options=["runtime", "diagnostics"],
        )

    @_with_translation_bundle
    async def async_step_runtime(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if not self._interface_options:
            self._interface_options = await self.hass.async_add_executor_job(_get_ipv4_interfaces)
        errors: dict[str, str] = {}
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            connection_type = self._config_entry.data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND)
            branch = get_connection_branch(connection_type)
            errors = EybondLocalConfigFlow._validate_connection_inputs(
                flat_input,
                fields=branch.form_layout.runtime_fields,
            )
            if not errors:
                persisted_options = build_runtime_option_settings(connection_type, flat_input)
                persisted_options[CONF_POLL_INTERVAL] = flat_input[CONF_POLL_INTERVAL]
                persisted_options[CONF_CONTROL_MODE] = flat_input[CONF_CONTROL_MODE]
                return self.async_create_entry(data=persisted_options)

        connection_type = self._config_entry.data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND)
        branch = get_connection_branch(connection_type)
        connection_values = branch.build_runtime_option_values(
            data=self._config_entry.data,
            options=self._config_entry.options,
            default_server_ip=self._config_entry.data[CONF_SERVER_IP],
            default_broadcast=DEFAULT_DISCOVERY_TARGET,
        )
        poll_interval = self._config_entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
        control_mode = self._config_entry.options.get(
            CONF_CONTROL_MODE,
            self._config_entry.data.get(CONF_CONTROL_MODE, DEFAULT_CONTROL_MODE),
        )

        data_schema = vol.Schema(
            {
                vol.Required(CONF_POLL_INTERVAL, default=poll_interval): _POLL_INTERVAL_SELECTOR,
                vol.Required(CONF_CONTROL_MODE, default=control_mode): _control_mode_selector(),
                vol.Required("connection"): section(
                    vol.Schema(
                        self._build_connection_fields_schema(
                            connection_type,
                            fields=branch.form_layout.runtime_fields,
                            values=connection_values,
                        )
                    ),
                    {"collapsed": True},
                ),
            }
        )

        return self.async_show_form(
            step_id="runtime",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={
                "model_name": self._config_entry.data.get(CONF_DETECTED_MODEL, "Unknown"),
                "serial_number": self._config_entry.data.get(CONF_DETECTED_SERIAL, "Unknown"),
                "confidence": self._confidence_label(
                    self._config_entry.data.get(CONF_DETECTION_CONFIDENCE, "none")
                ),
                "control_summary": self._control_summary(
                    control_mode=control_mode,
                    confidence=self._config_entry.data.get(CONF_DETECTION_CONFIDENCE, "none"),
                ),
            },
        )

    @_with_translation_bundle
    async def async_step_diagnostics(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        placeholders = self._diagnostics_placeholders()
        primary_action = placeholders["support_workflow_primary_action"]
        menu_options = self._diagnostics_menu_options(primary_action)

        return self.async_show_menu(
            step_id="diagnostics",
            menu_options=menu_options,
            description_placeholders=placeholders,
        )

    @_with_translation_bundle
    async def async_step_advanced_metadata(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        placeholders = self._diagnostics_placeholders()
        menu_options: list[str] = ["export_support_bundle"]

        if coordinator is not None and getattr(coordinator, "smartess_smg_bridge_plan", None) is not None:
            menu_options.append("create_smartess_smg_bridge")
        if coordinator is not None and getattr(coordinator, "smartess_known_family_draft_plan", None) is not None:
            menu_options.append("create_smartess_draft")
        if coordinator is not None and coordinator.effective_profile_name:
            menu_options.append("create_profile_draft")
        if coordinator is not None and coordinator.effective_register_schema_name:
            menu_options.append("create_schema_draft")

        return self.async_show_menu(
            step_id="advanced_metadata",
            menu_options=menu_options,
            description_placeholders=placeholders,
        )

    @_with_translation_bundle
    async def async_step_create_support_package(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "support_archive_title",
                    "Support Archive",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "ensure_entry_loaded",
                    "Ensure the entry is loaded and the inverter has been detected, then try again.",
                ),
            )

        can_refresh_cloud_evidence = bool(
            getattr(coordinator, "smartess_cloud_export_available", False)
        )
        saved_cloud_evidence_path = self._current_cloud_evidence_path(coordinator)
        had_saved_cloud_evidence = bool(saved_cloud_evidence_path)

        if user_input is None and can_refresh_cloud_evidence:
            return self._show_create_support_package_form(
                coordinator=coordinator,
                saved_cloud_evidence_path=saved_cloud_evidence_path,
            )

        archive_cloud_mode = self._default_support_archive_cloud_mode(
            had_saved_cloud_evidence=had_saved_cloud_evidence,
        )
        smartess_username = ""
        smartess_password = ""

        if can_refresh_cloud_evidence:
            form_input = user_input or {}
            archive_cloud_mode = str(
                form_input.get(CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE)
                or self._default_support_archive_cloud_mode(
                    had_saved_cloud_evidence=had_saved_cloud_evidence,
                )
            )
            smartess_username = str(form_input.get("username") or "").strip()
            smartess_password = str(form_input.get("password") or "")
            wants_inline_refresh = archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH
            errors: dict[str, str] = {}
            if wants_inline_refresh:
                if not smartess_username:
                    errors["username"] = "required"
                if not smartess_password:
                    errors["password"] = "required"
            if errors:
                return self._show_create_support_package_form(
                    coordinator=coordinator,
                    saved_cloud_evidence_path=saved_cloud_evidence_path,
                    user_input=form_input,
                    errors=errors,
                )

        try:
            path = await coordinator.async_export_support_package_with_cloud_refresh(
                smartess_username=smartess_username,
                smartess_password=smartess_password,
            )
        except Exception as exc:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "support_archive_title",
                    "Support Archive",
                ),
                status=self._diagnostics_result_tr(
                    "support_archive_failed_status",
                    "Support archive export failed: {error}",
                    {"error": str(exc)},
                ),
                next_step=self._diagnostics_result_tr(
                    (
                        "support_archive_failed_next_refresh"
                        if archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH
                        else "support_archive_failed_next"
                    ),
                    (
                        "Check the SmartESS credentials, or rerun Create support archive and choose a different SmartESS cloud evidence mode."
                        if archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH
                        else "Check whether the entry is loaded and the Home Assistant config directory is writable, then try again."
                    ),
                ),
            )

        download_url = str(coordinator.data.values.get("support_package_download_url") or "")
        return await self._async_show_diagnostics_result(
            action_title=self._diagnostics_result_tr(
                "support_archive_created_title",
                "Support Archive Created",
            ),
            status=self._diagnostics_result_tr(
                "support_archive_created_status",
                "A combined support archive with runtime data, raw capture evidence, an anonymized replay fixture, and matching SmartESS cloud evidence when available was written to the Home Assistant config directory.\n\n{support_archive_cloud_detail}",
                {
                    "support_archive_cloud_detail": self._support_archive_cloud_result_detail(
                        archive_cloud_mode=archive_cloud_mode,
                        had_saved_cloud_evidence=had_saved_cloud_evidence,
                    )
                },
            ),
            path=path,
            download_url=download_url,
            next_step=self._diagnostics_result_tr(
                "support_archive_created_next",
                "Send this single ZIP file to the developer. Create local experimental drafts only after the archive has been reviewed.",
            ),
        )

    @_with_translation_bundle
    async def async_step_reload_local_metadata(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "reload_local_metadata_title",
                    "Reload Local Metadata",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "wait_for_entry_loaded",
                    "Wait for the entry to finish loading, then try again.",
                ),
            )

        await coordinator.async_reload_local_metadata()
        return await self._async_show_diagnostics_result(
            action_title=self._diagnostics_result_tr(
                "reload_local_metadata_triggered_title",
                "Local Metadata Reload Triggered",
            ),
            status=self._diagnostics_result_tr(
                "reload_local_metadata_triggered_status",
                "Local metadata caches were cleared and the entry reload was requested.",
            ),
            next_step=self._diagnostics_result_tr(
                "reload_local_metadata_triggered_next",
                "Refresh the device page after the entry reconnects to confirm whether local overrides were applied.",
            ),
        )

    @_with_translation_bundle
    async def async_step_rollback_local_metadata(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        rollback_paths = self._local_metadata_rollback_paths()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "rollback_local_metadata_title",
                    "Rollback Local Metadata",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "wait_for_entry_loaded",
                    "Wait for the entry to finish loading, then try again.",
                ),
            )

        if not rollback_paths.paths:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "rollback_local_metadata_title",
                    "Rollback Local Metadata",
                ),
                status=self._diagnostics_result_tr(
                    "rollback_local_metadata_unavailable_status",
                    "No active managed local metadata override is available to roll back for this entry.",
                ),
                next_step=self._diagnostics_result_tr(
                    "rollback_local_metadata_unavailable_next",
                    "Create or activate a local override first, or use Reload local metadata if the files were already removed manually.",
                ),
            )

        if user_input is not None:
            try:
                removed_paths = await coordinator.async_rollback_local_metadata()
            except Exception as exc:
                return await self._async_show_diagnostics_result(
                    action_title=self._diagnostics_result_tr(
                        "rollback_local_metadata_title",
                        "Rollback Local Metadata",
                    ),
                    status=self._diagnostics_result_tr(
                        "rollback_local_metadata_failed_status",
                        "Local metadata rollback failed: {error}",
                        {"error": str(exc)},
                    ),
                    next_step=self._diagnostics_result_tr(
                        "rollback_local_metadata_failed_next",
                        "Check whether the active override files still exist under /config/eybond_local/, then try again.",
                    ),
                )

            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "rollback_local_metadata_done_title",
                    "Local Metadata Rolled Back",
                ),
                status=self._diagnostics_result_tr(
                    "rollback_local_metadata_done_status",
                    "The active managed local override files were removed and the entry reload was requested.",
                ),
                path=" ; ".join(removed_paths),
                next_step=self._diagnostics_result_tr(
                    "rollback_local_metadata_done_next",
                    "Refresh the device page after the entry reconnects to confirm that the built-in metadata is active again.",
                ),
            )

        not_available = self._tr("common.dynamic.not_available", "Not available")
        return self.async_show_form(
            step_id="rollback_local_metadata",
            data_schema=vol.Schema({}),
            description_placeholders={
                "rollback_target_count": str(len(rollback_paths.paths)),
                "rollback_profile_path": str(rollback_paths.profile_path or not_available),
                "rollback_schema_path": str(rollback_paths.schema_path or not_available),
            },
        )

    @_with_translation_bundle
    async def async_step_export_support_bundle(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "support_bundle_title",
                    "Support Bundle",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "ensure_entry_loaded",
                    "Ensure the entry is loaded and the inverter has been detected, then try again.",
                ),
            )

        path = await coordinator.async_export_support_bundle()
        return await self._async_show_diagnostics_result(
            action_title=self._diagnostics_result_tr(
                "support_bundle_exported_title",
                "Support Bundle Exported",
            ),
            status=self._diagnostics_result_tr(
                "support_bundle_exported_status",
                "A support bundle was written to the Home Assistant config directory.",
            ),
            path=path,
            next_step=self._diagnostics_result_tr(
                "support_bundle_exported_next",
                "Use the JSON bundle for troubleshooting or as source material for a new local experimental draft.",
            ),
        )

    @_with_translation_bundle
    async def async_step_export_smartess_cloud_evidence(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "smartess_cloud_evidence_title",
                    "SmartESS Cloud Evidence",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "ensure_entry_loaded",
                    "Ensure the entry is loaded and the inverter has been detected, then try again.",
                ),
            )

        if user_input is not None:
            try:
                path = await coordinator.async_export_smartess_cloud_evidence(
                    username=str(user_input.get("username") or "").strip(),
                    password=str(user_input.get("password") or ""),
                )
            except Exception as exc:
                return await self._async_show_diagnostics_result(
                    action_title=self._diagnostics_result_tr(
                        "smartess_cloud_evidence_title",
                        "SmartESS Cloud Evidence",
                    ),
                    status=self._diagnostics_result_tr(
                        "smartess_cloud_evidence_failed_status",
                        "SmartESS cloud evidence export failed: {error}",
                        {"error": str(exc)},
                    ),
                    next_step=self._diagnostics_result_tr(
                        "smartess_cloud_evidence_failed_next",
                        "Check the SmartESS credentials and collector mapping, then run the export again.",
                    ),
                )

            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "smartess_cloud_evidence_exported_title",
                    "SmartESS Cloud Evidence Exported",
                ),
                status=self._diagnostics_result_tr(
                    "smartess_cloud_evidence_exported_status",
                    "A SmartESS cloud bundle was written to the Home Assistant config directory and linked to this entry by collector identity.",
                ),
                path=path,
                next_step=self._smartess_cloud_exported_next_step(),
            )

        collector_pn = str(getattr(coordinator, "smartess_collector_pn", "") or "")
        return self.async_show_form(
            step_id="export_smartess_cloud_evidence",
            data_schema=vol.Schema(
                {
                    vol.Required("username", default=""): _IP_TEXT_SELECTOR,
                    vol.Required("password", default=""): _PASSWORD_TEXT_SELECTOR,
                }
            ),
            description_placeholders={
                "collector_pn": collector_pn or self._tr("common.dynamic.not_available", "Not available"),
                "cloud_evidence_path": self._diagnostics_placeholders()["cloud_evidence_path"],
            },
        )

    @_with_translation_bundle
    async def async_step_create_smartess_draft(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        plan = getattr(coordinator, "smartess_known_family_draft_plan", None) if coordinator is not None else None
        if coordinator is None or plan is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "smartess_draft_title",
                    "SmartESS Draft",
                ),
                status=self._diagnostics_result_tr(
                    "smartess_draft_unavailable_status",
                    "No safe SmartESS known-family draft source is available for this entry yet.",
                ),
                next_step=self._diagnostics_result_tr(
                    "smartess_draft_unavailable_next",
                    "Export SmartESS cloud evidence first, or wait until a known SmartESS family mapping is confirmed.",
                ),
            )

        if user_input is not None:
            output_profile = str(user_input.get("output_profile") or "").strip() or None
            output_schema = str(user_input.get("output_schema") or "").strip() or None
            overwrite = bool(user_input.get("overwrite", False))
            auto_activate = draft_activates_automatically(plan.source_profile_name, output_profile) and draft_activates_automatically(
                plan.source_schema_name,
                output_schema,
            )
            try:
                profile_path, schema_path = await coordinator.async_create_smartess_known_family_draft_named(
                    output_profile_name=output_profile,
                    output_schema_name=output_schema,
                    overwrite=overwrite,
                )
            except Exception as exc:
                return await self._async_show_diagnostics_result(
                    action_title=self._diagnostics_result_tr(
                        "smartess_draft_title",
                        "SmartESS Draft",
                    ),
                    status=self._diagnostics_result_tr(
                        "smartess_draft_failed_status",
                        "SmartESS draft creation failed: {error}",
                        {"error": str(exc)},
                    ),
                    next_step=self._diagnostics_result_tr(
                        "smartess_draft_failed_next",
                        "Re-export SmartESS cloud evidence or adjust the output file names, then try again.",
                    ),
                )

            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "smartess_draft_created_title",
                    "SmartESS Draft Created",
                ),
                status=(
                    self._diagnostics_result_tr(
                        "smartess_draft_created_status_active",
                        "SmartESS-derived local profile and schema drafts were created and will override the built-in metadata after reload.",
                    )
                    if auto_activate
                    else self._diagnostics_result_tr(
                        "smartess_draft_created_status_inactive",
                        "SmartESS-derived local profile and schema drafts were created, but they will not override the built-in metadata automatically.",
                    )
                ),
                path=f"{profile_path} ; {schema_path}",
                next_step=(
                    self._diagnostics_result_tr(
                        "smartess_draft_created_next_active",
                        "Reload local metadata and verify the generated SmartESS draft against the live inverter. Create a support archive afterward if you want to share the result.",
                    )
                    if auto_activate
                    else self._diagnostics_result_tr(
                        "smartess_draft_created_next_inactive",
                        "Rename the draft outputs to {profile_name} and {schema_name} if you want them to override the built-in metadata automatically, then reload local metadata.",
                        {
                            "profile_name": plan.source_profile_name,
                            "schema_name": plan.source_schema_name,
                        },
                    )
                ),
            )

        return self.async_show_form(
            step_id="create_smartess_draft",
            data_schema=vol.Schema(
                {
                    vol.Optional("output_profile", default=plan.source_profile_name): _IP_TEXT_SELECTOR,
                    vol.Optional("output_schema", default=plan.source_schema_name): _IP_TEXT_SELECTOR,
                    vol.Required("overwrite", default=True): _BOOLEAN_SELECTOR,
                }
            ),
            description_placeholders={
                "driver_label": plan.driver_label,
                "reason": plan.reason,
                "source_profile": plan.source_profile_name,
                "source_schema": plan.source_schema_name,
                "cloud_evidence_path": self._diagnostics_placeholders()["cloud_evidence_path"],
                "profile_override": self._diagnostics_placeholders()["profile_override_status"],
                "schema_override": self._diagnostics_placeholders()["schema_override_status"],
            },
        )

    @_with_translation_bundle
    async def async_step_create_smartess_smg_bridge(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        plan = getattr(coordinator, "smartess_smg_bridge_plan", None) if coordinator is not None else None
        if coordinator is None or plan is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "smartess_smg_bridge_title",
                    "SmartESS SMG Bridge",
                ),
                status=self._diagnostics_result_tr(
                    "smartess_smg_bridge_unavailable_status",
                    "No safe SmartESS SMG bridge is available for this entry yet.",
                ),
                next_step=self._diagnostics_result_tr(
                    "smartess_smg_bridge_unavailable_next",
                    "Export SmartESS cloud evidence first, or wait until enough SmartESS fields match the active SMG runtime metadata.",
                ),
            )

        if user_input is not None:
            output_profile = str(user_input.get("output_profile") or "").strip() or None
            output_schema = str(user_input.get("output_schema") or "").strip() or None
            overwrite = bool(user_input.get("overwrite", False))
            auto_activate = draft_activates_automatically(plan.source_profile_name, output_profile) and draft_activates_automatically(
                plan.source_schema_name,
                output_schema,
            )
            try:
                profile_path, schema_path = await coordinator.async_create_smartess_smg_bridge_named(
                    output_profile_name=output_profile,
                    output_schema_name=output_schema,
                    overwrite=overwrite,
                )
            except Exception as exc:
                return await self._async_show_diagnostics_result(
                    action_title=self._diagnostics_result_tr(
                        "smartess_smg_bridge_title",
                        "SmartESS SMG Bridge",
                    ),
                    status=self._diagnostics_result_tr(
                        "smartess_smg_bridge_failed_status",
                        "SmartESS SMG bridge creation failed: {error}",
                        {"error": str(exc)},
                    ),
                    next_step=self._diagnostics_result_tr(
                        "smartess_smg_bridge_failed_next",
                        "Re-export SmartESS cloud evidence or adjust the output file names, then try again.",
                    ),
                )

            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "smartess_smg_bridge_created_title",
                    "SmartESS SMG Bridge Created",
                ),
                status=(
                    self._diagnostics_result_tr(
                        "smartess_smg_bridge_created_status_active",
                        "A SmartESS-backed SMG bridge draft was created and will override the built-in metadata after reload.",
                    )
                    if auto_activate
                    else self._diagnostics_result_tr(
                        "smartess_smg_bridge_created_status_inactive",
                        "A SmartESS-backed SMG bridge draft was created, but it will not override the built-in metadata automatically.",
                    )
                ),
                path=f"{profile_path} ; {schema_path}",
                next_step=(
                    self._diagnostics_result_tr(
                        "smartess_smg_bridge_created_next_active",
                        "Reload local metadata and verify that the bridge exposes the additional SMG controls and config readbacks you expect.",
                    )
                    if auto_activate
                    else self._diagnostics_result_tr(
                        "smartess_smg_bridge_created_next_inactive",
                        "Rename the draft outputs to {profile_name} and {schema_name} if you want them to override the built-in metadata automatically, then reload local metadata.",
                        {
                            "profile_name": plan.source_profile_name,
                            "schema_name": plan.source_schema_name,
                        },
                    )
                ),
            )

        return self.async_show_form(
            step_id="create_smartess_smg_bridge",
            data_schema=vol.Schema(
                {
                    vol.Optional("output_profile", default=plan.source_profile_name): _IP_TEXT_SELECTOR,
                    vol.Optional("output_schema", default=plan.source_schema_name): _IP_TEXT_SELECTOR,
                    vol.Required("overwrite", default=True): _BOOLEAN_SELECTOR,
                }
            ),
            description_placeholders={
                "bridge_label": str(getattr(plan, "bridge_label", "SmartESS SMG bridge") or "SmartESS SMG bridge"),
                "reason": plan.reason,
                "source_profile": plan.source_profile_name,
                "source_schema": plan.source_schema_name,
                "matched_controls": str(len(getattr(plan, "profile_enable_keys", ()) or ())),
                "matched_readbacks": str(len(getattr(plan, "measurement_enable_keys", ()) or ())),
                "blocked_fields": str(len(getattr(plan, "blocked_field_titles", ()) or ())),
                "skipped_fields": str(len(getattr(plan, "skipped_field_titles", ()) or ())),
                "cloud_evidence_path": self._diagnostics_placeholders()["cloud_evidence_path"],
                "profile_override": self._diagnostics_placeholders()["profile_override_status"],
                "schema_override": self._diagnostics_placeholders()["schema_override_status"],
            },
        )

    @_with_translation_bundle
    async def async_step_create_profile_draft(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        source_profile_name = coordinator.effective_profile_name if coordinator is not None else ""
        if coordinator is None or not source_profile_name:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "profile_draft_title",
                    "Profile Draft",
                ),
                status=self._diagnostics_result_tr(
                    "profile_draft_unavailable_status",
                    "No detected driver profile is available for this entry.",
                ),
                next_step=self._diagnostics_result_tr(
                    "profile_draft_unavailable_next",
                    "Run detection again or set a manual driver hint before creating a local draft.",
                ),
            )

        if user_input is not None:
            output_profile = str(user_input.get("output_profile") or "").strip() or None
            overwrite = bool(user_input.get("overwrite", False))
            auto_activate = draft_activates_automatically(source_profile_name, output_profile)
            path = await coordinator.async_create_local_profile_draft_named(
                output_profile_name=output_profile,
                overwrite=overwrite,
            )
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "profile_draft_created_title",
                    "Local Profile Draft Created",
                ),
                status=(
                    self._diagnostics_result_tr(
                        "profile_draft_created_status_active",
                        "A local experimental profile draft was created and will override the built-in profile after reload.",
                    )
                    if auto_activate
                    else self._diagnostics_result_tr(
                        "profile_draft_created_status_inactive",
                        "A local experimental profile draft was created, but it will not override the built-in profile automatically.",
                    )
                ),
                path=path,
                next_step=(
                    self._diagnostics_result_tr(
                        "draft_reload_next",
                        "Edit the draft, then reload local metadata to activate it.",
                    )
                    if auto_activate
                    else self._diagnostics_result_tr(
                        "draft_rename_profile_next",
                        "Rename the draft to {name} if you want it to override the built-in profile automatically, then reload local metadata.",
                        {"name": source_profile_name},
                    )
                ),
            )

        data_schema = vol.Schema(
            {
                vol.Optional("output_profile", default=source_profile_name): _IP_TEXT_SELECTOR,
                vol.Required("overwrite", default=True): _BOOLEAN_SELECTOR,
            }
        )
        return self.async_show_form(
            step_id="create_profile_draft",
            data_schema=data_schema,
            description_placeholders={
                "source_profile": source_profile_name,
                "suggested_output": source_profile_name,
                "current_override": self._diagnostics_placeholders()["profile_override_status"],
                "activation_hint": self._tr(
                    "common.dynamic.profile_activation_hint",
                    "Leave the suggested file name unchanged if you want the local draft to override the built-in profile after reload.",
                ),
            },
        )

    @_with_translation_bundle
    async def async_step_create_schema_draft(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        source_schema_name = coordinator.effective_register_schema_name if coordinator is not None else ""
        if coordinator is None or not source_schema_name:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "register_schema_draft_title",
                    "Register Schema Draft",
                ),
                status=self._diagnostics_result_tr(
                    "register_schema_unavailable_status",
                    "No detected register schema is available for this entry.",
                ),
                next_step=self._diagnostics_result_tr(
                    "register_schema_unavailable_next",
                    "Run detection again or set a manual driver hint before creating a local draft.",
                ),
            )

        if user_input is not None:
            output_schema = str(user_input.get("output_schema") or "").strip() or None
            overwrite = bool(user_input.get("overwrite", False))
            auto_activate = draft_activates_automatically(source_schema_name, output_schema)
            path = await coordinator.async_create_local_schema_draft_named(
                output_schema_name=output_schema,
                overwrite=overwrite,
            )
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "register_schema_draft_created_title",
                    "Local Register Schema Draft Created",
                ),
                status=(
                    self._diagnostics_result_tr(
                        "register_schema_draft_created_status_active",
                        "A local experimental register schema draft was created and will override the built-in schema after reload.",
                    )
                    if auto_activate
                    else self._diagnostics_result_tr(
                        "register_schema_draft_created_status_inactive",
                        "A local experimental register schema draft was created, but it will not override the built-in schema automatically.",
                    )
                ),
                path=path,
                next_step=(
                    self._diagnostics_result_tr(
                        "draft_reload_next",
                        "Edit the draft, then reload local metadata to activate it.",
                    )
                    if auto_activate
                    else self._diagnostics_result_tr(
                        "draft_rename_schema_next",
                        "Rename the draft to {name} if you want it to override the built-in schema automatically, then reload local metadata.",
                        {"name": source_schema_name},
                    )
                ),
            )

        data_schema = vol.Schema(
            {
                vol.Optional("output_schema", default=source_schema_name): _IP_TEXT_SELECTOR,
                vol.Required("overwrite", default=True): _BOOLEAN_SELECTOR,
            }
        )
        return self.async_show_form(
            step_id="create_schema_draft",
            data_schema=data_schema,
            description_placeholders={
                "source_schema": source_schema_name,
                "suggested_output": source_schema_name,
                "current_override": self._diagnostics_placeholders()["schema_override_status"],
                "activation_hint": self._tr(
                    "common.dynamic.schema_activation_hint",
                    "Leave the suggested file name unchanged if you want the local draft to override the built-in register schema after reload.",
                ),
            },
        )

    @_with_translation_bundle
    async def async_step_diagnostics_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if user_input is not None:
            return await self.async_step_diagnostics()

        return self.async_show_form(
            step_id="diagnostics_result",
            data_schema=vol.Schema({}),
            description_placeholders=self._diagnostics_result,
        )

    def _control_summary(self, *, control_mode: str, confidence: str) -> str:
        if control_mode == CONTROL_MODE_FULL:
            return self._tr("common.dynamic.control_full", "All controls are enabled.")
        if control_mode == CONTROL_MODE_READ_ONLY:
            return self._tr(
                "common.dynamic.control_read_only",
                "Monitoring only — no control entities are exposed.",
            )
        if confidence == "high":
            return self._tr(
                "common.dynamic.control_auto",
                "Tested controls are enabled automatically.",
            )
        return self._tr(
            "common.dynamic.control_waiting",
            "Monitoring only until a high-confidence detection is confirmed.",
        )

    def _confidence_label(self, confidence: str) -> str:
        return {
            "high": self._tr("common.dynamic.confidence_high", "High confidence"),
            "medium": self._tr("common.dynamic.confidence_medium", "Medium confidence"),
            "low": self._tr("common.dynamic.confidence_low", "Low confidence"),
            "none": self._tr("common.dynamic.confidence_none", "No confidence"),
        }.get(confidence, confidence)

    def _coordinator(self):
        return getattr(self._config_entry, "runtime_data", None)

    def _metadata_source_summary(self, metadata) -> str:
        if metadata is None:
            return self._tr("common.dynamic.not_available", "Not available")
        source_path = getattr(metadata, "source_path", "") or self._tr(
            "common.dynamic.unknown_path", "Unknown path"
        )
        source_scope = getattr(metadata, "source_scope", "") or "unknown"
        if source_scope == "builtin":
            return self._tr(
                "common.dynamic.built_in_metadata",
                "Built-in metadata ({path})",
                {"path": source_path},
            )
        if source_scope == "external":
            return self._tr(
                "common.dynamic.local_override",
                "Local override ({path})",
                {"path": source_path},
            )
        return self._tr(
            "common.dynamic.external_metadata",
            "External metadata ({path})",
            {"path": source_path},
        )

    def _diagnostics_menu_options(self, primary_action: str) -> list[str]:
        coordinator = self._coordinator()
        rollback_paths = self._local_metadata_rollback_paths()
        menu_options: list[str] = ["create_support_package"]

        if primary_action == "reload_local_metadata":
            menu_options.append("reload_local_metadata")

        if rollback_paths.paths and "rollback_local_metadata" not in menu_options:
            menu_options.append("rollback_local_metadata")

        if (
            coordinator is not None
            and bool(getattr(coordinator, "smartess_cloud_export_available", False))
            and "export_smartess_cloud_evidence" not in menu_options
        ):
            menu_options.append("export_smartess_cloud_evidence")

        menu_options.append("advanced_metadata")
        return menu_options

    def _current_cloud_evidence_path(self, coordinator=None) -> str:
        """Return the latest SmartESS cloud evidence path visible to diagnostics."""

        coordinator = coordinator or self._coordinator()
        if coordinator is None:
            return ""

        live_path = str(getattr(coordinator, "smartess_cloud_evidence_path", "") or "").strip()
        if live_path:
            return live_path

        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}
        return str(values.get("cloud_evidence_path") or "").strip()

    def _default_support_archive_cloud_mode(self, *, had_saved_cloud_evidence: bool) -> str:
        if had_saved_cloud_evidence:
            return SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED
        return SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY

    def _support_archive_cloud_mode_label(self, archive_cloud_mode: str) -> str:
        return {
            SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED: self._tr(
                "common.dynamic.support_archive_cloud_mode_use_saved",
                "Use saved SmartESS cloud evidence",
            ),
            SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH: self._tr(
                "common.dynamic.support_archive_cloud_mode_refresh",
                "Fetch or refresh SmartESS cloud evidence now",
            ),
            SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY: self._tr(
                "common.dynamic.support_archive_cloud_mode_archive_only",
                "Create the archive without SmartESS cloud evidence",
            ),
        }.get(archive_cloud_mode, archive_cloud_mode)

    def _support_archive_cloud_mode_selector(
        self,
        *,
        had_saved_cloud_evidence: bool,
    ) -> SelectSelector:
        options: list[SelectOptionDict] = []
        if had_saved_cloud_evidence:
            options.append(
                SelectOptionDict(
                    value=SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
                    label=self._support_archive_cloud_mode_label(
                        SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
                    ),
                )
            )
        else:
            options.append(
                SelectOptionDict(
                    value=SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY,
                    label=self._support_archive_cloud_mode_label(
                        SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY,
                    ),
                )
            )
        options.append(
            SelectOptionDict(
                value=SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                label=self._support_archive_cloud_mode_label(
                    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                ),
            )
        )
        return SelectSelector(
            SelectSelectorConfig(
                options=options,
                mode=SelectSelectorMode.DROPDOWN,
            )
        )

    def _support_archive_cloud_plan_summary(
        self,
        *,
        had_saved_cloud_evidence: bool,
        can_refresh_cloud_evidence: bool,
    ) -> str:
        if had_saved_cloud_evidence and can_refresh_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_plan_saved_refreshable",
                "Saved SmartESS cloud evidence will be included automatically, or you can refresh it in this same step before the archive is built.",
            )
        if had_saved_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_plan_saved_only",
                "Saved SmartESS cloud evidence will be included automatically when it matches this entry.",
            )
        if can_refresh_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_plan_refreshable",
                "No SmartESS cloud evidence is saved yet. You can fetch it in this step and include it in the same archive, or continue without it.",
            )
        return self._tr(
            "common.dynamic.support_archive_cloud_plan_unavailable",
            "No SmartESS cloud evidence is currently available for this entry.",
        )

    def _support_archive_cloud_result_detail(
        self,
        *,
        archive_cloud_mode: str,
        had_saved_cloud_evidence: bool,
    ) -> str:
        if archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH:
            return self._tr(
                "common.dynamic.support_archive_cloud_result_refreshed",
                "Fresh SmartESS cloud evidence was fetched in this step and included in the archive.",
            )
        if had_saved_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_result_saved",
                "Saved SmartESS cloud evidence was included in the archive.",
            )
        return self._tr(
            "common.dynamic.support_archive_cloud_result_none",
            "No SmartESS cloud evidence was included in the archive.",
        )

    def _show_create_support_package_form(
        self,
        *,
        coordinator,
        saved_cloud_evidence_path: str,
        user_input: dict[str, Any] | None = None,
        errors: dict[str, str] | None = None,
    ) -> ConfigFlowResult:
        had_saved_cloud_evidence = bool(saved_cloud_evidence_path)
        can_refresh_cloud_evidence = bool(
            getattr(coordinator, "smartess_cloud_export_available", False)
        )
        defaults = {
            CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE: str(
                (user_input or {}).get(CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE)
                or self._default_support_archive_cloud_mode(
                    had_saved_cloud_evidence=had_saved_cloud_evidence,
                )
            ),
            "username": str((user_input or {}).get("username") or ""),
            "password": str((user_input or {}).get("password") or ""),
        }
        not_available = self._tr("common.dynamic.not_available", "Not available")
        not_created_yet = self._tr("common.dynamic.not_created_yet", "Not created yet")
        return self.async_show_form(
            step_id="create_support_package",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE,
                        default=defaults[CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE],
                    ): self._support_archive_cloud_mode_selector(
                        had_saved_cloud_evidence=had_saved_cloud_evidence,
                    ),
                    vol.Optional("username", default=defaults["username"]): _IP_TEXT_SELECTOR,
                    vol.Optional("password", default=defaults["password"]): _PASSWORD_TEXT_SELECTOR,
                }
            ),
            errors=errors or {},
            description_placeholders={
                "collector_pn": str(
                    getattr(coordinator, "smartess_collector_pn", "") or not_available
                ),
                "cloud_evidence_path": saved_cloud_evidence_path or not_created_yet,
                "smartess_archive_plan_summary": self._support_archive_cloud_plan_summary(
                    had_saved_cloud_evidence=had_saved_cloud_evidence,
                    can_refresh_cloud_evidence=can_refresh_cloud_evidence,
                ),
                "refresh_mode_label": self._support_archive_cloud_mode_label(
                    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                ),
            },
        )

    def _smartess_cloud_diagnostics_hint(self) -> str:
        coordinator = self._coordinator()
        if coordinator is None or not bool(getattr(coordinator, "smartess_cloud_export_available", False)):
            return ""

        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}
        cloud_evidence_path = str(values.get("cloud_evidence_path") or "").strip()

        if getattr(coordinator, "smartess_smg_bridge_plan", None) is not None:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_bridge",
                "Current SmartESS cloud evidence is ready to generate a SmartESS SMG bridge for this runtime.",
            )
        elif getattr(coordinator, "smartess_known_family_draft_plan", None) is not None:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_draft",
                "Current SmartESS cloud evidence is ready to generate a SmartESS draft for this runtime.",
            )
        elif cloud_evidence_path:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_refresh",
                "SmartESS cloud evidence is already saved for this entry and can be refreshed after app-side changes.",
            )
        else:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_available",
                "SmartESS cloud evidence is available for this entry even if local detection is already high-confidence.",
            )

        return self._tr(
            "common.dynamic.smartess_cloud_diagnostics_hint",
            "**SmartESS cloud:** {detail} It can still refine local metadata or re-enable bridge-backed entities for an existing device. The visible entity count may stay the same when existing entities are upgraded instead of creating new IDs. **Create support archive** can include saved cloud evidence directly and can refresh it inline before the ZIP is built. Use **Export SmartESS cloud evidence** only when you need the evidence itself for Advanced metadata tools or for standalone review.",
            {"detail": detail},
        )

    def _smartess_cloud_exported_next_step(self) -> str:
        coordinator = self._coordinator()
        if coordinator is not None and getattr(coordinator, "smartess_smg_bridge_plan", None) is not None:
            return self._diagnostics_result_tr(
                "smartess_cloud_evidence_exported_next_bridge",
                "Open Advanced metadata tools to create the SmartESS SMG bridge, then reload local metadata to apply it. If you only need the evidence, create a support archive instead.",
            )
        if coordinator is not None and getattr(coordinator, "smartess_known_family_draft_plan", None) is not None:
            return self._diagnostics_result_tr(
                "smartess_cloud_evidence_exported_next_draft",
                "Open Advanced metadata tools to create the SmartESS draft, then reload local metadata to apply it. If you only need the evidence, create a support archive instead.",
            )
        return self._diagnostics_result_tr(
            "smartess_cloud_evidence_exported_next",
            "Open Advanced metadata tools to review what can be generated from this evidence. If local overrides already exist, reload local metadata there after updating them, or create a support archive to share the evidence with the developer.",
        )

    def _support_action_label(self, action: str) -> str:
        return {
            "create_support_package": self._tr(
                "common.dynamic.action_create_support_package",
                "Create support archive",
            ),
            "export_smartess_cloud_evidence": self._tr(
                "common.dynamic.action_export_smartess_cloud_evidence",
                "Export SmartESS cloud evidence",
            ),
            "export_support_bundle": self._tr(
                "common.dynamic.action_export_support_bundle",
                "Export support bundle",
            ),
            "create_smartess_draft": self._tr(
                "common.dynamic.action_create_smartess_draft",
                "Create SmartESS draft",
            ),
            "create_smartess_smg_bridge": self._tr(
                "common.dynamic.action_create_smartess_smg_bridge",
                "Create SmartESS SMG bridge",
            ),
            "reload_local_metadata": self._tr(
                "common.dynamic.action_reload_local_metadata",
                "Reload local metadata",
            ),
            "rollback_local_metadata": self._tr(
                "common.dynamic.action_rollback_local_metadata",
                "Rollback local metadata",
            ),
            "create_profile_draft": self._tr(
                "common.dynamic.action_create_profile_draft",
                "Create local profile draft",
            ),
            "create_schema_draft": self._tr(
                "common.dynamic.action_create_schema_draft",
                "Create local register schema draft",
            ),
            "advanced_metadata": self._tr(
                "common.dynamic.action_advanced_metadata",
                "Advanced metadata tools",
            ),
        }.get(action, action)

    def _local_metadata_rollback_paths(self):
        coordinator = self._coordinator()
        return resolve_local_metadata_rollback_paths(
            config_dir=Path(self.hass.config.config_dir),
            profile_name=(getattr(coordinator, "effective_profile_name", "") or None),
            schema_name=(getattr(coordinator, "effective_register_schema_name", "") or None),
            profile_metadata=getattr(coordinator, "effective_profile_metadata", None),
            schema_metadata=getattr(coordinator, "effective_register_schema_metadata", None),
        )

    def _support_workflow_translation_key(self, level: str, field: str) -> str:
        return f"common.dynamic.support_workflow_{level}_{field}"

    def _diagnostics_result_tr(
        self,
        field: str,
        default: str,
        placeholders: dict[str, Any] | None = None,
    ) -> str:
        return self._tr(
            f"common.dynamic.diagnostics_result_{field}",
            default,
            placeholders,
        )

    def _localized_support_workflow(self, values: dict[str, Any]) -> dict[str, str]:
        level = str(values.get("support_workflow_level") or "unknown")
        primary_action = str(values.get("support_workflow_primary_action") or "create_support_package")
        step_1 = self._tr(
            self._support_workflow_translation_key(level, "step_1"),
            str(values.get("support_workflow_step_1") or "Run the primary diagnostics action."),
        )
        step_2 = self._tr(
            self._support_workflow_translation_key(level, "step_2"),
            str(values.get("support_workflow_step_2") or "Send the ZIP file to the developer."),
        )
        step_3 = self._tr(
            self._support_workflow_translation_key(level, "step_3"),
            str(values.get("support_workflow_step_3") or "Use advanced metadata tools only if requested."),
        )
        return {
            "support_workflow_level": level,
            "support_workflow_level_label": self._tr(
                self._support_workflow_translation_key(level, "level_label"),
                str(values.get("support_workflow_level_label") or "Unknown support"),
            ),
            "support_workflow_summary": self._tr(
                self._support_workflow_translation_key(level, "summary"),
                str(values.get("support_workflow_summary") or "Support status is not available yet."),
            ),
            "support_workflow_next_action": self._tr(
                self._support_workflow_translation_key(level, "next_action"),
                str(values.get("support_workflow_next_action") or "Run detection or create a support archive when the inverter is available."),
            ),
            "support_workflow_step_1": step_1,
            "support_workflow_step_2": step_2,
            "support_workflow_step_3": step_3,
            "support_workflow_plan": self._tr(
                "common.dynamic.plan_template",
                "Step 1: {step_1} Step 2: {step_2} Step 3: {step_3}",
                {"step_1": step_1, "step_2": step_2, "step_3": step_3},
            ),
            "support_workflow_advanced_hint": self._tr(
                self._support_workflow_translation_key(level, "advanced_hint"),
                str(values.get("support_workflow_advanced_hint") or "Advanced metadata tools are secondary and should be used only after the primary support path is complete."),
            ),
            "support_workflow_primary_action": primary_action,
            "support_workflow_primary_action_label": self._support_action_label(primary_action),
        }

    def _diagnostics_placeholders(self) -> dict[str, str]:
        coordinator = self._coordinator()
        values = coordinator.data.values if coordinator is not None else {}
        effective_owner_name = coordinator.effective_owner_name if coordinator is not None else ""
        effective_owner_key = coordinator.effective_owner_key if coordinator is not None else ""
        smartess_family_name = coordinator.smartess_family_name if coordinator is not None else ""
        effective_profile_name = coordinator.effective_profile_name if coordinator is not None else ""
        effective_register_schema_name = (
            coordinator.effective_register_schema_name if coordinator is not None else ""
        )
        profile_metadata = coordinator.effective_profile_metadata if coordinator is not None else None
        register_schema_metadata = (
            coordinator.effective_register_schema_metadata if coordinator is not None else None
        )
        config_dir = Path(self.hass.config.config_dir)
        profile_override = local_profile_override_details(
            config_dir,
            effective_profile_name or None,
        )
        schema_override = local_register_schema_override_details(
            config_dir,
            effective_register_schema_name or None,
        )
        placeholders = {
            "model_name": self._config_entry.data.get(
                CONF_DETECTED_MODEL,
                self._tr("common.dynamic.unknown", "Unknown"),
            ),
            "serial_number": self._config_entry.data.get(
                CONF_DETECTED_SERIAL,
                self._tr("common.dynamic.unknown", "Unknown"),
            ),
            "effective_owner_name": effective_owner_name or self._tr("common.dynamic.not_available", "Not available"),
            "effective_owner_key": effective_owner_key or self._tr("common.dynamic.not_available", "Not available"),
            "smartess_family_name": smartess_family_name,
            "smartess_family_line": (
                self._tr(
                    "common.dynamic.smartess_family_line",
                    "\n**SmartESS family:** {family}",
                    {"family": smartess_family_name},
                )
                if smartess_family_name
                else ""
            ),
            "profile_name": effective_profile_name or self._tr("common.dynamic.not_available", "Not available"),
            "register_schema_name": effective_register_schema_name or self._tr("common.dynamic.not_available", "Not available"),
            "support_archive_action_label": self._support_action_label("create_support_package"),
            "effective_profile_source": self._metadata_source_summary(profile_metadata),
            "effective_schema_source": self._metadata_source_summary(register_schema_metadata),
            "profile_override_status": profile_override["status"],
            "schema_override_status": schema_override["status"],
            "suggested_profile_output": effective_profile_name or self._tr("common.dynamic.not_available", "Not available"),
            "suggested_schema_output": effective_register_schema_name or self._tr("common.dynamic.not_available", "Not available"),
            "support_package_path": str(values.get("support_package_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "support_package_download_url": str(values.get("support_package_download_url") or ""),
            "support_package_download_markdown": (
                self._tr(
                    "common.dynamic.download_support_archive",
                    "[Download support archive]({url})",
                    {"url": values["support_package_download_url"]},
                )
                if values.get("support_package_download_url")
                else self._tr("common.dynamic.not_available_yet", "Not available yet")
            ),
            "support_bundle_path": str(values.get("support_bundle_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "cloud_evidence_path": self._current_cloud_evidence_path(coordinator)
            or self._tr("common.dynamic.not_created_yet", "Not created yet"),
            "local_profile_draft_path": str(values.get("local_profile_draft_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "local_schema_draft_path": str(values.get("local_schema_draft_path") or self._tr("common.dynamic.not_created_yet", "Not created yet")),
            "local_metadata_status": str(values.get("local_metadata_status") or self._tr("common.dynamic.no_diagnostics_action", "No diagnostics action has been run yet.")),
            "smartess_cloud_diagnostics_hint": self._smartess_cloud_diagnostics_hint(),
        }
        placeholders.update(self._localized_support_workflow(values))
        return placeholders

    async def _async_show_diagnostics_result(
        self,
        *,
        action_title: str,
        status: str,
        path: str = "",
        download_url: str = "",
        next_step: str = "",
    ) -> ConfigFlowResult:
        self._diagnostics_result = {
            "action_title": action_title,
            "status": status,
            "path": path or self._tr("common.dynamic.not_applicable", "Not applicable"),
            "download_url": download_url or "",
            "download_markdown": (
                self._tr(
                    "common.dynamic.download_file",
                    "[Download file]({url})",
                    {"url": download_url},
                )
                if download_url
                else self._tr("common.dynamic.not_available", "Not available")
            ),
            "next_step": next_step
            or self._tr(
                "common.dynamic.return_to_diagnostics",
                "Return to diagnostics to run another action.",
            ),
        }
        return await self.async_step_diagnostics_result()
