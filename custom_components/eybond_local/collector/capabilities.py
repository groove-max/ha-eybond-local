"""Collector kind and capability profile helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

COLLECTOR_KIND_UNKNOWN = "unknown"
COLLECTOR_KIND_FACTORY_EYBOND = "factory_eybond"
COLLECTOR_KIND_ESP_EYBOND_BRIDGE = "esp_eybond_bridge"
COLLECTOR_KIND_ENTRY_KEY = "collector_kind"
COLLECTOR_HARDWARE_VERSION_ENTRY_KEY = "collector_hardware_version"

_RUNTIME_UART_UNAVAILABLE_HARDWARE_MARKERS = (
    "bk72",
    "bk723",
    "rtl87",
    "libretiny",
)
_ESP_COLLECTOR_HARDWARE_PREFIX = "esp-collector/"


@dataclass(frozen=True, slots=True)
class EspCollectorHardwareToken:
    """Parsed ESP EyeBond Collector hardware-version token."""

    is_bridge: bool = False
    version: str = ""
    platform: str = ""


def parse_esp_collector_hardware_token(raw: object) -> EspCollectorHardwareToken:
    """Parse ``esp-collector/<version>/<platform...>`` hardware tokens defensively."""

    text = str(raw or "").strip()
    if not text.lower().startswith(_ESP_COLLECTOR_HARDWARE_PREFIX):
        return EspCollectorHardwareToken()

    remainder = text[len(_ESP_COLLECTOR_HARDWARE_PREFIX) :].strip()
    version, _separator, platform = remainder.partition("/")
    return EspCollectorHardwareToken(
        is_bridge=True,
        version=version.strip(),
        platform=platform.strip(),
    )


@dataclass(frozen=True, slots=True)
class CollectorCapabilityProfile:
    """One normalized collector kind/capability profile."""

    collector_kind: str
    virtual_bridge: bool
    cloud_connection_supported: bool
    cloud_profile_key: str
    cloud_evidence: bool
    proxy_capture: bool
    shadow_learning: bool
    wifi_management: bool
    uart_management: bool
    uart_runtime_speed_change: bool
    identity_probe: str

    @property
    def ha_only_required(self) -> bool:
        """Return whether the collector has no supported vendor-cloud side."""

        return not self.cloud_connection_supported


FACTORY_COLLECTOR_CAPABILITIES = CollectorCapabilityProfile(
    collector_kind=COLLECTOR_KIND_FACTORY_EYBOND,
    virtual_bridge=False,
    cloud_connection_supported=True,
    cloud_profile_key="",
    cloud_evidence=True,
    proxy_capture=True,
    shadow_learning=True,
    wifi_management=True,
    uart_management=False,
    uart_runtime_speed_change=False,
    identity_probe="",
)

UNKNOWN_COLLECTOR_CAPABILITIES = CollectorCapabilityProfile(
    collector_kind=COLLECTOR_KIND_UNKNOWN,
    virtual_bridge=False,
    cloud_connection_supported=False,
    cloud_profile_key="",
    cloud_evidence=False,
    proxy_capture=False,
    shadow_learning=False,
    wifi_management=True,
    uart_management=False,
    uart_runtime_speed_change=False,
    identity_probe="collector_hardware_version",
)


def collector_capability_profile(
    *,
    virtual_bridge: bool = False,
    collector_kind: object = "",
    cloud_profile_key: object = "",
    hardware_version: object = "",
) -> CollectorCapabilityProfile:
    """Return collector capabilities for a normalized collector kind."""

    normalized_kind = str(collector_kind or "").strip()
    if normalized_kind == COLLECTOR_KIND_UNKNOWN:
        return UNKNOWN_COLLECTOR_CAPABILITIES

    if not virtual_bridge:
        return FACTORY_COLLECTOR_CAPABILITIES

    hardware = str(hardware_version or "").strip().lower()
    runtime_uart_available = not any(
        marker in hardware for marker in _RUNTIME_UART_UNAVAILABLE_HARDWARE_MARKERS
    )
    return CollectorCapabilityProfile(
        collector_kind=COLLECTOR_KIND_ESP_EYBOND_BRIDGE,
        virtual_bridge=True,
        cloud_connection_supported=False,
        cloud_profile_key=str(cloud_profile_key or "local_only").strip() or "local_only",
        cloud_evidence=False,
        proxy_capture=False,
        shadow_learning=False,
        wifi_management=True,
        uart_management=True,
        uart_runtime_speed_change=runtime_uart_available,
        identity_probe="collector_hardware_version",
    )


def collector_capability_profile_from_runtime(
    *,
    collector: object | None = None,
    values: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
    hardware_version: object = "",
) -> CollectorCapabilityProfile:
    """Build one collector capability profile from runtime/config evidence."""

    # Runtime resolution is fail-closed for pending/manual collectors.  Factory
    # capability is granted only by a persisted collector kind or a real
    # inverter identity.  A driver hint/key alone is not identity: it may be a
    # fallback route while the collector is still unbound.
    runtime_values = values or {}
    entry_data = data or {}
    entry_options = options or {}
    resolved_hardware = (
        hardware_version
        or runtime_values.get("collector_hardware_version", "")
        or entry_options.get(COLLECTOR_HARDWARE_VERSION_ENTRY_KEY, "")
        or entry_data.get(COLLECTOR_HARDWARE_VERSION_ENTRY_KEY, "")
    )
    hardware_token = parse_esp_collector_hardware_token(resolved_hardware)
    persisted_kind = str(
        entry_options.get(COLLECTOR_KIND_ENTRY_KEY)
        or entry_data.get(COLLECTOR_KIND_ENTRY_KEY)
        or ""
    ).strip()
    is_bridge = bool(
        getattr(collector, "collector_virtual_bridge", False)
        or runtime_values.get("collector_virtual_bridge")
        or hardware_token.is_bridge
        or persisted_kind == COLLECTOR_KIND_ESP_EYBOND_BRIDGE
        or entry_data.get("collector_virtual_bridge")
        or entry_options.get("collector_virtual_bridge")
    )
    profile_key = (
        getattr(collector, "collector_cloud_profile_key", "")
        or runtime_values.get("collector_cloud_profile_key")
        or entry_data.get("collector_cloud_profile_key")
        or entry_options.get("collector_cloud_profile_key")
        or ("local_only" if is_bridge else "")
    )
    has_inverter_identity = bool(
        entry_data.get("detected_model")
        or entry_data.get("detected_serial")
        or entry_options.get("detected_model")
        or entry_options.get("detected_serial")
        or runtime_values.get("model_name")
        or runtime_values.get("serial_number")
    )
    is_known_factory = bool(
        persisted_kind == COLLECTOR_KIND_FACTORY_EYBOND
        or (
            persisted_kind in {"", COLLECTOR_KIND_UNKNOWN}
            and has_inverter_identity
            and not is_bridge
        )
    )
    if not is_bridge and not is_known_factory:
        return collector_capability_profile(collector_kind=COLLECTOR_KIND_UNKNOWN)
    return collector_capability_profile(
        virtual_bridge=is_bridge,
        collector_kind=COLLECTOR_KIND_ESP_EYBOND_BRIDGE if is_bridge else COLLECTOR_KIND_FACTORY_EYBOND,
        cloud_profile_key=profile_key,
        hardware_version=resolved_hardware,
    )


def collector_profile_entry_fields(
    profile: CollectorCapabilityProfile,
    *,
    hardware_version: object = "",
) -> dict[str, object]:
    """Return durable config-entry fields for one collector profile.

    ``collector_kind`` is the normalized source of truth.  The older
    ``collector_virtual_bridge`` / ``collector_bridge_*`` fields are kept as
    compatibility evidence for existing code and support packages.
    """

    # ``unknown`` means that classification has not happened yet; persisting it
    # would turn an absence of evidence into a sticky negative fact and block
    # runtime promotion after the inverter is identified. Only positive kinds
    # cross the config-entry boundary.
    if profile.collector_kind == COLLECTOR_KIND_UNKNOWN:
        return {}

    fields: dict[str, object] = {COLLECTOR_KIND_ENTRY_KEY: profile.collector_kind}
    hardware = str(hardware_version or "").strip()
    if hardware:
        fields[COLLECTOR_HARDWARE_VERSION_ENTRY_KEY] = hardware
    if not profile.virtual_bridge:
        return fields

    token = parse_esp_collector_hardware_token(hardware)
    fields["collector_virtual_bridge"] = True
    fields["collector_bridge_kind"] = "esp-collector"
    if token.version:
        fields["collector_bridge_version"] = token.version
    return fields


def collector_capability_profile_from_entry(
    data: dict[str, Any] | None,
    options: dict[str, Any] | None,
) -> CollectorCapabilityProfile:
    """Resolve collector capabilities only from persisted entry fields."""

    return collector_capability_profile_from_runtime(
        data=data or {},
        options=options or {},
        hardware_version=(
            (options or {}).get(COLLECTOR_HARDWARE_VERSION_ENTRY_KEY)
            or (data or {}).get(COLLECTOR_HARDWARE_VERSION_ENTRY_KEY)
            or ""
        ),
    )
