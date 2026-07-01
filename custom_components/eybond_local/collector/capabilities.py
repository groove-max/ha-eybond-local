"""Collector kind and capability profile helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..const import (
    COLLECTOR_OPERATION_HA_ONLY,
    COLLECTOR_OPERATION_SMARTESS_AND_HA,
)


COLLECTOR_KIND_FACTORY_EYBOND = "factory_eybond"
COLLECTOR_KIND_ESP_EYBOND_BRIDGE = "esp_eybond_bridge"

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
    allowed_operation_modes: tuple[str, ...]
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
        return self.allowed_operation_modes == (COLLECTOR_OPERATION_HA_ONLY,)


FACTORY_COLLECTOR_CAPABILITIES = CollectorCapabilityProfile(
    collector_kind=COLLECTOR_KIND_FACTORY_EYBOND,
    virtual_bridge=False,
    allowed_operation_modes=(
        COLLECTOR_OPERATION_SMARTESS_AND_HA,
        COLLECTOR_OPERATION_HA_ONLY,
    ),
    cloud_profile_key="",
    cloud_evidence=True,
    proxy_capture=True,
    shadow_learning=True,
    wifi_management=True,
    uart_management=False,
    uart_runtime_speed_change=False,
    identity_probe="",
)


def collector_capability_profile(
    *,
    virtual_bridge: bool = False,
    cloud_profile_key: object = "",
    hardware_version: object = "",
) -> CollectorCapabilityProfile:
    """Return collector capabilities for a normalized collector kind."""

    if not virtual_bridge:
        return FACTORY_COLLECTOR_CAPABILITIES

    hardware = str(hardware_version or "").strip().lower()
    runtime_uart_available = not any(
        marker in hardware for marker in _RUNTIME_UART_UNAVAILABLE_HARDWARE_MARKERS
    )
    return CollectorCapabilityProfile(
        collector_kind=COLLECTOR_KIND_ESP_EYBOND_BRIDGE,
        virtual_bridge=True,
        allowed_operation_modes=(COLLECTOR_OPERATION_HA_ONLY,),
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

    # Default-to-factory is intentional, not fail-open: a factory collector has
    # no positive "I am factory" signal — it is the ABSENCE of the hardware
    # token. The bridge is detected from FC=2 parameter 6
    # (collector_hardware_version="esp-collector/<version>/<platform...>") and
    # persisted to entry data/options below. The OR over all persisted signals
    # keeps already-created entries stable when a transient runtime read is
    # missing. The cloud-only flows this profile gates are all additionally
    # user-initiated and no-op on a local-only bridge.
    runtime_values = values or {}
    entry_data = data or {}
    entry_options = options or {}
    resolved_hardware = hardware_version or runtime_values.get("collector_hardware_version", "")
    hardware_token = parse_esp_collector_hardware_token(resolved_hardware)
    is_bridge = bool(
        getattr(collector, "collector_virtual_bridge", False)
        or runtime_values.get("collector_virtual_bridge")
        or hardware_token.is_bridge
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
    return collector_capability_profile(
        virtual_bridge=is_bridge,
        cloud_profile_key=profile_key,
        hardware_version=resolved_hardware,
    )
