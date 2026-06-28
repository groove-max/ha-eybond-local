"""Collector kind and capability profile helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .at_runtime import parse_collector_vdtu
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
        identity_probe="AT+VDTU",
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
    # no positive "I am factory" signal — it is the ABSENCE of a bridge signal,
    # and collectors with older firmware that never answer AT+VDTU must behave
    # as factory (documented backward-compat). The bridge is detected once at
    # onboarding and persisted to entry data/options below, and the OR over all
    # signals means a known bridge never flips back to factory on a transient
    # missing runtime signal. The cloud-only flows this profile gates are all
    # additionally user-initiated and no-op on a bridge.
    runtime_values = values or {}
    entry_data = data or {}
    entry_options = options or {}
    bridge_from_vdtu = parse_collector_vdtu(runtime_values.get("collector_vdtu_raw"))
    is_bridge = bool(
        getattr(collector, "collector_virtual_bridge", False)
        or runtime_values.get("collector_virtual_bridge")
        or bridge_from_vdtu.is_virtual_bridge
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
    resolved_hardware = hardware_version or runtime_values.get("collector_hardware_version", "")
    return collector_capability_profile(
        virtual_bridge=is_bridge,
        cloud_profile_key=profile_key,
        hardware_version=resolved_hardware,
    )
