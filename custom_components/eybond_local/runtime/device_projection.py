"""Pure Home Assistant device-info payload projections.

The coordinator remains the authority for registry reads/writes, stale-device
removal and deduplication.  These helpers only calculate the desired metadata
mapping from already-selected runtime and entry context.
"""

from __future__ import annotations

from collections.abc import Mapping

from ..collector.capabilities import collector_capability_profile_from_runtime
from ..const import DOMAIN
from ..naming import collector_display_name


def build_inverter_device_info_payload(
    *,
    entry_id: str,
    entry_title: str,
    detected_model: object,
    detected_serial: object,
    inverter: object | None,
) -> dict[str, object]:
    """Build stable metadata for the inverter-side HA device."""

    name = "EyeBond Inverter"
    model = None
    serial_number = None
    persisted_model = str(detected_model or "").strip()
    persisted_serial = str(detected_serial or "").strip()
    runtime_model = str(getattr(inverter, "model_name", "") or "").strip()
    runtime_serial = str(getattr(inverter, "serial_number", "") or "").strip()

    if runtime_model or runtime_serial:
        name = runtime_model or persisted_model or name
        model = runtime_model or persisted_model or None
        serial_number = runtime_serial or persisted_serial or None
    else:
        if persisted_model:
            name = persisted_model
            model = persisted_model
        elif entry_title:
            name = entry_title
        if persisted_serial:
            serial_number = persisted_serial

    payload: dict[str, object] = {
        "identifiers": {(DOMAIN, entry_id)},
        "name": name,
        "manufacturer": "OEM / EyeBond",
        "via_device": (DOMAIN, f"{entry_id}:collector"),
    }
    if model:
        payload["model"] = model
    if serial_number:
        payload["serial_number"] = serial_number
    return payload


def build_collector_device_info_payload(
    *,
    entry_id: str,
    collector_ip: object,
    collector_pn: object,
    collector: object | None,
    values: Mapping[str, object] | None,
    entry_data: Mapping[str, object] | None,
    entry_options: Mapping[str, object] | None,
) -> dict[str, object]:
    """Build stable metadata for the collector-side HA device."""

    runtime_values = dict(values or {})
    data = dict(entry_data or {})
    options = dict(entry_options or {})
    model = "EyeBond Collector"
    serial_number = str(collector_pn or "").strip()
    normalized_collector_ip = str(collector_ip or "").strip()
    sw_version = ""
    hw_version = str(runtime_values.get("collector_hardware_version") or "").strip()
    collector_type = str(runtime_values.get("collector_type") or "").strip()

    manufacturer = ""
    configuration_url = ""
    profile = collector_capability_profile_from_runtime(
        collector=collector,
        values=runtime_values,
        data=data,
        options=options,
    )

    if collector is not None:
        if collector_type:
            model = collector_type
        elif collector.profile_name:
            model = collector.profile_name
        elif collector.smartess_protocol_name:
            model = collector.smartess_protocol_name
        elif collector.smartess_protocol_asset_name:
            model = collector.smartess_protocol_asset_name
        if collector.smartess_collector_version:
            sw_version = collector.smartess_collector_version
    elif collector_type:
        model = collector_type

    if profile.virtual_bridge:
        manufacturer = "ESP EyeBond Collector (community)"
        model = "ESP EyeBond Collector"
        bridge_version = str(
            getattr(collector, "collector_bridge_version", "")
            or runtime_values.get("collector_bridge_version")
            or options.get("collector_bridge_version")
            or data.get("collector_bridge_version")
            or ""
        ).strip()
        if bridge_version:
            sw_version = bridge_version
        configuration_url = "https://github.com/groove-max/esp-eybond-collector"

    payload: dict[str, object] = {
        "identifiers": {(DOMAIN, f"{entry_id}:collector")},
        "name": collector_display_name(
            collector_pn=serial_number,
            collector_ip=normalized_collector_ip,
        ),
        "model": model,
    }
    if manufacturer:
        payload["manufacturer"] = manufacturer
    if serial_number:
        payload["serial_number"] = serial_number
    if sw_version:
        payload["sw_version"] = sw_version
    if hw_version:
        payload["hw_version"] = hw_version
    if configuration_url:
        payload["configuration_url"] = configuration_url
    return payload


__all__ = [
    "build_collector_device_info_payload",
    "build_inverter_device_info_payload",
]
