"""HA Bluetooth (local or proxy) admission and support export for BLE inspection."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from pathlib import Path

from ..collector.smartess_ble import (
    BleakSmartEssBleLink, SmartEssBleSession, VENDOR_LAYOUT, normalize_ble_uuid,
    normalize_discovered_candidate,
)
from ..collector_identity import validated_collector_pn
from ..const import CONF_COLLECTOR_PN, DOMAIN
from ..support.ble_inspection import BleInspection
from ..support.diagnostic_evidence import diagnostic_evidence_scope
from ..support.diagnostic_export import export_diagnostic_run
from ..support.download import async_register_download_views, sign_diagnostic_run_download_url
from ..support.owned_worker import async_finish_owned_task, async_run_owned_worker


_ACTIVE = "ble_inspection_active_addresses"


def _bluetooth():
    from homeassistant.components import bluetooth
    return bluetooth


def _ha_device_client(device):
    # HA's Bleak wrapper selects the local/proxy backend from this device.
    # Do not use the standalone provisioning connector: it may refresh by
    # address and fall back to scanning after a connection failure.
    from bleak import BleakClient
    return BleakClient(device)


def inspection_candidates(hass) -> dict:
    """Use only HA connectable discovery, including ESPHome remote scanners."""

    candidates = {}
    for info in _bluetooth().async_discovered_service_info(hass, connectable=True):
        device = getattr(info, "device", None)
        name = str(getattr(info, "name", "") or "")
        uuids = getattr(info, "service_uuids", ()) or ()
        candidate = normalize_discovered_candidate(
            address=str(getattr(info, "address", "") or ""), device=device,
            device_name=str(getattr(device, "name", "") or name),
            advertisement_local_name=name, service_uuids=uuids,
            manufacturer_data=getattr(info, "manufacturer_data", None),
        )
        if candidate is not None and (
            candidate.local_pn or VENDOR_LAYOUT.service_uuid in {
                normalize_ble_uuid(value) for value in uuids if value
            }
        ):
            candidates[candidate.address] = candidate
    return candidates


def inspection_scope(entry):
    coordinator = getattr(entry, "runtime_data", None)
    snapshot = getattr(coordinator, "data", None)
    collector = getattr(snapshot, "collector", None)
    live_pn = validated_collector_pn(
        collector.get("collector_pn", "") if isinstance(collector, Mapping)
        else getattr(collector, "collector_pn", "")
    )
    configured_pn = validated_collector_pn(entry.data.get(CONF_COLLECTOR_PN, ""))
    # A stale/different TCP identity must not bind BLE evidence from the selected
    # collector to another inverter's runtime snapshot. A known short prefix of
    # the configured full PN is the existing collector identity convention.
    if live_pn and configured_pn and not configured_pn.startswith(live_pn):
        return None
    return diagnostic_evidence_scope(
        entry.entry_id, configured_pn=entry.data.get(CONF_COLLECTOR_PN, ""),
        collector=collector,
        inverter=getattr(snapshot, "inverter", None),
    )


async def async_inspect_collector_ble(hass, entry, *, address: str, append_crlf: bool,
                                      publish_download: bool = False) -> dict:
    """Own one attempt through cancellation and export, without changing the entry."""

    if not isinstance(address, str) or type(append_crlf) is not bool or type(publish_download) is not bool:
        raise ValueError("ble_inspection_invalid_arguments")
    candidates = inspection_candidates(hass)
    if address not in candidates:
        raise ValueError("ble_inspection_device_unavailable")
    # A remembered address/advertisement is not a connection route. Re-resolve
    # immediately before use; never fall back to the host's raw scanner here.
    device = _bluetooth().async_ble_device_from_address(hass, address, connectable=True)
    if device is None:
        raise ValueError("ble_inspection_device_unavailable")
    active = hass.data.setdefault(DOMAIN, {}).setdefault(_ACTIVE, set())
    key = address.upper()
    if key in active:
        raise ValueError("ble_inspection_busy")
    expected_pn = validated_collector_pn(entry.data.get(CONF_COLLECTOR_PN, ""))
    scope = inspection_scope(entry)
    inspection = BleInspection(expected_pn, append_crlf=append_crlf)
    session = SmartEssBleSession(BleakSmartEssBleLink(
        address, device=device, client_factory=lambda _address: _ha_device_client(device),
    ))
    response = {}
    active.add(key)

    async def finish():
        try:
            await session.disconnect()
        except Exception:
            inspection.cleanup_error = "ble_cleanup_failed"
        bound = (
            scope if inspection.identity_status == "matched" and scope == inspection_scope(entry)
            and validated_collector_pn(entry.data.get(CONF_COLLECTOR_PN, "")) == expected_pn
            else None
        )
        if inspection.identity_status == "matched" and bound is None:
            inspection.error = inspection.error or "collector_identity_scope_changed"
        result = inspection.result(address=address)
        response.update(success=result.success, output=result.output, error=result.error,
                        context=result.context, download_url="", result_path="")
        try:
            exported = await async_run_owned_worker(
                export_diagnostic_run, config_dir=Path(hass.config.config_dir),
                entry_id=entry.entry_id, result=result, device_scope=bound,
            )
            response["result_path"] = str(exported.result_path)
            response["cache_status"] = result.context.get("support_archive_cache_status", "")
        except Exception:
            response["export_error"] = "ble_inspection_export_failed"
            return
        if publish_download:
            try:
                if async_register_download_views(hass):
                    response["download_url"] = sign_diagnostic_run_download_url(
                        hass, entry.entry_id, exported.shareable_path.name,
                    )
            except Exception:
                response["download_error"] = "ble_inspection_download_failed"

    try:
        try:
            await inspection.run(session)
        finally:
            await async_finish_owned_task(asyncio.create_task(finish()))
    finally:
        active.discard(key)
    return response
