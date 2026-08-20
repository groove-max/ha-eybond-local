"""Bluetooth discovery and collector Wi-Fi provisioning lifecycle."""

from __future__ import annotations

import asyncio
import importlib
import logging
from collections.abc import Callable
from contextlib import suppress
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigFlowResult,
)
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
)

from .collector.smartess_ble import (
    BleakSmartEssBleLink,
    BleakSmartEssBleScanner,
    SmartEssBleCandidate,
    SmartEssBleError,
    SmartEssBleHostCapability,
    SmartEssBleProvisioner,
    SmartEssBleProvisionOutcome,
    SmartEssBleSession,
    SmartEssBleWifiNetwork,
    async_probe_ble_host_capability,
    normalize_discovered_candidate,
)
from .config_common import (
    _async_timeout,
)
from .const import (
    CONF_SERVER_IP,
)
from .flow_presentation import (
    _PASSWORD_TEXT_SELECTOR,
    CONF_WIFI_PASSWORD,
    CONF_WIFI_SSID,
    _ble_wifi_selector,
    _exception_detail,
)
from .flow_translation import (
    with_translation_bundle as _with_translation_bundle,
)

logger = logging.getLogger(__name__)

CONF_BLE_ADDRESS = "ble_address"

CONF_BLE_ACTION = "ble_action"

BLE_ADDRESS_RESCAN = "__rescan__"

BLE_ACTION_RESCAN = "rescan"

BLE_ACTION_REFRESH_WIFI = "refresh_wifi"

BLE_ACTION_APPLY = "apply"

_BLE_SCAN_TIMEOUT = 5.0

_BLE_CONNECT_TIMEOUT = 30.0

_BLE_WIFI_SCAN_TIMEOUT = 30.0

_BLE_WIFI_SCAN_ATTEMPTS = 3

_BLE_WIFI_SCAN_RETRY_DELAY = 1.0

_BLE_PROVISION_TIMEOUT = 45.0

_BLE_ADDRESS_TEXT_SELECTOR = TextSelector(TextSelectorConfig())


def _sort_ble_candidates(
    candidates: tuple[SmartEssBleCandidate, ...],
) -> tuple[SmartEssBleCandidate, ...]:
    return tuple(
        sorted(
            candidates,
            key=lambda candidate: (
                str(
                    candidate.preferred_name or candidate.local_pn or candidate.address
                ).lower(),
                str(candidate.address).lower(),
            ),
        )
    )


def _ble_candidate_label(
    candidate: SmartEssBleCandidate,
    *,
    already_added_label: str = "",
) -> str:
    parts: list[str] = []
    for part in (
        str(candidate.preferred_name or "").strip(),
        str(candidate.local_pn or "").strip(),
        str(candidate.address or "").strip(),
    ):
        if part and part not in parts:
            parts.append(part)
    label = " - ".join(parts)
    if already_added_label:
        label = f"{label} ({already_added_label})"
    return label


def _ble_candidate_by_address(
    candidates: tuple[SmartEssBleCandidate, ...],
    address: str,
) -> SmartEssBleCandidate | None:
    normalized_address = str(address or "").strip()
    return next(
        (
            candidate
            for candidate in candidates
            if candidate.address == normalized_address
        ),
        None,
    )


def _ble_candidate_selector(
    candidates: tuple[SmartEssBleCandidate, ...],
    *,
    already_added_addresses: set[str] | None = None,
    already_added_label: str = "",
) -> SelectSelector:
    already_added_addresses = already_added_addresses or set()
    options = [
        *[
            SelectOptionDict(
                value=candidate.address,
                label=_ble_candidate_label(
                    candidate,
                    already_added_label=(
                        already_added_label
                        if candidate.address in already_added_addresses
                        else ""
                    ),
                ),
            )
            for candidate in _sort_ble_candidates(candidates)
        ],
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _ble_action_selector(
    *,
    rescan_label: str,
    refresh_label: str,
    apply_label: str,
) -> SelectSelector:
    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(value=BLE_ACTION_RESCAN, label=rescan_label),
                SelectOptionDict(value=BLE_ACTION_REFRESH_WIFI, label=refresh_label),
                SelectOptionDict(value=BLE_ACTION_APPLY, label=apply_label),
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _is_retryable_ble_wifi_scan_error(exc: SmartEssBleError) -> bool:
    code = str(exc)
    return code in {
        "ble_not_connected",
        "ble_notification_timeout",
    }


class BluetoothProvisioningFlowMixin:
    """Bluetooth discovery and collector Wi-Fi provisioning lifecycle."""

    @_with_translation_bundle
    async def async_step_bluetooth_setup(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        ble_candidates: tuple[SmartEssBleCandidate, ...] = ()
        wifi_networks: tuple[SmartEssBleWifiNetwork, ...] = ()
        previous_ble_address = self._ble_selected_address
        defaults = dict(user_input or {})
        selected_ble_value = str(defaults.get(CONF_BLE_ADDRESS, "") or "").strip()
        selected_ble_action = str(
            defaults.get(CONF_BLE_ACTION, BLE_ACTION_APPLY) or BLE_ACTION_APPLY
        ).strip()
        if selected_ble_action not in {
            BLE_ACTION_RESCAN,
            BLE_ACTION_REFRESH_WIFI,
            BLE_ACTION_APPLY,
        }:
            selected_ble_action = BLE_ACTION_APPLY
        rescan_requested = (
            user_input is not None and selected_ble_action == BLE_ACTION_RESCAN
        )
        refresh_requested = (
            user_input is not None and selected_ble_action == BLE_ACTION_REFRESH_WIFI
        )
        apply_requested = (
            user_input is not None and selected_ble_action == BLE_ACTION_APPLY
        )

        # Refreshing the Wi-Fi list should also clear stale Wi-Fi values and re-run
        # nearby collector discovery before the selected collector is queried again.
        if refresh_requested:
            defaults.pop(CONF_WIFI_SSID, None)
            defaults.pop(CONF_WIFI_PASSWORD, None)

        submitted_ssid = str(defaults.get(CONF_WIFI_SSID, "") or "").strip()
        submitted_password = str(defaults.get(CONF_WIFI_PASSWORD, "") or "")

        if refresh_requested or rescan_requested:
            self._ble_last_error = ""
            selected_ble_value = selected_ble_value or previous_ble_address
        if refresh_requested:
            self._ble_wifi_scan_attempted_addresses.clear()
            self._ble_wifi_scan_failed_addresses.clear()

        capability = await self._async_probe_ble_setup_capability()
        if not capability.available:
            self._ble_last_error = str(
                capability.detail or capability.reason or ""
            ).strip()
            errors["base"] = "ble_unavailable"
        else:
            try:
                ble_candidates = await self._async_discover_smartess_ble_candidates(
                    force_active_scan=rescan_requested or refresh_requested,
                )
            except SmartEssBleError as exc:
                errors["base"] = self._ble_flow_error_key(exc)

        default_ble_address = selected_ble_value
        if ble_candidates:
            candidate_addresses = {candidate.address for candidate in ble_candidates}
            if default_ble_address not in candidate_addresses:
                default_ble_address = ble_candidates[0].address
            ble_address_selector: SelectSelector | TextSelector = (
                _ble_candidate_selector(
                    ble_candidates,
                    already_added_addresses=self._already_added_ble_candidate_addresses(
                        ble_candidates
                    ),
                    already_added_label=self._tr(
                        "common.dynamic.status_already_added", "Already added"
                    ),
                )
            )
            ble_address_marker: vol.Marker = vol.Required(
                CONF_BLE_ADDRESS,
                default=default_ble_address,
            )
        else:
            ble_address_selector = _BLE_ADDRESS_TEXT_SELECTOR
            ble_address_marker = vol.Optional(
                CONF_BLE_ADDRESS, default=default_ble_address
            )

        self._ble_selected_address = str(default_ble_address or "").strip()
        already_added_addresses = self._already_added_ble_candidate_addresses(
            ble_candidates
        )

        selected_candidate = _ble_candidate_by_address(
            ble_candidates, default_ble_address
        )
        selected_already_added = default_ble_address in already_added_addresses
        if selected_already_added and user_input is not None:
            errors[CONF_BLE_ADDRESS] = "already_added_candidate"

        should_scan_selected_wifi = (
            default_ble_address
            and not errors
            and not selected_already_added
            and (user_input is None or refresh_requested)
        )
        if should_scan_selected_wifi:
            cached_wifi_networks = self._ble_wifi_networks_by_address.get(
                default_ble_address, ()
            )
            try:
                wifi_networks = await self._async_scan_smartess_ble_wifi_networks(
                    default_ble_address,
                    ble_device=selected_candidate.device
                    if selected_candidate is not None
                    else None,
                )
                self._ble_wifi_networks_by_address[default_ble_address] = wifi_networks
                self._ble_wifi_scan_failed_addresses.discard(default_ble_address)
                self._ble_last_error = ""
            except SmartEssBleError as exc:
                self._ble_wifi_scan_failed_addresses.add(default_ble_address)
                self._ble_last_error = str(exc)
                if cached_wifi_networks:
                    wifi_networks = cached_wifi_networks
                else:
                    errors["base"] = self._ble_flow_error_key(exc)
                logger.info(
                    "SmartESS BLE Wi-Fi scan unavailable address=%s error=%s",
                    default_ble_address,
                    exc,
                )
            finally:
                self._ble_wifi_scan_attempted_addresses.add(default_ble_address)
        elif default_ble_address in self._ble_wifi_networks_by_address:
            wifi_networks = self._ble_wifi_networks_by_address[default_ble_address]

        if refresh_requested or rescan_requested:
            selected_ble_action = BLE_ACTION_APPLY

        if user_input is not None and not errors:
            if apply_requested:
                if not default_ble_address:
                    errors[CONF_BLE_ADDRESS] = "ble_address_invalid"
                if not submitted_ssid:
                    errors[CONF_WIFI_SSID] = "ble_wifi_ssid_invalid"
                if not submitted_password:
                    errors[CONF_WIFI_PASSWORD] = "ble_wifi_password_invalid"

            if apply_requested and not errors:
                selected_candidate = _ble_candidate_by_address(
                    ble_candidates, default_ble_address
                )
                try:
                    await self._async_run_smartess_ble_bootstrap(
                        ble_address=default_ble_address,
                        ssid=submitted_ssid,
                        password=submitted_password,
                        ble_device=selected_candidate.device
                        if selected_candidate is not None
                        else None,
                    )
                except SmartEssBleError as exc:
                    self._ble_last_error = str(exc)
                    errors["base"] = self._ble_flow_error_key(exc)
                else:
                    self._ble_last_error = ""
                    return await self.async_step_auto()

        default_wifi_ssid = submitted_ssid
        if not default_wifi_ssid and wifi_networks:
            default_wifi_ssid = wifi_networks[0].ssid
        wifi_ssid_selector = _ble_wifi_selector(wifi_networks)

        data_schema: dict[vol.Marker, Any] = {
            ble_address_marker: ble_address_selector,
            vol.Optional(CONF_WIFI_SSID, default=default_wifi_ssid): wifi_ssid_selector,
        }
        data_schema[
            vol.Optional(
                CONF_WIFI_PASSWORD, default=str(defaults.get(CONF_WIFI_PASSWORD, ""))
            )
        ] = _PASSWORD_TEXT_SELECTOR
        data_schema[vol.Required(CONF_BLE_ACTION, default=selected_ble_action)] = (
            _ble_action_selector(
                rescan_label=self._bluetooth_rescan_action_label(),
                refresh_label=self._bluetooth_refresh_wifi_action_label(),
                apply_label=self._bluetooth_apply_action_label(),
            )
        )

        return self.async_show_form(
            step_id="bluetooth_setup",
            data_schema=vol.Schema(data_schema),
            errors=errors,
            description_placeholders=self._bluetooth_setup_placeholders(),
        )

    def _home_assistant_bluetooth_module(self) -> object | None:
        try:
            return importlib.import_module("homeassistant.components.bluetooth")
        except Exception:
            return None

    def _hass_bluetooth_scanner_count(self, bluetooth: object | None = None) -> int:
        bluetooth = bluetooth or self._home_assistant_bluetooth_module()
        if bluetooth is None:
            return 0

        count = 0
        scanner_count = getattr(bluetooth, "async_scanner_count", None)
        if callable(scanner_count):
            for kwargs in ({"connectable": True}, {"connectable": False}, {}):
                try:
                    value = scanner_count(self.hass, **kwargs)
                except TypeError:
                    if kwargs:
                        continue
                    try:
                        value = scanner_count(self.hass)
                    except Exception:
                        continue
                except Exception:
                    continue
                try:
                    count = max(count, int(value))
                except (TypeError, ValueError):
                    continue

        current_scanners = getattr(bluetooth, "async_current_scanners", None)
        if callable(current_scanners):
            for kwargs in ({"connectable": True}, {"connectable": False}, {}):
                try:
                    value = current_scanners(self.hass, **kwargs)
                except TypeError:
                    if kwargs:
                        continue
                    try:
                        value = current_scanners(self.hass)
                    except Exception:
                        continue
                except Exception:
                    continue
                if isinstance(value, dict):
                    count = max(count, len(value))
                    continue
                if value is None:
                    continue
                try:
                    count = max(count, len(tuple(value)))
                except TypeError:
                    continue

        return count

    def _hass_bluetooth_backend_capability(self) -> SmartEssBleHostCapability | None:
        bluetooth = self._home_assistant_bluetooth_module()
        if bluetooth is None:
            return None

        scanner_count = self._hass_bluetooth_scanner_count(bluetooth)
        if scanner_count > 0:
            return SmartEssBleHostCapability(
                available=True,
                backend="home_assistant_bluetooth",
                reason="ha_bluetooth_scanners_available",
                detail=f"{scanner_count} Home Assistant Bluetooth scanner(s) available",
            )

        if self._hass_bluetooth_service_infos(
            bluetooth
        ) or self._hass_bluetooth_devices(bluetooth):
            return SmartEssBleHostCapability(
                available=True,
                backend="home_assistant_bluetooth",
                reason="ha_bluetooth_cache_available",
                detail="Home Assistant Bluetooth already has cached devices",
            )

        return SmartEssBleHostCapability(
            available=False,
            backend="home_assistant_bluetooth",
            reason="ha_bluetooth_unavailable",
        )

    async def _async_probe_ble_setup_capability(self) -> SmartEssBleHostCapability:
        local_capability = await async_probe_ble_host_capability()
        self._ble_local_adapter_available = bool(
            getattr(local_capability, "available", False)
        )

        ha_capability = self._hass_bluetooth_backend_capability()
        self._ble_ha_backend_available = bool(
            ha_capability is not None and ha_capability.available
        )

        if self._ble_local_adapter_available:
            if isinstance(local_capability, SmartEssBleHostCapability):
                return local_capability
            return SmartEssBleHostCapability(
                available=True,
                backend=str(getattr(local_capability, "backend", "bleak") or "bleak"),
                reason=str(
                    getattr(local_capability, "reason", "backend_available")
                    or "backend_available"
                ),
                detail=str(getattr(local_capability, "detail", "") or ""),
            )

        if ha_capability is not None and ha_capability.available:
            return ha_capability

        if isinstance(local_capability, SmartEssBleHostCapability):
            return local_capability
        return SmartEssBleHostCapability(
            available=False,
            backend=str(getattr(local_capability, "backend", "bleak") or "bleak"),
            reason=str(
                getattr(local_capability, "reason", "ble_unavailable")
                or "ble_unavailable"
            ),
            detail=str(getattr(local_capability, "detail", "") or ""),
        )

    def _bluetooth_setup_placeholders(self) -> dict[str, str]:
        return {
            "selected_scan_interface": self._selected_interface_label(
                self._auto_config.get(CONF_SERVER_IP, self._local_ip)
            ),
            "ble_last_error": self._ble_last_error
            or self._tr("common.dynamic.none", "None"),
        }

    def _bluetooth_rescan_action_label(self) -> str:
        return self._tr(
            "common.dynamic.bluetooth_action_rescan",
            "Refresh collector list",
        )

    def _bluetooth_refresh_wifi_action_label(self) -> str:
        return self._tr(
            "common.dynamic.bluetooth_action_refresh_wifi",
            "Refresh Wi-Fi list for current collector",
        )

    def _bluetooth_apply_action_label(self) -> str:
        return self._tr(
            "common.dynamic.bluetooth_action_apply",
            "Apply settings to current collector",
        )

    @staticmethod
    def _ble_device_name(device: object | None) -> str:
        return str(getattr(device, "name", None) or "").strip()

    @staticmethod
    def _ble_log_value(value: object, *, limit: int = 140) -> str:
        try:
            text = str(value)
        except Exception:
            text = f"<{type(value).__name__}>"
        text = " ".join(text.split())
        if len(text) <= limit:
            return text
        return text[: limit - 3] + "..."

    @classmethod
    def _ble_device_log_summary(cls, device: object | None) -> str:
        if device is None:
            return "none"

        parts = [f"type={type(device).__name__}"]
        for attribute in ("address", "name", "rssi"):
            value = getattr(device, attribute, None)
            if value not in (None, ""):
                parts.append(f"{attribute}={cls._ble_log_value(value)}")

        details = getattr(device, "details", None)
        if details is not None:
            parts.append(f"details_type={type(details).__name__}")
            if isinstance(details, dict):
                keys = ",".join(sorted(str(key) for key in details)[:8])
                if keys:
                    parts.append(f"details_keys={keys}")

        metadata = getattr(device, "metadata", None)
        if isinstance(metadata, dict):
            keys = ",".join(sorted(str(key) for key in metadata)[:8])
            if keys:
                parts.append(f"metadata_keys={keys}")
            service_uuids = metadata.get("uuids") or metadata.get("service_uuids")
            if service_uuids:
                rendered = ",".join(str(value) for value in list(service_uuids)[:6])
                parts.append(f"metadata_uuids={cls._ble_log_value(rendered)}")
            manufacturer_data = metadata.get("manufacturer_data")
            if isinstance(manufacturer_data, dict):
                ids = ",".join(str(key) for key in sorted(manufacturer_data)[:8])
                if ids:
                    parts.append(f"manufacturer_ids={ids}")

        return " ".join(parts)

    def _resolve_ble_connect_device(
        self, address: str, ble_device: object | None = None
    ) -> object | None:
        resolved_device = self._hass_bluetooth_device_from_address(address)
        if resolved_device is not None:
            if not self._ble_device_name(resolved_device):
                logger.info(
                    "SmartESS BLE Home Assistant connectable device lacks a usable name for address=%s; "
                    "still preferring it over the current discovery candidate ha_device=%s candidate_device=%s",
                    address,
                    self._ble_device_log_summary(resolved_device),
                    self._ble_device_log_summary(ble_device),
                )
            logger.info(
                "SmartESS BLE using Home Assistant connectable device address=%s selected_device=%s "
                "candidate_device=%s",
                address,
                self._ble_device_log_summary(resolved_device),
                self._ble_device_log_summary(ble_device),
            )
            return resolved_device
        bluetooth = self._home_assistant_bluetooth_module()
        if bluetooth is not None and callable(
            getattr(bluetooth, "async_ble_device_from_address", None)
        ):
            logger.warning(
                "SmartESS BLE found no Home Assistant connectable device for address=%s; "
                "falling back to address-only connection candidate_device=%s",
                address,
                self._ble_device_log_summary(ble_device),
            )
            return None
        logger.info(
            "SmartESS BLE using discovery candidate without Home Assistant lookup address=%s candidate_device=%s",
            address,
            self._ble_device_log_summary(ble_device),
        )
        return ble_device

    @staticmethod
    def _ble_flow_error_key(exc: SmartEssBleError) -> str:
        code = str(exc)
        if code in {
            "adapter_not_found",
            "backend_missing",
            "backend_not_supported",
            "ble_backend_missing",
            "host_unavailable",
            "permission_denied",
            "probe_failed",
        }:
            return "ble_unavailable"
        if code == "ble_address_invalid":
            return "ble_address_invalid"
        if code == "ble_wifi_ssid_invalid":
            return "ble_wifi_ssid_invalid"
        if code == "ble_wifi_password_invalid":
            return "ble_wifi_password_invalid"
        if code == "ble_scan_failed" or code.startswith("ble_scan_failed:"):
            return "ble_scan_failed"
        if code == "ble_wifi_scan_failed" or code.startswith("ble_wifi_scan_failed:"):
            return "ble_wifi_scan_failed"
        if code == "ble_provision_failed" or code.startswith("ble_provision_failed:"):
            return "ble_provision_failed"
        return "ble_provision_failed"

    async def _async_discover_smartess_ble_candidates(
        self,
        *,
        force_active_scan: bool = False,
    ) -> tuple[SmartEssBleCandidate, ...]:
        if force_active_scan:
            ha_candidates = await self._async_discover_smartess_ble_candidates_from_hass_advertisements(
                timeout=_BLE_SCAN_TIMEOUT
            )
            if not ha_candidates:
                ha_candidates = (
                    self._async_discovered_smartess_ble_candidates_from_hass()
                )
        else:
            ha_candidates = self._async_discovered_smartess_ble_candidates_from_hass()
            if not ha_candidates:
                ha_candidates = await self._async_discover_smartess_ble_candidates_from_hass_advertisements(
                    timeout=_BLE_SCAN_TIMEOUT
                )
            if not ha_candidates:
                ha_candidates = (
                    self._async_discovered_smartess_ble_candidates_from_hass()
                )
        if ha_candidates:
            self._ble_last_error = ""
            return _sort_ble_candidates(ha_candidates)

        if self._ble_ha_backend_available and not self._ble_local_adapter_available:
            logger.info(
                "SmartESS BLE scan found no collector candidates in Home Assistant Bluetooth data; "
                "skipping raw Bleak fallback because no local adapter is available"
            )
            return ()

        scanner = BleakSmartEssBleScanner()
        try:
            candidates = _sort_ble_candidates(
                await scanner.discover_candidates(timeout=_BLE_SCAN_TIMEOUT)
            )
            if candidates:
                self._ble_last_error = ""
            else:
                logger.info(
                    "SmartESS BLE scan found no collector candidates after %.1fs",
                    _BLE_SCAN_TIMEOUT,
                )
            return candidates
        except SmartEssBleError:
            raise
        except PermissionError as exc:
            raise SmartEssBleError("permission_denied") from exc
        except FileNotFoundError as exc:
            raise SmartEssBleError("adapter_not_found") from exc
        except NotImplementedError as exc:
            raise SmartEssBleError("backend_not_supported") from exc
        except OSError as exc:
            raise SmartEssBleError("host_unavailable") from exc
        except Exception as exc:
            detail = _exception_detail(exc)
            logger.warning("SmartESS BLE scan failed error=%s", detail)
            raise SmartEssBleError(f"ble_scan_failed:{detail}") from exc

    async def _async_refresh_ble_device_before_wifi_scan_retry(
        self,
        ble_address: str,
        *,
        attempt: int,
        error: str,
    ) -> object | None:
        try:
            candidates = await self._async_discover_smartess_ble_candidates(
                force_active_scan=True
            )
        except SmartEssBleError as exc:
            logger.info(
                "SmartESS BLE Wi-Fi scan active rediscovery failed before retry address=%s attempt=%d error=%s refresh_error=%s",
                ble_address,
                attempt,
                error,
                exc,
            )
            return None

        candidate = _ble_candidate_by_address(candidates, ble_address)
        if candidate is None:
            logger.info(
                "SmartESS BLE Wi-Fi scan active rediscovery did not find selected collector before retry address=%s attempt=%d error=%s",
                ble_address,
                attempt,
                error,
            )
            return None

        logger.info(
            "SmartESS BLE Wi-Fi scan refreshed selected collector before retry address=%s attempt=%d error=%s device=%s",
            ble_address,
            attempt,
            error,
            self._ble_device_log_summary(candidate.device),
        )
        return candidate.device

    async def _async_discover_smartess_ble_candidates_from_hass_advertisements(
        self,
        *,
        timeout: float,
    ) -> tuple[SmartEssBleCandidate, ...]:
        try:
            bluetooth = importlib.import_module("homeassistant.components.bluetooth")
        except Exception:
            return ()

        register_callback = getattr(bluetooth, "async_register_callback", None)
        scanning_mode = getattr(bluetooth, "BluetoothScanningMode", None)
        if not callable(register_callback) or scanning_mode is None:
            return ()

        active_mode = getattr(scanning_mode, "ACTIVE", None)
        if active_mode is None:
            return ()

        deduped: dict[str, SmartEssBleCandidate] = {}
        advertisement_count = 0
        registration_errors: list[str] = []
        advertisement_samples: list[str] = []
        advertisement_sample_keys: set[str] = set()

        def _handle_advertisement(service_info: object, _change: object) -> None:
            nonlocal advertisement_count
            advertisement_count += 1
            if len(advertisement_samples) < 12:
                sample = self._hass_bluetooth_service_info_summary(service_info)
                if sample and sample not in advertisement_sample_keys:
                    advertisement_sample_keys.add(sample)
                    advertisement_samples.append(sample)
            candidate = self._smartess_ble_candidate_from_hass_service_info(
                service_info
            )
            if candidate is not None:
                deduped[candidate.address] = candidate

        unload_callbacks: list[Callable[[], None]] = []
        for matcher in (
            {"manufacturer_id": 0x3545, "connectable": False},
            {"manufacturer_id": 0x3545, "connectable": True},
            {"local_name": "E50*", "connectable": False},
            {"local_name": "E50*", "connectable": True},
            {"local_name": "V00*", "connectable": False},
            {"local_name": "V00*", "connectable": True},
            {"connectable": False},
            {"connectable": True},
        ):
            try:
                unload = register_callback(
                    self.hass, _handle_advertisement, matcher, active_mode
                )
            except Exception as exc:
                registration_errors.append(f"{matcher}: {exc}")
                logger.debug(
                    "SmartESS BLE HA callback registration failed matcher=%s error=%s",
                    matcher,
                    exc,
                )
                continue
            if callable(unload):
                unload_callbacks.append(unload)

        if not unload_callbacks:
            return ()

        try:
            await asyncio.sleep(float(timeout))
        finally:
            for unload in unload_callbacks:
                try:
                    unload()
                except Exception as exc:
                    logger.debug(
                        "SmartESS BLE HA callback cleanup failed error=%s", exc
                    )

        if not deduped:
            logger.warning(
                "SmartESS BLE HA advertisement scan found no collector candidates after %.1fs "
                "registered_callbacks=%d advertisements=%d registration_errors=%s samples=%s",
                timeout,
                len(unload_callbacks),
                advertisement_count,
                registration_errors or "none",
                advertisement_samples or "none",
            )

        return tuple(deduped.values())

    def _async_discovered_smartess_ble_candidates_from_hass(
        self,
    ) -> tuple[SmartEssBleCandidate, ...]:
        try:
            bluetooth = importlib.import_module("homeassistant.components.bluetooth")
        except Exception:
            return ()

        service_infos = self._hass_bluetooth_service_infos(bluetooth)
        devices = self._hass_bluetooth_devices(bluetooth)

        if not service_infos and not devices:
            return ()

        deduped: dict[str, SmartEssBleCandidate] = {}
        for service_info in service_infos or ():
            candidate = self._smartess_ble_candidate_from_hass_service_info(
                service_info
            )
            if candidate is not None:
                deduped[candidate.address] = candidate
        for device in devices:
            candidate = self._smartess_ble_candidate_from_hass_device(device)
            if candidate is not None and candidate.address not in deduped:
                deduped[candidate.address] = candidate
        return tuple(deduped.values())

    def _hass_bluetooth_service_infos(self, bluetooth: object) -> tuple[object, ...]:
        discovered_service_info = getattr(
            bluetooth, "async_discovered_service_info", None
        )
        if not callable(discovered_service_info):
            return ()

        service_infos: list[object] = []
        seen_keys: set[tuple[str, str]] = set()
        call_variants = (
            {"connectable": True},
            {"connectable": False},
            {},
        )
        for kwargs in call_variants:
            try:
                result = discovered_service_info(self.hass, **kwargs)
            except TypeError:
                if kwargs:
                    continue
                try:
                    result = discovered_service_info(self.hass)
                except Exception:
                    continue
            except Exception:
                continue
            for service_info in result or ():
                key = (
                    str(getattr(service_info, "address", "") or ""),
                    str(getattr(service_info, "name", "") or ""),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                service_infos.append(service_info)
        return tuple(service_infos)

    def _hass_bluetooth_devices(self, bluetooth: object) -> tuple[object, ...]:
        devices: list[object] = []
        seen_addresses: set[str] = set()
        for attr in ("async_scanner_devices", "async_scanner_devices_by_address"):
            provider = getattr(bluetooth, attr, None)
            if not callable(provider):
                continue
            for kwargs in ({"connectable": True}, {"connectable": False}, {}):
                try:
                    result = provider(self.hass, **kwargs)
                except TypeError:
                    if kwargs:
                        continue
                    try:
                        result = provider(self.hass)
                    except Exception:
                        continue
                except Exception:
                    continue
                values = result.values() if isinstance(result, dict) else result or ()
                for device in values:
                    address = str(getattr(device, "address", "") or "").strip()
                    if not address or address in seen_addresses:
                        continue
                    seen_addresses.add(address)
                    devices.append(device)
        return tuple(devices)

    @staticmethod
    def _smartess_ble_candidate_from_hass_service_info(
        service_info: object,
    ) -> SmartEssBleCandidate | None:
        advertisement = getattr(service_info, "advertisement", None)
        device = getattr(service_info, "device", None)
        service_name = str(getattr(service_info, "name", "") or "").strip()
        return normalize_discovered_candidate(
            address=str(getattr(service_info, "address", "") or "").strip(),
            device_name=str(getattr(device, "name", "") or service_name).strip(),
            advertisement_local_name=str(
                getattr(advertisement, "local_name", "") or service_name
            ).strip(),
            manufacturer_data=getattr(service_info, "manufacturer_data", None)
            or getattr(advertisement, "manufacturer_data", None),
            service_uuids=getattr(service_info, "service_uuids", None)
            or getattr(advertisement, "service_uuids", None)
            or (),
            device=device,
        )

    @staticmethod
    def _hass_bluetooth_service_info_summary(service_info: object) -> str:
        advertisement = getattr(service_info, "advertisement", None)
        device = getattr(service_info, "device", None)
        manufacturer_data = (
            getattr(service_info, "manufacturer_data", None)
            or getattr(advertisement, "manufacturer_data", None)
            or {}
        )
        manufacturer_summary: list[str] = []
        if isinstance(manufacturer_data, dict):
            for key, value in list(manufacturer_data.items())[:4]:
                data = bytes(value or b"")
                ascii_preview = data.decode("ascii", errors="ignore")[:24]
                manufacturer_summary.append(
                    f"0x{int(key):04x}:{data[:12].hex()}:{ascii_preview}"
                )
        service_uuids = (
            getattr(service_info, "service_uuids", None)
            or getattr(advertisement, "service_uuids", None)
            or ()
        )
        uuid_summary = ",".join(str(value) for value in tuple(service_uuids)[:4])
        return (
            f"address={str(getattr(service_info, 'address', '') or '').strip()} "
            f"name={str(getattr(service_info, 'name', '') or '').strip()} "
            f"local_name={str(getattr(advertisement, 'local_name', '') or '').strip()} "
            f"device_name={str(getattr(device, 'name', '') or '').strip()} "
            f"rssi={str(getattr(service_info, 'rssi', '') or '').strip()} "
            f"source={str(getattr(service_info, 'source', '') or '').strip()} "
            f"connectable={str(getattr(service_info, 'connectable', '') or '').strip()} "
            f"manufacturer={manufacturer_summary or 'none'} "
            f"service_uuids={uuid_summary or 'none'}"
        )

    @staticmethod
    def _smartess_ble_candidate_from_hass_device(
        device: object,
    ) -> SmartEssBleCandidate | None:
        address = str(getattr(device, "address", "") or "").strip()
        if not address:
            return None
        device_name = str(getattr(device, "name", "") or "").strip()
        metadata = getattr(device, "metadata", None)
        if not isinstance(metadata, dict):
            metadata = {}
        return normalize_discovered_candidate(
            address=address,
            device_name=device_name,
            advertisement_local_name=str(
                metadata.get("local_name") or device_name
            ).strip(),
            manufacturer_data=metadata.get("manufacturer_data"),
            service_uuids=metadata.get("uuids") or (),
            device=device,
        )

    async def _async_scan_smartess_ble_wifi_networks(
        self,
        ble_address: str,
        ble_device: object | None = None,
    ) -> tuple[SmartEssBleWifiNetwork, ...]:
        if not ble_address:
            return ()

        current_ble_device = ble_device
        for attempt in range(1, _BLE_WIFI_SCAN_ATTEMPTS + 1):
            resolved_device = self._resolve_ble_connect_device(
                ble_address, current_ble_device
            )
            session = SmartEssBleSession(
                BleakSmartEssBleLink(ble_address, device=resolved_device)
            )
            try:
                async with _async_timeout(_BLE_CONNECT_TIMEOUT):
                    await session.connect()
                provisioner = SmartEssBleProvisioner(session)
                async with _async_timeout(_BLE_WIFI_SCAN_TIMEOUT):
                    networks = tuple(await provisioner.scan_wifi_networks())
                if provisioner.last_firmware_version:
                    self._ble_fw_version_by_address[ble_address] = (
                        provisioner.last_firmware_version
                    )
                return networks
            except TimeoutError as exc:
                timeout = (
                    _BLE_WIFI_SCAN_TIMEOUT
                    if session.connected
                    else _BLE_CONNECT_TIMEOUT
                )
                logger.warning(
                    "SmartESS BLE Wi-Fi scan timed out address=%s timeout=%.1fs",
                    ble_address,
                    timeout,
                )
                raise SmartEssBleError("ble_wifi_scan_failed:timeout") from exc
            except SmartEssBleError as exc:
                if (
                    attempt < _BLE_WIFI_SCAN_ATTEMPTS
                    and _is_retryable_ble_wifi_scan_error(exc)
                ):
                    logger.info(
                        "SmartESS BLE Wi-Fi scan retrying after BLE session error address=%s attempt=%d/%d error=%s",
                        ble_address,
                        attempt,
                        _BLE_WIFI_SCAN_ATTEMPTS,
                        exc,
                    )
                    current_ble_device = (
                        await self._async_refresh_ble_device_before_wifi_scan_retry(
                            ble_address,
                            attempt=attempt,
                            error=str(exc),
                        )
                    )
                    await asyncio.sleep(_BLE_WIFI_SCAN_RETRY_DELAY * attempt)
                    continue
                if str(exc) == "ble_notification_timeout":
                    raise SmartEssBleError(
                        "ble_wifi_scan_failed:notification_timeout"
                    ) from exc
                raise
            except PermissionError as exc:
                raise SmartEssBleError("ble_unavailable") from exc
            except Exception as exc:
                detail = _exception_detail(exc)
                if attempt < _BLE_WIFI_SCAN_ATTEMPTS:
                    logger.info(
                        "SmartESS BLE Wi-Fi scan retrying address=%s attempt=%d/%d error=%s",
                        ble_address,
                        attempt,
                        _BLE_WIFI_SCAN_ATTEMPTS,
                        detail,
                    )
                    current_ble_device = (
                        await self._async_refresh_ble_device_before_wifi_scan_retry(
                            ble_address,
                            attempt=attempt,
                            error=detail,
                        )
                    )
                    await asyncio.sleep(_BLE_WIFI_SCAN_RETRY_DELAY * attempt)
                    continue
                logger.info(
                    "SmartESS BLE Wi-Fi scan failed address=%s error=%s",
                    ble_address,
                    detail,
                )
                raise SmartEssBleError(f"ble_wifi_scan_failed:{detail}") from exc
            finally:
                with suppress(Exception):
                    await session.disconnect()

        raise SmartEssBleError("ble_wifi_scan_failed:retry_exhausted")

    async def _async_run_smartess_ble_bootstrap(
        self,
        *,
        ble_address: str,
        ssid: str,
        password: str,
        ble_device: object | None = None,
    ) -> None:
        if not ble_address:
            raise SmartEssBleError("ble_address_invalid")

        resolved_device = self._resolve_ble_connect_device(ble_address, ble_device)
        session = SmartEssBleSession(
            BleakSmartEssBleLink(ble_address, device=resolved_device)
        )
        try:
            async with _async_timeout(_BLE_PROVISION_TIMEOUT):
                await session.connect()
                provisioner = SmartEssBleProvisioner(session)
                resolved_info = None
                cached_fw_version = self._known_smartess_ble_firmware_version(
                    ble_address
                )
                if cached_fw_version:
                    resolved_info = await provisioner.query_device_info(
                        known_fw_version=cached_fw_version
                    )
                result = await provisioner.provision_wifi(
                    ssid=ssid,
                    password=password,
                    info=resolved_info,
                )
                if provisioner.last_firmware_version:
                    self._ble_fw_version_by_address[ble_address] = (
                        provisioner.last_firmware_version
                    )
        except TimeoutError as exc:
            logger.warning(
                "SmartESS BLE provisioning timed out address=%s timeout=%.1fs",
                ble_address,
                _BLE_PROVISION_TIMEOUT,
            )
            raise SmartEssBleError("ble_provision_failed:timeout") from exc
        except SmartEssBleError as exc:
            if str(exc) == "ble_notification_timeout":
                raise SmartEssBleError(
                    "ble_provision_failed:notification_timeout"
                ) from exc
            raise
        except PermissionError as exc:
            raise SmartEssBleError("ble_unavailable") from exc
        except Exception as exc:
            detail = _exception_detail(exc)
            logger.warning(
                "SmartESS BLE provisioning failed address=%s error=%s",
                ble_address,
                detail,
            )
            raise SmartEssBleError(f"ble_provision_failed:{detail}") from exc
        finally:
            with suppress(Exception):
                await session.disconnect()

        logger.info(
            "SmartESS BLE provisioning result address=%s branch=%s outcome=%s status=%s details=%s",
            ble_address,
            result.branch.value,
            result.outcome.value,
            result.status_code,
            result.details,
        )

        if result.outcome == SmartEssBleProvisionOutcome.FAILURE:
            detail = f"{result.branch.value}:{result.status_code}"
            if result.details is not None:
                detail = f"{detail}:{','.join(result.details)}"
            raise SmartEssBleError(f"ble_provision_failed:{detail}")
