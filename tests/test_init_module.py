from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_homeassistant_entity_registry_stub() -> None:
    """Provide the HA entity-registry module for local pure-unit runs."""

    if "homeassistant.helpers.entity_registry" in sys.modules:
        return

    homeassistant_module = sys.modules.setdefault(
        "homeassistant",
        types.ModuleType("homeassistant"),
    )
    helpers_module = sys.modules.setdefault(
        "homeassistant.helpers",
        types.ModuleType("homeassistant.helpers"),
    )
    entity_registry_module = types.ModuleType("homeassistant.helpers.entity_registry")

    def _missing(*_args, **_kwargs):
        raise AssertionError("entity_registry stub must be patched by the test")

    entity_registry_module.async_get = _missing
    entity_registry_module.async_entries_for_config_entry = _missing
    helpers_module.entity_registry = entity_registry_module
    homeassistant_module.helpers = helpers_module
    sys.modules["homeassistant.helpers.entity_registry"] = entity_registry_module


def _install_voluptuous_stub() -> None:
    """Provide the voluptuous module for local pure-unit runs without HA deps."""

    if "voluptuous" in sys.modules:
        return

    voluptuous_module = types.ModuleType("voluptuous")

    class Schema:
        def __init__(self, schema):
            self.schema = schema

        def __call__(self, value):
            return value

    def Required(key, default=None):
        return key

    def Optional(key, default=None):
        return key

    def All(*validators):
        return validators

    def Range(**kwargs):
        return kwargs

    def In(container):
        return container

    voluptuous_module.Schema = Schema
    voluptuous_module.Required = Required
    voluptuous_module.Optional = Optional
    voluptuous_module.All = All
    voluptuous_module.Range = Range
    voluptuous_module.In = In
    sys.modules["voluptuous"] = voluptuous_module


_install_voluptuous_stub()
_install_homeassistant_entity_registry_stub()


from custom_components.eybond_local import (
    ConfigEntryNotReady,
    _async_cleanup_obsolete_entities,
    _async_finalize_expert_entity_migration,
    _async_initial_refresh_for_setup,
    _async_remove_legacy_runtime_select_entities,
    _async_self_heal_collector_cloud_family,
    _async_self_heal_entry_title,
    _async_self_heal_expert_defaults,
    _async_self_heal_enabled_defaults,
    _async_self_heal_valuecloud_driver_hint,
    _default_enabled_unique_ids,
    _default_enabled_unique_ids_for_current_runtime,
    _is_integration_disabled,
    _prime_metadata_caches,
    _register_entry_stop_shutdown,
    async_setup_entry,
)
from custom_components.eybond_local.collector.transport import CollectorListenerBindError
from custom_components.eybond_local.tooling import (
    default_enabled_tooling_button_keys_for_runtime,
    tooling_button_keys_for_runtime,
)
from custom_components.eybond_local.models import (
    BinarySensorDescription,
    MeasurementDescription,
    WriteCapability,
)


def _runtime_entity_key_module_stubs() -> dict[str, types.ModuleType]:
    select_module = types.ModuleType("custom_components.eybond_local.select")
    select_module.default_enabled_runtime_select_keys_for_runtime = (
        lambda *, has_inverter_identity=True: ("collector_operation_mode",)
    )
    text_module = types.ModuleType("custom_components.eybond_local.text")
    text_module.default_enabled_collector_text_keys_for_runtime = lambda: ()
    tooling_module = types.ModuleType("custom_components.eybond_local.tooling")
    tooling_module.default_enabled_tooling_button_keys_for_runtime = (
        default_enabled_tooling_button_keys_for_runtime
    )
    return {
        "custom_components.eybond_local.select": select_module,
        "custom_components.eybond_local.text": text_module,
        "custom_components.eybond_local.tooling": tooling_module,
    }


class InitModuleTests(unittest.TestCase):
    def test_is_integration_disabled_accepts_string_marker(self) -> None:
        class _RegistryEntryDisabler:
            INTEGRATION = object()

        self.assertTrue(
            _is_integration_disabled(
                "integration",
                _RegistryEntryDisabler.INTEGRATION,
            )
        )
        self.assertFalse(
            _is_integration_disabled(
                "user",
                _RegistryEntryDisabler.INTEGRATION,
            )
        )

    def test_prime_metadata_caches_delegates_to_registry(self) -> None:
        with patch("custom_components.eybond_local.drivers.registry.prime_metadata_caches") as prime:
            _prime_metadata_caches()

        prime.assert_called_once_with()

    def test_default_enabled_unique_ids_include_derived_energy_defaults(self) -> None:
        with patch.dict(sys.modules, _runtime_entity_key_module_stubs()):
            unique_ids = _default_enabled_unique_ids("entry123")

        self.assertIn("entry123_battery_power", unique_ids)
        self.assertIn("entry123_last_error", unique_ids)
        self.assertIn("entry123_estimated_load_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_pv_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_pv_to_home_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_battery_to_home_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_grid_to_home_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_grid_import_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_grid_export_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_battery_charge_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_battery_discharge_energy_daily", unique_ids)
        self.assertNotIn("entry123_estimated_load_energy", unique_ids)
        self.assertNotIn("entry123_estimated_pv_energy", unique_ids)
        self.assertNotIn("entry123_estimated_pv_energy_monthly", unique_ids)
        self.assertNotIn("entry123_estimated_pv_to_home_energy", unique_ids)
        self.assertNotIn("entry123_estimated_battery_to_home_energy", unique_ids)
        self.assertNotIn("entry123_estimated_grid_to_home_energy", unique_ids)
        self.assertNotIn("entry123_estimated_grid_import_energy", unique_ids)
        self.assertNotIn("entry123_estimated_grid_export_energy", unique_ids)
        self.assertNotIn("entry123_estimated_battery_charge_energy", unique_ids)
        self.assertNotIn("entry123_estimated_battery_discharge_energy", unique_ids)
        self.assertIn("entry123_pv_to_home_power", unique_ids)
        self.assertIn("entry123_pv_to_battery_power", unique_ids)
        self.assertIn("entry123_pv_to_grid_power", unique_ids)
        self.assertIn("entry123_battery_to_home_power", unique_ids)
        self.assertIn("entry123_grid_to_home_power", unique_ids)
        self.assertIn("entry123_grid_to_battery_power", unique_ids)
        self.assertIn("entry123_output_source_priority", unique_ids)
        self.assertIn("entry123_charge_source_priority", unique_ids)
        self.assertIn("entry123_battery_float_voltage", unique_ids)
        self.assertIn("entry123_max_ac_charge_current", unique_ids)
        self.assertIn("entry123_collector_signal_strength", unique_ids)
        self.assertIn("entry123_collector_signal_quality", unique_ids)
        self.assertIn("entry123_collector_operation_mode", unique_ids)
        self.assertIn("entry123_select_collector_operation_mode", unique_ids)
        self.assertIn("entry123_collector_onboarding_status", unique_ids)
        self.assertIn("entry123_collector_serial_baudrate", unique_ids)
        self.assertIn("entry123_number_proxy_capture_duration_minutes", unique_ids)
        self.assertNotIn("entry123_select_control_mode", unique_ids)
        self.assertNotIn("entry123_text_collector_callback_endpoint", unique_ids)

    def test_current_runtime_default_enabled_unique_ids_follow_capability_policy(self) -> None:
        turn_on_mode = WriteCapability(
            key="turn_on_mode",
            register=1,
            value_kind="enum",
            note="",
            tested=True,
            enum_map={0: "Disabled", 1: "Enabled"},
            enabled_default=True,
        )
        output_mode = WriteCapability(
            key="output_mode",
            register=2,
            value_kind="enum",
            note="",
            tested=False,
            enum_map={0: "Utility", 1: "Battery"},
            enabled_default=True,
        )
        inverter = type(
            "FakeInverter",
            (),
            {"capabilities": (turn_on_mode, output_mode), "capability_presets": ()},
        )()

        with (
            patch(
                "custom_components.eybond_local.drivers.registry.measurements_for_runtime",
                return_value=(
                    MeasurementDescription(
                        key="pv_power",
                        name="PV Power",
                        enabled_default=True,
                    ),
                ),
            ),
            patch(
                "custom_components.eybond_local.drivers.registry.binary_sensors_for_runtime",
                return_value=(
                    BinarySensorDescription(
                        key="fault_active",
                        name="Fault Active",
                        enabled_default=True,
                    ),
                ),
            ),
            patch.dict(sys.modules, _runtime_entity_key_module_stubs()),
        ):
            unique_ids = _default_enabled_unique_ids_for_current_runtime(
                "entry123",
                types.SimpleNamespace(async_set_proxy_capture_duration_minutes=AsyncMock()),
                None,
                inverter,
                lambda capability: capability.key == "turn_on_mode",
                lambda _preset: True,
            )

        self.assertIn("entry123_pv_power", unique_ids)
        self.assertIn("entry123_binary_sensor_fault_active", unique_ids)
        self.assertIn("entry123_number_proxy_capture_duration_minutes", unique_ids)
        self.assertIn("entry123_select_turn_on_mode", unique_ids)
        self.assertNotIn("entry123_select_output_mode", unique_ids)

    def test_current_runtime_default_enabled_unique_ids_hide_legacy_signal_entities(self) -> None:
        coordinator = types.SimpleNamespace(
            collector_cloud_family="legacy_binary",
            async_set_proxy_capture_duration_minutes=AsyncMock(),
            data=types.SimpleNamespace(inverter=None),
        )

        with (
            patch(
                "custom_components.eybond_local.drivers.registry.measurements_for_runtime",
                return_value=(
                    MeasurementDescription(
                        key="collector_signal_strength",
                        name="Collector Signal Strength",
                        enabled_default=True,
                    ),
                    MeasurementDescription(
                        key="collector_signal_quality",
                        name="Collector Signal Quality",
                        enabled_default=True,
                    ),
                    MeasurementDescription(
                        key="collector_onboarding_status",
                        name="Collector Onboarding Status",
                        enabled_default=True,
                    ),
                ),
            ),
            patch(
                "custom_components.eybond_local.drivers.registry.binary_sensors_for_runtime",
                return_value=(),
            ),
            patch.dict(sys.modules, _runtime_entity_key_module_stubs()),
        ):
            unique_ids = _default_enabled_unique_ids_for_current_runtime(
                "entry123",
                coordinator,
                None,
                None,
                lambda _capability: True,
                lambda _preset: True,
                has_inverter_identity=False,
            )

        self.assertNotIn("entry123_collector_signal_strength", unique_ids)
        self.assertNotIn("entry123_collector_signal_quality", unique_ids)
        self.assertIn("entry123_collector_onboarding_status", unique_ids)

    def test_tooling_button_keys_only_include_clock_sync_for_allowed_profile(self) -> None:
        self.assertEqual(
            tooling_button_keys_for_runtime(
                {"turn_on_mode", "battery_float_voltage"},
                "smg_modbus.json",
            ),
            (
                "create_support_package",
                "reload_local_metadata",
                "create_local_profile_draft",
                "create_local_schema_draft",
                "apply_collector_changes",
                "rediscover_collector",
                "reboot_collector",
                "start_proxy_capture",
                "stop_proxy_capture",
            ),
        )

    def test_default_enabled_tooling_button_keys_include_collector_actions(self) -> None:
        self.assertEqual(
            default_enabled_tooling_button_keys_for_runtime(
                {"turn_on_mode", "battery_float_voltage"},
                "smg_modbus.json",
            ),
            (
                "create_support_package",
                "apply_collector_changes",
                "rediscover_collector",
                "reboot_collector",
                "start_proxy_capture",
                "stop_proxy_capture",
            ),
        )
        self.assertEqual(
            default_enabled_tooling_button_keys_for_runtime(
                {"turn_on_mode", "battery_float_voltage"},
                "smg_modbus.json",
                collector_proxy_capture_allowed=False,
            ),
            (
                "create_support_package",
                "apply_collector_changes",
                "rediscover_collector",
                "reboot_collector",
            ),
        )
        self.assertEqual(
            default_enabled_tooling_button_keys_for_runtime(
                {"inverter_date_write", "inverter_time_write"},
                "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            ),
            (
                "create_support_package",
                "apply_collector_changes",
                "rediscover_collector",
                "reboot_collector",
                "start_proxy_capture",
                "stop_proxy_capture",
                "sync_inverter_clock",
            ),
        )
        self.assertEqual(
            tooling_button_keys_for_runtime(
                {"inverter_date_write", "inverter_time_write"},
                "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            ),
            (
                "create_support_package",
                "reload_local_metadata",
                "create_local_profile_draft",
                "create_local_schema_draft",
                "apply_collector_changes",
                "rediscover_collector",
                "reboot_collector",
                "start_proxy_capture",
                "stop_proxy_capture",
                "sync_inverter_clock",
            ),
        )
        self.assertEqual(
            tooling_button_keys_for_runtime(
                {"inverter_date_write", "inverter_time_write"},
                "smg_modbus.json",
            ),
            (
                "create_support_package",
                "reload_local_metadata",
                "create_local_profile_draft",
                "create_local_schema_draft",
                "apply_collector_changes",
                "rediscover_collector",
                "reboot_collector",
                "start_proxy_capture",
                "stop_proxy_capture",
            ),
        )
        self.assertEqual(
            tooling_button_keys_for_runtime(
                {"inverter_date_write", "inverter_time_write"},
                "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
                collector_proxy_capture_allowed=False,
            ),
            (
                "create_support_package",
                "reload_local_metadata",
                "create_local_profile_draft",
                "create_local_schema_draft",
                "apply_collector_changes",
                "rediscover_collector",
                "reboot_collector",
                "sync_inverter_clock",
            ),
        )

    def test_current_runtime_default_enabled_unique_ids_include_clock_sync_tool_only_when_supported(self) -> None:
        date_write = WriteCapability(
            key="inverter_date_write",
            register=696,
            value_kind="date_words",
            note="",
            tested=True,
            enabled_default=False,
        )
        time_write = WriteCapability(
            key="inverter_time_write",
            register=699,
            value_kind="time_words",
            note="",
            tested=True,
            enabled_default=False,
        )
        inverter = type(
            "FakeInverter",
            (),
            {"capabilities": (date_write, time_write), "capability_presets": ()},
        )()

        with (
            patch(
                "custom_components.eybond_local.drivers.registry.measurements_for_runtime",
                return_value=(),
            ),
            patch(
                "custom_components.eybond_local.drivers.registry.binary_sensors_for_runtime",
                return_value=(),
            ),
            patch.dict(sys.modules, _runtime_entity_key_module_stubs()),
        ):
            unique_ids = _default_enabled_unique_ids_for_current_runtime(
                "entry123",
                types.SimpleNamespace(async_set_proxy_capture_duration_minutes=AsyncMock()),
                None,
                inverter,
                lambda capability: capability.key in {"inverter_date_write", "inverter_time_write"},
                lambda _preset: True,
            )

        self.assertIn("entry123_tool_create_support_package", unique_ids)
        self.assertIn("entry123_tool_apply_collector_changes", unique_ids)
        self.assertIn("entry123_tool_reboot_collector", unique_ids)
        self.assertIn("entry123_tool_start_proxy_capture", unique_ids)
        self.assertIn("entry123_tool_stop_proxy_capture", unique_ids)
        self.assertIn("entry123_tool_sync_inverter_clock", unique_ids)

    def test_current_runtime_default_enabled_unique_ids_skip_proxy_entities_when_collector_forbids_proxy(
        self,
    ) -> None:
        coordinator = types.SimpleNamespace(
            async_set_proxy_capture_duration_minutes=AsyncMock(),
            collector_capabilities=types.SimpleNamespace(proxy_capture=False),
            data=types.SimpleNamespace(inverter=None),
        )

        with (
            patch(
                "custom_components.eybond_local.drivers.registry.measurements_for_runtime",
                return_value=(),
            ),
            patch(
                "custom_components.eybond_local.drivers.registry.binary_sensors_for_runtime",
                return_value=(),
            ),
            patch.dict(sys.modules, _runtime_entity_key_module_stubs()),
        ):
            unique_ids = _default_enabled_unique_ids_for_current_runtime(
                "entry123",
                coordinator,
                None,
                None,
                lambda _capability: True,
                lambda _preset: True,
                has_inverter_identity=False,
            )

        self.assertIn("entry123_tool_create_support_package", unique_ids)
        self.assertIn("entry123_tool_reboot_collector", unique_ids)
        self.assertNotIn("entry123_tool_start_proxy_capture", unique_ids)
        self.assertNotIn("entry123_tool_stop_proxy_capture", unique_ids)
        self.assertNotIn("entry123_number_proxy_capture_duration_minutes", unique_ids)

    def test_self_heal_reenables_existing_integration_disabled_tool_entity(self) -> None:
        async def _run() -> None:
            entity_entry = types.SimpleNamespace(
                unique_id="entry123_tool_apply_collector_changes",
                entity_id="button.smg_6200_collector_apply_collector_changes",
                disabled_by="integration",
            )

            class _Registry:
                def __init__(self) -> None:
                    self.updated: list[tuple[str, object | None]] = []

                def async_update_entity(self, entity_id: str, *, disabled_by=None) -> None:
                    self.updated.append((entity_id, disabled_by))

            registry = _Registry()

            helpers_module = types.ModuleType("homeassistant.helpers")
            entity_registry_module = types.ModuleType("homeassistant.helpers.entity_registry")

            def async_get(_hass):
                return registry

            def async_entries_for_config_entry(_registry, _entry_id):
                return [entity_entry]

            class RegistryEntryDisabler:
                INTEGRATION = object()

            entity_registry_module.async_get = async_get
            entity_registry_module.async_entries_for_config_entry = async_entries_for_config_entry
            entity_registry_module.RegistryEntryDisabler = RegistryEntryDisabler
            helpers_module.entity_registry = entity_registry_module

            async def async_add_executor_job(func, *args):
                return func(*args)

            hass = types.SimpleNamespace(async_add_executor_job=async_add_executor_job)
            entry = types.SimpleNamespace(entry_id="entry123")
            coordinator = types.SimpleNamespace(
                current_driver=None,
                identified_inverter=None,
                data=types.SimpleNamespace(inverter=None),
                can_expose_capability=lambda _capability: True,
                can_expose_preset=lambda _preset: True,
            )

            with (
                patch.dict(
                    sys.modules,
                    {
                        "homeassistant.helpers": helpers_module,
                        "homeassistant.helpers.entity_registry": entity_registry_module,
                    },
                ),
                patch(
                    "custom_components.eybond_local._default_enabled_unique_ids_for_current_runtime",
                    return_value={"entry123_tool_apply_collector_changes"},
                ),
            ):
                await _async_self_heal_enabled_defaults(hass, entry, coordinator)

            self.assertEqual(
                registry.updated,
                [("button.smg_6200_collector_apply_collector_changes", None)],
            )

        asyncio.run(_run())

    def test_self_heal_collector_cloud_family_from_unique_registry_ip(self) -> None:
        from custom_components.eybond_local.support.collector_registry import (
            remember_collector_original_endpoint,
        )

        async def _run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                remember_collector_original_endpoint(
                    config_dir=Path(tmp),
                    collector_pn="E50000200000000001",
                    original_endpoint_raw="iot.eybond.com,18899,TCP",
                    cloud_profile_key="valuecloud_at",
                    source="test_registry",
                    observed_at="2026-06-24T20:52:14+00:00",
                    last_seen_ip="192.168.8.110",
                )
                updates: list[dict[str, object]] = []

                async def async_add_executor_job(func, *args):
                    return func(*args)

                class _ConfigEntries:
                    def async_update_entry(self, entry, *, data=None, options=None, title=None) -> None:
                        del entry, title
                        updates.append({"data": data or {}, "options": options or {}})

                hass = types.SimpleNamespace(
                    config=types.SimpleNamespace(config_dir=tmp),
                    async_add_executor_job=async_add_executor_job,
                    config_entries=_ConfigEntries(),
                )
                entry = types.SimpleNamespace(
                    data={
                        "collector_pn": "E5000020000000",
                        "collector_ip": "192.168.8.110",
                    },
                    options={},
                )

                await _async_self_heal_collector_cloud_family(hass, entry)

                self.assertEqual(len(updates), 1)
                self.assertEqual(updates[0]["data"]["collector_cloud_family"], "valuecloud_at")
                self.assertEqual(
                    updates[0]["options"]["collector_original_server_endpoint"],
                    "iot.eybond.com,18899,TCP",
                )
                self.assertEqual(
                    updates[0]["options"]["collector_original_server_endpoint_profile_key"],
                    "valuecloud_at",
                )

        asyncio.run(_run())

    def test_self_heal_valuecloud_driver_hint_migrates_stale_internal_key(self) -> None:
        async def _run() -> None:
            updates: list[dict[str, object]] = []

            class _ConfigEntries:
                def async_update_entry(self, entry, *, data=None, options=None, title=None) -> None:
                    del entry, title
                    updates.append({"data": data or {}, "options": options or {}})

            hass = types.SimpleNamespace(config_entries=_ConfigEntries())
            entry = types.SimpleNamespace(
                entry_id="entry123",
                data={
                    "collector_cloud_family": "valuecloud_at",
                    "driver_hint": "valuecloud_pi30",
                },
                options={"driver_hint": "valuecloud_pi30"},
            )

            await _async_self_heal_valuecloud_driver_hint(hass, entry)

            self.assertEqual(len(updates), 1)
            self.assertEqual(updates[0]["data"]["driver_hint"], "eybond_g_ascii")
            self.assertEqual(updates[0]["options"]["driver_hint"], "eybond_g_ascii")

        asyncio.run(_run())

    def test_self_heal_valuecloud_driver_hint_does_not_alias_other_families(self) -> None:
        async def _run() -> None:
            updates: list[dict[str, object]] = []

            class _ConfigEntries:
                def async_update_entry(self, entry, *, data=None, options=None, title=None) -> None:
                    del entry, data, options, title
                    updates.append({})

            hass = types.SimpleNamespace(config_entries=_ConfigEntries())
            entry = types.SimpleNamespace(
                entry_id="entry123",
                data={
                    "collector_cloud_family": "smartess_at",
                    "driver_hint": "valuecloud_pi30",
                },
                options={"driver_hint": "valuecloud_pi30"},
            )

            await _async_self_heal_valuecloud_driver_hint(hass, entry)

            self.assertEqual(updates, [])

        asyncio.run(_run())

    def test_self_heal_disables_expert_only_text_entity_outside_full_control(self) -> None:
        async def _run() -> None:
            entity_entry = types.SimpleNamespace(
                unique_id="entry123_text_collector_callback_endpoint",
                entity_id="text.smg_6200_collector_collector_callback_endpoint",
                disabled_by=None,
            )

            class _Registry:
                def __init__(self) -> None:
                    self.updated: list[tuple[str, object | None]] = []

                def async_update_entity(self, entity_id: str, *, disabled_by=None) -> None:
                    self.updated.append((entity_id, disabled_by))

            registry = _Registry()

            helpers_module = types.ModuleType("homeassistant.helpers")
            entity_registry_module = types.ModuleType("homeassistant.helpers.entity_registry")

            def async_get(_hass):
                return registry

            def async_entries_for_config_entry(_registry, _entry_id):
                return [entity_entry]

            class RegistryEntryDisabler:
                INTEGRATION = "integration"

            entity_registry_module.async_get = async_get
            entity_registry_module.async_entries_for_config_entry = async_entries_for_config_entry
            entity_registry_module.RegistryEntryDisabler = RegistryEntryDisabler
            helpers_module.entity_registry = entity_registry_module

            hass = types.SimpleNamespace()
            entry = types.SimpleNamespace(
                entry_id="entry123",
                runtime_data=types.SimpleNamespace(control_mode="auto"),
            )

            with patch.dict(
                sys.modules,
                {
                    "homeassistant.helpers": helpers_module,
                    "homeassistant.helpers.entity_registry": entity_registry_module,
                },
            ):
                await _async_self_heal_expert_defaults(hass, entry)

            self.assertEqual(
                registry.updated,
                [("text.smg_6200_collector_collector_callback_endpoint", "integration")],
            )

        asyncio.run(_run())

    def test_self_heal_reenables_expert_only_text_entity_in_full_control(self) -> None:
        async def _run() -> None:
            entity_entry = types.SimpleNamespace(
                unique_id="entry123_text_collector_callback_endpoint",
                entity_id="text.smg_6200_collector_collector_callback_endpoint",
                disabled_by="integration",
            )

            class _Registry:
                def __init__(self) -> None:
                    self.updated: list[tuple[str, object | None]] = []

                def async_update_entity(self, entity_id: str, *, disabled_by=None) -> None:
                    self.updated.append((entity_id, disabled_by))

            registry = _Registry()

            helpers_module = types.ModuleType("homeassistant.helpers")
            entity_registry_module = types.ModuleType("homeassistant.helpers.entity_registry")

            def async_get(_hass):
                return registry

            def async_entries_for_config_entry(_registry, _entry_id):
                return [entity_entry]

            class RegistryEntryDisabler:
                INTEGRATION = "integration"

            entity_registry_module.async_get = async_get
            entity_registry_module.async_entries_for_config_entry = async_entries_for_config_entry
            entity_registry_module.RegistryEntryDisabler = RegistryEntryDisabler
            helpers_module.entity_registry = entity_registry_module

            hass = types.SimpleNamespace()
            entry = types.SimpleNamespace(
                entry_id="entry123",
                runtime_data=types.SimpleNamespace(control_mode="full"),
            )

            with patch.dict(
                sys.modules,
                {
                    "homeassistant.helpers": helpers_module,
                    "homeassistant.helpers.entity_registry": entity_registry_module,
                },
            ):
                await _async_self_heal_expert_defaults(hass, entry)

            self.assertEqual(
                registry.updated,
                [("text.smg_6200_collector_collector_callback_endpoint", None)],
            )

        asyncio.run(_run())

    def test_cleanup_removes_capability_entities_hidden_by_current_control_mode(self) -> None:
        async def _run() -> None:
            capability = WriteCapability(
                key="output_mode",
                register=2,
                value_kind="enum",
                note="",
                tested=False,
                enum_map={0: "Utility", 1: "Battery"},
                enabled_default=True,
            )
            inverter = type(
                "FakeInverter",
                (),
                {
                    "capabilities": (capability,),
                    "capability_presets": (),
                    "profile_name": "smg_modbus.json",
                    "register_schema_name": "modbus_smg/models/smg_6200.json",
                },
            )()
            entity_entry = types.SimpleNamespace(
                unique_id="entry123_select_output_mode",
                entity_id="select.smg_6200_output_mode",
            )

            class _Registry:
                def __init__(self) -> None:
                    self.removed: list[str] = []

                def async_remove(self, entity_id: str) -> None:
                    self.removed.append(entity_id)

            registry = _Registry()

            coordinator = types.SimpleNamespace(
                current_driver=None,
                identified_inverter=inverter,
                data=types.SimpleNamespace(inverter=inverter),
                can_expose_capability=lambda _capability: False,
                can_expose_preset=lambda _preset: False,
            )
            hass = types.SimpleNamespace()
            entry = types.SimpleNamespace(entry_id="entry123")
            button_module = types.ModuleType("custom_components.eybond_local.button")
            button_module._tooling_button_specs = lambda: ()
            select_module = types.ModuleType("custom_components.eybond_local.select")
            select_module.runtime_select_keys_for_runtime = (
                lambda *, has_inverter_identity=True: ()
            )
            text_module = types.ModuleType("custom_components.eybond_local.text")
            text_module.collector_text_keys_for_runtime = lambda: ()
            tooling_module = types.ModuleType("custom_components.eybond_local.tooling")
            tooling_module.tooling_button_keys_for_runtime = (
                lambda capability_keys, profile_name, has_inverter_identity=True, **_kwargs: ()
            )
            derived_energy_module = types.ModuleType("custom_components.eybond_local.derived_energy")
            derived_energy_module.derived_energy_cycle_descriptions_for_keys = lambda _keys: ()
            derived_energy_module.derived_energy_descriptions_for_keys = lambda _keys: ()
            derived_energy_module.derived_energy_entity_descriptions_for_keys = lambda _keys: ()

            with (
                patch(
                    "homeassistant.helpers.entity_registry.async_get",
                    return_value=registry,
                ),
                patch(
                    "homeassistant.helpers.entity_registry.async_entries_for_config_entry",
                    return_value=[entity_entry],
                ),
                patch(
                    "custom_components.eybond_local.drivers.registry.measurements_for_runtime",
                    return_value=(),
                ),
                patch(
                    "custom_components.eybond_local.drivers.registry.binary_sensors_for_runtime",
                    return_value=(),
                ),
                patch.dict(
                    sys.modules,
                    {
                        "custom_components.eybond_local.button": button_module,
                        "custom_components.eybond_local.select": select_module,
                        "custom_components.eybond_local.text": text_module,
                        "custom_components.eybond_local.tooling": tooling_module,
                        "custom_components.eybond_local.derived_energy": derived_energy_module,
                    },
                ),
            ):
                await _async_cleanup_obsolete_entities(hass, entry, coordinator)

            self.assertEqual(registry.removed, ["select.smg_6200_output_mode"])

        asyncio.run(_run())

    def test_cleanup_removes_legacy_signal_sensor_entities(self) -> None:
        async def _run() -> None:
            legacy_signal_entry = types.SimpleNamespace(
                unique_id="entry123_collector_signal_strength",
                entity_id="sensor.collector_signal_strength",
            )
            onboarding_entry = types.SimpleNamespace(
                unique_id="entry123_collector_onboarding_status",
                entity_id="sensor.collector_onboarding_status",
            )

            class _Registry:
                def __init__(self) -> None:
                    self.removed: list[str] = []

                def async_remove(self, entity_id: str) -> None:
                    self.removed.append(entity_id)

            registry = _Registry()
            inverter = types.SimpleNamespace(
                capabilities=(),
                capability_presets=(),
                profile_name="",
                register_schema_name="",
            )
            coordinator = types.SimpleNamespace(
                collector_cloud_family="legacy_binary",
                current_driver=None,
                identified_inverter=inverter,
                data=types.SimpleNamespace(inverter=inverter),
                can_expose_capability=lambda _capability: True,
                can_expose_preset=lambda _preset: True,
            )
            hass = types.SimpleNamespace()
            entry = types.SimpleNamespace(entry_id="entry123")
            button_module = types.ModuleType("custom_components.eybond_local.button")
            button_module._tooling_button_specs = lambda: ()
            select_module = types.ModuleType("custom_components.eybond_local.select")
            select_module.runtime_select_keys_for_runtime = (
                lambda *, has_inverter_identity=True: ()
            )
            text_module = types.ModuleType("custom_components.eybond_local.text")
            text_module.collector_text_keys_for_runtime = lambda: ()
            tooling_module = types.ModuleType("custom_components.eybond_local.tooling")
            tooling_module.tooling_button_keys_for_runtime = (
                lambda capability_keys, profile_name, has_inverter_identity=True, **_kwargs: ()
            )
            derived_energy_module = types.ModuleType("custom_components.eybond_local.derived_energy")
            derived_energy_module.derived_energy_cycle_descriptions_for_keys = lambda _keys: ()
            derived_energy_module.derived_energy_descriptions_for_keys = lambda _keys: ()
            derived_energy_module.derived_energy_entity_descriptions_for_keys = lambda _keys: ()

            with (
                patch("homeassistant.helpers.entity_registry.async_get", return_value=registry),
                patch(
                    "homeassistant.helpers.entity_registry.async_entries_for_config_entry",
                    return_value=[legacy_signal_entry, onboarding_entry],
                ),
                patch(
                    "custom_components.eybond_local.drivers.registry.measurements_for_runtime",
                    return_value=(
                        MeasurementDescription(
                            key="collector_signal_strength",
                            name="Collector Signal Strength",
                            enabled_default=True,
                        ),
                        MeasurementDescription(
                            key="collector_onboarding_status",
                            name="Collector Onboarding Status",
                            enabled_default=True,
                        ),
                    ),
                ),
                patch(
                    "custom_components.eybond_local.drivers.registry.binary_sensors_for_runtime",
                    return_value=(),
                ),
                patch.dict(
                    sys.modules,
                    {
                        "custom_components.eybond_local.button": button_module,
                        "custom_components.eybond_local.select": select_module,
                        "custom_components.eybond_local.text": text_module,
                        "custom_components.eybond_local.tooling": tooling_module,
                        "custom_components.eybond_local.derived_energy": derived_energy_module,
                    },
                ),
            ):
                await _async_cleanup_obsolete_entities(hass, entry, coordinator)

            self.assertEqual(registry.removed, ["sensor.collector_signal_strength"])

        asyncio.run(_run())

    def test_cleanup_skips_when_effective_metadata_snapshot_is_not_safe(self) -> None:
        async def _run() -> None:
            entity_entry = types.SimpleNamespace(
                unique_id="entry123_pv2_power",
                entity_id="sensor.anenji_pv2_power",
            )

            class _Registry:
                def __init__(self) -> None:
                    self.removed: list[str] = []

                def async_remove(self, entity_id: str) -> None:
                    self.removed.append(entity_id)

            registry = _Registry()
            coordinator = types.SimpleNamespace(
                identified_inverter=None,
                effective_metadata_snapshot=types.SimpleNamespace(is_valid=False),
                effective_metadata=None,
                current_driver=None,
                data=types.SimpleNamespace(inverter=None),
                can_expose_capability=lambda _capability: True,
                can_expose_preset=lambda _preset: True,
            )
            hass = types.SimpleNamespace()
            entry = types.SimpleNamespace(entry_id="entry123")

            with (
                patch("homeassistant.helpers.entity_registry.async_get", return_value=registry),
                patch(
                    "homeassistant.helpers.entity_registry.async_entries_for_config_entry",
                    return_value=[entity_entry],
                ),
                self.assertLogs("custom_components.eybond_local", level="DEBUG") as logs,
            ):
                await _async_cleanup_obsolete_entities(hass, entry, coordinator)

            self.assertEqual(registry.removed, [])
            self.assertTrue(
                any("Skipping obsolete entity cleanup" in message for message in logs.output)
            )

        asyncio.run(_run())

    def test_cleanup_skips_when_snapshot_falls_back_to_different_profile(self) -> None:
        async def _run() -> None:
            entity_entry = types.SimpleNamespace(
                unique_id="entry123_pv2_power",
                entity_id="sensor.anenji_pv2_power",
            )

            class _Registry:
                def __init__(self) -> None:
                    self.removed: list[str] = []

                def async_remove(self, entity_id: str) -> None:
                    self.removed.append(entity_id)

            registry = _Registry()
            coordinator = types.SimpleNamespace(
                identified_inverter=None,
                effective_metadata_snapshot=types.SimpleNamespace(
                    is_valid=True,
                    effective_owner_key="modbus_smg",
                    profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                    register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
                ),
                effective_metadata=types.SimpleNamespace(
                    effective_owner_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="smg_modbus.json",
                    profile_metadata=types.SimpleNamespace(source_name="smg_modbus.json"),
                    register_schema_metadata=types.SimpleNamespace(source_name="smg_modbus.json"),
                ),
                current_driver=None,
                data=types.SimpleNamespace(inverter=None),
                can_expose_capability=lambda _capability: True,
                can_expose_preset=lambda _preset: True,
            )
            hass = types.SimpleNamespace()
            entry = types.SimpleNamespace(entry_id="entry123")

            with (
                patch("homeassistant.helpers.entity_registry.async_get", return_value=registry),
                patch(
                    "homeassistant.helpers.entity_registry.async_entries_for_config_entry",
                    return_value=[entity_entry],
                ),
                self.assertLogs("custom_components.eybond_local", level="DEBUG") as logs,
            ):
                await _async_cleanup_obsolete_entities(hass, entry, coordinator)

            self.assertEqual(registry.removed, [])
            self.assertTrue(
                any("effective_metadata_mismatch_from_snapshot" in message for message in logs.output)
            )

        asyncio.run(_run())

    def test_cleanup_uses_safe_snapshot_and_preserves_anenji_pv2_entity(self) -> None:
        async def _run() -> None:
            pv2_entry = types.SimpleNamespace(
                unique_id="entry123_pv2_power",
                entity_id="sensor.anenji_pv2_power",
            )
            legacy_entry = types.SimpleNamespace(
                unique_id="entry123_legacy_metric",
                entity_id="sensor.legacy_metric",
            )

            class _Registry:
                def __init__(self) -> None:
                    self.removed: list[str] = []

                def async_remove(self, entity_id: str) -> None:
                    self.removed.append(entity_id)

            registry = _Registry()
            fake_driver = types.SimpleNamespace(
                key="modbus_smg",
                write_capabilities=(),
                capability_presets=(),
            )
            coordinator = types.SimpleNamespace(
                identified_inverter=None,
                effective_metadata_snapshot=types.SimpleNamespace(
                    is_valid=True,
                    effective_owner_key="modbus_smg",
                    profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                    register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
                ),
                effective_metadata=types.SimpleNamespace(
                    effective_owner_key="modbus_smg",
                    profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                    register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
                    profile_metadata=types.SimpleNamespace(
                        driver_key="modbus_smg",
                        protocol_family="modbus_smg",
                        source_name="modbus_smg/models/anenji_4200_protocol_1.json",
                        groups=(),
                        capabilities=(),
                        presets=(),
                    ),
                    register_schema_metadata=types.SimpleNamespace(
                        driver_key="modbus_smg",
                        protocol_family="modbus_smg",
                        source_name="modbus_smg/models/anenji_4200_protocol_1.json",
                    ),
                ),
                current_driver=None,
                data=types.SimpleNamespace(inverter=None),
                can_expose_capability=lambda _capability: True,
                can_expose_preset=lambda _preset: True,
            )
            hass = types.SimpleNamespace()
            entry = types.SimpleNamespace(entry_id="entry123")
            button_module = types.ModuleType("custom_components.eybond_local.button")
            button_module._tooling_button_specs = lambda: ()
            select_module = types.ModuleType("custom_components.eybond_local.select")
            select_module.runtime_select_keys_for_runtime = (
                lambda *, has_inverter_identity=True: ()
            )
            text_module = types.ModuleType("custom_components.eybond_local.text")
            text_module.collector_text_keys_for_runtime = lambda: ()
            tooling_module = types.ModuleType("custom_components.eybond_local.tooling")
            tooling_module.tooling_button_keys_for_runtime = (
                lambda capability_keys, profile_name, has_inverter_identity=True, **_kwargs: ()
            )
            derived_energy_module = types.ModuleType("custom_components.eybond_local.derived_energy")
            derived_energy_module.derived_energy_cycle_descriptions_for_keys = lambda _keys: ()
            derived_energy_module.derived_energy_descriptions_for_keys = lambda _keys: ()
            derived_energy_module.derived_energy_entity_descriptions_for_keys = lambda _keys: ()

            with (
                patch("homeassistant.helpers.entity_registry.async_get", return_value=registry),
                patch(
                    "homeassistant.helpers.entity_registry.async_entries_for_config_entry",
                    return_value=[pv2_entry, legacy_entry],
                ),
                patch(
                    "custom_components.eybond_local.platform_context.get_driver",
                    side_effect=lambda key: fake_driver if key == fake_driver.key else None,
                ),
                patch(
                    "custom_components.eybond_local.drivers.registry.measurements_for_runtime",
                    return_value=(
                        MeasurementDescription(
                            key="pv2_power",
                            name="PV2 Power",
                            enabled_default=True,
                        ),
                    ),
                ),
                patch(
                    "custom_components.eybond_local.drivers.registry.binary_sensors_for_runtime",
                    return_value=(),
                ),
                patch.dict(
                    sys.modules,
                    {
                        "custom_components.eybond_local.button": button_module,
                        "custom_components.eybond_local.select": select_module,
                        "custom_components.eybond_local.text": text_module,
                        "custom_components.eybond_local.tooling": tooling_module,
                        "custom_components.eybond_local.derived_energy": derived_energy_module,
                    },
                ),
            ):
                await _async_cleanup_obsolete_entities(hass, entry, coordinator)

            self.assertEqual(registry.removed, ["sensor.legacy_metric"])

        asyncio.run(_run())

    def test_remove_legacy_runtime_select_entities_removes_control_mode_select(self) -> None:
        async def _run() -> None:
            legacy_control_mode_entry = types.SimpleNamespace(
                unique_id="entry123_select_control_mode",
                entity_id="select.smg_6200_control_mode",
            )
            runtime_select_entry = types.SimpleNamespace(
                unique_id="entry123_select_collector_operation_mode",
                entity_id="select.collector_pn_e50000200000000001_collector_operation_mode",
            )

            class _Registry:
                def __init__(self) -> None:
                    self.removed: list[str] = []

                def async_remove(self, entity_id: str) -> None:
                    self.removed.append(entity_id)

            registry = _Registry()
            hass = types.SimpleNamespace()
            entry = types.SimpleNamespace(entry_id="entry123")

            with (
                patch("homeassistant.helpers.entity_registry.async_get", return_value=registry),
                patch(
                    "homeassistant.helpers.entity_registry.async_entries_for_config_entry",
                    return_value=[legacy_control_mode_entry, runtime_select_entry],
                ),
            ):
                await _async_remove_legacy_runtime_select_entities(hass, entry)

            self.assertEqual(registry.removed, ["select.smg_6200_control_mode"])

        asyncio.run(_run())

    def test_self_heal_updates_legacy_inverter_first_entry_title(self) -> None:
        async def _run() -> None:
            updated: list[tuple[object, str]] = []

            class _ConfigEntries:
                def async_update_entry(self, entry, *, title=None, data=None, options=None) -> None:
                    del data, options
                    updated.append((entry, title))

            hass = types.SimpleNamespace(config_entries=_ConfigEntries())
            entry = types.SimpleNamespace(
                entry_id="entry123",
                title="SMG 6200 (92632500000001)",
                data={
                    "collector_pn": "E5000020000000",
                    "collector_ip": "192.168.1.55",
                    "server_ip": "192.168.1.50",
                    "detected_model": "SMG 6200",
                    "detected_serial": "92632500000001",
                },
            )

            await _async_self_heal_entry_title(hass, entry)

            self.assertEqual(updated, [(entry, "Collector PN E5000020000000")])

        asyncio.run(_run())

    def test_initial_refresh_for_setup_continues_on_timeout(self) -> None:
        async def _run() -> None:
            refresh_started = asyncio.Event()
            release_refresh = asyncio.Event()
            created_tasks: list[asyncio.Task] = []
            unload_callbacks: list[object] = []

            async def async_refresh() -> None:
                refresh_started.set()
                await release_refresh.wait()

            def async_create_task(coro):
                task = asyncio.create_task(coro)
                created_tasks.append(task)
                return task

            hass = types.SimpleNamespace(async_create_task=async_create_task)
            entry = types.SimpleNamespace(
                entry_id="entry123",
                async_on_unload=unload_callbacks.append,
            )
            coordinator = types.SimpleNamespace(async_refresh=async_refresh)

            with patch("custom_components.eybond_local._SETUP_INITIAL_REFRESH_TIMEOUT", 0.01):
                await _async_initial_refresh_for_setup(hass, entry, coordinator)

            await refresh_started.wait()
            self.assertEqual(len(created_tasks), 1)
            self.assertEqual(len(unload_callbacks), 1)
            self.assertFalse(created_tasks[0].done())

            unload_callbacks[0]()
            with self.assertRaises(asyncio.CancelledError):
                await created_tasks[0]

        asyncio.run(_run())

    def test_finalize_expert_entity_migration_times_out_block_till_done(self) -> None:
        async def _run() -> None:
            async def async_block_till_done() -> None:
                await asyncio.Event().wait()

            hass = types.SimpleNamespace(async_block_till_done=async_block_till_done)
            entry = types.SimpleNamespace(entry_id="entry123", runtime_data=object())

            with (
                patch("custom_components.eybond_local._EXPERT_ENTITY_MIGRATION_SETTLE_TIMEOUT", 0.01),
                patch(
                    "custom_components.eybond_local._async_self_heal_expert_defaults",
                    new=AsyncMock(),
                ) as self_heal,
                patch(
                    "custom_components.eybond_local._async_remove_legacy_runtime_select_entities",
                    new=AsyncMock(),
                ) as legacy_cleanup,
                patch(
                    "custom_components.eybond_local._async_self_heal_sensor_display_precision",
                    new=AsyncMock(),
                ) as sensor_precision,
            ):
                await _async_finalize_expert_entity_migration(hass, entry)

            self_heal.assert_awaited_once_with(hass, entry)
            legacy_cleanup.assert_awaited_once_with(hass, entry)
            sensor_precision.assert_awaited_once_with(hass, entry)

        asyncio.run(_run())

    def test_async_setup_entry_continues_after_initial_refresh_timeout(self) -> None:
        async def _run() -> None:
            refresh_started = asyncio.Event()
            release_refresh = asyncio.Event()
            created_tasks: list[asyncio.Task] = []
            unload_callbacks: list[object] = []
            forwarded: list[tuple[object, object]] = []
            stop_listeners: list[object] = []

            class _ConfigEntries:
                async def async_forward_entry_setups(self, entry, platforms) -> None:
                    forwarded.append((entry, tuple(platforms)))

            async def async_add_executor_job(func, *args):
                return func(*args)

            def async_create_task(coro):
                task = asyncio.create_task(coro)
                created_tasks.append(task)
                return task

            class _Bus:
                def async_listen_once(self, event_type: str, callback):
                    stop_listeners.append((event_type, callback))

                    def _unsub() -> None:
                        return None

                    return _unsub

            hass = types.SimpleNamespace(
                async_add_executor_job=async_add_executor_job,
                async_create_task=async_create_task,
                bus=_Bus(),
                config_entries=_ConfigEntries(),
            )

            class _Entry:
                def __init__(self) -> None:
                    self.entry_id = "entry123"
                    self.data = {"driver_hint": "pi30"}
                    self.options = {}
                    self.title = "Collector 192.168.88.88"
                    self.runtime_data = None

                def async_on_unload(self, callback) -> None:
                    unload_callbacks.append(callback)

                def add_update_listener(self, listener):
                    return listener

            entry = _Entry()

            class _FakeCoordinator:
                def __init__(self, _hass, _entry) -> None:
                    self.data = types.SimpleNamespace(inverter=None)
                    self.current_driver = None
                    self.platforms_initialized = False
                    self.shutdown_calls = 0

                async def async_setup(self) -> None:
                    return None

                async def async_refresh(self) -> None:
                    refresh_started.set()
                    await release_refresh.wait()

                async def async_shutdown(self) -> None:
                    self.shutdown_calls += 1

                def async_sync_device_registry(self) -> None:
                    return None

                @property
                def has_inverter_identity(self) -> bool:
                    return bool(getattr(self.data, "inverter", None))

                def mark_entity_platforms_initialized(
                    self,
                    *,
                    has_inverter_identity=None,
                    has_driver_fallback=None,
                ) -> None:
                    self.platforms_started_with_inverter_identity = has_inverter_identity
                    self.platforms_started_with_driver_fallback = has_driver_fallback
                    self.platforms_initialized = True

            runtime_coordinator_module = types.ModuleType(
                "custom_components.eybond_local.runtime.coordinator"
            )
            runtime_coordinator_module.EybondLocalCoordinator = _FakeCoordinator

            with (
                patch("custom_components.eybond_local._configure_local_metadata_roots"),
                patch("custom_components.eybond_local._prime_metadata_caches"),
                patch(
                    "custom_components.eybond_local.services.async_setup_services",
                    new=AsyncMock(),
                ) as setup_services,
                patch("custom_components.eybond_local._async_self_heal_server_ip", new=AsyncMock()),
                patch("custom_components.eybond_local._async_self_heal_entry_title", new=AsyncMock()),
                patch("custom_components.eybond_local._async_self_heal_enabled_defaults", new=AsyncMock()),
                patch("custom_components.eybond_local._async_cleanup_obsolete_entities", new=AsyncMock()),
                patch("custom_components.eybond_local._async_finalize_expert_entity_migration", new=AsyncMock()),
                patch("custom_components.eybond_local._SETUP_INITIAL_REFRESH_TIMEOUT", 0.01),
                patch.dict(
                    sys.modules,
                    {
                        "custom_components.eybond_local.runtime.coordinator": runtime_coordinator_module,
                    },
                ),
            ):
                result = await async_setup_entry(hass, entry)

            self.assertTrue(result)
            self.assertIsNotNone(entry.runtime_data)
            self.assertTrue(entry.runtime_data.platforms_initialized)
            setup_services.assert_awaited_once_with(hass)
            self.assertEqual(len(forwarded), 1)
            self.assertGreaterEqual(len(unload_callbacks), 2)
            self.assertEqual(len(stop_listeners), 2)
            self.assertEqual(stop_listeners[0][0], "homeassistant_stop")
            self.assertEqual(stop_listeners[1][0], "homeassistant_started")
            await stop_listeners[0][1](types.SimpleNamespace())
            self.assertEqual(entry.runtime_data.shutdown_calls, 1)
            await refresh_started.wait()

            refresh_task = created_tasks[0]
            self.assertFalse(refresh_task.done())
            refresh_cancel_callback = next(
                callback
                for callback in unload_callbacks
                if getattr(callback, "args", ()) == (refresh_task,)
            )
            refresh_cancel_callback()
            with self.assertRaises(asyncio.CancelledError):
                await refresh_task

        asyncio.run(_run())

    def test_async_setup_entry_retries_transient_listener_bind_failure(self) -> None:
        async def _run() -> None:
            coordinators: list[object] = []

            async def async_add_executor_job(func, *args):
                return func(*args)

            class _ConfigEntries:
                async def async_forward_entry_setups(self, entry, platforms) -> None:
                    raise AssertionError("platforms should not be forwarded after bind failure")

            hass = types.SimpleNamespace(
                async_add_executor_job=async_add_executor_job,
                config_entries=_ConfigEntries(),
            )

            class _Entry:
                def __init__(self) -> None:
                    self.entry_id = "entry123"
                    self.data = {"driver_hint": "pi30"}
                    self.options = {}
                    self.title = "Collector 192.168.1.55"
                    self.runtime_data = None

                def async_on_unload(self, callback) -> None:
                    return None

                def add_update_listener(self, listener):
                    return listener

            entry = _Entry()

            class _FakeCoordinator:
                def __init__(self, _hass, _entry) -> None:
                    self.shutdown_calls = 0
                    coordinators.append(self)

                async def async_setup(self) -> None:
                    raise CollectorListenerBindError(
                        "192.168.1.50",
                        8899,
                        OSError(
                            "could not bind on any address out of [('192.168.1.50', 8899)]"
                        ),
                    )

                async def async_shutdown(self) -> None:
                    self.shutdown_calls += 1

            runtime_coordinator_module = types.ModuleType(
                "custom_components.eybond_local.runtime.coordinator"
            )
            runtime_coordinator_module.EybondLocalCoordinator = _FakeCoordinator

            with (
                patch("custom_components.eybond_local._configure_local_metadata_roots"),
                patch("custom_components.eybond_local._prime_metadata_caches"),
                patch(
                    "custom_components.eybond_local.services.async_setup_services",
                    new=AsyncMock(),
                ),
                patch("custom_components.eybond_local._async_self_heal_server_ip", new=AsyncMock()),
                patch("custom_components.eybond_local._async_self_heal_entry_title", new=AsyncMock()),
                patch.dict(
                    sys.modules,
                    {
                        "custom_components.eybond_local.runtime.coordinator": runtime_coordinator_module,
                    },
                ),
            ):
                with self.assertRaises(ConfigEntryNotReady):
                    await async_setup_entry(hass, entry)

            self.assertIsNone(entry.runtime_data)
            self.assertEqual(len(coordinators), 1)
            self.assertEqual(coordinators[0].shutdown_calls, 1)

        asyncio.run(_run())


class StopShutdownHookTests(unittest.TestCase):
    """The HA-stop hook must never keep shutdown hostage to a hung teardown."""

    @staticmethod
    def _registered_hook(coordinator):
        captured = {}

        class _Bus:
            def async_listen_once(self, _event, callback):
                captured["callback"] = callback
                return lambda: None

        class _Hass:
            bus = _Bus()

        class _Entry:
            entry_id = "entry-stop-test"

            def async_on_unload(self, _unsub) -> None:
                return None

        _register_entry_stop_shutdown(_Hass(), _Entry(), coordinator)
        return captured["callback"]

    def test_stop_hook_awaits_clean_shutdown(self) -> None:
        coordinator = types.SimpleNamespace(async_shutdown=AsyncMock())

        async def _run() -> None:
            hook = self._registered_hook(coordinator)
            await hook(None)

        asyncio.run(_run())
        coordinator.async_shutdown.assert_awaited_once()

    def test_stop_hook_abandons_hung_shutdown_after_timeout(self) -> None:
        release = asyncio.Event()

        async def _hung_shutdown() -> None:
            await release.wait()

        coordinator = types.SimpleNamespace(async_shutdown=_hung_shutdown)

        async def _run() -> None:
            hook = self._registered_hook(coordinator)
            with patch("custom_components.eybond_local._STOP_SHUTDOWN_TIMEOUT", 0.05):
                with self.assertLogs(
                    "custom_components.eybond_local", level="WARNING"
                ) as logs:
                    await asyncio.wait_for(hook(None), timeout=5.0)
            self.assertTrue(
                any("did not finish" in line for line in logs.output)
            )
            release.set()

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
