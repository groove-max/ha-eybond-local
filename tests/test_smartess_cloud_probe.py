from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


_PROBE_PATH = REPO_ROOT / ".local" / "tools" / "smartess_cloud_probe.py"
if not _PROBE_PATH.is_file():
    raise unittest.SkipTest(f"smartess_cloud_probe not present at {_PROBE_PATH}")
_spec = importlib.util.spec_from_file_location(
    "smartess_cloud_probe", _PROBE_PATH
)
assert _spec is not None and _spec.loader is not None
smartess_cloud_probe = importlib.util.module_from_spec(_spec)
sys.modules["smartess_cloud_probe"] = smartess_cloud_probe
_spec.loader.exec_module(smartess_cloud_probe)


class SmartEssCloudProbeTests(unittest.TestCase):
    def test_parser_accepts_device_bundle_command(self) -> None:
        parser = smartess_cloud_probe._build_parser()

        args = parser.parse_args(
            [
                "device-bundle",
                "--token",
                "token123456789",
                "--secret",
                "secret123456789",
                "--pn",
                "E50000200000000001",
                "--sn",
                "E50000200000000001000001",
                "--devcode",
                "0x0948",
                "--devaddr",
                "0x01",
            ]
        )

        self.assertEqual(args.command, "device-bundle")
        self.assertEqual(args.pagesize, 50)
        self.assertIs(args.func, smartess_cloud_probe._run_device_bundle)

    def test_parser_accepts_learn_settings_command(self) -> None:
        parser = smartess_cloud_probe._build_parser()

        args = parser.parse_args(
            [
                "learn-settings",
                "--token",
                "token123456789",
                "--secret",
                "secret123456789",
                "--pn",
                "E50000200000000001",
                "--sn",
                "E50000200000000001000001",
                "--devcode",
                "2376",
                "--devaddr",
                "1",
                "--field-id",
                "sys_eybond_ctrl_53",
                "--dry-run",
            ]
        )

        self.assertEqual(args.command, "learn-settings")
        self.assertEqual(args.field_id, ["sys_eybond_ctrl_53"])
        self.assertIs(args.func, smartess_cloud_probe._run_learn_settings)

    def test_build_device_control_action_uses_ctrl_device_fields(self) -> None:
        action = smartess_cloud_probe.build_device_control_action(
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            field_id="sys_eybond_ctrl_53",
            value="1",
        )

        self.assertEqual(
            action,
            (
                "&action=ctrlDevice"
                "&pn=E50000200000000001"
                "&sn=E50000200000000001000001"
                "&devcode=2376"
                "&devaddr=1"
                "&id=sys_eybond_ctrl_53"
                "&val=1"
            ),
        )

    def test_learn_settings_dry_run_plans_choice_fields(self) -> None:
        args = argparse.Namespace(
            username="",
            password="",
            token="token123456789",
            secret="secret123456789",
            base_url=smartess_cloud_probe.DEFAULT_BASE_URL,
            language=smartess_cloud_probe.DEFAULT_LANGUAGE,
            app_id=smartess_cloud_probe.DEFAULT_APP_ID,
            app_version=smartess_cloud_probe.DEFAULT_APP_VERSION,
            company_key=smartess_cloud_probe.DEFAULT_COMPANY_KEY,
            timeout=smartess_cloud_probe.DEFAULT_TIMEOUT,
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            field_id=[],
            include_numeric=False,
            numeric_value=smartess_cloud_probe.DEFAULT_LEARN_NUMERIC_VALUE,
            all_choice_values=False,
            max_fields=0,
            delay_seconds=0,
            output="",
            dry_run=True,
            confirm_cloud_write=False,
            continue_on_error=True,
        )
        settings_envelope = smartess_cloud_probe.ApiEnvelope(
            err=0,
            desc="ok",
            dat={
                "field": [
                    {
                        "id": "sys_eybond_ctrl_53",
                        "name": "Backlight Control",
                        "item": [
                            {"key": "0", "val": "Backlight Timing Off"},
                            {"key": "1", "val": "Backlight On"},
                        ],
                    },
                    {
                        "id": "bat_eybond_ctrl_76",
                        "name": "Max.Charging Current",
                        "unit": "A",
                    },
                ],
                "two_tier": {},
            },
            raw={},
        )

        with patch.object(
            smartess_cloud_probe,
            "fetch_signed_action",
            return_value=settings_envelope,
        ) as fetch:
            payload = smartess_cloud_probe._run_learn_settings(args)

        self.assertEqual(fetch.call_count, 1)
        normalized = payload["normalized"]
        self.assertEqual(normalized["planned_write_count"], 1)
        self.assertEqual(normalized["results"][0]["field_id"], "sys_eybond_ctrl_53")
        self.assertEqual(normalized["results"][0]["value"], "0")
        self.assertEqual(normalized["results"][0]["status"], "planned")

    def test_learn_settings_requires_confirm_for_live_writes(self) -> None:
        args = argparse.Namespace(
            username="",
            password="",
            token="token123456789",
            secret="secret123456789",
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=False,
        )

        with self.assertRaisesRegex(
            smartess_cloud_probe.SmartEssCloudError,
            "requires_confirm_cloud_write",
        ):
            smartess_cloud_probe._run_learn_settings(args)

    def test_device_bundle_collects_all_cloud_payloads(self) -> None:
        args = argparse.Namespace(
            username="",
            password="",
            token="token123456789",
            secret="secret123456789",
            base_url=smartess_cloud_probe.DEFAULT_BASE_URL,
            language=smartess_cloud_probe.DEFAULT_LANGUAGE,
            app_id=smartess_cloud_probe.DEFAULT_APP_ID,
            app_version=smartess_cloud_probe.DEFAULT_APP_VERSION,
            company_key=smartess_cloud_probe.DEFAULT_COMPANY_KEY,
            timeout=smartess_cloud_probe.DEFAULT_TIMEOUT,
            device_type=smartess_cloud_probe.DEFAULT_DEVICE_TYPE,
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=0x0948,
            devaddr=0x01,
            search="",
            status="",
            brand="",
            order_by="",
            page=0,
            pagesize=50,
            collector_pn="",
            cloud_evidence_config_dir="",
            cloud_evidence_entry_id="",
        )
        envelopes = [
            smartess_cloud_probe.ApiEnvelope(
                err=0,
                desc="ok",
                dat={
                    "page": 0,
                    "pagesize": 50,
                    "total": 1,
                    "device": [
                        {
                            "pn": args.pn,
                            "sn": args.sn,
                            "devcode": args.devcode,
                            "devaddr": args.devaddr,
                            "devName": "PI30-like",
                        }
                    ],
                },
                raw={},
            ),
            smartess_cloud_probe.ApiEnvelope(
                err=0,
                desc="ok",
                dat={
                    "pars": {
                        "PV": [
                            {"id": 1, "par": "PV1 Voltage", "val": "230.0", "unit": "V"}
                        ]
                    }
                },
                raw={},
            ),
            smartess_cloud_probe.ApiEnvelope(
                err=0,
                desc="ok",
                dat={
                    "field": [
                        {
                            "id": "bse_eybond_ctrl_48",
                            "name": "Output Mode",
                            "item": [
                                {"key": "0", "val": "Single"},
                                {"key": "1", "val": "Parallel"},
                            ],
                        },
                        {
                            "id": "bse_eybond_ctrl_49",
                            "name": "Output priority",
                            "item": [
                                {"key": "0", "val": "UTI"},
                                {"key": "1", "val": "SOL"},
                                {"key": "2", "val": "SBU"},
                                {"key": "3", "val": "SUB"},
                                {"key": "4", "val": "SUF"},
                            ],
                        },
                        {
                            "id": "bat_eybond_ctrl_76",
                            "name": "Max.Charging Current",
                            "unit": "A",
                        },
                        {
                            "id": "sys_forced_eq_charging",
                            "name": "Forced EQ Charging",
                            "item": [{"key": "1", "val": "Forced EQ Charging Once"}],
                        },
                        {
                            "id": "sys_eybond_ctrl_98",
                            "name": "Boot method",
                            "item": [
                                {"key": "0", "val": "Can be powered on locally or remotely"},
                                {"key": "1", "val": "Only boot locally"},
                            ],
                        },
                    ],
                    "two_tier": {},
                },
                raw={},
            ),
            smartess_cloud_probe.ApiEnvelope(
                err=0,
                desc="ok",
                dat={"flow": {"pv": 1200, "load": 800}},
                raw={},
            ),
        ]

        with patch.object(smartess_cloud_probe, "fetch_signed_action", side_effect=envelopes) as fetch:
            payload = smartess_cloud_probe._run_device_bundle(args)

        self.assertEqual(fetch.call_count, 4)
        self.assertEqual(payload["request"]["command"], "device-bundle")
        self.assertEqual(payload["request"]["actions"]["device_list"], "webQueryDeviceEs")
        self.assertEqual(payload["request"]["params"]["pagesize"], 50)
        self.assertEqual(payload["normalized"]["device_list"]["device_count"], 1)
        self.assertEqual(payload["normalized"]["device_detail"]["section_counts"], {"PV": 1})
        normalized_settings = payload["normalized"]["device_settings"]
        self.assertEqual(payload["responses"]["device_settings"]["dat"]["two_tier"], {})
        self.assertEqual(normalized_settings["field_count"], 5)
        self.assertEqual(normalized_settings["mapped_field_count"], 3)
        self.assertEqual(normalized_settings["exact_0925_field_count"], 3)
        self.assertEqual(normalized_settings["probable_0925_field_count"], 1)
        self.assertEqual(normalized_settings["cloud_only_field_count"], 1)
        self.assertFalse(normalized_settings["current_values_included"])
        self.assertEqual(normalized_settings["write_action"], "ctrlDevice")
        self.assertEqual(normalized_settings["value_kind_counts"]["enum"], 1)
        self.assertEqual(normalized_settings["value_kind_counts"]["bool"], 2)
        self.assertEqual(normalized_settings["value_kind_counts"]["number"], 1)
        self.assertEqual(normalized_settings["value_kind_counts"]["action"], 1)
        self.assertEqual(normalized_settings["fields"][0]["bucket"], "probable_0925")
        self.assertEqual(normalized_settings["fields"][1]["binding"]["register"], 4537)
        self.assertEqual(normalized_settings["fields"][1]["bucket"], "exact_0925")
        self.assertEqual(normalized_settings["fields"][1]["choices"][3]["label"], "SUB")
        self.assertEqual(normalized_settings["fields"][2]["binding"]["register"], 4541)
        self.assertEqual(normalized_settings["fields"][2]["bucket"], "exact_0925")
        self.assertEqual(normalized_settings["fields"][3]["value_kind"], "action")
        self.assertEqual(normalized_settings["fields"][3]["binding"]["register"], 5012)
        self.assertEqual(normalized_settings["fields"][3]["asset_register"], 5012)
        self.assertEqual(normalized_settings["fields"][3]["bucket"], "exact_0925")
        self.assertEqual(normalized_settings["fields"][4]["bucket"], "cloud_only")
        self.assertEqual(payload["responses"]["energy_flow"]["dat"], {"flow": {"pv": 1200, "load": 800}})

        expected_actions = [
            smartess_cloud_probe.build_device_list_action(
                device_type=args.device_type,
                page=args.page,
                pagesize=args.pagesize,
                search=args.search,
                pn=args.pn,
                status=args.status,
                brand=args.brand,
                order_by=args.order_by,
            ),
            smartess_cloud_probe.build_device_detail_action(
                pn=args.pn,
                sn=args.sn,
                devcode=args.devcode,
                devaddr=args.devaddr,
            ),
            smartess_cloud_probe.build_device_settings_action(
                pn=args.pn,
                sn=args.sn,
                devcode=args.devcode,
                devaddr=args.devaddr,
            ),
            smartess_cloud_probe.build_device_energy_flow_action(
                pn=args.pn,
                sn=args.sn,
                devcode=args.devcode,
                devaddr=args.devaddr,
            ),
        ]
        self.assertEqual([call.kwargs["action"] for call in fetch.call_args_list], expected_actions)

    def test_normalize_device_settings_tracks_current_values_and_unknown_fields(self) -> None:
        normalized = smartess_cloud_probe.normalize_device_settings(
            {
                "field": [
                    {
                        "id": "sys_custom_toggle",
                        "name": "Custom Cloud Toggle",
                        "val": "1",
                        "item": [
                            {"key": "0", "val": "Off"},
                            {"key": "1", "val": "On"},
                        ],
                    },
                    {
                        "id": "bat_eybond_ctrl_76",
                        "name": "Max.Charging Current",
                        "unit": "A",
                        "currentValue": "80",
                    },
                ],
                "two_tier": {},
            }
        )

        assert normalized is not None
        self.assertTrue(normalized["current_values_included"])
        self.assertEqual(normalized["fields_with_current_value"], 2)
        self.assertEqual(normalized["fields_without_current_value"], 0)
        self.assertEqual(normalized["cloud_only_field_count"], 1)
        self.assertEqual(normalized["exact_0925_field_count"], 1)

        custom_toggle = normalized["fields"][0]
        max_charge_current = normalized["fields"][1]

        self.assertEqual(custom_toggle["bucket"], "cloud_only")
        self.assertEqual(custom_toggle["current_value"], 1)
        self.assertEqual(custom_toggle["value_kind"], "bool")
        self.assertEqual(max_charge_current["bucket"], "exact_0925")
        self.assertEqual(max_charge_current["binding"]["register"], 4541)
        self.assertEqual(max_charge_current["current_value"], 80)
        self.assertEqual(max_charge_current["value_kind"], "number")

    def test_normalize_device_settings_recognizes_generic_anenji_title_aliases(self) -> None:
        normalized = smartess_cloud_probe.normalize_device_settings(
            {
                "field": [
                    {
                        "id": "bse_eybond_ctrl_main_output_priority",
                        "name": "Main Output Priority",
                        "item": [
                            {"key": "1", "val": "PV-Utility-Battery"},
                            {"key": "2", "val": "PV-Battery-Utility"},
                        ],
                    },
                    {
                        "id": "bse_eybond_ctrl_output_voltage_setting",
                        "name": "Output Voltage Setting",
                        "unit": "V",
                    },
                    {
                        "id": "bse_eybond_ctrl_output_frequency_setting",
                        "name": "Output Frequency Setting",
                        "unit": "Hz",
                    },
                    {
                        "id": "bat_eybond_ctrl_maximum_charging_voltage",
                        "name": "Maximum charging voltage",
                        "unit": "V",
                    },
                    {
                        "id": "bat_eybond_ctrl_floating_charge_voltage",
                        "name": "Floating charge voltage",
                        "unit": "V",
                    },
                    {
                        "id": "bat_eybond_ctrl_maximum_charging_current",
                        "name": "Maximum charging current",
                        "unit": "A",
                    },
                    {
                        "id": "bat_eybond_ctrl_battery_eq_mode_enable",
                        "name": "Battery Eq mode enable",
                        "item": [
                            {"key": "0", "val": "Disable"},
                            {"key": "1", "val": "Enable"},
                        ],
                    },
                    {
                        "id": "bat_eybond_ctrl_battery_overvoltage_protection_point",
                        "name": "Battery overvoltage protection point",
                        "unit": "V",
                    },
                    {
                        "id": "bat_eybond_ctrl_bat_eq_time",
                        "name": "bat_eq_time",
                        "unit": "min",
                    },
                    {
                        "id": "sys_eybond_ctrl_clean_generation_power",
                        "name": "Clean Generation Power",
                        "item": [{"key": "170", "val": "Clean Generation Power"}],
                    },
                    {
                        "id": "sys_eybond_ctrl_equalization_activated_immediately",
                        "name": "Equalization activated immediately",
                        "item": [{"key": "1", "val": "Execute Once"}],
                    },
                ],
                "two_tier": {},
            }
        )

        assert normalized is not None
        self.assertEqual(normalized["mapped_field_count"], 11)
        self.assertEqual(normalized["exact_0925_field_count"], 10)
        self.assertEqual(normalized["probable_0925_field_count"], 1)
        self.assertEqual(normalized["cloud_only_field_count"], 0)

        fields = normalized["fields"]
        self.assertEqual(fields[0]["bucket"], "exact_0925")
        self.assertEqual(fields[0]["binding"]["register"], 4537)
        self.assertEqual(fields[1]["bucket"], "exact_0925")
        self.assertEqual(fields[1]["binding"]["register"], 4542)
        self.assertEqual(fields[2]["bucket"], "exact_0925")
        self.assertEqual(fields[2]["binding"]["register"], 4540)
        self.assertEqual(fields[3]["bucket"], "exact_0925")
        self.assertEqual(fields[3]["binding"]["register"], 4546)
        self.assertEqual(fields[4]["bucket"], "exact_0925")
        self.assertEqual(fields[4]["binding"]["register"], 4547)
        self.assertEqual(fields[5]["bucket"], "exact_0925")
        self.assertEqual(fields[5]["binding"]["register"], 4541)
        self.assertEqual(fields[6]["bucket"], "exact_0925")
        self.assertEqual(fields[6]["binding"]["register"], 5011)
        self.assertEqual(fields[7]["bucket"], "probable_0925")
        self.assertNotIn("asset_register", fields[7])
        self.assertEqual(fields[8]["bucket"], "exact_0925")
        self.assertEqual(fields[8]["binding"]["register"], 4550)
        self.assertEqual(fields[9]["bucket"], "exact_0925")
        self.assertEqual(fields[9]["binding"]["register"], 5001)
        self.assertEqual(fields[10]["bucket"], "exact_0925")
        self.assertEqual(fields[10]["binding"]["register"], 5012)

    def test_normalize_device_settings_keeps_bridge_only_aliases_cloud_only(self) -> None:
        normalized = smartess_cloud_probe.normalize_device_settings(
            {
                "field": [
                    {
                        "id": "bse_eybond_ctrl_main_output_priority",
                        "name": "Main Output Priority",
                    },
                    {
                        "id": "bat_eybond_ctrl_battery_type",
                        "name": "Battery Type",
                    },
                    {
                        "id": "bat_eybond_ctrl_soc_recovery",
                        "name": "SOC recovery value of battery discharge in mains mode",
                    },
                    {
                        "id": "sys_eybond_read_lcd_backlight",
                        "name": "LCD backlight",
                    },
                    {
                        "id": "sys_eybond_read_lcd_homepage",
                        "name": "LCD automatically returns to the homepage",
                    },
                    {
                        "id": "sys_eybond_read_overload_restart",
                        "name": "Overload automatic restart",
                    },
                    {
                        "id": "sys_eybond_read_overtemp_restart",
                        "name": "Automatic restart when over temperature",
                    },
                    {
                        "id": "sys_eybond_read_overload_bypass",
                        "name": "Overload transfer to bypass enable",
                    },
                ],
                "two_tier": {},
            }
        )

        assert normalized is not None
        self.assertEqual(normalized["mapped_field_count"], 3)
        self.assertEqual(normalized["exact_0925_field_count"], 3)
        self.assertEqual(normalized["cloud_only_field_count"], 5)

        fields_by_title = {
            str(field["title"]).lower(): field
            for field in normalized["fields"]
        }
        self.assertEqual(fields_by_title["main output priority"]["binding"]["register"], 4537)
        self.assertEqual(fields_by_title["battery type"]["binding"]["register"], 4539)
        self.assertEqual(
            fields_by_title["soc recovery value of battery discharge in mains mode"]["binding"]["register"],
            4545,
        )
        for title in (
            "lcd backlight",
            "lcd automatically returns to the homepage",
            "overload automatic restart",
            "automatic restart when over temperature",
            "overload transfer to bypass enable",
        ):
            self.assertEqual(fields_by_title[title]["bucket"], "cloud_only")
            self.assertNotIn("binding", fields_by_title[title])

    def test_normalize_device_settings_maps_0925_valuecloud_field_ids(self) -> None:
        field_ids_and_names = [
            ("grd_ac_input_range", "AC Input Range"),
            ("los_output_source_priority", "Output Source Priority"),
            ("bat_charger_source_priority", "Charger Source Priority"),
            ("los_output_voltage", "Output Voltage"),
            ("los_output_frequency", "Output Frequency"),
            ("bat_max_total_charge_current", "Max Total Charge Current"),
            ("bat_max_utility_charge_current", "Max Utility Charge Current"),
            ("bat_battery_type", "Battery Type"),
            ("bat_sp_bulk_charging_voltage", "Bulk Charging Voltage"),
            ("bat_sp_floting_charging_voltage", "Floating Charging Voltage"),
            ("bat_sp_low_battery_voltage", "Low Battery Cut-off Voltage"),
            (
                "bat_sp_utility_mode_voltage",
                "Comeback utility mode voltage point (SBU priority)",
            ),
            (
                "bat_sp_battery_mode_voltage",
                "Comeback battery mode voltage point (SBU priority)",
            ),
            ("bat_battery_equalization", "Battery Equalization"),
            (
                "bat_sp_battery_equalization_voltage",
                "Battery Equalization Voltage",
            ),
            ("bat_sp_battery_equalized_time", "Battery Equalized Time"),
            ("bat_sp_battery_equalized_timeout", "Battery Equalized Timeout"),
            (
                "bat_sp_battery_equalization_interval",
                "Battery Equalization Interval",
            ),
            (
                "bat_battery_equalization_activated_immediately",
                "Battery Equalization Activated Immediately",
            ),
            ("pvs_clear_all_generation", "Clear All Historical Power Generation"),
            ("cts_buzzer_alarm", "Buzzer Alarm"),
            (
                "cts_beeps_while_primary_source_interupt",
                "Beeps While Primary Source Interrupt",
            ),
            ("cts_lcd_backlight", "LCD Backlight"),
            ("cts_return_to_the_main_page", "Return To The Main LCD Page"),
            ("los_overload_auto_restart", "Overload Auto Restart"),
            ("cts_over_temperature_auto_restart", "Over Temperature Auto Restart"),
            ("los_transfer_to_bypass_overload", "Transfer To Bypass Overload"),
            ("sys_system_time", "system time"),
            ("cts_record_fault_code", "Record Fault Code"),
            ("cts_restore_defaults", "Restore Defaults"),
        ]

        normalized = smartess_cloud_probe.normalize_device_settings(
            {
                "field": [
                    {
                        "id": field_id,
                        "name": name,
                        "item": [{"key": "0", "val": "Off"}, {"key": "1", "val": "On"}],
                    }
                    for field_id, name in field_ids_and_names
                ],
                "two_tier": {},
            }
        )

        assert normalized is not None
        self.assertEqual(normalized["field_count"], 30)
        self.assertEqual(normalized["mapped_field_count"], 29)
        self.assertEqual(normalized["exact_0925_field_count"], 29)
        self.assertEqual(normalized["cloud_only_field_count"], 1)

        by_id = {field["cloud_id"]: field for field in normalized["fields"]}
        self.assertEqual(by_id["los_output_source_priority"]["binding"]["register"], 4537)
        self.assertEqual(by_id["grd_ac_input_range"]["binding"]["register"], 4538)
        self.assertEqual(by_id["cts_buzzer_alarm"]["binding"]["register"], 5002)
        self.assertEqual(by_id["cts_restore_defaults"]["binding"]["register"], 5016)
        self.assertEqual(by_id["sys_system_time"]["bucket"], "cloud_only")

    def test_device_bundle_exports_cloud_evidence_into_ha_config_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            args = argparse.Namespace(
                username="",
                password="",
                token="token123456789",
                secret="secret123456789",
                base_url=smartess_cloud_probe.DEFAULT_BASE_URL,
                language=smartess_cloud_probe.DEFAULT_LANGUAGE,
                app_id=smartess_cloud_probe.DEFAULT_APP_ID,
                app_version=smartess_cloud_probe.DEFAULT_APP_VERSION,
                company_key=smartess_cloud_probe.DEFAULT_COMPANY_KEY,
                timeout=smartess_cloud_probe.DEFAULT_TIMEOUT,
                device_type=smartess_cloud_probe.DEFAULT_DEVICE_TYPE,
                pn="E50000200000000001",
                sn="E50000200000000001000001",
                devcode=0x0948,
                devaddr=0x01,
                search="",
                status="",
                brand="",
                order_by="",
                page=0,
                pagesize=50,
                collector_pn="E5000020000000",
                cloud_evidence_config_dir=temp_dir,
                cloud_evidence_entry_id="entry123",
            )
            envelopes = [
                smartess_cloud_probe.ApiEnvelope(
                    err=0,
                    desc="ok",
                    dat={
                        "page": 0,
                        "pagesize": 50,
                        "total": 1,
                        "device": [{"pn": args.pn, "sn": args.sn}],
                    },
                    raw={},
                ),
                smartess_cloud_probe.ApiEnvelope(
                    err=0,
                    desc="ok",
                    dat={"pars": {"PV": [{"id": 1, "par": "PV1 Voltage", "val": "230.0"}]}},
                    raw={},
                ),
                smartess_cloud_probe.ApiEnvelope(err=0, desc="ok", dat={"field": [], "two_tier": {}}, raw={}),
                smartess_cloud_probe.ApiEnvelope(err=0, desc="ok", dat={"flow": {}}, raw={}),
            ]

            with patch.object(
                smartess_cloud_probe,
                "fetch_signed_action",
                side_effect=envelopes,
            ):
                payload = smartess_cloud_probe._run_device_bundle(args)

            self.assertIn("cloud_evidence_path", payload)
            evidence_path = Path(payload["cloud_evidence_path"])
            self.assertTrue(evidence_path.exists())

            raw = json.loads(evidence_path.read_text(encoding="utf-8"))
            self.assertEqual(raw["match"]["entry_id"], "entry123")
            self.assertEqual(raw["match"]["collector_pn"], "E5000020000000")
            self.assertEqual(raw["device_identity"]["devcode"], 2376)
            self.assertEqual(raw["summary"]["actions"], ["device_list", "device_detail", "device_settings", "energy_flow"])
            self.assertEqual(raw["summary"]["settings_field_count"], 0)
            self.assertEqual(raw["summary"]["settings_exact_0925_field_count"], 0)
            self.assertEqual(raw["summary"]["settings_probable_0925_field_count"], 0)
            self.assertEqual(raw["summary"]["settings_cloud_only_field_count"], 0)
            self.assertEqual(raw["summary"]["settings_write_action"], "ctrlDevice")


if __name__ == "__main__":
    unittest.main()
