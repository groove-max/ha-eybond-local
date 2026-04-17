from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from tools import smartess_cloud_probe


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
                "E50000253884199645",
                "--sn",
                "E50000253884199645094801",
                "--devcode",
                "0x0948",
                "--devaddr",
                "0x01",
            ]
        )

        self.assertEqual(args.command, "device-bundle")
        self.assertEqual(args.pagesize, 50)
        self.assertIs(args.func, smartess_cloud_probe._run_device_bundle)

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
            pn="E50000253884199645",
            sn="E50000253884199645094801",
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
                pn="E50000253884199645",
                sn="E50000253884199645094801",
                devcode=0x0948,
                devaddr=0x01,
                search="",
                status="",
                brand="",
                order_by="",
                page=0,
                pagesize=50,
                collector_pn="E5000025388419",
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
            self.assertEqual(raw["match"]["collector_pn"], "E5000025388419")
            self.assertEqual(raw["device_identity"]["devcode"], 2376)
            self.assertEqual(raw["summary"]["actions"], ["device_list", "device_detail", "device_settings", "energy_flow"])
            self.assertEqual(raw["summary"]["settings_field_count"], 0)
            self.assertEqual(raw["summary"]["settings_exact_0925_field_count"], 0)
            self.assertEqual(raw["summary"]["settings_probable_0925_field_count"], 0)
            self.assertEqual(raw["summary"]["settings_cloud_only_field_count"], 0)
            self.assertEqual(raw["summary"]["settings_write_action"], "ctrlDevice")


if __name__ == "__main__":
    unittest.main()