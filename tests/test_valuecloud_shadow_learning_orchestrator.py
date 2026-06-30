from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.shadow_learning import (  # noqa: E402
    ShadowWriteObservation,
)
from custom_components.eybond_local.support.valuecloud_shadow_learning_orchestrator import (  # noqa: E402
    async_orchestrate_valuecloud_shadow_learning,
    build_valuecloud_learning_plan,
)
from custom_components.eybond_local.valuecloud_cloud import (  # noqa: E402
    ValueCloudEnvelope,
    ValueCloudSession,
)


def _batch_control() -> dict[str, object]:
    return {
        "groups": [
            {
                "controlItemId": 10,
                "parameters": [
                    {
                        "id": "cltd_lcd_backlight",
                        "detailsId": 20,
                        "order": 3,
                        "name": "LCD Backlight",
                        "readwrite": "RW",
                        "item": {"0": "Off", "1": "On"},
                    },
                    {
                        "id": "cltd_restore_default",
                        "detailsId": 21,
                        "order": 4,
                        "name": "Restore Default",
                        "readwrite": "RW",
                        "item": {"1": "Restore"},
                    },
                    {
                        "id": "cltd_max_charging_current",
                        "detailsId": 22,
                        "order": 5,
                        "name": "Maximum Charging Current",
                        "readwrite": "RW",
                        "val": "15.0",
                    },
                ],
            }
        ]
    }


class ValueCloudShadowLearningOrchestratorTests(unittest.TestCase):
    def test_plan_uses_batch_control_metadata_and_skips_destructive_actions(self) -> None:
        plan = build_valuecloud_learning_plan(_batch_control())

        self.assertEqual(
            [(item["field_id"], item["value"]) for item in plan],
            [
                ("cltd_lcd_backlight", "0"),
                ("cltd_lcd_backlight", "1"),
                ("cltd_max_charging_current", "15.0"),
            ],
        )
        self.assertTrue(all(item["field_id"] != "cltd_restore_default" for item in plan))
        self.assertEqual(plan[0]["controlItemId"], 10)
        self.assertEqual(plan[0]["detailsId"], 20)
        self.assertEqual(plan[0]["order"], 3)

    def test_plan_accepts_valuecloud_list_choice_metadata(self) -> None:
        plan = build_valuecloud_learning_plan(
            {
                "groups": [
                    {
                        "controlItemId": 10,
                        "parameters": [
                            {
                                "id": "cltd_inverter_remote_switch",
                                "detailsId": 20,
                                "order": 3,
                                "name": "Controls the inverter switch",
                                "readwrite": "RW",
                                "item": [
                                    {"key": "0", "val": "Open"},
                                    {"key": "1", "val": "Close"},
                                ],
                            }
                        ],
                    }
                ]
            }
        )

        self.assertEqual(
            [(item["field_id"], item["value"], item["value_label"]) for item in plan],
            [
                ("cltd_inverter_remote_switch", "0", "Turn on / cancel remote shutdown"),
                ("cltd_inverter_remote_switch", "1", "Remote shutdown"),
            ],
        )

    def test_plan_uses_protocol_labels_for_known_g_ascii_valuecloud_controls(self) -> None:
        plan = build_valuecloud_learning_plan(
            {
                "groups": [
                    {
                        "controlItemId": 10,
                        "parameters": [
                            {
                                "id": "cltd_inverter_output_voltage",
                                "detailsId": 20,
                                "order": 3,
                                "name": "Inverter output voltage",
                                "readwrite": "RW",
                                "item": [
                                    {"key": "208", "val": "208"},
                                    {"key": "220", "val": "208"},
                                    {"key": "230", "val": "230"},
                                ],
                            }
                        ],
                    }
                ]
            }
        )

        self.assertEqual(
            [
                (item["title"], item["value"], item["value_label"], item["read_key"])
                for item in plan
            ],
            [
                ("Output voltage setting", "208", "208 V", "g_ascii_setting_output_voltage"),
                ("Output voltage setting", "220", "220 V", "g_ascii_setting_output_voltage"),
                ("Output voltage setting", "230", "230 V", "g_ascii_setting_output_voltage"),
            ],
        )

    def test_plan_merges_legacy_valuecloud_controls_after_batch_controls(self) -> None:
        plan = build_valuecloud_learning_plan(
            {
                "groups": [
                    {
                        "controlItemId": 10,
                        "parameters": [
                            {
                                "id": "cltd_inverter_remote_switch",
                                "detailsId": 20,
                                "order": 3,
                                "name": "Remote switch",
                                "readwrite": "RW",
                                "item": {"1": "Close"},
                            }
                        ],
                    }
                ]
            },
            control_strategy={
                "fields": [
                    {
                        "id": "cltd_inverter_remote_switch",
                        "name": "Remote switch legacy duplicate",
                        "readwrite": "RW",
                        "datatype": 3,
                        "item": [{"key": "0", "val": "Open"}],
                    },
                    {
                        "id": "cltd_set_output_mode",
                        "name": "Output mode",
                        "readwrite": "RW",
                        "datatype": 3,
                        "item": [{"key": "12337", "val": "UPS mode"}],
                    },
                ]
            },
        )

        self.assertEqual(
            [(item["field_id"], item["value"], item["transport"]) for item in plan],
            [
                ("cltd_inverter_remote_switch", "1", "batch_setUp"),
                ("cltd_set_output_mode", "12337", "legacy_ctrlDevice"),
            ],
        )

    def test_orchestrator_posts_valuecloud_setup_and_correlates_ascii_write(self) -> None:
        calls: list[dict[str, object]] = []
        observations = [
            ShadowWriteObservation(
                timestamp="2026-06-29T12:00:00+00:00",
                source="shadow_learning",
                unit=0,
                function_code=0,
                register=-1,
                values=(),
                devcode=None,
                devaddr=None,
                raw_payload_hex="50424c31",
                protocol="eybond_g_ascii",
                command="PBL",
                value="1",
            )
        ]

        def setup_action(**kwargs):
            calls.append(kwargs)
            return ValueCloudEnvelope(
                code=200,
                message="ok",
                error_message="",
                success=True,
                data={},
                raw={"code": 200, "success": True},
                headers={},
            )

        def current_observations_since(cursor: int):
            return tuple(observations[cursor:])

        result = asyncio.run(
            async_orchestrate_valuecloud_shadow_learning(
                batch_control={
                    "groups": [
                        {
                            "controlItemId": 10,
                            "parameters": [
                                {
                                    "id": "cltd_lcd_backlight",
                                    "detailsId": 20,
                                    "order": 3,
                                    "name": "LCD Backlight",
                                    "readwrite": "RW",
                                    "item": {"1": "On"},
                                }
                            ],
                        }
                    ]
                },
                session=ValueCloudSession(token="token", secret="secret"),
                pn="I200",
                sn="DEV1",
                devcode=2506,
                devaddr=1,
                dry_run=False,
                confirm_cloud_write=True,
                shadow_session_state="ready",
                field_ids=[],
                observation_cursor=lambda: 0,
                current_observations_since=current_observations_since,
                setup_action=setup_action,
            )
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["control_item_id"], 10)
        self.assertEqual(calls[0]["control_id"], "cltd_lcd_backlight")
        self.assertEqual(calls[0]["details_id"], 20)
        self.assertEqual(calls[0]["value"], "1")
        self.assertEqual(result["correlation"]["matched_count"], 1)
        self.assertEqual(result["captured_not_applied_count"], 1)
        self.assertEqual(result["planned_write_count"], 1)
        self.assertEqual(result["plan"][0]["field_id"], "cltd_lcd_backlight")
        observation = result["correlation"]["matched"][0]["observation"]
        self.assertEqual(observation["protocol"], "eybond_g_ascii")
        self.assertEqual(observation["command"], "PBL")

    def test_orchestrator_uses_legacy_ctrl_device_when_batch_metadata_is_empty(self) -> None:
        calls: list[dict[str, object]] = []
        observations = [
            ShadowWriteObservation(
                timestamp="2026-06-29T12:00:00+00:00",
                source="shadow_learning",
                unit=0,
                function_code=0,
                register=-1,
                values=(),
                devcode=None,
                devaddr=None,
                raw_payload_hex="50524f3132333337",
                protocol="eybond_g_ascii",
                command="PRO",
                value="12337",
            )
        ]

        def legacy_setup_action(**kwargs):
            calls.append(kwargs)
            return ValueCloudEnvelope(
                code=200,
                message="ok",
                error_message="",
                success=True,
                data={},
                raw={"code": 200, "success": True},
                headers={},
            )

        result = asyncio.run(
            async_orchestrate_valuecloud_shadow_learning(
                batch_control={"groups": []},
                control_strategy={
                    "fields": [
                        {
                            "id": "cltd_set_output_priority",
                            "name": "Output priority",
                            "readwrite": "RW",
                            "datatype": 3,
                            "item": [{"key": "12337", "val": "PV first"}],
                        }
                    ]
                },
                session=ValueCloudSession(token="token", secret="secret"),
                pn="I200",
                sn="DEV1",
                devcode=2506,
                devaddr=1,
                dry_run=False,
                confirm_cloud_write=True,
                shadow_session_state="ready",
                field_ids=[],
                observation_cursor=lambda: 0,
                current_observations_since=lambda cursor: tuple(observations[cursor:]),
                legacy_setup_action=legacy_setup_action,
            )
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["control_id"], "cltd_set_output_priority")
        self.assertEqual(calls[0]["value"], "12337")
        self.assertEqual(calls[0]["datatype"], 3)
        self.assertEqual(result["plan"][0]["transport"], "legacy_ctrlDevice")
        self.assertEqual(result["captured_not_applied_count"], 1)


if __name__ == "__main__":
    unittest.main()
