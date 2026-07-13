from __future__ import annotations

import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components" / "eybond_local"
TRANSLATION_FILES = (
    COMPONENT / "strings.json",
    *sorted((COMPONENT / "translations").glob("*.json")),
)
FLOW_TRANSLATION_FILES = tuple(
    sorted((COMPONENT / "flow_translations").glob("*.json"))
)


class TranslationShapeTests(unittest.TestCase):
    def test_option_flow_errors_are_declared_at_options_error_level(self) -> None:
        for path in TRANSLATION_FILES:
            with self.subTest(path=path.name):
                payload = json.loads(path.read_text(encoding="utf-8"))
                self.assertIn("proxy_capture_action_failed", payload["options"]["error"])
                invalid_steps = [
                    step_id
                    for step_id, step_payload in payload["options"]["step"].items()
                    if isinstance(step_payload, dict) and "errors" in step_payload
                ]
                self.assertEqual(invalid_steps, [])

    def test_phase4_connection_strategy_strings_exist(self) -> None:
        # The new connection-strategy runtime field labels exist in every
        # HA-native bundle (strings.json + translations/*.json).
        for path in TRANSLATION_FILES:
            with self.subTest(path=path.name):
                payload = json.loads(path.read_text(encoding="utf-8"))
                runtime = payload["options"]["step"]["runtime"]
                self.assertIn("connection_strategy", runtime["data"])
                self.assertIn("proxy_enabled", runtime["data"])
                self.assertIn("connection_strategy", runtime["data_description"])

    def test_phase4_connection_strategy_flow_labels_exist(self) -> None:
        # The dynamic select-option labels exist in every flow_translations bundle.
        for path in FLOW_TRANSLATION_FILES:
            with self.subTest(path=path.name):
                dynamic = json.loads(path.read_text(encoding="utf-8"))["common"]["dynamic"]
                self.assertIn("connection_strategy_inbound", dynamic)
                self.assertIn("connection_strategy_callback_on_demand", dynamic)
                self.assertIn("connection_strategy_bridge_note", dynamic)

    def test_listener_rediscovery_action_is_translated(self) -> None:
        for path in TRANSLATION_FILES:
            with self.subTest(path=path.name):
                payload = json.loads(path.read_text(encoding="utf-8"))
                steps = payload["options"]["step"]
                self.assertIn("rediscover_devices", steps["listener"]["menu_options"])
                self.assertIn("rediscover_devices", steps)
                self.assertIn("rediscover_devices_done", steps)
                self.assertIn(
                    "confirm_rediscover_devices",
                    steps["rediscover_devices"]["data"],
                )


if __name__ == "__main__":
    unittest.main()
