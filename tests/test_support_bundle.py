from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.bundle import (
    build_support_bundle_payload,
    export_support_bundle,
)


def _sample_support_bundle_payload() -> dict[str, object]:
    return build_support_bundle_payload(
        entry_id="entry123",
        entry_title="SMG 6200",
        connected=True,
        collector={"collector_pn": "E5000025388419"},
        inverter={
            "driver_key": "modbus_smg",
            "model_name": "SMG 6200",
            "variant_key": "default",
            "serial_number": "92632511100118",
            "profile_name": "smg_modbus.json",
            "register_schema_name": "modbus_smg/models/smg_6200.json",
        },
        values={"operating_mode": "Off-Grid"},
        data={"server_ip": "192.168.1.50"},
        options={"poll_interval": 10},
        profile_name="smg_modbus.json",
        register_schema_name="modbus_smg/models/smg_6200.json",
        variant_key="default",
    )


class SupportBundleTests(unittest.TestCase):
    def test_builds_support_bundle_payload(self) -> None:
        raw = _sample_support_bundle_payload()

        self.assertEqual(raw["entry"]["entry_id"], "entry123")
        self.assertEqual(raw["source_metadata"]["profile_name"], "smg_modbus.json")
        self.assertEqual(raw["source_metadata"]["variant_key"], "default")
        self.assertEqual(raw["runtime"]["values"]["operating_mode"], "Off-Grid")

    def test_exports_support_bundle_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            payload = _sample_support_bundle_payload()

            path = export_support_bundle(
                config_dir=config_dir,
                entry_id="entry123",
                entry_title="SMG 6200",
                connected=bool(payload["runtime"]["connected"]),
                collector=payload["runtime"]["collector"],
                inverter=payload["runtime"]["inverter"],
                values=payload["runtime"]["values"],
                data=payload["entry"]["data"],
                options=payload["entry"]["options"],
                profile_name=payload["source_metadata"]["profile_name"],
                register_schema_name=payload["source_metadata"]["register_schema_name"],
                variant_key=payload["source_metadata"]["variant_key"],
            )

            self.assertEqual(path.suffix, ".json")
            self.assertTrue(path.exists())
            raw = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(raw["entry"]["entry_id"], "entry123")
            self.assertEqual(raw["source_metadata"]["profile_name"], "smg_modbus.json")
            self.assertEqual(raw["source_metadata"]["variant_key"], "default")
            self.assertEqual(
                raw["source_metadata"]["register_schema_name"],
                "modbus_smg/models/smg_6200.json",
            )
            self.assertEqual(raw["runtime"]["values"]["operating_mode"], "Off-Grid")


if __name__ == "__main__":
    unittest.main()
