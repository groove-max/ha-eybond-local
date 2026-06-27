from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.transport_profile import (
    collector_cloud_family_from_entry_context,
    resolve_collector_transport_profile,
    resolve_collector_transport_profile_from_entry_context,
    runtime_owner_key_from_entry_context,
)
from custom_components.eybond_local.const import (
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_DRIVER_HINT,
)


class CollectorTransportProfileTests(unittest.TestCase):
    def test_smartess_at_unknown_runtime_uses_at_text(self) -> None:
        profile = resolve_collector_transport_profile(
            cloud_family="smartess_at",
            runtime_owner_key="",
        )

        self.assertEqual(profile.cloud_family, "smartess_at")
        self.assertEqual(profile.session_protocol, "at_text")
        self.assertEqual(profile.identity_strategy, "at_dtupn")

    def test_smartess_at_smg_runtime_keeps_framed_payload(self) -> None:
        profile = resolve_collector_transport_profile(
            cloud_family="smartess_at",
            runtime_owner_key="modbus_smg",
        )

        self.assertEqual(profile.cloud_family, "smartess_at")
        self.assertEqual(profile.runtime_owner_key, "modbus_smg")
        self.assertEqual(profile.session_protocol, "eybond_framed")
        self.assertEqual(profile.identity_strategy, "framed_heartbeat_then_fc2_pn")

    def test_must_runtime_uses_framed_payload_even_without_cloud_family(self) -> None:
        profile = resolve_collector_transport_profile(
            cloud_family="",
            runtime_owner_key="must_pv_ph18",
        )

        self.assertEqual(profile.cloud_family, "")
        self.assertEqual(profile.runtime_owner_key, "must_pv_ph18")
        self.assertEqual(profile.session_protocol, "eybond_framed")
        self.assertEqual(profile.identity_strategy, "framed_heartbeat_then_fc2_pn")

    def test_entry_context_recovers_family_from_original_endpoint_profile(self) -> None:
        profile = resolve_collector_transport_profile_from_entry_context(
            {},
            {
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY: "smartess_at",
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: "dtu_ess.eybond.com,18899,TCP",
            },
        )

        self.assertEqual(profile.cloud_family, "smartess_at")
        self.assertEqual(profile.session_protocol, "at_text")

    def test_entry_context_prefers_explicit_family_and_driver_hint(self) -> None:
        data = {
            CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
            CONF_DRIVER_HINT: "modbus_smg",
        }

        self.assertEqual(collector_cloud_family_from_entry_context(data, {}), "smartess_at")
        self.assertEqual(runtime_owner_key_from_entry_context(data, {}), "modbus_smg")
        self.assertEqual(
            resolve_collector_transport_profile_from_entry_context(
                data,
                {},
            ).session_protocol,
            "eybond_framed",
        )


if __name__ == "__main__":
    unittest.main()
