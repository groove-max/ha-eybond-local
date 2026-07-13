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

    def test_driver_owner_key_does_not_change_bootstrap_transport(self) -> None:
        # Phase-2 invariant: the driver (runtime owner) key must NOT influence the
        # bootstrap transport. modbus_smg / must_pv_ph18 used to force framed; now
        # the profile derives only from the cloud-family legacy hint, and the
        # owner key is a diagnostic field only.
        for owner in ("", "modbus_smg", "must_pv_ph18", "pi30", "srne_modbus"):
            profile = resolve_collector_transport_profile(
                cloud_family="smartess_at",
                runtime_owner_key=owner,
            )
            self.assertEqual(profile.runtime_owner_key, owner)
            # Same cloud family => same transport regardless of the driver.
            self.assertEqual(profile.session_protocol, "at_text")
            self.assertEqual(profile.identity_strategy, "at_dtupn")

    def test_owner_key_without_cloud_family_is_fail_closed(self) -> None:
        # No cloud family and no live observation: the bootstrap is fail-closed
        # (no assumed transport), never framed-because-of-the-driver.
        for owner in ("must_pv_ph18", "modbus_smg", "pi30", ""):
            profile = resolve_collector_transport_profile(
                cloud_family="",
                runtime_owner_key=owner,
            )
            self.assertEqual(profile.runtime_owner_key, owner)
            self.assertEqual(profile.session_protocol, "")
            self.assertEqual(profile.identity_strategy, "")

    def test_virtual_bridge_pi30_smartess_at_keeps_at_text_legacy_hint(self) -> None:
        profile = resolve_collector_transport_profile(
            cloud_family="smartess_at",
            runtime_owner_key="pi30",
            virtual_bridge=True,
        )

        self.assertEqual(profile.cloud_family, "smartess_at")
        self.assertEqual(profile.runtime_owner_key, "pi30")
        self.assertEqual(profile.session_protocol, "at_text")
        self.assertEqual(profile.identity_strategy, "at_dtupn")
        self.assertEqual(profile.raw_passthrough_frame_format, "transparent")

    def test_virtual_bridge_without_family_does_not_choose_payload_transport(self) -> None:
        profile = resolve_collector_transport_profile(
            cloud_family="",
            runtime_owner_key="",
            virtual_bridge=True,
        )

        self.assertEqual(profile.cloud_family, "")
        self.assertEqual(profile.runtime_owner_key, "")
        self.assertEqual(profile.session_protocol, "")
        self.assertEqual(profile.identity_strategy, "")

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

    def test_entry_context_reads_family_and_owner_but_owner_does_not_pick_transport(self) -> None:
        data = {
            CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
            CONF_DRIVER_HINT: "modbus_smg",
        }

        self.assertEqual(collector_cloud_family_from_entry_context(data, {}), "smartess_at")
        # The owner key is still read (diagnostics) ...
        self.assertEqual(runtime_owner_key_from_entry_context(data, {}), "modbus_smg")
        # ... but it no longer forces framed: the bootstrap follows the cloud
        # family only, and a live SessionHandle overrides even that.
        self.assertEqual(
            resolve_collector_transport_profile_from_entry_context(
                data,
                {},
            ).session_protocol,
            "at_text",
        )


if __name__ == "__main__":
    unittest.main()
