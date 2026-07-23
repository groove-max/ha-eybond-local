from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.transport_profile import (
    apply_observed_collector_session_protocol,
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
from custom_components.eybond_local.metadata.collector_cloud_profile_catalog_loader import (
    load_collector_cloud_profile_catalog,
)


class CollectorTransportProfileTests(unittest.TestCase):
    def test_no_cloud_family_in_catalog_selects_a_wire(self) -> None:
        for family in load_collector_cloud_profile_catalog().profiles:
            profile = resolve_collector_transport_profile(cloud_family=family)
            self.assertEqual(profile.session_protocol, "", family)
            self.assertEqual(profile.identity_strategy, "", family)

    def test_smartess_at_without_wire_evidence_is_neutral(self) -> None:
        profile = resolve_collector_transport_profile(
            cloud_family="smartess_at",
            runtime_owner_key="",
        )

        self.assertEqual(profile.cloud_family, "smartess_at")
        self.assertEqual(profile.session_protocol, "")
        self.assertEqual(profile.identity_strategy, "")

    def test_metadata_never_chooses_bootstrap_transport(self) -> None:
        # Neither the inverter driver nor the collector's cloud family is wire
        # evidence. Until a session proves its protocol, the profile is neutral.
        for owner in ("", "modbus_smg", "must_pv_ph18", "pi30", "srne_modbus"):
            profile = resolve_collector_transport_profile(
                cloud_family="smartess_at",
                runtime_owner_key=owner,
            )
            self.assertEqual(profile.runtime_owner_key, owner)
            self.assertEqual(profile.session_protocol, "")
            self.assertEqual(profile.identity_strategy, "")

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

    def test_virtual_bridge_metadata_does_not_choose_wire(self) -> None:
        profile = resolve_collector_transport_profile(
            cloud_family="smartess_at",
            runtime_owner_key="pi30",
            virtual_bridge=True,
        )

        self.assertEqual(profile.cloud_family, "smartess_at")
        self.assertEqual(profile.runtime_owner_key, "pi30")
        self.assertEqual(profile.session_protocol, "")
        self.assertEqual(profile.identity_strategy, "")
        self.assertEqual(profile.raw_passthrough_frame_format, "")

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
        self.assertEqual(profile.session_protocol, "")

    def test_entry_context_reads_family_and_owner_but_owner_does_not_pick_transport(self) -> None:
        data = {
            CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
            CONF_DRIVER_HINT: "modbus_smg",
        }

        self.assertEqual(collector_cloud_family_from_entry_context(data, {}), "smartess_at")
        # The owner key is still read (diagnostics) ...
        self.assertEqual(runtime_owner_key_from_entry_context(data, {}), "modbus_smg")
        # ... but neither field selects a protocol.
        self.assertEqual(
            resolve_collector_transport_profile_from_entry_context(
                data,
                {},
            ).session_protocol,
            "",
        )


class ObservedSessionProtocolOverrideTests(unittest.TestCase):
    """The protocol -> transport-profile map lives in the transport-profile
    authority (moved out of the runtime coordinator). Characterizes the exact
    behavior that was previously hand-coded in the coordinator."""

    def _base(self, family: str):
        return resolve_collector_transport_profile(cloud_family=family, runtime_owner_key="")

    def test_no_observation_returns_base(self) -> None:
        base = self._base("smartess_at")
        self.assertIs(apply_observed_collector_session_protocol(base, ""), base)

    def test_confirmed_at_observation_selects_at_profile(self) -> None:
        base = self._base("smartess_at")
        profile = apply_observed_collector_session_protocol(base, "at_text")
        self.assertEqual(profile.session_protocol, "at_text")
        self.assertEqual(profile.identity_strategy, "at_dtupn")
        self.assertEqual(profile.raw_passthrough_frame_format, "transparent")

    def test_unknown_observation_returns_base(self) -> None:
        base = self._base("smartess_at")
        self.assertIs(apply_observed_collector_session_protocol(base, "mystery"), base)

    def test_observed_framed_selects_framed_profile(self) -> None:
        base = self._base("smartess_at")
        profile = apply_observed_collector_session_protocol(base, "eybond_framed")
        self.assertEqual(profile.session_protocol, "eybond_framed")
        self.assertEqual(profile.identity_strategy, "framed_heartbeat_then_fc2_pn")
        self.assertEqual(profile.raw_passthrough_bootstrap, "")
        self.assertEqual(profile.raw_passthrough_frame_format, "")
        self.assertEqual(profile.raw_passthrough_min_interval_ms, 0)
        # Diagnostic provenance fields are preserved.
        self.assertEqual(profile.cloud_family, base.cloud_family)
        self.assertEqual(profile.runtime_owner_key, base.runtime_owner_key)

    def test_observed_at_text_does_not_inherit_unrelated_cloud_dialect(self) -> None:
        base = self._base("legacy_binary")
        self.assertEqual(base.session_protocol, "")
        profile = apply_observed_collector_session_protocol(base, "at_text")
        self.assertEqual(profile.session_protocol, "at_text")
        self.assertEqual(profile.identity_strategy, "at_dtupn")
        self.assertEqual(profile.raw_passthrough_bootstrap, "")
        self.assertEqual(profile.raw_passthrough_frame_format, "")
        self.assertEqual(profile.raw_passthrough_min_interval_ms, 0)


if __name__ == "__main__":
    unittest.main()
