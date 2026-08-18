from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.registry import (
    support_marker as driver_support_marker,
)
from custom_components.eybond_local.support.bundle import (
    build_support_bundle_payload,
    export_support_bundle,
)
from custom_components.eybond_local.telemetry import (
    TypedTelemetryFrame,
    fold_driver_telemetry,
)


def _smg_marker_payload(*, variant_key: str = "", profile_name: str = ""):
    """Return the authoritative SMG driver marker payload (or None)."""

    marker = driver_support_marker(
        "modbus_smg", variant_key=variant_key, profile_name=profile_name
    )
    return marker.as_payload() if marker is not None else None


def _sample_cloud_evidence() -> dict[str, object]:
    return {
        "evidence_version": 1,
        "source": "smartess_cloud_probe",
        "match": {"entry_id": "entry123", "collector_pn": "E5000020000000"},
        "device_identity": {
            "pn": "E50000200000000001",
            "sn": "E50000200000000001000001",
            "devcode": 2376,
            "devaddr": 1,
        },
        "summary": {"actions": ["device_list", "device_detail"]},
        "payload": {"request": {"command": "device-bundle"}},
    }


def _sample_support_bundle_payload() -> dict[str, object]:
    telemetry = fold_driver_telemetry(
        TypedTelemetryFrame.empty(),
        driver_key="modbus_smg",
        values={"operating_mode": "Off-Grid"},
        replace=True,
    )
    return build_support_bundle_payload(
        entry_id="entry123",
        entry_title="SMG 6200",
        connected=True,
        collector={"collector_pn": "E5000020000000"},
        inverter={
            "driver_key": "modbus_smg",
            "model_name": "SMG 6200",
            "variant_key": "default",
            "serial_number": "92632500000001",
            "profile_name": "smg_modbus.json",
            "register_schema_name": "modbus_smg/models/smg_6200.json",
        },
        values={"operating_mode": "Off-Grid"},
        telemetry=telemetry,
        data={"server_ip": "192.168.1.50"},
        options={"poll_interval": 10},
        profile_name="smg_modbus.json",
        register_schema_name="modbus_smg/models/smg_6200.json",
        variant_key="default",
        cloud_evidence=_sample_cloud_evidence(),
    )


def _recovery_contract_data() -> dict[str, object]:
    """Entry data carrying a FULL recovery contract with network-looking values.

    The snapshots are deliberately address-shaped so the non-disclosure test
    below can prove the bundle never leaks them.
    """

    from custom_components.eybond_local.connection.recovery_contract import (
        CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
        CallbackRecoveryProof,
        INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
        InboundRecoveryProof,
        RECOVERY_CONTRACT_KEY,
        RecoveryContract,
    )

    pn = "V001020SYN62344022"
    ts = "2026-07-16T10:00:00+00:00"
    contract = (
        RecoveryContract.empty_for_pn(pn, identity_source="fc2_parameter_2")
        .with_inbound_proof(
            InboundRecoveryProof(
                method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
                collector_pn=pn,
                identity_source="fc2_parameter_2",
                verified_at=ts,
            ),
            updated_at=ts,
        )
        .with_callback_proof(
            CallbackRecoveryProof(
                method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
                collector_pn=pn,
                identity_source="at_dtupn",
                verified_at=ts,
                trigger_target="203.0.113.55:58899",
                advertised_ha_endpoint="192.0.2.10:18899",
                listener_port=18899,
            ),
            updated_at=ts,
        )
    )
    return {
        "server_ip": "192.168.1.50",
        "collector_pn": pn,
        RECOVERY_CONTRACT_KEY: contract.to_record(),
    }


class SupportBundleTests(unittest.TestCase):
    def test_builds_support_bundle_payload(self) -> None:
        raw = _sample_support_bundle_payload()

        self.assertEqual(raw["entry"]["entry_id"], "entry123")
        self.assertEqual(raw["source_metadata"]["profile_name"], "smg_modbus.json")
        self.assertEqual(raw["source_metadata"]["variant_key"], "default")
        self.assertEqual(raw["runtime"]["values"]["operating_mode"], "Off-Grid")
        self.assertNotIn("operating_mode", raw["runtime"]["metadata"])
        self.assertEqual(
            raw["runtime"]["telemetry"]["values"]["operating_mode"],
            "Off-Grid",
        )
        self.assertEqual(raw["runtime"]["telemetry"]["driver_key"], "modbus_smg")
        self.assertEqual(raw["runtime"]["telemetry"]["fresh_count"], 1)
        self.assertEqual(raw["roles"]["collector"]["identity"]["collector_pn"], "E5000020000000")
        self.assertEqual(raw["roles"]["inverter"]["identity"]["model_name"], "SMG 6200")
        self.assertEqual(raw["roles"]["inverter"]["values"]["operating_mode"], "Off-Grid")
        self.assertEqual(
            raw["roles"]["inverter"]["measurements"]["operating_mode"],
            "Off-Grid",
        )
        self.assertEqual(raw["evidence"]["cloud"]["source"], "smartess_cloud_probe")

    def test_typed_telemetry_is_split_from_broad_support_metadata(self) -> None:
        telemetry = fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key="pi30",
            values={"output_power": 420},
            replace=True,
        ).as_carried()
        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="PI30",
            connected=False,
            collector=None,
            inverter={"driver_key": "pi30"},
            values={
                "output_power": 420,
                "runtime_driver_state": "offline",
                "runtime_probe_log": [{"driver": "pi30"}],
            },
            telemetry=telemetry,
            data={},
            options={},
            profile_name="",
            register_schema_name="",
        )

        self.assertEqual(raw["runtime"]["telemetry"]["carried_count"], 1)
        self.assertEqual(
            raw["runtime"]["telemetry"]["points"][0]["freshness"],
            "carried",
        )
        self.assertNotIn("output_power", raw["runtime"]["metadata"])
        self.assertEqual(
            raw["runtime"]["metadata"]["runtime_driver_state"], "offline"
        )
        self.assertIn("runtime_probe_log", raw["runtime"]["metadata"])

    def test_support_telemetry_rejects_duck_frames(self) -> None:
        common = dict(
            entry_id="entry123",
            entry_title="PI30",
            connected=True,
            collector=None,
            inverter=None,
            values={},
            data={},
            options={},
            profile_name="",
            register_schema_name="",
        )
        with self.assertRaises(TypeError):
            build_support_bundle_payload(**common, telemetry=object())

    def test_builds_support_bundle_payload_with_role_value_split(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="Collector PN E5000020000000",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={"driver_key": "modbus_smg", "model_name": "SMG 6200"},
            values={
                "collector_signal_strength": -67,
                "smartess_protocol_asset_id": "0925",
                "integration_build_git_describe": "v0.2.0-beta.2-75-gabcdef0",
                "runtime_reconnect_count": 1,
                "runtime_inverter_probe_log": [
                    {
                        "driver": "pi30",
                        "elapsed_ms": 1234,
                        "outcome": "matched",
                        "saw_response": True,
                    }
                ],
                "runtime_inverter_probe_total_ms": 1234,
                "last_error": "",
                "operating_mode": "Off-Grid",
            },
            data={
                "collector_ip": "192.168.1.55",
                "collector_pn": "E5000020000000",
                "connection_strategy": "callback_on_demand",
                "endpoint_control_policy": "external",
            },
            # A stale legacy value must not drive the support projection.
            options={"collector_operation_mode": "home_assistant_only"},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
        )

        self.assertIn("collector_signal_strength", raw["roles"]["collector"]["values"])
        self.assertIn("smartess_protocol_asset_id", raw["roles"]["collector"]["values"])
        self.assertIn("integration_build_git_describe", raw["roles"]["integration"]["values"])
        self.assertIn("runtime_reconnect_count", raw["roles"]["integration"]["values"])
        self.assertEqual(
            raw["roles"]["integration"]["values"]["runtime_inverter_probe_log"][0][
                "driver"
            ],
            "pi30",
        )
        self.assertEqual(
            raw["roles"]["integration"]["values"][
                "runtime_inverter_probe_total_ms"
            ],
            1234,
        )
        self.assertIn("last_error", raw["roles"]["integration"]["values"])
        self.assertIn("operating_mode", raw["roles"]["inverter"]["values"])
        self.assertNotIn("operation_mode", raw["roles"]["collector"]["identity"])
        self.assertEqual(
            raw["roles"]["collector"]["identity"]["operating_profile"],
            "smartess_cloud_home_assistant",
        )
        self.assertEqual(
            raw["roles"]["diagnostics"]["collector_identity"][
                "operating_profile"
            ],
            {
                "profile": "smartess_cloud_home_assistant",
                "stable": True,
                "reason": "callback_external",
            },
        )

    def test_builds_support_bundle_payload_with_descriptor_decision_shadow_evidence(self) -> None:
        descriptor_decision_shadow = {
            "kind": "descriptor_decision_shadow",
            "agreement": "match",
            "evaluation": {"status": "resolved", "resolved_key": "smg_6200"},
        }

        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="SMG 6200",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "SMG 6200",
                "details": {
                    "device_catalog": {
                        "descriptor_decision": descriptor_decision_shadow,
                    },
                },
            },
            values={"operating_mode": "Off-Grid"},
            data={},
            options={},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
            variant_key="default",
        )

        self.assertEqual(
            raw["evidence"]["descriptor_decision_shadow"],
            descriptor_decision_shadow,
        )

    def test_builds_support_bundle_with_canonical_catalog_detection(self) -> None:
        catalog_detection = {
            "resolution": "exact",
            "candidate_keys": ["smg_6200"],
            "surface_key": "smg_6200_full",
            "catalog_version": "2026.06.2",
        }

        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="SMG 6200",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "SMG 6200",
                "details": {
                    "device_catalog": {
                        "compiled_resolution": catalog_detection,
                    },
                },
            },
            values={},
            data={},
            options={},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
            variant_key="default",
        )

        self.assertEqual(raw["evidence"]["catalog_detection"], catalog_detection)

    def test_builds_support_bundle_payload_with_smartess_raw_effective_split(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-smartess",
            entry_title="SmartESS 0925",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "pi30",
                "model_name": "SmartESS 0925",
                "variant_key": "default",
                "serial_number": "92632500000001",
                "profile_name": "pi30_ascii/models/smartess_0925_compat.json",
                "register_schema_name": "pi30_ascii/models/smartess_0925_compat.json",
            },
            values={"operating_mode": "Off-Grid"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="pi30_ascii/models/smartess_0925_compat.json",
            register_schema_name="pi30_ascii/models/smartess_0925_compat.json",
            variant_key="default",
            effective_owner_key="pi30",
            effective_owner_name="PI30-family runtime",
            smartess_family_name="SmartESS 0925",
            raw_profile_name="smartess_local/models/0925.json",
            raw_register_schema_name="smartess_local/models/0925.json",
            smartess_protocol_asset_id="0925",
            smartess_profile_key="smartess_0925",
        )

        self.assertEqual(raw["source_metadata"]["effective_owner_key"], "pi30")
        self.assertEqual(raw["source_metadata"]["effective_owner_name"], "PI30-family runtime")
        self.assertEqual(raw["source_metadata"]["smartess_family_name"], "SmartESS 0925")
        self.assertEqual(
            raw["source_metadata"]["raw_profile_name"],
            "smartess_local/models/0925.json",
        )
        self.assertEqual(raw["source_metadata"]["smartess_protocol_asset_id"], "0925")

    def test_builds_support_bundle_payload_with_family_fallback_marker(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-fallback",
            entry_title="SMG Family",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "SMG Family",
                "variant_key": "family_fallback",
                "serial_number": "92632500000001",
                "profile_name": "modbus_smg/family_fallback.json",
                "register_schema_name": "modbus_smg/base.json",
            },
            values={"operating_mode": "Off-Grid"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="modbus_smg/family_fallback.json",
            register_schema_name="modbus_smg/base.json",
            variant_key="family_fallback",
            support_marker=_smg_marker_payload(
                variant_key="family_fallback",
                profile_name="modbus_smg/family_fallback.json",
            ),
        )

        marker = raw["source_metadata"]["support_marker"]
        self.assertEqual(marker["key"], "read_only_unverified_smg_family")
        self.assertEqual(marker["label"], "Read-only unverified SMG family")
        self.assertTrue(marker["read_only"])
        self.assertEqual(marker["verification"], "unverified")

    def test_builds_support_bundle_payload_with_non_fallback_read_only_smg_profile_marker(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-doc-backed",
            entry_title="SMG Candidate",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "SMG Candidate",
                "variant_key": "doc_backed_variant",
                "serial_number": "SMG11K240123",
                "profile_name": "modbus_smg/family_fallback.json",
                "register_schema_name": "modbus_smg/base.json",
            },
            values={"operating_mode": "Off-Grid"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="modbus_smg/family_fallback.json",
            register_schema_name="modbus_smg/base.json",
            variant_key="doc_backed_variant",
            support_marker=_smg_marker_payload(
                variant_key="doc_backed_variant",
                profile_name="modbus_smg/family_fallback.json",
            ),
        )

        marker = raw["source_metadata"]["support_marker"]
        self.assertEqual(marker["key"], "read_only_unverified_smg_family")
        self.assertEqual(marker["label"], "Read-only unverified SMG family")
        self.assertTrue(marker["read_only"])
        self.assertEqual(marker["verification"], "unverified")

    def test_builds_support_bundle_payload_without_read_only_marker_for_untested_anenji_4200_profile(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-anenji-4200",
            entry_title="Anenji 4200",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "Anenji 4200 (Protocol 1)",
                "variant_key": "anenji_4200_protocol_1",
                "serial_number": "99432409105281",
                "profile_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                "register_schema_name": "modbus_smg/models/anenji_4200_protocol_1.json",
            },
            values={"operating_mode": "Off-Grid"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
            register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
            variant_key="anenji_4200_protocol_1",
        )

        self.assertIsNone(raw["source_metadata"]["support_marker"])

    def test_builds_support_bundle_payload_without_smg_marker_for_eybond_g_ascii_family(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-eybond-g-ascii",
            entry_title="EyeBond G-ASCII",
            connected=True,
            collector={"collector_pn": "A0000000000001"},
            inverter={
                "driver_key": "eybond_g_ascii",
                "model_name": "EyeBond G-ASCII inverter",
                "variant_key": "g_ascii_family",
                "serial_number": "A0000000000001",
            },
            values={"protocol_id": "EYBOND_G_ASCII"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="",
            register_schema_name="",
            variant_key="g_ascii_family",
            effective_owner_key="eybond_g_ascii",
        )

        self.assertIsNone(raw["source_metadata"]["support_marker"])

    def test_bundle_shows_recovery_structure_without_network_values(self) -> None:
        # The bundle exposes the proof STRUCTURE (booleans/methods/timestamps)
        # and never the raw trigger target / advertised endpoint snapshots.
        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="SMG 6200",
            connected=True,
            collector={"collector_pn": "V001020SYN62344022"},
            inverter=None,
            values={},
            data=_recovery_contract_data(),
            options={},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
        )

        recovery = raw["roles"]["diagnostics"]["recovery"]
        self.assertTrue(recovery["recovery_contract_valid"])
        self.assertTrue(recovery["recovery_contract_identity_strong"])
        self.assertTrue(recovery["recovery_contract_pn_bound"])
        self.assertTrue(recovery["inbound_recovery_verified"])
        self.assertEqual(
            recovery["inbound_recovery_method"], "reboot_reconnect_no_trigger"
        )
        self.assertTrue(recovery["callback_recovery_verified"])
        self.assertEqual(
            recovery["callback_recovery_method"], "reset_unicast_reconnect_same_pn"
        )
        self.assertTrue(recovery["callback_route_bound"])
        self.assertTrue(recovery["advertised_endpoint_bound"])

        # Non-disclosure: the whole serialized bundle carries neither the raw
        # route/endpoint snapshots nor even their address components. (The raw
        # entry data section is deliberately not part of a support bundle.)
        serialized = json.dumps(raw)
        for secret in (
            "203.0.113.55:58899",
            "192.0.2.10:18899",
            "203.0.113.55",
            "192.0.2.10",
        ):
            self.assertNotIn(secret, serialized)

    def test_bundle_never_leaks_strategy_transition_recovery_route(self) -> None:
        # Batch 8A: the persisted degraded recovery state carries the full
        # repair route (trigger target / advertised HA endpoint / local bind).
        # The support bundle exposes ONLY the typed diagnostics view; the whole
        # serialized bundle must contain none of the route values or components.
        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            StrategyTransitionRecoveryState,
        )
        from custom_components.eybond_local.const import (
            CONF_STRATEGY_TRANSITION_STATE,
        )

        state = StrategyTransitionRecoveryState.create(
            collector_pn="V001020SYN62344022",
            now="2026-07-16T10:00:00+00:00",
            trigger_target_host="203.0.113.77",
            trigger_udp_port=58899,
            advertised_host="public.forward.example",
            advertised_port=48899,
            trigger_bind_host="10.9.8.7",
            listener_bind_host="10.9.8.7",
            local_listener_port=18899,
        )
        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="SMG 6200",
            connected=True,
            collector={"collector_pn": "V001020SYN62344022"},
            inverter=None,
            values={},
            data={
                "server_ip": "192.168.1.50",
                "collector_pn": "V001020SYN62344022",
                CONF_STRATEGY_TRANSITION_STATE: state.to_record(),
            },
            options={},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
        )

        # The redacted diagnostics view IS present (kind / route-completeness),
        # and it is the typed diagnostics -- not the raw record.
        identity = raw["roles"]["diagnostics"]["collector_identity"]
        state_diag = identity["connection_strategy_transition_state"]
        self.assertEqual(state_diag["kind"], "callback_transition_unproven")
        self.assertTrue(state_diag["route_complete"])
        # Blocker 5: the lifecycle PHASE is surfaced (redacted diagnostics only,
        # a bounded enum value) so support can tell a write-ahead pending state
        # from a confirmed-unproven one -- while the raw route stays out.
        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            RECOVERY_PHASE_PENDING,
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
        )

        self.assertIn(
            state_diag["phase"],
            {RECOVERY_PHASE_PENDING, RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN},
        )
        self.assertNotIn("trigger_target_host", state_diag)
        self.assertNotIn("advertised_host", state_diag)
        self.assertNotIn("trigger_bind_host", state_diag)
        self.assertNotIn("listener_bind_host", state_diag)

        # The verbatim entry-data copy is a redacted pointer, not the record.
        self.assertIsInstance(
            raw["entry"]["data"][CONF_STRATEGY_TRANSITION_STATE], str
        )
        self.assertIn(
            "redacted", raw["entry"]["data"][CONF_STRATEGY_TRANSITION_STATE]
        )

        # Non-disclosure over the WHOLE serialized bundle: no route value or
        # component anywhere.
        serialized = json.dumps(raw)
        for secret in (
            "203.0.113.77",
            "public.forward.example",
            "10.9.8.7",
            "203.0.113.77:58899",
            "public.forward.example:48899",
        ):
            self.assertNotIn(secret, serialized)

    def test_bundle_shows_confirmed_phase_for_persisted_confirmed_state(self) -> None:
        # Batch 8A.1: a persisted CONFIRMED-unproven recovery state must surface
        # its phase in the redacted diagnostics as exactly the confirmed phase
        # (never the pending default), while still leaking no route value.
        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
            StrategyTransitionRecoveryState,
        )
        from custom_components.eybond_local.const import (
            CONF_STRATEGY_TRANSITION_STATE,
        )

        confirmed = StrategyTransitionRecoveryState.create(
            collector_pn="V001020SYN62344022",
            now="2026-07-16T10:00:00+00:00",
            trigger_target_host="203.0.113.77",
            trigger_udp_port=58899,
            advertised_host="public.forward.example",
            advertised_port=48899,
            trigger_bind_host="10.9.8.7",
            listener_bind_host="10.9.8.7",
            local_listener_port=18899,
        ).with_phase(
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
            now="2026-07-16T11:00:00+00:00",
        )
        # The persisted record round-trips to the confirmed phase (guards the
        # persistence fix end to end through what the coordinator stores).
        record = confirmed.to_record()
        self.assertEqual(record["phase"], RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN)

        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="SMG 6200",
            connected=True,
            collector={"collector_pn": "V001020SYN62344022"},
            inverter=None,
            values={},
            data={
                "server_ip": "192.168.1.50",
                "collector_pn": "V001020SYN62344022",
                CONF_STRATEGY_TRANSITION_STATE: record,
            },
            options={},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
        )
        state_diag = raw["roles"]["diagnostics"]["collector_identity"][
            "connection_strategy_transition_state"
        ]
        self.assertEqual(
            state_diag["phase"], RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN
        )
        # Still no raw route anywhere in the serialized bundle.
        serialized = json.dumps(raw)
        for secret in ("203.0.113.77", "public.forward.example", "10.9.8.7"):
            self.assertNotIn(secret, serialized)

    def test_bundle_recovery_section_reports_absent_contract_honestly(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="SMG 6200",
            connected=True,
            collector=None,
            inverter=None,
            values={},
            data={"server_ip": "192.168.1.50"},
            options={},
            profile_name="",
            register_schema_name="",
        )
        recovery = raw["roles"]["diagnostics"]["recovery"]
        self.assertFalse(recovery["recovery_contract_valid"])
        self.assertFalse(recovery["inbound_recovery_verified"])
        self.assertFalse(recovery["callback_recovery_verified"])
        self.assertFalse(recovery["callback_route_bound"])

    def test_export_support_bundle_writes_json_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = export_support_bundle(
                config_dir=Path(temp_dir),
                entry_id="entry123",
                entry_title="SMG 6200",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter={"model_name": "SMG 6200"},
                values={"operating_mode": "Off-Grid"},
                data={"server_ip": "192.168.1.50"},
                options={"poll_interval": 10},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
                variant_key="default",
                cloud_evidence=_sample_cloud_evidence(),
            )

            self.assertTrue(path.exists())
            exported = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(exported["entry"]["entry_id"], "entry123")
            self.assertEqual(exported["runtime"]["values"]["operating_mode"], "Off-Grid")
            self.assertEqual(exported["evidence"]["cloud"]["source"], "smartess_cloud_probe")
if __name__ == "__main__":
    unittest.main()
