from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.effective_metadata import (  # noqa: E402
    resolve_effective_metadata_selection,
)
from custom_components.eybond_local.metadata.profile_loader import (  # noqa: E402
    clear_profile_loader_cache,
    set_external_profile_roots,
)
from custom_components.eybond_local.metadata.register_schema_loader import (  # noqa: E402
    clear_register_schema_loader_cache,
    set_external_register_schema_roots,
)
from custom_components.eybond_local.models import CollectorInfo  # noqa: E402
from custom_components.eybond_local.device_scoped_overlay import (  # noqa: E402
    filter_learned_read_measurements_for_activation,
)
from custom_components.eybond_local.support.shadow_learning_review_model import (  # noqa: E402
    attach_learned_read_review_model,
    build_activation_selection,
    build_learned_control_review_model,
    normalize_activation_selection,
)


class DeviceScopedOverlayActivationTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_external_profile_roots(())
        set_external_register_schema_roots(())
        clear_profile_loader_cache()
        clear_register_schema_loader_cache()
        super().tearDown()

    def test_activation_scope_base_profile_mismatch_prevents_overlay_application(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="SN-001",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000",
                    smartess_device_address=1,
                    smartess_protocol_profile_key="smartess_0925",
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {
                            "effective_owner_key": "modbus_smg",
                            "base_profile_name": "modbus_smg/family_fallback.json",
                            "base_register_schema_name": "modbus_smg/models/smg_6200.json",
                        },
                    }
                },
            )

            self.assertFalse(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, "smg_modbus.json")

    def test_activation_scope_with_matching_profile_and_smartess_key_applies_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="SN-001",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000",
                    smartess_device_address=1,
                    smartess_protocol_profile_key="smartess_0925",
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {
                            "effective_owner_key": "modbus_smg",
                            "base_profile_name": "smg_modbus.json",
                            "base_register_schema_name": "modbus_smg/models/smg_6200.json",
                            "smartess_protocol_profile_key": "smartess_0925",
                        },
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, profile_name)
            self.assertEqual(selection.register_schema_name, schema_name)

    def test_placeholder_protocol_asset_id_does_not_suppress_overlay(self) -> None:
        # Regression: ValueCloud / AT collectors can record "0000" in the
        # activation scope as a placeholder protocol asset id while the local
        # runtime exposes the same absence of identity as an empty string. This
        # must not suppress the learned overlay after reload.
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="SN-001",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000",
                    smartess_device_address=1,
                    smartess_protocol_asset_id="",
                    smartess_protocol_profile_key="",
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {
                            "effective_owner_key": "modbus_smg",
                            "base_profile_name": "smg_modbus.json",
                            "base_register_schema_name": "modbus_smg/models/smg_6200.json",
                            "smartess_protocol_asset_id": "0000",
                        },
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, profile_name)

    def test_overlay_applies_despite_cloud_sn_vs_modbus_serial_mismatch(self) -> None:
        # Regression: session.cloud_sn is the SmartESS device serial (e.g.
        # "SN-001") and never equals inverter.serial_number (the Modbus serial),
        # so gating on it silently suppressed every activated overlay and the
        # learned control never appeared. Identity is pinned by
        # collector_pn/devcode/devaddr, so the overlay must apply.
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="92632500000001",  # Modbus serial != session.cloud_sn
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000",
                    smartess_device_address=1,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, profile_name)

    def test_overlay_applies_when_runtime_devaddr_unknown(self) -> None:
        # Confirm-only: the overlay's session.devaddr (from SmartESS) must not
        # block when the runtime device address is unknown (None) — the device is
        # pinned by collector_pn. This was the second silent suppressor that kept
        # the activated control from appearing.
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="92632500000001",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000",
                    smartess_device_address=None,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, profile_name)

    def test_activation_scope_serial_mismatch_does_not_suppress(self) -> None:
        # Regression: the inverter serial is read from a Modbus holding register and
        # is not reliably populated on every coordinator update (it can be empty on
        # early refreshes, before identity registers are read). Because the overlay
        # is resolved on every update -- including the early ones that run while
        # entities are first set up -- gating on the serial intermittently suppressed
        # the activated overlay, so the learned control never materialized. A stale
        # or differing activation_scope serial must NOT suppress the overlay when the
        # stable identity still matches.
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="92632500000001",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000",
                    smartess_device_address=1,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {"inverter_serial": "SN-DIFFERENT"},
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, profile_name)

    def test_activation_scope_poisoned_base_names_self_heal(self) -> None:
        # Regression: activating an overlay while another overlay is already active
        # captured ``effective_*_name`` (the PREVIOUS learned overlay) into
        # activation_scope.base_profile_name / base_register_schema_name. On reload the
        # runtime base resolves to the built-in name, so the raw comparison always
        # failed and silently suppressed the activation -- the learned control never
        # materialized. The scope matcher now rebases both sides to the built-in base,
        # so an activation poisoned with learned base names still self-heals.
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="92632500000001",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000",
                    smartess_device_address=1,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {
                            # Poisoned with the previous overlay's learned names.
                            "base_profile_name": profile_name,
                            "base_register_schema_name": schema_name,
                        },
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, profile_name)

    def test_collector_pn_prefix_form_still_matches(self) -> None:
        # Regression: the datalogger PN is reported as a short physical prefix early in
        # the handshake ("E5000020000000") and upgraded to the full PN later
        # ("E50000200000000001"). The overlay manifest captured the short form; an exact
        # compare against the upgraded runtime PN intermittently suppressed the overlay
        # during the early refreshes that gate entity setup, so the learned controls never
        # appeared. A prefix relationship is the same datalogger and must still match.
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="92632500000001",
                ),
                collector=CollectorInfo(
                    # Manifest session recorded "E5000020000000"; runtime upgraded to full.
                    collector_pn="E50000200000000001",
                    smartess_device_address=1,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)

    def test_collector_pn_unrelated_value_still_fails(self) -> None:
        # The prefix tolerance must not match an unrelated datalogger.
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="92632500000001",
                ),
                collector=CollectorInfo(
                    collector_pn="Z9999999999999",
                    smartess_device_address=1,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                    }
                },
            )

            self.assertFalse(selection.device_scoped_overlay_active)

    def test_active_overlay_reloads_learned_metadata_over_builtin_snapshot(self) -> None:
        # Regression (the real "control never appears" cause): the coordinator passes a
        # persisted snapshot whose metadata reflects the BUILT-IN base. When an overlay is
        # active, the effective profile/schema MUST be reloaded from the activated learned
        # names -- otherwise profile_metadata stays the built-in (zero device-scoped
        # controls), so nothing merges into the runtime inverter and the learned control
        # never becomes an entity.
        from custom_components.eybond_local.metadata.effective_metadata_snapshot import (
            effective_metadata_snapshot_from_dict,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            snapshot = effective_metadata_snapshot_from_dict(
                {
                    "effective_owner_key": "modbus_smg",
                    "profile_name": "smg_modbus.json",
                    "register_schema_name": "modbus_smg/models/smg_6200.json",
                    "variant_key": "default",
                    "is_valid": True,
                }
            )
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="92632500000001",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000", smartess_device_address=1
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                    }
                },
                persisted_snapshot=snapshot,
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            device_scoped = [
                capability
                for capability in selection.profile_metadata.capabilities
                if capability.is_device_scoped_experimental
            ]
            self.assertTrue(
                device_scoped,
                "active overlay must reload learned metadata, not reuse the builtin snapshot",
            )

    def test_overlay_stays_active_when_inverter_identity_not_yet_known(self) -> None:
        # Regression: entity platforms are set up right after the activation reload, often
        # before the live inverter is detected (the collector reconnects after a scan), so
        # the runtime inverter's model/variant are still empty. Gating on them then
        # suppressed the overlay at the very moment entities are created -- and because the
        # snapshot-backed inverter is itself built from effective_metadata, the learned
        # capabilities never flowed into it. Unknown (empty) identity must NOT reject; only
        # a concrete mismatch does.
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="",
                    model_name="",
                    variant_key="",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000",
                    smartess_device_address=1,
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {
                            "effective_owner_key": "modbus_smg",
                            "inverter_model": "SMG 6200",
                            "variant_key": "default",
                        },
                    }
                },
            )

            self.assertTrue(selection.device_scoped_overlay_active)

    def test_activation_scope_variant_and_model_must_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
            selection = resolve_effective_metadata_selection(
                inverter=types.SimpleNamespace(
                    driver_key="modbus_smg",
                    profile_name="smg_modbus.json",
                    register_schema_name="modbus_smg/models/smg_6200.json",
                    serial_number="SN-001",
                    variant_key="family_fallback",
                    model_name="SMG-6200",
                ),
                collector=CollectorInfo(
                    collector_pn="E5000020000000",
                    smartess_device_address=1,
                    smartess_protocol_profile_key="smartess_0925",
                ),
                entry_options={
                    "device_scoped_overlay_activation": {
                        "profile_name": profile_name,
                        "register_schema_name": schema_name,
                        "scope": "device",
                        "activation_scope": {
                            "effective_owner_key": "modbus_smg",
                            "base_profile_name": "smg_modbus.json",
                            "base_register_schema_name": "modbus_smg/models/smg_6200.json",
                            "variant_key": "verified_model",
                            "inverter_model": "SMG-5200",
                            "smartess_protocol_profile_key": "smartess_0925",
                        },
                    }
                },
            )

            self.assertFalse(selection.device_scoped_overlay_active)
            self.assertEqual(selection.profile_name, "smg_modbus.json")


    def _matching_overlay_selection(self, temp_dir: str, selection: dict | None) -> object:
        profile_name, schema_name = _write_local_overlay_files(Path(temp_dir))
        activation = {
            "profile_name": profile_name,
            "register_schema_name": schema_name,
            "scope": "device",
        }
        if selection is not None:
            activation.update(selection)
        return resolve_effective_metadata_selection(
            inverter=types.SimpleNamespace(
                driver_key="modbus_smg",
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
                serial_number="SN-001",
            ),
            collector=CollectorInfo(
                collector_pn="E5000020000000",
                smartess_device_address=1,
            ),
            entry_options={"device_scoped_overlay_activation": activation},
        )

    def test_activation_without_selection_resolves_no_selected_control_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            selection = self._matching_overlay_selection(temp_dir, None)

            self.assertTrue(selection.device_scoped_overlay_active)
            # Legacy activation declares no selection -> None keeps prior expose-all behavior.
            self.assertIsNone(selection.device_scoped_overlay_selected_control_keys)

    def test_activation_with_selection_resolves_selected_control_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            selection = self._matching_overlay_selection(
                temp_dir,
                {"selected_control_keys": ["learned_shadow_705"]},
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            self.assertEqual(
                selection.device_scoped_overlay_selected_control_keys,
                frozenset({"learned_shadow_705"}),
            )

    def test_activation_with_empty_selection_resolves_empty_frozenset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            selection = self._matching_overlay_selection(
                temp_dir,
                {"selected_control_keys": []},
            )

            self.assertTrue(selection.device_scoped_overlay_active)
            # An explicit empty selection resolves to an empty set (expose none), not None.
            self.assertEqual(
                selection.device_scoped_overlay_selected_control_keys,
                frozenset(),
            )

    def test_activation_derives_keys_from_selected_controls_when_keys_absent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            selection = self._matching_overlay_selection(
                temp_dir,
                {
                    "selected_controls": [
                        {"key": "learned_shadow_705", "label": "My Control"}
                    ]
                },
            )

            self.assertEqual(
                selection.device_scoped_overlay_selected_control_keys,
                frozenset({"learned_shadow_705"}),
            )


class ActivationSelectionModelTests(unittest.TestCase):
    def _review_model(self) -> dict:
        normal_capability = {
            "key": "learned_power_save_701",
            "title": "Power Save",
            "register": 701,
            "value_kind": "bool",
            "learned_provenance": {
                "scope": "device",
                "cloud_field_id": "power_save",
                "confidence": "high",
            },
        }
        risky_capability = {
            "key": "learned_factory_reset_702",
            "title": "Factory Reset",
            "register": 702,
            "value_kind": "action",
            "learned_provenance": {
                "scope": "device",
                "cloud_field_id": "factory_reset",
                "confidence": "high",
                "safety_class": "destructive_action",
            },
        }
        return build_learned_control_review_model([normal_capability, risky_capability])

    def _review_model_with_reads(self) -> dict:
        return attach_learned_read_review_model(
            self._review_model(),
            learned_read_sensors=[
                {
                    "key": "learned_read_344",
                    "register": 344,
                    "title": "Output 2 Cut-Off SOC Status",
                    "kind": "numeric",
                    "spec_set": "config",
                },
                {
                    "key": "learned_read_239",
                    "register": 239,
                    "title": "Output 2 Apparent Power",
                    "kind": "numeric",
                    "spec_set": "live",
                },
            ],
        )

    def test_build_activation_selection_records_labels_and_excluded_reasons(self) -> None:
        review_model = self._review_model()
        selections = {
            "controls": {
                "learned_power_save_701": {
                    "key": "learned_power_save_701",
                    "label": "Eco Mode",
                    "enabled": True,
                },
                "learned_factory_reset_702": {
                    "key": "learned_factory_reset_702",
                    "label": "Factory Reset",
                    "enabled": False,
                },
            },
            "enabled_by_user": ["learned_power_save_701"],
            "excluded_by_user": ["learned_factory_reset_702"],
        }

        selection = build_activation_selection(
            review_model=review_model, selections=selections
        )

        self.assertEqual(selection["selected_control_keys"], ["learned_power_save_701"])
        self.assertEqual(len(selection["selected_controls"]), 1)
        selected = selection["selected_controls"][0]
        self.assertEqual(selected["key"], "learned_power_save_701")
        # The user-facing label is preserved on the activation.
        self.assertEqual(selected["label"], "Eco Mode")

        self.assertEqual(len(selection["excluded_controls"]), 1)
        excluded = selection["excluded_controls"][0]
        self.assertEqual(excluded["key"], "learned_factory_reset_702")
        self.assertEqual(excluded["risk_level"], "high")
        # The risk reason is preserved so support evidence explains the exclusion.
        self.assertIn("destructive_action", excluded["reasons"])

    def test_build_activation_selection_marks_user_excluded_normal_control(self) -> None:
        review_model = self._review_model()
        selections = {
            "controls": {
                # The user turns off a normal-risk control that would default to enabled.
                "learned_power_save_701": {
                    "key": "learned_power_save_701",
                    "label": "Eco Mode",
                    "enabled": False,
                },
            },
        }

        selection = build_activation_selection(
            review_model=review_model, selections=selections
        )

        self.assertEqual(selection["selected_control_keys"], [])
        excluded_keys = {item["key"]: item for item in selection["excluded_controls"]}
        self.assertIn("learned_power_save_701", excluded_keys)
        self.assertEqual(
            excluded_keys["learned_power_save_701"]["reasons"], ["user_excluded"]
        )

    def test_build_activation_selection_defaults_to_review_model_decisions(self) -> None:
        review_model = self._review_model()

        # With no user choices, normal-risk controls are selected and risky ones excluded.
        selection = build_activation_selection(review_model=review_model, selections=None)

        self.assertEqual(selection["selected_control_keys"], ["learned_power_save_701"])
        self.assertEqual(
            [item["key"] for item in selection["excluded_controls"]],
            ["learned_factory_reset_702"],
        )

    def test_build_activation_selection_records_selected_read_sensors(self) -> None:
        review_model = self._review_model_with_reads()
        selections = {
            "read_sensors": {
                "learned_read_344": {
                    "key": "learned_read_344",
                    "label": "OP2 Cut-Off SOC",
                    "enabled": True,
                },
                "learned_read_239": {
                    "key": "learned_read_239",
                    "label": "OP2 Apparent Power",
                    "enabled": False,
                },
            }
        }

        selection = build_activation_selection(
            review_model=review_model, selections=selections
        )

        self.assertEqual(selection["selected_read_sensor_keys"], ["learned_read_344"])
        self.assertEqual(
            selection["selected_read_sensors"][0]["label"], "OP2 Cut-Off SOC"
        )
        self.assertEqual(
            selection["excluded_read_sensors"][0]["reasons"], ["user_excluded"]
        )

    def test_build_activation_selection_does_not_mutate_review_model(self) -> None:
        review_model = self._review_model()
        before = len(review_model["learned_all"])

        build_activation_selection(review_model=review_model, selections=None)

        self.assertEqual(len(review_model["learned_all"]), before)

    def test_normalize_activation_selection_derives_keys_from_selected_controls(self) -> None:
        normalized = normalize_activation_selection(
            {
                "selected_controls": [
                    {"key": "learned_a_1", "label": "A"},
                    {"key": "learned_b_2", "label": "B"},
                ],
                "excluded_controls": [
                    {"key": "learned_c_3", "reasons": ["one_shot_action"]}
                ],
            }
        )

        self.assertEqual(normalized["selected_control_keys"], ["learned_a_1", "learned_b_2"])
        self.assertEqual(normalized["excluded_controls"][0]["reasons"], ["one_shot_action"])

    def test_normalize_activation_selection_handles_missing_selection(self) -> None:
        normalized = normalize_activation_selection(None)

        self.assertEqual(normalized["selected_controls"], [])
        self.assertEqual(normalized["excluded_controls"], [])
        self.assertEqual(normalized["selected_control_keys"], [])
        self.assertEqual(normalized["selected_read_sensors"], [])
        self.assertEqual(normalized["excluded_read_sensors"], [])
        self.assertEqual(normalized["selected_read_sensor_keys"], [])

    def test_normalize_activation_selection_derives_keys_from_selected_reads(self) -> None:
        normalized = normalize_activation_selection(
            {
                "selected_read_sensors": [
                    {"key": "learned_read_344", "label": "OP2 SOC"},
                    {"key": "learned_read_239", "label": "OP2 Power"},
                ],
                "excluded_read_sensors": [
                    {"key": "learned_read_240", "reasons": ["user_excluded"]}
                ],
            }
        )

        self.assertEqual(
            normalized["selected_read_sensor_keys"],
            ["learned_read_239", "learned_read_344"],
        )
        self.assertEqual(
            normalized["excluded_read_sensors"][0]["reasons"], ["user_excluded"]
        )


class LearnedReadRuntimeFilterTests(unittest.TestCase):
    def test_filter_keeps_only_selected_learned_read_sensors(self) -> None:
        descriptions = [
            types.SimpleNamespace(key="battery_voltage"),
            types.SimpleNamespace(key="learned_read_344"),
            types.SimpleNamespace(key="learned_read_239"),
        ]

        filtered = filter_learned_read_measurements_for_activation(
            descriptions,
            entry_data={},
            entry_options={
                "device_scoped_overlay_activation": {
                    "selected_read_sensor_keys": ["learned_read_344"]
                }
            },
        )

        self.assertEqual(
            [description.key for description in filtered],
            ["battery_voltage", "learned_read_344"],
        )

    def test_filter_is_fail_open_for_legacy_activation_without_read_selection(self) -> None:
        descriptions = [
            types.SimpleNamespace(key="learned_read_344"),
            types.SimpleNamespace(key="learned_read_239"),
        ]

        filtered = filter_learned_read_measurements_for_activation(
            descriptions,
            entry_data={},
            entry_options={"device_scoped_overlay_activation": {}},
        )

        self.assertEqual([description.key for description in filtered], ["learned_read_344", "learned_read_239"])


def _write_local_overlay_files(root: Path) -> tuple[str, str]:
    profiles_root = root / "profiles"
    schemas_root = root / "register_schemas"
    profile_name = "learned/shadow_learning/device/overlay_profile.json"
    schema_name = "learned/shadow_learning/device/overlay_schema.json"
    profile_path = profiles_root / profile_name
    schema_path = schemas_root / schema_name
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.parent.mkdir(parents=True, exist_ok=True)

    profile_path.write_text(
        json.dumps(
            {
                "extends": "smg_modbus.json",
                "profile_key": "local_shadow_test",
                "title": "Local Shadow Test",
                "driver_key": "modbus_smg",
                "protocol_family": "modbus_smg",
                "groups": [{"key": "config", "title": "Config"}],
                "shadow_learning_overlay": {
                    "scope": "device",
                    "source_profile_name": "smg_modbus.json",
                    "source_schema_name": "modbus_smg/models/smg_6200.json",
                    "session": {
                        "collector_pn": "E5000020000000",
                        "cloud_sn": "SN-001",
                        "devaddr": 1,
                    },
                },
                "capabilities": [
                    {
                        "key": "learned_shadow_705",
                        "register": 705,
                        "value_kind": "u16",
                        "note": "learned",
                        "provenance": "cloud_hint",
                        "learned_provenance": {"scope": "device"},
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    schema_path.write_text(
        json.dumps(
            {
                "extends": "builtin:modbus_smg/models/smg_6200.json",
                "schema_key": "local_shadow_test",
                "title": "Local Shadow Test",
                "driver_key": "modbus_smg",
                "protocol_family": "modbus_smg",
                "shadow_learning_overlay": {"scope": "device"},
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    set_external_profile_roots((profiles_root,))
    set_external_register_schema_roots((schemas_root,))
    clear_profile_loader_cache()
    clear_register_schema_loader_cache()
    return profile_name, schema_name


if __name__ == "__main__":
    unittest.main()
