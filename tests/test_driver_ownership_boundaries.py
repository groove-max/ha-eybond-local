"""Architecture guards for the Phase 8 driver-ownership cleanup.

Three protocol/model policies moved out of neutral layers and into the drivers
that own them:

1. Modbus write-error classification -> ``drivers/modbus_write_error.py`` mixin.
2. ASCII support-probe command plans -> owning drivers (PI30, G-ASCII).
3. SMG read-only unverified family-fallback support marker -> the SMG driver.

These tests prove the neutral layers hold none of that policy and the drivers
reproduce the exact previous behaviour.
"""

from __future__ import annotations

import ast
import dataclasses
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.const import DRIVER_HINT_AUTO  # noqa: E402
from custom_components.eybond_local.drivers.base import InverterDriver  # noqa: E402
from custom_components.eybond_local.drivers.eybond_g_ascii import EybondGAsciiDriver  # noqa: E402
from custom_components.eybond_local.drivers.pi30 import Pi30Driver  # noqa: E402
from custom_components.eybond_local.drivers.registry import (  # noqa: E402
    iter_drivers,
    support_marker as driver_support_marker,
)
from custom_components.eybond_local.drivers.smg import SmgModbusDriver  # noqa: E402
from custom_components.eybond_local.drivers.srne import SrneModbusDriver  # noqa: E402
from custom_components.eybond_local.drivers.modbus_write_error import (  # noqa: E402
    ModbusWriteErrorMixin,
    classify_modbus_write_error,
)
from custom_components.eybond_local.drivers.support_marker import (  # noqa: E402
    DriverSupportMarker,
    DriverSupportWorkflow,
)
from custom_components.eybond_local.drivers.support_probe import SupportProbeRequest  # noqa: E402
from custom_components.eybond_local.drivers.write_error import (  # noqa: E402
    WriteErrorClassification,
)
from custom_components.eybond_local.models import ProbeTarget, WriteCapability  # noqa: E402
from custom_components.eybond_local.payload.ascii_line import build_ascii_line_request  # noqa: E402
from custom_components.eybond_local.payload.modbus import ModbusError  # noqa: E402
from custom_components.eybond_local.payload.pi30 import build_request as build_pi30_request  # noqa: E402
from custom_components.eybond_local.support.bundle import build_support_bundle_payload  # noqa: E402
from custom_components.eybond_local.support.workflow import build_support_workflow_state  # noqa: E402

_CC = REPO_ROOT / "custom_components" / "eybond_local"
_HUB = _CC / "runtime" / "hub.py"
_BUNDLE = _CC / "support" / "bundle.py"
_WORKFLOW = _CC / "support" / "workflow.py"


def _module_names(source: str) -> set[str]:
    """Return Name/Attribute identifiers referenced in a module."""

    tree = ast.parse(source)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add((alias.asname or alias.name).split(".")[0])
    return names


def _string_constants(source: str) -> list[str]:
    tree = ast.parse(source)
    return [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def _capability(**overrides) -> WriteCapability:
    fields = dict(key="test_capability", register=354, value_kind="uint", note="")
    fields.update(overrides)
    return WriteCapability(**fields)


class _NeutralDriver(InverterDriver):
    """A minimal concrete driver that overrides nothing policy-specific."""

    key = "neutral_test"
    name = "Neutral Test Driver"
    probe_targets = (ProbeTarget(devcode=1, collector_addr=255, device_addr=1),)
    measurements = ()

    async def async_probe(self, transport, target):
        return None

    async def async_read_values(self, transport, inverter, **kwargs):
        return {}

    async def async_write_capability(self, transport, inverter, capability_key, value):
        return value


# --- Guard 1: hub holds no Modbus/ASCII protocol policy ------------------------


class HubHoldsNoProtocolPolicyGuard(unittest.TestCase):
    def test_hub_has_no_modbus_error_or_exception_code_parser(self) -> None:
        source = _HUB.read_text(encoding="utf-8")
        names = _module_names(source)
        self.assertNotIn("ModbusError", names)
        # No exception-code PARSER: the removed parser matched this prefix.
        # (Reading a neutral CapabilityBlocker.exception_code field is fine.)
        for literal in _string_constants(source):
            self.assertFalse(
                literal.startswith("exception_code:"),
                msg="hub must not parse Modbus exception-code strings",
            )

    def test_hub_has_no_probe_command_literals_or_builders(self) -> None:
        source = _HUB.read_text(encoding="utf-8")
        names = _module_names(source)
        self.assertNotIn("build_pi30_request", names)
        self.assertNotIn("build_ascii_line_request", names)
        command_literals = {"QPI", "QMOD", "QPIGS", "QPIRI", "QID", "GPV"}
        self.assertEqual(
            command_literals.intersection(_string_constants(source)),
            set(),
            msg="hub must not contain PI30/G-ASCII probe command literals",
        )


# --- Guard 2: support layers hold no SMG model policy --------------------------


class SupportLayersHoldNoModelPolicyGuard(unittest.TestCase):
    def test_bundle_and_workflow_have_no_smg_fallback_literals(self) -> None:
        for path in (_BUNDLE, _WORKFLOW):
            source = path.read_text(encoding="utf-8")
            for forbidden in ("modbus_smg", "family_fallback", "modbus_smg/family_fallback.json"):
                self.assertNotIn(
                    forbidden,
                    source,
                    msg=f"{path.name} must not infer SMG state via {forbidden!r}",
                )
            names = _module_names(source)
            self.assertNotIn("is_read_only_unverified_smg_family", names)
            self.assertNotIn("build_support_marker", names)


# --- Guard 3: base driver is neutral ------------------------------------------


class BaseDriverIsNeutralGuard(unittest.TestCase):
    def setUp(self) -> None:
        self.driver = _NeutralDriver()

    def test_empty_write_error_classification(self) -> None:
        classification = self.driver.classify_write_error(
            _capability(), ModbusError("exception_code:1"), operating_mode="Off-Grid"
        )
        self.assertIsInstance(classification, WriteErrorClassification)
        self.assertTrue(classification.is_empty)

    def test_empty_support_probe_plan(self) -> None:
        self.assertEqual(self.driver.support_probe_plan(), ())

    def test_no_support_marker(self) -> None:
        self.assertIsNone(
            self.driver.support_marker(variant_key="family_fallback", profile_name="x")
        )


# --- Guard 4: Modbus classification preserves codes 1/2/3/7 + transient --------


class ModbusClassificationSemanticsGuard(unittest.TestCase):
    def _classify(self, code: int, **cap):
        return classify_modbus_write_error(
            _capability(**cap),
            ModbusError(f"exception_code:{code}"),
            operating_mode="Off-Grid",
        )

    def test_code_1_illegal_function_blocker(self) -> None:
        c = self._classify(1)
        self.assertIsNone(c.user_error)
        self.assertEqual(c.blocker.code, "illegal_function")
        self.assertEqual(c.blocker.exception_code, 1)

    def test_code_2_illegal_data_address_blocker(self) -> None:
        c = self._classify(2)
        self.assertEqual(c.blocker.code, "illegal_data_address")
        self.assertEqual(c.blocker.clear_on, "redetect")

    def test_code_3_user_error_no_persistent_blocker(self) -> None:
        c = self._classify(3, minimum=0, maximum=60, divisor=1)
        self.assertIsNone(c.blocker)
        self.assertIsInstance(c.user_error, ValueError)
        self.assertIn("illegal_data_value:test_capability", str(c.user_error))

    def test_code_7_mode_restricted_when_unsafe_running(self) -> None:
        c = self._classify(7, unsafe_while_running=True, safe_operating_modes=("Standby",))
        self.assertEqual(c.blocker.code, "mode_restricted")
        self.assertEqual(c.blocker.clear_on, "mode_change")

    def test_code_7_unsupported_or_locked_otherwise(self) -> None:
        c = self._classify(7, unsafe_while_running=False)
        self.assertEqual(c.blocker.code, "unsupported_or_locked")

    def test_unknown_code_is_empty(self) -> None:
        self.assertTrue(self._classify(4).is_empty)

    def test_non_modbus_and_pre_write_read_error_are_transient(self) -> None:
        # A non-Modbus error (e.g. a transient pre-write read failure) must not
        # create a blocker -- the classifier stays empty and the hub re-raises.
        from custom_components.eybond_local.drivers.smg import CapabilityPreWriteReadError

        for exc in (
            CapabilityPreWriteReadError("pre_write_read_failed"),
            RuntimeError("collector_disconnected"),
            ModbusError("request_timeout"),
        ):
            classification = classify_modbus_write_error(
                _capability(), exc, operating_mode="Off-Grid"
            )
            self.assertTrue(classification.is_empty, msg=f"{exc!r} must be transient")

    def test_smg_driver_opts_into_shared_classifier(self) -> None:
        c = SmgModbusDriver().classify_write_error(
            _capability(), ModbusError("exception_code:1"), operating_mode="Off-Grid"
        )
        self.assertEqual(c.blocker.code, "illegal_function")


# --- Guard 5: PI30 / G-ASCII plans reproduce exact command set + bytes ---------


class ProbePlanFidelityGuard(unittest.TestCase):
    def test_pi30_plan_matches_previous_command_set_and_bytes(self) -> None:
        plan = Pi30Driver().support_probe_plan()
        self.assertEqual(
            [p.command for p in plan],
            ["QPI", "QMOD", "QPIGS", "QPIRI", "QID"],
        )
        for probe in plan:
            self.assertEqual(probe.payload_family, "pi30_ascii")
            self.assertEqual(probe.request, build_pi30_request(probe.command))

    def test_g_ascii_plan_matches_previous_gpv_request(self) -> None:
        plan = EybondGAsciiDriver().support_probe_plan()
        self.assertEqual([p.command for p in plan], ["GPV"])
        self.assertEqual(plan[0].payload_family, "eybond_g_ascii")
        self.assertEqual(plan[0].request, build_ascii_line_request("GPV"))


# --- Guard 6: failed-detection sweep gathers both plans, no bound driver -------


class ProbeSweepGathersBothPlansGuard(unittest.TestCase):
    def test_registry_sweep_yields_pi30_and_g_ascii_without_bound_driver(self) -> None:
        # This mirrors exactly what the hub iterates: iter_drivers(AUTO) with no
        # already-bound self._driver. Both driver plans must be present.
        plans = [
            probe
            for driver in iter_drivers(DRIVER_HINT_AUTO)
            for probe in driver.support_probe_plan()
        ]
        commands = [p.command for p in plans]
        self.assertEqual(commands, ["QPI", "QMOD", "QPIGS", "QPIRI", "QID", "GPV"])


# --- Guard 7: only the SMG driver produces the fallback marker -----------------


class SmgMarkerAuthorityGuard(unittest.TestCase):
    def test_smg_variant_and_profile_produce_the_marker(self) -> None:
        for kwargs in (
            {"variant_key": "family_fallback"},
            {"profile_name": "modbus_smg/family_fallback.json"},
            {"variant_key": "doc_backed_variant", "profile_name": "modbus_smg/family_fallback.json"},
        ):
            marker = driver_support_marker("modbus_smg", **kwargs)
            self.assertIsNotNone(marker)
            payload = marker.as_payload()
            self.assertEqual(payload["key"], "read_only_unverified_smg_family")
            self.assertTrue(payload["read_only"])
            self.assertEqual(payload["verification"], "unverified")

    def test_real_smg_model_produces_no_marker(self) -> None:
        self.assertIsNone(
            driver_support_marker(
                "modbus_smg",
                variant_key="anenji_4200_protocol_1",
                profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
            )
        )

    def test_other_drivers_with_similar_strings_produce_no_marker(self) -> None:
        # Coincidentally similar variant/profile strings on a non-SMG driver must
        # NOT produce the SMG marker: the SMG driver is the sole authority.
        for driver_key in ("eybond_g_ascii", "pi30", "srne", ""):
            self.assertIsNone(
                driver_support_marker(
                    driver_key,
                    variant_key="family_fallback",
                    profile_name="modbus_smg/family_fallback.json",
                ),
                msg=f"{driver_key!r} must not own the SMG fallback marker",
            )


# --- Guard 8: support layers consume the marker without reconstructing ---------


class SupportLayersConsumeMarkerGuard(unittest.TestCase):
    def _marker(self):
        return driver_support_marker("modbus_smg", variant_key="family_fallback")

    def test_bundle_embeds_supplied_marker_and_never_reconstructs(self) -> None:
        common = dict(
            entry_id="e",
            entry_title="t",
            connected=True,
            collector={},
            inverter={"driver_key": "modbus_smg", "variant_key": "family_fallback"},
            values={},
            data={},
            options={},
            profile_name="modbus_smg/family_fallback.json",
            register_schema_name="modbus_smg/base.json",
            variant_key="family_fallback",
            effective_owner_key="modbus_smg",
        )
        # With the authoritative marker -> embedded verbatim.
        embedded = build_support_bundle_payload(
            **common, support_marker=self._marker().as_payload()
        )
        self.assertEqual(
            embedded["source_metadata"]["support_marker"]["key"],
            "read_only_unverified_smg_family",
        )
        # Without a marker -> None even though variant/profile "look" like SMG:
        # the bundle never reconstructs model policy.
        none_marker = build_support_bundle_payload(**common, support_marker=None)
        self.assertIsNone(none_marker["source_metadata"]["support_marker"])

    def test_workflow_renders_supplied_marker_and_never_reconstructs(self) -> None:
        rendered = build_support_workflow_state(
            has_inverter=True,
            variant_key="family_fallback",
            profile_name="modbus_smg/family_fallback.json",
            effective_owner_key="modbus_smg",
            effective_owner_name="SMG-family runtime",
            detection_confidence="medium",
            support_marker_workflow=self._marker().workflow,
        )
        self.assertEqual(rendered["level"], "family_fallback")
        # Without the marker workflow, the same inputs must NOT reconstruct it.
        not_reconstructed = build_support_workflow_state(
            has_inverter=True,
            variant_key="family_fallback",
            profile_name="modbus_smg/family_fallback.json",
            effective_owner_key="modbus_smg",
            effective_owner_name="SMG-family runtime",
            detection_confidence="medium",
            support_marker_workflow=None,
        )
        self.assertNotEqual(not_reconstructed["level"], "family_fallback")


# --- Follow-up fix 1: probe descriptor carries no owning-driver identity -------


class ProbeDescriptorHasNoOwnerIdentityGuard(unittest.TestCase):
    def test_support_probe_request_has_no_driver_key_field(self) -> None:
        fields = {f.name for f in dataclasses.fields(SupportProbeRequest)}
        self.assertEqual(fields, {"payload_family", "command", "request"})
        self.assertNotIn("driver_key", fields)

    def test_hub_records_registry_driver_key_as_probe_provenance(self) -> None:
        # The hub's evidence dict must stamp each attempt with driver.key from the
        # registry iteration -- the authoritative owner -- not a descriptor field.
        source = _HUB.read_text(encoding="utf-8")
        tree = ast.parse(source)
        func = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.AsyncFunctionDef)
            and n.name == "_async_capture_at_text_ascii_probe"
        )
        # There is a dict key "driver_key" whose value is the attribute driver.key.
        stamped = False
        for node in ast.walk(func):
            if isinstance(node, ast.Dict):
                for k, v in zip(node.keys, node.values):
                    if (
                        isinstance(k, ast.Constant)
                        and k.value == "driver_key"
                        and isinstance(v, ast.Attribute)
                        and v.attr == "key"
                        and isinstance(v.value, ast.Name)
                        and v.value.id == "driver"
                    ):
                        stamped = True
        self.assertTrue(stamped, msg="hub must record driver.key as probe provenance")


# --- Follow-up fix 2: typed immutable workflow across the boundary -------------


class TypedWorkflowBoundaryGuard(unittest.TestCase):
    def test_marker_workflow_is_typed_and_immutable(self) -> None:
        marker = driver_support_marker("modbus_smg", variant_key="family_fallback")
        self.assertIsInstance(marker, DriverSupportMarker)
        self.assertIsInstance(marker.workflow, DriverSupportWorkflow)
        # Frozen marker and frozen nested workflow: neither can be mutated.
        with self.assertRaises(dataclasses.FrozenInstanceError):
            marker.workflow.level = "mutated"
        with self.assertRaises(dataclasses.FrozenInstanceError):
            marker.key = "mutated"
        # Slots: no stray __dict__ that would allow attribute injection.
        self.assertFalse(hasattr(marker.workflow, "__dict__"))

    def test_workflow_layer_does_not_unpack_arbitrary_mapping(self) -> None:
        source = _WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("**support_marker_workflow", source)
        # The support-workflow signature must type the parameter, not accept dict.
        self.assertIn("support_marker_workflow: DriverSupportWorkflow | None", source)

    def test_incomplete_or_foreign_mapping_cannot_cross_boundary(self) -> None:
        # A plain/incomplete mapping is not a DriverSupportWorkflow; the typed
        # signature is the boundary. Rendering reads attributes, so a mapping
        # (no ``.level`` attribute) fails fast rather than silently mis-rendering.
        with self.assertRaises(AttributeError):
            build_support_workflow_state(
                has_inverter=True,
                detection_confidence="medium",
                support_marker_workflow={"level": "family_fallback"},  # type: ignore[arg-type]
            )

    def test_exact_previous_workflow_output_preserved(self) -> None:
        marker = driver_support_marker("modbus_smg", variant_key="family_fallback")
        rendered = build_support_workflow_state(
            has_inverter=True,
            variant_key="family_fallback",
            profile_name="modbus_smg/family_fallback.json",
            effective_owner_key="modbus_smg",
            effective_owner_name="SMG-family runtime",
            detection_confidence="medium",
            support_marker_workflow=marker.workflow,
        )
        self.assertEqual(rendered["level"], "family_fallback")
        self.assertEqual(rendered["level_label"], "Read-only unverified SMG family")
        self.assertEqual(
            rendered["summary"],
            "This inverter is using read-only unverified SMG-family metadata. "
            "Built-in writes are intentionally disabled until the exact model is verified.",
        )
        self.assertEqual(rendered["primary_action"], "create_support_package")
        self.assertEqual(rendered["step_1"], "Create a support archive.")
        self.assertEqual(rendered["step_2"], "Send the ZIP file to the developer.")
        self.assertEqual(
            rendered["step_3"],
            "Treat the current SMG support as read-only until the exact model is verified.",
        )
        self.assertIn("read-only fallback", rendered["next_action"])
        self.assertIn("unverified", rendered["advanced_hint"])
        # ``plan`` is derived from the three steps, exactly as before.
        self.assertEqual(
            rendered["plan"],
            "Step 1: Create a support archive. Step 2: Send the ZIP file to the developer. "
            "Step 3: Treat the current SMG support as read-only until the exact model is verified.",
        )


# --- Follow-up fix 3: read-only SRNE does not opt into write classification ----


class SrneReadOnlyClassificationGuard(unittest.TestCase):
    def test_srne_does_not_inherit_modbus_write_error_mixin(self) -> None:
        self.assertNotIsInstance(SrneModbusDriver(), ModbusWriteErrorMixin)
        self.assertNotIn(ModbusWriteErrorMixin, SrneModbusDriver.__mro__)

    def test_srne_classification_is_empty_for_modbus_exception(self) -> None:
        classification = SrneModbusDriver().classify_write_error(
            _capability(), ModbusError("exception_code:1"), operating_mode="Off-Grid"
        )
        self.assertTrue(classification.is_empty)

    def test_write_capable_modbus_drivers_still_classify(self) -> None:
        from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver
        from custom_components.eybond_local.drivers.must import MustPvPh18Driver
        from custom_components.eybond_local.drivers.smartess_local import SmartEssLocalDriver

        for driver in (
            SmgModbusDriver(),
            MustPvPh18Driver(),
            SmartEssLocalDriver(),
            ModbusCatalogDriver(),
        ):
            self.assertIsInstance(driver, ModbusWriteErrorMixin, msg=driver.key)


# --- Follow-up fix 4: registry.support_marker fails honestly -------------------


class _RaisingDriver(_NeutralDriver):
    key = "raising_test"

    def support_marker(self, *, variant_key="", profile_name=""):
        raise RuntimeError("driver_support_marker_defect")


class RegistrySupportMarkerFailsHonestlyGuard(unittest.TestCase):
    def test_registry_catches_key_error_only(self) -> None:
        source = (_CC / "drivers" / "registry.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        func = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "support_marker"
        )
        handlers = [h for n in ast.walk(func) for h in getattr(n, "handlers", [])]
        self.assertTrue(handlers, msg="support_marker must have an except clause")
        for handler in handlers:
            self.assertIsInstance(handler.type, ast.Name)
            self.assertEqual(
                handler.type.id, "KeyError", msg="only KeyError may be caught"
            )

    def test_unknown_driver_key_returns_none(self) -> None:
        from custom_components.eybond_local.drivers.registry import (
            support_marker as registry_support_marker,
        )

        self.assertIsNone(registry_support_marker("no_such_driver", variant_key="x"))
        self.assertIsNone(registry_support_marker("", variant_key="x"))

    def test_real_driver_error_propagates(self) -> None:
        # A defect inside an existing driver's support_marker must NOT be masked
        # as "no marker": it propagates out of the registry dispatch.
        import custom_components.eybond_local.drivers.registry as registry_module

        original = registry_module.get_driver
        registry_module.get_driver = lambda key: (
            _RaisingDriver() if key == "raising_test" else original(key)
        )
        try:
            with self.assertRaisesRegex(RuntimeError, "driver_support_marker_defect"):
                registry_module.support_marker("raising_test", variant_key="x")
        finally:
            registry_module.get_driver = original


if __name__ == "__main__":
    unittest.main()
