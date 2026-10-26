"""Model-scoped advisory errors never become limits, retries or write authority."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from custom_components.eybond_local.drivers.modbus_write_error import ModbusWriteErrorMixin
from custom_components.eybond_local.drivers.capability_codec import encode_capability_words
from custom_components.eybond_local.metadata import profile_loader
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.payload.modbus import ModbusError
from custom_components.eybond_local.runtime.hub.management import HubManagementMixin
from custom_components.eybond_local.runtime.write_rejection import with_write_rejection_hint
from custom_components.eybond_local.schema import serialize_capability
from custom_components.eybond_local.telemetry import (
    TelemetryFreshness, TelemetryOrigin, TelemetryPoint, TypedTelemetryFrame,
)

PROFILE = "modbus_smg/models/sandisolar_sd_11kp48v_wifi.json"
KEY = "op1_offgrid_soc_protection_value"
GRID = "low_dc_protection_soc_grid_mode"


def snapshot():
    profile = profile_loader.load_driver_profile(PROFILE)
    inverter = DetectedInverter(
        driver_key="modbus_smg", protocol_family="modbus_smg",
        model_name="Sandisolar SD 11KP48V WIFI", serial_number="synthetic",
        probe_target=ProbeTarget(devcode=1, collector_addr=2, device_addr=1),
        capabilities=profile.capabilities, capability_groups=profile.groups,
    )
    return RuntimeSnapshot(
        connected=True, inverter=inverter,
        telemetry=TypedTelemetryFrame("modbus_smg", (
            TelemetryPoint(GRID, 20, TelemetryFreshness.FRESH),
            TelemetryPoint(KEY, 18, TelemetryFreshness.FRESH),
        )),
    )


class WriteRejectionHintTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = snapshot()
        self.cap = self.snapshot.inverter.get_capability(KEY)
        self.error = ModbusWriteErrorMixin().classify_write_error(
            self.cap, ModbusError("exception_code:3")
        ).user_error

    def render(self, *, age=1, request=25):
        return str(with_write_rejection_hint(
            self.error, self.cap, requested_value=request,
            snapshot=self.snapshot, observation_age_seconds=age,
        ))

    def test_exact_model_hint_preserves_wire_limits_and_validation_status(self):
        self.assertEqual(self.cap.register, 649)
        self.assertEqual((self.cap.minimum, self.cap.maximum), (0, 100))
        self.assertFalse(self.cap.tested)
        self.assertEqual(self.cap.provenance, "inferred")
        self.assertEqual(encode_capability_words(self.cap, 25), [25])
        for profile_name in (
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            "modbus_smg/protocols/communication_protocol_4.json",
        ):
            cap = profile_loader.load_driver_profile(profile_name).get_capability(KEY)
            self.assertEqual(cap.write_rejection_hint, "")
            self.assertEqual(cap.write_rejection_context, ())

    def test_message_qualifies_bms_mode_and_retains_original_exception(self):
        message = self.render()
        self.assertTrue(message.startswith(str(self.error)))
        for expected in ("not a confirmed cause", "communicating with the BMS",
                         "1 percentage point", "95%", "OP2 cut-off SOC (08)",
                         "Requested value: 25%", "Grid Mode' = 20%"):
            self.assertIn(expected, message)
        self.assertNotIn("maximum is 19", message)

    def test_request_within_documented_relationship_does_not_claim_cause(self):
        self.assertIn("not a confirmed cause", self.render(request=18))

    def test_zero_is_a_reading_not_missing(self):
        point = replace(self.snapshot.telemetry.points[0], value=0)
        self.snapshot.telemetry = TypedTelemetryFrame("modbus_smg", (point,))
        self.assertIn("Grid Mode' = 0%", self.render())

    def test_no_numbers_from_stale_invalid_or_missing_observation_time(self):
        for age in (None, -1, 30.01, float("nan"), float("inf"), True, "1"):
            with self.subTest(age=age):
                message = self.render(age=age)
                self.assertNotIn("Grid Mode' =", message)
                self.assertIn("No fresh readings", message)
                self.assertIn("1 percentage point", message)
        self.assertIn("Grid Mode' = 20%", self.render(age=30))

    def test_failed_offline_and_other_driver_snapshots_do_not_supply_numbers(self):
        for field, value in (("connected", False), ("last_error", "request_timeout"),
                             ("inverter", None),
                             ("telemetry", TypedTelemetryFrame("another_driver", self.snapshot.telemetry.points))):
            with self.subTest(field=field):
                previous = getattr(self.snapshot, field)
                setattr(self.snapshot, field, value)
                self.assertNotIn("Grid Mode' =", self.render())
                setattr(self.snapshot, field, previous)

    def test_carried_canonical_and_invalid_values_do_not_supply_numbers(self):
        source = self.snapshot.telemetry.points[0]
        cases = [replace(source, freshness=TelemetryFreshness.CARRIED),
                 replace(source, origin=TelemetryOrigin.CANONICAL, source_keys=("raw",))]
        cases += [replace(source, value=value) for value in (None, True, "20", "unavailable")]
        for point in cases:
            with self.subTest(point=point):
                self.snapshot.telemetry = TypedTelemetryFrame("modbus_smg", (point,))
                self.assertNotIn("Grid Mode' =", self.render())

    def test_metadata_and_startup_details_never_substitute_for_typed_readings(self):
        self.snapshot.telemetry = TypedTelemetryFrame.empty()
        self.snapshot.values[GRID] = 88
        self.snapshot.inverter.details[GRID] = 99
        self.assertNotIn("Grid Mode' =", self.render())

    def test_no_hint_returns_exact_original_error(self):
        cap = replace(self.cap, write_rejection_hint="", write_rejection_context=())
        self.assertIs(with_write_rejection_hint(
            self.error, cap, requested_value=25, snapshot=self.snapshot,
            observation_age_seconds=1,
        ), self.error)

    def test_only_value_rejection_opts_into_advice(self):
        driver = ModbusWriteErrorMixin()
        for code in (1, 2, 3, 4, 7):
            verdict = driver.classify_write_error(self.cap, ModbusError(f"exception_code:{code}"))
            self.assertEqual(verdict.include_profile_hint, code == 3)
        self.assertFalse(driver.classify_write_error(self.cap, TimeoutError()).include_profile_hint)

    def test_hint_is_exported_without_dynamic_ui_bounds(self):
        result = serialize_capability(self.cap, self.snapshot.inverter, self.snapshot.runtime_values())
        self.assertEqual(result["write_rejection_hint"], self.cap.write_rejection_hint)
        self.assertEqual(result["write_rejection_context"], [GRID])
        self.assertEqual((result["minimum"], result["maximum"]), (0, 100))

    def test_malformed_hint_metadata_is_rejected(self):
        for raw in (
            {"write_rejection_hint": None}, {"write_rejection_hint": " "},
            {"write_rejection_context": [GRID]},
            {"write_rejection_hint": "hint", "write_rejection_context": GRID},
            {"write_rejection_hint": "hint", "write_rejection_context": [GRID, GRID]},
            {"write_rejection_hint": "hint", "write_rejection_context": [{}]},
        ):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                profile_loader._parse_write_rejection_hint(raw)

    def test_unknown_related_capability_is_rejected(self):
        profile = profile_loader.load_driver_profile(PROFILE)
        cap = replace(self.cap, write_rejection_context=("missing",))
        profile = replace(profile, capabilities=tuple(
            cap if item.key == KEY else item for item in profile.capabilities
        ))
        with self.assertRaisesRegex(ValueError, "unknown_write_rejection_context"):
            profile_loader._validate_profile(profile)


class HubWriteRejectionHintTests(unittest.IsolatedAsyncioTestCase):
    def make_hub(self, failures):
        snap = snapshot()
        hub = HubManagementMixin()
        hub._inverter = snap.inverter
        hub._driver = ModbusWriteErrorMixin()
        hub._driver.async_write_capability = AsyncMock(side_effect=failures)
        hub._async_ensure_connected = AsyncMock()
        hub.async_refresh = AsyncMock(return_value=snap)
        hub._link_manager = SimpleNamespace(transport=object())
        hub._runtime_read_state = {}
        hub._write_blockers = {}
        return hub

    async def test_real_hub_value_rejection_is_one_write_no_blocker_or_extra_read(self):
        hub = self.make_hub(ModbusError("exception_code:3"))
        with self.assertRaises(ValueError) as caught:
            await hub.async_write_capability(KEY, 25)
        self.assertIn("Grid Mode' = 20%", str(caught.exception))
        self.assertIsInstance(caught.exception.__cause__, ModbusError)
        hub._driver.async_write_capability.assert_awaited_once()
        hub.async_refresh.assert_awaited_once()
        self.assertEqual(hub._write_blockers, {})

    async def test_long_prewrite_refresh_drops_numbers_not_advice(self):
        hub = self.make_hub(ModbusError("exception_code:3"))
        with patch("custom_components.eybond_local.runtime.hub.management.monotonic",
                   side_effect=(100.0, 131.0, 132.0)):
            with self.assertRaises(ValueError) as caught:
                await hub.async_write_capability(KEY, 25)
        self.assertIn("No fresh readings", str(caught.exception))
        self.assertIn("1 percentage point", str(caught.exception))

    async def test_reconnect_invalidates_prewrite_numbers(self):
        hub = self.make_hub([ConnectionError("collector_disconnected"), ModbusError("exception_code:3")])
        with self.assertRaises(ValueError) as caught:
            await hub.async_write_capability(KEY, 25)
        self.assertIn("No fresh readings", str(caught.exception))
        self.assertEqual(hub._driver.async_write_capability.await_count, 2)
        self.assertEqual(hub._write_blockers, {})

    async def test_success_is_not_preblocked_by_documented_relationship(self):
        hub = self.make_hub(None)
        hub._driver.async_write_capability.return_value = 25
        snap = hub.async_refresh.return_value
        snap.telemetry = TypedTelemetryFrame("modbus_smg", tuple(
            replace(point, value=25) if point.key == KEY else point
            for point in snap.telemetry.points
        ))
        self.assertEqual(await hub.async_write_capability(KEY, 25), 25)
        hub._driver.async_write_capability.assert_awaited_once()
        self.assertEqual(hub._write_blockers, {})

    async def test_other_protocol_errors_keep_original_semantics(self):
        error = ModbusError("exception_code:2")
        hub = self.make_hub(error)
        with self.assertRaises(ModbusError) as caught:
            await hub.async_write_capability(KEY, 25)
        self.assertIs(caught.exception, error)
        self.assertEqual(hub._write_blockers[KEY].code, "illegal_data_address")
