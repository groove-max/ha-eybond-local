"""Captured native commands -> overlay -> actual hub write/readback lifecycle."""

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs

from custom_components.eybond_local.collector.protocol import HEADER_SIZE, build_collector_request
from custom_components.eybond_local.drivers import pi30
from custom_components.eybond_local.dessmonitor_cloud import (
    DessMonitorApiEnvelope, DessMonitorControlField, DessMonitorDeviceIdentity, DessMonitorSession,
)
from custom_components.eybond_local.drivers.pi30_settings import project_learned_settings
from custom_components.eybond_local.metadata.profile_loader import _parse_capability
from custom_components.eybond_local.payload.pi30 import Pi30Error, build_request, parse_response
from custom_components.eybond_local.smartess_cloud import ApiEnvelope, SessionCredentials
from custom_components.eybond_local.support.shadow_learning.backend import InProcessShadowLearningHandler
from custom_components.eybond_local.support.shadow_learning.orchestrator import async_orchestrate_shadow_learning_settings
from custom_components.eybond_local.support.shadow_learning.dessmonitor_orchestrator import async_orchestrate_dessmonitor_shadow_learning
from test_pi30_capability_acquisition import _saved_binding, _session, QPIRI
from test_pi30_setting_readback import _Hub
from test_pi30_shadow_learning import correlation, overlay, seed


class Pi30LearnedReadbackTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.inverter = _saved_binding()
        self.hub = _Hub(self.inverter)
        self.session = _session({})
        for patcher in (
            patch.object(pi30.Pi30Driver, "_session", return_value=self.session),
            patch.object(pi30, "_async_collect_runtime_values", new=AsyncMock(
                return_value=({"operating_mode_code": "L"}, frozenset(), {}))),
            patch.object(pi30, "async_acquire_charge_current_controls", new=AsyncMock(return_value={})),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def activate(self, captured):
        raw, result = overlay(captured)
        self.assertEqual(result.generated_capability_count, 1)
        cap = _parse_capability(raw["capabilities"][0], {}, {}, {})
        self.inverter.capabilities = (cap,)
        self.hub.values.clear()
        return cap

    def commands(self):
        return [call.args[0] for call in self.session.request.await_args_list]

    async def test_boolean_exact_case_and_fresh_flag_confirmation(self):
        cap = self.activate(correlation())
        self.hub._pi30_flag_command_case = "upper"
        self.session.request.side_effect = lambda cmd: "ACK" if cmd == "PEa" else "EaDx"
        self.assertEqual(await self.hub.async_write_capability(cap.key, True), "On")
        self.assertEqual(self.hub.values[cap.key], "On")
        self.assertEqual(self.commands(), ["PEa", "QFLAG"])
        self.assertNotIn(cap.key, self.inverter.details)

    async def test_orchestrator_backend_correlation_overlay_and_hub_readback(self):
        await self._exercise_orchestrator("smartess")

    async def test_dessmonitor_backend_correlation_overlay_and_hub_readback(self):
        await self._exercise_orchestrator("dessmonitor")

    async def _exercise_orchestrator(self, provider):
        native, blockers = seed()
        self.assertFalse(blockers)
        with tempfile.TemporaryDirectory() as directory:
            backend = InProcessShadowLearningHandler(seed=native, output_path=Path(directory) / "trace")
            intercepted = []

            async def dispatch(_fetch, **kwargs):
                value = parse_qs(kwargs["action"])["val"][0] if provider == "smartess" else kwargs["value"]
                command = {"48": "POP00", "49": "POP02"}[value]
                intercepted.append(command)
                reply = await backend._handle_frame(build_collector_request(
                    len(intercepted), build_request(command), devcode=0x994,
                    collector_addr=255, fcode=4,
                ), remote="synthetic")
                self.assertEqual(parse_response(reply[HEADER_SIZE:]), "NAK")
                return (ApiEnvelope(err=0, desc="", dat={}, raw={}) if provider == "smartess"
                        else DessMonitorApiEnvelope(err=0, desc="", dat={}))

            kwargs = dict(confirm_cloud_write=True, shadow_session_state="ready", all_choice_values=True,
                observation_cursor=backend.observation_cursor,
                current_observations_since=backend.observations_since,
                wait_for_observations_since=backend.wait_for_observations_since,
                is_session_ready=lambda: True, dispatch_action=dispatch,
            )
            if provider == "smartess":
                result = await async_orchestrate_shadow_learning_settings(
                    settings_dat={"field": [{"id": "native_mode", "name": "Mode", "item": [
                        {"key": "48", "val": "First"}, {"key": "49", "val": "Second"},
                    ]}]}, session=SessionCredentials(token="synthetic", secret="synthetic"),
                    pn="SYNTHETIC-PN", sn="SYNTHETIC-SN", devcode=0x994, devaddr=255,
                    dry_run=False, field_ids=[], include_numeric=False, **kwargs,
                )
            else:
                result = await async_orchestrate_dessmonitor_shadow_learning(
                    control_fields=(DessMonitorControlField(field_id="native_mode", title="Mode",
                        choices=(("48", "First"), ("49", "Second"))),),
                    session=DessMonitorSession(token="synthetic", secret="synthetic"),
                    identity=DessMonitorDeviceIdentity(pn="SYNTHETIC-PN", sn="SYNTHETIC-SN",
                                                       devcode=0x994, devaddr=255), **kwargs,
                )
            self.assertEqual(intercepted, ["POP00", "POP02"])
            self.assertEqual(result["captured_not_applied_count"], 2)
            self.assertEqual(result["leaked_count"], 0)
            self.assertEqual(self.commands(), [])  # Learning did not touch the inverter.
            cap = self.activate(result["correlation"])
            fields = QPIRI.split()
            fields[16] = "2"
            self.session.request.side_effect = lambda cmd: "ACK" if cmd == "POP02" else " ".join(fields)
            self.assertEqual(await self.hub.async_write_capability(cap.key, "Second"), "Second")
            self.assertEqual(self.commands(), ["POP02", "QPIRI"])

    async def test_provider_codes_and_labels_are_not_native_codes_or_builtin_labels(self):
        cap = self.activate(correlation(("POP00", "POP02"), ("48", "49"), ("First", "Second")))
        fields = QPIRI.split()
        fields[16] = "2"
        self.session.request.side_effect = lambda cmd: "ACK" if cmd == "POP02" else " ".join(fields)
        self.assertEqual(await self.hub.async_write_capability(cap.key, "Second"), "Second")
        self.assertEqual(self.hub.values[cap.key], "Second")
        self.assertEqual(self.commands(), ["POP02", "QPIRI"])

    async def test_numeric_unsampled_24v_value_reads_physical_units(self):
        captured = correlation(("PCVV27.9", "PCVV28.9"), ("27.9", "28.9"))
        for sample in captured["matched"]:
            sample.update(value_source="numeric_probe", value_label="",
                          numeric_evidence={"kind": "number", "unit": "V"})
        cap = self.activate(captured)
        fields = QPIRI.split()
        fields[10] = "28.5"
        self.session.request.side_effect = lambda cmd: "ACK" if cmd == "PCVV28.5" else " ".join(fields)
        self.assertEqual(await self.hub.async_write_capability(cap.key, 28.5), 28.5)
        self.assertEqual(self.hub.values[cap.key], 28.5)
        self.assertEqual(self.commands(), ["PCVV28.5", "QPIRI"])

    async def test_ack_old_value_timeout_and_missing_flag_never_confirm_cached_value(self):
        for response in ("EDa", "ExD", Pi30Error("request_timeout")):
            with self.subTest(response=response):
                cap = self.activate(correlation())
                self.hub._runtime_read_state.clear()
                self.hub.values[cap.key] = "On"
                self.session.request.reset_mock()

                async def request(cmd):
                    if cmd == "PEa":
                        return "ACK"
                    if isinstance(response, Exception):
                        raise response
                    return response

                self.session.request.side_effect = request
                with self.assertRaisesRegex(RuntimeError, "write_not_confirmed"):
                    await self.hub.async_write_capability(cap.key, True)
                self.assertEqual(self.commands(), ["PEa", "QFLAG", "QFLAG"])
                self.assertNotEqual(self.hub.values.get(cap.key), "On")

    async def test_nak_is_not_retried_and_unknown_write_does_not_use_stale_state(self):
        cap = self.activate(correlation())
        self.session.request.side_effect = Pi30Error("nak")
        with self.assertRaises(Pi30Error):
            await self.hub.async_write_capability(cap.key, True)
        self.assertEqual(self.commands(), ["PEa"])
        cap = replace(cap, command_map={0: "PZZ0", 1: "PZZ1"})
        self.inverter.capabilities = (cap,)
        self.hub.values[cap.key] = "On"
        self.session.request.reset_mock()
        self.session.request.side_effect = lambda cmd: "ACK"
        with self.assertRaisesRegex(RuntimeError, "write_not_confirmed"):
            await self.hub.async_write_capability(cap.key, True)
        self.assertEqual(self.commands(), ["PZZ1"])
        self.assertNotIn(cap.key, self.hub.values)

    async def test_periodic_read_projects_external_changes_and_invalidates_unknown_choice(self):
        cap = self.activate(correlation(("POP00", "POP02"), ("48", "49"), ("First", "Second")))
        fields = QPIRI.split()
        fields[16] = "2"
        self.session.request.side_effect = lambda cmd: " ".join(fields) if cmd == "QPIRI" else "EaDx"
        state = {}

        async def read(now):
            return await self.hub._driver.async_read_values(
                None, self.inverter, runtime_state=state, now_monotonic=now,
            )

        await read(100)
        self.assertEqual((await read(160)).values[cap.key], "Second")
        await read(170)
        fields[16] = "1"  # Valid native option not captured in this cloud enum.
        result = await read(221)
        self.assertNotIn(cap.key, result.values)
        self.assertIn(cap.key, result.removed_keys)
        self.assertTrue(all(cmd.startswith("Q") for cmd in self.commands()))

    def test_no_cross_protocol_read_key_or_ambiguous_projection(self):
        captured = correlation()
        for sample in captured["matched"]:
            sample["read_key"] = "output_source_priority"
        cap = self.activate(captured)
        self.assertEqual(cap.value_key, cap.key)
        for mapping in ({0: "PEa", 1: "PEA"}, {0: "PDa", 1: "PEx"}, {0: "PZZ0", 1: "PZZ1"}):
            candidate = replace(cap, command_map=mapping)
            values, _ = project_learned_settings({"buzzer_enabled": True}, (candidate,), "QFLAG")
            self.assertNotIn(cap.key, values)


if __name__ == "__main__":
    unittest.main()
