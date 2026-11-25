"""FGI-S5000: sanitized mains-only replay and checks on the public register index."""

import asyncio
import csv
import json
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.drivers.capability_codec import encode_capability_words
from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver, _clock_advanced
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.runtime.driver_detection import (
    async_detect_inverter, async_detect_inverter_candidates,
)
from custom_components.eybond_local.drivers.probe_evidence import DriverProbeIncomplete


ROOT = Path(__file__).resolve().parents[1]
SAMPLE = json.loads((ROOT / "tests/fixtures/fgi_5ku_mains_only.json").read_text())
TARGET = ProbeTarget(**SAMPLE["target"])


def transport(changes=None):
    words = {a + i: v for a, block in SAMPLE["signature"] + SAMPLE["blocks"]
             for i, v in enumerate(block)}
    words.update(changes or {})
    return FixtureTransport(registers=words, input_registers={}, command_responses=None,
                            probe_target=TARGET)


class Fgi5kuTests(unittest.TestCase):
    def test_auto_selects_the_same_pack_without_a_driver_or_map_override(self):
        async def exercise():
            first = await async_detect_inverter(transport(), driver_hint="auto")
            scan = await async_detect_inverter_candidates(transport(), driver_hint="auto")
            self.assertFalse(scan.budget_exhausted)
            self.assertEqual(len(scan.candidates), 1)
            for context in (first, scan.candidates[0]):
                inverter = context.inverter
                self.assertEqual(context.driver.key, "modbus_catalog")
                self.assertEqual(inverter.variant_key, "fgi_5ku")
                self.assertEqual(inverter.profile_name, "modbus_catalog/fgi_5ku.json")
                self.assertEqual(inverter.register_schema_name, "fgi_5ku/base.json")
                self.assertEqual(len(inverter.capabilities), 74)

        asyncio.run(exercise())

    def test_hardware_signature_and_data(self):
        async def exercise():
            driver, link = ModbusCatalogDriver(), transport()
            requests = []
            forward = link.async_send_forward

            async def record(payload, **kwargs):
                requests.append((payload[1], int.from_bytes(payload[2:4], "big"),
                                 int.from_bytes(payload[4:6], "big")))
                return await forward(payload, **kwargs)

            link.async_send_forward = record
            inverter = await driver.async_probe(link, TARGET)
            assert inverter and inverter.variant_key == "fgi_5ku"
            # A negative lookup on unrelated maps must not invalidate this match.
            assert (3, 56, 2) in requests and (3, 64, 1) in requests
            result = await driver.async_read_values(link, inverter)
            values = result.values
            assert (values["grid_voltage"], values["output_voltage"]) == (232.8, 232.8)
            assert (values["grid_frequency"], values["grid_power"]) == (49.99, -81)
            assert values["output_power"] == 0 and values["work_mode"] == "Utility"
            assert values["setting_bulk_voltage"] == 27.6
            assert values["setting_secondary_duration"] is None
            assert values["function_flags_high"] is None
            verified = {c.key for c in inverter.capabilities if c.tested}
            assert {"backlight_auto_off", "bulk_voltage", "float_voltage", "battery_low_cutoff",
                    "ac_charge_limit", "low_shutdown_soc"} <= verified
            assert {"generator_max_limit", "charging_disabled", "grid_feed_limit"}.isdisjoint(verified)
            assert values["bms_voltage"] is None and values["bms_soc_shutdown_threshold"] is None
            assert values["setting_backlight_auto_off"] == "On"
            live_bms = transport({0x191: 2, 0x193: 271})
            connected = (await driver.async_read_values(live_bms, inverter)).values
            assert connected["bms_voltage"] == 27.1
            assert connected["bms_current"] is None  # 0x7FFF even while linked
            assert all(c.word_count == 1 and c.write_function == 16 for c in inverter.capabilities)
            with patch("custom_components.eybond_local.drivers.modbus_catalog.asyncio.sleep",
                       new_callable=AsyncMock) as sleep:
                assert await driver.async_write_capability(link, inverter, "backlight_auto_off", 0) == "Off"
                sleep.assert_awaited_once_with(1.3)
            # A non-zero high word must contribute to energy; no rollover or sign inversion.
            other = transport({0x221: 1, 0x222: 2})
            assert (await driver.async_read_values(other, inverter)).values["load_energy_day"] == 655.38
            # A matching model word alone (or on a different protocol revision) is not sufficient.
            for anchor in (16, 17, 56, 57, 64):
                wrong = await driver.async_probe(transport({anchor: 99}), TARGET)
                assert wrong is None or wrong.variant_key != "fgi_5ku"

        asyncio.run(exercise())

    def test_user_page_and_independent_write_evidence(self):
        caps = load_driver_profile("modbus_catalog/fgi_5ku.json").capabilities
        by_key = {c.key: c for c in caps}
        # Reset/acknowledge aren't settings; reserved, logger-owned and VML1K2-only.
        excluded = {0x411D, 0x4126, 0x4133, 0x4143, 0x4148}
        assert {c.register for c in caps} == set(range(0x4100, 0x414D)) - excluded
        assert len(by_key) == len(caps) == 74
        spec = load_register_schema("fgi_5ku/base.json").spec_set("controls")
        readable = {s.key: s.register for s in spec}
        assert all(c.value_kind == "action" or readable[c.value_key] == c.register for c in caps)
        fixtures = json.loads((ROOT / "tests/fixtures/fgi_5ku_write_readbacks.json").read_text())
        rows = [(int(a, 16), old, wanted, observed, restored)
                for a, old, wanted, observed, restored in fixtures["attempts"]]
        assert all(old == restored and old != wanted for _, old, wanted, _, restored in rows)
        for c in caps:
            matching = [(wanted, restored) for a, _, wanted, observed, restored in rows
                        if a == c.register and wanted == observed]
            if c.value_kind == "action":
                matching = [r for r in matching if c.action_value in r]
            if c.key == "secondary_duration":
                matching = [r for r in matching if r[0] <= 900]
            assert c.tested == (bool(matching) or c.key in fixtures["earlier_verified_display_controls"]), c.key
        assert encode_capability_words(by_key["bulk_voltage"], 27.7) == [277]
        assert encode_capability_words(by_key["equalization_timeout"], 125) == [125]
        for v in (124, 125.9):
            with self.assertRaises(ValueError):
                encode_capability_words(by_key["equalization_timeout"], v)
        assert encode_capability_words(by_key["secondary_duration_off"], None) == [0xFFF1]
        assert encode_capability_words(by_key["secondary_duration_unlimited"], None) == [0xFFF2]
        assert by_key["discharge_current_code"].runtime_state({"control_discharge_current_code_word": 36}).editable is False
        assert not by_key["equalize_now"].runtime_state({"setting_battery_type": "LiFePO4"}).editable
        assert not by_key["model_configuration"].runtime_state({"work_mode": "Utility"}).editable
        assert by_key["model_configuration"].runtime_state({"work_mode": "Standby"}).editable
        assert _clock_advanced(59, 0, 1.3) and _clock_advanced(59, 1, 1.3)
        assert not any(_clock_advanced(59, value, 1.3) for value in (2, 58, 65535))
        assert not _clock_advanced(59, 0, 7.0)

    def test_prime_once_and_never_trust_echo(self):
        async def exercise():
            driver, link = ModbusCatalogDriver(), transport()
            inverter = await driver.async_probe(link, TARGET)
            forward = link.async_send_forward
            calls = []
            fail_at = None

            async def record(payload, **kwargs):
                address = int.from_bytes(payload[2:4], "big")
                calls.append(address)
                if address == fail_at:
                    raise TimeoutError("test slow page")
                reply = await forward(payload, **kwargs)
                if payload[1] == 16 and address == 0x411C:
                    link._registers[address] = 50  # echoed, but unchanged internally
                return reply

            link.async_send_forward = record
            state = {}
            result = await driver.async_read_values(link, inverter, runtime_state=state)
            assert calls == [64, 80, 128, 150, 0x4100, 0x4110, 0x4120, 0x4130, 0x4140]
            assert result.values["setting_secondary_duration"] is None
            assert result.values["setting_secondary_duration_mode_word"] == 0xFFF1
            calls.clear()
            await driver.async_read_values(link, inverter, runtime_state=state)
            assert calls == [64, 80, 128, 150, 336]
            with patch("custom_components.eybond_local.drivers.modbus_catalog.asyncio.sleep", new_callable=AsyncMock):
                await driver.async_write_capability(link, inverter, "battery_low_warning", 23.9, runtime_state=state)
            calls.clear()
            await driver.async_read_values(link, inverter, runtime_state=state)
            assert calls[-5:] == [0x4100, 0x4110, 0x4120, 0x4130, 0x4140]
            state.clear()
            calls.clear()
            fail_at = 0x4110
            result = await driver.async_read_values(link, inverter, runtime_state=state)
            assert calls[:5] == [64, 80, 128, 150, 0x4100]
            assert set(calls[5:]) == {0x4110}  # the Modbus session may retry that read
            assert result.values["setting_bulk_voltage"] == 27.6
            calls.clear()
            await driver.async_read_values(link, inverter, runtime_state=state)
            assert calls == [64, 80, 128, 150, 336]
            with patch("custom_components.eybond_local.drivers.modbus_catalog.asyncio.sleep", new_callable=AsyncMock):
                with self.assertRaisesRegex(RuntimeError, "readback_mismatch:generator_max_limit"):
                    await driver.async_write_capability(link, inverter, "generator_max_limit", 4.9)

        asyncio.run(exercise())

    def test_slow_but_responsive_catalog_and_hard_caller_deadline(self):
        async def exercise():
            link = transport()
            forward = link.async_send_forward

            async def slow(payload, **kwargs):
                await asyncio.sleep(.8)
                return await forward(payload, **kwargs)

            link.async_send_forward = slow
            loop = asyncio.get_running_loop()
            start = loop.time()
            result = await async_detect_inverter(link, driver_hint="modbus_catalog")
            self.assertGreater(loop.time() - start, 6)  # historical aggregate deadline
            self.assertEqual(result.inverter.variant_key, "fgi_5ku")
            for fn in (async_detect_inverter, async_detect_inverter_candidates):
                end = loop.time() + .04
                remaining = lambda: max(0, end - loop.time())
                if fn is async_detect_inverter:
                    with self.assertRaisesRegex(DriverProbeIncomplete, "budget_exhausted"):
                        await fn(link, driver_hint="modbus_catalog", remaining_seconds=remaining)
                else:
                    partial = await fn(link, driver_hint="modbus_catalog", remaining_seconds=remaining)
                    self.assertTrue(partial.budget_exhausted)
                    self.assertFalse(partial.candidates)

            link.async_send_forward = AsyncMock(side_effect=asyncio.TimeoutError)
            with self.assertRaisesRegex(DriverProbeIncomplete, "detection_incomplete"):
                await async_detect_inverter(link, driver_hint="modbus_catalog")

        asyncio.run(exercise())

    def test_index_and_polling_budget(self):
        with (ROOT / "docs/maintainer/fgi_5ku_registers.csv").open() as f:
            rows = list(csv.DictReader(f))
        index = {int(r["address"], 16): r for r in rows}
        assert len(index) == len(rows) == 1030
        assert Counter(r["fc03_evidence"] for r in rows) == {
            "crc_ok": 691, "exception_4": 13, "not_probed": 326}
        schema = load_register_schema("fgi_5ku/base.json")
        assert sum(not b.key.startswith("control_") for b in schema.blocks) == 4
        assert {(b.start, b.count) for b in schema.blocks} == {
            (a, len(block)) for a, block in SAMPLE["blocks"]}
        assert all(b.function == 3 and b.count <= 16 for b in schema.blocks)
        for spec in (*schema.spec_set("runtime"), *schema.spec_set("controls")):
            for addr in range(spec.register, spec.register + spec.word_count):
                assert spec.key in index[addr]["runtime_keys"].split(";")
                assert index[addr]["fc03_evidence"] == "crc_ok"
        assert index[0x4133]["write_policy"] == "none"  # factory reset
