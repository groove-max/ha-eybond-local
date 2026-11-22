"""A failed socket is an interrupted sweep, not an unsupported inverter."""

from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.runtime.driver_detection import (
    DriverSweepNoMatch, async_detect_inverter, async_detect_inverter_candidates,
)


class DetectionLinkLossTests(unittest.IsolatedAsyncioTestCase):
    async def test_signature_disconnect_stops_before_other_protocols(self):
        for detect in (async_detect_inverter, async_detect_inverter_candidates):
            calls = []
            transport = SimpleNamespace(connected=True)

            class Driver:
                key = "synthetic"
                signature_timeout = 1
                probe_timeout = 1
                probe_targets = (ProbeTarget(1, 255, 1), ProbeTarget(1, 1, 1))

                async def async_probe_signature(self, transport, target):
                    calls.append(target)
                    transport.connected = False
                    return False  # drivers may swallow wire errors

                async def async_probe(self, *_):
                    raise AssertionError("must not probe an already closed socket")

            with patch("custom_components.eybond_local.runtime.driver_detection.iter_drivers", return_value=(Driver(), Driver())):
                with self.assertRaisesRegex(RuntimeError, "^collector_disconnected$") as caught:
                    await detect(transport, driver_hint="auto")
            self.assertNotIsInstance(caught.exception, DriverSweepNoMatch)
            self.assertEqual(len(calls), 1)

    async def test_probe_disconnect_cannot_be_called_silence_or_match(self):
        for detect in (async_detect_inverter, async_detect_inverter_candidates):
            for returned in (None, object()):
                calls = []
                transport = SimpleNamespace(connected=True)

                class Driver:
                    key = "synthetic"
                    signature_timeout = 0
                    probe_timeout = 1
                    probe_targets = (ProbeTarget(1, 255, 1), ProbeTarget(1, 1, 1))

                    async def async_probe(self, transport, target):
                        calls.append(target)
                        transport._inner.connected = False  # tracking proxy delegates reads
                        return returned

                with patch("custom_components.eybond_local.runtime.driver_detection.iter_drivers", return_value=(Driver(), Driver())):
                    with self.assertRaisesRegex(RuntimeError, "^collector_disconnected$") as caught:
                        await detect(transport, driver_hint="auto")
                self.assertNotIsInstance(caught.exception, DriverSweepNoMatch)
                self.assertEqual(len(calls), 1)

    async def test_connected_negative_still_checks_remaining_drivers(self):
        calls = []

        class Driver:
            key = "synthetic"
            signature_timeout = 0
            probe_timeout = 1
            probe_targets = (ProbeTarget(1, 255, 1),)

            async def async_probe(self, *_):
                calls.append(self)
                return None

        with patch("custom_components.eybond_local.runtime.driver_detection.iter_drivers", return_value=(Driver(), Driver())):
            with self.assertRaises(DriverSweepNoMatch):
                await async_detect_inverter(SimpleNamespace(connected=True), driver_hint="auto")
        self.assertEqual(len(calls), 2)

    async def test_candidate_before_disconnect_is_logged_but_not_selected(self):
        transport = SimpleNamespace(connected=True)

        class Match:
            key = protocol_family = "modbus_smg"
            signature_timeout = 0
            probe_timeout = 1
            probe_targets = (ProbeTarget(1, 255, 1),)

            async def async_probe(self, _, target):
                return DetectedInverter(driver_key=self.key, protocol_family=self.protocol_family,
                                        model_name="family", serial_number="private-serial", probe_target=target)

        class Interrupt(Match):
            key = "other"

            async def async_probe(self, _, target):
                transport.connected = False
                return None

        logger = "custom_components.eybond_local.runtime.driver_detection"
        with patch(logger + ".iter_drivers", return_value=(Match(), Interrupt())), self.assertLogs(
            logger, level="DEBUG",
        ) as logs, self.assertRaisesRegex(RuntimeError, "^collector_disconnected$"):
            await async_detect_inverter_candidates(transport, driver_hint="auto")
        messages = [record.getMessage() for record in logs.records if "selection pending" in record.getMessage()]
        self.assertEqual(len(messages), 1)
        self.assertRegex(messages[0], r"^Deep detection found candidate driver=modbus_smg elapsed_ms=\d+; selection pending$")


if __name__ == "__main__":
    unittest.main()
