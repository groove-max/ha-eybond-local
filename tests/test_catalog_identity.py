from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.catalog_identity import (  # noqa: E402
    ERROR_INVERTER_LINK_DOWN,
    InverterIdentityNoDataError,
    async_probe_catalog_identity,
    probe_indicates_link_down,
)
from custom_components.eybond_local.drivers.smg import SmgModbusDriver  # noqa: E402
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.metadata.device_catalog_loader import (  # noqa: E402
    MATCH_DEVICE,
    MATCH_NO_DATA,
    clear_device_catalog_cache,
)
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402
from custom_components.eybond_local.onboarding.driver_detection import (  # noqa: E402
    async_detect_inverter,
)


def _ascii_words(text: str, *, word_count: int) -> dict[int, int]:
    padded = text.encode("ascii").ljust(word_count * 2, b"\x00")
    return {
        offset: int.from_bytes(padded[offset * 2 : offset * 2 + 2], "big")
        for offset in range(word_count)
    }


class _RegisterSession:
    """Minimal modbus session over a register dict; gaps read as zero."""

    def __init__(self, registers: dict[int, int]) -> None:
        self._registers = registers
        self.reads: list[tuple[int, int]] = []

    async def read_holding(self, register: int, count: int) -> list[int]:
        self.reads.append((register, count))
        return [self._registers.get(register + offset, 0) for offset in range(count)]


class _FailingSession:
    async def read_holding(self, register: int, count: int) -> list[int]:
        raise RuntimeError("transport_down")


def _smg_6200_identity_registers() -> dict[int, int]:
    registers = {171: 7680, 184: 1, 643: 6200, 644: 4}
    for offset, value in _ascii_words("92632500000001", word_count=12).items():
        registers[186 + offset] = value
    return registers


class CatalogIdentityProbeTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        _force_patch = patch(
            "custom_components.eybond_local.metadata.device_catalog_loader."
            "FORCE_UNSUPPORTED_MODELS",
            False,
        )
        _force_patch.start()
        self.addCleanup(_force_patch.stop)
        clear_device_catalog_cache()
        self.addCleanup(clear_device_catalog_cache)

    async def test_probe_matches_corpus_smg_6200(self) -> None:
        session = _RegisterSession(_smg_6200_identity_registers())
        probe = await async_probe_catalog_identity(session)
        self.assertIsNotNone(probe)
        self.assertEqual(probe.match.kind, MATCH_DEVICE)
        self.assertEqual(probe.match.entry.entry_key, "smg_6200")
        self.assertEqual(probe.layout_code, 1)
        self.assertEqual(probe.model_code, 7680)
        self.assertEqual(probe.rated_power, 6200)
        self.assertEqual(probe.serial_ascii, "92632500000001")
        self.assertFalse(probe_indicates_link_down(probe))
        # The probe must stay within the declared identity window.
        self.assertEqual(session.reads, [(171, 14), (186, 12), (643, 2)])

    async def test_zero_identity_region_is_link_down(self) -> None:
        probe = await async_probe_catalog_identity(_RegisterSession({}))
        self.assertIsNotNone(probe)
        self.assertEqual(probe.match.kind, MATCH_NO_DATA)
        self.assertTrue(probe_indicates_link_down(probe))

    async def test_unreadable_transport_yields_no_opinion(self) -> None:
        probe = await async_probe_catalog_identity(_FailingSession())
        self.assertIsNone(probe)
        self.assertFalse(probe_indicates_link_down(probe))

    async def test_unreadable_fingerprint_block_yields_no_opinion(self) -> None:
        # Only the serial block is readable (a foreign layout rejecting the
        # 171-window read): the probe must abstain, not report link-down.
        class _SerialOnlySession:
            async def read_holding(self, register: int, count: int) -> list[int]:
                if register != 186:
                    raise RuntimeError("illegal_address")
                return [0x3932] * count

        probe = await async_probe_catalog_identity(_SerialOnlySession())
        self.assertIsNone(probe)
        self.assertFalse(probe_indicates_link_down(probe))

    async def test_as_details_serializes_match(self) -> None:
        probe = await async_probe_catalog_identity(
            _RegisterSession(_smg_6200_identity_registers())
        )
        details = probe.as_details()
        self.assertEqual(details["kind"], MATCH_DEVICE)
        self.assertEqual(details["entry_key"], "smg_6200")
        self.assertEqual(details["tier"], "full")
        self.assertIn("rated_power", details["confidence_signals"])


class SmgProbeCatalogAuthorityTest(unittest.IsolatedAsyncioTestCase):
    """Driver-level behavior: catalog decides, details attach, zeros stop the probe."""
    def setUp(self) -> None:
        _force_patch = patch(
            "custom_components.eybond_local.metadata.device_catalog_loader."
            "FORCE_UNSUPPORTED_MODELS",
            False,
        )
        _force_patch.start()
        self.addCleanup(_force_patch.stop)


    def _smg_family_registers(self, *, rated_power: int = 6200) -> dict[int, int]:
        registers: dict[int, int] = {
            register: 0
            for start, stop in (
                (100, 110),
                (171, 185),
                (186, 198),
                (201, 235),
                (300, 344),
                (351, 352),
                (406, 407),
                (420, 421),
                (626, 645),
            )
            for register in range(start, stop)
        }
        for offset, value in _ascii_words("SMG II 6200", word_count=12).items():
            registers[172 + offset] = value
        for offset, value in _ascii_words("SMG11K240001", word_count=12).items():
            registers[186 + offset] = value
        registers.update(
            {
                171: 0x1E00,
                184: 1,
                201: 3,
                202: 2300,
                203: 5000,
                210: 2295,
                212: 5000,
                215: 512,
                301: 1,
                303: 3,
                313: 1,
                320: 2300,
                321: 5000,
                322: 2,
                323: 620,
                324: 560,
                325: 540,
                326: 520,
                327: 480,
                329: 470,
                331: 1,
                332: 600,
                341: 25,
                342: 45,
                343: 15,
                643: rated_power,
                644: 16,
            }
        )
        return registers

    async def test_probe_attaches_catalog_match_details(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
            registers=self._smg_family_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        shadow = inverter.details.get("device_catalog")
        self.assertIsInstance(shadow, dict)
        self.assertEqual(shadow["kind"], MATCH_DEVICE)
        self.assertEqual(shadow["entry_key"], "smg_6200")
        # The binding now comes FROM the catalog entry itself.
        self.assertEqual(inverter.variant_key, "default")
        self.assertEqual(inverter.register_schema_name, "modbus_smg/models/smg_6200.json")
        self.assertEqual(inverter.profile_name, "smg_modbus.json")

    async def test_probe_raises_link_down_on_zero_identity(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        registers = {
            register: 0
            for start, stop in ((100, 110), (171, 198), (201, 235), (300, 344), (626, 645))
            for register in range(start, stop)
        }
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )
        with self.assertRaises(InverterIdentityNoDataError):
            await driver.async_probe(transport, target)


class DetectionLinkDownTest(unittest.IsolatedAsyncioTestCase):
    async def test_detection_surfaces_link_down_instead_of_no_driver(self) -> None:
        class _LinkDownDriver:
            key = "modbus_smg"
            probe_targets = (ProbeTarget(devcode=1, collector_addr=0xFF, device_addr=1),)
            probe_timeout = 0
            signature_timeout = 0

            async def async_probe(self, transport, target):
                raise InverterIdentityNoDataError()

            async def async_probe_signature(self, transport, target):
                return False

        with patch(
            "custom_components.eybond_local.onboarding.driver_detection.iter_drivers",
            return_value=(_LinkDownDriver(),),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                await async_detect_inverter(object(), driver_hint="auto")
        self.assertEqual(str(ctx.exception), ERROR_INVERTER_LINK_DOWN)

    async def test_link_down_does_not_mask_a_real_match_by_another_driver(self) -> None:
        sentinel = object()

        class _LinkDownDriver:
            key = "modbus_smg"
            probe_targets = (ProbeTarget(devcode=1, collector_addr=0xFF, device_addr=1),)
            probe_timeout = 0
            signature_timeout = 0

            async def async_probe(self, transport, target):
                raise InverterIdentityNoDataError()

            async def async_probe_signature(self, transport, target):
                return False

        class _MatchingDriver:
            key = "pi30"
            probe_targets = (ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=1),)
            probe_timeout = 0
            signature_timeout = 0

            async def async_probe(self, transport, target):
                from custom_components.eybond_local.models import DetectedInverter

                return DetectedInverter(
                    driver_key="pi30",
                    protocol_family="pi30",
                    model_name="PI30 Unit",
                    serial_number="X1",
                    probe_target=target,
                )

            async def async_probe_signature(self, transport, target):
                return False

        with patch(
            "custom_components.eybond_local.onboarding.driver_detection.iter_drivers",
            return_value=(_LinkDownDriver(), _MatchingDriver()),
        ):
            context = await async_detect_inverter(object(), driver_hint="auto")
        self.assertEqual(context.driver.key, "pi30")


if __name__ == "__main__":
    unittest.main()
