from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch, sentinel


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import custom_components.eybond_local.drivers.modbus_catalog as catalog_module  # noqa: E402
import custom_components.eybond_local.drivers.must as must_module  # noqa: E402
import custom_components.eybond_local.drivers.smg as smg_module  # noqa: E402
import custom_components.eybond_local.drivers.smartess_local as smartess_module  # noqa: E402
import custom_components.eybond_local.drivers.srne as srne_module  # noqa: E402
from custom_components.eybond_local.drivers.modbus_catalog import (  # noqa: E402
    ModbusCatalogDriver,
)
from custom_components.eybond_local.drivers.must import MustPvPh18Driver  # noqa: E402
from custom_components.eybond_local.drivers.smg import SmgModbusDriver  # noqa: E402
from custom_components.eybond_local.drivers.smartess_local import (  # noqa: E402
    SmartEssLocalDriver,
)
from custom_components.eybond_local.drivers.srne import SrneModbusDriver  # noqa: E402
from custom_components.eybond_local.models import (  # noqa: E402
    DetectedInverter,
    ProbeTarget,
)


PN = "E50000200000000001"


def _inverter(*, schema: str = "schema.json") -> DetectedInverter:
    return DetectedInverter(
        driver_key="test",
        protocol_family="modbus",
        model_name="Test",
        serial_number="serial",
        probe_target=ProbeTarget(
            devcode=2376,
            collector_addr=5,
            device_addr=1,
        ),
        register_schema_name=schema,
    )


class DriverLocalRegisterEvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_smg_and_must_use_driver_capture_ranges_as_holding_reads(self) -> None:
        for module, driver in (
            (smg_module, SmgModbusDriver()),
            (must_module, MustPvPh18Driver()),
        ):
            capture = AsyncMock(return_value=sentinel.snapshot)
            with self.subTest(driver=driver.key), patch.object(
                module,
                "_support_capture_ranges",
                return_value=((300, 2), (400, 1)),
            ), patch.object(
                module,
                "async_capture_modbus_snapshot",
                new=capture,
            ):
                result = await driver.async_capture_local_register_snapshot(
                    sentinel.transport,
                    _inverter(),
                    collector_pn=PN,
                )

            self.assertIs(result, sentinel.snapshot)
            kwargs = capture.await_args.kwargs
            self.assertEqual(kwargs["collector_pn"], PN)
            self.assertEqual(kwargs["driver_key"], driver.key)
            self.assertEqual(
                [(plan.function, plan.start, plan.count) for plan in kwargs["plans"]],
                [(3, 300, 2), (3, 400, 1)],
            )
            self.assertEqual(
                {plan.probe_target for plan in kwargs["plans"]},
                {_inverter().probe_target},
            )

    async def test_srne_uses_schema_blocks_as_holding_reads(self) -> None:
        capture = AsyncMock(return_value=sentinel.snapshot)
        schema = SimpleNamespace(
            blocks=(
                SimpleNamespace(start=10, count=2),
                SimpleNamespace(start=20, count=1),
            )
        )
        with patch.object(
            srne_module,
            "load_register_schema",
            return_value=schema,
        ), patch.object(
            srne_module,
            "async_capture_modbus_snapshot",
            new=capture,
        ):
            result = await SrneModbusDriver().async_capture_local_register_snapshot(
                sentinel.transport,
                _inverter(),
                collector_pn=PN,
            )

        self.assertIs(result, sentinel.snapshot)
        plans = capture.await_args.kwargs["plans"]
        self.assertEqual(
            [(plan.function, plan.start, plan.count) for plan in plans],
            [(3, 10, 2), (3, 20, 1)],
        )

    async def test_catalog_preserves_each_schema_function_code(self) -> None:
        capture = AsyncMock(return_value=sentinel.snapshot)
        schema = SimpleNamespace(
            blocks=(
                SimpleNamespace(start=10, count=2, function=3),
                SimpleNamespace(start=20, count=1, function=4),
            )
        )
        with patch.object(
            catalog_module,
            "load_register_schema",
            return_value=schema,
        ), patch.object(
            catalog_module,
            "async_capture_modbus_snapshot",
            new=capture,
        ):
            result = await ModbusCatalogDriver().async_capture_local_register_snapshot(
                sentinel.transport,
                _inverter(),
                collector_pn=PN,
            )

        self.assertIs(result, sentinel.snapshot)
        plans = capture.await_args.kwargs["plans"]
        self.assertEqual(
            [(plan.function, plan.start, plan.count) for plan in plans],
            [(3, 10, 2), (4, 20, 1)],
        )

    async def test_smartess_uses_raw_data_route_without_support_normalization(self) -> None:
        capture = AsyncMock(return_value=sentinel.snapshot)
        schema = SimpleNamespace(
            blocks=(SimpleNamespace(key="live", start=300, count=2),)
        )
        with patch.object(
            smartess_module,
            "load_register_schema",
            return_value=schema,
        ), patch.object(
            smartess_module,
            "async_capture_modbus_snapshot",
            new=capture,
        ):
            result = await SmartEssLocalDriver().async_capture_local_register_snapshot(
                sentinel.transport,
                _inverter(),
                collector_pn=PN,
            )

        self.assertIs(result, sentinel.snapshot)
        plans = capture.await_args.kwargs["plans"]
        self.assertEqual(len(plans), 1)
        self.assertEqual((plans[0].function, plans[0].start, plans[0].count), (3, 300, 2))
        self.assertEqual(plans[0].collector_addr, 0xFF)
        self.assertEqual(plans[0].device_addr, 1)


if __name__ == "__main__":
    unittest.main()
