"""Read-advertised current choices never imply a tested write or a model."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from custom_components.eybond_local.drivers.pi30_controls import (
    async_enrich_charge_current_controls, parse_charge_current_choices,
)
from custom_components.eybond_local.models import WriteCapability
from custom_components.eybond_local.payload.pi30 import Pi30Error


class ChargeCurrentChoiceTests(unittest.IsolatedAsyncioTestCase):
    def test_parser_rejects_invalid_or_ambiguous_lists(self):
        self.assertEqual(parse_charge_current_choices("002 010 020"), (2, 10, 20))
        for payload in ("", "NAK", "ACK", "010 010", "000 010", "10 020", "-01", "010.0",
                        "010\r\nAT+RESET", "010  020", " 010", "010 ", "010\t020",
                        "1000", "０１０", " ".join(f"{i:03}" for i in range(1, 66))):
            with self.subTest(payload=payload), self.assertRaises(Pi30Error):
                parse_charge_current_choices(payload)

    async def test_failed_lists_do_not_add_controls_or_drop_existing(self):
        existing = (WriteCapability(key="existing", register=0, value_kind="bool", note="test"),)
        for replies in ((Pi30Error("nak"), TimeoutError()), ("010 010", "010 020")):
            session = SimpleNamespace(request=AsyncMock(side_effect=replies))
            result = await async_enrich_charge_current_controls(
                session, {"max_charging_current": 50, "max_ac_charging_current": 30},
                existing, timeout=1,
            )
            self.assertEqual(result, existing)
            self.assertEqual(session.request.await_count, 2)

    async def test_existing_controls_are_preserved_without_queries(self):
        existing = tuple(WriteCapability(
            key=key, register=0, value_kind="enum", tested=True, note="existing",
        ) for key in ("max_charging_current", "max_ac_charging_current"))
        session = SimpleNamespace(request=AsyncMock())
        self.assertEqual(await async_enrich_charge_current_controls(
            session, {"max_charging_current": 50, "max_ac_charging_current": 10},
            existing, timeout=1,
        ), existing)
        session.request.assert_not_awaited()

    async def test_each_control_requires_its_own_list(self):
        session = SimpleNamespace(request=AsyncMock(side_effect=["010 050", Pi30Error("nak")]))
        result = await async_enrich_charge_current_controls(
            session, {"max_charging_current": 50, "max_ac_charging_current": 10},
            (), timeout=1,
        )
        self.assertEqual([cap.key for cap in result], ["max_charging_current"])
        self.assertFalse(result[0].tested)
        self.assertEqual(result[0].enum_options, ["10 A", "50 A"])

    async def test_queries_require_current_readback_and_time_budget(self):
        session = SimpleNamespace(request=AsyncMock())
        for values, timeout in (({}, 1), ({"max_charging_current": True}, 1),
                                ({"max_charging_current": 50}, 0)):
            self.assertEqual(await async_enrich_charge_current_controls(
                session, values, (), timeout=timeout,
            ), ())
        session.request.assert_not_awaited()

    async def test_timeout_is_bounded_and_cancellation_propagates(self):
        started = asyncio.Event()
        async def block(*args):
            started.set()
            await asyncio.Event().wait()
        session = SimpleNamespace(request=block)
        values = {"max_charging_current": 50, "max_ac_charging_current": 10}
        result = await asyncio.wait_for(async_enrich_charge_current_controls(
            session, values, (), timeout=0.02,
        ), timeout=0.5)
        self.assertEqual(result, ())
        started.clear()
        task = asyncio.create_task(async_enrich_charge_current_controls(session, values, (), timeout=1))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


if __name__ == "__main__":
    unittest.main()
