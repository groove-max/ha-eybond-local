"""Options-flow admission for the short-ASCII auxiliary 0200 poll."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

import test_config_flow
from custom_components.eybond_local.const import (
    CONF_ADMIT_SHORT_ASCII_MPPT,
    DEFAULT_ADMIT_SHORT_ASCII_MPPT,
)
from custom_components.eybond_local.flows.options import short_ascii_mppt as mppt_step

COMPONENT = ROOT / "custom_components" / "eybond_local"


class _DescribedKey(str):
    """Stub marker that keeps the suggested value the form offered."""

    description: dict


def _optional(key, default=None, description=None, **_kwargs):
    marker = _DescribedKey(key)
    marker.description = dict(description or {})
    marker.default = default
    return marker


def _suggested(form) -> bool:
    schema = form["data_schema"].schema
    key = next(iter(schema))
    if str(key) != CONF_ADMIT_SHORT_ASCII_MPPT:
        raise AssertionError(f"unexpected field {key!r}")
    return key.description["suggested_value"]


class ShortAsciiMpptOptionsFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._optional = mppt_step.vol.Optional
        mppt_step.vol.Optional = _optional
        self._cases = test_config_flow.ConfigFlowTests()
        self._cases.setUp()

    def tearDown(self) -> None:
        mppt_step.vol.Optional = self._optional
        self._cases.doCleanups()
        super().tearDown()

    def _flow(self, options: dict):
        flow = self._cases._make_options_flow()
        flow._config_entry.options = dict(options)
        return flow

    async def test_menu_lists_short_ascii_mppt_after_runtime(self) -> None:
        menu = await self._flow({}).async_step_init()
        options = menu["menu_options"]
        self.assertEqual(options[options.index("runtime") + 1], "short_ascii_mppt")

    async def test_suggested_value_round_trips_without_dropping_options(self) -> None:
        unrelated = {"poll_interval": 30}
        flow = self._flow(unrelated)
        form = await flow.async_step_short_ascii_mppt()
        self.assertEqual(form["type"], "form")
        self.assertEqual(form["step_id"], "short_ascii_mppt")
        suggested = _suggested(form)
        self.assertIs(suggested, DEFAULT_ADMIT_SHORT_ASCII_MPPT)
        self.assertIs(suggested, False)

        saved = await flow.async_step_short_ascii_mppt({
            CONF_ADMIT_SHORT_ASCII_MPPT: suggested,
        })
        self.assertEqual(saved["type"], "create_entry")
        self.assertEqual(saved["data"]["poll_interval"], 30)
        self.assertIn(CONF_ADMIT_SHORT_ASCII_MPPT, saved["data"])
        self.assertIs(saved["data"][CONF_ADMIT_SHORT_ASCII_MPPT], False)

        enabled = self._flow({**unrelated, CONF_ADMIT_SHORT_ASCII_MPPT: True})
        enabled_form = await enabled.async_step_short_ascii_mppt()
        self.assertIs(_suggested(enabled_form), True)
        turned_on = await enabled.async_step_short_ascii_mppt({
            CONF_ADMIT_SHORT_ASCII_MPPT: True,
        })
        self.assertIs(turned_on["data"][CONF_ADMIT_SHORT_ASCII_MPPT], True)
        self.assertEqual(turned_on["data"]["poll_interval"], 30)

        turned_off = await enabled.async_step_short_ascii_mppt({
            CONF_ADMIT_SHORT_ASCII_MPPT: False,
        })
        self.assertIn(CONF_ADMIT_SHORT_ASCII_MPPT, turned_off["data"])
        self.assertIs(turned_off["data"][CONF_ADMIT_SHORT_ASCII_MPPT], False)
        self.assertEqual(turned_off["data"]["poll_interval"], 30)

    def test_option_strings_exist_in_every_bundle(self) -> None:
        files = (
            COMPONENT / "strings.json",
            *sorted((COMPONENT / "translations").glob("*.json")),
        )
        for path in files:
            with self.subTest(path=path.name):
                options = json.loads(path.read_text(encoding="utf-8"))["options"]["step"]
                self.assertIn("short_ascii_mppt", options["init"]["menu_options"])
                step = options["short_ascii_mppt"]
                self.assertIn("0200", step["description"])
                self.assertEqual(set(step["data"]), {CONF_ADMIT_SHORT_ASCII_MPPT})
                self.assertEqual(set(step["data_description"]), {CONF_ADMIT_SHORT_ASCII_MPPT})


if __name__ == "__main__":
    unittest.main()
