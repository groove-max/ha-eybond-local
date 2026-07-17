from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
COMPONENT = ROOT / "custom_components" / "eybond_local"
TRANSLATION_FILES = (
    COMPONENT / "strings.json",
    *sorted((COMPONENT / "translations").glob("*.json")),
)
FLOW_TRANSLATION_FILES = tuple(
    sorted((COMPONENT / "flow_translations").glob("*.json"))
)

# Every recovery failure code the config flow can surface to the user, and the
# exact flow_translations key it must resolve to.
RECOVERY_FAIL_KEYS = (
    "recovery_fail_silent_ambiguous",
    "recovery_fail_identity_mismatch",
    "recovery_fail_silent_probe_failed",
    "recovery_fail_silent_probe_unavailable",
    "recovery_fail_callback_timeout",
    "recovery_fail_inbound_timeout",
    "recovery_fail_restart_unsupported",
    "recovery_fail_ownership_unavailable",
    "recovery_fail_generic",
)


class TranslationShapeTests(unittest.TestCase):
    def test_option_flow_errors_are_declared_at_options_error_level(self) -> None:
        for path in TRANSLATION_FILES:
            with self.subTest(path=path.name):
                payload = json.loads(path.read_text(encoding="utf-8"))
                self.assertIn("proxy_capture_action_failed", payload["options"]["error"])
                invalid_steps = [
                    step_id
                    for step_id, step_payload in payload["options"]["step"].items()
                    if isinstance(step_payload, dict) and "errors" in step_payload
                ]
                self.assertEqual(invalid_steps, [])

    def test_connection_strategy_strings_exist_without_retired_proxy_toggle(self) -> None:
        # The active connection-strategy labels remain in every HA-native
        # bundle, while the unimplemented steady-proxy control is absent.
        for path in TRANSLATION_FILES:
            with self.subTest(path=path.name):
                payload = json.loads(path.read_text(encoding="utf-8"))
                runtime = payload["options"]["step"]["runtime"]
                self.assertIn("connection_strategy", runtime["data"])
                self.assertNotIn("proxy_enabled", runtime["data"])
                self.assertNotIn("proxy_enabled", runtime["data_description"])
                self.assertIn("connection_strategy", runtime["data_description"])

    def test_phase4_connection_strategy_flow_labels_exist(self) -> None:
        # The dynamic select-option labels exist in every flow_translations bundle.
        for path in FLOW_TRANSLATION_FILES:
            with self.subTest(path=path.name):
                dynamic = json.loads(path.read_text(encoding="utf-8"))["common"]["dynamic"]
                self.assertIn("connection_strategy_inbound", dynamic)
                self.assertIn("connection_strategy_callback_on_demand", dynamic)
                self.assertIn("connection_strategy_bridge_note", dynamic)

    def test_listener_rediscovery_action_is_translated(self) -> None:
        for path in TRANSLATION_FILES:
            with self.subTest(path=path.name):
                payload = json.loads(path.read_text(encoding="utf-8"))
                steps = payload["options"]["step"]
                self.assertIn("rediscover_devices", steps["listener"]["menu_options"])
                self.assertIn("rediscover_devices", steps)
                self.assertIn("rediscover_devices_done", steps)
                self.assertIn(
                    "confirm_rediscover_devices",
                    steps["rediscover_devices"]["data"],
                )


class RecoveryFailureLocalizationTests(unittest.TestCase):
    """The runtime-generated recovery-failure explanations are really localized.

    They resolve through the SAME loader ``_tr`` uses (flow_translations), so a
    missing localized key here silently regresses ru/uk to the English
    fallback -- exactly the bug these tests pin.
    """

    def _dynamic(self, path: Path) -> dict:
        return json.loads(path.read_text(encoding="utf-8"))["common"]["dynamic"]

    def test_every_recovery_fail_key_exists_in_each_bundle(self) -> None:
        for path in FLOW_TRANSLATION_FILES:
            dynamic = self._dynamic(path)
            for key in RECOVERY_FAIL_KEYS:
                with self.subTest(path=path.name, key=key):
                    value = dynamic.get(key)
                    self.assertIsInstance(value, str)
                    self.assertTrue(value.strip(), f"{key} is empty in {path.name}")

    def test_flow_translation_files_share_one_key_set(self) -> None:
        # Not only the recovery keys: the whole dynamic key sets must stay
        # synchronized across en/ru/uk so no language silently loses a string.
        key_sets = {
            path.name: frozenset(self._dynamic(path)) for path in FLOW_TRANSLATION_FILES
        }
        reference_name, reference = next(iter(key_sets.items()))
        for name, keys in key_sets.items():
            with self.subTest(bundle=name):
                self.assertEqual(
                    keys,
                    reference,
                    msg=(
                        f"{name} dynamic keys differ from {reference_name}: "
                        f"missing={sorted(reference - keys)} "
                        f"extra={sorted(keys - reference)}"
                    ),
                )

    def _load_bundle(self, language: str) -> dict:
        # The REAL loader every flow uses -- not a hand-built dict.
        from custom_components.eybond_local.config_flow import (
            _load_translation_bundle,
        )

        bundle = _load_translation_bundle(language)
        self.assertIsInstance(bundle, dict)
        return bundle

    def test_loader_resolves_recovery_keys_per_language(self) -> None:
        seen: dict[str, set[str]] = {}
        for language in ("en", "ru", "uk"):
            bundle = self._load_bundle(language)
            dynamic = bundle["common"]["dynamic"]
            for key in RECOVERY_FAIL_KEYS:
                with self.subTest(language=language, key=key):
                    self.assertIn(key, dynamic)
                    seen.setdefault(key, set()).add(dynamic[key])
        # Each key really differs across the three languages (no bundle is
        # quietly reusing another language's text).
        for key, texts in seen.items():
            with self.subTest(key=key):
                self.assertEqual(len(texts), 3, f"{key} is not distinct across languages")

    def _explain(self, language: str, code: str) -> str:
        # Drive the PRODUCTION path end to end: the flow loads its bundle
        # through the real executor/loader for the requested language, then
        # renders the explanation.
        import asyncio

        import test_config_flow as scaffold
        from custom_components.eybond_local.config_flow import EybondLocalConfigFlow

        flow = EybondLocalConfigFlow()
        flow.hass = scaffold._FakeHass(None)
        flow.context = {"language": language}

        async def _run() -> str:
            await flow._async_ensure_translation_bundle()
            return flow._recovery_failure_explanation(code)

        return asyncio.run(_run())

    def test_explanation_matches_language_not_english_fallback(self) -> None:
        for code in (
            "recovery_silent_session_ambiguous",
            "recovery_identity_mismatch",
            "recovery_silent_probe_failed",
            "recovery_silent_probe_unavailable",
            "callback_recovery_timeout",
            "inbound_reconnect_timeout",
            "restart_not_supported",
            "recovery_ownership_unavailable",
        ):
            english = self._explain("en", code)
            russian = self._explain("ru", code)
            ukrainian = self._explain("uk", code)
            with self.subTest(code=code):
                # Real localized text, distinct from the English rendering.
                self.assertNotEqual(russian, english)
                self.assertNotEqual(ukrainian, english)
                self.assertNotEqual(russian, ukrainian)
                # Cyrillic actually present (the fallback is pure ASCII).
                self.assertTrue(any("Ѐ" <= ch <= "ӿ" for ch in russian))
                self.assertTrue(any("Ѐ" <= ch <= "ӿ" for ch in ukrainian))

    def test_unknown_code_uses_localized_generic(self) -> None:
        russian = self._explain("ru", "some_internal_code_xyz")
        english = self._explain("en", "some_internal_code_xyz")
        # The generic branch, localized -- never the raw code, never English.
        self.assertNotIn("some_internal_code_xyz", russian)
        self.assertNotEqual(russian, english)
        self.assertTrue(any("Ѐ" <= ch <= "ӿ" for ch in russian))
        # And it equals the ru generic string exactly.
        ru_bundle = self._load_bundle("ru")
        self.assertEqual(
            russian, ru_bundle["common"]["dynamic"]["recovery_fail_generic"]
        )


if __name__ == "__main__":
    unittest.main()
