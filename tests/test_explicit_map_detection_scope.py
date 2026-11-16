"""The explicit map choice covers the whole detection pass, not one sweep."""

from __future__ import annotations

import sys
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.explicit_selection import (  # noqa: E402
    explicitly_selected_catalog_entry,
)
from custom_components.eybond_local.runtime.hub.detection import (  # noqa: E402
    _scoped_to_explicit_catalog_entry,
)


ENTRY_KEY = "lvyuan_ty_sic_6_5kbe_w1"


class _Hub:
    """Records the selection visible inside the decorated pass."""

    def __init__(self, selection: str) -> None:
        self._explicit_catalog_entry_key = selection
        self.seen: list[str] = []

    @_scoped_to_explicit_catalog_entry
    async def detect(self) -> str:
        self.seen.append(explicitly_selected_catalog_entry())
        return "detected"


class _ExplodingHub(_Hub):
    @_scoped_to_explicit_catalog_entry
    async def detect(self) -> str:
        raise RuntimeError("boom")


class DetectionScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_choice_is_visible_to_the_whole_pass(self) -> None:
        hub = _Hub(ENTRY_KEY)
        self.assertEqual(await hub.detect(), "detected")
        self.assertEqual(hub.seen, [ENTRY_KEY])

    async def test_choice_is_cleared_after_the_pass(self) -> None:
        hub = _Hub(ENTRY_KEY)
        await hub.detect()
        self.assertEqual(explicitly_selected_catalog_entry(), "")

    async def test_no_choice_leaves_detection_unchanged(self) -> None:
        hub = _Hub("")
        await hub.detect()
        self.assertEqual(hub.seen, [""])

    async def test_choice_does_not_leak_into_the_next_pass(self) -> None:
        await _Hub(ENTRY_KEY).detect()
        plain = _Hub("")
        await plain.detect()
        self.assertEqual(plain.seen, [""])

    async def test_choice_is_cleared_when_the_pass_raises(self) -> None:
        hub = _ExplodingHub(ENTRY_KEY)
        with self.assertRaises(RuntimeError):
            await hub.detect()
        self.assertEqual(explicitly_selected_catalog_entry(), "")

    async def test_the_real_detection_method_is_decorated(self) -> None:
        """Guard the wiring: an undecorated method silently drops the choice."""

        from custom_components.eybond_local.runtime.hub import detection

        method = detection.HubDetectionMixin._async_detect_driver
        self.assertEqual(method.__name__, "_async_detect_driver")
        self.assertTrue(getattr(method, "__eybond_explicit_scope__", False))


if __name__ == "__main__":
    unittest.main()
