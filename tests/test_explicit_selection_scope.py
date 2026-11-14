"""Entry-scoped selection for the experimental catalog map.

A ContextVar is inherited by tasks, so a per-pass selection cannot rely on
"only set a value when there is one": a hub with an empty selection would run
inside whichever selection its parent task happened to have. These tests pin
the unconditional scope, its restoration on every exit path, and the child-task
isolation that the automatic-binding boundary depends on.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.explicit_selection import (  # noqa: E402
    explicit_catalog_entry,
    explicitly_selected_catalog_entry,
)
from custom_components.eybond_local.runtime.hub import detection as detection_module  # noqa: E402


LVYUAN_ENTRY = "lvyuan_ty_sic_6_5kbe_w1"
OTHER_ENTRY = "lvyuan_ty_sic_3_6kbe_w1"


class _Hub:
    """Only the attribute the decorator reads; detection itself is stubbed."""

    def __init__(self, selection: str) -> None:
        self._explicit_catalog_entry_key = selection
        self.observed: list[str] = []

    @detection_module._scoped_to_explicit_catalog_entry
    async def _detect(self) -> str:
        self.observed.append(explicitly_selected_catalog_entry())
        await asyncio.sleep(0)
        self.observed.append(explicitly_selected_catalog_entry())
        return explicitly_selected_catalog_entry()


class EntryScopedSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_selection_does_not_inherit_a_parent_choice(self) -> None:
        """An unselected entry must not see another entry's map."""

        # This is the exact shape the maintainer reproduced: a hub with no
        # selection, run inside a task created under another entry's choice.
        with explicit_catalog_entry(LVYUAN_ENTRY):
            inherited = await asyncio.create_task(_Hub("")._detect())

        self.assertEqual(inherited, "")

    async def test_child_task_of_unselected_entry_sees_empty_scope(self) -> None:
        """A grandchild task inherits the pass's own empty value."""

        hub = _Hub("")

        @detection_module._scoped_to_explicit_catalog_entry
        async def _spawn(self) -> str:
            async def _child() -> str:
                return explicitly_selected_catalog_entry()

            return await asyncio.create_task(_child())

        with explicit_catalog_entry(LVYUAN_ENTRY):
            observed = await asyncio.create_task(_spawn(hub))

        self.assertEqual(observed, "")

    async def test_selected_entry_scope_reaches_child_tasks(self) -> None:
        """The named entry keeps its own map visible to child tasks."""

        hub = _Hub(LVYUAN_ENTRY)

        @detection_module._scoped_to_explicit_catalog_entry
        async def _spawn(self) -> str:
            async def _child() -> str:
                return explicitly_selected_catalog_entry()

            return await asyncio.create_task(_child())

        self.assertEqual(await _spawn(hub), LVYUAN_ENTRY)

    async def test_scope_restores_after_success(self) -> None:
        """A completed pass leaves the caller's own scope untouched."""

        with explicit_catalog_entry(OTHER_ENTRY):
            await _Hub(LVYUAN_ENTRY)._detect()
            self.assertEqual(explicitly_selected_catalog_entry(), OTHER_ENTRY)

    async def test_scope_restores_after_failure(self) -> None:
        """A raising pass still restores the enclosing scope."""

        hub = _Hub(LVYUAN_ENTRY)

        @detection_module._scoped_to_explicit_catalog_entry
        async def _boom(self) -> None:
            raise RuntimeError("detection failed")

        with explicit_catalog_entry(OTHER_ENTRY):
            with self.assertRaises(RuntimeError):
                await _boom(hub)
            self.assertEqual(explicitly_selected_catalog_entry(), OTHER_ENTRY)

    async def test_scope_restores_after_cancellation(self) -> None:
        """A cancelled pass still restores the enclosing scope."""

        hub = _Hub(LVYUAN_ENTRY)

        @detection_module._scoped_to_explicit_catalog_entry
        async def _wait(self) -> None:
            await asyncio.sleep(3600)

        with explicit_catalog_entry(OTHER_ENTRY):
            task = asyncio.create_task(_wait(hub))
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(explicitly_selected_catalog_entry(), OTHER_ENTRY)


if __name__ == "__main__":
    unittest.main()

