from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.branch_registry import (
    get_connection_branch,
    get_connection_branch_for_spec,
    supported_connection_types,
)
from custom_components.eybond_local.connection.models import ConnectionSpec, EybondConnectionSpec


class ConnectionBranchRegistryTests(unittest.TestCase):
    def test_supported_connection_types_lists_registered_branches(self) -> None:
        self.assertEqual(supported_connection_types(), ("eybond",))

    def test_get_connection_branch_returns_eybond_branch(self) -> None:
        branch = get_connection_branch("eybond")

        self.assertEqual(branch.connection_type, "eybond")
        self.assertIs(branch.spec_type, EybondConnectionSpec)

    def test_get_connection_branch_for_spec_validates_branch_spec_type(self) -> None:
        with self.assertRaisesRegex(ValueError, "connection_spec_branch_mismatch:eybond:ConnectionSpec"):
            get_connection_branch_for_spec(ConnectionSpec(type="eybond"))


if __name__ == "__main__":
    unittest.main()
