"""Startup identity for an explicitly selected experimental catalog entry.

The LVYUAN map carries confirmed telemetry but no non-settable per-model
identity word, so it never binds from an automatic sweep. That leaves startup
with a problem: the persisted-model path refuses a provisional descriptor on
purpose, so a restart would silently drop a unit the user configured. These
tests pin the two halves of the answer -- the explicit choice restores the
unit, and its absence leaves it unbound.
"""

from __future__ import annotations

import sys
from pathlib import Path
import types
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.runtime.coordinator import startup as startup_module  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget, RuntimeSnapshot  # noqa: E402


ENTRY_KEY = "lvyuan_ty_sic_6_5kbe_w1"


def _surface() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        driver_key="modbus_catalog",
        variant_key=ENTRY_KEY,
        profile_name="",
        register_schema_name=f"{ENTRY_KEY}/base.json",
        read_only=True,
    )


def _coordinator(options: dict, data: dict) -> object:
    coordinator = object.__new__(startup_module.CoordinatorStartupIdentityMixin)
    coordinator.config_entry = types.SimpleNamespace(
        entry_id="entry-1",
        data={
            "connection_type": "eybond",
            "control_mode": "read_only",
            "driver_hint": "auto",
            **data,
        },
        options=options,
    )
    coordinator.data = RuntimeSnapshot(values={})
    # The mixin reads the persisted metadata snapshot; these tests exercise the
    # explicit-selection branch, so the snapshot stays empty and invalid.
    coordinator.effective_metadata_snapshot = types.SimpleNamespace(
        is_valid=False,
        profile_name="",
        register_schema_name="",
        variant_key="",
        surface_key="",
    )
    coordinator.detection_confidence = data.get("detection_confidence", "")
    coordinator._connection_spec = types.SimpleNamespace(
        collector_ip="192.0.2.11",
        collector_pn="E5000SYNTHETIC5507",
        collector_cloud_family="smartess_at",
        server_ip="192.0.2.56",
        tcp_port=8899,
        effective_advertised_server_ip="192.0.2.56",
        effective_advertised_tcp_port=8899,
    )
    return coordinator


class ExplicitlySelectedStartupTests(unittest.TestCase):
    def test_explicit_choice_restores_the_experimental_map(self) -> None:
        """The user's choice must survive a restart.

        Everything except the stored selection refuses this map, so without
        the explicit branch a restart would leave the inverter unbound -- the
        user's own choice would be what breaks it.
        """

        driver = types.SimpleNamespace(
            key="modbus_catalog",
            probe_targets=(ProbeTarget(devcode=23711, collector_addr=1, device_addr=1),),
        )
        profile = types.SimpleNamespace(
            driver_key="modbus_catalog",
            protocol_family="modbus_catalog",
            groups=(),
            capabilities=(),
            presets=(),
        )
        register_schema = types.SimpleNamespace(driver_key="modbus_catalog")
        coordinator = _coordinator({"device_catalog_entry_key": ENTRY_KEY}, {})

        with (
            patch.object(
                startup_module,
                "resolve_explicitly_selected_model_surface",
                return_value=(types.SimpleNamespace(), _surface()),
            ) as resolve_selected,
            patch.object(
                startup_module,
                "resolve_unique_persisted_model_surface",
                return_value=None,
            ) as resolve_persisted,
            patch.object(startup_module, "get_driver", return_value=driver),
            patch.object(startup_module, "load_driver_profile", return_value=profile),
            patch.object(
                startup_module,
                "load_register_schema",
                return_value=register_schema,
            ),
        ):
            inverter = coordinator._prime_startup_inverter_from_persisted_metadata()

        resolve_selected.assert_called_once_with(ENTRY_KEY)
        resolve_persisted.assert_not_called()
        self.assertIsNotNone(inverter)
        self.assertEqual(inverter.driver_key, "modbus_catalog")
        self.assertEqual(inverter.variant_key, ENTRY_KEY)
        self.assertEqual(
            inverter.details["identity_source"],
            "explicitly_selected_catalog_entry",
        )

    def test_no_choice_keeps_the_experimental_map_unbound(self) -> None:
        """Without a choice the same unit must stay unbound.

        Same entry, healthy detected model, high confidence -- and still no
        binding, because the only route into this map is the user naming it.
        """

        coordinator = _coordinator(
            {},
            {
                "detected_model": "LVYUAN TY-SIC-6.5KBE-W1",
                "detection_confidence": "high",
            },
        )

        with (
            patch.object(
                startup_module,
                "resolve_explicitly_selected_model_surface",
                return_value=None,
            ) as resolve_selected,
            patch.object(
                startup_module,
                "resolve_unique_persisted_model_surface",
                return_value=None,
            ),
        ):
            inverter = coordinator._prime_startup_inverter_from_persisted_metadata()

        self.assertIsNone(inverter)
        resolve_selected.assert_not_called()

    def test_unknown_explicit_entry_fails_closed(self) -> None:
        """A stale selection must not silently bind something else."""

        coordinator = _coordinator({"device_catalog_entry_key": "removed_entry"}, {})

        with (
            patch.object(
                startup_module,
                "resolve_explicitly_selected_model_surface",
                return_value=None,
            ),
            patch.object(
                startup_module,
                "resolve_unique_persisted_model_surface",
                return_value=None,
            ),
        ):
            inverter = coordinator._prime_startup_inverter_from_persisted_metadata()

        self.assertIsNone(inverter)


if __name__ == "__main__":
    unittest.main()
