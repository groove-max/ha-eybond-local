"""Issue #62: 0925 data/config routes belong to one verified inverter."""
from dataclasses import replace
from pathlib import Path
import copy
import unittest

from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.payload.modbus import build_read_request
from custom_components.eybond_local.support.shadow_learning.backend import (
    InProcessShadowLearningHandler, build_shadow_learning_seed,
)
from custom_components.eybond_local.support.shadow_learning.routes import captured_learning_targets


def inverter():
    return DetectedInverter(
        driver_key="smartess_local", protocol_family="smartess_local", model_name="Synthetic 0925",
        serial_number="", probe_target=ProbeTarget(1, 5, 5), variant_key="smartess_0925",
        profile_name="smartess_local/models/0925.json", register_schema_name="smartess_local/models/0925.json",
    )


def capture():
    # Sanitized route/range shape from #62; no user's identifiers or serials.
    return {
        "capture_kind": "smartess_0925_modbus_register_dump", "driver_key": "smartess_local",
        "probe_target": {"devcode": 1, "collector_addr": 5, "device_addr": 5},
        "captured_ranges": [
            {"start": 4501, "count": 14, "collector_addr": 255, "words": [0] * 14},
            {"start": 5004, "count": 1, "collector_addr": 5, "words": [1]},
        ],
        "fixture_ranges": [{"start": 4501, "values": [0] * 14}, {"start": 5004, "values": [1]}],
    }


def seed(raw=None):
    device = inverter()
    result, blockers = build_shadow_learning_seed(
        session_id="synthetic", entry_id="synthetic", collector_pn="E5000020000000",
        collector_cloud_profile_key="", collector_cloud_profile_label="",
        collector_cloud_profile_source="", collector_cloud_profile_confidence="",
        collector_callback_endpoint="127.0.0.1,8899,TCP", collector_cloud_family="legacy_binary",
        effective_metadata_snapshot={"driver_key": "smartess_local", "profile_name": device.profile_name,
                                     "register_schema_name": device.register_schema_name},
        raw_capture=capture() if raw is None else raw, identified_inverter=device,
        inverter_target=(1, 5), inverter_device_addr=5, inspect_cloud_protocol=True,
    )
    assert not blockers, blockers
    return result


def cloud_read(*, devcode=1, collector_addr=255, unit=5, start=4501, count=45):
    return build_collector_request(1, build_read_request(unit, start, count),
                                   devcode=devcode, collector_addr=collector_addr, fcode=4)


class SplitLearningRouteTests(unittest.TestCase):
    def test_real_split_route_shape_accepts_cloud_read_on_data_route(self):
        candidate = seed()
        self.assertEqual(candidate.captured_inverter_targets, ((1, 5), (1, 255)))
        backend = InProcessShadowLearningHandler(seed=candidate, output_path=Path("unused"))
        self.assertEqual(backend.confirm_cloud_protocol(cloud_read()), "ready")
        # Configuration reads still use their own route; do not rewrite the
        # primary route to 255 or silently switch the bound profile.
        self.assertEqual(backend.confirm_cloud_protocol(cloud_read(collector_addr=5, start=5004, count=1)), "ready")
        self.assertEqual(backend._seed.inverter_target, (1, 5))

    def test_foreign_route_unit_and_devcode_stay_rejected(self):
        for kw in ({"collector_addr": 6}, {"unit": 6}, {"devcode": 2}):
            with self.subTest(kw=kw):
                backend = InProcessShadowLearningHandler(seed=seed(), output_path=Path("unused"))
                self.assertEqual(backend.confirm_cloud_protocol(cloud_read(**kw)), "cloud_protocol_route_mismatch")

    def test_no_capture_no_route_alias_even_for_0925(self):
        backend = InProcessShadowLearningHandler(seed=replace(seed(), captured_inverter_targets=()),
                                                 output_path=Path("unused"))
        self.assertEqual(backend.confirm_cloud_protocol(cloud_read()), "cloud_protocol_route_mismatch")

    def test_capture_must_match_driver_target_and_successful_catalog_plan(self):
        for mutate in (
            lambda c: c.update(driver_key="pi30"),
            lambda c: c["probe_target"].update(device_addr=6),
            lambda c: c["probe_target"].update(devcode=2),
            lambda c: c["captured_ranges"][0].update(collector_addr=6),
            lambda c: c["captured_ranges"][0].update(start=100),
            lambda c: c["captured_ranges"][0].update(count=13),
            lambda c: c["captured_ranges"][0].update(words=[0] * 13),
            lambda c: c["captured_ranges"][0].update(words=[False] * 14),
            lambda c: c["captured_ranges"][0].update(function=4),
            lambda c: c["captured_ranges"][0].update(error="timeout"),
        ):
            raw = copy.deepcopy(capture())
            mutate(raw)
            self.assertNotIn((1, 255), captured_learning_targets(inverter(), raw))
