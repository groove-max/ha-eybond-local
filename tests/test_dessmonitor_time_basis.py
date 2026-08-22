from __future__ import annotations

import ast
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DessMonitorApiEnvelope,
    DessMonitorCloudError,
    DessMonitorDeviceIdentity,
    DessMonitorSession,
)
import custom_components.eybond_local.dessmonitor_time_basis as basis_module  # noqa: E402
from custom_components.eybond_local.dessmonitor_time_basis import (  # noqa: E402
    DESSMONITOR_TIME_BASIS_AUTHORITY,
    DESSMONITOR_TIME_BASIS_SOURCE_ACTION,
    DessMonitorDeviceTimeBasis,
    fetch_device_time_basis,
    parse_device_time_basis,
)


FULL_PN = "E50000200000000001"
FOREIGN_PN = "E50000200000000002"
SOURCE = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "dessmonitor_time_basis.py"
)


def _identity() -> DessMonitorDeviceIdentity:
    return DessMonitorDeviceIdentity(
        pn=FULL_PN,
        sn="92632511100118",
        devcode=2376,
        devaddr=1,
    )


def _row(*, pn: str = FULL_PN, timezone: object = 28800) -> dict[str, object]:
    return {
        "pn": pn,
        "sn": "92632511100118",
        "devcode": 2376,
        "devaddr": 1,
        "timezone": timezone,
    }


class DessMonitorTimeBasisModelTests(unittest.TestCase):
    def test_roundtrip_and_utc_conversion_use_exact_provider_offset(self) -> None:
        original = DessMonitorDeviceTimeBasis(
            identity=_identity(),
            offset_seconds=28800,
        )

        record = original.to_record()
        parsed = DessMonitorDeviceTimeBasis.from_record(
            json.loads(json.dumps(record))
        )

        self.assertEqual(record["authority"], DESSMONITOR_TIME_BASIS_AUTHORITY)
        self.assertEqual(record["source_action"], DESSMONITOR_TIME_BASIS_SOURCE_ACTION)
        self.assertEqual(
            original.to_utc_timestamp("2026-08-22 10:00:00"),
            "2026-08-22T02:00:00+00:00",
        )
        self.assertEqual(parsed, original)
        self.assertEqual(parsed.to_record(), record)

    def test_direct_constructor_and_parser_reject_forged_offsets(self) -> None:
        for offset in (True, 50401, -43201, 61):
            with self.subTest(offset=offset):
                with self.assertRaises((TypeError, ValueError)):
                    DessMonitorDeviceTimeBasis(  # type: ignore[arg-type]
                        identity=_identity(),
                        offset_seconds=offset,
                    )
        with self.assertRaises(ValueError):
            DessMonitorDeviceTimeBasis(
                identity=_identity(),
                offset_seconds=0,
            ).to_utc_timestamp("2026-08-22T10:00:00+00:00")

        class _DuckAuthority:
            def __eq__(self, _other):
                return True

        record = DessMonitorDeviceTimeBasis(
            identity=_identity(),
            offset_seconds=28800,
        ).to_record()
        record["authority"] = _DuckAuthority()
        self.assertIsNone(DessMonitorDeviceTimeBasis.from_record(record))


class DessMonitorTimeBasisParserTests(unittest.TestCase):
    def test_exact_identity_row_is_required(self) -> None:
        basis = parse_device_time_basis(
            {"device": [_row()]},
            expected_identity=_identity(),
        )

        self.assertEqual(basis.offset_seconds, 28800)

        for rows in (
            [_row(pn=FOREIGN_PN)],
            [{**_row(), "sn": "foreign"}],
            [{**_row(), "devcode": 9999}],
            [_row(timezone=True)],
            [_row(), _row()],
            [],
        ):
            with self.subTest(rows=rows):
                with self.assertRaisesRegex(
                    DessMonitorCloudError,
                    "device_timezone_ambiguous",
                ):
                    parse_device_time_basis(
                        {"device": rows},
                        expected_identity=_identity(),
                    )

    def test_fetch_uses_read_only_query_device_info_for_one_identity(self) -> None:
        captured: list[str] = []

        def fetch(*, action, **_kwargs):
            captured.append(action)
            return DessMonitorApiEnvelope(
                err=0,
                desc="ERR_NONE",
                dat={"device": [_row()]},
            )

        with patch.object(basis_module, "fetch_signed_action", side_effect=fetch):
            basis = fetch_device_time_basis(
                session=DessMonitorSession(token="token", secret="secret"),
                identity=_identity(),
            )

        query = parse_qs(captured[0].removeprefix("&"))
        self.assertEqual(query["action"], [DESSMONITOR_TIME_BASIS_SOURCE_ACTION])
        self.assertEqual(
            query["device"],
            [f"{FULL_PN},2376,1,92632511100118"],
        )
        self.assertNotIn("ctrlDevice", captured[0])
        self.assertEqual(basis.offset_seconds, 28800)


class DessMonitorTimeBasisArchitectureTests(unittest.TestCase):
    def test_time_basis_has_no_local_runtime_or_binding_dependency(self) -> None:
        tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module or "")

        for forbidden in (
            "drivers",
            "runtime",
            "flows",
            "read_learning_binder",
            "overlay_generator",
        ):
            self.assertFalse(any(forbidden in item for item in imports))
        source = SOURCE.read_text(encoding="utf-8")
        self.assertIn("queryDeviceInfo", source)
        self.assertNotIn("datetime.now", source)
        self.assertNotIn("time.time", source)


if __name__ == "__main__":
    unittest.main()
