from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import custom_components.eybond_local.dessmonitor_cloud as dessmonitor_module  # noqa: E402
from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DEFAULT_BASE_URL,
    DEFAULT_MAX_METADATA_FIELDS,
    DEFAULT_MAX_RESPONSE_BYTES,
    DessMonitorApiEnvelope,
    DessMonitorControlField,
    DessMonitorDeviceIdentity,
    DessMonitorEvidenceBundle,
    DessMonitorSession,
    DessMonitorTelemetryField,
    build_login_url,
    build_signed_action_url,
    fetch_read_only_evidence,
)


FULL_PN = "E50000200000000001"


def _ok(dat) -> DessMonitorApiEnvelope:
    return DessMonitorApiEnvelope(err=0, desc="ERR_NONE", dat=dat)


class DessMonitorModelTests(unittest.TestCase):
    def test_session_repr_never_discloses_signed_material(self) -> None:
        session = DessMonitorSession(
            token="token-secret-value",
            secret="request-secret-value",
            uid="private-user-id",
            usr="private-account-name",
        )

        rendered = repr(session)

        self.assertNotIn("token-secret-value", rendered)
        self.assertNotIn("request-secret-value", rendered)
        self.assertNotIn("private-user-id", rendered)
        self.assertNotIn("private-account-name", rendered)

        envelope = DessMonitorApiEnvelope(
            err=0,
            desc="private provider detail",
            dat={"account": "private-account-name"},
        )
        self.assertNotIn("private provider detail", repr(envelope))
        self.assertNotIn("private-account-name", repr(envelope))

    def test_direct_constructors_are_strict_and_json_safe(self) -> None:
        identity = DessMonitorDeviceIdentity(
            pn=FULL_PN,
            sn="92632511100118",
            devcode=2376,
            devaddr=1,
        )
        field = DessMonitorTelemetryField(
            field_id="pv_voltage",
            title="PV Voltage",
            value="123.4",
            unit="V",
            section="pv_",
            source_action="querySPDeviceLastData",
        )
        control = DessMonitorControlField(
            field_id="bat_eybond_ctrl_75",
            title="Charger Source Priority",
            choices=(("0", "Utility first"), ("3", "Only PV charging is allowed")),
            current_value="Only PV charging is allowed",
        )
        raw = b"device packet"
        bundle = DessMonitorEvidenceBundle(
            identity=identity,
            telemetry_fields=(field,),
            chart_fields=(),
            key_parameters=(),
            control_fields=(control,),
            raw_packet_sha256=hashlib.sha256(raw).hexdigest(),
            raw_packet_length=len(raw),
        )

        serialized = json.dumps(bundle.to_record())

        self.assertIn("Charger Source Priority", serialized)
        self.assertNotIn("token", serialized.lower())
        with self.assertRaises(TypeError):
            DessMonitorDeviceIdentity(  # type: ignore[arg-type]
                pn=FULL_PN, sn="sn", devcode=True, devaddr=1
            )
        with self.assertRaises(ValueError):
            DessMonitorTelemetryField(
                field_id="id",
                title=" padded ",
                value="",
                unit="",
                section="",
                source_action="querySPDeviceLastData",
            )
        with self.assertRaises(ValueError):
            DessMonitorControlField(
                field_id="id",
                title="Title",
                choices=(("1", "One"), ("1", "Duplicate")),
            )
        with self.assertRaisesRegex(ValueError, "metadata_limit"):
            DessMonitorEvidenceBundle(
                identity=identity,
                telemetry_fields=(field,) * (DEFAULT_MAX_METADATA_FIELDS + 1),
                chart_fields=(),
                key_parameters=(),
                control_fields=(),
            )


class DessMonitorSigningTests(unittest.TestCase):
    def test_http_response_body_is_bounded_before_json_decode(self) -> None:
        class _OversizedResponse:
            read_limit = 0

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, limit):
                self.read_limit = limit
                return b"x" * (limit + 1)

        response = _OversizedResponse()
        with patch.object(dessmonitor_module, "urlopen", return_value=response):
            with self.assertRaisesRegex(Exception, "response_too_large"):
                dessmonitor_module.login_with_password(
                    username="account",
                    password="password",
                )

        self.assertEqual(response.read_limit, DEFAULT_MAX_RESPONSE_BYTES + 1)

    def test_login_targets_dessmonitor_authority_and_signs_exact_action(self) -> None:
        with patch(
            "custom_components.eybond_local.dessmonitor_cloud._salt_millis",
            return_value="1700000000000",
        ):
            url = build_login_url(username="user@example.com", password="secret")

        action = (
            "&action=authSource&usr=user%40example.com"
            "&company-key=bnrl_frRFjEz8Mkn&source=1"
        )
        expected = hashlib.sha1(
            (
                "1700000000000"
                + hashlib.sha1(b"secret").hexdigest()
                + action
            ).encode()
        ).hexdigest()
        self.assertTrue(url.startswith(DEFAULT_BASE_URL))
        self.assertIn(f"sign={expected}", url)
        self.assertNotIn("android.shinemonitor.com", url)
        self.assertNotIn("secret", url)

    def test_signed_read_url_is_exact_and_rejects_duck_session(self) -> None:
        session = DessMonitorSession(token="token-1", secret="secret-1")
        action = "&action=querySPKeyParameters&devcode=2376"
        with patch(
            "custom_components.eybond_local.dessmonitor_cloud._salt_millis",
            return_value="1700000000001",
        ):
            url = build_signed_action_url(action=action, session=session)

        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["action"], ["querySPKeyParameters"])
        self.assertEqual(query["devcode"], ["2376"])
        self.assertEqual(query["source"], ["1"])
        with self.assertRaises(TypeError):
            build_signed_action_url(action=action, session=object())  # type: ignore[arg-type]


class DessMonitorBundleTests(unittest.TestCase):
    def test_metadata_arrays_and_scalar_values_are_bounded(self) -> None:
        identity = {
            "device": [
                {
                    "pn": FULL_PN,
                    "sn": "92632511100118",
                    "devcode": 2376,
                    "devaddr": 1,
                }
            ]
        }
        oversized_rows = [
            {
                "id": f"field_{index}",
                "par": "X" * 700,
                "val": index,
                "unit": "V",
            }
            for index in range(DEFAULT_MAX_METADATA_FIELDS + 100)
        ]
        responses = {
            "webQueryDeviceEs": _ok(identity),
            "querySPDeviceLastData": _ok({"pars": {"main": oversized_rows}}),
            "queryDeviceChartField": _ok([]),
            "querySPKeyParameters": _ok([]),
            "queryDeviceCtrlField": _ok({"field": []}),
            "queryDeviceLastRawData": _ok(None),
        }

        def fetch(*, action, **_kwargs):
            action_name = parse_qs(action.removeprefix("&"))["action"][0]
            return responses[action_name]

        with (
            patch.object(
                dessmonitor_module,
                "login_with_password",
                return_value=(
                    _ok({}),
                    DessMonitorSession(token="token-1", secret="secret-1"),
                ),
            ),
            patch.object(
                dessmonitor_module,
                "fetch_signed_action",
                side_effect=fetch,
            ),
        ):
            bundle = fetch_read_only_evidence(
                username="account",
                password="password",
                collector_pn=FULL_PN,
            )

        self.assertEqual(bundle.metadata_field_count, DEFAULT_MAX_METADATA_FIELDS)
        self.assertEqual(len(bundle.telemetry_fields), DEFAULT_MAX_METADATA_FIELDS)
        self.assertLessEqual(len(bundle.telemetry_fields[0].title), 512)

    def test_read_only_bundle_is_identity_bound_and_uses_no_write_action(self) -> None:
        identity_list = _ok(
            {
                "device": [
                    {
                        "pn": FULL_PN,
                        "sn": "92632511100118",
                        "devcode": 2376,
                        "devaddr": 1,
                    }
                ]
            }
        )
        responses = {
            "webQueryDeviceEs": identity_list,
            "querySPDeviceLastData": _ok(
                {
                    "pars": {
                        "pv_": [
                            {
                                "id": "sy_eybond_read_2",
                                "par": "PV Voltage",
                                "val": "123.4",
                                "unit": "V",
                            }
                        ]
                    }
                }
            ),
            "queryDeviceChartField": _ok(
                [{"e0": "pv_voltage", "e1": "PV Voltage", "e3": "V"}]
            ),
            "querySPKeyParameters": _ok(
                [{"e0": "battery_voltage", "e1": "Battery Voltage", "e3": "V"}]
            ),
            "queryDeviceCtrlField": _ok(
                {
                    "field": [
                        {
                            "id": "bat_eybond_ctrl_75",
                            "name": "Charger Source Priority",
                            "item": [
                                {"key": "0", "val": "Utility first"},
                                {"key": "3", "val": "Only PV charging is allowed"},
                            ],
                        }
                    ]
                }
            ),
            "queryDeviceCtrlValue": _ok({"val": "Only PV charging is allowed"}),
            "queryDeviceLastRawData": _ok({"raw": "01020304"}),
        }
        actions: list[str] = []

        def fetch(*, action, **_kwargs):
            action_name = parse_qs(action.removeprefix("&"))["action"][0]
            actions.append(action_name)
            return responses[action_name]

        with (
            patch(
                "custom_components.eybond_local.dessmonitor_cloud.login_with_password",
                return_value=(
                    _ok({}),
                    DessMonitorSession(token="token-1", secret="secret-1"),
                ),
            ),
            patch(
                "custom_components.eybond_local.dessmonitor_cloud.fetch_signed_action",
                side_effect=fetch,
            ),
        ):
            bundle = fetch_read_only_evidence(
                username="account",
                password="password",
                collector_pn=FULL_PN[:14],
            )

        self.assertEqual(bundle.identity.pn, FULL_PN)
        self.assertEqual(bundle.telemetry_fields[0].title, "PV Voltage")
        self.assertEqual(bundle.control_fields[0].current_value, "Only PV charging is allowed")
        self.assertGreater(bundle.raw_packet_length, 0)
        self.assertNotIn("ctrlDevice", actions)
        self.assertEqual(
            actions,
            [
                "webQueryDeviceEs",
                "querySPDeviceLastData",
                "queryDeviceChartField",
                "querySPKeyParameters",
                "queryDeviceCtrlField",
                "queryDeviceLastRawData",
                "queryDeviceCtrlValue",
            ],
        )

    def test_foreign_or_ambiguous_identity_fails_closed_before_metadata(self) -> None:
        responses = [
            _ok(
                {
                    "device": [
                        {
                            "pn": "OTHER0000000000001",
                            "sn": "foreign",
                            "devcode": 2376,
                            "devaddr": 1,
                        }
                    ]
                }
            )
        ]
        with (
            patch(
                "custom_components.eybond_local.dessmonitor_cloud.login_with_password",
                return_value=(
                    _ok({}),
                    DessMonitorSession(token="token-1", secret="secret-1"),
                ),
            ),
            patch(
                "custom_components.eybond_local.dessmonitor_cloud.fetch_signed_action",
                side_effect=responses,
            ) as fetch,
        ):
            with self.assertRaisesRegex(Exception, "device_identity_ambiguous:0"):
                fetch_read_only_evidence(
                    username="account",
                    password="password",
                    collector_pn=FULL_PN,
                )

        self.assertEqual(fetch.call_count, 1)

    def test_credentials_and_session_never_enter_normalized_bundle(self) -> None:
        bundle = DessMonitorEvidenceBundle(
            identity=DessMonitorDeviceIdentity(
                pn=FULL_PN,
                sn="92632511100118",
                devcode=2376,
                devaddr=1,
            ),
            telemetry_fields=(),
            chart_fields=(),
            key_parameters=(),
            control_fields=(),
        )

        rendered = json.dumps(bundle.to_record())

        self.assertNotIn("username", rendered)
        self.assertNotIn("password", rendered)
        self.assertNotIn("secret", rendered)
        self.assertNotIn("token", rendered)


if __name__ == "__main__":
    unittest.main()
