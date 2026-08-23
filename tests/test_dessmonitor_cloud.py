from __future__ import annotations

from dataclasses import fields
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.error import URLError
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
    DessMonitorActionRejectedError,
    DessMonitorCloudError,
    DessMonitorControlField,
    DessMonitorDeviceIdentity,
    DessMonitorEvidenceBundle,
    DessMonitorSession,
    DessMonitorTelemetryField,
    build_device_control_action,
    build_login_url,
    build_signed_action_url,
    fetch_read_only_evidence,
    fetch_read_only_evidence_for_session,
    fetch_signed_action,
    send_device_control,
)


FULL_PN = "E50000200000000001"


def _ok(dat) -> DessMonitorApiEnvelope:
    return DessMonitorApiEnvelope(err=0, desc="ERR_NONE", dat=dat)


class DessMonitorModelTests(unittest.TestCase):
    def test_session_repr_never_discloses_signed_material(self) -> None:
        session = DessMonitorSession(
            token="token-secret-value",
            secret="request-secret-value",
        )

        rendered = repr(session)

        self.assertNotIn("token-secret-value", rendered)
        self.assertNotIn("request-secret-value", rendered)
        envelope = DessMonitorApiEnvelope(
            err=0,
            desc="private provider detail",
            dat={"account": "private-account-name"},
        )
        self.assertNotIn("private provider detail", repr(envelope))
        self.assertNotIn("private-account-name", repr(envelope))

    def test_control_action_is_strict_and_identity_bound(self) -> None:
        identity = DessMonitorDeviceIdentity(
            pn=FULL_PN,
            sn="90000000000001",
            devcode=2376,
            devaddr=1,
        )
        action = build_device_control_action(
            identity=identity,
            field_id="bat_eybond_ctrl_75",
            value="3",
        )
        params = parse_qs(action.removeprefix("&"))
        self.assertEqual(
            params,
            {
                "action": ["ctrlDevice"],
                "pn": [FULL_PN],
                "sn": ["90000000000001"],
                "devcode": ["2376"],
                "devaddr": ["1"],
                "id": ["bat_eybond_ctrl_75"],
                "val": ["3"],
            },
        )
        for field_id, value in ((" padded ", "3"), ("id", " 3"), ("", "3")):
            with self.subTest(field_id=field_id, value=value):
                with self.assertRaises(ValueError):
                    build_device_control_action(
                        identity=identity,
                        field_id=field_id,
                        value=value,
                    )
        with self.assertRaises(TypeError):
            build_device_control_action(  # type: ignore[arg-type]
                identity=object(), field_id="id", value="3"
            )

    def test_send_control_uses_exact_session_and_propagates_typed_rejection(self) -> None:
        identity = DessMonitorDeviceIdentity(
            pn=FULL_PN,
            sn="90000000000001",
            devcode=2376,
            devaddr=1,
        )
        session = DessMonitorSession(token="token-1", secret="secret-1")
        with patch.object(
            dessmonitor_module,
            "fetch_signed_action",
            return_value=_ok({"dat": "AA BB"}),
        ) as fetch:
            envelope = send_device_control(
                session=session,
                identity=identity,
                field_id="bat_eybond_ctrl_75",
                value="3",
            )
        self.assertEqual(envelope.err, 0)
        self.assertIs(fetch.call_args.kwargs["session"], session)
        self.assertIn("&action=ctrlDevice", fetch.call_args.kwargs["action"])

        rejection = DessMonitorActionRejectedError(
            err=11,
            action="ctrlDevice",
            desc="ERR_NO_PERMISSION",
        )
        with patch.object(
            dessmonitor_module,
            "fetch_signed_action",
            side_effect=rejection,
        ):
            with self.assertRaises(DessMonitorActionRejectedError) as raised:
                send_device_control(
                    session=session,
                    identity=identity,
                    field_id="bat_eybond_ctrl_75",
                    value="3",
                )
        self.assertIs(raised.exception, rejection)

        with self.assertRaises(TypeError):
            send_device_control(  # type: ignore[arg-type]
                session=object(),
                identity=identity,
                field_id="id",
                value="1",
            )

    def test_direct_constructors_are_strict_and_json_safe(self) -> None:
        identity = DessMonitorDeviceIdentity(
            pn=FULL_PN,
            sn="90000000000001",
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

        with self.assertRaisesRegex(TypeError, "unavailable_actions"):
            DessMonitorEvidenceBundle(
                identity=identity,
                telemetry_fields=(),
                chart_fields=(),
                key_parameters=(),
                control_fields=(),
                unavailable_actions=["querySPKeyParameters"],  # type: ignore[arg-type]
            )
        for unavailable_actions in (
            ("querySPKeyParameters", "querySPKeyParameters"),
            ("unknownAction",),
        ):
            with self.subTest(unavailable_actions=unavailable_actions):
                with self.assertRaisesRegex(ValueError, "unavailable_actions"):
                    DessMonitorEvidenceBundle(
                        identity=identity,
                        telemetry_fields=(),
                        chart_fields=(),
                        key_parameters=(),
                        control_fields=(),
                        unavailable_actions=unavailable_actions,
                    )

        with self.assertRaisesRegex(ValueError, "session_token_invalid"):
            DessMonitorSession(
                token="x" * (dessmonitor_module.DEFAULT_MAX_TEXT_LENGTH + 1),
                secret="secret",
            )

        error = DessMonitorCloudError(
            "http_error:503",
            stage="querySPDeviceLastData",
        )
        self.assertEqual(error.reason_code, "http_error:503")
        self.assertEqual(error.stage, "querySPDeviceLastData")
        for reason, stage in (
            ("private detail", "querySPDeviceLastData"),
            ("network_error", " padded "),
            ("network_error", "token=secret"),
        ):
            with self.subTest(reason=reason, stage=stage):
                with self.assertRaises(ValueError):
                    DessMonitorCloudError(reason, stage=stage)

    def test_session_bound_metadata_refuses_duck_before_any_request(self) -> None:
        with patch.object(dessmonitor_module, "fetch_signed_action") as fetch:
            with self.assertRaises(TypeError):
                fetch_read_only_evidence_for_session(  # type: ignore[arg-type]
                    session=object(),
                    collector_pn=FULL_PN,
                )
        fetch.assert_not_called()

    def test_required_metadata_actions_are_strict_before_any_request(self) -> None:
        session = DessMonitorSession(token="token", secret="secret")
        cases = (
            (["queryDeviceCtrlField"], TypeError),
            ((object(),), TypeError),
            (("queryDeviceCtrlField", "queryDeviceCtrlField"), ValueError),
            ((" queryDeviceCtrlField ",), ValueError),
            (("unknownAction",), ValueError),
            (("",), ValueError),
        )
        with patch.object(dessmonitor_module, "fetch_signed_action") as fetch:
            for required_actions, expected in cases:
                with self.subTest(required_actions=required_actions):
                    with self.assertRaises(expected):
                        fetch_read_only_evidence_for_session(
                            session=session,
                            collector_pn=FULL_PN,
                            required_actions=required_actions,  # type: ignore[arg-type]
                        )
        fetch.assert_not_called()


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

    def test_login_rejects_malformed_session_material_without_coercion(self) -> None:
        cases = (
            ({"token": 7, "secret": "secret"}, "invalid_login_session_token"),
            ({"token": " token ", "secret": "secret"}, "invalid_login_session_token"),
            (
                {
                    "token": "t" * (dessmonitor_module.DEFAULT_MAX_TEXT_LENGTH + 1),
                    "secret": "secret",
                },
                "invalid_login_session_token",
            ),
            ({"token": "token", "secret": b"secret"}, "invalid_login_session_secret"),
        )
        for dat, expected in cases:
            with self.subTest(expected=expected, value_type=type(next(iter(dat.values())))):
                with patch.object(
                    dessmonitor_module,
                    "_http_get_json",
                    return_value=_ok(dat),
                ):
                    with self.assertRaises(dessmonitor_module.DessMonitorCloudError) as raised:
                        dessmonitor_module.login_with_password(
                            username="account",
                            password="password",
                        )

                self.assertEqual(str(raised.exception), expected)
                self.assertNotIn(repr(dat), str(raised.exception))

    def test_login_ignores_unneeded_provider_account_metadata(self) -> None:
        provider_payload = {
            "token": "token",
            "secret": "secret",
            "uid": 9,
            "usr": {"unexpected": "shape"},
            "role": 0,
            "expire": 604800,
        }
        with patch.object(
            dessmonitor_module,
            "_http_get_json",
            return_value=_ok(provider_payload),
        ):
            _envelope, session = dessmonitor_module.login_with_password(
                username="account",
                password="password",
            )

        self.assertEqual(session.token, "token")
        self.assertEqual(session.secret, "secret")
        self.assertEqual(
            {item.name for item in fields(DessMonitorSession)},
            {"token", "secret"},
        )

    def test_network_error_never_discloses_provider_reason(self) -> None:
        sensitive_reason = "temporary failure for account@example.com"
        with patch.object(
            dessmonitor_module,
            "urlopen",
            side_effect=URLError(sensitive_reason),
        ):
            with self.assertRaises(dessmonitor_module.DessMonitorCloudError) as raised:
                dessmonitor_module.login_with_password(
                    username="account",
                    password="password",
                )

        self.assertEqual(str(raised.exception), "network_error")
        self.assertEqual(raised.exception.reason_code, "network_error")
        self.assertEqual(raised.exception.stage, "authSource")
        self.assertNotIn(sensitive_reason, str(raised.exception))

    def test_signed_request_failure_carries_only_its_action_stage(self) -> None:
        session = DessMonitorSession(token="token-1", secret="secret-1")
        with patch.object(
            dessmonitor_module,
            "_http_get_json",
            side_effect=DessMonitorCloudError("invalid_json"),
        ):
            with self.assertRaises(DessMonitorCloudError) as raised:
                fetch_signed_action(
                    action="&action=queryDeviceLastRawData&pn=collector",
                    session=session,
                )

        self.assertEqual(raised.exception.reason_code, "invalid_json")
        self.assertEqual(raised.exception.stage, "queryDeviceLastRawData")
        self.assertNotIn("token-1", repr(raised.exception))
        self.assertNotIn("collector", repr(raised.exception))

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
    def test_required_control_metadata_retries_once_then_succeeds(self) -> None:
        identity = _ok(
            {
                "device": [
                    {
                        "pn": FULL_PN,
                        "sn": "90000000000001",
                        "devcode": 2376,
                        "devaddr": 1,
                    }
                ]
            }
        )
        control_calls = 0

        def fetch(*, action, **_kwargs):
            nonlocal control_calls
            action_name = parse_qs(action.removeprefix("&"))["action"][0]
            if action_name == "webQueryDeviceEs":
                return identity
            if action_name == "querySPDeviceLastData":
                return _ok({"pars": {"main": []}})
            if action_name == "queryDeviceCtrlField":
                control_calls += 1
                if control_calls == 1:
                    raise DessMonitorCloudError(
                        "network_error",
                        stage=action_name,
                    )
                return _ok(
                    {
                        "field": [
                            {
                                "id": "mode",
                                "name": "Mode",
                                "item": [{"key": "1", "val": "One"}],
                            }
                        ]
                    }
                )
            return _ok(None)

        with patch.object(
            dessmonitor_module,
            "fetch_signed_action",
            side_effect=fetch,
        ):
            bundle = fetch_read_only_evidence_for_session(
                session=DessMonitorSession(token="token", secret="secret"),
                collector_pn=FULL_PN,
                max_control_values=0,
                required_actions=("queryDeviceCtrlField",),
            )

        self.assertEqual(control_calls, 2)
        self.assertEqual(
            [(field.field_id, field.choices) for field in bundle.control_fields],
            [("mode", (("1", "One"),))],
        )
        self.assertNotIn("queryDeviceCtrlField", bundle.unavailable_actions)

    def test_required_control_metadata_second_failure_propagates(self) -> None:
        identity = _ok(
            {
                "device": [
                    {
                        "pn": FULL_PN,
                        "sn": "90000000000001",
                        "devcode": 2376,
                        "devaddr": 1,
                    }
                ]
            }
        )
        control_calls = 0

        def fetch(*, action, **_kwargs):
            nonlocal control_calls
            action_name = parse_qs(action.removeprefix("&"))["action"][0]
            if action_name == "webQueryDeviceEs":
                return identity
            if action_name == "queryDeviceCtrlField":
                control_calls += 1
                raise DessMonitorCloudError(
                    "network_error",
                    stage=action_name,
                )
            return _ok(None)

        with patch.object(
            dessmonitor_module,
            "fetch_signed_action",
            side_effect=fetch,
        ):
            with self.assertRaises(DessMonitorCloudError) as raised:
                fetch_read_only_evidence_for_session(
                    session=DessMonitorSession(token="token", secret="secret"),
                    collector_pn=FULL_PN,
                    max_control_values=0,
                    required_actions=("queryDeviceCtrlField",),
                )

        self.assertEqual(control_calls, 2)
        self.assertEqual(raised.exception.reason_code, "network_error")
        self.assertEqual(raised.exception.stage, "queryDeviceCtrlField")

    def test_live_key_parameter_shape_is_normalized_without_duck_coercion(self) -> None:
        identity = _ok(
            {
                "device": [
                    {
                        "pn": FULL_PN,
                        "sn": "90000000000001",
                        "devcode": 2376,
                        "devaddr": 1,
                    }
                ]
            }
        )

        class _DuckString(str):
            pass

        def fetch(*, action, **_kwargs):
            action_name = parse_qs(action.removeprefix("&"))["action"][0]
            if action_name == "webQueryDeviceEs":
                return identity
            if action_name == "querySPKeyParameters":
                return _ok(
                    {
                        "keys": [
                            "PV_OUTPUT_POWER",
                            "GRID_ACTIVE_POWER",
                            "UNKNOWN_PROVIDER_FIELD",
                            " padded ",
                            _DuckString("DUCK_FIELD"),
                            7,
                        ]
                    }
                )
            return _ok(None)

        with patch.object(
            dessmonitor_module,
            "fetch_signed_action",
            side_effect=fetch,
        ):
            bundle = fetch_read_only_evidence_for_session(
                session=DessMonitorSession(token="token", secret="secret"),
                collector_pn=FULL_PN,
                max_control_values=0,
            )

        self.assertEqual(
            [(field.field_id, field.title) for field in bundle.key_parameters],
            [
                ("PV_OUTPUT_POWER", "PV Power"),
                ("GRID_ACTIVE_POWER", "Grid Power"),
                ("UNKNOWN_PROVIDER_FIELD", "Unknown Provider Field"),
            ],
        )

    def test_control_value_transport_failure_stops_repeated_timeouts(self) -> None:
        identity = _ok(
            {
                "device": [
                    {
                        "pn": FULL_PN,
                        "sn": "90000000000001",
                        "devcode": 2376,
                        "devaddr": 1,
                    }
                ]
            }
        )
        controls = [
            {"id": f"control_{index}", "name": f"Control {index}"}
            for index in range(4)
        ]
        control_value_calls = 0
        progress_detail: list[tuple[str, int, int]] = []

        def fetch(*, action, **_kwargs):
            nonlocal control_value_calls
            action_name = parse_qs(action.removeprefix("&"))["action"][0]
            if action_name == "webQueryDeviceEs":
                return identity
            if action_name == "queryDeviceCtrlField":
                return _ok({"field": controls})
            if action_name == "queryDeviceCtrlValue":
                control_value_calls += 1
                raise DessMonitorCloudError("network_error", stage=action_name)
            return _ok(None)

        with patch.object(
            dessmonitor_module,
            "fetch_signed_action",
            side_effect=fetch,
        ):
            bundle = fetch_read_only_evidence_for_session(
                session=DessMonitorSession(token="token", secret="secret"),
                collector_pn=FULL_PN,
                progress_detail=lambda *args: progress_detail.append(args),
            )

        self.assertEqual(control_value_calls, 1)
        self.assertEqual(len(bundle.control_fields), 4)
        self.assertTrue(
            all(not field.current_value for field in bundle.control_fields)
        )
        self.assertIn("queryDeviceCtrlValue", bundle.unavailable_actions)
        self.assertEqual(
            progress_detail,
            [("queryDeviceCtrlValue", 1, 4)],
        )

    def test_control_value_rejection_does_not_hide_later_field_values(self) -> None:
        identity = _ok(
            {
                "device": [
                    {
                        "pn": FULL_PN,
                        "sn": "90000000000001",
                        "devcode": 2376,
                        "devaddr": 1,
                    }
                ]
            }
        )
        controls = [
            {"id": "unsupported", "name": "Unsupported"},
            {"id": "available", "name": "Available"},
        ]
        control_value_calls = 0
        progress_detail: list[tuple[str, int, int]] = []

        def fetch(*, action, **_kwargs):
            nonlocal control_value_calls
            query = parse_qs(action.removeprefix("&"))
            action_name = query["action"][0]
            if action_name == "webQueryDeviceEs":
                return identity
            if action_name == "queryDeviceCtrlField":
                return _ok({"field": controls})
            if action_name == "queryDeviceCtrlValue":
                control_value_calls += 1
                if query["id"][0] == "unsupported":
                    raise DessMonitorActionRejectedError(
                        err=7,
                        action=action_name,
                        desc="not available",
                    )
                return _ok({"val": "42"})
            return _ok(None)

        with patch.object(
            dessmonitor_module,
            "fetch_signed_action",
            side_effect=fetch,
        ):
            bundle = fetch_read_only_evidence_for_session(
                session=DessMonitorSession(token="token", secret="secret"),
                collector_pn=FULL_PN,
                progress_detail=lambda *args: progress_detail.append(args),
            )

        self.assertEqual(control_value_calls, 2)
        self.assertEqual(
            [field.current_value for field in bundle.control_fields],
            ["", "42"],
        )
        self.assertIn("queryDeviceCtrlValue", bundle.unavailable_actions)
        self.assertEqual(
            progress_detail,
            [
                ("queryDeviceCtrlValue", 1, 2),
                ("queryDeviceCtrlValue", 2, 2),
            ],
        )

    def test_optional_metadata_failure_preserves_independent_evidence(self) -> None:
        identity = _ok(
            {
                "device": [
                    {
                        "pn": FULL_PN,
                        "sn": "90000000000001",
                        "devcode": 2376,
                        "devaddr": 1,
                    }
                ]
            }
        )
        responses = {
            "webQueryDeviceEs": identity,
            "querySPDeviceLastData": _ok(
                {
                    "pars": {
                        "pv_": [
                            {
                                "id": "pv_voltage",
                                "par": "PV Voltage",
                                "val": "123.4",
                                "unit": "V",
                            }
                        ]
                    }
                }
            ),
            "queryDeviceChartField": _ok([]),
            "queryDeviceCtrlField": _ok({"field": []}),
            "queryDeviceLastRawData": _ok(None),
        }
        progress: list[str] = []

        def fetch(*, action, **_kwargs):
            action_name = parse_qs(action.removeprefix("&"))["action"][0]
            if action_name == "querySPKeyParameters":
                raise DessMonitorCloudError(
                    "network_error",
                    stage=action_name,
                )
            return responses[action_name]

        with patch.object(
            dessmonitor_module,
            "fetch_signed_action",
            side_effect=fetch,
        ):
            bundle = fetch_read_only_evidence_for_session(
                session=DessMonitorSession(token="token", secret="secret"),
                collector_pn=FULL_PN,
                progress=progress.append,
            )

        self.assertEqual(bundle.telemetry_fields[0].title, "PV Voltage")
        self.assertEqual(
            bundle.unavailable_actions,
            ("querySPKeyParameters",),
        )
        self.assertEqual(
            bundle.to_record()["unavailable_actions"],
            ["querySPKeyParameters"],
        )
        self.assertEqual(progress[0], "webQueryDeviceEs")
        self.assertEqual(progress[-1], "metadata_bundle")

    def test_all_optional_metadata_failures_do_not_mint_empty_success(self) -> None:
        identity = _ok(
            {
                "device": [
                    {
                        "pn": FULL_PN,
                        "sn": "90000000000001",
                        "devcode": 2376,
                        "devaddr": 1,
                    }
                ]
            }
        )
        calls = 0

        def fetch(*, action, **_kwargs):
            nonlocal calls
            calls += 1
            action_name = parse_qs(action.removeprefix("&"))["action"][0]
            if action_name == "webQueryDeviceEs":
                return identity
            raise DessMonitorCloudError("network_error", stage=action_name)

        with patch.object(
            dessmonitor_module,
            "fetch_signed_action",
            side_effect=fetch,
        ):
            with self.assertRaises(DessMonitorCloudError) as raised:
                fetch_read_only_evidence_for_session(
                    session=DessMonitorSession(token="token", secret="secret"),
                    collector_pn=FULL_PN,
                )

        self.assertEqual(raised.exception.reason_code, "device_metadata_missing")
        self.assertEqual(raised.exception.stage, "metadata_bundle")
        self.assertEqual(calls, 6)

    def test_metadata_arrays_and_scalar_values_are_bounded(self) -> None:
        identity = {
            "device": [
                {
                    "pn": FULL_PN,
                    "sn": "90000000000001",
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
                        "sn": "90000000000001",
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
                sn="90000000000001",
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
