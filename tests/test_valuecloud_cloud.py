from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.valuecloud_cloud import (  # noqa: E402
    LOGIN_PATH,
    ValueCloudEnvelope,
    ValueCloudSession,
    _headers_for_path,
    fetch_device_bundle_for_collector_with_session,
    login_with_password,
)


def _envelope(data, *, code=200, success=True, headers=None) -> ValueCloudEnvelope:
    return ValueCloudEnvelope(
        code=code,
        message="ok",
        error_message="",
        success=success,
        data=data,
        raw={"code": code, "success": success, "data": data},
        headers=dict(headers or {}),
    )


class ValueCloudCloudTests(unittest.TestCase):
    def test_authenticated_headers_sign_request_path_with_secret(self) -> None:
        session = ValueCloudSession(
            token="token-123",
            secret="secret-456",
            auth="auth-789",
        )

        headers = _headers_for_path(
            "ppe/api/auth/app/querySPDeviceLastData",
            session=session,
        )

        expected_sign = hmac.new(
            b"secret-456",
            b"/ppe/api/auth/app/querySPDeviceLastData",
            hashlib.sha256,
        ).hexdigest()
        self.assertEqual(headers["token"], "token-123")
        self.assertEqual(headers["Auth"], "auth-789")
        self.assertEqual(headers["sign"], expected_sign)
        self.assertEqual(headers["project"], "IOT")

    def test_login_extracts_token_secret_auth_and_masks_nothing_in_error_path(self) -> None:
        with patch(
            "custom_components.eybond_local.valuecloud_cloud._http_json",
            return_value=_envelope(
                {
                    "token": "token-123",
                    "secret": "secret-456",
                    "userId": "user-1",
                },
                headers={"Auth": "auth-789"},
            ),
        ) as http_json:
            envelope, session = login_with_password(
                username="user@example.com",
                password="plain-password",
            )

        self.assertEqual(envelope.data["token"], session.token)
        self.assertEqual(session.secret, "secret-456")
        self.assertEqual(session.auth, "auth-789")
        self.assertEqual(session.user_id, "user-1")
        self.assertEqual(session.source_endpoint, LOGIN_PATH)
        self.assertEqual(http_json.call_count, 1)
        self.assertEqual(http_json.call_args.kwargs["path"], LOGIN_PATH)
        sent_body = http_json.call_args.kwargs["body"]
        self.assertEqual(sent_body["account"], "user@example.com")
        self.assertEqual(sent_body["password"], hashlib.sha1(b"plain-password").hexdigest())
        self.assertEqual(sent_body["project"], "IOT")

    def test_device_bundle_normalizes_valuecloud_sections_and_controls(self) -> None:
        session = ValueCloudSession(
            token="token-123",
            secret="secret-456",
            auth="auth-789",
            user_id="user-1",
            account="user@example.com",
            source_endpoint="ppr/app/login/pub/login",
        )
        envelopes = [
            _envelope(
                {
                    "items": [
                        {
                            "pn": "A0000000000001",
                            "deviceSn": "TY-SIC-3.6KBE-W1",
                            "devcode": 2452,
                            "devaddr": 255,
                            "deviceName": "LVYUAN TY-SIC-3.6KBE-W1",
                            "userId": "user-1",
                        }
                    ],
                    "total": 1,
                }
            ),
            _envelope(
                {
                    "pars": {
                        "gd_": [
                            {"id": "gd_1", "par": "Grid Voltage", "val": "230.1", "unit": "V"}
                        ],
                        "pv_": [
                            {"id": "pv_1", "par": "PV Power", "val": "900", "unit": "W"}
                        ],
                    }
                }
            ),
            _envelope(
                [
                    {"id": "par_1", "name": "Output Mode", "unit": "", "item": {"0": "Grid"}}
                ]
            ),
            _envelope(
                [
                    {
                        "id": "ctrl_1",
                        "name": "LCD Backlight",
                        "val": "1",
                        "displayValue": "On",
                    }
                ]
            ),
            _envelope([]),
        ]

        with patch(
            "custom_components.eybond_local.valuecloud_cloud.fetch_authenticated_envelope",
            side_effect=envelopes,
        ):
            payload = fetch_device_bundle_for_collector_with_session(
                session=session,
                collector_pn="A0000000000001",
            )

        self.assertEqual(payload["request"]["provider"], "valuecloud")
        self.assertEqual(payload["request"]["params"]["devcode"], 2452)
        self.assertEqual(payload["normalized"]["device_list"]["device_count"], 1)
        self.assertEqual(
            payload["normalized"]["device_detail"]["section_counts"],
            {"gd_": 1, "pv_": 1},
        )
        self.assertEqual(payload["normalized"]["device_pars"]["field_count"], 1)
        self.assertEqual(payload["normalized"]["control_strategy"]["current_value_count"], 1)
        serialized = json.dumps(payload)
        self.assertNotIn("plain-password", serialized)
        self.assertNotIn("user@example.com", serialized)


if __name__ == "__main__":
    unittest.main()
