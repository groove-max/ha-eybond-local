"""The final ZIP boundary must not trust upstream masking of stored evidence."""

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZipFile

from custom_components.eybond_local.support.package import export_support_package
from custom_components.eybond_local.support.redaction import sanitize_support_payload


class SupportArchiveRedactionTests(unittest.TestCase):
    def test_all_members_and_download_copy_remove_old_unmasked_session(self):
        credentials = {
            "token": "SYNTHETIC-TOKEN", "Secret": "SYNTHETIC-SECRET",
            "uid": "SYNTHETIC-UID", "usr": "SYNTHETIC-ACCOUNT",
            "password": "SYNTHETIC-PASSWORD", "email": "SYNTHETIC-EMAIL",
        }
        useful = {"devcode": 2376, "device_addr": 1, "register": 171,
                  "values": [14080, 1, 6200], "timestamp": 1792771200.125,
                  "signature": {"mode": "manual"}}
        cloud = {"payload": {"session": credentials, "device": useful,
                             "nested": [credentials, {"more": (credentials,)}]}}
        bundle = {"entry": {"options": credentials}, "evidence": {"cloud": cloud}}
        original = deepcopy(bundle)
        shadow = {
            "evidence/shadow_learning/trace.jsonl":
                json.dumps({"session": credentials, **useful}) + '\n{"token":"SYNTHETIC-BROKEN"\n',
            "evidence/shadow_learning/session_manifest.json": cloud,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stored = root / "old_cloud.json"
            stored.write_text(json.dumps(cloud))
            stored_before = stored.read_bytes()
            with patch("custom_components.eybond_local.support.package._build_shadow_learning_archive_members",
                       return_value=(shadow, {"trace": "evidence/shadow_learning/trace.jsonl"})):
                result = export_support_package(
                    config_dir=root, entry_id="synthetic", entry_title="Synthetic",
                    support_bundle=bundle, raw_capture=cloud, fixture=cloud,
                    anonymized_fixture=cloud, profile_source=cloud, register_schema_source=cloud,
                    publish_download_copy=True,
                )
            self.assertEqual(result.path.read_bytes(), result.download_path.read_bytes())
            self.assertEqual(stored.read_bytes(), stored_before)
            with ZipFile(result.path) as archive:
                for name in archive.namelist():
                    text = archive.read(name).decode()
                    for marker in (*credentials.values(), "SYNTHETIC-BROKEN"):
                        self.assertNotIn(marker, text, name)
                exported = json.loads(archive.read("evidence/cloud_evidence.json"))
                self.assertEqual(exported["payload"]["session"], {})
                self.assertEqual(exported["payload"]["device"], useful)
                records = archive.read("evidence/shadow_learning/trace.jsonl").decode().splitlines()
                self.assertEqual(json.loads(records[0])["timestamp"], useful["timestamp"])
                self.assertEqual(json.loads(records[1]), {"kind": "redacted_invalid_jsonl_record"})
        self.assertEqual(bundle, original)

    def test_nested_legacy_json_and_signed_urls_are_sanitized(self):
        value = {
            "response": json.dumps({"session": {"token": "old-token", "usr": "old-user"}, "err": 0}),
            "url": "https://url-user:url-pass@example.invalid/public/?action=query&%74oken=url-token&sign=url-sign&usr=url-account&pn=fixture#access_token=fragment-token",
            "headers": {"Authorization": "Bearer header-token", "Set-Cookie": "session=cookie-secret"},
            "tuple": ({"api_key": "old-key", "user_id": "old-id", "register": 171},),
            "download_url": "/api/eybond_local/support/synthetic?authSig=download-secret",
        }
        result = sanitize_support_payload(value)
        text = json.dumps(result)
        for marker in ("old-token", "old-user", "url-user", "url-pass", "url-token", "url-sign",
                       "url-account", "fragment-token", "header-token", "cookie-secret", "old-key", "old-id",
                       "download-secret"):
            self.assertNotIn(marker, text)
        self.assertEqual(json.loads(result["response"]), {"session": {}, "err": 0})
        self.assertEqual(result["tuple"], [{"register": 171}])
        self.assertEqual(result["url"], "https://example.invalid/public/?action=query&pn=fixture")
        self.assertEqual(result["download_url"], "/api/eybond_local/support/synthetic")

    def test_opaque_objects_cannot_leak_their_repr(self):
        class SecretObject:
            def __str__(self):
                return "sensitive repr"
        self.assertEqual(sanitize_support_payload({"value": SecretObject()}),
                         {"value": "<unsupported:SecretObject>"})
