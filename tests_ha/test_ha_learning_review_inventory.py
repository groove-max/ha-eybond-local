"""HA OS users obtain the CLI inventory through ordinary support exports."""

from io import BytesIO
import json
from pathlib import Path
import re
import threading
from unittest.mock import AsyncMock
from zipfile import ZipFile

import pytest

from custom_components.eybond_local.support.shadow_learning import reviewed_recovery as review
from tests.test_reviewed_learning_recovery import history_fixture
from tests.test_reviewed_learning_inventory import files
from test_ha_config_flow import collector_entry


KEY = "cloud_learning_protection_review_inventory"


@pytest.mark.parametrize("kind", ["bundle", "archive"])
@pytest.mark.parametrize("history,digest_collision", [
    ("valid", False), ("valid", True), ("corrupt", False), ("absent", False),
])
async def test_support_export_contains_safe_worker_inventory_without_recovery(
    hass, collector_entry, fake_runtime, monkeypatch, tmp_path, hass_client_no_auth,
    kind, history, digest_collision,
):
    monkeypatch.setattr(hass.config, "config_dir", str(tmp_path))
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    pn = coordinator.smartess_collector_pn
    root = Path(hass.config.config_dir)
    if history != "absent":
        store, hold, states, evidence = await hass.async_add_executor_job(
            lambda: history_fixture(root, entry_id=collector_entry.entry_id, pn=pn))
        if history == "corrupt":
            await hass.async_add_executor_job(evidence[0].write_text, "private invalid trace")
        before = await hass.async_add_executor_job(lambda: (files(store._root), files(evidence[0].parent)))

    loop_thread = threading.get_ident()
    inspected = []
    inventories = []
    original = review.ReviewedLearningRecoveryStore.inspect
    def inspect(self, collector_pn, **kwargs):
        assert threading.get_ident() != loop_thread
        assert collector_pn == pn and kwargs["entry_id"] == collector_entry.entry_id
        inspected.append(True)
        inventory = original(self, collector_pn, **kwargs)
        if digest_collision:
            # Exercise export with deterministic synthetic hash values containing
            # digit runs. Real history inspection still runs; no approval or
            # receipt is built from these test-only hashes.
            inventory["history_sha256"] = "abcdef" + "1234567890123456" + "abcdef" * 7
            for run in inventory["runs"]:
                run["trace_sha256"] = "31323334353637383930313233343536" + "a" * 32
        inventories.append(inventory)
        return inventory
    monkeypatch.setattr(review.ReviewedLearningRecoveryStore, "inspect", inspect)
    def forbidden(*args, **kwargs):
        raise AssertionError("support inspection must not approve or restore")
    monkeypatch.setattr(review.ReviewedLearningRecoveryStore, "approve", forbidden)
    monkeypatch.setattr(review.ReviewedLearningRecoveryStore, "apply", forbidden)
    restore = AsyncMock(side_effect=forbidden)
    monkeypatch.setattr(coordinator, "_async_restore_proxy_capture_endpoint", restore)

    if kind == "bundle":
        path = Path(await coordinator.async_export_support_bundle())
        bundle = json.loads(await hass.async_add_executor_job(path.read_text))
    else:
        path = Path(await coordinator.async_export_support_package())
        client = await hass_client_no_auth()
        url = coordinator.data.values["support_package_download_url"]
        response = await client.get(url)
        assert response.status == 200
        downloaded = await response.read()
        assert downloaded == await hass.async_add_executor_job(path.read_bytes)
        with ZipFile(BytesIO(downloaded)) as archive:
            bundle = json.loads(archive.read("support_bundle.json"))

    result = bundle["runtime"]["metadata"][KEY]
    assert inspected
    assert result == bundle["runtime"]["values"][KEY]
    if history == "valid":
        assert result["status"] == "review_required" and result["run_count"] == 2
        assert [run["original"] for run in result["runs"]] == [True, False]
        assert result == inventories[-1]
        assert re.fullmatch(r"[0-9a-f]{64}", result["history_sha256"])
        assert all(re.fullmatch(r"[0-9a-f]{64}", run["trace_sha256"]) for run in result["runs"])
        for secret in (pn, str(root), hold.entry_id, states[0].original_endpoint, states[-1].route_owner_id):
            assert secret not in json.dumps(result)
    else:
        assert result == ({"status": "no_hold"} if history == "absent" else {
            "status": "unavailable", "reason": "history_invalid_or_unreadable",
        })
    restore.assert_not_awaited()
    if history != "absent":
        assert await hass.async_add_executor_job(lambda: (files(store._root), files(evidence[0].parent))) == before
        assert await hass.async_add_executor_job(store.load, pn) == hold
    assert await hass.config_entries.async_unload(collector_entry.entry_id)
    await hass.async_block_till_done()
