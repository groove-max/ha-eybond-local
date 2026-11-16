"""HA OS users obtain the CLI inventory through ordinary support exports."""

from io import BytesIO
import json
from pathlib import Path
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
@pytest.mark.parametrize("history", ["valid", "corrupt", "absent"])
async def test_support_export_contains_safe_worker_inventory_without_recovery(
    hass, collector_entry, fake_runtime, monkeypatch, tmp_path, hass_client_no_auth,
    kind, history,
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
    original = review.ReviewedLearningRecoveryStore.inspect
    def inspect(self, collector_pn, **kwargs):
        assert threading.get_ident() != loop_thread
        assert collector_pn == pn and kwargs["entry_id"] == collector_entry.entry_id
        inspected.append(True)
        return original(self, collector_pn, **kwargs)
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
        assert all(len(run["trace_sha256"]) == 64 for run in result["runs"])
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
