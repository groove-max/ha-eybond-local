"""Quarantine interrupted/legacy learning before any endpoint restoration."""

import hashlib
import asyncio
from dataclasses import replace
from pathlib import Path

from ...connection.cloud_learning_protection import CloudLearningHold, CloudLearningProtectionRequired
from ...support.owned_worker import async_run_owned_worker
from ...support.shadow_learning.protection_store import CloudLearningProtectionStore
from ...support.shadow_learning.session import (
    inspect_shadow_learning_recovery, load_shadow_learning_session_state,
    find_shadow_learning_session_for_collector,
    shadow_learning_session_timestamp,
)


def _recovery_lock(coordinator):
    lock = getattr(coordinator, "_no_request_recovery_lock", None)
    if lock is None:
        lock = coordinator._no_request_recovery_lock = asyncio.Lock()
    return lock


async def async_complete_no_request_recovery(coordinator, state, hold) -> bool:
    async with _recovery_lock(coordinator):
        current = await coordinator._async_active_shadow_learning_state(require_process=False)
        if current is None or current.route_owner_id != state.route_owner_id:
            return False
        return await _async_complete_no_request_recovery(coordinator, current, hold)


async def _async_complete_no_request_recovery(coordinator, state, hold) -> bool:
    """Finish an exact durable receipt; never turn a receipt into dispatch permission."""
    c = coordinator
    if (state is None or state.protection_version != 1
            or state.entry_id != c.config_entry.entry_id or state.entry_id != hold.entry_id
            or state.collector_pn != c.smartess_collector_pn or state.collector_pn != hold.collector_pn
            or state.cloud_source != hold.source
            or state.route_owner_id != "shadow_learning:" + hold.study_id):
        return False
    store = CloudLearningProtectionStore(Path(c.hass.config.config_dir))
    try:
        await async_run_owned_worker(store.require_no_request_receipt, hold=hold)
    except CloudLearningProtectionRequired:
        return False
    # Receipt first, journal second, archive last. Every interrupted prefix either
    # remains held or can finish this exact no-send correction after restart.
    await c._async_save_shadow_learning_session_state(replace(
        state, cloud_commands_possible=False, last_restore_error="",
        updated_at=shadow_learning_session_timestamp(),
    ))
    await async_run_owned_worker(store.retire_no_request, hold=hold)
    await c._cloud_learning_mutation_fence.accept_no_request_retirement(hold)
    c._protected_learning_run = None
    c._cloud_learning_hold_notified = False
    c._publish_tooling_values(cloud_learning_protection_status="not_held", cloud_learning_protection_reason="")
    from homeassistant.components import persistent_notification
    from ...const import DOMAIN
    persistent_notification.async_dismiss(c.hass, f"{DOMAIN}_cloud_learning_hold_{c.config_entry.entry_id}")
    return True


async def async_preserve_interrupted_learning_hold(coordinator) -> None:
    async with _recovery_lock(coordinator):
        await _async_preserve_interrupted_learning_hold(coordinator)


async def _async_preserve_interrupted_learning_hold(coordinator) -> None:
    """Recover blocking intent, never recover permission to send cloud commands."""

    c = coordinator
    state = await c._async_active_shadow_learning_state(require_process=False)
    if state is None:
        # Removing/re-adding an entry must not hide a collector's old scoped
        # journal. Recover only its safety obligation, never the foreign owner.
        state = await async_run_owned_worker(
            find_shadow_learning_session_for_collector,
            config_dir=Path(c.hass.config.config_dir), collector_pn=c.smartess_collector_pn,
        )
    store = CloudLearningProtectionStore(Path(c.hass.config.config_dir))
    if state is not None:
        try:
            existing = await async_run_owned_worker(store.load, collector_pn=c.smartess_collector_pn)
            if existing is not None and await _async_complete_no_request_recovery(c, state, existing):
                return
            if existing is not None:
                from .learning_completion import async_recover_completed_study
                if await async_recover_completed_study(c, state, existing):
                    return
        except CloudLearningProtectionRequired:
            return
    if state is not None and state.protection_version == 1 and not state.cloud_commands_possible:
        return
    config_dir = Path(c.hass.config.config_dir)
    if state is None:
        facts = await async_run_owned_worker(
            inspect_shadow_learning_recovery, config_dir=config_dir,
            entry_id=c.config_entry.entry_id, collector_pn=c.smartess_collector_pn,
        )
        disk_status = facts.get("shadow_learning_recovery_disk_status")
        if disk_status in {
            "missing", "collector_mismatch",
        }:
            return
        if disk_status == "foreign_entry":
            # Entry IDs can change when a collector is removed/re-added. PN,
            # not an obsolete config-entry ID, owns the safety obligation.
            previous = await async_run_owned_worker(load_shadow_learning_session_state, config_dir=config_dir)
            if previous is not None:
                if previous.collector_pn != c.smartess_collector_pn:
                    return
                if previous.protection_version == 1 and not previous.cloud_commands_possible:
                    return
        pn, owner = c.smartess_collector_pn, "unreadable_recovery"
    else:
        pn, owner = state.collector_pn, state.route_owner_id
    store = CloudLearningProtectionStore(config_dir)
    try:
        existing = await async_run_owned_worker(store.load, collector_pn=pn)
    except CloudLearningProtectionRequired:
        return  # The fence treats unreadable/corrupt holds as blocking.
    if existing is not None:
        return
    await store.async_create(hold=CloudLearningHold(
        entry_id=c.config_entry.entry_id,
        study_id="recovery_" + hashlib.sha256(owner.encode()).hexdigest(),
        collector_pn=pn, source="legacy", armed_at=shadow_learning_session_timestamp(),
    ))
