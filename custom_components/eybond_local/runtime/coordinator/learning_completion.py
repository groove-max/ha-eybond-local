"""Retire only this live study, after both pumps and all HTTP actions drained."""
from dataclasses import replace
import logging
from pathlib import Path

from ...connection.cloud_learning_protection import CloudLearningProtectionRequired
from ...support.owned_worker import async_run_owned_worker
from ...support.shadow_learning.completion import CloudCommandCompletion
from ...support.shadow_learning.completion_store import CompletedLearningStore
from ...support.shadow_learning.session import shadow_learning_session_timestamp

_LOGGER = logging.getLogger(__name__)


async def async_recover_completed_study(c, state, hold) -> bool:
    """Finish durable receipt -> journal -> hold archive after an interrupted stop.

    No dispatch permission is recreated. The ordinary endpoint-restoration path
    still owns the operation lease and verifies the original endpoint separately.
    """
    if (state is None or state.protection_version != 1
            or state.status not in {"restoring", "restore_failed"}
            or state.entry_id != c.config_entry.entry_id or state.entry_id != hold.entry_id
            or state.collector_pn != c.smartess_collector_pn or state.collector_pn != hold.collector_pn
            or state.cloud_source != hold.source
            or state.route_owner_id != "shadow_learning:" + hold.study_id):
        return False
    store = CompletedLearningStore(Path(c.hass.config.config_dir))
    try:
        receipt = await async_run_owned_worker(store.require_completed_receipt, hold=hold)
    except CloudLearningProtectionRequired:
        return False
    await c._async_save_shadow_learning_session_state(replace(
        state, cloud_commands_possible=False, last_restore_error="",
        updated_at=shadow_learning_session_timestamp(),
    ))
    await async_run_owned_worker(store.retire_completed, hold=hold)
    await c._cloud_learning_mutation_fence.accept_completed_retirement(hold)
    c._protected_learning_run = None
    c._cloud_learning_hold_notified = False
    c._publish_tooling_values(
        cloud_learning_protection_status="not_held", cloud_learning_protection_reason="",
        cloud_learning_protection_release_method="verified_command_completion",
        cloud_learning_protection_completed_command_count=len(receipt["commands"]),
        cloud_learning_completion_blocked_reason="",
        cloud_learning_completion_confirmed_command_count=len(receipt["commands"]),
    )
    _LOGGER.info("Learning protection retired after %s verified command completions; endpoint recovery may proceed",
                 len(receipt["commands"]))
    from homeassistant.components import persistent_notification
    from ...const import DOMAIN
    persistent_notification.async_dismiss(c.hass, f"{DOMAIN}_cloud_learning_hold_{c.config_entry.entry_id}")
    return True


async def async_complete_live_study(c, state, run, completion) -> bool:
    from .learning_isolation import ProtectedLearningRun
    if type(run) is not ProtectedLearningRun or run.hold is None or type(completion) is not CloudCommandCompletion:
        return False
    hold = run.hold
    # A successful retry must NOT release an older indeterminate study's hold.
    if (completion.entry_id, completion.study_id, completion.collector_pn) != (
            hold.entry_id, hold.study_id, hold.collector_pn):
        return False
    if run.route_owner_id != "shadow_learning:" + hold.study_id or c._protected_learning_run is not run:
        return False
    commands = completion.completed_commands()
    if commands is None:
        c._publish_tooling_values(
            cloud_learning_completion_blocked_reason=completion.reason or "completion_not_sealed",
            cloud_learning_completion_confirmed_command_count=len(completion.commands),
        )
        return False
    store = CompletedLearningStore(Path(c.hass.config.config_dir))
    await async_run_owned_worker(store.record_completed, hold=hold, commands=commands)
    return await async_recover_completed_study(c, state, hold)
