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


async def async_restore_after_uncertain_study(c, state, token):
    """A delivery receipt is useful evidence, not a prerequisite for returning service."""
    from ...connection import collector_endpoint_operation as operations
    from ...support.shadow_learning.protection_store import CloudLearningProtectionStore
    from ...support.shadow_learning.session import persist_stopped_learning_restore
    if (state is None or token is None or not operations.COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.owns(token)
            or token.operation_kind != operations.OPERATION_SHADOW_LEARNING
            or token.entry_id != state.entry_id or token.owner_ref != state.route_owner_id
            or state.entry_id != c.config_entry.entry_id or state.collector_pn != c.smartess_collector_pn
            or c._shadow_learning_process_running()):
        return False
    root = Path(c.hass.config.config_dir)
    store = CloudLearningProtectionStore(root)
    hold = await async_run_owned_worker(store.load, collector_pn=state.collector_pn)
    if not operations.COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.owns(token):
        return False
    try:
        persisted = await async_run_owned_worker(persist_stopped_learning_restore, config_dir=root, state=state, hold=hold)
    finally:
        c._shadow_learning_session_state_loaded = False
    if not persisted or not operations.COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.owns(token):
        return False
    await c._cloud_learning_mutation_fence.retire_after_stopped_study(hold)
    c._cloud_learning_hold_notified = False
    c._publish_tooling_values(cloud_learning_protection_status="not_held", cloud_learning_protection_reason="",
                             cloud_learning_protection_release_method="stopped_with_unconfirmed_delivery")
    from homeassistant.components import persistent_notification
    from ...const import DOMAIN
    persistent_notification.async_dismiss(c.hass, f"{DOMAIN}_cloud_learning_hold_{c.config_entry.entry_id}")
    return True


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
    failed_sends = sum(row["terminal"]["kind"] == "dispatch_failed" for row in receipt["commands"])
    c._publish_tooling_values(
        cloud_learning_protection_status="not_held", cloud_learning_protection_reason="",
        cloud_learning_protection_release_method=(
            "cloud_send_failed_after_observation" if failed_sends else "verified_command_completion"),
        cloud_learning_protection_completed_command_count=len(receipt["commands"]),
        cloud_learning_completion_blocked_reason="",
        cloud_learning_completion_confirmed_command_count=len(receipt["commands"]) - failed_sends,
        cloud_learning_completion_failed_send_count=failed_sends,
    )
    _LOGGER.info("Learning protection retired: %s commands accounted for, %s cloud send failures; restoring endpoint",
                 len(receipt["commands"]), failed_sends)
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
