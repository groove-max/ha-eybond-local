"""Consume explicit historical review only after owned route/HTTP shutdown."""
from pathlib import Path

from ...connection import collector_endpoint_operation as operations
from ...support.owned_worker import async_run_owned_worker
from ...support.shadow_learning.reviewed_recovery import ReviewedLearningRecoveryStore


async def async_recover_reviewed_history(c, state, token) -> bool:
    if state is None:
        return False
    store = ReviewedLearningRecoveryStore(Path(c.hass.config.config_dir))
    if not await async_run_owned_worker(store.receipt_present, state=state):
        return False
    if (token is None or not operations.COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.owns(token)
            or token.entry_id != state.entry_id or state.entry_id != c.config_entry.entry_id
            or token.owner_ref != state.route_owner_id
            or token.operation_kind != operations.OPERATION_SHADOW_LEARNING
            or state.collector_pn != c.smartess_collector_pn
            or c._shadow_learning_process_running()):
        raise RuntimeError("reviewed_recovery_route_not_stopped_or_owned")
    try:
        corrected, plan = await async_run_owned_worker(store.apply, expected_state=state)
    finally:
        # Rename/fsync/cancellation can finish a durable prefix. Never overwrite
        # that prefix from an old coordinator cache in the stop error path.
        c._shadow_learning_session_state_loaded = False
    if not operations.COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.owns(token):
        raise RuntimeError("reviewed_recovery_owner_changed")
    await c._cloud_learning_mutation_fence.accept_reviewed_retirement(corrected)
    c._cloud_learning_hold_notified = False
    count = sum(item["reviewed_commands"] for item in plan["evidence"])
    c._publish_tooling_values(
        cloud_learning_protection_status="not_held", cloud_learning_protection_reason="",
        cloud_learning_protection_release_method="maintainer_reviewed_terminal_history",
        cloud_learning_protection_reviewed_run_count=len(plan["history"]),
        cloud_learning_protection_reviewed_command_count=count,
    )
    from homeassistant.components import persistent_notification
    from ...const import DOMAIN
    persistent_notification.async_dismiss(c.hass, f"{DOMAIN}_cloud_learning_hold_{c.config_entry.entry_id}")
    return True
