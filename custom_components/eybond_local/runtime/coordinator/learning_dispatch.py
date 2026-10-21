"""Durable write-ahead arming and exact-run ownership for cloud test commands."""

from __future__ import annotations

from dataclasses import replace
import asyncio
import logging
from pathlib import Path

from ...collector_identity import validated_collector_pn
from ...connection import collector_endpoint_operation as endpoint_operation
from ...connection.cloud_learning_protection import CloudLearningHold, CloudLearningProtectionRequired
from ...support.owned_worker import async_run_owned_worker, async_finish_owned_task
from ...support.shadow_learning.protection_store import CloudLearningProtectionStore
from ...support.shadow_learning.cloud_dispatch import async_dispatch_cloud_action, async_confirm_owned_action_not_sent
from ...support.shadow_learning.proxy import route_status_has_verified_identity
from ...support.shadow_learning.session import (
    ShadowLearningSessionState,
    shadow_learning_session_is_expired,
    shadow_learning_session_timestamp,
)
from .learning_isolation import ProtectedLearningRun
from .learning_route import learning_listener_port

_LOGGER = logging.getLogger(__name__)


async def async_dispatch_owned_shadow_action(
    coordinator, callable_, *, collector_pn: str, route_owner_id: str, action_kwargs: dict,
):
    """Revalidate the frozen run owner under the stop/unload preparation guard.

    Never adopt an absent lease: an old flow cannot resurrect a stopped study or
    borrow a replacement study's ready route. Cancellation drains the actual HTTP
    thread before releasing preparation, including repeated cancellation/error.
    """

    if (
        type(collector_pn) is not str
        or not collector_pn
        or validated_collector_pn(collector_pn) != collector_pn
        or type(route_owner_id) is not str
        or not route_owner_id
        or route_owner_id != route_owner_id.strip()
        or not callable(callable_)
    ):
        raise CloudLearningProtectionRequired("cloud_learning_hold_dispatch_context_invalid")
    token = endpoint_operation.CollectorEndpointOperationToken(
        entry_id=coordinator.config_entry.entry_id,
        operation_kind=endpoint_operation.OPERATION_SHADOW_LEARNING,
        owner_ref=route_owner_id,
    )
    async with coordinator._async_cloud_tool_preparation():
        state = await coordinator._async_active_shadow_learning_state(require_process=False)
        run = getattr(coordinator, "_protected_learning_run", None)
        if type(run) is not ProtectedLearningRun:
            raise CloudLearningProtectionRequired("cloud_learning_hold_local_proof_missing")
        if run.hold is None:
            await coordinator._async_require_cloud_learning_unheld()
        # All awaited checks precede the final synchronous checks. A new shutdown,
        # owner, PN or route state observed during disk IO is not permission to send.
        def validate_context():
            if (
            getattr(coordinator, "_shutdown_complete", False)
            or type(state) is not ShadowLearningSessionState
            or state.entry_id != token.entry_id
            or state.collector_pn != collector_pn
            or state.route_owner_id != route_owner_id
            or state.protection_version != 1
            or state.cloud_source != run.source
            or (run.entry_id, run.route_owner_id, run.collector_pn, run.local_endpoint)
            != (state.entry_id, state.route_owner_id, state.collector_pn, state.proxy_endpoint)
            or coordinator._protected_learning_run is not run
            or run.local_port != learning_listener_port(coordinator)
            or action_kwargs.get("pn", collector_pn) != collector_pn
            or state.status not in {"ready", "learning"}
            or shadow_learning_session_is_expired(state)
            or coordinator.smartess_collector_pn != collector_pn
            or not endpoint_operation.COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.owns(token)
            or not coordinator._runtime.shadow_learning_route_owned_by(
                entry_id=token.entry_id, owner_id=route_owner_id,
            )
            or not route_status_has_verified_identity(
                coordinator._shadow_learning_route_status(), collector_pn,
            )
            ):
                raise CloudLearningProtectionRequired("cloud_learning_hold_dispatch_context_changed")
        validate_context()
        store = CloudLearningProtectionStore(Path(coordinator.hass.config.config_dir))
        first_action = run.hold is None
        if run.hold is None:
            state = replace(state, cloud_commands_possible=True)
            await coordinator._async_save_shadow_learning_session_state(state)
            validate_context()
            hold = CloudLearningHold(
                entry_id=run.entry_id, study_id=route_owner_id.removeprefix("shadow_learning:"),
                collector_pn=collector_pn, source=run.source, armed_at=shadow_learning_session_timestamp(),
            )
            # No HTTP before file + directory fsync. A cancellation here leaves
            # a conservative hold; neither restart nor another flow can resume.
            await store.async_create(hold=hold)
            validate_context()
            run = replace(run, hold=hold)
            coordinator._protected_learning_run = run
        await async_run_owned_worker(store.require_exact_hold, hold=run.hold)
        validate_context()
        from ...support.shadow_learning.completion import CloudCommandCompletion
        get_completion = getattr(coordinator._runtime, "shadow_learning_completion", None)
        completion = get_completion() if callable(get_completion) else None
        if (type(completion) is not CloudCommandCompletion
                or (completion.entry_id, completion.study_id, completion.collector_pn)
                != (run.entry_id, run.route_owner_id.removeprefix("shadow_learning:"), collector_pn)):
            completion = None
        if completion is not None:
            completion.begin(run.source, action_kwargs)
        try:
            envelope = await async_dispatch_cloud_action(callable_, **action_kwargs)
            if completion is not None:
                completion.finish(envelope)
            return envelope
        except BaseException as exc:
            if completion is not None:
                # A provider may report a qualified terminal device refusal as
                # a typed exception. All other failures still fail completion;
                # preserve/re-raise the original outcome for the orchestrator.
                completion.finish(exc)
            # Only these clients use this exact operation-owned HTTP resource.
            # Generic exceptions/ValueCloud callbacks have no no-send evidence.
            from .learning_recovery import async_complete_no_request_recovery
            if first_action:
                async def finalize_no_request():
                    if not await async_confirm_owned_action_not_sent(callable_, action_kwargs):
                        return
                    # Joined worker + zero lifetime request attempts, not parsed
                    # exception text. Do not repeat the failed command.
                    await async_run_owned_worker(store.record_no_request, hold=run.hold)
                    if not await async_complete_no_request_recovery(coordinator, state, run.hold):
                        raise CloudLearningProtectionRequired("cloud_learning_no_request_recovery_mismatch")
                    _LOGGER.info("Cloud learning stopped before its first HTTP request; endpoint recovery is permitted")
                try:
                    await async_finish_owned_task(asyncio.create_task(finalize_no_request()))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _LOGGER.warning("Cloud learning no-request recovery incomplete: %s", type(exc).__name__)
            raise
