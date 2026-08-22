"""Coordinator-lifetime collection of repeated local register evidence.

The lifecycle in this module owns only a retained asyncio task and typed local
wire evidence.  It knows nothing about cloud APIs, history correlation,
entities, overlays, or writes.  A caller may therefore start a long read-only
observation from a short-lived UI flow without making that flow the task owner.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..drivers.local_register_evidence import LocalRegisterSnapshot
from ..drivers.local_register_series import (
    LocalRegisterSeriesPlan,
    LocalRegisterSnapshotSeries,
    async_capture_local_register_series,
)


LOCAL_REGISTER_COLLECTION_AUTHORITY = "coordinator_lifetime_read_only_collection"
LOCAL_REGISTER_COLLECTION_STATE_IDLE = "idle"
LOCAL_REGISTER_COLLECTION_STATE_RUNNING = "running"
LOCAL_REGISTER_COLLECTION_STATE_COMPLETE = "complete"
LOCAL_REGISTER_COLLECTION_STATE_FAILED = "failed"
LOCAL_REGISTER_COLLECTION_STATE_CANCELLED = "cancelled"

LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_UNAVAILABLE = "snapshot_unavailable"
LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_INVALID = "snapshot_invalid"
LOCAL_REGISTER_COLLECTION_FAILURE_IDENTITY_CHANGED = "identity_changed"
LOCAL_REGISTER_COLLECTION_FAILURE_DRIVER_CHANGED = "driver_changed"
LOCAL_REGISTER_COLLECTION_FAILURE_CAPTURE_FAILED = "capture_failed"

_STATES = frozenset(
    {
        LOCAL_REGISTER_COLLECTION_STATE_IDLE,
        LOCAL_REGISTER_COLLECTION_STATE_RUNNING,
        LOCAL_REGISTER_COLLECTION_STATE_COMPLETE,
        LOCAL_REGISTER_COLLECTION_STATE_FAILED,
        LOCAL_REGISTER_COLLECTION_STATE_CANCELLED,
    }
)
_FAILURE_REASONS = frozenset(
    {
        "",
        LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_UNAVAILABLE,
        LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_INVALID,
        LOCAL_REGISTER_COLLECTION_FAILURE_IDENTITY_CHANGED,
        LOCAL_REGISTER_COLLECTION_FAILURE_DRIVER_CHANGED,
        LOCAL_REGISTER_COLLECTION_FAILURE_CAPTURE_FAILED,
    }
)


def _aware_timestamp(value: object, reason: str) -> datetime:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except ValueError as exc:
        raise ValueError(reason) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(reason)
    return parsed


@dataclass(frozen=True, slots=True)
class LocalRegisterCollectionStatus:
    """One strict public view of a retained local-series task."""

    state: str
    plan: LocalRegisterSeriesPlan | None
    started_at: str
    completed_at: str
    completed_sample_count: int
    failure_reason: str

    def __post_init__(self) -> None:
        if type(self.state) is not str:
            raise TypeError("local_register_collection_state_invalid")
        if self.state not in _STATES:
            raise ValueError("local_register_collection_state_invalid")
        if self.plan is not None and type(self.plan) is not LocalRegisterSeriesPlan:
            raise TypeError("local_register_collection_plan_invalid")
        if type(self.completed_sample_count) is not int:
            raise TypeError("local_register_collection_sample_count_invalid")
        if type(self.failure_reason) is not str:
            raise TypeError("local_register_collection_failure_invalid")
        if self.failure_reason not in _FAILURE_REASONS:
            raise ValueError("local_register_collection_failure_invalid")

        if self.state == LOCAL_REGISTER_COLLECTION_STATE_IDLE:
            if (
                self.plan is not None
                or self.started_at
                or self.completed_at
                or self.completed_sample_count != 0
                or self.failure_reason
            ):
                raise ValueError("local_register_collection_idle_shape_invalid")
            return

        if self.plan is None:
            raise ValueError("local_register_collection_plan_missing")
        started = _aware_timestamp(
            self.started_at,
            "local_register_collection_started_at_invalid",
        )
        if not 0 <= self.completed_sample_count <= self.plan.sample_count:
            raise ValueError("local_register_collection_sample_count_invalid")

        if self.state == LOCAL_REGISTER_COLLECTION_STATE_RUNNING:
            if self.completed_at or self.failure_reason:
                raise ValueError("local_register_collection_running_shape_invalid")
            return

        completed = _aware_timestamp(
            self.completed_at,
            "local_register_collection_completed_at_invalid",
        )
        if completed < started:
            raise ValueError("local_register_collection_time_order_invalid")
        if self.state == LOCAL_REGISTER_COLLECTION_STATE_COMPLETE:
            if (
                self.completed_sample_count != self.plan.sample_count
                or self.failure_reason
            ):
                raise ValueError("local_register_collection_complete_shape_invalid")
            return
        if self.state == LOCAL_REGISTER_COLLECTION_STATE_FAILED:
            if not self.failure_reason:
                raise ValueError("local_register_collection_failed_shape_invalid")
            return
        if self.failure_reason:
            raise ValueError("local_register_collection_cancelled_shape_invalid")

    @classmethod
    def idle(cls) -> "LocalRegisterCollectionStatus":
        return cls(
            state=LOCAL_REGISTER_COLLECTION_STATE_IDLE,
            plan=None,
            started_at="",
            completed_at="",
            completed_sample_count=0,
            failure_reason="",
        )

    @property
    def active(self) -> bool:
        return self.state == LOCAL_REGISTER_COLLECTION_STATE_RUNNING

    @property
    def series_available(self) -> bool:
        return self.state == LOCAL_REGISTER_COLLECTION_STATE_COMPLETE

    def to_record(self) -> dict[str, Any]:
        return {
            "authority": LOCAL_REGISTER_COLLECTION_AUTHORITY,
            "read_only": True,
            "cloud_mapping_proven": False,
            "activation_allowed": False,
            "state": self.state,
            "plan": self.plan.to_record() if self.plan is not None else None,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "completed_sample_count": self.completed_sample_count,
            "failure_reason": self.failure_reason,
            "series_available": self.series_available,
        }


SnapshotCapture = Callable[[], Coroutine[Any, Any, LocalRegisterSnapshot | None]]
TaskFactory = Callable[[Coroutine[Any, Any, None]], asyncio.Task[None]]
Clock = Callable[[], datetime]
UpdateObserver = Callable[
    [LocalRegisterCollectionStatus, LocalRegisterSnapshotSeries | None],
    None,
]


class LocalRegisterCollectionManager:
    """Retain exactly one cancellable repeated local-read task."""

    def __init__(
        self,
        *,
        capture_snapshot: SnapshotCapture,
        create_task: TaskFactory = asyncio.create_task,
        clock: Clock = lambda: datetime.now(timezone.utc),
        on_update: UpdateObserver | None = None,
    ) -> None:
        if not callable(capture_snapshot) or not callable(create_task):
            raise TypeError("local_register_collection_callback_invalid")
        if not callable(clock) or (on_update is not None and not callable(on_update)):
            raise TypeError("local_register_collection_callback_invalid")
        self._capture_snapshot = capture_snapshot
        self._create_task = create_task
        self._clock = clock
        self._on_update = on_update
        self._status = LocalRegisterCollectionStatus.idle()
        self._latest_series: LocalRegisterSnapshotSeries | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def status(self) -> LocalRegisterCollectionStatus:
        return self._status

    @property
    def latest_series(self) -> LocalRegisterSnapshotSeries | None:
        return self._latest_series

    def start(self, plan: LocalRegisterSeriesPlan) -> LocalRegisterCollectionStatus:
        """Start one retained task and return immediately."""

        if type(plan) is not LocalRegisterSeriesPlan:
            raise TypeError("local_register_collection_plan_invalid")
        if self._task is not None and not self._task.done():
            raise RuntimeError("local_register_collection_busy")

        started_at = self._timestamp()
        prior_status = self._status
        prior_series = self._latest_series
        self._latest_series = None
        self._set_status(
            LocalRegisterCollectionStatus(
                state=LOCAL_REGISTER_COLLECTION_STATE_RUNNING,
                plan=plan,
                started_at=started_at,
                completed_at="",
                completed_sample_count=0,
                failure_reason="",
            )
        )
        coroutine = self._async_run(plan, started_at)
        try:
            task = self._create_task(coroutine)
        except BaseException:
            coroutine.close()
            self._status = prior_status
            self._latest_series = prior_series
            self._notify()
            raise
        if not isinstance(task, asyncio.Task):
            task.cancel()
            self._status = prior_status
            self._latest_series = prior_series
            self._notify()
            raise TypeError("local_register_collection_task_invalid")
        self._task = task
        task.add_done_callback(self._task_done)
        return self._status

    async def async_cancel(self) -> LocalRegisterCollectionStatus:
        """Cancel and await the retained task; never leave it detached."""

        task = self._task
        if task is None:
            return self._status
        if task.done():
            self._task_done(task)
            return self._status
        task.cancel()
        pending_cancel: asyncio.CancelledError | None = None
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as exc:
                if task.done():
                    break
                pending_cancel = exc
                task.cancel()
        self._task_done(task)
        if pending_cancel is not None:
            raise pending_cancel
        return self._status

    async def async_shutdown(self) -> None:
        """Complete cancellation before the owning runtime transport stops."""

        await self.async_cancel()

    async def _async_run(
        self,
        plan: LocalRegisterSeriesPlan,
        started_at: str,
    ) -> None:
        async def _capture() -> LocalRegisterSnapshot:
            candidate = await self._capture_snapshot()
            if candidate is None:
                raise RuntimeError(
                    LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_UNAVAILABLE
                )
            if type(candidate) is not LocalRegisterSnapshot:
                raise TypeError("local_register_series_snapshot_invalid")
            completed = self._status.completed_sample_count + 1
            self._set_status(
                LocalRegisterCollectionStatus(
                    state=LOCAL_REGISTER_COLLECTION_STATE_RUNNING,
                    plan=plan,
                    started_at=started_at,
                    completed_at="",
                    completed_sample_count=completed,
                    failure_reason="",
                )
            )
            return candidate

        try:
            series = await async_capture_local_register_series(
                capture_snapshot=_capture,
                sample_count=plan.sample_count,
                sample_interval_seconds=plan.sample_interval_seconds,
            )
        except asyncio.CancelledError:
            self._set_status(
                LocalRegisterCollectionStatus(
                    state=LOCAL_REGISTER_COLLECTION_STATE_CANCELLED,
                    plan=plan,
                    started_at=started_at,
                    completed_at=self._timestamp(),
                    completed_sample_count=self._status.completed_sample_count,
                    failure_reason="",
                )
            )
            raise
        except Exception as exc:
            self._set_status(
                LocalRegisterCollectionStatus(
                    state=LOCAL_REGISTER_COLLECTION_STATE_FAILED,
                    plan=plan,
                    started_at=started_at,
                    completed_at=self._timestamp(),
                    completed_sample_count=self._status.completed_sample_count,
                    failure_reason=_failure_reason(exc),
                )
            )
            return

        if type(series) is not LocalRegisterSnapshotSeries:
            self._set_status(
                LocalRegisterCollectionStatus(
                    state=LOCAL_REGISTER_COLLECTION_STATE_FAILED,
                    plan=plan,
                    started_at=started_at,
                    completed_at=self._timestamp(),
                    completed_sample_count=self._status.completed_sample_count,
                    failure_reason=LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_INVALID,
                )
            )
            return
        self._latest_series = series
        self._set_status(
            LocalRegisterCollectionStatus(
                state=LOCAL_REGISTER_COLLECTION_STATE_COMPLETE,
                plan=plan,
                started_at=started_at,
                completed_at=self._timestamp(),
                completed_sample_count=plan.sample_count,
                failure_reason="",
            )
        )

    def _task_done(self, task: asyncio.Task[None]) -> None:
        # A task cancelled before its coroutine first runs never reaches the
        # CancelledError handler above.  Close that scheduler-sized race here.
        if self._status.state != LOCAL_REGISTER_COLLECTION_STATE_RUNNING:
            return
        if task.cancelled():
            state = LOCAL_REGISTER_COLLECTION_STATE_CANCELLED
            reason = ""
        else:
            state = LOCAL_REGISTER_COLLECTION_STATE_FAILED
            reason = LOCAL_REGISTER_COLLECTION_FAILURE_CAPTURE_FAILED
        self._set_status(
            LocalRegisterCollectionStatus(
                state=state,
                plan=self._status.plan,
                started_at=self._status.started_at,
                completed_at=self._timestamp(),
                completed_sample_count=self._status.completed_sample_count,
                failure_reason=reason,
            )
        )

    def _timestamp(self) -> str:
        value = self._clock()
        if type(value) is not datetime:
            raise TypeError("local_register_collection_clock_invalid")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("local_register_collection_clock_invalid")
        return value.isoformat()

    def _set_status(self, status: LocalRegisterCollectionStatus) -> None:
        if type(status) is not LocalRegisterCollectionStatus:
            raise TypeError("local_register_collection_status_invalid")
        self._status = status
        self._notify()

    def _notify(self) -> None:
        if self._on_update is None:
            return
        try:
            self._on_update(self._status, self._latest_series)
        except Exception:
            # Presentation/support publication is supplemental.  It may never
            # take ownership of, cancel, or change the evidence task verdict.
            return


def _failure_reason(exc: Exception) -> str:
    reason = str(exc)
    if reason == LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_UNAVAILABLE:
        return reason
    if reason == "local_register_series_identity_changed":
        return LOCAL_REGISTER_COLLECTION_FAILURE_IDENTITY_CHANGED
    if reason == "local_register_series_driver_changed":
        return LOCAL_REGISTER_COLLECTION_FAILURE_DRIVER_CHANGED
    if reason in {
        "local_register_series_snapshot_invalid",
        "local_register_series_snapshots_overlap",
    }:
        return LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_INVALID
    return LOCAL_REGISTER_COLLECTION_FAILURE_CAPTURE_FAILED


__all__ = [
    "LOCAL_REGISTER_COLLECTION_AUTHORITY",
    "LOCAL_REGISTER_COLLECTION_FAILURE_CAPTURE_FAILED",
    "LOCAL_REGISTER_COLLECTION_FAILURE_DRIVER_CHANGED",
    "LOCAL_REGISTER_COLLECTION_FAILURE_IDENTITY_CHANGED",
    "LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_INVALID",
    "LOCAL_REGISTER_COLLECTION_FAILURE_SNAPSHOT_UNAVAILABLE",
    "LOCAL_REGISTER_COLLECTION_STATE_CANCELLED",
    "LOCAL_REGISTER_COLLECTION_STATE_COMPLETE",
    "LOCAL_REGISTER_COLLECTION_STATE_FAILED",
    "LOCAL_REGISTER_COLLECTION_STATE_IDLE",
    "LOCAL_REGISTER_COLLECTION_STATE_RUNNING",
    "LocalRegisterCollectionManager",
    "LocalRegisterCollectionStatus",
]
