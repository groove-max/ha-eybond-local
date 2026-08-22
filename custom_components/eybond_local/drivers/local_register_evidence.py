"""Typed live-wire Modbus register observations owned by inverter drivers.

Drivers define the exact route/function/ranges.  The shared executor performs
bounded reads and timestamps successful blocks.  This is raw local evidence,
not decoded telemetry and not a cloud-to-register binding.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..collector_identity import validated_collector_pn
from ..models import ProbeTarget
from ..payload.modbus import ModbusSession


LOCAL_REGISTER_EVIDENCE_SCHEMA_VERSION = 1
LOCAL_REGISTER_EVIDENCE_AUTHORITY = "live_local_wire_observation"
LOCAL_REGISTER_EVIDENCE_SOURCE = "driver_modbus_read"

_MAX_BLOCKS = 256
_MAX_REGISTER_COUNT = 125


def local_register_evidence_timestamp() -> str:
    """Return one aware UTC timestamp for a local wire observation."""

    return datetime.now(timezone.utc).isoformat()


def _aware_datetime(value: object, reason: str) -> datetime:
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


def _bounded_int(
    value: object,
    *,
    minimum: int,
    maximum: int,
    reason: str,
) -> int:
    if type(value) is not int:
        raise TypeError(reason)
    if not minimum <= value <= maximum:
        raise ValueError(reason)
    return value


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


def _required_collector_pn(value: object) -> str:
    if type(value) is not str:
        raise TypeError("local_register_collector_pn_invalid")
    if validated_collector_pn(value) != value:
        raise ValueError("local_register_collector_pn_invalid")
    return value


@dataclass(frozen=True, slots=True)
class LocalRegisterReadPlan:
    """One exact live Modbus block read including tunnel provenance."""

    devcode: int
    collector_addr: int
    device_addr: int
    function: int
    start: int
    count: int

    def __post_init__(self) -> None:
        _bounded_int(
            self.devcode,
            minimum=0,
            maximum=0xFFFF,
            reason="local_register_devcode_invalid",
        )
        _bounded_int(
            self.collector_addr,
            minimum=0,
            maximum=0xFF,
            reason="local_register_collector_addr_invalid",
        )
        _bounded_int(
            self.device_addr,
            minimum=0,
            maximum=0xFF,
            reason="local_register_device_addr_invalid",
        )
        if type(self.function) is not int:
            raise TypeError("local_register_function_invalid")
        if self.function not in {3, 4}:
            raise ValueError("local_register_function_invalid")
        _bounded_int(
            self.start,
            minimum=0,
            maximum=0xFFFF,
            reason="local_register_start_invalid",
        )
        _bounded_int(
            self.count,
            minimum=1,
            maximum=_MAX_REGISTER_COUNT,
            reason="local_register_count_invalid",
        )
        if self.start + self.count > 0x10000:
            raise ValueError("local_register_range_invalid")

    @classmethod
    def for_target(
        cls,
        target: ProbeTarget,
        *,
        function: int,
        start: int,
        count: int,
    ) -> "LocalRegisterReadPlan":
        if type(target) is not ProbeTarget:
            raise TypeError("local_register_probe_target_invalid")
        return cls(
            devcode=target.devcode,
            collector_addr=target.collector_addr,
            device_addr=target.device_addr,
            function=function,
            start=start,
            count=count,
        )

    @property
    def probe_target(self) -> ProbeTarget:
        return ProbeTarget(
            devcode=self.devcode,
            collector_addr=self.collector_addr,
            device_addr=self.device_addr,
        )

    def to_record(self) -> dict[str, int]:
        return {
            "devcode": self.devcode,
            "collector_addr": self.collector_addr,
            "device_addr": self.device_addr,
            "function": self.function,
            "start": self.start,
            "count": self.count,
        }

    @classmethod
    def from_record(cls, record: object) -> "LocalRegisterReadPlan | None":
        if type(record) is not dict or set(record) != {
            "devcode",
            "collector_addr",
            "device_addr",
            "function",
            "start",
            "count",
        }:
            return None
        try:
            return cls(
                devcode=record["devcode"],
                collector_addr=record["collector_addr"],
                device_addr=record["device_addr"],
                function=record["function"],
                start=record["start"],
                count=record["count"],
            )
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True, slots=True)
class LocalRegisterBlockObservation:
    """One successful raw block read and its completion timestamp."""

    plan: LocalRegisterReadPlan
    observed_at: str
    values: tuple[int, ...]

    def __post_init__(self) -> None:
        if type(self.plan) is not LocalRegisterReadPlan:
            raise TypeError("local_register_observation_plan_invalid")
        _aware_datetime(
            self.observed_at,
            "local_register_observed_at_invalid",
        )
        if type(self.values) is not tuple:
            raise TypeError("local_register_values_invalid")
        if len(self.values) != self.plan.count:
            raise ValueError("local_register_value_count_mismatch")
        for value in self.values:
            _bounded_int(
                value,
                minimum=0,
                maximum=0xFFFF,
                reason="local_register_value_invalid",
            )

    def to_record(self) -> dict[str, Any]:
        return {
            "plan": self.plan.to_record(),
            "observed_at": self.observed_at,
            "values": list(self.values),
        }

    @classmethod
    def from_record(
        cls,
        record: object,
    ) -> "LocalRegisterBlockObservation | None":
        if type(record) is not dict or set(record) != {
            "plan",
            "observed_at",
            "values",
        }:
            return None
        plan = LocalRegisterReadPlan.from_record(record["plan"])
        if plan is None or type(record["values"]) is not list:
            return None
        try:
            return cls(
                plan=plan,
                observed_at=record["observed_at"],
                values=tuple(record["values"]),
            )
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True, slots=True)
class LocalRegisterSnapshot:
    """Bounded set of real local Modbus reads for one collector identity."""

    collector_pn: str
    driver_key: str
    started_at: str
    completed_at: str
    planned_block_count: int
    failed_block_count: int
    blocks: tuple[LocalRegisterBlockObservation, ...]

    def __post_init__(self) -> None:
        _required_collector_pn(self.collector_pn)
        _required_token(self.driver_key, "local_register_driver_key_invalid")
        started = _aware_datetime(
            self.started_at,
            "local_register_started_at_invalid",
        )
        completed = _aware_datetime(
            self.completed_at,
            "local_register_completed_at_invalid",
        )
        if completed < started:
            raise ValueError("local_register_snapshot_time_order_invalid")
        _bounded_int(
            self.planned_block_count,
            minimum=1,
            maximum=_MAX_BLOCKS,
            reason="local_register_planned_count_invalid",
        )
        _bounded_int(
            self.failed_block_count,
            minimum=0,
            maximum=_MAX_BLOCKS,
            reason="local_register_failed_count_invalid",
        )
        if type(self.blocks) is not tuple:
            raise TypeError("local_register_blocks_invalid")
        if len(self.blocks) + self.failed_block_count != self.planned_block_count:
            raise ValueError("local_register_snapshot_count_mismatch")
        seen: set[LocalRegisterReadPlan] = set()
        for block in self.blocks:
            if type(block) is not LocalRegisterBlockObservation:
                raise TypeError("local_register_block_invalid")
            observed = _aware_datetime(
                block.observed_at,
                "local_register_observed_at_invalid",
            )
            if not started <= observed <= completed:
                raise ValueError("local_register_block_time_outside_snapshot")
            if block.plan in seen:
                raise ValueError("local_register_plan_duplicate")
            seen.add(block.plan)

    @property
    def observed_register_count(self) -> int:
        return sum(len(block.values) for block in self.blocks)

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": LOCAL_REGISTER_EVIDENCE_SCHEMA_VERSION,
            "authority": LOCAL_REGISTER_EVIDENCE_AUTHORITY,
            "source": LOCAL_REGISTER_EVIDENCE_SOURCE,
            "cloud_mapping_proven": False,
            "collector_pn": self.collector_pn,
            "driver_key": self.driver_key,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "planned_block_count": self.planned_block_count,
            "failed_block_count": self.failed_block_count,
            "observed_register_count": self.observed_register_count,
            "blocks": [block.to_record() for block in self.blocks],
        }

    @classmethod
    def from_record(cls, record: object) -> "LocalRegisterSnapshot | None":
        if type(record) is not dict or set(record) != {
            "schema_version",
            "authority",
            "source",
            "cloud_mapping_proven",
            "collector_pn",
            "driver_key",
            "started_at",
            "completed_at",
            "planned_block_count",
            "failed_block_count",
            "observed_register_count",
            "blocks",
        }:
            return None
        if (
            type(record["schema_version"]) is not int
            or record["schema_version"] != LOCAL_REGISTER_EVIDENCE_SCHEMA_VERSION
            or type(record["authority"]) is not str
            or record["authority"] != LOCAL_REGISTER_EVIDENCE_AUTHORITY
            or type(record["source"]) is not str
            or record["source"] != LOCAL_REGISTER_EVIDENCE_SOURCE
            or record["cloud_mapping_proven"] is not False
            or type(record["blocks"]) is not list
        ):
            return None
        blocks: list[LocalRegisterBlockObservation] = []
        for raw_block in record["blocks"]:
            block = LocalRegisterBlockObservation.from_record(raw_block)
            if block is None:
                return None
            blocks.append(block)
        try:
            snapshot = cls(
                collector_pn=record["collector_pn"],
                driver_key=record["driver_key"],
                started_at=record["started_at"],
                completed_at=record["completed_at"],
                planned_block_count=record["planned_block_count"],
                failed_block_count=record["failed_block_count"],
                blocks=tuple(blocks),
            )
        except (TypeError, ValueError):
            return None
        if (
            type(record["observed_register_count"]) is not int
            or record["observed_register_count"] != snapshot.observed_register_count
        ):
            return None
        return snapshot


RegisterBlockReader = Callable[[LocalRegisterReadPlan], Awaitable[list[int]]]
TimestampProvider = Callable[[], str]


async def async_capture_local_register_snapshot(
    *,
    collector_pn: str,
    driver_key: str,
    plans: tuple[LocalRegisterReadPlan, ...],
    reader: RegisterBlockReader,
    timestamp_provider: TimestampProvider = local_register_evidence_timestamp,
) -> LocalRegisterSnapshot:
    """Execute one bounded driver-owned read plan and timestamp successes."""

    _required_collector_pn(collector_pn)
    _required_token(driver_key, "local_register_driver_key_invalid")
    if type(plans) is not tuple or not plans or len(plans) > _MAX_BLOCKS:
        raise ValueError("local_register_plans_invalid")
    if any(type(plan) is not LocalRegisterReadPlan for plan in plans):
        raise TypeError("local_register_plan_invalid")
    if len(set(plans)) != len(plans):
        raise ValueError("local_register_plan_duplicate")
    if not callable(reader) or not callable(timestamp_provider):
        raise TypeError("local_register_capture_callback_invalid")

    started_at = timestamp_provider()
    _aware_datetime(started_at, "local_register_started_at_invalid")
    blocks: list[LocalRegisterBlockObservation] = []
    failed_count = 0
    for plan in plans:
        try:
            values = await reader(plan)
        except asyncio.CancelledError:
            raise
        except Exception:
            failed_count += 1
            continue
        if type(values) is not list:
            raise TypeError("local_register_reader_values_invalid")
        block = LocalRegisterBlockObservation(
            plan=plan,
            observed_at=timestamp_provider(),
            values=tuple(values),
        )
        blocks.append(block)
    completed_at = timestamp_provider()
    return LocalRegisterSnapshot(
        collector_pn=collector_pn,
        driver_key=driver_key,
        started_at=started_at,
        completed_at=completed_at,
        planned_block_count=len(plans),
        failed_block_count=failed_count,
        blocks=tuple(blocks),
    )


async def async_read_modbus_plan(
    plan: LocalRegisterReadPlan,
    *,
    session_factory: Callable[[ProbeTarget], ModbusSession],
) -> list[int]:
    """Read one exact plan through a driver-owned Modbus session factory."""

    if type(plan) is not LocalRegisterReadPlan:
        raise TypeError("local_register_plan_invalid")
    if not callable(session_factory):
        raise TypeError("local_register_session_factory_invalid")
    session = session_factory(plan.probe_target)
    if type(session) is not ModbusSession:
        raise TypeError("local_register_session_invalid")
    return await session.read_registers(
        plan.start,
        plan.count,
        function=plan.function,
    )


async def async_capture_modbus_snapshot(
    *,
    collector_pn: str,
    driver_key: str,
    plans: tuple[LocalRegisterReadPlan, ...],
    session_factory: Callable[[ProbeTarget], ModbusSession],
) -> LocalRegisterSnapshot:
    """Capture one driver plan through its exact session/route policy."""

    if not callable(session_factory):
        raise TypeError("local_register_session_factory_invalid")

    async def _read(plan: LocalRegisterReadPlan) -> list[int]:
        return await async_read_modbus_plan(
            plan,
            session_factory=session_factory,
        )

    return await async_capture_local_register_snapshot(
        collector_pn=collector_pn,
        driver_key=driver_key,
        plans=plans,
        reader=_read,
    )


__all__ = [
    "LOCAL_REGISTER_EVIDENCE_AUTHORITY",
    "LOCAL_REGISTER_EVIDENCE_SCHEMA_VERSION",
    "LOCAL_REGISTER_EVIDENCE_SOURCE",
    "LocalRegisterBlockObservation",
    "LocalRegisterReadPlan",
    "LocalRegisterSnapshot",
    "async_capture_local_register_snapshot",
    "async_capture_modbus_snapshot",
    "async_read_modbus_plan",
    "local_register_evidence_timestamp",
]
