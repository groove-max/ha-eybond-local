"""Trusted DESSMonitor device-time basis from one exact cloud identity.

History endpoints return naive device-local timestamps. The official read-only
``queryDeviceInfo`` action separately returns the device timezone as seconds
relative to UTC, together with PN/SN/devcode/devaddr. This module accepts that
offset only when exactly one response row matches the already-resolved cloud
identity. It does not fetch history or correlate local telemetry.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

from .collector_identity import pn_is_same_identity
from .dessmonitor_cloud import (
    DEFAULT_BASE_URL,
    DEFAULT_LANGUAGE,
    DEFAULT_TIMEOUT,
    DessMonitorCloudError,
    DessMonitorDeviceIdentity,
    DessMonitorSession,
    fetch_signed_action,
)


DESSMONITOR_TIME_BASIS_SCHEMA_VERSION = 1
DESSMONITOR_TIME_BASIS_AUTHORITY = "provider_exact_device_timezone_offset"
DESSMONITOR_TIME_BASIS_SOURCE_ACTION = "queryDeviceInfo"

_MIN_TIMEZONE_OFFSET_SECONDS = -12 * 60 * 60
_MAX_TIMEZONE_OFFSET_SECONDS = 14 * 60 * 60


def _offset_seconds(value: object) -> int:
    if type(value) is not int:
        raise TypeError("dessmonitor_timezone_offset_invalid")
    if value < _MIN_TIMEZONE_OFFSET_SECONDS or value > _MAX_TIMEZONE_OFFSET_SECONDS:
        raise ValueError("dessmonitor_timezone_offset_invalid")
    if value % 60:
        raise ValueError("dessmonitor_timezone_offset_invalid")
    return value


def _device_local_timestamp(value: object) -> tuple[str, datetime]:
    if type(value) is not str:
        raise TypeError("dessmonitor_device_local_timestamp_invalid")
    if not value or value != value.strip():
        raise ValueError("dessmonitor_device_local_timestamp_invalid")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError("dessmonitor_device_local_timestamp_invalid") from exc
    if parsed.strftime("%Y-%m-%d %H:%M:%S") != value:
        raise ValueError("dessmonitor_device_local_timestamp_invalid")
    return value, parsed


@dataclass(frozen=True, slots=True)
class DessMonitorDeviceTimeBasis:
    """One exact provider-owned timezone offset for one cloud identity."""

    identity: DessMonitorDeviceIdentity
    offset_seconds: int

    def __post_init__(self) -> None:
        if type(self.identity) is not DessMonitorDeviceIdentity:
            raise TypeError("dessmonitor_time_basis_identity_invalid")
        _offset_seconds(self.offset_seconds)

    def to_utc_timestamp(self, device_local_timestamp: str) -> str:
        """Convert only with the exact provider-observed offset."""

        _, parsed = _device_local_timestamp(device_local_timestamp)
        device_zone = timezone(timedelta(seconds=self.offset_seconds))
        return parsed.replace(tzinfo=device_zone).astimezone(timezone.utc).isoformat()

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": DESSMONITOR_TIME_BASIS_SCHEMA_VERSION,
            "authority": DESSMONITOR_TIME_BASIS_AUTHORITY,
            "provider_id": "smartess",
            "source_id": "dessmonitor",
            "source_action": DESSMONITOR_TIME_BASIS_SOURCE_ACTION,
            "identity": self.identity.to_record(),
            "offset_seconds": self.offset_seconds,
        }

    @classmethod
    def from_record(cls, record: object) -> "DessMonitorDeviceTimeBasis | None":
        expected_keys = {
            "schema_version",
            "authority",
            "provider_id",
            "source_id",
            "source_action",
            "identity",
            "offset_seconds",
        }
        if type(record) is not dict or set(record) != expected_keys:
            return None
        identity = record.get("identity")
        if type(identity) is not dict or set(identity) != {
            "pn",
            "sn",
            "devcode",
            "devaddr",
        }:
            return None
        for key, expected in (
            ("authority", DESSMONITOR_TIME_BASIS_AUTHORITY),
            ("provider_id", "smartess"),
            ("source_id", "dessmonitor"),
            ("source_action", DESSMONITOR_TIME_BASIS_SOURCE_ACTION),
        ):
            if type(record.get(key)) is not str or record.get(key) != expected:
                return None
        if (
            type(record.get("schema_version")) is not int
            or record.get("schema_version") != DESSMONITOR_TIME_BASIS_SCHEMA_VERSION
        ):
            return None
        try:
            return cls(
                identity=DessMonitorDeviceIdentity(
                    pn=identity["pn"],
                    sn=identity["sn"],
                    devcode=identity["devcode"],
                    devaddr=identity["devaddr"],
                ),
                offset_seconds=record["offset_seconds"],
            )
        except (TypeError, ValueError, KeyError):
            return None


def parse_device_time_basis(
    dat: object,
    *,
    expected_identity: DessMonitorDeviceIdentity,
) -> DessMonitorDeviceTimeBasis:
    """Accept exactly one timezone row matching the full device identity."""

    if type(expected_identity) is not DessMonitorDeviceIdentity:
        raise TypeError("dessmonitor_time_basis_identity_invalid")
    # The published API contract wraps rows in ``dat.device``.  The live
    # ``web.dessmonitor.com`` endpoint used by the official web application
    # returns the same rows directly as ``dat``.  Both shapes carry identical
    # provider fields and pass through the exact same identity/offset gate;
    # no mapping, duck sequence or coercion is accepted here.
    if type(dat) is dict and type(dat.get("device")) is list:
        rows = dat["device"]
    elif type(dat) is list:
        rows = dat
    else:
        raise DessMonitorCloudError("device_timezone_payload_invalid")
    candidates: list[int] = []
    for row in rows:
        if type(row) is not dict:
            continue
        pn = row.get("pn")
        sn = row.get("sn")
        devcode = row.get("devcode")
        devaddr = row.get("devaddr")
        offset = row.get("timezone")
        if (
            type(pn) is not str
            or not pn
            or pn != pn.strip()
            or type(sn) is not str
            or sn != expected_identity.sn
            or type(devcode) is not int
            or devcode != expected_identity.devcode
            or type(devaddr) is not int
            or devaddr != expected_identity.devaddr
            or not pn_is_same_identity(expected_identity.pn, pn)
        ):
            continue
        try:
            candidates.append(_offset_seconds(offset))
        except (TypeError, ValueError):
            continue
    if len(candidates) != 1:
        raise DessMonitorCloudError(
            f"device_timezone_ambiguous:{len(candidates)}"
        )
    return DessMonitorDeviceTimeBasis(
        identity=expected_identity,
        offset_seconds=candidates[0],
    )


def fetch_device_time_basis(
    *,
    session: DessMonitorSession,
    identity: DessMonitorDeviceIdentity,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    timeout: float = DEFAULT_TIMEOUT,
) -> DessMonitorDeviceTimeBasis:
    """Fetch one read-only exact-device timezone observation."""

    if type(identity) is not DessMonitorDeviceIdentity:
        raise TypeError("dessmonitor_time_basis_identity_invalid")
    device = ",".join(
        (
            identity.pn,
            str(identity.devcode),
            str(identity.devaddr),
            identity.sn,
        )
    )
    envelope = fetch_signed_action(
        action=(
            f"&action={DESSMONITOR_TIME_BASIS_SOURCE_ACTION}"
            f"&device={quote(device, safe='')}"
        ),
        session=session,
        base_url=base_url,
        language=language,
        timeout=timeout,
    )
    return parse_device_time_basis(
        envelope.dat,
        expected_identity=identity,
    )


__all__ = [
    "DESSMONITOR_TIME_BASIS_AUTHORITY",
    "DESSMONITOR_TIME_BASIS_SCHEMA_VERSION",
    "DESSMONITOR_TIME_BASIS_SOURCE_ACTION",
    "DessMonitorDeviceTimeBasis",
    "fetch_device_time_basis",
    "parse_device_time_basis",
]
