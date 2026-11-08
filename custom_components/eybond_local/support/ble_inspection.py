"""Bounded, fixed collector metadata queries; no provisioning or runtime authority."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re

from ..collector.at import CollectorAtError, CollectorAtQueryStatus, parse_at_response
from ..collector.smartess_ble import SmartEssBleError, VENDOR_LAYOUT
from ..collector.ble_diagnostics import BleExchangeDiagnostics
from ..collector_endpoint import inspect_collector_server_endpoint
from ..collector_identity import validated_collector_pn
from .diagnostic_runner import DiagnosticRunResult


BLE_INSPECTION_QUERIES = ("DTUPN", "FWVER", "ATVER", "CLDSRVHOST1")
CONNECT_TIMEOUT = 20.0
QUERY_TIMEOUT = 4.0
MAX_REPLY_CHARS = 512


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _value(command: str, response: str):
    if len(response) > MAX_REPLY_CHARS:
        raise ValueError("reply_too_large")
    # Never treat an echoed query, status or unrelated notification as a value.
    text = response.strip()
    if not text.startswith(f"AT+{command}:") or "\n" in text or "\r" in text:
        # Preserve the existing typed read-status classification where possible.
        parse_at_response(text, expected_command=command)
        raise ValueError("unexpected_reply")
    value = parse_at_response(text, expected_command=command).value
    if re.fullmatch(r"[RW]\d{3}", value):
        raise ValueError("status_not_value")
    if command == "DTUPN":
        pn = validated_collector_pn(value)
        if not pn or pn != value:
            raise ValueError("invalid_identity")
        return pn
    if command in {"FWVER", "ATVER"}:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,79}", value):
            raise ValueError("invalid_version")
        return value
    endpoint = inspect_collector_server_endpoint(
        value, require_explicit_port=False, require_explicit_protocol=False,
    )
    return {
        "host": endpoint.host,
        # Host-only firmware does not report its port/protocol; do not invent them.
        "port": endpoint.port if endpoint.has_explicit_port else None,
        "protocol": endpoint.protocol if endpoint.has_explicit_protocol else None,
        "port_reported": endpoint.has_explicit_port,
        "protocol_reported": endpoint.has_explicit_protocol,
    }


@dataclass
class BleInspection:
    """One attempt's evidence; connection cleanup belongs to the caller."""

    expected_pn: str
    append_crlf: bool = False
    started_at: str = field(default_factory=_now)
    observations: list[dict] = field(default_factory=list)
    identity_status: str = "unavailable"
    error: str | None = None
    cleanup_error: str | None = None
    layout: str = ""
    connection_status: str = "not_started"
    connection_elapsed_ms: int = 0

    async def run(self, session) -> None:
        """Send only the fixed queries, once each, under bounded waits."""

        try:
            self.connection_status = "connecting"
            started = asyncio.get_running_loop().time()
            try:
                async with asyncio.timeout(CONNECT_TIMEOUT):
                    layout = await session.connect()
            finally:
                self.connection_elapsed_ms = max(0, int((asyncio.get_running_loop().time() - started) * 1000))
            # The session connects and subscribes before returning. This is not
            # proof that the AT handler answered or the identity matched.
            self.connection_status = "ready"
            self.layout = layout.name
            if layout != VENDOR_LAYOUT:
                self.error = "unsupported_ble_layout"
                return
            for command in BLE_INSPECTION_QUERIES:
                observation = {"command": f"AT+{command}?", "status": "pending"}
                self.observations.append(observation)
                diagnostics = BleExchangeDiagnostics()
                try:
                    async with asyncio.timeout(QUERY_TIMEOUT):
                        response = await session.exchange_text(
                            observation["command"], timeout=QUERY_TIMEOUT,
                            append_crlf=self.append_crlf, response=True,
                            diagnostics=diagnostics,
                        )
                    observation["response_size"] = len(response)
                    observation["value"] = _value(command, response)
                    observation["status"] = "received"
                except (TimeoutError, SmartEssBleError) as exc:
                    observation["status"] = "timeout" if (
                        isinstance(exc, TimeoutError) or str(exc) == "ble_notification_timeout"
                    ) else "transport_error"
                    if observation["status"] == "transport_error":
                        self.error = "ble_transport_error"
                        break
                except CollectorAtQueryStatus as exc:
                    observation.update(status="query_status", code=exc.status)
                except (CollectorAtError, ValueError):
                    # Unknown bytes can contain credentials/identifiers. Keep size
                    # and classification, not unparsed text/hex in support evidence.
                    observation["status"] = "unrecognized_reply"
                finally:
                    # Preserve evidence even when the outer deadline cancels a
                    # write or response wait. Never export payload or exception text.
                    observation["transport"] = diagnostics.snapshot()
                if command == "DTUPN" and observation["status"] == "received":
                    self.identity_status = (
                        "matched" if self.expected_pn == observation["value"] else "mismatch"
                    ) if self.expected_pn else "unbound"
                    if self.identity_status == "mismatch":
                        self.error = "collector_identity_mismatch"
                        break
        except asyncio.CancelledError:
            if self.connection_status == "connecting":
                self.connection_status = "cancelled"
            self.error = "cancelled"
            if self.observations and self.observations[-1]["status"] == "pending":
                self.observations[-1]["status"] = "cancelled"
            raise
        except TimeoutError:
            self.connection_status = "timeout"
            self.error = "ble_connect_timeout"
        except Exception:
            if self.observations and self.observations[-1]["status"] == "pending":
                self.observations[-1]["status"] = "transport_error"
                self.error = "ble_transport_error"
            else:
                self.connection_status = "failed"
                self.error = "ble_connection_failed"

    def result(self, *, address: str) -> DiagnosticRunResult:
        complete = (
            len(self.observations) == len(BLE_INSPECTION_QUERIES)
            and all(item["status"] == "received" for item in self.observations)
        )
        error = self.error or self.cleanup_error or (
            None if complete and self.identity_status == "matched" else "partial_or_unbound"
        )
        context = {
            "diagnostic_kind": "ble_collector_inspection", "requested_wire": "ble",
            "ble_address": address, "layout": self.layout,
            "line_ending": "crlf" if self.append_crlf else "none",
            "identity_status": self.identity_status, "expected_collector_pn": self.expected_pn,
            "cleanup_error": self.cleanup_error,
            "settings_changed": False, "endpoint_restoration_verified": False,
            "unparsed_replies_included": False,
            "connection": {"status": self.connection_status, "elapsed_ms": self.connection_elapsed_ms},
        }
        lines = [f"Identity: {self.identity_status}", f"Result: {error or 'complete'}"]
        for item in self.observations:
            lines.append(f"{item['command']} — {item['status']}: {item.get('value', '')}")
        return DiagnosticRunResult(
            success=error is None, output="\n".join(lines), results=list(self.observations),
            context=context, started_at=self.started_at, finished_at=_now(), error=error,
        )
