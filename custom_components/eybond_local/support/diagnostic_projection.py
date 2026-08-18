"""Best-effort diagnostic projections from one runtime transport.

The projection deliberately owns no coordinator, Home Assistant, session,
or persistence state.  It may inspect transport internals for diagnostics, but
must never let an unavailable or malformed diagnostic surface block a run.
"""

from __future__ import annotations


def build_runtime_transport_debug(transport: object | None) -> dict[str, object]:
    """Return transport internals needed to diagnose raw command routing."""

    debug: dict[str, object] = {
        "transport_type": type(transport).__name__ if transport is not None else "",
        "transport_id": id(transport) if transport is not None else 0,
        "transport_connected": bool(getattr(transport, "connected", False)),
    }
    try:
        collector = getattr(transport, "collector_info", None)
        if collector is not None:
            debug.update(
                {
                    "collector_remote_ip": getattr(collector, "remote_ip", "") or "",
                    "collector_remote_port": getattr(collector, "remote_port", None),
                    "collector_pn_present": bool(
                        str(getattr(collector, "collector_pn", "") or "").strip()
                    ),
                    "raw_request_count": getattr(collector, "raw_request_count", 0),
                    "raw_response_count": getattr(collector, "raw_response_count", 0),
                    "raw_timeout_count": getattr(collector, "raw_timeout_count", 0),
                    "raw_unhandled_line_count": getattr(
                        collector,
                        "raw_unhandled_line_count",
                        0,
                    ),
                    "raw_last_spacing_wait_ms": getattr(
                        collector,
                        "raw_last_spacing_wait_ms",
                        0,
                    ),
                    "raw_last_response_duration_ms": getattr(
                        collector,
                        "raw_last_response_duration_ms",
                        0,
                    ),
                    "raw_last_total_duration_ms": getattr(
                        collector,
                        "raw_last_total_duration_ms",
                        0,
                    ),
                    "raw_last_request_ascii": getattr(
                        collector,
                        "raw_last_request_ascii",
                        "",
                    )
                    or "",
                    "raw_last_response_ascii": getattr(
                        collector,
                        "raw_last_response_ascii",
                        "",
                    )
                    or "",
                    "raw_last_timeout_request_ascii": getattr(
                        collector,
                        "raw_last_timeout_request_ascii",
                        "",
                    )
                    or "",
                    "raw_last_parser": getattr(collector, "raw_last_parser", "") or "",
                    "raw_last_frame_format": getattr(
                        collector,
                        "raw_last_frame_format",
                        "",
                    )
                    or "",
                }
            )
    except Exception as exc:  # noqa: BLE001 - diagnostics must not block scenario
        debug["collector_info_error"] = str(exc)

    try:
        connection_getter = getattr(transport, "_at_connection", None)
        if callable(connection_getter):
            connection = connection_getter(create_placeholder=False)
            debug["at_connection_id"] = id(connection) if connection is not None else 0
            debug["at_connection_connected"] = bool(
                getattr(connection, "connected", False)
            )
            if connection is not None:
                reader_task = getattr(connection, "_reader_task", None)
                writer = getattr(connection, "_writer", None)
                pending_raw = getattr(connection, "_pending_raw_response", None)
                debug.update(
                    {
                        "at_reader_task_done": bool(
                            reader_task is not None and reader_task.done()
                        ),
                        "at_writer_closing": bool(
                            writer is not None and writer.is_closing()
                        ),
                        "at_pending_raw_present": pending_raw is not None,
                        "at_pending_raw_done": bool(
                            pending_raw is not None and pending_raw.done()
                        ),
                        "at_raw_frame_format": getattr(
                            connection,
                            "_raw_passthrough_frame_format",
                            "",
                        )
                        or "",
                    }
                )
    except Exception as exc:  # noqa: BLE001 - diagnostics must not block scenario
        debug["connection_debug_error"] = str(exc)
    return debug


__all__ = ["build_runtime_transport_debug"]
