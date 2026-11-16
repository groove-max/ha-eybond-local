"""Bounded failure evidence for learning, detached from recovery operations."""

from __future__ import annotations

from pathlib import Path

from ...collector.management import CollectorManagementTransportError


def _learning_failure_location(exc: Exception) -> str:
    """Return code coordinates only, never exception text, locals or URLs."""
    location = "unknown"
    trace = exc.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        path = Path(code.co_filename)
        if "eybond_local" in path.parts:
            location = f"{path.name}:{code.co_name}:{trace.tb_lineno}"
        trace = trace.tb_next
    return location


def _learning_management_failure(exc: Exception) -> dict[str, object]:
    """Preserve the primary typed failure even if endpoint recovery fails too."""
    if not isinstance(exc, CollectorManagementTransportError):
        return {}
    calls = []
    trace = exc.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        path = Path(code.co_filename)
        if "eybond_local" in path.parts:
            calls.append(f"{path.name}:{code.co_name}:{trace.tb_lineno}")
        trace = trace.tb_next
    return {"management_failure": {
        "error_code": exc.diagnostic_code,
        "failed_request": exc.request_diagnostics,
        **({"operation_context": dict(exc.operation_context)} if exc.operation_context else {}),
        "call_path": calls[-12:],
    }}
