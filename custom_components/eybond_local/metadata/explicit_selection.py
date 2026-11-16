"""Explicit user selection of a deliberately non-automatic catalog device.

A few catalog devices carry capture evidence that is sound for telemetry but
is NOT sufficient to claim automatic identity -- the LVYUAN TY-SIC family is
the current case.  Their descriptors declare explicit_selection_only, so they
never bind from an automatic sweep.  Binding them requires the user to name
the exact catalog entry.

The choice travels here, out of band, on purpose:

* drivers have no access to the config entry, and threading a new keyword
  through async_detect_inverter would widen the shared detection API for a
  boundary that only the catalog enforces;
* giving the model its own driver key would silently drop the canonical
  telemetry variants that are registered for the shared modbus_catalog key.

The runtime sets the selection from CONF_DEVICE_CATALOG_ENTRY for the
duration of one detection pass; the catalog resolver reads it while it
decides whether a single candidate may bind.  Absent a selection the boundary
is fail-closed: the entry reads as empty and the device stays unresolvable.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Iterator

_EXPLICIT_CATALOG_ENTRY: ContextVar[str] = ContextVar(
    "eybond_local_explicit_catalog_entry",
    default="",
)


def explicitly_selected_catalog_entry() -> str:
    """Return the catalog entry the user named explicitly, if any."""

    return _EXPLICIT_CATALOG_ENTRY.get()


@contextmanager
def explicit_catalog_entry(entry_key: str) -> Iterator[None]:
    """Scope one explicit catalog-entry selection to the enclosing block."""

    token: Token[str] = _EXPLICIT_CATALOG_ENTRY.set(str(entry_key or "").strip())
    try:
        yield
    finally:
        _EXPLICIT_CATALOG_ENTRY.reset(token)


__all__ = ["explicit_catalog_entry", "explicitly_selected_catalog_entry"]
