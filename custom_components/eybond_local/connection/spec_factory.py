"""Build typed connection specs from persisted or flow-local values.

The models module owns only immutable connection value objects.  This factory is
the one composition point that combines those models with branch metadata, so
``models`` and ``branch_registry`` retain a one-way dependency.
"""

from __future__ import annotations

from collections.abc import Mapping

from ..const import CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND
from .branch_registry import get_connection_branch
from .models import ConnectionSpec, ConnectionType


def resolve_connection_type(data: Mapping[str, object]) -> ConnectionType:
    """Return the effective connection type for stored config-entry data."""

    connection_type = str(
        data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND)
        or CONNECTION_TYPE_EYBOND
    )
    return get_connection_branch(connection_type).connection_type


def build_connection_spec(
    data: Mapping[str, object],
    options: Mapping[str, object],
) -> ConnectionSpec:
    """Build one typed connection spec from config-entry data and options."""

    branch = get_connection_branch(resolve_connection_type(data))
    return branch.build_connection_spec(data, options)


def build_connection_spec_from_values(
    connection_type: str,
    values: Mapping[str, object],
) -> ConnectionSpec:
    """Build one typed connection spec from branch-local values alone."""

    raw_data = dict(values)
    raw_data[CONF_CONNECTION_TYPE] = connection_type or CONNECTION_TYPE_EYBOND
    return build_connection_spec(raw_data, {})


__all__ = [
    "build_connection_spec",
    "build_connection_spec_from_values",
    "resolve_connection_type",
]
