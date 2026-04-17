"""Resolve effective metadata names for runtime tooling and support flows."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ..const import CONF_SMARTESS_PROFILE_KEY, CONF_SMARTESS_PROTOCOL_ASSET_ID
from ..models import CollectorInfo
from ..runtime_labels import runtime_path_label
from .profile_loader import DriverProfileMetadata, load_driver_profile
from .register_schema_loader import load_register_schema
from .register_schema_models import RegisterSchemaMetadata
from .smartess_protocol_catalog_loader import (
    SmartEssProtocolCatalogEntry,
    resolve_smartess_protocol_catalog_entry,
)


@dataclass(frozen=True, slots=True)
class EffectiveMetadataSelection:
    """Resolved metadata names, ownership, and SmartESS hints for one entry state."""

    effective_owner_key: str = ""
    effective_owner_name: str = ""
    smartess_family_name: str = ""
    raw_profile_name: str = ""
    raw_register_schema_name: str = ""
    profile_name: str = ""
    register_schema_name: str = ""
    profile_metadata: DriverProfileMetadata | None = None
    register_schema_metadata: RegisterSchemaMetadata | None = None
    smartess_protocol: SmartEssProtocolCatalogEntry | None = None


def resolve_effective_metadata_selection(
    *,
    inverter: Any = None,
    driver: Any = None,
    collector: CollectorInfo | None = None,
    entry_data: Mapping[str, Any] | None = None,
) -> EffectiveMetadataSelection:
    """Resolve effective metadata names from runtime state and SmartESS hints."""

    smartess_protocol = _resolve_smartess_protocol_hint(
        collector=collector,
        entry_data=entry_data,
    )

    profile_name = _normalized_name(getattr(inverter, "profile_name", ""))
    if not profile_name:
        profile_name = _normalized_name(getattr(driver, "profile_name", ""))
    if not profile_name and smartess_protocol is not None:
        profile_name = smartess_protocol.profile_name

    register_schema_name = _normalized_name(getattr(inverter, "register_schema_name", ""))
    if not register_schema_name:
        register_schema_name = _normalized_name(getattr(driver, "register_schema_name", ""))
    if not register_schema_name and smartess_protocol is not None:
        register_schema_name = smartess_protocol.register_schema_name

    profile_metadata = load_driver_profile(profile_name) if profile_name else None
    register_schema_metadata = (
        load_register_schema(register_schema_name) if register_schema_name else None
    )

    effective_owner_key = _normalized_name(getattr(inverter, "driver_key", ""))
    if not effective_owner_key:
        effective_owner_key = _normalized_name(getattr(driver, "key", ""))
    if not effective_owner_key and profile_metadata is not None:
        effective_owner_key = _normalized_name(getattr(profile_metadata, "driver_key", ""))
    if not effective_owner_key and register_schema_metadata is not None:
        effective_owner_key = _normalized_name(
            getattr(register_schema_metadata, "driver_key", "")
        )

    effective_owner_name = _effective_owner_name_from_key(effective_owner_key)
    if not effective_owner_name:
        effective_owner_name = _normalized_name(getattr(driver, "name", ""))
    smartess_family_name = (
        _smartess_driver_name(smartess_protocol) if smartess_protocol is not None else ""
    )

    return EffectiveMetadataSelection(
        effective_owner_key=effective_owner_key,
        effective_owner_name=effective_owner_name,
        smartess_family_name=smartess_family_name,
        raw_profile_name=_normalized_name(
            getattr(smartess_protocol, "raw_profile_name", "")
        ),
        raw_register_schema_name=_normalized_name(
            getattr(smartess_protocol, "raw_register_schema_name", "")
        ),
        profile_name=profile_name,
        register_schema_name=register_schema_name,
        profile_metadata=profile_metadata,
        register_schema_metadata=register_schema_metadata,
        smartess_protocol=smartess_protocol,
    )


def _resolve_smartess_protocol_hint(
    *,
    collector: CollectorInfo | None,
    entry_data: Mapping[str, Any] | None,
) -> SmartEssProtocolCatalogEntry | None:
    live_asset_id = _normalized_name(getattr(collector, "smartess_protocol_asset_id", ""))
    live_profile_key = _normalized_name(getattr(collector, "smartess_protocol_profile_key", ""))
    saved_asset_id = _normalized_name((entry_data or {}).get(CONF_SMARTESS_PROTOCOL_ASSET_ID, ""))
    saved_profile_key = _normalized_name((entry_data or {}).get(CONF_SMARTESS_PROFILE_KEY, ""))
    return resolve_smartess_protocol_catalog_entry(
        asset_id=live_asset_id or saved_asset_id,
        profile_key=live_profile_key or saved_profile_key,
    )


def _smartess_driver_name(protocol: SmartEssProtocolCatalogEntry) -> str:
    if protocol.asset_id:
        return f"SmartESS {protocol.asset_id}"
    if protocol.profile_key.startswith("smartess_"):
        suffix = protocol.profile_key.removeprefix("smartess_").strip("_")
        if suffix:
            return f"SmartESS {suffix}"
    return "SmartESS"


def _normalized_name(value: Any) -> str:
    return str(value or "").strip()


def _effective_owner_name_from_key(driver_key: str) -> str:
    normalized_key = _normalized_name(driver_key)
    if not normalized_key:
        return ""
    label = runtime_path_label(normalized_key)
    if label and label != normalized_key:
        return label
    try:
        from ..drivers.registry import get_driver

        return _normalized_name(get_driver(normalized_key).name)
    except KeyError:
        return normalized_key