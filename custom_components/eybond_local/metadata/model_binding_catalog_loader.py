"""Load canonical built-in driver metadata bindings from JSON files."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path


MODEL_BINDING_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "protocol_catalogs" / "model_bindings.json"
)


@dataclass(frozen=True, slots=True)
class DriverModelBinding:
    """Canonical metadata binding for one built-in driver/variant pair."""

    driver_key: str
    protocol_family: str
    variant_key: str
    profile_name: str = ""
    register_schema_name: str = ""


@dataclass(frozen=True, slots=True)
class DriverModelBindingCatalog:
    """Catalog of canonical built-in driver metadata bindings."""

    bindings: dict[tuple[str, str], DriverModelBinding]


@lru_cache(maxsize=None)
def load_driver_model_binding_catalog() -> DriverModelBindingCatalog:
    """Load the built-in driver metadata binding catalog."""

    raw = json.loads(MODEL_BINDING_CATALOG_PATH.read_text(encoding="utf-8"))
    entries = tuple(
        _parse_binding(item)
        for item in raw.get("bindings", [])
        if isinstance(item, dict)
    )
    return DriverModelBindingCatalog(
        bindings={(entry.driver_key, entry.variant_key): entry for entry in entries},
    )


def clear_driver_model_binding_catalog_cache() -> None:
    """Clear cached built-in driver metadata bindings."""

    load_driver_model_binding_catalog.cache_clear()


def resolve_driver_model_binding(
    driver_key: str,
    *,
    variant_key: str = "default",
) -> DriverModelBinding | None:
    """Resolve one canonical metadata binding by driver and variant key."""

    normalized_driver_key = str(driver_key).strip()
    normalized_variant_key = str(variant_key or "default").strip() or "default"
    bindings = load_driver_model_binding_catalog().bindings
    binding = bindings.get((normalized_driver_key, normalized_variant_key))
    if binding is not None or normalized_variant_key == "default":
        return binding
    return bindings.get((normalized_driver_key, "default"))


def _parse_binding(raw: dict[str, object]) -> DriverModelBinding:
    return DriverModelBinding(
        driver_key=str(raw["driver_key"]).strip(),
        protocol_family=str(raw.get("protocol_family", "")).strip(),
        variant_key=str(raw.get("variant_key", "default")).strip() or "default",
        profile_name=str(raw.get("profile_name", "")).strip(),
        register_schema_name=str(raw.get("register_schema_name", "")).strip(),
    )