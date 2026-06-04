"""Load declarative SMG identity anchor catalog metadata from JSON files."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path


SMG_IDENTITY_ANCHOR_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "protocol_catalogs" / "smg_identity_anchors.json"
)

_ANCHOR_SOURCE_TYPES = frozenset({"block", "spec", "scalar"})


@dataclass(frozen=True, slots=True)
class SmgIdentityAnchorReadGroup:
    """One logical SMG identity anchor read group."""

    key: str
    description: str = ""


@dataclass(frozen=True, slots=True)
class SmgIdentityAnchorLayoutGroup:
    """One SMG identity anchor schema/layout group."""

    key: str
    register_schema_name: str
    description: str = ""


@dataclass(frozen=True, slots=True)
class SmgIdentityAnchor:
    """One declarative SMG identity anchor entry."""

    key: str
    read_group: str
    source_type: str
    block_key: str = ""
    spec_set_key: str = ""
    register_key: str = ""
    scalar_key: str = ""
    variants: tuple[str, ...] = ()
    layout_groups: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SmgIdentityAnchorCatalog:
    """Declarative SMG identity anchor catalog."""

    protocol_family: str
    layout_groups: dict[str, SmgIdentityAnchorLayoutGroup]
    base_layout_groups: tuple[str, ...]
    variant_layout_groups: dict[str, tuple[str, ...]]
    read_groups: dict[str, SmgIdentityAnchorReadGroup]
    anchors: dict[str, SmgIdentityAnchor]

    def anchors_for_group(self, group_key: str) -> tuple[SmgIdentityAnchor, ...]:
        """Return anchors in declaration order for one read group."""

        return tuple(anchor for anchor in self.anchors.values() if anchor.read_group == group_key)


@lru_cache(maxsize=None)
def load_smg_identity_anchor_catalog() -> SmgIdentityAnchorCatalog:
    """Load the built-in SMG identity anchor catalog."""

    raw = json.loads(SMG_IDENTITY_ANCHOR_CATALOG_PATH.read_text(encoding="utf-8"))
    read_groups = tuple(
        _parse_read_group(item)
        for item in raw.get("read_groups", [])
        if isinstance(item, dict)
    )
    layout_groups = tuple(
        _parse_layout_group(item)
        for item in raw.get("layout_groups", [])
        if isinstance(item, dict)
    )
    base_layout_groups = tuple(
        str(item).strip()
        for item in raw.get("base_layout_groups", [])
        if str(item).strip()
    )
    variant_layout_groups_raw = raw.get("variant_layout_groups", {})
    if not isinstance(variant_layout_groups_raw, dict):
        variant_layout_groups_raw = {}
    variant_layout_groups = {
        str(variant_key).strip(): tuple(
            str(layout_key).strip()
            for layout_key in layout_group_keys
            if str(layout_key).strip()
        )
        for variant_key, layout_group_keys in variant_layout_groups_raw.items()
        if str(variant_key).strip() and isinstance(layout_group_keys, list)
    }
    anchors = tuple(
        _parse_anchor(item)
        for item in raw.get("anchors", [])
        if isinstance(item, dict)
    )

    catalog = SmgIdentityAnchorCatalog(
        protocol_family=str(raw.get("protocol_family", "modbus_smg")).strip() or "modbus_smg",
        layout_groups=_keyed_map_or_raise(
            items=layout_groups,
            kind="layout_group",
        ),
        base_layout_groups=base_layout_groups,
        variant_layout_groups=variant_layout_groups,
        read_groups=_keyed_map_or_raise(
            items=read_groups,
            kind="read_group",
        ),
        anchors=_keyed_map_or_raise(
            items=anchors,
            kind="anchor",
        ),
    )
    _validate_catalog(catalog)
    return catalog


def clear_smg_identity_anchor_catalog_cache() -> None:
    """Clear cached SMG identity anchor catalog metadata."""

    load_smg_identity_anchor_catalog.cache_clear()


def resolve_smg_identity_anchor(anchor_key: str) -> SmgIdentityAnchor | None:
    """Resolve one SMG identity anchor by key."""

    normalized = str(anchor_key).strip()
    if not normalized:
        return None
    return load_smg_identity_anchor_catalog().anchors.get(normalized)


def _parse_read_group(raw: dict[str, object]) -> SmgIdentityAnchorReadGroup:
    return SmgIdentityAnchorReadGroup(
        key=str(raw["key"]).strip(),
        description=str(raw.get("description", "")).strip(),
    )


def _parse_layout_group(raw: dict[str, object]) -> SmgIdentityAnchorLayoutGroup:
    return SmgIdentityAnchorLayoutGroup(
        key=str(raw["key"]).strip(),
        register_schema_name=str(raw.get("register_schema_name", "")).strip(),
        description=str(raw.get("description", "")).strip(),
    )


def _parse_anchor(raw: dict[str, object]) -> SmgIdentityAnchor:
    variants = tuple(
        str(item).strip()
        for item in raw.get("variants", [])
        if str(item).strip()
    )
    layout_groups = tuple(
        str(item).strip()
        for item in raw.get("layout_groups", [])
        if str(item).strip()
    )
    return SmgIdentityAnchor(
        key=str(raw["key"]).strip(),
        read_group=str(raw["read_group"]).strip(),
        source_type=str(raw.get("source_type", "")).strip(),
        block_key=str(raw.get("block_key", "")).strip(),
        spec_set_key=str(raw.get("spec_set_key", "")).strip(),
        register_key=str(raw.get("register_key", "")).strip(),
        scalar_key=str(raw.get("scalar_key", "")).strip(),
        variants=variants,
        layout_groups=layout_groups,
    )


def _validate_catalog(catalog: SmgIdentityAnchorCatalog) -> None:
    if not catalog.protocol_family:
        raise ValueError("smg_identity_anchor_catalog:missing_protocol_family")

    if not catalog.read_groups:
        raise ValueError("smg_identity_anchor_catalog:missing_read_groups")

    if not catalog.layout_groups:
        raise ValueError("smg_identity_anchor_catalog:missing_layout_groups")

    if not catalog.base_layout_groups:
        raise ValueError("smg_identity_anchor_catalog:missing_base_layout_groups")

    if not catalog.anchors:
        raise ValueError("smg_identity_anchor_catalog:missing_anchors")

    for layout_group_key, layout_group in catalog.layout_groups.items():
        if not layout_group_key:
            raise ValueError("smg_identity_anchor_catalog:invalid_layout_group_key")
        if layout_group.key != layout_group_key:
            raise ValueError(
                f"smg_identity_anchor_catalog:layout_group_key_mismatch:{layout_group.key}"
            )
        if not layout_group.register_schema_name:
            raise ValueError(
                f"smg_identity_anchor_catalog:missing_layout_group_schema:{layout_group.key}"
            )

    seen_base_layout_groups: set[str] = set()
    for layout_group_key in catalog.base_layout_groups:
        if layout_group_key not in catalog.layout_groups:
            raise ValueError(
                f"smg_identity_anchor_catalog:unknown_base_layout_group:{layout_group_key}"
            )
        if layout_group_key in seen_base_layout_groups:
            raise ValueError(
                f"smg_identity_anchor_catalog:duplicate_base_layout_group:{layout_group_key}"
            )
        seen_base_layout_groups.add(layout_group_key)

    variant_layout_group_owner: dict[str, str] = {}
    for variant_key, layout_group_keys in catalog.variant_layout_groups.items():
        if not variant_key:
            raise ValueError("smg_identity_anchor_catalog:invalid_variant_layout_group_key")
        seen_variant_layout_groups: set[str] = set()
        for layout_group_key in layout_group_keys:
            if layout_group_key not in catalog.layout_groups:
                raise ValueError(
                    "smg_identity_anchor_catalog:unknown_variant_layout_group:"
                    f"{variant_key}:{layout_group_key}"
                )
            if layout_group_key in seen_variant_layout_groups:
                raise ValueError(
                    "smg_identity_anchor_catalog:duplicate_variant_layout_group:"
                    f"{variant_key}:{layout_group_key}"
                )
            if layout_group_key in seen_base_layout_groups:
                raise ValueError(
                    "smg_identity_anchor_catalog:ambiguous_variant_layout_group_with_base:"
                    f"{variant_key}:{layout_group_key}"
                )
            existing_variant_key = variant_layout_group_owner.get(layout_group_key)
            if existing_variant_key is not None and existing_variant_key != variant_key:
                raise ValueError(
                    "smg_identity_anchor_catalog:ambiguous_variant_layout_group_owner:"
                    f"{layout_group_key}:{existing_variant_key}:{variant_key}"
                )
            variant_layout_group_owner[layout_group_key] = variant_key
            seen_variant_layout_groups.add(layout_group_key)

    for group_key, group in catalog.read_groups.items():
        if not group_key:
            raise ValueError("smg_identity_anchor_catalog:invalid_read_group_key")
        if group.key != group_key:
            raise ValueError(f"smg_identity_anchor_catalog:read_group_key_mismatch:{group.key}")

    for anchor_key, anchor in catalog.anchors.items():
        if not anchor_key:
            raise ValueError("smg_identity_anchor_catalog:invalid_anchor_key")
        if anchor.key != anchor_key:
            raise ValueError(f"smg_identity_anchor_catalog:anchor_key_mismatch:{anchor.key}")
        if anchor.read_group not in catalog.read_groups:
            raise ValueError(
                f"smg_identity_anchor_catalog:unknown_read_group:{anchor.key}:{anchor.read_group}"
            )
        if anchor.source_type not in _ANCHOR_SOURCE_TYPES:
            raise ValueError(
                f"smg_identity_anchor_catalog:invalid_source_type:{anchor.key}:{anchor.source_type}"
            )
        if not anchor.layout_groups:
            raise ValueError(f"smg_identity_anchor_catalog:missing_anchor_layout_groups:{anchor.key}")
        seen_anchor_layout_groups: set[str] = set()
        for layout_group_key in anchor.layout_groups:
            if layout_group_key not in catalog.layout_groups:
                raise ValueError(
                    f"smg_identity_anchor_catalog:unknown_anchor_layout_group:{anchor.key}:{layout_group_key}"
                )
            if layout_group_key in seen_anchor_layout_groups:
                raise ValueError(
                    f"smg_identity_anchor_catalog:duplicate_anchor_layout_group:{anchor.key}:{layout_group_key}"
                )
            seen_anchor_layout_groups.add(layout_group_key)
        _validate_anchor_source(anchor)


def _validate_anchor_source(anchor: SmgIdentityAnchor) -> None:
    if anchor.source_type == "block":
        if not anchor.block_key:
            raise ValueError(f"smg_identity_anchor_catalog:missing_block_key:{anchor.key}")
        return

    if anchor.source_type == "spec":
        if not anchor.spec_set_key:
            raise ValueError(f"smg_identity_anchor_catalog:missing_spec_set_key:{anchor.key}")
        if not anchor.register_key:
            raise ValueError(f"smg_identity_anchor_catalog:missing_register_key:{anchor.key}")
        return

    if anchor.source_type == "scalar":
        if not anchor.scalar_key:
            raise ValueError(f"smg_identity_anchor_catalog:missing_scalar_key:{anchor.key}")


def _keyed_map_or_raise(*, items, kind: str) -> dict[str, object]:
    keyed: dict[str, object] = {}
    for item in items:
        key = str(item.key).strip()
        if not key:
            raise ValueError(f"smg_identity_anchor_catalog:invalid_{kind}_key")
        if key in keyed:
            raise ValueError(f"smg_identity_anchor_catalog:duplicate_{kind}:{key}")
        keyed[key] = item
    return keyed
