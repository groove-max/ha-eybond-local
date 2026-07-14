"""Provider-neutral result DTOs for cloud-evidence onboarding assist.

Leaf module (no provider imports) so both the provider implementations and the
config flow can share these render-ready shapes without an import cycle and
without the flow ever parsing a raw provider payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CloudEvidenceSettingHighlight:
    """One render-ready highlighted cloud setting (already normalized)."""

    title: str
    bucket: str = ""
    current_value: str = ""
    register: int | None = None


@dataclass(frozen=True)
class CloudEvidenceOnboardingAssist:
    """Normalized onboarding-assist result a config flow renders directly.

    Every field is already-normalized data (paths, counts, identity strings, and
    render-ready highlights). The flow renders these fields without importing a
    provider client, a draft resolver, or parsing a raw provider payload.
    """

    collector_pn: str
    evidence_path: str = ""
    inferred_asset_id: str = ""
    inferred_profile_key: str = ""
    inferred_driver_key: str = ""
    inferred_family_label: str = ""
    inferred_reason: str = ""
    exact_field_count: int = 0
    probable_field_count: int = 0
    cloud_only_field_count: int = 0
    current_values_included: bool = False
    total_field_count: int = 0
    mapped_field_count: int = 0
    fields_with_current_value: int = 0
    device_pn: str = ""
    device_sn: str = ""
    device_name: str = ""
    device_alias: str = ""
    device_status: str = ""
    device_brand: str = ""
    device_devcode: int | None = None
    device_devaddr: int | None = None
    detail_sections: tuple[str, ...] = ()
    highlight_settings: tuple[CloudEvidenceSettingHighlight, ...] = field(default_factory=tuple)
