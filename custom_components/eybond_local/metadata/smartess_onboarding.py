"""SmartESS onboarding-assist interpretation of cloud evidence.

All SmartESS-specific parsing of a cloud-evidence payload lives here (device
preview, detail sections, settings highlights, digest counts), so the config
flow renders a normalized :class:`CloudEvidenceOnboardingAssist` without ever
touching the raw provider payload. This is the SmartESS provider's onboarding
interpretation -- the flow owns none of it.
"""

from __future__ import annotations

from typing import Any

from ..metadata.profile_loader import load_driver_profile
from ..support.cloud_evidence_result import (
    CloudEvidenceOnboardingAssist,
    CloudEvidenceSettingHighlight,
)

SMARTESS_ONBOARDING_SOURCE = "smartess_cloud_onboarding"


def _bundle_payload(evidence: dict[str, Any]) -> dict[str, Any]:
    payload = evidence.get("payload") if isinstance(evidence, dict) else None
    return payload if isinstance(payload, dict) else {}


def _device_preview(evidence: dict[str, Any]) -> dict[str, Any]:
    identity = evidence.get("device_identity") if isinstance(evidence, dict) else None
    identity = identity if isinstance(identity, dict) else {}
    normalized = _bundle_payload(evidence).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    normalized_list = normalized.get("device_list")
    normalized_list = normalized_list if isinstance(normalized_list, dict) else {}
    devices = normalized_list.get("devices")
    devices = devices if isinstance(devices, list) else []

    device_preview: dict[str, Any] = {}
    identity_pn = str(identity.get("pn") or "").strip()
    identity_sn = str(identity.get("sn") or "").strip()
    for item in devices:
        if not isinstance(item, dict):
            continue
        item_pn = str(item.get("pn") or "").strip()
        item_sn = str(item.get("sn") or "").strip()
        if identity_pn and item_pn == identity_pn:
            device_preview = item
            break
        if identity_sn and item_sn == identity_sn:
            device_preview = item
            break
    if not device_preview:
        for item in devices:
            if isinstance(item, dict):
                device_preview = item
                break

    return {
        "pn": identity_pn or str(device_preview.get("pn") or "").strip(),
        "sn": identity_sn or str(device_preview.get("sn") or "").strip(),
        "devcode": identity.get("devcode")
        if identity.get("devcode") not in (None, "")
        else device_preview.get("devcode"),
        "devaddr": identity.get("devaddr")
        if identity.get("devaddr") not in (None, "")
        else device_preview.get("devaddr"),
        "name": str(device_preview.get("devName") or "").strip(),
        "alias": str(device_preview.get("devalias") or "").strip(),
        "status": str(device_preview.get("status") or "").strip(),
        "brand": str(device_preview.get("brand") or "").strip(),
    }


def _detail_sections(evidence: dict[str, Any]) -> tuple[str, ...]:
    summary = evidence.get("summary") if isinstance(evidence, dict) else None
    summary = summary if isinstance(summary, dict) else {}
    normalized = _bundle_payload(evidence).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    normalized_detail = normalized.get("device_detail")
    normalized_detail = normalized_detail if isinstance(normalized_detail, dict) else {}
    section_counts = normalized_detail.get("section_counts")
    section_counts = section_counts if isinstance(section_counts, dict) else {}

    previews: list[str] = []
    if section_counts:
        for key in sorted(section_counts):
            previews.append(f"{key} ({section_counts[key]})")
    else:
        detail_sections = summary.get("detail_sections")
        if isinstance(detail_sections, list):
            previews.extend(str(item).strip() for item in detail_sections if str(item).strip())
    return tuple(previews)


def _highlight_settings(
    evidence: dict[str, Any],
    *,
    limit: int = 5,
) -> tuple[CloudEvidenceSettingHighlight, ...]:
    normalized = _bundle_payload(evidence).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    normalized_settings = normalized.get("device_settings")
    normalized_settings = normalized_settings if isinstance(normalized_settings, dict) else {}
    fields = normalized_settings.get("fields")
    fields = fields if isinstance(fields, list) else []

    bucket_priority = {"exact_0925": 0, "probable_0925": 1, "cloud_only": 2}

    def _register_for_field(field: dict[str, Any]) -> int | None:
        binding = field.get("binding")
        if isinstance(binding, dict):
            register = binding.get("register")
            if isinstance(register, int):
                return register
        register = field.get("asset_register")
        if isinstance(register, int):
            return register
        return None

    def _choice_label(field: dict[str, Any], value: Any) -> str:
        choices = field.get("choices")
        if not isinstance(choices, list):
            return ""
        value_text = str(value)
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            if choice.get("value") == value:
                return str(choice.get("label") or "").strip()
            if str(choice.get("raw_value") or "") == value_text:
                return str(choice.get("label") or "").strip()
        return ""

    def _current_value_preview(field: dict[str, Any]) -> str:
        if not field.get("has_current_value"):
            return ""
        current_value = field.get("current_value")
        label = _choice_label(field, current_value)
        if label:
            return label
        text = str(current_value).strip()
        if not text:
            return ""
        unit = str(field.get("unit") or "").strip()
        return f"{text} {unit}".strip()

    candidates = [
        field for field in fields if isinstance(field, dict) and str(field.get("title") or "").strip()
    ]
    candidates.sort(
        key=lambda field: (
            0 if field.get("has_current_value") else 1,
            bucket_priority.get(str(field.get("bucket") or ""), 9),
            0 if _register_for_field(field) is not None else 1,
            str(field.get("title") or "").lower(),
        )
    )

    highlights: list[CloudEvidenceSettingHighlight] = []
    for field in candidates:
        highlights.append(
            CloudEvidenceSettingHighlight(
                title=str(field.get("title") or "").strip(),
                bucket=str(field.get("bucket") or "").strip(),
                current_value=_current_value_preview(field),
                register=_register_for_field(field),
            )
        )
        if len(highlights) >= limit:
            break
    return tuple(highlights)


def build_smartess_onboarding_assist(
    *,
    evidence_payload: dict[str, Any],
    evidence_path: str,
    plan: Any,
    collector_pn: str,
) -> CloudEvidenceOnboardingAssist:
    """Build the normalized onboarding-assist DTO from a SmartESS evidence record.

    ``plan`` is the resolved known-family draft plan (or ``None``). The inferred
    local driver key is resolved here from the plan's source profile.
    """

    summary = dict(evidence_payload.get("summary") or {})
    device_preview = _device_preview(evidence_payload)
    normalized = _bundle_payload(evidence_payload).get("normalized")
    normalized = normalized if isinstance(normalized, dict) else {}
    device_settings = normalized.get("device_settings")
    device_settings = device_settings if isinstance(device_settings, dict) else {}

    inferred_driver_key = ""
    if plan is not None and getattr(plan, "source_profile_name", ""):
        try:
            inferred_driver_key = str(
                load_driver_profile(plan.source_profile_name).driver_key or ""
            ).strip()
        except Exception:
            inferred_driver_key = ""

    return CloudEvidenceOnboardingAssist(
        collector_pn=collector_pn,
        evidence_path=str(evidence_path),
        inferred_asset_id=getattr(plan, "asset_id", "") if plan is not None else "",
        inferred_profile_key=getattr(plan, "profile_key", "") if plan is not None else "",
        inferred_driver_key=inferred_driver_key,
        inferred_family_label=getattr(plan, "driver_label", "") if plan is not None else "",
        inferred_reason=getattr(plan, "reason", "") if plan is not None else "",
        exact_field_count=int(summary.get("settings_exact_0925_field_count") or 0),
        probable_field_count=int(summary.get("settings_probable_0925_field_count") or 0),
        cloud_only_field_count=int(summary.get("settings_cloud_only_field_count") or 0),
        current_values_included=bool(summary.get("settings_current_values_included", False)),
        total_field_count=int(
            device_settings.get("field_count") or summary.get("settings_field_count") or 0
        ),
        mapped_field_count=int(
            device_settings.get("mapped_field_count")
            or summary.get("settings_mapped_field_count")
            or 0
        ),
        fields_with_current_value=int(device_settings.get("fields_with_current_value") or 0),
        device_pn=str(device_preview.get("pn") or "").strip(),
        device_sn=str(device_preview.get("sn") or "").strip(),
        device_name=str(device_preview.get("name") or "").strip(),
        device_alias=str(device_preview.get("alias") or "").strip(),
        device_status=str(device_preview.get("status") or "").strip(),
        device_brand=str(device_preview.get("brand") or "").strip(),
        device_devcode=device_preview.get("devcode")
        if device_preview.get("devcode") not in ("", None)
        else None,
        device_devaddr=device_preview.get("devaddr")
        if device_preview.get("devaddr") not in ("", None)
        else None,
        detail_sections=_detail_sections(evidence_payload),
        highlight_settings=_highlight_settings(evidence_payload),
    )
