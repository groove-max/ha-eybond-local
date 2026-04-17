"""User-facing guidance for support capture and experimental metadata workflows."""

from __future__ import annotations

from typing import Any

from ..control_policy import normalize_confidence


_SMG_FAMILY_FALLBACK_VARIANT = "family_fallback"


def _workflow_state(
    *,
    level: str,
    level_label: str,
    summary: str,
    next_action: str,
    primary_action: str,
    step_1: str,
    step_2: str,
    step_3: str,
    advanced_hint: str,
) -> dict[str, str]:
    """Build one consistent support workflow payload."""

    return {
        "level": level,
        "level_label": level_label,
        "summary": summary,
        "next_action": next_action,
        "primary_action": primary_action,
        "step_1": step_1,
        "step_2": step_2,
        "step_3": step_3,
        "plan": f"Step 1: {step_1} Step 2: {step_2} Step 3: {step_3}",
        "advanced_hint": advanced_hint,
    }


def _has_smartess_collector_hint(
    *,
    smartess_protocol_asset_id: str = "",
    smartess_profile_key: str = "",
    smartess_collector_version: str = "",
) -> bool:
    """Return true when onboarding captured any SmartESS-side collector evidence."""

    return any(
        str(value or "").strip()
        for value in (
            smartess_protocol_asset_id,
            smartess_profile_key,
            smartess_collector_version,
        )
    )


def build_support_workflow_state(
    *,
    has_inverter: bool,
    variant_key: str = "",
    effective_owner_key: str = "",
    effective_owner_name: str = "",
    smartess_family_name: str = "",
    detection_confidence: str | None = None,
    profile_source_scope: str = "",
    schema_source_scope: str = "",
    smartess_protocol_asset_id: str = "",
    smartess_profile_key: str = "",
    smartess_collector_version: str = "",
) -> dict[str, str]:
    """Return one compact support workflow status and the recommended next step."""

    confidence = normalize_confidence(detection_confidence)
    normalized_variant_key = str(variant_key or "").strip()
    owner_known = any(
        str(value or "").strip()
        for value in (effective_owner_key, effective_owner_name)
    )
    if profile_source_scope == "external" or schema_source_scope == "external":
        return _workflow_state(
            level="experimental",
            level_label="Experimental local metadata",
            summary="Local experimental metadata is active for this entry.",
            next_action=(
                "Reload and test the local metadata changes. If support is still incomplete, "
                "create a support archive and send the ZIP file to the developer."
            ),
            primary_action="reload_local_metadata",
            step_1="Reload local metadata.",
            step_2="Confirm that the local override is active and test the inverter again.",
            step_3="If support is still incomplete, create a support archive and send the ZIP file to the developer.",
            advanced_hint="Advanced metadata is already active here. Use the advanced tools to iterate on the local override, not as the first troubleshooting step.",
        )

    if not has_inverter and not owner_known:
        if _has_smartess_collector_hint(
            smartess_protocol_asset_id=smartess_protocol_asset_id,
            smartess_profile_key=smartess_profile_key,
            smartess_collector_version=smartess_collector_version,
        ):
            return _workflow_state(
                level="smartess_pending",
                level_label="SmartESS collector evidence",
                summary="The collector exposed SmartESS metadata, but no local inverter driver is matched yet.",
                next_action=(
                    "Create a support archive and send the ZIP file to the developer. "
                    "SmartESS app support can still rely on a separate cloud-normalized "
                    "identity even when the local descriptor is generic, missing, or incomplete."
                ),
                primary_action="create_support_package",
                step_1="Create a support archive.",
                step_2="Send the ZIP file to the developer.",
                step_3="Treat the local SmartESS descriptor as collector evidence only until a built-in mapping is confirmed.",
                advanced_hint="Do not treat a generic or missing local SmartESS descriptor as proof that the inverter is unsupported in the SmartESS app.",
            )
        return _workflow_state(
            level="unknown",
            level_label="Unknown support",
            summary="No supported inverter driver is matched right now.",
            next_action=(
                "Create a support archive and send the ZIP file to the developer. "
                "It will include collector evidence and generic raw register capture for review."
            ),
            primary_action="create_support_package",
            step_1="Create a support archive.",
            step_2="Send the ZIP file to the developer.",
            step_3="Wait for built-in support before working with local experimental metadata.",
            advanced_hint="Do not start with local drafts here. The first goal is to collect evidence the developer can analyze.",
        )

    if not has_inverter:
        return _workflow_state(
            level="pending",
            level_label="Pending confirmation",
            summary=(
                f"An internal runtime path is known ({effective_owner_name or effective_owner_key}), "
                "but the inverter is not currently confirmed."
                if (effective_owner_name or effective_owner_key)
                else "An internal runtime path is known, but the inverter is not currently confirmed."
            ),
            next_action=(
                "Create a support archive and send the ZIP file to the developer before "
                "editing local drafts."
            ),
            primary_action="create_support_package",
            step_1="Create a support archive.",
            step_2="Send the ZIP file to the developer.",
            step_3="Only move on to local drafts after the developer reviews the evidence.",
            advanced_hint=(
                "Treat local drafts as a second-stage tool here. First confirm connectivity or send the support evidence."
                + (
                    f" SmartESS family context: {smartess_family_name}."
                    if str(smartess_family_name or "").strip()
                    else ""
                )
            ),
        )

    if normalized_variant_key == _SMG_FAMILY_FALLBACK_VARIANT:
        return _workflow_state(
            level="family_fallback",
            level_label="Read-only unverified SMG family",
            summary=(
                "This inverter is using the generic SMG family fallback. "
                "Built-in writes are intentionally disabled until the exact model is verified."
            ),
            next_action=(
                "Create a support archive and send the ZIP file to the developer. "
                "This will help confirm the exact SMG-family model and move it beyond the read-only fallback."
            ),
            primary_action="create_support_package",
            step_1="Create a support archive.",
            step_2="Send the ZIP file to the developer.",
            step_3="Treat the current SMG support as read-only until the exact model is verified.",
            advanced_hint=(
                "Do not create local writable drafts for this device yet. "
                "The current fallback is intentionally read-only and unverified."
            ),
        )

    if confidence == "high":
        return _workflow_state(
            level="builtin",
            level_label="Built-in support",
            summary="Built-in support is active for this inverter.",
            next_action=(
                "Use Create Support Archive only if you want to report a bug, capture more "
                "evidence, or help extend support for a related model."
            ),
            primary_action="create_support_package",
            step_1="Keep using built-in support normally.",
            step_2="Create a support archive only if you need to report a bug or help extend support.",
            step_3="Send the ZIP file to the developer if requested.",
            advanced_hint="You usually do not need advanced metadata tools in this state. Use them only for deliberate experimental work.",
        )

    if confidence in {"medium", "low"}:
        return _workflow_state(
            level="partial",
            level_label="Partial support",
            summary="The inverter is reachable, but built-in support is only partially confirmed.",
            next_action=(
                "Create a support archive and send the ZIP file to the developer before "
                "creating local experimental drafts."
            ),
            primary_action="create_support_package",
            step_1="Create a support archive.",
            step_2="Send the ZIP file to the developer.",
            step_3="Use local experimental metadata only if the developer asks for it.",
            advanced_hint="Do not start editing drafts yet. The developer should look at the support archive first.",
        )

    return _workflow_state(
        level="unknown",
        level_label="Unknown support",
        summary="Support status is unknown for this entry.",
        next_action=(
            "Create a support archive and send the ZIP file to the developer."
        ),
        primary_action="create_support_package",
        step_1="Run the primary diagnostics action.",
        step_2="Send the ZIP file to the developer.",
        step_3="Use advanced metadata tools only if you are explicitly asked to.",
        advanced_hint="Advanced metadata tools are intentionally secondary. Use them only after the main evidence path is complete.",
    )
