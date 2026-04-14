"""User-facing guidance for support capture and experimental metadata workflows."""

from __future__ import annotations

from typing import Any

from ..control_policy import normalize_confidence


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


def build_support_workflow_state(
    *,
    has_inverter: bool,
    driver_name: str = "",
    detection_confidence: str | None = None,
    profile_source_scope: str = "",
    schema_source_scope: str = "",
) -> dict[str, str]:
    """Return one compact support workflow status and the recommended next step."""

    confidence = normalize_confidence(detection_confidence)
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

    if not has_inverter and not driver_name:
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
            summary="A driver base is known, but the inverter is not currently confirmed.",
            next_action=(
                "Create a support archive and send the ZIP file to the developer before "
                "editing local drafts."
            ),
            primary_action="create_support_package",
            step_1="Create a support archive.",
            step_2="Send the ZIP file to the developer.",
            step_3="Only move on to local drafts after the developer reviews the evidence.",
            advanced_hint="Treat local drafts as a second-stage tool here. First confirm connectivity or send the support evidence.",
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
