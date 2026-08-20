"""Pure onboarding-result metadata projection helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .collector.capabilities import (
    CollectorCapabilityProfile,
    collector_capability_profile_from_runtime,
    collector_profile_entry_fields,
)
from .collector.transport_profile import (
    collector_session_protocol_from_inventory_state,
)
from .collector_identity import (
    identity_source_is_strong,
    pn_is_same_identity,
    reconcile_pn,
)
from .connection.admission import ObservedCollectorSession
from .const import (
    COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE,
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE,
    CONF_COLLECTOR_PN,
    CONF_DRIVER_HINT,
    CONF_SMARTESS_COLLECTOR_VERSION,
    CONF_SMARTESS_DEVICE_ADDRESS,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
    DRIVER_HINT_AUTO,
)
from .drivers.catalog_identity import ERROR_INVERTER_LINK_DOWN
from .flow_presentation import (
    _clear_runtime_inverter_facts,
)
from .models import (
    OnboardingResult,
)


def _collector_identity_matches(left: str, right: str) -> bool:
    """Return whether two collector PN values look like the same collector."""

    normalized_left = str(left or "").strip()
    normalized_right = str(right or "").strip()
    if not normalized_left or not normalized_right:
        return False
    if normalized_left == normalized_right:
        return True
    if min(len(normalized_left), len(normalized_right)) < 10:
        return False
    return bool(
        normalized_left.startswith(normalized_right)
        or normalized_right.startswith(normalized_left)
    )


def _result_indicates_inverter_link_down(result: OnboardingResult | None) -> bool:
    """True when the collector answered but the inverter link was down."""

    return (
        result is not None
        and result.match is None
        and str(result.last_error or "") == ERROR_INVERTER_LINK_DOWN
    )


def _apply_collector_profile_metadata(
    target: dict[str, Any],
    result: OnboardingResult | None,
) -> None:
    """Persist normalized collector profile metadata into one entry payload."""

    profile = _result_collector_capabilities(result)
    hardware_version = ""
    collector = getattr(result, "collector", None) if result is not None else None
    collector_info = getattr(collector, "collector", None)
    if collector_info is not None:
        hardware_version = str(
            getattr(collector_info, "collector_hardware_version", "") or ""
        ).strip()
    match = getattr(result, "match", None) if result is not None else None
    details = getattr(match, "details", None)
    if not hardware_version and isinstance(details, dict):
        hardware_version = str(details.get("collector_hardware_version") or "").strip()
    target.update(
        collector_profile_entry_fields(
            profile,
            hardware_version=hardware_version,
        )
    )
    if profile.virtual_bridge and collector_info is not None:
        bridge_version = str(
            getattr(collector_info, "collector_bridge_version", "") or ""
        ).strip()
        if bridge_version and not target.get("collector_bridge_version"):
            target["collector_bridge_version"] = bridge_version


def _result_is_virtual_bridge(result: OnboardingResult | None) -> bool:
    """Return True when an onboarding result positively identified an ESP bridge."""

    return _result_collector_capabilities(result).virtual_bridge


def _result_collector_capabilities(
    result: OnboardingResult | None,
) -> CollectorCapabilityProfile:
    """Return collector capabilities inferred from one onboarding result."""

    if result is None:
        return collector_capability_profile_from_runtime()
    collector = getattr(result, "collector", None)
    collector_info = getattr(collector, "collector", None)
    match = getattr(result, "match", None)
    details = getattr(match, "details", None)
    values = dict(details) if isinstance(details, dict) else {}
    if match is not None:
        values.setdefault("driver_key", getattr(match, "driver_key", ""))
        values.setdefault("model_name", getattr(match, "model_name", ""))
        values.setdefault("serial_number", getattr(match, "serial_number", ""))
    return collector_capability_profile_from_runtime(
        collector=collector_info,
        values=values,
        data={},
        options={},
    )


def _apply_smartess_detection_metadata(
    data: dict[str, Any],
    result: OnboardingResult | None,
) -> None:
    """Persist SmartESS onboarding metadata when the probe captured it."""

    if result is None:
        return

    collector_info = (
        result.collector.collector if result.collector is not None else None
    )
    match_details = result.match.details if result.match is not None else {}

    def _pick(detail_key: str, collector_attr: str) -> Any:
        value = match_details.get(detail_key)
        if value not in (None, ""):
            return value
        if collector_info is None:
            return None
        value = getattr(collector_info, collector_attr, None)
        if value in (None, ""):
            return None
        return value

    mapping = (
        (
            CONF_SMARTESS_COLLECTOR_VERSION,
            "smartess_collector_version",
            "smartess_collector_version",
        ),
        (
            CONF_SMARTESS_PROTOCOL_ASSET_ID,
            "smartess_protocol_asset_id",
            "smartess_protocol_asset_id",
        ),
        (
            CONF_SMARTESS_PROFILE_KEY,
            "smartess_profile_key",
            "smartess_protocol_profile_key",
        ),
        (
            CONF_SMARTESS_DEVICE_ADDRESS,
            "smartess_device_address",
            "smartess_device_address",
        ),
    )
    for config_key, detail_key, collector_attr in mapping:
        value = _pick(detail_key, collector_attr)
        if value is not None:
            data[config_key] = value


def _apply_collector_first_entry_semantics(data: dict[str, Any]) -> None:
    """Make a new normal entry authoritative only for its collector.

    Scan-time inverter matches are previews collected before the entry owns the
    proven collector session.  They must not become durable inverter identity or
    select the runtime driver.  The runtime performs that detection on the owned
    session and persists the resulting model, serial and driver through its
    existing identity writer.
    """

    data[CONF_DRIVER_HINT] = DRIVER_HINT_AUTO
    _clear_runtime_inverter_facts(data)


def _apply_confirmed_session_protocol_evidence(
    data: dict[str, Any],
    result: OnboardingResult | None,
) -> None:
    """Persist exact-session wire evidence already proven during admission.

    A protocol-shaped string or a cloud family is not evidence.  This accepts
    only the exact typed observed session carried by the admission result, a
    strong identity source, and a PN matching the durable entry identity.  The
    resulting four-field record is the same trust boundary runtime writes after
    its own live observation, allowing a fully-silent reconnect to register the
    correct identity reader immediately on first setup.
    """

    if result is None or type(result.observed_session) is not ObservedCollectorSession:
        return
    observed = result.observed_session
    entry_pn = data.get(CONF_COLLECTOR_PN)
    if (
        type(entry_pn) is not str
        or not entry_pn
        or entry_pn != entry_pn.strip()
        or not identity_source_is_strong(observed.identity_source)
        or not pn_is_same_identity(entry_pn, observed.collector_pn)
    ):
        return
    protocol = collector_session_protocol_from_inventory_state(
        state="",
        protocol_shape=observed.protocol_shape,
    )
    if not protocol:
        return
    data.update(
        {
            CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL: protocol,
            CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE: (
                COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE
            ),
            CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN: reconcile_pn(
                entry_pn, observed.collector_pn
            ),
            CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT: (
                datetime.now(timezone.utc).isoformat()
            ),
        }
    )


def _apply_collector_cloud_family_metadata(
    data: dict[str, Any],
    result: OnboardingResult | None,
) -> None:
    """Persist the collector cloud family learned from CLDSRVHOST1/onboarding."""

    if result is None:
        return
    match_details = result.match.details if result.match is not None else {}
    family = str(match_details.get("collector_cloud_family") or "").strip()
    if (
        not family
        and result.collector is not None
        and result.collector.collector is not None
    ):
        family = str(result.collector.collector.collector_cloud_family or "").strip()
    if family:
        data[CONF_COLLECTOR_CLOUD_FAMILY] = family


def _smartess_collector_firmware_version_for_result(
    result: OnboardingResult | None,
) -> str:
    if result is None:
        return ""
    match_details = result.match.details if result.match is not None else {}
    value = str(match_details.get("smartess_collector_version") or "").strip()
    if value:
        return value
    collector_info = (
        result.collector.collector if result.collector is not None else None
    )
    if collector_info is None:
        return ""
    return str(collector_info.smartess_collector_version or "").strip()
