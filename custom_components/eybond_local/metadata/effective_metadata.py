"""Resolve effective metadata names for runtime tooling and support flows."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import logging
from typing import Any

from ..const import (
    CONF_COLLECTOR_PN,
    CONF_SMARTESS_DEVICE_ADDRESS,
    CONF_SMARTESS_PROFILE_KEY,
    CONF_SMARTESS_PROTOCOL_ASSET_ID,
)
from ..models import CollectorInfo
from ..runtime_labels import runtime_path_label
from .profile_loader import (
    DriverProfileMetadata,
    builtin_base_profile_name,
    load_driver_profile,
    load_driver_profile_raw,
)
from .register_schema_loader import builtin_base_schema_name, load_register_schema
from .register_schema_models import RegisterSchemaMetadata
from .effective_metadata_snapshot import EffectiveMetadataSnapshot
from .smartess_protocol_catalog_loader import (
    SmartEssProtocolCatalogEntry,
    resolve_smartess_protocol_catalog_entry,
)

_LOGGER = logging.getLogger(__name__)

_DEVICE_SCOPED_OVERLAY_ACTIVATION_KEY = "device_scoped_overlay_activation"


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
    device_scoped_overlay_active: bool = False
    device_scoped_overlay_scope: str = ""
    device_scoped_overlay_profile_name: str = ""
    device_scoped_overlay_register_schema_name: str = ""
    device_scoped_overlay_selected_control_keys: frozenset[str] | None = None


def resolve_effective_metadata_selection(
    *,
    inverter: Any = None,
    driver: Any = None,
    collector: CollectorInfo | None = None,
    entry_data: Mapping[str, Any] | None = None,
    entry_options: Mapping[str, Any] | None = None,
    persisted_snapshot: EffectiveMetadataSnapshot | None = None,
) -> EffectiveMetadataSelection:
    """Resolve effective metadata names from runtime state and SmartESS hints."""

    smartess_protocol = _resolve_smartess_protocol_hint(
        collector=collector,
        entry_data=entry_data,
    )

    snapshot_profile_metadata, snapshot_register_schema_metadata = _resolve_snapshot_metadata(
        persisted_snapshot
    )

    profile_name = _normalized_name(getattr(inverter, "profile_name", ""))
    if not profile_name and snapshot_profile_metadata is not None:
        profile_name = _normalized_name(getattr(snapshot_profile_metadata, "source_name", ""))
    if not profile_name and inverter is None:
        # An EMPTY profile on a *detected* inverter is authoritative: the
        # catalog binds partial / unidentified tiers to profile_name="" on
        # purpose (base reads, controls locked until learning). Only synthesize
        # the driver's full default profile when there is no detected inverter
        # at all -- otherwise a partial-tier device would silently inherit the
        # complete control set and overlay generation would dedupe against the
        # wrong base.
        profile_name = _normalized_name(getattr(driver, "profile_name", ""))

    register_schema_name = _normalized_name(getattr(inverter, "register_schema_name", ""))
    if not register_schema_name and snapshot_register_schema_metadata is not None:
        register_schema_name = _normalized_name(
            getattr(snapshot_register_schema_metadata, "source_name", "")
        )
    if not register_schema_name:
        register_schema_name = _normalized_name(getattr(driver, "register_schema_name", ""))

    activated_profile_name, activated_register_schema_name = _resolve_activated_device_overlay_names(
        inverter=inverter,
        collector=collector,
        entry_data=entry_data,
        entry_options=entry_options,
        effective_owner_key=_normalized_name(getattr(inverter, "driver_key", ""))
        or _normalized_name(getattr(driver, "key", "")),
        base_profile_name=profile_name,
        base_register_schema_name=register_schema_name,
        smartess_protocol=smartess_protocol,
    )
    device_scoped_overlay_active = bool(
        activated_profile_name and activated_register_schema_name
    )
    device_scoped_overlay_selected_control_keys: frozenset[str] | None = None
    if device_scoped_overlay_active:
        profile_name = activated_profile_name
        register_schema_name = activated_register_schema_name
        device_scoped_overlay_selected_control_keys = _resolve_overlay_selected_control_keys(
            entry_data, entry_options
        )

    profile_metadata = snapshot_profile_metadata
    register_schema_metadata = snapshot_register_schema_metadata
    if device_scoped_overlay_active:
        # The persisted snapshot reflects the built-in base (it records base names), so its
        # metadata lacks the activated learned capabilities. When an overlay is active the
        # effective profile/schema ARE the activated learned names -- reload them instead of
        # reusing the snapshot's base metadata, otherwise ``profile_metadata`` would carry
        # zero device-scoped controls and nothing would merge into the runtime inverter.
        profile_metadata = None
        register_schema_metadata = None
    if profile_metadata is None and profile_name:
        profile_metadata = load_driver_profile(profile_name)
    if register_schema_metadata is None and register_schema_name:
        register_schema_metadata = load_register_schema(register_schema_name)

    effective_owner_key = _normalized_name(getattr(inverter, "driver_key", ""))
    if not effective_owner_key and snapshot_profile_metadata is not None:
        effective_owner_key = _normalized_name(persisted_snapshot.effective_owner_key)
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
        device_scoped_overlay_active=device_scoped_overlay_active,
        device_scoped_overlay_scope="device" if device_scoped_overlay_active else "",
        device_scoped_overlay_profile_name=activated_profile_name,
        device_scoped_overlay_register_schema_name=activated_register_schema_name,
        device_scoped_overlay_selected_control_keys=device_scoped_overlay_selected_control_keys,
    )


def _resolve_activated_device_overlay_names(
    *,
    inverter: Any,
    collector: CollectorInfo | None,
    entry_data: Mapping[str, Any] | None,
    entry_options: Mapping[str, Any] | None,
    effective_owner_key: str,
    base_profile_name: str,
    base_register_schema_name: str,
    smartess_protocol: SmartEssProtocolCatalogEntry | None,
) -> tuple[str, str]:
    activation = _device_scoped_overlay_activation(entry_data, entry_options)
    profile_name = _normalized_name(activation.get("profile_name", ""))
    register_schema_name = _normalized_name(activation.get("register_schema_name", ""))
    if not profile_name or not register_schema_name:
        return "", ""

    try:
        profile_metadata = load_driver_profile(profile_name)
        register_schema_metadata = load_register_schema(register_schema_name)
    except FileNotFoundError:
        return "", ""
    except Exception:
        # A corrupt or invalid activated overlay must never brick the
        # integration: fall back to the base profile/schema instead of letting
        # the error propagate into every coordinator update.
        _LOGGER.warning(
            "Activated device overlay could not be loaded (profile=%s, schema=%s); "
            "falling back to the base profile",
            profile_name,
            register_schema_name,
            exc_info=True,
        )
        return "", ""

    if _normalized_name(getattr(profile_metadata, "source_scope", "")) != "external":
        return "", ""
    if _normalized_name(getattr(register_schema_metadata, "source_scope", "")) != "external":
        return "", ""

    manifest = _overlay_manifest_from_activation(activation, profile_name)
    if not _device_scope_matches_runtime(
        manifest=manifest,
        activation=activation,
        inverter=inverter,
        collector=collector,
        entry_data=entry_data,
        effective_owner_key=effective_owner_key,
        base_profile_name=base_profile_name,
        base_register_schema_name=base_register_schema_name,
        smartess_protocol=smartess_protocol,
    ):
        return "", ""

    return profile_name, register_schema_name


def _device_scoped_overlay_activation(
    entry_data: Mapping[str, Any] | None,
    entry_options: Mapping[str, Any] | None,
) -> dict[str, Any]:
    for container in (entry_options, entry_data):
        if not isinstance(container, Mapping):
            continue
        raw = container.get(_DEVICE_SCOPED_OVERLAY_ACTIVATION_KEY)
        if isinstance(raw, Mapping):
            return {str(key): value for key, value in raw.items()}
    return {}


def _resolve_overlay_selected_control_keys(
    entry_data: Mapping[str, Any] | None,
    entry_options: Mapping[str, Any] | None,
) -> frozenset[str] | None:
    """Resolve the user-selected control keys recorded on an active overlay activation.

    Returns ``None`` when the activation declares no selection (a legacy activation that
    predates selected-control activation), so runtime keeps exposing every learned control
    for backward compatibility. Returns a (possibly empty) frozenset when a selection is
    present: only those keys are exposed, and an empty selection exposes none. The keys are
    read from ``selected_control_keys`` when available, otherwise derived from the recorded
    ``selected_controls`` list.
    """

    activation = _device_scoped_overlay_activation(entry_data, entry_options)
    raw_keys = activation.get("selected_control_keys")
    if raw_keys is None:
        selected_controls = activation.get("selected_controls")
        if not isinstance(selected_controls, (list, tuple)):
            return None
        raw_keys = [
            control.get("key")
            for control in selected_controls
            if isinstance(control, Mapping)
        ]
    if not isinstance(raw_keys, (list, tuple, set, frozenset)):
        return None
    return frozenset(str(key).strip() for key in raw_keys if str(key or "").strip())


def _overlay_manifest_from_activation(
    activation: Mapping[str, Any],
    profile_name: str,
) -> dict[str, Any]:
    raw_manifest = activation.get("shadow_learning_overlay")
    if isinstance(raw_manifest, Mapping):
        return {str(key): value for key, value in raw_manifest.items()}
    try:
        raw_profile = load_driver_profile_raw(profile_name)
    except FileNotFoundError:
        return {}
    manifest = raw_profile.get("shadow_learning_overlay")
    if not isinstance(manifest, Mapping):
        return {}
    return {str(key): value for key, value in manifest.items()}


def _device_scope_matches_runtime(
    *,
    manifest: Mapping[str, Any],
    activation: Mapping[str, Any],
    inverter: Any,
    collector: CollectorInfo | None,
    entry_data: Mapping[str, Any] | None,
    effective_owner_key: str,
    base_profile_name: str,
    base_register_schema_name: str,
    smartess_protocol: SmartEssProtocolCatalogEntry | None,
) -> bool:
    if _normalized_name(manifest.get("scope", "")) not in {"", "device"}:
        return False

    source_profile_name = _normalized_name(manifest.get("source_profile_name", ""))
    if source_profile_name and source_profile_name != _normalized_name(base_profile_name):
        return False
    source_schema_name = _normalized_name(manifest.get("source_schema_name", ""))
    if source_schema_name and source_schema_name != _normalized_name(base_register_schema_name):
        return False

    session = manifest.get("session")
    if isinstance(session, Mapping):
        collector_pn_expected = _normalized_name(session.get("collector_pn", ""))
        collector_pn_actual = _normalized_name(
            getattr(collector, "collector_pn", "")
            or (entry_data or {}).get(CONF_COLLECTOR_PN, "")
        )
        # Prefix-tolerant compare: the datalogger PN is reported as a short physical
        # prefix (e.g. "E5000020000000") early in the handshake and upgraded to the
        # full PN (e.g. "E50000200000000001") once more frames arrive
        # (``_prefer_more_complete_collector_pn``). An exact compare here intermittently
        # failed the device-scope match during the early refreshes that gate entity
        # setup, so the activated overlay was suppressed and its learned controls never
        # materialized. One value being a prefix of the other is the same datalogger.
        if (
            collector_pn_expected
            and collector_pn_actual
            and collector_pn_expected != collector_pn_actual
            and not collector_pn_expected.startswith(collector_pn_actual)
            and not collector_pn_actual.startswith(collector_pn_expected)
        ):
            return False

        # NOTE: the session here is the *SmartESS cloud* device identity, which
        # does not map cleanly onto the local runtime, so only ``collector_pn``
        # (the physical datalogger PN — identical in both namespaces) is gated on.
        # The following SmartESS fields are intentionally NOT compared, because
        # the runtime does not carry a matching value and gating on them silently
        # suppressed every activated overlay so the learned control never appeared:
        #   - ``cloud_sn``  : SmartESS device serial (e.g. "…000001"), never equal
        #                     to inverter.serial_number (the Modbus serial).
        #   - ``devcode``   : SmartESS device type (e.g. 2376); the runtime
        #                     collector reports an unrelated ``last_devcode`` (1).
        #   - ``devaddr``   : SmartESS device address (1); often unknown locally.
        # The device is pinned reliably by collector_pn (above) plus the
        # activation_scope identity (inverter serial / model / owner / base
        # profile + schema), all of which are runtime-vs-runtime comparisons.

    activation_scope = activation.get("activation_scope")
    if isinstance(activation_scope, Mapping):
        # Skipped when the runtime owner key is not yet known (empty) -- e.g. when the
        # live inverter has not been detected at entity-setup time, so the snapshot-backed
        # inverter (built from effective_metadata) needs the overlay to resolve active to
        # receive its learned capabilities. Only a concrete mismatch rejects the overlay.
        expected_owner_key = _normalized_name(activation_scope.get("effective_owner_key", ""))
        runtime_owner_key = _normalized_name(effective_owner_key)
        if expected_owner_key and runtime_owner_key and expected_owner_key != runtime_owner_key:
            return False
        # Both sides are rebased to their built-in base before comparison. When an
        # overlay is activated while another overlay is already active, the captured
        # ``base_profile_name``/``base_register_schema_name`` are the *previous*
        # overlay's learned names (taken from ``effective_*_name``), not the built-in
        # base. Comparing those raw against the post-reload runtime base (the built-in
        # name) would always fail and silently suppress the activation. Rebasing both
        # sides self-heals such poisoned activations and keeps clean ones matching.
        expected_base_profile = _normalized_name(
            builtin_base_profile_name(activation_scope.get("base_profile_name", ""))
        )
        if expected_base_profile and expected_base_profile != _normalized_name(
            builtin_base_profile_name(base_profile_name)
        ):
            return False
        expected_base_schema = _normalized_name(
            builtin_base_schema_name(activation_scope.get("base_register_schema_name", ""))
        )
        if expected_base_schema and expected_base_schema != _normalized_name(
            builtin_base_schema_name(base_register_schema_name)
        ):
            return False
        expected_asset_id = _normalized_protocol_asset_id(
            activation_scope.get("smartess_protocol_asset_id", "")
        )
        runtime_asset_id = _normalized_name(
            getattr(smartess_protocol, "asset_id", "")
            or getattr(collector, "smartess_protocol_asset_id", "")
            or (entry_data or {}).get(CONF_SMARTESS_PROTOCOL_ASSET_ID, "")
        )
        if expected_asset_id and expected_asset_id != runtime_asset_id:
            return False
        expected_profile_key = _normalized_name(
            activation_scope.get("smartess_protocol_profile_key", "")
        )
        runtime_profile_key = _normalized_name(
            getattr(smartess_protocol, "profile_key", "")
            or getattr(collector, "smartess_protocol_profile_key", "")
            or (entry_data or {}).get(CONF_SMARTESS_PROFILE_KEY, "")
        )
        if expected_profile_key and expected_profile_key != runtime_profile_key:
            return False
        # ``variant_key`` and ``inverter_model`` are skipped when the runtime value is
        # not yet known (empty). Entity platforms are set up right after the activation
        # reload, often before the live inverter is detected (e.g. while the collector
        # reconnects after a shadow-learning scan). At that moment ``inverter`` is the
        # snapshot-backed placeholder (or None), so ``model_name``/``variant_key`` are
        # empty -- and gating on them would suppress the overlay at exactly the moment
        # entities are created, so the learned control never appears. The snapshot-backed
        # inverter is itself built from ``effective_metadata``, so the overlay must stay
        # active here for the learned capabilities to flow into it. Only a concrete
        # mismatch (both sides known and different) rejects the overlay; the device is
        # still pinned by collector_pn + effective_owner_key + base profile/schema.
        expected_variant_key = _normalized_name(activation_scope.get("variant_key", ""))
        runtime_variant_key = _normalized_name(getattr(inverter, "variant_key", ""))
        if (
            expected_variant_key
            and runtime_variant_key
            and expected_variant_key != runtime_variant_key
        ):
            return False
        expected_inverter_model = _normalized_name(activation_scope.get("inverter_model", ""))
        runtime_inverter_model = _normalized_name(getattr(inverter, "model_name", ""))
        if (
            expected_inverter_model
            and runtime_inverter_model
            and expected_inverter_model != runtime_inverter_model
        ):
            return False
        # NOTE: ``inverter_serial`` is intentionally NOT gated on. The serial is read
        # from a Modbus holding register and is not reliably populated on every
        # coordinator update (it can be empty during early refreshes, before the
        # identity registers are read). Because the overlay is resolved on every
        # update -- including those early ones that run while entities are first set
        # up -- gating on the serial intermittently suppressed the activated overlay,
        # so the learned control never materialized as an entity. The device is
        # already pinned by collector_pn + effective_owner_key + base profile/schema
        # + inverter_model + variant_key (all above), which are stable at resolve
        # time, so dropping the serial gate does not widen the match in practice.
        expected_device_address = _to_optional_int(
            activation_scope.get("smartess_device_address")
        )
        runtime_device_address = _to_optional_int(
            getattr(collector, "smartess_device_address", None)
            if collector is not None
            else (entry_data or {}).get(CONF_SMARTESS_DEVICE_ADDRESS)
        )
        if expected_device_address is not None and expected_device_address != runtime_device_address:
            return False

    return True


def _to_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


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


def _resolve_snapshot_metadata(
    snapshot: EffectiveMetadataSnapshot | None,
) -> tuple[DriverProfileMetadata | None, RegisterSchemaMetadata | None]:
    if snapshot is None or not snapshot.is_valid:
        return None, None
    try:
        profile_metadata = load_driver_profile(snapshot.profile_name)
        register_schema_metadata = load_register_schema(snapshot.register_schema_name)
    except FileNotFoundError:
        return None, None
    return profile_metadata, register_schema_metadata


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


def _normalized_protocol_asset_id(value: Any) -> str:
    """Normalize protocol asset ids used for device-overlay scope matching.

    Some AT collectors report ``0000`` as a placeholder protocol id when no real
    SmartESS protocol asset has been resolved. Treating that placeholder as a
    concrete identity silently suppresses activated learned overlays after reload
    because the runtime often has the equivalent value as an empty string.
    """

    normalized = _normalized_name(value)
    return "" if normalized in {"0", "00", "000", "0000"} else normalized


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
