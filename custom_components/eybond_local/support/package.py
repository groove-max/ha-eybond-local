"""Support archive export helpers for unsupported or partially supported inverters."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any
import zipfile

from ..const import (
    LOCAL_METADATA_DIR,
    LOCAL_PROFILES_DIR,
    LOCAL_REGISTER_SCHEMAS_DIR,
    LOCAL_SUPPORT_PACKAGES_DIR,
)
from .bundle import build_support_marker
from .masking import mask_numeric_identifiers
from .shadow_learning_review_model import build_control_discovery_evidence


_CLOUD_EVIDENCE_ARCHIVE_MEMBER = "evidence/cloud_evidence.json"
_SHADOW_TRACE_DIR = "shadow_learning_traces"
_SHADOW_ARCHIVE_ROOT = "evidence/shadow_learning"
_SHADOW_TRACE_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/trace.jsonl"
_SHADOW_EVENTS_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/events.jsonl"
_SHADOW_WRITES_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/writes.jsonl"
_SHADOW_SESSION_MANIFEST_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/session_manifest.json"
_SHADOW_LEARN_PLAN_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/learn_plan.json"
_SHADOW_ORCHESTRATION_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/orchestration.json"
_SHADOW_CORRELATION_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/correlation_report.json"
_SHADOW_OVERLAY_PROFILE_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/generated_overlay_profile.json"
_SHADOW_OVERLAY_SCHEMA_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/generated_overlay_schema.json"
_SHADOW_ACTIVATION_MANIFEST_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/activation_manifest.json"
_SHADOW_CONTROL_DISCOVERY_MEMBER = f"{_SHADOW_ARCHIVE_ROOT}/control_discovery.json"
_DEVICE_SCOPED_OVERLAY_ACTIVATION_OPTION_KEY = "device_scoped_overlay_activation"

_SENSITIVE_FIELD_PARTS = ("password", "secret", "token", "authorization")


@dataclass(frozen=True, slots=True)
class SupportPackageExportResult:
    """One exported support archive plus its optional HA download URL."""

    path: Path
    download_path: Path | None = None
    download_url: str | None = None


def support_packages_root(config_dir: Path) -> Path:
    """Return the support package output directory."""

    return config_dir / LOCAL_METADATA_DIR / LOCAL_SUPPORT_PACKAGES_DIR


def support_packages_public_root(config_dir: Path) -> Path:
    """Return the Home Assistant static file directory for support packages."""

    return config_dir / "www" / LOCAL_METADATA_DIR / LOCAL_SUPPORT_PACKAGES_DIR


def support_package_download_url(filename: str) -> str:
    """Return the Home Assistant `/local` URL for one exported support package."""

    return f"/local/{LOCAL_METADATA_DIR}/{LOCAL_SUPPORT_PACKAGES_DIR}/{filename}"


def export_support_package(
    *,
    config_dir: Path,
    entry_id: str,
    entry_title: str,
    support_bundle: dict[str, Any],
    raw_capture: dict[str, Any] | None,
    fixture: dict[str, Any] | None,
    anonymized_fixture: dict[str, Any] | None,
    profile_source: dict[str, Any] | None = None,
    register_schema_source: dict[str, Any] | None = None,
    overwrite: bool = False,
    publish_download_copy: bool = False,
) -> SupportPackageExportResult:
    """Write one combined support archive and optionally publish one `/local` copy."""

    cloud_evidence = _support_bundle_cloud_evidence(support_bundle)
    support_marker = _support_bundle_support_marker(support_bundle)
    archived_support_bundle = _archive_support_bundle_payload(support_bundle)
    shadow_members, shadow_manifest_members = _build_shadow_learning_archive_members(
        config_dir=config_dir,
        support_bundle=support_bundle,
    )

    packages_root = support_packages_root(config_dir)
    packages_root.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = packages_root / f"{entry_id}_{timestamp}.zip"
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)

    created_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "archive_version": 2,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "entry": {
            "entry_id": entry_id,
            "title": entry_title,
        },
        "effective_metadata": {
            "profile_source": profile_source,
            "register_schema_source": register_schema_source,
        },
        "support_marker": support_marker,
        "sharing_guidance": {
            "recommended_artifact": destination.name,
            "note": (
                "Send this ZIP file to the developer. It includes runtime metadata, "
                "capture evidence, an anonymized replay fixture, and any "
                "matching cloud evidence exported into the HA config dir. Long "
                "numeric identifiers (collector PN, serial numbers) are masked "
                "in every member."
            ),
        },
        "archive_members": {
            "support_bundle": "support_bundle.json",
            "raw_capture": "raw_capture.json" if raw_capture is not None else None,
            "raw_fixture": "fixture/raw_fixture.json" if fixture is not None else None,
            "anonymized_fixture": (
                "fixture/anonymized_fixture.json"
                if anonymized_fixture is not None
                else None
            ),
            "cloud_evidence": _CLOUD_EVIDENCE_ARCHIVE_MEMBER if cloud_evidence is not None else None,
            "shadow_learning": shadow_manifest_members or None,
        },
    }

    readme_lines = [
        "EyeBond Local Support Archive",
        "",
        f"Created at: {created_at}",
        f"Entry: {entry_title} ({entry_id})",
    ]
    if support_marker is not None:
        readme_lines.extend(
            [
                "",
                f"Support marker: {support_marker['label']}",
                str(support_marker.get("summary") or ""),
            ]
        )
    readme_lines.extend(
        [
            "",
            "Send this ZIP file to the developer. The main files are:",
            "- manifest.json",
            "- support_bundle.json (includes explicit collector, inverter, and integration role sections)",
        ]
    )
    if raw_capture is not None:
        readme_lines.append("- raw_capture.json")
    if anonymized_fixture is not None:
        readme_lines.append("- fixture/anonymized_fixture.json")
    if fixture is not None:
        readme_lines.append("- fixture/raw_fixture.json")
    if cloud_evidence is not None:
        readme_lines.append("- evidence/cloud_evidence.json")
    if shadow_manifest_members:
        readme_lines.append(f"- {_SHADOW_ARCHIVE_ROOT}/... (optional shadow-learning evidence)")
    if shadow_manifest_members.get("control_discovery"):
        readme_lines.append(
            f"- {_SHADOW_CONTROL_DISCOVERY_MEMBER} "
            "(all discovered controls, selected/excluded subsets, user labels, risk reasons)"
        )
    readme_lines.extend(
        [
            "",
            "When cloud evidence is present, support_bundle.json references evidence/cloud_evidence.json instead of duplicating the full payload.",
            "",
            "raw_capture.json may include supplemental family-level discovery ranges when the driver collects extra evidence for nearby variants.",
        ]
    )
    if shadow_manifest_members:
        readme_lines.extend(
            [
                "",
                "When shadow-learning artifacts are available locally, the archive adds optional evidence/shadow_learning members for trace events, write observations, generated overlays, and activation metadata.",
            ]
        )

    archive_members = {
        "manifest.json": manifest,
        "support_bundle.json": archived_support_bundle,
        "raw_capture.json": raw_capture,
        _CLOUD_EVIDENCE_ARCHIVE_MEMBER: cloud_evidence,
        "fixture/raw_fixture.json": fixture,
        "fixture/anonymized_fixture.json": anonymized_fixture,
        "README.txt": "\n".join(readme_lines) + "\n",
    }
    archive_members.update(shadow_members)

    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for member_name, payload in archive_members.items():
            if payload is None:
                continue
            # The whole archive is a sharing artifact ("send this ZIP to the
            # developer"), so every member gets the same identifier masking
            # the diagnostic exports use — a PN must not be starred out in
            # one file and printed in full in the next.  JSONL members are
            # masked per parsed record: masking their raw text would also
            # star out epoch timestamps and other legitimate long numbers.
            if isinstance(payload, str) and member_name.endswith(".jsonl"):
                payload = _mask_jsonl_text(payload)
            else:
                payload = mask_numeric_identifiers(payload)
            if member_name == "manifest.json":
                # The archive's own filename carries a compact timestamp that
                # the identifier mask would star out, leaving the manifest
                # pointing at a file that does not exist.
                payload["sharing_guidance"]["recommended_artifact"] = destination.name
            if isinstance(payload, str):
                archive.writestr(member_name, payload)
                continue
            archive.writestr(
                member_name,
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=False,
                    default=_json_default,
                )
                + "\n",
            )

    if not publish_download_copy:
        return SupportPackageExportResult(path=destination)

    public_root = support_packages_public_root(config_dir)
    public_root.mkdir(parents=True, exist_ok=True)
    public_destination = public_root / destination.name
    shutil.copy2(destination, public_destination)
    return SupportPackageExportResult(
        path=destination,
        download_path=public_destination,
        download_url=support_package_download_url(destination.name),
    )


def _support_bundle_cloud_evidence(support_bundle: dict[str, Any]) -> dict[str, Any] | None:
    evidence = support_bundle.get("evidence") if isinstance(support_bundle, dict) else None
    cloud_evidence = evidence.get("cloud") if isinstance(evidence, dict) else None
    return cloud_evidence if isinstance(cloud_evidence, dict) else None


def _support_bundle_support_marker(
    support_bundle: dict[str, Any],
) -> dict[str, Any] | None:
    source_metadata = support_bundle.get("source_metadata") if isinstance(support_bundle, dict) else None
    if not isinstance(source_metadata, dict):
        return None

    support_marker = source_metadata.get("support_marker")
    if isinstance(support_marker, dict):
        return support_marker

    return build_support_marker(
        variant_key=str(source_metadata.get("variant_key", "") or ""),
        profile_name=str(source_metadata.get("profile_name", "") or ""),
        effective_owner_key=str(source_metadata.get("effective_owner_key", "") or ""),
    )


def _archive_support_bundle_payload(support_bundle: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(support_bundle, dict):
        return support_bundle

    archived_payload = deepcopy(support_bundle)
    evidence = archived_payload.get("evidence")
    if not isinstance(evidence, dict):
        return archived_payload

    cloud_evidence = evidence.get("cloud")
    if cloud_evidence is None:
        return archived_payload

    evidence["cloud"] = {
        "archive_member": _CLOUD_EVIDENCE_ARCHIVE_MEMBER,
    }
    return archived_payload


def _build_shadow_learning_archive_members(
    *,
    config_dir: Path,
    support_bundle: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    if not isinstance(support_bundle, dict):
        return {}, {}

    members: dict[str, Any] = {}
    manifest_members: dict[str, str] = {}

    runtime_values = _dict_value(_dict_value(support_bundle, "runtime"), "values")
    entry_payload = _dict_value(support_bundle, "entry")
    entry_options = _dict_value(entry_payload, "options")

    trace_path = _resolve_shadow_trace_path(
        config_dir=config_dir,
        support_bundle=support_bundle,
        runtime_values=runtime_values,
    )
    if trace_path is not None:
        trace_data = _load_shadow_trace_payload(trace_path)
        if trace_data is not None:
            members[_SHADOW_TRACE_MEMBER] = trace_data["raw_jsonl"]
            manifest_members["trace"] = _SHADOW_TRACE_MEMBER
        session_manifest = _load_shadow_session_manifest_from_trace(trace_path)
        if session_manifest is not None:
            members[_SHADOW_SESSION_MANIFEST_MEMBER] = session_manifest
            manifest_members["session_manifest"] = _SHADOW_SESSION_MANIFEST_MEMBER
        events = _load_shadow_events_from_trace(trace_path)
        if events:
            members[_SHADOW_EVENTS_MEMBER] = _to_jsonl(events)
            manifest_members["events"] = _SHADOW_EVENTS_MEMBER
        writes = _load_shadow_writes_from_trace(trace_path)
        if writes:
            members[_SHADOW_WRITES_MEMBER] = _to_jsonl(writes)
            manifest_members["writes"] = _SHADOW_WRITES_MEMBER

    activation_manifest = _dict_value(
        entry_options,
        _DEVICE_SCOPED_OVERLAY_ACTIVATION_OPTION_KEY,
    )
    if not activation_manifest:
        activation_manifest = _dict_value(
            runtime_values,
            "shadow_learning_activation",
        )
    overlay_profile_name = ""
    overlay_schema_name = ""
    if activation_manifest:
        sanitized_activation = _sanitize_payload(activation_manifest)
        members[_SHADOW_ACTIVATION_MANIFEST_MEMBER] = sanitized_activation
        manifest_members["activation_manifest"] = _SHADOW_ACTIVATION_MANIFEST_MEMBER
        overlay_profile_name = str(sanitized_activation.get("profile_name") or "")
        overlay_schema_name = str(sanitized_activation.get("register_schema_name") or "")

    overlay_profile_path = _resolve_overlay_profile_path(
        config_dir=config_dir,
        runtime_values=runtime_values,
        overlay_profile_name=overlay_profile_name,
    )
    overlay_schema_path = _resolve_overlay_schema_path(
        config_dir=config_dir,
        runtime_values=runtime_values,
        overlay_schema_name=overlay_schema_name,
    )
    overlay_profile_payload = _load_json_dict(overlay_profile_path) if overlay_profile_path is not None else None
    overlay_schema_payload = _load_json_dict(overlay_schema_path) if overlay_schema_path is not None else None
    if overlay_profile_payload is not None and "shadow_learning_overlay" in overlay_profile_payload:
        members[_SHADOW_OVERLAY_PROFILE_MEMBER] = _sanitize_payload(overlay_profile_payload)
        manifest_members["generated_overlay_profile"] = _SHADOW_OVERLAY_PROFILE_MEMBER
    if overlay_schema_payload is not None and "shadow_learning_overlay" in overlay_schema_payload:
        members[_SHADOW_OVERLAY_SCHEMA_MEMBER] = _sanitize_payload(overlay_schema_payload)
        manifest_members["generated_overlay_schema"] = _SHADOW_OVERLAY_SCHEMA_MEMBER

    review_model = _overlay_review_model(overlay_profile_payload)
    if review_model is None:
        review_model = _overlay_review_model(overlay_schema_payload)
    control_discovery = build_control_discovery_evidence(
        review_model=review_model,
        activation=activation_manifest or None,
    )
    discovery_counts = control_discovery.get("counts", {})
    if (
        discovery_counts.get("discovered")
        or discovery_counts.get("selected")
        or discovery_counts.get("excluded")
    ):
        members[_SHADOW_CONTROL_DISCOVERY_MEMBER] = _sanitize_payload(control_discovery)
        manifest_members["control_discovery"] = _SHADOW_CONTROL_DISCOVERY_MEMBER

    orchestration = _dict_value(runtime_values, "shadow_learning_orchestration")
    if orchestration:
        members[_SHADOW_ORCHESTRATION_MEMBER] = _sanitize_payload(orchestration)
        manifest_members["orchestration"] = _SHADOW_ORCHESTRATION_MEMBER
    correlation_report = _dict_value(runtime_values, "shadow_learning_correlation")
    if not correlation_report and orchestration:
        correlation_report = _dict_value(orchestration, "correlation")
    if not correlation_report and overlay_profile_payload is not None:
        overlay_manifest = _dict_value(overlay_profile_payload, "shadow_learning_overlay")
        summary = _dict_value(overlay_manifest, "correlation_summary")
        if summary:
            correlation_report = {
                "source": "overlay_manifest",
                "correlation_summary": summary,
            }
    if correlation_report:
        members[_SHADOW_CORRELATION_MEMBER] = _sanitize_payload(correlation_report)
        manifest_members["correlation_report"] = _SHADOW_CORRELATION_MEMBER

    learn_plan = _dict_value(runtime_values, "shadow_learning_plan")
    if not learn_plan:
        learn_plan = _dict_value(runtime_values, "shadow_learning_preview_plan")
    if not learn_plan and orchestration:
        results = orchestration.get("results") if isinstance(orchestration, dict) else None
        if isinstance(results, list):
            learn_plan = {
                "source": "orchestration_results",
                "items": results,
                "count": len(results),
            }
    if not learn_plan and overlay_profile_payload is not None:
        overlay_manifest = _dict_value(overlay_profile_payload, "shadow_learning_overlay")
        learned_capabilities = overlay_manifest.get("learned_capabilities") if isinstance(overlay_manifest, dict) else None
        if isinstance(learned_capabilities, list):
            learn_plan = {
                "source": "overlay_manifest",
                "items": learned_capabilities,
                "count": len(learned_capabilities),
            }
    if learn_plan:
        members[_SHADOW_LEARN_PLAN_MEMBER] = _sanitize_payload(learn_plan)
        manifest_members["learn_plan"] = _SHADOW_LEARN_PLAN_MEMBER

    return members, manifest_members


def _resolve_shadow_trace_path(
    *,
    config_dir: Path,
    support_bundle: dict[str, Any],
    runtime_values: dict[str, Any],
) -> Path | None:
    trace_root = Path(config_dir) / LOCAL_METADATA_DIR / _SHADOW_TRACE_DIR
    explicit_path = _safe_absolute_path(
        runtime_values.get("shadow_learning_trace_path"),
        must_exist=True,
        root=trace_root,
    )
    if explicit_path is not None and _shadow_trace_matches_support_bundle(
        explicit_path,
        support_bundle,
    ):
        return explicit_path

    if not trace_root.exists() or not trace_root.is_dir():
        return None

    entry_payload = _dict_value(support_bundle, "entry")
    runtime_collector = _dict_value(_dict_value(support_bundle, "runtime"), "collector")
    entry_id = _scalar_string(entry_payload, "entry_id")
    collector_pn = str(
        _scalar_string(runtime_collector, "collector_pn")
        or _scalar_string(_dict_value(entry_payload, "data"), "collector_pn")
    )
    stems = tuple(
        stem
        for stem in {
            _slugify(entry_id),
            _slugify(collector_pn),
        }
        if stem
    )
    candidates = sorted(
        [
            candidate
            for candidate in trace_root.glob("*.jsonl")
            if candidate.is_file()
            and (
                not stems
                or any(candidate.name.startswith(f"{stem}_") for stem in stems)
            )
        ],
        key=lambda candidate: candidate.stat().st_mtime,
        reverse=True,
    )
    for candidate in candidates:
        if _shadow_trace_matches_support_bundle(candidate, support_bundle):
            return candidate
    return None


def _shadow_trace_matches_support_bundle(
    trace_path: Path,
    support_bundle: dict[str, Any],
) -> bool:
    """Return whether one shadow-learning trace belongs to this support bundle."""

    manifest = _load_shadow_session_manifest_from_trace(trace_path)
    if not manifest:
        return False

    entry_payload = _dict_value(support_bundle, "entry")
    runtime_collector = _dict_value(_dict_value(support_bundle, "runtime"), "collector")
    expected_entry_id = _scalar_string(entry_payload, "entry_id")
    expected_collector_pn = (
        _scalar_string(runtime_collector, "collector_pn")
        or _scalar_string(_dict_value(entry_payload, "data"), "collector_pn")
    )

    manifest_entry_id = str(manifest.get("entry_id") or "").strip()
    if expected_entry_id and manifest_entry_id:
        return manifest_entry_id == expected_entry_id

    manifest_identities = {
        str(manifest.get(key) or "").strip()
        for key in ("collector_pn", "cloud_pn", "cloud_sn")
    }
    manifest_identities.discard("")
    if expected_collector_pn and manifest_identities:
        return expected_collector_pn in manifest_identities

    return False


def _load_shadow_trace_payload(trace_path: Path) -> dict[str, Any] | None:
    sanitized = _sanitize_jsonl_trace(trace_path)
    if not sanitized:
        return None
    return {"raw_jsonl": sanitized}


def _sanitize_jsonl_trace(trace_path: Path) -> str:
    records: list[dict[str, Any]] = []
    for line in _iter_jsonl_dicts(trace_path):
        records.append(_sanitize_payload(line))
    return _to_jsonl(records)


def _load_shadow_session_manifest_from_trace(trace_path: Path) -> dict[str, Any] | None:
    for line in _iter_jsonl_dicts(trace_path):
        if str(line.get("kind") or "") != "shadow_session_manifest":
            continue
        return _sanitize_payload({key: value for key, value in line.items() if key != "kind"})
    return None


def _load_shadow_events_from_trace(trace_path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in _iter_jsonl_dicts(trace_path):
        kind = str(line.get("kind") or "").strip()
        if not kind:
            continue
        events.append(
            _sanitize_payload(
                {
                    "timestamp": str(line.get("timestamp") or ""),
                    "kind": kind,
                    "direction": str(line.get("direction") or ""),
                    "payload": dict(line.get("payload") or {}),
                }
            )
        )
    return events


def _load_shadow_writes_from_trace(trace_path: Path) -> list[dict[str, Any]]:
    writes: list[dict[str, Any]] = []
    for line in _iter_jsonl_dicts(trace_path):
        if str(line.get("kind") or "") not in {
            "shadow_modbus_write_observation",
            "shadow_protocol_write_observation",
        }:
            continue
        payload = line.get("payload")
        if isinstance(payload, dict):
            writes.append(_sanitize_payload(payload))
    return writes


def _iter_jsonl_dicts(path: Path):
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                text = str(raw_line or "").strip()
                if not text:
                    continue
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    yield parsed
    except (FileNotFoundError, OSError, UnicodeDecodeError):
        return


def _resolve_overlay_profile_path(
    *,
    config_dir: Path,
    runtime_values: dict[str, Any],
    overlay_profile_name: str,
) -> Path | None:
    explicit = _safe_absolute_path(
        runtime_values.get("local_profile_draft_path"),
        must_exist=True,
        root=Path(config_dir) / LOCAL_METADATA_DIR / LOCAL_PROFILES_DIR,
    )
    if explicit is not None:
        return explicit
    return _resolve_local_metadata_path(
        config_dir=config_dir,
        subdir=LOCAL_PROFILES_DIR,
        relative_name=overlay_profile_name,
    )


def _resolve_overlay_schema_path(
    *,
    config_dir: Path,
    runtime_values: dict[str, Any],
    overlay_schema_name: str,
) -> Path | None:
    explicit = _safe_absolute_path(
        runtime_values.get("local_schema_draft_path"),
        must_exist=True,
        root=Path(config_dir) / LOCAL_METADATA_DIR / LOCAL_REGISTER_SCHEMAS_DIR,
    )
    if explicit is not None:
        return explicit
    return _resolve_local_metadata_path(
        config_dir=config_dir,
        subdir=LOCAL_REGISTER_SCHEMAS_DIR,
        relative_name=overlay_schema_name,
    )


def _resolve_local_metadata_path(
    *,
    config_dir: Path,
    subdir: str,
    relative_name: str,
) -> Path | None:
    normalized_name = str(relative_name or "").strip()
    if not normalized_name:
        return None
    root = (Path(config_dir) / LOCAL_METADATA_DIR / subdir).resolve()
    candidate = (root / normalized_name).resolve()
    if candidate != root and root not in candidate.parents:
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    return candidate


def _safe_absolute_path(
    value: Any,
    *,
    must_exist: bool,
    root: Path | None = None,
) -> Path | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    path = Path(normalized)
    if not path.is_absolute():
        return None
    path = path.resolve()
    if root is not None:
        resolved_root = Path(root).resolve()
        if path == resolved_root or resolved_root not in path.parents:
            return None
    if must_exist and (not path.exists() or not path.is_file()):
        return None
    return path


def _load_json_dict(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _json_default(value: Any) -> Any:
    """Best-effort JSON fallback so a stray non-serializable value can't fail an export.

    Runtime snapshots occasionally carry sets/frozensets (e.g. selected-control keys) or
    other container types; the support archive must still serialize rather than abort with
    "Object of type frozenset is not JSON serializable".
    """

    if isinstance(value, (set, frozenset)):
        return sorted(value, key=str)
    if isinstance(value, (tuple, list)):
        return list(value)
    return str(value)


def _mask_jsonl_text(text: str) -> str:
    """Mask identifiers per parsed JSONL record, preserving numeric fields.

    Numbers stay numbers (epoch timestamps, register words); only strings
    inside each record are masked. A line that fails to parse falls back to
    plain-text masking rather than passing through unmasked.
    """

    masked_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            masked_lines.append(line)
            continue
        try:
            record = json.loads(stripped)
        except ValueError:
            masked_lines.append(mask_numeric_identifiers(line))
            continue
        masked_lines.append(
            json.dumps(
                mask_numeric_identifiers(record),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                default=_json_default,
            )
        )
    return "\n".join(masked_lines) + ("\n" if text.endswith("\n") else "")


def _to_jsonl(records: list[dict[str, Any]]) -> str:
    return "".join(
        json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=_json_default,
        )
        + "\n"
        for record in records
    )


def _slugify(value: Any) -> str:
    text = "".join(
        character.lower() if character.isalnum() else "_"
        for character in str(value or "").strip()
    ).strip("_")
    return text or ""


def _dict_value(payload: dict[str, Any] | None, key: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def _scalar_string(payload: dict[str, Any] | None, key: str) -> str:
    if not isinstance(payload, dict):
        return ""
    value = payload.get(key)
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        return ""
    return str(value or "").strip()


def _overlay_review_model(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return the embedded learned-control review model from a generated overlay payload."""

    overlay = _dict_value(payload, "shadow_learning_overlay")
    review_model = overlay.get("review_model")
    return review_model if isinstance(review_model, dict) else None


def _sanitize_payload(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key or "").lower()
            if any(part in normalized_key for part in _SENSITIVE_FIELD_PARTS):
                continue
            sanitized[str(key)] = _sanitize_payload(item)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_payload(item) for item in value]
    return value


def sanitize_shadow_learning_artifact_value(value: Any) -> Any:
    """Return a credential-free JSON-compatible shadow-learning artifact value."""

    return _sanitize_payload(value)


def build_shadow_learning_runtime_values(
    *,
    plan: dict[str, Any] | None = None,
    orchestration: dict[str, Any] | None = None,
    correlation: dict[str, Any] | None = None,
    trace_path: str = "",
    profile_draft_path: str = "",
    schema_draft_path: str = "",
    activation: dict[str, Any] | None = None,
    session_id: str = "",
    device_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build sanitized runtime values consumed by support-package assembly."""

    sanitized_plan = sanitize_shadow_learning_artifact_value(plan or {})
    sanitized_orchestration = sanitize_shadow_learning_artifact_value(
        orchestration or {}
    )
    sanitized_correlation = sanitize_shadow_learning_artifact_value(
        correlation
        or (
            sanitized_orchestration.get("correlation")
            if isinstance(sanitized_orchestration, dict)
            else {}
        )
        or {}
    )
    sanitized_activation = sanitize_shadow_learning_artifact_value(activation or {})
    sanitized_scope = sanitize_shadow_learning_artifact_value(device_scope or {})
    bundle = {
        "session_id": str(session_id or "").strip(),
        "device_scope": sanitized_scope,
        "trace_path": str(trace_path or "").strip(),
        "plan": sanitized_plan,
        "orchestration": sanitized_orchestration,
        "correlation": sanitized_correlation,
        "profile_draft_path": str(profile_draft_path or "").strip(),
        "schema_draft_path": str(schema_draft_path or "").strip(),
        "activation": sanitized_activation,
    }
    values: dict[str, Any] = {
        "shadow_learning_artifacts": bundle,
        "shadow_learning_session_id": bundle["session_id"],
        "shadow_learning_device_scope": sanitized_scope,
        "shadow_learning_plan": sanitized_plan,
        "shadow_learning_orchestration": sanitized_orchestration,
        "shadow_learning_correlation": sanitized_correlation,
        "shadow_learning_activation": sanitized_activation,
    }
    if bundle["trace_path"]:
        values["shadow_learning_trace_path"] = bundle["trace_path"]
    if bundle["profile_draft_path"]:
        values["local_profile_draft_path"] = bundle["profile_draft_path"]
    if bundle["schema_draft_path"]:
        values["local_schema_draft_path"] = bundle["schema_draft_path"]
    return values
