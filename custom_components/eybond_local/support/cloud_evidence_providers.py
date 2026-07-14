"""Provider-neutral cloud-evidence contract + registry.

The runtime coordinator owns Home Assistant orchestration (executor jobs, config
entry I/O, notifications, presenting results). It must NOT own provider policy:
which cloud families map to which provider, how a provider's evidence is fetched,
how a provider's evidence resolves into a learning/draft candidate, or any
SmartESS/ValueCloud/SMG interpretation. All of that lives behind one neutral
contract here.

Selecting one provider can never execute another provider's code: each
implementation calls ONLY its own provider's building blocks
(``fetch_and_export_smartess_*`` vs ``fetch_and_export_valuecloud_*``), and an
unknown provider resolves to a fail-closed implementation.

Credentials are ephemeral method arguments used only for the fetch; they are
never stored on an implementation and never reach ``diagnostics``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..metadata.smartess_draft import (
    create_smartess_known_family_draft,
    resolve_smartess_known_family_draft_plan,
)
from ..smartess_cloud import classify_smartess_cloud_error
from ..metadata.smartess_onboarding import (
    SMARTESS_ONBOARDING_SOURCE,
    build_smartess_onboarding_assist,
)
from ..metadata.smartess_smg_bridge import (
    create_smartess_smg_bridge_draft,
    resolve_smartess_smg_bridge_plan,
)
from .cloud_control_discovery import (
    CloudControlDiscoveryRunner,
    SmartEssControlDiscoveryRunner,
    UnavailableControlDiscoveryRunner,
    ValueCloudControlDiscoveryRunner,
)
from .cloud_evidence import (
    CloudEvidenceRecord,
    fetch_and_export_smartess_device_bundle_cloud_evidence,
    fetch_and_export_valuecloud_device_bundle_cloud_evidence,
    infer_evidence_provider,
    load_latest_cloud_evidence,
)
from .cloud_evidence_result import (
    CloudEvidenceOnboardingAssist,
    CloudEvidenceSettingHighlight,
)

__all__ = [
    "CloudEvidenceContext",
    "CloudEvidenceDraftCandidate",
    "CloudEvidenceOnboardingAssist",
    "CloudEvidenceProvider",
    "CloudEvidenceSettingHighlight",
    "DRAFT_KIND_KNOWN_FAMILY",
    "DRAFT_KIND_SMG_BRIDGE",
    "SmartEssCloudEvidenceProvider",
    "UnavailableCloudEvidenceProvider",
    "ValueCloudCloudEvidenceProvider",
    "cloud_evidence_provider_supported",
    "resolve_cloud_evidence_provider",
    "supported_cloud_evidence_providers",
]

# Neutral draft-candidate kinds. The coordinator asks for a candidate by kind and
# never names a provider-specific plan type.
DRAFT_KIND_KNOWN_FAMILY = "known_family"
DRAFT_KIND_SMG_BRIDGE = "smg_bridge"


@dataclass(frozen=True)
class CloudEvidenceContext:
    """Explicit, normalized inputs one provider needs to act for a config entry.

    Only already-resolved DATA -- never provider policy, never the raw collector
    snapshot or the whole config-entry mapping. The coordinator/config-flow
    gather the fields; provider-specific INTERPRETATION stays in the provider.
    No credentials, peer IP, hostname, cloud family, collector kind, or transport
    field may appear here -- none of them select a provider or a transport.
    """

    config_dir: Path
    entry_id: str
    collector_pn: str
    protocol_asset_id: str = ""
    protocol_profile_key: str = ""
    effective_owner_key: str = ""
    effective_profile_name: str = ""
    effective_register_schema_name: str = ""
    effective_profile_path: str = ""
    effective_register_schema_path: str = ""


@dataclass(frozen=True)
class CloudEvidenceDraftCandidate:
    """One normalized learning/draft candidate resolved from cloud evidence.

    ``plan`` is the opaque provider-specific plan object; the coordinator passes
    it straight back to ``create_draft`` and (for backward-compatible entity/flow
    surfaces) may expose it, but it never interprets its provider-specific fields.
    """

    kind: str
    label: str
    reason: str
    plan: Any


class CloudEvidenceProvider(ABC):
    """One cloud-evidence provider behind the neutral contract."""

    provider_id: str = ""
    export_status_label: str = "Cloud evidence exported"

    @abstractmethod
    def export_available(self, context: CloudEvidenceContext) -> bool:
        """Return whether an evidence export can be attempted for this context."""

    @abstractmethod
    def export(
        self,
        context: CloudEvidenceContext,
        *,
        username: str,
        password: str,
    ) -> CloudEvidenceRecord:
        """Fetch + persist one evidence bundle (blocking; run in an executor)."""

    def load_latest(self, context: CloudEvidenceContext) -> CloudEvidenceRecord | None:
        """Return the latest persisted evidence record OWNED BY THIS PROVIDER.

        Scoped by ``provider_id`` so one provider never returns another provider's
        record for the same entry/PN; a record whose provenance cannot be
        established is skipped (fail closed).
        """

        return load_latest_cloud_evidence(
            context.config_dir,
            entry_id=context.entry_id,
            collector_pn=context.collector_pn,
            provider=self.provider_id,
        )

    def owns_record(self, record: CloudEvidenceRecord | None) -> bool:
        """Return whether a record is safe for THIS provider to interpret.

        A missing record (``None``) is absent, not foreign, so it is allowed. A
        PRESENT record must be owned by this provider (explicit/legacy
        provenance) -- a foreign, unknown-provenance, or contradictory record is
        refused so a caller can never make one provider interpret another's data.
        """

        if record is None:
            return True
        if not self.provider_id:
            return False
        return infer_evidence_provider(record.payload) == self.provider_id

    def _require_owned_record(self, record: CloudEvidenceRecord | None) -> None:
        if not self.owns_record(record):
            raise RuntimeError(
                f"cloud_evidence_record_not_owned:{self.provider_id or 'unknown'}"
            )

    def resolve_draft_candidates(
        self,
        context: CloudEvidenceContext,
        record: CloudEvidenceRecord | None,
    ) -> tuple[CloudEvidenceDraftCandidate, ...]:
        """Resolve normalized learning/draft candidates from evidence (none by default).

        A present-but-foreign record yields NO candidates (never interpreted).
        """

        return ()

    def draft_candidate(
        self,
        context: CloudEvidenceContext,
        record: CloudEvidenceRecord | None,
        kind: str,
    ) -> CloudEvidenceDraftCandidate | None:
        for candidate in self.resolve_draft_candidates(context, record):
            if candidate.kind == kind:
                return candidate
        return None

    def create_draft(
        self,
        context: CloudEvidenceContext,
        record: CloudEvidenceRecord | None,
        candidate: CloudEvidenceDraftCandidate,
        *,
        output_profile_name: str | None,
        output_schema_name: str | None,
        overwrite: bool,
    ) -> tuple[str, str]:
        """Create one local draft pair from a candidate (blocking; executor)."""

        raise RuntimeError(
            f"cloud_evidence_draft_not_supported:{self.provider_id or 'unknown'}"
        )

    def build_onboarding_assist(
        self,
        context: CloudEvidenceContext,
        *,
        username: str,
        password: str,
    ) -> CloudEvidenceOnboardingAssist:
        """Fetch + interpret evidence into a render-ready onboarding-assist DTO.

        Blocking (run in an executor). Providers without an onboarding-assist
        surface fail closed. The result contains no raw payload -- only normalized
        fields the config flow renders directly.
        """

        raise RuntimeError(
            f"cloud_evidence_onboarding_assist_not_supported:{self.provider_id or 'unknown'}"
        )

    def classify_error(self, exc: BaseException) -> str:
        """Return a provider-neutral error CODE for one raised cloud error.

        Lets the config flow render a translated message without importing a
        provider client's error taxonomy. The base returns a generic code.
        """

        return "unexpected"

    def diagnostics(self, context: CloudEvidenceContext) -> dict[str, object]:
        """Return SAFE diagnostics -- never credentials or raw cloud payloads."""

        return {
            "provider": self.provider_id,
            "export_available": self.export_available(context),
            "control_discovery_available": self.control_discovery_available,
        }

    @property
    def control_discovery_available(self) -> bool:
        """Return whether this provider implements cloud control discovery."""

        return False

    def control_discovery_runner(self) -> CloudControlDiscoveryRunner:
        """Return this provider's runner, fail-closed by default.

        The evidence-provider registry is the single provider-selection
        authority.  Control discovery is a capability of that selected
        provider, not a second registry with a duplicated provider allow-list.
        """

        return UnavailableControlDiscoveryRunner(self.provider_id)


class SmartEssCloudEvidenceProvider(CloudEvidenceProvider):
    """SmartESS cloud-evidence provider (fetch + known-family/SMG-bridge drafts)."""

    provider_id = "smartess"
    export_status_label = "SmartESS cloud evidence exported"

    @property
    def control_discovery_available(self) -> bool:
        return True

    def control_discovery_runner(self) -> CloudControlDiscoveryRunner:
        return SmartEssControlDiscoveryRunner()

    def export_available(self, context: CloudEvidenceContext) -> bool:
        return bool(str(context.collector_pn or "").strip())

    def classify_error(self, exc: BaseException) -> str:
        return classify_smartess_cloud_error(exc)

    def export(
        self,
        context: CloudEvidenceContext,
        *,
        username: str,
        password: str,
    ) -> CloudEvidenceRecord:
        return fetch_and_export_smartess_device_bundle_cloud_evidence(
            config_dir=context.config_dir,
            username=username,
            password=password,
            collector_pn=context.collector_pn,
            source="smartess_cloud_diagnostics",
            entry_id=context.entry_id,
        )

    def resolve_draft_candidates(
        self,
        context: CloudEvidenceContext,
        record: CloudEvidenceRecord | None,
    ) -> tuple[CloudEvidenceDraftCandidate, ...]:
        # Never interpret a foreign / unknown-provenance record.
        if not self.owns_record(record):
            return ()
        payload = record.payload if record is not None else None
        candidates: list[CloudEvidenceDraftCandidate] = []
        known = resolve_smartess_known_family_draft_plan(
            smartess_protocol_asset_id=context.protocol_asset_id,
            smartess_profile_key=context.protocol_profile_key,
            cloud_evidence=payload,
        )
        if known is not None:
            candidates.append(
                CloudEvidenceDraftCandidate(
                    kind=DRAFT_KIND_KNOWN_FAMILY,
                    label="SmartESS known family",
                    reason=str(getattr(known, "reason", "") or ""),
                    plan=known,
                )
            )
        smg = resolve_smartess_smg_bridge_plan(
            effective_owner_key=context.effective_owner_key,
            source_profile_name=context.effective_profile_name,
            source_schema_name=context.effective_register_schema_name,
            source_profile_path=context.effective_profile_path,
            source_schema_path=context.effective_register_schema_path,
            cloud_evidence=payload,
        )
        if smg is not None:
            candidates.append(
                CloudEvidenceDraftCandidate(
                    kind=DRAFT_KIND_SMG_BRIDGE,
                    label=str(getattr(smg, "bridge_label", "") or "SmartESS SMG bridge"),
                    reason=str(getattr(smg, "reason", "") or ""),
                    plan=smg,
                )
            )
        return tuple(candidates)

    def create_draft(
        self,
        context: CloudEvidenceContext,
        record: CloudEvidenceRecord | None,
        candidate: CloudEvidenceDraftCandidate,
        *,
        output_profile_name: str | None,
        output_schema_name: str | None,
        overwrite: bool,
    ) -> tuple[str, str]:
        # A draft is written from a record: it must be present AND owned.
        if record is None:
            raise RuntimeError("cloud_evidence_record_not_available")
        self._require_owned_record(record)
        payload = record.payload
        if candidate.kind == DRAFT_KIND_KNOWN_FAMILY:
            return create_smartess_known_family_draft(
                config_dir=context.config_dir,
                plan=candidate.plan,
                cloud_evidence=payload,
                output_profile_name=output_profile_name,
                output_schema_name=output_schema_name,
                overwrite=overwrite,
            )
        if candidate.kind == DRAFT_KIND_SMG_BRIDGE:
            return create_smartess_smg_bridge_draft(
                config_dir=context.config_dir,
                plan=candidate.plan,
                cloud_evidence=payload,
                output_profile_name=output_profile_name,
                output_schema_name=output_schema_name,
                overwrite=overwrite,
            )
        raise RuntimeError(f"cloud_evidence_draft_kind_not_supported:{candidate.kind}")

    def build_onboarding_assist(
        self,
        context: CloudEvidenceContext,
        *,
        username: str,
        password: str,
    ) -> CloudEvidenceOnboardingAssist:
        record = fetch_and_export_smartess_device_bundle_cloud_evidence(
            config_dir=context.config_dir,
            username=username,
            password=password,
            collector_pn=context.collector_pn,
            source=SMARTESS_ONBOARDING_SOURCE,
            entry_id=context.entry_id,
        )
        plan = resolve_smartess_known_family_draft_plan(
            smartess_protocol_asset_id=context.protocol_asset_id,
            smartess_profile_key=context.protocol_profile_key,
            cloud_evidence=record.payload,
        )
        return build_smartess_onboarding_assist(
            evidence_payload=record.payload,
            evidence_path=str(record.path),
            plan=plan,
            collector_pn=context.collector_pn,
        )


class ValueCloudCloudEvidenceProvider(CloudEvidenceProvider):
    """ValueCloud cloud-evidence provider (fetch only; no draft candidates yet)."""

    provider_id = "valuecloud"
    export_status_label = "Cloud evidence exported"

    @property
    def control_discovery_available(self) -> bool:
        return True

    def control_discovery_runner(self) -> CloudControlDiscoveryRunner:
        return ValueCloudControlDiscoveryRunner()

    def export_available(self, context: CloudEvidenceContext) -> bool:
        return bool(str(context.collector_pn or "").strip())

    def export(
        self,
        context: CloudEvidenceContext,
        *,
        username: str,
        password: str,
    ) -> CloudEvidenceRecord:
        return fetch_and_export_valuecloud_device_bundle_cloud_evidence(
            config_dir=context.config_dir,
            username=username,
            password=password,
            collector_pn=context.collector_pn,
            source="valuecloud_cloud_diagnostics",
            entry_id=context.entry_id,
        )


class UnavailableCloudEvidenceProvider(CloudEvidenceProvider):
    """Fail-closed provider for an unknown/unsupported cloud family."""

    def __init__(self, requested_provider_id: str = "") -> None:
        self._requested = str(requested_provider_id or "").strip().lower()

    def export_available(self, context: CloudEvidenceContext) -> bool:
        return False

    def export(
        self,
        context: CloudEvidenceContext,
        *,
        username: str,
        password: str,
    ) -> CloudEvidenceRecord:
        raise RuntimeError(
            f"cloud_evidence_provider_not_supported:{self._requested or 'unknown'}"
        )

    def load_latest(self, context: CloudEvidenceContext) -> CloudEvidenceRecord | None:
        # An unsupported provider still cannot READ another provider's evidence
        # from a claim it does not own: fail closed.
        return None

    def control_discovery_runner(self) -> CloudControlDiscoveryRunner:
        return UnavailableControlDiscoveryRunner(self._requested)


_PROVIDERS: dict[str, CloudEvidenceProvider] = {
    "smartess": SmartEssCloudEvidenceProvider(),
    "valuecloud": ValueCloudCloudEvidenceProvider(),
}


def supported_cloud_evidence_providers() -> tuple[str, ...]:
    """Return the sorted set of supported cloud-evidence provider ids."""

    return tuple(sorted(_PROVIDERS))


def cloud_evidence_provider_supported(provider_id: object) -> bool:
    """Return whether one provider id has a real (non-fail-closed) implementation."""

    return str(provider_id or "").strip().lower() in _PROVIDERS


def resolve_cloud_evidence_provider(provider_id: object) -> CloudEvidenceProvider:
    """Return the implementation for one provider id, fail-closed for unknown ones."""

    key = str(provider_id or "").strip().lower()
    implementation = _PROVIDERS.get(key)
    if implementation is not None:
        return implementation
    return UnavailableCloudEvidenceProvider(key)
