"""DESSMonitor active cloud/local correlation operation."""

from __future__ import annotations

from ..collector_identity import pn_is_same_identity
from ..dessmonitor_cloud import (
    DessMonitorEvidenceBundle,
    fetch_read_only_evidence_for_session,
    login_with_password,
)
from .cloud_active_workflow import (
    ACTIVE_CORRELATION_NO_SAFE_CONTROLS,
    DEFAULT_CONTROL_DISCOVERY_TIMEOUT_POLICY,
    CloudActiveCorrelationOperation,
)
from .cloud_learning_runner import CloudLearningOutcome
from .shadow_learning.dessmonitor_orchestrator import (
    async_orchestrate_dessmonitor_shadow_learning,
    build_dessmonitor_learning_plan,
)


class DessMonitorActiveCorrelationOperation(CloudActiveCorrelationOperation):
    """Authenticate, resolve identity, then correlate exact ``ctrlDevice`` writes."""

    provider_id = "smartess"
    source_id = "dessmonitor"
    timeout_policy = DEFAULT_CONTROL_DISCOVERY_TIMEOUT_POLICY

    async def async_correlate(
        self,
        *,
        executor,
        collector_pn,
        username,
        password,
        fallback_identity,
        max_fields,
        progress,
        orchestrator_callbacks,
        adopt_identity,
        start_shadow_route,
        on_learning,
    ) -> CloudLearningOutcome:
        del fallback_identity
        if type(max_fields) is not int:
            raise TypeError("dessmonitor_learning_max_fields_invalid")
        if max_fields < 0:
            raise ValueError("dessmonitor_learning_max_fields_invalid")

        progress(0.08, "fetching")
        _login_envelope, cloud_session = await executor(
            lambda: login_with_password(
                username=username,
                password=password,
                timeout=self.timeout_policy.action_request,
            )
        )
        progress(0.14, "fetching")
        bundle = await executor(
            lambda: fetch_read_only_evidence_for_session(
                session=cloud_session,
                collector_pn=collector_pn,
                timeout=self.timeout_policy.metadata_request,
                # Active correlation needs the provider-declared choice values,
                # not a slow best-effort sweep of current values. Current-only
                # numeric controls remain excluded rather than being invented.
                max_control_values=0,
                required_actions=("queryDeviceCtrlField",),
            )
        )
        if type(bundle) is not DessMonitorEvidenceBundle:
            raise TypeError("dessmonitor_learning_bundle_invalid")
        if not pn_is_same_identity(collector_pn, bundle.identity.pn):
            raise ValueError("dessmonitor_learning_identity_mismatch")
        if not build_dessmonitor_learning_plan(
            bundle.control_fields,
            max_fields=max_fields,
        ):
            raise RuntimeError(ACTIVE_CORRELATION_NO_SAFE_CONTROLS)

        identity = bundle.identity.to_record()
        adopt_identity(identity)
        progress(0.22, "connecting")
        await start_shadow_route()
        on_learning()
        progress(0.30, "testing")
        result = await async_orchestrate_dessmonitor_shadow_learning(
            control_fields=bundle.control_fields,
            session=cloud_session,
            identity=bundle.identity,
            confirm_cloud_write=True,
            shadow_session_state="learning",
            field_ids=(),
            all_choice_values=True,
            max_fields=max_fields,
            continue_on_error=True,
            delay_seconds=0.0,
            timeout=self.timeout_policy.action_request,
            **dict(orchestrator_callbacks),
        )
        result = dict(result)
        result["source"] = self.source_id
        result["metadata_only"] = False
        return CloudLearningOutcome(
            identity=identity,
            result=result,
            read_bindings=None,
            metadata_evidence=bundle.to_record(),
        )


__all__ = ["DessMonitorActiveCorrelationOperation"]
