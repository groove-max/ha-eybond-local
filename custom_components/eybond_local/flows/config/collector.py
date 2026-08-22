"""Selected collector capability and identity projections."""

from __future__ import annotations

from ...collector_identity import (
    pn_is_same_identity,
    reconcile_pn,
)
from .result_model import (
    _result_is_virtual_bridge,
    _smartess_collector_firmware_version_for_result,
)
from ...connection.admission import ObservedCollectorSession
from ...connection.admission_transaction import (
    CollectorAdmissionTransaction,
)
from ...connection.recovery.verification import (
    CallbackRecoveryRoute,
)
from ...const import (
    CONF_DRIVER_HINT,
    CONF_SMARTESS_COLLECTOR_VERSION,
    DRIVER_HINT_AUTO,
)
from ...models import (
    OnboardingResult,
)


class SelectedCollectorFlowMixin:
    """Selected collector capability and identity projections."""

    def _selected_result_is_virtual_bridge(self) -> bool:
        """Return True when the selected onboarding result is a detected bridge.

        Reads the bridge verdict carried from the onboarding hardware token (see
        ``onboarding/eybond.py``). Positive-only and fail-safe: a factory
        collector or an unread token leaves no verdict, so the confirm step
        behaves exactly as before — the runtime path still corrects identity and
        menu gating after the entry runs.
        """

        return _result_is_virtual_bridge(self._selected_result)

    def _reset_scan_progress(self) -> None:
        """Reset scan-progress bookkeeping before one new scan attempt starts."""

        self._scan_task = None
        self._scan_started_monotonic = None
        self._scan_progress_stage = "preparing"
        self._scan_progress_visible = False
        self._ble_last_error = ""
        self._smartess_cloud_assist_mode = ""
        self._smartess_cloud_assist_last_error = ""
        self._smartess_cloud_assist_last_error_code = ""

    def _set_selected_result(self, result: OnboardingResult | None) -> None:
        """Persist the selected onboarding result."""

        self._selected_result = result

    def _selected_poll_policy_driver_key(self) -> str:
        """Return the detected driver that owns onboarding poll limits."""

        result = self._selected_result
        if result is not None and result.match is not None:
            return str(result.match.driver_key or DRIVER_HINT_AUTO)
        return str(self._auto_config.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO))

    def _selected_poll_policy_match(self):
        """Return the detected match (model identity) for the poll policy, if any.

        A catalog driver may pick a model-specific policy from it; ``None`` when
        no match is selected yet.
        """

        result = self._selected_result
        return result.match if result is not None else None

    def _selected_collector_ip(self) -> str:
        result = self._selected_result
        if result is None or result.collector is None:
            return ""
        verified_route = self._verified_callback_route_for_result(result)
        if verified_route is not None:
            return verified_route.trigger_target_ip
        return str(result.collector.ip or result.collector.target_ip or "").strip()

    def _verified_callback_route_for_result(
        self, result: OnboardingResult
    ) -> CallbackRecoveryRoute | None:
        """Return the callback route earned by this result's terminal proof.

        The selected scan result still describes the physical TCP observation,
        whose peer may be a router/NAT address. It must never become the
        operational callback target merely because verification completed.
        Conversely, an offered/selected route is only a hint until the same
        transaction returns a strict callback proof for that exact route.
        """

        transaction = self._admission_transaction
        if (
            result is not self._selected_result
            or type(transaction) is not CollectorAdmissionTransaction
        ):
            return None
        route = transaction.request.callback_route
        if type(route) is not CallbackRecoveryRoute or route.invalid_reason():
            return None
        terminal = transaction.terminal_input
        proof = terminal.callback_proof
        if proof is None:
            return None
        if (
            not pn_is_same_identity(terminal.collector_pn, transaction.expected_pn)
            or not pn_is_same_identity(proof.collector_pn, transaction.expected_pn)
            or proof.trigger_target != route.trigger_target
            or proof.advertised_ha_endpoint != route.advertised_ha_endpoint
            or proof.listener_port != route.listener_port
        ):
            return None
        return route

    def _selected_result_is_passive_callback(self) -> bool:
        result = self._selected_result
        if result is None:
            return False
        return self._result_is_passive_callback(result)

    @staticmethod
    def _result_is_passive_callback(result: OnboardingResult) -> bool:
        collector = result.collector
        return bool(
            result.connection_mode == "callback_listener"
            or (collector is not None and collector.source == "callback_listener")
        )

    @staticmethod
    def _collector_identity_projection(
        result: OnboardingResult | None,
    ) -> tuple[str, bool]:
        """Return the reconciled PN and whether the result contains a conflict.

        ``ObservedCollectorSession`` is the typed admission evidence carried by
        a silent callback scan.  It may be the only place where the exact-session
        FC=2 probe produced a PN, so the older ``CollectorInfo`` projection must
        not make that result disappear from the UI.  Conversely, no source may
        silently override another identity: all non-empty facts must denote the
        same collector before short/full reconciliation is allowed.
        """

        if result is None:
            return "", False

        def _exact_pn(value: object) -> str:
            if type(value) is not str or not value or value != value.strip():
                return ""
            return value

        identities: list[str] = []

        collector_info = (
            result.collector.collector if result.collector is not None else None
        )
        if collector_info is not None:
            collector_pn = _exact_pn(collector_info.collector_pn)
            if collector_pn:
                identities.append(collector_pn)

        observed = result.observed_session
        if type(observed) is ObservedCollectorSession:
            identities.append(observed.collector_pn)

        match_details = result.match.details if result.match is not None else {}
        match_pn = _exact_pn(match_details.get("collector_pn"))
        if match_pn:
            identities.append(match_pn)

        for index, identity in enumerate(identities):
            if any(
                not pn_is_same_identity(identity, candidate)
                for candidate in identities[index + 1 :]
            ):
                return "", True

        collector_pn = ""
        for identity in identities:
            collector_pn = reconcile_pn(collector_pn, identity)
        return collector_pn, False

    def _collector_pn_for_result(self, result: OnboardingResult | None) -> str:
        collector_pn, conflict = self._collector_identity_projection(result)
        return "" if conflict else collector_pn

    def _known_smartess_ble_firmware_version(self, ble_address: str) -> str:
        cached_fw_version = str(
            self._ble_fw_version_by_address.get(ble_address, "") or ""
        ).strip()
        if cached_fw_version:
            return cached_fw_version
        for result in (self._selected_result, self._manual_result):
            fw_version = _smartess_collector_firmware_version_for_result(result)
            if fw_version:
                return fw_version
        return str(
            self._auto_config.get(CONF_SMARTESS_COLLECTOR_VERSION)
            or self._manual_config.get(CONF_SMARTESS_COLLECTOR_VERSION)
            or ""
        ).strip()

    def _smartess_detected_hint_values(
        self, result: OnboardingResult | None
    ) -> tuple[str, str]:
        if result is None:
            return "", ""

        collector_info = (
            result.collector.collector if result.collector is not None else None
        )
        match_details = result.match.details if result.match is not None else {}
        asset_id = str(
            match_details.get("smartess_protocol_asset_id")
            or getattr(collector_info, "smartess_protocol_asset_id", "")
            or ""
        ).strip()
        profile_key = str(
            match_details.get("smartess_profile_key")
            or getattr(collector_info, "smartess_protocol_profile_key", "")
            or ""
        ).strip()
        return asset_id, profile_key
