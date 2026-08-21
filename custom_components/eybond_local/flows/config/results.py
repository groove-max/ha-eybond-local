"""Scan-result identity, deduplication, and user presentation."""

from __future__ import annotations

from typing import Any

from ...collector.smartess_ble import (
    SmartEssBleCandidate,
)
from .result_model import (
    _collector_identity_matches,
)
from ...connection.admission import ObservedCollectorSession
from ...connection.recovery.verification import (
    CallbackRecoveryRoute,
)
from ...const import (
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_PN,
    CONF_DETECTED_SERIAL,
    CONF_SERVER_IP,
    DEFAULT_TCP_PORT,
    DOMAIN,
    DRIVER_HINT_AUTO,
)
from ...models import (
    OnboardingResult,
)
from ...onboarding.presentation import (
    confidence_sort_score,
    scan_result_sort_key,
    scan_result_status_code,
)


class ScanResultPresentationMixin:
    """Scan-result identity, deduplication, and user presentation."""

    def _result_label(self, result: OnboardingResult) -> str:
        collector = result.collector
        collector_ip = (
            collector.ip
            if collector is not None
            else self._tr("common.dynamic.unknown", "Unknown")
        )
        status_label = self._result_status_label(result)
        if self._is_route_scan_result(result):
            return self._tr(
                "common.dynamic.result_label_identify_route",
                "Check address {collector_ip}",
                {"collector_ip": collector_ip},
            )
        collector_pn = self._collector_pn_for_result(result)
        if (
            self._result_is_passive_callback(result)
            and type(result.observed_session) is ObservedCollectorSession
            and self._existing_entry_for_result(result) is None
        ):
            return self._tr(
                "common.dynamic.result_label_identity_needs_route",
                "PN {collector_pn} — choose a reachable device address (connection from {peer_ip})",
                {
                    "collector_pn": collector_pn,
                    "peer_ip": collector_ip,
                },
            )
        if self._result_is_passive_callback(result):
            if not collector_pn:
                return self._tr(
                    "common.dynamic.result_label_incoming_unidentified",
                    "{status_label}: connection from {peer_ip}",
                    {
                        "status_label": status_label,
                        "peer_ip": collector_ip,
                    },
                )
            return self._tr(
                "common.dynamic.result_label_incoming",
                "{status_label}: PN {collector_pn} — connection from {peer_ip}",
                {
                    "status_label": status_label,
                    "collector_pn": collector_pn,
                    "peer_ip": collector_ip,
                },
            )
        if collector_pn:
            return self._tr(
                "common.dynamic.result_label_identified",
                "{status_label}: PN {collector_pn} — {collector_ip}",
                {
                    "status_label": status_label,
                    "collector_pn": collector_pn,
                    "collector_ip": collector_ip,
                },
            )
        return self._tr(
            "common.dynamic.result_label_observed",
            "{status_label}: {collector_ip}",
            {
                "status_label": status_label,
                "collector_ip": collector_ip,
            },
        )

    @staticmethod
    def _escape_markdown_table_cell(value: object) -> str:
        return str(value).replace("|", "\\|").replace("\n", " ")

    def _onboarding_confirm_table(
        self,
        heading_key: str,
        heading_fallback: str,
        rows: list[tuple[str, str, str]],
    ) -> str:
        lines = [
            self._tr(heading_key, heading_fallback),
            "",
            f"| {self._tr('common.dynamic.onboarding_confirm_table_label', 'Detail')} | {self._tr('common.dynamic.onboarding_confirm_table_value', 'Value')} |",
            "|---|---|",
        ]
        for label_key, label_fallback, value in rows:
            lines.append(
                f"| {self._tr(label_key, label_fallback)} | {self._escape_markdown_table_cell(value)} |"
            )
        return "\n".join(lines)

    def _result_placeholders(self, result: OnboardingResult) -> dict[str, str]:
        collector = result.collector
        observed_peer_ip = (
            (collector.ip if collector is not None and collector.ip else "")
            or (
                collector.target_ip
                if collector is not None and collector.target_ip
                else ""
            )
            or self._tr("common.dynamic.unknown", "Unknown")
        )
        verified_callback_route = self._verified_callback_route_for_result(result)
        collector_ip = (
            verified_callback_route.trigger_target_ip
            if verified_callback_route is not None
            else observed_peer_ip
        )
        not_available_yet = self._tr(
            "common.dynamic.not_available_yet", "Not available yet"
        )
        collector_pn = self._collector_pn_for_result(result)
        collector_rows = [
            (
                "common.dynamic.onboarding_confirm_collector_pn_label",
                "Collector PN",
                collector_pn or not_available_yet,
            ),
        ]
        if verified_callback_route is not None:
            if verified_callback_route.trigger_target_ip == observed_peer_ip:
                collector_rows.append(
                    (
                        "common.dynamic.onboarding_confirm_collector_ip_label",
                        "Collector IP",
                        verified_callback_route.trigger_target_ip,
                    )
                )
            else:
                collector_rows.extend(
                    [
                        (
                            "common.dynamic.onboarding_confirm_callback_route_label",
                            "Callback address",
                            verified_callback_route.trigger_target_ip,
                        ),
                        (
                            "common.dynamic.onboarding_confirm_connection_source_label",
                            "Incoming connection source",
                            observed_peer_ip,
                        ),
                    ]
                )
        else:
            collector_rows.append(
                (
                    "common.dynamic.onboarding_confirm_collector_ip_label",
                    "Collector IP",
                    collector_ip,
                )
            )
        collector_confirm_table = self._onboarding_confirm_table(
            "common.dynamic.onboarding_confirm_collector_heading",
            "**Collector**",
            collector_rows,
        )
        # The setup transaction admits the collector only.  Even when scanning
        # produced an inverter preview, the authoritative model/serial/driver is
        # detected after the entry owns the session.  Present one honest UX for
        # discovery, scan and explicit-route admission.
        inverter_confirm_table = self._tr(
            "common.dynamic.onboarding_confirm_inverter_pending_after_add",
            "**Inverter**\n\nThe collector will be added first. Home Assistant "
            "will then detect the inverter and create its entities automatically.",
        )
        return {
            "model_name": self._unconfirmed_inverter_label(),
            "serial_number": not_available_yet,
            "driver_key": DRIVER_HINT_AUTO,
            "collector_ip": collector_ip,
            "collector_pn": collector_pn
            or self._tr("common.dynamic.unknown", "Unknown"),
            "confidence": self._confidence_label("none"),
            "collector_confirm_table": collector_confirm_table,
            "inverter_confirm_table": inverter_confirm_table,
            "smartess_cloud_summary": "",
            "control_summary": "",
        }

    def _confidence_label(self, confidence: str) -> str:
        return {
            "high": self._tr("common.dynamic.confidence_high", "High confidence"),
            "medium": self._tr("common.dynamic.confidence_medium", "Medium confidence"),
            "low": self._tr("common.dynamic.confidence_low", "Low confidence"),
            "none": self._tr("common.dynamic.confidence_none", "No confidence"),
        }.get(confidence, confidence)

    def _result_unique_id(self, result: OnboardingResult) -> str:
        collector_ip = result.collector.ip if result.collector is not None else ""
        collector_pn = self._collector_pn_for_result(result)
        server_ip = self._auto_config.get(CONF_SERVER_IP, self._local_ip)
        return (
            f"collector:{collector_pn}"
            if collector_pn
            else f"inverter:{result.match.serial_number}"
            if result.match is not None and result.match.serial_number
            else f"collector_ip:{collector_ip}"
            if collector_ip
            else f"listener:{server_ip}:{DEFAULT_TCP_PORT}"
        )

    def _configured_collector_probe_skip_ips(self) -> frozenset[str]:
        """Collector IPs owned by existing entries: scans must not probe them.

        Probing would steal the collector's callback session from the running
        entry; the scan lists them as already added instead.
        """

        ips: set[str] = set()
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if entry.entry_id == self.context.get("entry_id"):
                continue
            ip = str(entry.data.get(CONF_COLLECTOR_IP, "") or "").strip()
            if ip:
                ips.add(ip)
        return frozenset(ips)

    def _existing_entry_for_result(self, result: OnboardingResult):
        collector = result.collector
        collector_pn = self._collector_pn_for_result(result)
        collector_ip = collector.ip if collector is not None else ""
        serial_number = result.match.serial_number if result.match is not None else ""
        candidate_unique_id = self._result_unique_id(result)
        candidate_has_strong_identity = bool(collector_pn or serial_number)

        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if entry.entry_id == self.context.get("entry_id"):
                continue
            entry_collector_pn = entry.data.get(CONF_COLLECTOR_PN, "")
            entry_serial = entry.data.get(CONF_DETECTED_SERIAL, "")
            entry_collector_ip = entry.data.get(CONF_COLLECTOR_IP, "")
            entry_has_strong_identity = bool(entry_collector_pn or entry_serial)
            if entry.unique_id and entry.unique_id == candidate_unique_id:
                if (
                    not candidate_has_strong_identity
                    and entry_has_strong_identity
                    and (
                        candidate_unique_id.startswith("collector_ip:")
                        or candidate_unique_id.startswith("manual:")
                    )
                ):
                    continue
                return entry
            if collector_pn and _collector_identity_matches(
                entry_collector_pn, collector_pn
            ):
                return entry
            # Collector PN is the durable entry identity. Some PI30-family
            # inverters ship the same placeholder serial on every unit; once
            # both sides have distinct collector PNs, that shared serial must
            # never collapse two physical collectors into one entry.
            distinct_collector_identities = bool(
                collector_pn
                and entry_collector_pn
                and not _collector_identity_matches(entry_collector_pn, collector_pn)
            )
            if (
                serial_number
                and entry_serial == serial_number
                and not distinct_collector_identities
            ):
                return entry
            if (
                not candidate_has_strong_identity
                and collector_ip
                and entry_collector_ip == collector_ip
                and not entry_has_strong_identity
            ):
                return entry
        return None

    def _already_added_ble_candidate_addresses(
        self,
        candidates: tuple[SmartEssBleCandidate, ...],
    ) -> set[str]:
        if not candidates:
            return set()

        existing_pns: set[str] = set()
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            entry_collector_pn = str(
                entry.data.get(CONF_COLLECTOR_PN, "") or ""
            ).strip()
            if entry_collector_pn:
                existing_pns.add(entry_collector_pn)
            entry_unique_id = str(getattr(entry, "unique_id", "") or "").strip()
            if entry_unique_id.startswith("collector:"):
                existing_pns.add(entry_unique_id.split(":", 1)[1])

        return {
            candidate.address
            for candidate in candidates
            if str(candidate.local_pn or "").strip() in existing_pns
        }

    @staticmethod
    def _is_visible_scan_result(result: OnboardingResult) -> bool:
        collector = result.collector
        if result.last_error == "already_configured":
            # Configured collectors are not probed, but the user must still
            # see that the scan accounted for them.
            return True
        if result.match is not None:
            return True
        if collector is None:
            return False
        collector_info = collector.collector
        return bool(
            collector.connected
            or collector.udp_reply
            or (collector_info is not None and collector_info.collector_pn)
        )

    def _is_addable_scan_result(self, result: OnboardingResult) -> bool:
        collector = result.collector
        collector_pn, identity_conflict = self._collector_identity_projection(result)
        if identity_conflict:
            return False
        return bool(
            collector_pn
            or result.match is not None
            or (collector is not None and collector.connected)
        )

    @staticmethod
    def _is_route_scan_result(result: OnboardingResult) -> bool:
        """Return whether the result is only an address worth identifying."""

        return scan_result_status_code(result) == "address_found"

    def _selectable_autodetect_results(self) -> dict[str, OnboardingResult]:
        """Return identified devices plus route-identification actions."""

        return {
            key: result
            for key, result in self._sorted_autodetect_items()
            if self._is_addable_scan_result(result)
            or self._is_route_scan_result(result)
            if self._existing_entry_for_result(result) is None
        }

    def _available_autodetect_results(self) -> dict[str, OnboardingResult]:
        return {
            key: result
            for key, result in self._sorted_autodetect_items()
            if self._is_addable_scan_result(result)
            if self._existing_entry_for_result(result) is None
        }

    def _scan_result_key(self, result: OnboardingResult) -> str:
        collector = result.collector
        collector_pn = self._collector_pn_for_result(result)
        if collector_pn:
            return f"collector:{collector_pn}"
        if collector is not None and collector.ip:
            return f"ip:{collector.ip}"
        if collector is not None and collector.target_ip:
            return f"target:{collector.target_ip}"
        if result.match is not None and result.match.serial_number:
            return f"serial:{result.match.serial_number}"
        return "unknown"

    @staticmethod
    def _scan_result_priority(
        result: OnboardingResult,
    ) -> tuple[int, int, int, int, int, int]:
        collector = result.collector
        collector_info = collector.collector if collector is not None else None
        return (
            # A route actually exercised by this scan is stronger continuation
            # evidence than a passive projection of the same PN.  Without this
            # rank, collapse order could discard the typed route and force an
            # unnecessary address-selection branch.
            1 if type(result.callback_route) is CallbackRecoveryRoute else 0,
            1 if result.match is not None else 0,
            1 if collector is not None and collector.connected else 0,
            1 if collector is not None and collector.udp_reply else 0,
            confidence_sort_score(result.confidence),
            1 if collector_info is not None and collector_info.collector_pn else 0,
        )

    def _sorted_autodetect_items(self) -> list[tuple[str, OnboardingResult]]:
        return sorted(
            self._autodetect_results.items(),
            key=lambda item: scan_result_sort_key(
                item[1],
                already_added=self._existing_entry_for_result(item[1]) is not None,
            ),
        )

    def _sort_scan_results(
        self, results: list[OnboardingResult]
    ) -> list[OnboardingResult]:
        return sorted(
            results,
            key=lambda result: scan_result_sort_key(
                result,
                already_added=self._existing_entry_for_result(result) is not None,
            ),
        )

    def _collapse_scan_results(
        self,
        results: Any,
    ) -> list[OnboardingResult]:
        collapsed: dict[str, OnboardingResult] = {}
        for result in results:
            key = self._scan_result_key(result)
            collector_pn = self._collector_pn_for_result(result)
            collector_ip = result.collector.ip if result.collector is not None else ""
            for existing_key, existing in collapsed.items():
                existing_pn = self._collector_pn_for_result(existing)
                if collector_pn and _collector_identity_matches(
                    existing_pn, collector_pn
                ):
                    key = existing_key
                    break
                # One collector seen through two sources (e.g. the skip-probe
                # marker for a configured IP plus a PN-carrying session-inventory
                # result) must collapse into one line. Same-IP merging applies
                # only when at least one side lacks a PN: two different
                # collectors behind one NAT IP both carry PNs and stay apart.
                if (
                    collector_ip
                    and existing.collector is not None
                    and existing.collector.ip == collector_ip
                    and (not collector_pn or not existing_pn)
                ):
                    key = existing_key
                    break
            current = collapsed.get(key)
            if current is None or self._scan_result_priority(
                result
            ) > self._scan_result_priority(current):
                collapsed[key] = result
        return list(collapsed.values())

    def _scan_results_placeholders(self) -> dict[str, str]:
        results = self._sorted_autodetect_items()
        available_count = 0
        already_added_count = 0
        selected_ip = self._auto_config.get(CONF_SERVER_IP, self._local_ip)
        refresh_action_label = self._refresh_scan_action_label()
        manual_action_label = self._scan_action_label("manual", "Manual setup")
        selected_label = self._selected_interface_label(selected_ip)
        for _, result in results:
            existing_entry = self._existing_entry_for_result(result)
            if existing_entry is not None:
                already_added_count += 1
            elif self._is_addable_scan_result(result):
                available_count += 1

        detected_count = len(results)
        candidate_list = "\n".join(
            self._scan_result_line(index, result)
            for index, (_, result) in enumerate(results, start=1)
        )
        if detected_count == 0:
            scan_summary = self._tr(
                "common.dynamic.scan_no_results_summary",
                "No compatible devices were found.",
            )
            next_hint = self._tr(
                "common.dynamic.scan_no_results_next",
                "Use **{refresh_action_label}** to try again, or **{manual_action_label}** to enter an address manually.",
                {
                    "refresh_action_label": refresh_action_label,
                    "manual_action_label": manual_action_label,
                },
            )
        elif available_count == 0 and already_added_count == detected_count:
            scan_summary = self._tr(
                "common.dynamic.scan_all_added_summary",
                "Found **{detected_count}** device candidate(s), but all of them are already configured.",
                {"detected_count": detected_count},
            )
            next_hint = self._tr(
                "common.dynamic.scan_all_added_next",
                "Use **{refresh_action_label}** to look again, or **{manual_action_label}** to enter a different address.",
                {
                    "refresh_action_label": refresh_action_label,
                    "manual_action_label": manual_action_label,
                },
            )
        elif available_count == 0:
            scan_summary = self._tr(
                "common.dynamic.scan_none_addable_summary",
                "Found **{detected_count}** search result(s), but none is ready to add yet.",
                {"detected_count": detected_count},
            )
            next_hint = self._tr(
                "common.dynamic.scan_none_addable_next",
                "Select a responding address, use **{refresh_action_label}** to try again, or **{manual_action_label}** to enter an address manually.",
                {
                    "refresh_action_label": refresh_action_label,
                    "manual_action_label": manual_action_label,
                },
            )
        else:
            scan_summary = self._tr(
                "common.dynamic.scan_addable_summary",
                "Found **{detected_count}** search result(s). **{available_count}** can continue setup, **{already_added_count}** already configured.",
                {
                    "detected_count": detected_count,
                    "available_count": available_count,
                    "already_added_count": already_added_count,
                },
            )
            next_hint = self._tr(
                "common.dynamic.scan_addable_next_select",
                "Select a device or responding address below, or use **{refresh_action_label}** or **{manual_action_label}** to search again or enter an address manually.",
                {
                    "refresh_action_label": refresh_action_label,
                    "manual_action_label": manual_action_label,
                },
            )

        return {
            "scan_summary": scan_summary,
            "scan_next_hint": next_hint,
            "selected_scan_interface": selected_label,
            "candidate_list": candidate_list,
        }

    def _choose_placeholders(self) -> dict[str, str]:
        return {
            "choose_summary": self._tr(
                "common.dynamic.choose_summary",
                "**{available_count}** found device(s) can continue setup. Already configured devices are not shown.",
                {"available_count": len(self._available_autodetect_results())},
            )
        }

    def _scan_result_line(self, index: int, result: OnboardingResult) -> str:
        collector = result.collector
        collector_ip = (
            collector.ip
            if collector is not None
            else self._tr("common.dynamic.unknown", "Unknown")
        )
        existing_entry = self._existing_entry_for_result(result)
        collector_pn = self._collector_pn_for_result(result)
        status_label = self._result_status_label(result, existing_entry is not None)
        is_passive_callback = self._result_is_passive_callback(result)

        status_code = scan_result_status_code(result, existing_entry is not None)
        details = []
        if collector_pn:
            details.append(f"PN {collector_pn}")
        if is_passive_callback:
            details.append(
                self._tr(
                    "common.dynamic.scan_line_connection_from",
                    "connection from {peer_ip}",
                    {"peer_ip": collector_ip},
                )
            )
        else:
            details.append(
                self._tr(
                    "common.dynamic.scan_line_route_address",
                    "address {collector_ip}",
                    {"collector_ip": collector_ip},
                )
            )
        line = f"{index}. **{status_label}** — " + " · ".join(details)

        if existing_entry is not None:
            line += " " + self._tr(
                "common.dynamic.scan_line_already_added",
                '*(already added as "{entry_title}")*',
                {"entry_title": existing_entry.title},
            )
        return line

    def _result_status_label(
        self, result: OnboardingResult, already_added: bool = False
    ) -> str:
        status_code = scan_result_status_code(result, already_added)
        return {
            "found": self._tr("common.dynamic.status_found", "Found"),
            "address_required": self._tr(
                "common.dynamic.status_address_required", "Address needed"
            ),
            "address_found": self._tr(
                "common.dynamic.status_address_found", "Address found"
            ),
            "already_added": self._tr(
                "common.dynamic.status_already_added", "Already added"
            ),
            "not_ready": self._tr("common.dynamic.status_not_ready", "Not ready"),
        }.get(status_code, self._tr("common.dynamic.status_not_ready", "Not ready"))
