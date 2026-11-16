"""Collector operation -> runtime snapshot -> role projection -> real support ZIP."""
from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
import zipfile
import asyncio

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_hub import _CollectorAtQueryTransport, _CollectorManagementTransport, _FakeLinkManager

from custom_components.eybond_local.collector.management import CollectorManagementTransportError
from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.runtime.hub import EybondHub
from custom_components.eybond_local.support.bundle import build_support_bundle_payload
from custom_components.eybond_local.support.package import export_support_package

PREFIX = "collector_management_last_"
CONTEXT_FIELDS = ("failed_request", "session_generation_start", "session_generation_end")


class CollectorManagementArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_endpoint_read_stops_before_second_query_after_session_handover(self):
        for use_identity in (False, True):
            with self.subTest(use_identity=use_identity):
                hub, link = self.make_hub()
                if use_identity:
                    link.owned_session_identity = ("entry", "old-session")
                send = link.transport.async_send_collector

                async def replace_session(**kwargs):
                    response = await send(**kwargs)
                    if use_identity:
                        # The monitor counter has not observed the replacement yet.
                        link.owned_session_identity = ("entry", "new-session")
                    else:
                        link.owned_session_generation += 1
                    return response

                with patch.object(link.transport, "async_send_collector", side_effect=replace_session):
                    with self.assertRaisesRegex(CollectorManagementTransportError,
                                                "collector_management_session_changed"):
                        await hub.async_get_collector_server_endpoint_state()
                self.assertEqual(link.transport.requests, [(2, b"\x15")])
                self.assertEqual(link.reset_calls, 0)

    async def test_endpoint_reads_follow_framed_at_framed_handover(self):
        hub, link = self.make_hub()
        framed = link.transport
        at = _CollectorAtQueryTransport({"CLDSRVHOST1": "at.example.test,18899,TCP"})
        first = await hub.async_get_collector_server_endpoint_state()
        self.assertEqual(first["current_endpoint"], framed.endpoint)
        link.transport = object()
        link.collector_at_transport = at
        with patch.object(at, "async_query", wraps=at.async_query) as query:
            second = await hub.async_get_collector_server_endpoint_state()
        query.assert_awaited_once_with("CLDSRVHOST1")
        self.assertEqual(second["current_endpoint"], "at.example.test,18899,TCP")
        link.collector_at_transport = None
        link.transport = framed
        third = await hub.async_get_collector_server_endpoint_state()
        self.assertEqual(third, first)
        self.assertEqual(framed.requests, [(2, b"\x15"), (2, b"\x1e")] * 2)

    async def test_at_read_does_not_publish_state_from_replaced_session(self):
        hub, link = self.make_hub()
        link.transport = object()
        link.owned_session_identity = ("entry", "old-session")
        at = link.collector_at_transport = _CollectorAtQueryTransport({
            "CLDSRVHOST1": "private-endpoint.example.test,18899,TCP",
        })
        query = at.async_query

        async def replace_session(command):
            response = await query(command)
            link.owned_session_identity = ("entry", "new-session")
            return response

        with patch.object(at, "async_query", side_effect=replace_session) as read:
            with self.assertRaisesRegex(CollectorManagementTransportError,
                                        "collector_management_session_changed"):
                await hub.async_get_collector_server_endpoint_state()
        read.assert_awaited_once_with("CLDSRVHOST1")
        self.assertNotIn("private-endpoint.example.test", json.dumps(self.archive(hub)))

    async def test_read_uses_live_identity_not_lagging_monitor_counter(self):
        hub, link = self.make_hub()
        link.owned_session_identity = ("entry", "same-session")
        send = link.transport.async_send_collector

        async def monitor_tick(**kwargs):
            response = await send(**kwargs)
            link.owned_session_generation += 1
            return response

        with patch.object(link.transport, "async_send_collector", side_effect=monitor_tick):
            state = await hub.async_get_collector_server_endpoint_state()
        self.assertEqual(state["current_endpoint"], link.transport.endpoint)
        self.assertEqual(link.transport.requests, [(2, b"\x15"), (2, b"\x1e")])

    async def test_failed_wire_context_survives_successful_at_recovery(self):
        hub, link = self.make_hub()
        link.owned_session_identity = ("entry", "same-session")
        with patch.object(link.transport, "async_send_collector",
                          side_effect=TimeoutError("private endpoint and credentials")):
            with self.assertRaises(CollectorManagementTransportError) as caught:
                await hub.async_get_collector_server_endpoint_state()
        link.transport = object()
        link.collector_at_transport = _CollectorAtQueryTransport({
            "CLDSRVHOST1": "recovered.example.test,18899,TCP",
        })
        await hub.async_get_collector_server_endpoint_state()
        self.assertEqual(caught.exception.request_diagnostics["parameter"], 21)
        operation = caught.exception.operation_context
        self.assertEqual(operation["adapter_start"], "framed_collector_commands")
        self.assertEqual(operation["adapter_end"], "framed_collector_commands")
        self.assertFalse(operation["session_identity_changed"])
        for view in self.views(self.archive(hub)):
            self.assertEqual(view[PREFIX + "status"], "ok")
            self.assertEqual(view[PREFIX + "failure"]["adapter_start"], operation["adapter_start"])
        self.assertNotIn("private endpoint", json.dumps(operation))

    async def test_timeout_records_handover_during_request_without_cross_wire_retry(self):
        hub, link = self.make_hub()
        link.owned_session_identity = ("entry", "old-session")
        at = _CollectorAtQueryTransport({})

        async def replace_and_timeout(**kwargs):
            link.transport = object()
            link.collector_at_transport = at
            link.owned_session_identity = ("entry", "new-session")
            raise TimeoutError("private socket details")

        with (
            patch.object(link.transport, "async_send_collector", side_effect=replace_and_timeout) as framed,
            patch.object(at, "async_query", wraps=at.async_query) as at_query,
        ):
            with self.assertRaises(CollectorManagementTransportError) as caught:
                await hub.async_get_collector_server_endpoint_state()
        self.assertEqual(framed.await_count, 1)
        at_query.assert_not_awaited()
        context = caught.exception.operation_context
        self.assertEqual(context["adapter_start"], "framed_collector_commands")
        self.assertEqual(context["adapter_end"], "at_commands")
        self.assertTrue(context["session_identity_changed"])
        self.assertEqual(caught.exception.diagnostic_code, "TimeoutError")
        self.assertEqual(link.reset_calls, 0)

    async def test_diagnostic_adapter_lookup_cannot_mask_error_or_export_arbitrary_text(self):
        for kwargs in ({"side_effect": ValueError("private adapter detail")},
                       {"return_value": "private adapter detail"}):
            with self.subTest(kwargs=kwargs):
                hub, link = self.make_hub()
                error = CollectorManagementTransportError("TimeoutError", query_parameter=21)
                with patch.object(link, "collector_management_adapter_id", **kwargs):
                    with self.assertRaises(CollectorManagementTransportError) as caught:
                        await hub._run_management_operation("read_endpoint_state", AsyncMock(side_effect=error))
                self.assertIs(caught.exception, error)
                self.assertEqual(error.operation_context["adapter_start"], "unknown")
                self.assertEqual(error.operation_context["adapter_end"], "unknown")
                self.assertNotIn("private adapter detail", json.dumps(self.archive(hub)))

    def make_hub(self):
        hub = EybondHub(connection=EybondConnectionSpec(
            server_ip="192.0.2.10", collector_ip="192.0.2.14", tcp_port=8899,
            udp_port=58899, discovery_target="192.0.2.255", discovery_interval=30,
            heartbeat_interval=60, request_timeout=5.0,
        ))
        link = _FakeLinkManager()
        link.owned_session_generation = 0
        link.transport = _CollectorManagementTransport()
        link.transport.endpoint = "private-endpoint.example.test,18899,TCP"
        hub._link_manager = link
        return hub, link

    def archive(self, hub, *, extra_values=None):
        snapshot = hub._build_snapshot(extra_values=extra_values)
        hub._last_snapshot = snapshot
        bundle = build_support_bundle_payload(
            entry_id="management-test", entry_title="Collector diagnostics",
            connected=snapshot.connected, collector=asdict(snapshot.collector),
            inverter=None, values=snapshot.values, telemetry=snapshot.telemetry,
            data={}, options={}, profile_name="", register_schema_name="",
        )
        with tempfile.TemporaryDirectory() as root:
            result = export_support_package(
                config_dir=Path(root), entry_id="management-test",
                entry_title="Collector diagnostics", support_bundle=bundle,
                raw_capture=None, fixture=None, anonymized_fixture=None,
            )
            with zipfile.ZipFile(result.path) as archive:
                return json.loads(archive.read("support_bundle.json"))

    def views(self, bundle):
        return (
            bundle["runtime"]["values"], bundle["runtime"]["metadata"],
            bundle["roles"]["collector"]["values"],
            bundle["roles"]["diagnostics"]["collector_management_route"],
        )

    async def fail_endpoint_read(self, hub, link, *, parameter=30, disconnect=False):
        send = link.transport.async_send_collector

        async def fail_query(**kwargs):
            if kwargs["payload"] == bytes((parameter,)):
                link.transport.requests.append((kwargs["fcode"], kwargs["payload"]))
                if disconnect:
                    link.owned_session_generation += 1
                    link.connected = False
                raise TimeoutError("private-response-with-password")
            return await send(**kwargs)

        with patch.object(link.transport, "async_send_collector", side_effect=fail_query):
            with self.assertRaises(CollectorManagementTransportError):
                await hub.async_get_collector_server_endpoint_state()

    async def test_exact_failed_read_and_session_change_survive_the_zip(self):
        for parameter in (21, 30):
            for disconnect in (False, True):
                with self.subTest(parameter=parameter, disconnect=disconnect):
                    hub, link = self.make_hub()
                    await self.fail_endpoint_read(hub, link, parameter=parameter,
                                                  disconnect=disconnect)
                    bundle = self.archive(hub)
                    for view in self.views(bundle):
                        self.assertEqual(view[PREFIX + "operation"], "read_endpoint_state")
                        self.assertEqual(view[PREFIX + "status"], "error")
                        self.assertEqual(view[PREFIX + "error_code"], "TimeoutError")
                        self.assertEqual(view[PREFIX + "failed_request"], {
                            "protocol": "eybond_framed", "function": 2, "parameter": parameter,
                        })
                        self.assertEqual(view[PREFIX + "session_generation_start"], 0)
                        self.assertEqual(view[PREFIX + "session_generation_end"], int(disconnect))
                    self.assertEqual(bundle["runtime"]["connected"], not disconnect)
                    blob = json.dumps(bundle)
                    self.assertNotIn("private-response-with-password", blob)
                    self.assertNotIn("private-endpoint.example.test", blob)
                    expected_reads = [(2, b"\x15")]
                    if parameter == 30:
                        expected_reads.append((2, b"\x1e"))
                    # No extra probe, retry, endpoint change, apply or reboot.
                    self.assertEqual(link.transport.requests, expected_reads)
                    self.assertEqual(link.reset_calls, 0)

    async def test_success_cancellation_and_other_error_clear_previous_request(self):
        for outcome, status in ((None, "ok"), (asyncio.CancelledError(), "cancelled"),
                                (ValueError("private-next-error"), "error")):
            with self.subTest(status=status):
                hub, link = self.make_hub()
                await self.fail_endpoint_read(hub, link)
                failed = self.archive(hub)
                # A following adapter/link need not expose a session generation.
                del link.owned_session_generation
                operation = hub._run_management_operation(
                    "next_action", AsyncMock(side_effect=outcome),
                )
                if outcome is None:
                    await operation
                else:
                    with self.assertRaises(type(outcome)):
                        await operation
                stale_context = {PREFIX + field: failed["runtime"]["values"][PREFIX + field]
                                 for field in CONTEXT_FIELDS}
                current = self.archive(hub, extra_values=stale_context)
                for view in self.views(current):
                    self.assertEqual(view[PREFIX + "operation"], "next_action")
                    self.assertEqual(view[PREFIX + "status"], status)
                    for field in CONTEXT_FIELDS:
                        self.assertNotIn(PREFIX + field, view)
                self.assertNotIn("private-next-error", json.dumps(current))
                # Rebuilding the snapshot never edits an already exported result.
                self.assertEqual(failed["runtime"]["values"][PREFIX + "failed_request"]["parameter"], 30)

    async def test_no_operation_has_no_carried_diagnostic_and_snapshots_are_detached(self):
        hub, link = self.make_hub()
        await self.fail_endpoint_read(hub, link)
        self.archive(hub)
        hub._last_snapshot.values[PREFIX + "failed_request"]["parameter"] = 99
        self.assertEqual(hub._last_management_operation["failed_request"]["parameter"], 30)
        current = self.archive(hub)
        self.assertEqual(current["runtime"]["values"][PREFIX + "failed_request"]["parameter"], 30)
        hub._last_management_operation = None
        # Historical failure is a separate timestamped record, not a carried
        # current-operation error. Clearing both owners removes all diagnostics.
        for view in self.views(self.archive(hub)):
            self.assertEqual(
                {key for key in view if key.startswith(PREFIX)}, {PREFIX + "failure"},
            )
        hub._last_management_failure = None
        for view in self.views(self.archive(hub)):
            self.assertFalse(any(key.startswith(PREFIX) for key in view))

    async def test_write_readback_phase_survives_successful_verification_in_zip(self):
        hub, link = self.make_hub()
        send = link.transport.async_send_collector
        reads = 0

        async def fail_readback(**kwargs):
            nonlocal reads
            if kwargs["fcode"] == 2 and kwargs["payload"] == bytes((21,)):
                reads += 1
                if reads == 2:
                    raise TimeoutError("private-response-with-password")
            return await send(**kwargs)

        with patch.object(link.transport, "async_send_collector", side_effect=fail_readback):
            with self.assertRaises(CollectorManagementTransportError):
                await hub.async_set_collector_server_endpoint("new.example.test,18899,TCP")
        await hub.async_get_collector_server_endpoint_state()
        bundle = self.archive(hub)
        for view in self.views(bundle):
            self.assertEqual(view[PREFIX + "status"], "ok")
            self.assertNotIn(PREFIX + "failed_request", view)
            failure = view[PREFIX + "failure"]
            self.assertEqual(failure["operation"], "write_endpoint")
            self.assertEqual(failure["failed_request"]["phase"], "readback")
            self.assertEqual(failure["failed_request"]["parameter"], 21)
            self.assertLessEqual(failure["timestamp"], view[PREFIX + "timestamp"])
        self.assertNotIn("private-response-with-password", json.dumps(bundle))
        hub._last_snapshot.values[PREFIX + "failure"]["failed_request"]["phase"] = "changed"
        self.assertEqual(hub._last_management_failure["failed_request"]["phase"], "readback")

    async def test_at_failure_does_not_inherit_framed_context(self):
        hub, link = self.make_hub()
        await self.fail_endpoint_read(hub, link)
        self.archive(hub)
        link.transport = object()
        link.collector_at_transport = _CollectorAtQueryTransport({})
        with patch.object(link.collector_at_transport, "async_query",
                          side_effect=OSError("private AT socket details")) as query:
            with self.assertRaises(CollectorManagementTransportError):
                await hub.async_get_collector_server_endpoint_state()
        query.assert_awaited_once_with("CLDSRVHOST1")
        bundle = self.archive(hub)
        for view in self.views(bundle):
            self.assertEqual(view[PREFIX + "error_code"], "OSError")
            self.assertNotIn(PREFIX + "failed_request", view)
            self.assertEqual(view[PREFIX + "session_generation_start"], 0)
            self.assertEqual(view[PREFIX + "session_generation_end"], 0)
        self.assertNotIn("private AT socket details", json.dumps(bundle))

    async def test_direct_transport_error_exports_only_a_class_code(self):
        for message, code in (("TimeoutError", "TimeoutError"),
                              ("private opaque transport detail", "CollectorManagementTransportError")):
            with self.subTest(message=message):
                hub, _ = self.make_hub()
                with self.assertRaises(CollectorManagementTransportError):
                    await hub._run_management_operation("read_endpoint_state", AsyncMock(
                        side_effect=CollectorManagementTransportError(message),
                    ))
                bundle = self.archive(hub)
                for view in self.views(bundle):
                    self.assertEqual(view[PREFIX + "error_code"], code)
                    self.assertNotIn(PREFIX + "failed_request", view)
                self.assertNotIn("private opaque transport detail", json.dumps(bundle))


if __name__ == "__main__":
    unittest.main()
