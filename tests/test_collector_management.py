"""Provider-neutral collector-management adapters.

The framed (FC=2/FC=3) and AT-text adapters carry the SAME endpoint/apply/reboot
operations behind the same normalized models; the parameter numbers and AT
command strings live only inside the implementations. The factory's ONLY input is
the negotiated management-adapter id; a conflict/unknown selects the fail-closed
unavailable adapter. Results are HONEST: readback is the real read (never the
requested value in disguise), status=applied only when a confirmed apply happened,
and an unconfirmed write/apply raises a typed error (never a simulated success).
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.at import CollectorAtError, CollectorAtResponse
from custom_components.eybond_local.collector.collector_wire import CollectorWireError
from custom_components.eybond_local.collector.collector_wire import (
    CollectorManagementError,
    CollectorManagementUnsupportedError,
)
from custom_components.eybond_local.collector.management import (
    AtTextCollectorManagementAdapter,
    CollectorEndpointWriteResult,
    CollectorManagementCommandError,
    CollectorManagementConfirmationError,
    CollectorManagementTransportError,
    FramedCollectorManagementAdapter,
    UnavailableCollectorManagementAdapter,
    select_collector_management_adapter,
)
from custom_components.eybond_local.collector.protocol import (
    FC_QUERY_COLLECTOR,
    FC_SET_COLLECTOR,
)
from custom_components.eybond_local.connection.session_handle import (
    ADAPTER_COLLECTOR_AT_COMMANDS,
    ADAPTER_COLLECTOR_FRAMED_COMMANDS,
    ADAPTER_NONE,
)


class _FakeFramedTransport:
    """Records FC=2/FC=3 collector calls; answers with canned payloads."""

    def __init__(self, *, echo_on_set: bool = True) -> None:
        self.calls: list[tuple[int, bytes]] = []
        self.endpoint = "old.host,18899,TCP"
        self.reboot_required = "1"
        self.uart = "2400,8,1,NONE"
        self.set_status = 0
        self._echo_on_set = echo_on_set

    async def async_send_collector(self, *, fcode, payload, devcode, collector_addr):
        self.calls.append((fcode, bytes(payload)))
        if fcode == FC_QUERY_COLLECTOR:
            param = payload[0]
            if param == 21:  # SET_SERVER_ENDPOINT read
                return object(), bytes((0, 21)) + self.endpoint.encode("ascii")
            if param == 30:  # QUERY_REBOOT_REQUIRED
                return object(), bytes((0, 30)) + self.reboot_required.encode("ascii")
            if param == 34:  # QUERY_SERIAL_BAUDRATE
                return object(), bytes((0, 34)) + self.uart.encode("ascii")
            return object(), bytes((0, param))
        if fcode == FC_SET_COLLECTOR:
            param = payload[0]
            value = payload[1:].decode("ascii")
            if param == 21 and self._echo_on_set:  # collector echoes on readback
                self.endpoint = value
            elif param == 21:  # collector does NOT echo the endpoint
                self.endpoint = ""
            elif param == 34:
                self.uart = f"{value},8,1,NONE"
            return object(), bytes((self.set_status, param))
        raise AssertionError(f"unexpected fcode {fcode}")


class _FakeAtTransport:
    def __init__(self, *, reject_intpara: bool = False) -> None:
        self.queries: list[str] = []
        self.writes: list[tuple[str, str]] = []
        self.endpoint = "iot.eybond.com,18899,TCP"
        self._reject_intpara = reject_intpara

    async def async_query(self, command: str) -> CollectorAtResponse:
        self.queries.append(command)
        if command == "CLDSRVHOST1":
            return CollectorAtResponse(command="CLDSRVHOST1", value=self.endpoint, raw="")
        return CollectorAtResponse(command=command, value="", raw="")

    async def async_write(self, command: str, value: str) -> CollectorAtResponse:
        self.writes.append((command, value))
        if command == "CLDSRVHOST1":
            self.endpoint = value
            return CollectorAtResponse(command="CLDSRVHOST1", value=value, raw="")
        if command == "INTPARA":
            if self._reject_intpara:
                return CollectorAtResponse(command="ERR", value="", raw="")
            return CollectorAtResponse(command="INTPARA", value="W000", raw="")
        return CollectorAtResponse(command=command, value="", raw="")


class FramedCollectorManagementAdapterTests(unittest.IsolatedAsyncioTestCase):
    def _adapter(self, transport):
        return FramedCollectorManagementAdapter(lambda: transport)

    async def test_write_endpoint_uses_fc3_param_21_and_applies_via_param_29(self) -> None:
        transport = _FakeFramedTransport()
        result = await self._adapter(transport).async_write_endpoint(
            "192.168.1.10,18899,TCP", apply_changes=True
        )
        self.assertIn(
            (FC_SET_COLLECTOR, bytes((21,)) + b"192.168.1.10,18899,TCP"), transport.calls
        )
        self.assertIn((FC_SET_COLLECTOR, bytes((29,)) + b"1"), transport.calls)
        self.assertIsInstance(result, CollectorEndpointWriteResult)
        self.assertEqual(result.requested_endpoint, "192.168.1.10,18899,TCP")
        self.assertEqual(result.readback_endpoint, "192.168.1.10,18899,TCP")
        self.assertEqual(result.previous_endpoint, "old.host,18899,TCP")
        self.assertTrue(result.write_confirmed)
        self.assertTrue(result.apply_requested)
        self.assertTrue(result.apply_performed)
        self.assertEqual(result.confirmation_source, "readback")
        self.assertEqual(result.adapter_id, ADAPTER_COLLECTOR_FRAMED_COMMANDS)

    async def test_apply_changes_false_does_not_apply(self) -> None:
        transport = _FakeFramedTransport()
        result = await self._adapter(transport).async_write_endpoint(
            "192.168.1.10,18899,TCP", apply_changes=False
        )
        self.assertNotIn((FC_SET_COLLECTOR, bytes((29,)) + b"1"), transport.calls)
        self.assertFalse(result.apply_requested)
        self.assertFalse(result.apply_performed)

    async def test_missing_readback_is_not_faked_to_requested(self) -> None:
        # The collector accepts the set (status 0) but does NOT echo the endpoint
        # on readback: readback_endpoint must stay empty, never the requested value.
        transport = _FakeFramedTransport(echo_on_set=False)
        result = await self._adapter(transport).async_write_endpoint(
            "9.9.9.9,18899,TCP", apply_changes=False
        )
        self.assertEqual(result.readback_endpoint, "")
        self.assertEqual(result.requested_endpoint, "9.9.9.9,18899,TCP")
        self.assertTrue(result.write_confirmed)
        self.assertEqual(result.confirmation_source, "set_ack")

    async def test_unconfirmed_set_raises_typed(self) -> None:
        transport = _FakeFramedTransport()
        transport.set_status = 1  # collector rejects the set
        with self.assertRaises(CollectorManagementConfirmationError):
            await self._adapter(transport).async_write_endpoint("x,1,TCP", apply_changes=False)

    async def test_read_endpoint_state_reads_params_21_and_30(self) -> None:
        transport = _FakeFramedTransport()
        state = await self._adapter(transport).async_read_endpoint_state()
        self.assertEqual(state.current_endpoint, "old.host,18899,TCP")
        self.assertEqual(state.reboot_required, "1")
        self.assertIn((FC_QUERY_COLLECTOR, bytes((21,))), transport.calls)
        self.assertIn((FC_QUERY_COLLECTOR, bytes((30,))), transport.calls)

    async def test_apply_and_reboot_send_param_29(self) -> None:
        for action, method in (("apply", "async_apply_changes"), ("reboot", "async_reboot")):
            transport = _FakeFramedTransport()
            result = await getattr(self._adapter(transport), method)()
            self.assertEqual(result.action, action)
            self.assertTrue(result.performed)
            self.assertIn((FC_SET_COLLECTOR, bytes((29,)) + b"1"), transport.calls)

    async def test_no_transport_is_unsupported(self) -> None:
        with self.assertRaises(CollectorManagementUnsupportedError):
            await FramedCollectorManagementAdapter(lambda: None).async_read_endpoint_state()

    async def test_uart_baudrate_uses_param_34_and_requires_readback(self) -> None:
        transport = _FakeFramedTransport()

        readback = await self._adapter(transport).async_set_uart_baudrate("9600")

        self.assertEqual(readback, "9600,8,1,NONE")
        self.assertIn((FC_SET_COLLECTOR, bytes((34,)) + b"9600"), transport.calls)
        self.assertIn((FC_QUERY_COLLECTOR, bytes((34,))), transport.calls)

    async def test_uart_baudrate_rejects_malformed_value_before_wire(self) -> None:
        transport = _FakeFramedTransport()
        for malformed in ("", " 9600", "9600 ", "9k6", 9600, True, None):
            with self.subTest(malformed=malformed):
                with self.assertRaises(TypeError):
                    await self._adapter(transport).async_set_uart_baudrate(malformed)
        self.assertEqual(transport.calls, [])

    async def test_capabilities(self) -> None:
        caps = FramedCollectorManagementAdapter(lambda: None).capabilities
        self.assertTrue(caps.read_endpoint_state)
        self.assertTrue(caps.write_endpoint)
        self.assertTrue(caps.apply_changes)
        self.assertTrue(caps.reboot)
        self.assertFalse(hasattr(caps, "read_metadata"))

    async def test_transport_resolved_lazily_each_call(self) -> None:
        first = _FakeFramedTransport()
        second = _FakeFramedTransport()
        box = {"t": first}
        adapter = FramedCollectorManagementAdapter(lambda: box["t"])
        await adapter.async_read_endpoint_state()
        box["t"] = second
        await adapter.async_read_endpoint_state()
        self.assertTrue(first.calls)
        self.assertTrue(second.calls)


class AtTextCollectorManagementAdapterTests(unittest.IsolatedAsyncioTestCase):
    def _adapter(self, transport):
        return AtTextCollectorManagementAdapter(lambda: transport)

    async def test_read_endpoint_state_uses_cldsrvhost1(self) -> None:
        transport = _FakeAtTransport()
        state = await self._adapter(transport).async_read_endpoint_state()
        self.assertEqual(state.current_endpoint, "iot.eybond.com,18899,TCP")
        self.assertEqual(state.reboot_required, "")  # no confirmed AT reboot read
        self.assertIn("CLDSRVHOST1", transport.queries)
        self.assertEqual(state.adapter_id, ADAPTER_COLLECTOR_AT_COMMANDS)

    async def test_write_endpoint_with_apply_uses_cldsrvhost1_and_intpara(self) -> None:
        transport = _FakeAtTransport()
        result = await self._adapter(transport).async_write_endpoint(
            "192.168.8.113,18899,TCP", apply_changes=True
        )
        self.assertIn(("CLDSRVHOST1", "192.168.8.113,18899,TCP"), transport.writes)
        self.assertIn(("INTPARA", "29,1"), transport.writes)
        self.assertEqual(result.requested_endpoint, "192.168.8.113,18899,TCP")
        self.assertEqual(result.readback_endpoint, "192.168.8.113,18899,TCP")
        self.assertEqual(result.previous_endpoint, "iot.eybond.com,18899,TCP")
        self.assertTrue(result.apply_performed)
        self.assertEqual(result.extra.get("at_apply_response"), "W000")
        self.assertEqual(result.adapter_id, ADAPTER_COLLECTOR_AT_COMMANDS)

    async def test_write_endpoint_apply_changes_false_skips_intpara(self) -> None:
        transport = _FakeAtTransport()
        result = await self._adapter(transport).async_write_endpoint(
            "x,1,TCP", apply_changes=False
        )
        self.assertNotIn(("INTPARA", "29,1"), transport.writes)
        self.assertFalse(result.apply_performed)

    async def test_standalone_apply_uses_intpara(self) -> None:
        transport = _FakeAtTransport()
        result = await self._adapter(transport).async_apply_changes()
        self.assertIn(("INTPARA", "29,1"), transport.writes)
        self.assertEqual(result.action, "apply")
        self.assertTrue(result.performed)

    async def test_apply_rejected_raises_typed_and_never_applied(self) -> None:
        # INTPARA is not echoed back -> unconfirmed apply -> typed error, and the
        # error is NOT swallowed into a warning with a successful result.
        transport = _FakeAtTransport(reject_intpara=True)
        with self.assertRaises(CollectorManagementConfirmationError):
            await self._adapter(transport).async_write_endpoint(
                "192.168.8.113,18899,TCP", apply_changes=True
            )
        with self.assertRaises(CollectorManagementConfirmationError):
            await self._adapter(transport).async_apply_changes()

    async def test_reboot_uses_vendor_intpara_restart_command(self) -> None:
        transport = _FakeAtTransport()

        result = await self._adapter(transport).async_reboot()

        self.assertEqual(transport.queries, ["CLDSRVHOST1"])
        self.assertEqual(transport.writes, [("INTPARA", "29,1")])
        self.assertEqual(result.action, "reboot")
        self.assertTrue(result.performed)
        self.assertEqual(result.current_endpoint, "iot.eybond.com,18899,TCP")
        self.assertEqual(result.adapter_id, ADAPTER_COLLECTOR_AT_COMMANDS)
        self.assertIn("restart accepted", result.warnings[0])

    async def test_uart_write_unsupported(self) -> None:
        with self.assertRaises(CollectorManagementUnsupportedError):
            await self._adapter(_FakeAtTransport()).async_set_uart_baudrate("9600")

    async def test_capabilities(self) -> None:
        caps = AtTextCollectorManagementAdapter(lambda: None).capabilities
        self.assertTrue(caps.read_endpoint_state)
        self.assertTrue(caps.write_endpoint)
        self.assertTrue(caps.apply_changes)
        self.assertTrue(caps.reboot)

    async def test_transport_error_is_wrapped_typed(self) -> None:
        class _Boom:
            async def async_query(self, command):
                raise OSError("socket gone")

            async def async_write(self, command, value):
                raise OSError("socket gone")

        with self.assertRaises(CollectorManagementTransportError):
            await self._adapter(_Boom()).async_read_endpoint_state()


class StrictValidationTests(unittest.IsolatedAsyncioTestCase):
    """FC=2 code/parameter and AT response-command are strictly validated."""

    async def test_framed_wrong_fc_code_raises_command_error(self) -> None:
        class _BadCode(_FakeFramedTransport):
            async def async_send_collector(self, *, fcode, payload, devcode, collector_addr):
                self.calls.append((fcode, bytes(payload)))
                if fcode == FC_QUERY_COLLECTOR:
                    return object(), bytes((1, payload[0]))  # non-zero code
                return object(), bytes((0, payload[0]))

        with self.assertRaises(CollectorManagementCommandError):
            await FramedCollectorManagementAdapter(lambda: _BadCode()).async_read_endpoint_state()

    async def test_framed_wrong_fc_parameter_raises_command_error(self) -> None:
        class _WrongParam(_FakeFramedTransport):
            async def async_send_collector(self, *, fcode, payload, devcode, collector_addr):
                self.calls.append((fcode, bytes(payload)))
                if fcode == FC_QUERY_COLLECTOR:
                    return object(), bytes((0, 99)) + b"x,1,TCP"  # foreign parameter
                return object(), bytes((0, payload[0]))

        with self.assertRaises(CollectorManagementCommandError):
            await FramedCollectorManagementAdapter(lambda: _WrongParam()).async_read_endpoint_state()

    async def test_at_wrong_response_command_raises_command_error(self) -> None:
        class _WrongCmd:
            async def async_query(self, command):
                return CollectorAtResponse(command="SOMETHINGELSE", value="x", raw="")

            async def async_write(self, command, value):
                return CollectorAtResponse(command=command, value="", raw="")

        with self.assertRaises(CollectorManagementCommandError):
            await AtTextCollectorManagementAdapter(lambda: _WrongCmd()).async_read_endpoint_state()


class WriteConfirmationTruthTableTests(unittest.IsolatedAsyncioTestCase):
    async def test_framed_readback_match_is_readback_source(self) -> None:
        result = await FramedCollectorManagementAdapter(
            lambda: _FakeFramedTransport()
        ).async_write_endpoint("192.168.1.10,18899,TCP", apply_changes=False)
        self.assertTrue(result.write_confirmed)
        self.assertEqual(result.confirmation_source, "readback")

    async def test_framed_missing_readback_valid_ack_is_set_ack(self) -> None:
        result = await FramedCollectorManagementAdapter(
            lambda: _FakeFramedTransport(echo_on_set=False)
        ).async_write_endpoint("9.9.9.9,18899,TCP", apply_changes=False)
        self.assertTrue(result.write_confirmed)
        self.assertEqual(result.readback_endpoint, "")
        self.assertEqual(result.confirmation_source, "set_ack")

    async def test_framed_mismatched_readback_raises_confirmation(self) -> None:
        class _Stale(_FakeFramedTransport):
            async def async_send_collector(self, *, fcode, payload, devcode, collector_addr):
                self.calls.append((fcode, bytes(payload)))
                if fcode == FC_QUERY_COLLECTOR and payload[0] == 21:
                    return object(), bytes((0, 21)) + b"other.host,18899,TCP"
                if fcode == FC_QUERY_COLLECTOR:
                    return object(), bytes((0, payload[0])) + b"0"
                return object(), bytes((0, payload[0]))

        with self.assertRaises(CollectorManagementConfirmationError):
            await FramedCollectorManagementAdapter(lambda: _Stale()).async_write_endpoint(
                "192.168.1.10,18899,TCP", apply_changes=False
            )

    async def test_at_readback_match_is_readback_source(self) -> None:
        result = await AtTextCollectorManagementAdapter(
            lambda: _FakeAtTransport()
        ).async_write_endpoint("192.168.8.113,18899,TCP", apply_changes=False)
        self.assertEqual(result.confirmation_source, "readback")

    async def test_at_w000_write_ack_with_empty_readback_is_at_command_echo(self) -> None:
        # CLDSRVHOST1 write returns W000 (self-confirmed), the readback answers
        # with no value: empty readback is accepted via at_command_echo.
        class _W000Write:
            async def async_query(self, command):
                return CollectorAtResponse(command="CLDSRVHOST1", value="", raw="")

            async def async_write(self, command, value):
                return CollectorAtResponse(command="CLDSRVHOST1", value="W000", raw="")

        result = await AtTextCollectorManagementAdapter(
            lambda: _W000Write()
        ).async_write_endpoint("192.168.8.113,18899,TCP", apply_changes=False)
        self.assertEqual(result.readback_endpoint, "")
        self.assertEqual(result.confirmation_source, "at_command_echo")

    async def test_at_empty_write_ack_and_empty_readback_is_confirmation_error(self) -> None:
        # An empty write status is NOT an ack; an empty readback then cannot
        # confirm the write -> ConfirmationError.
        class _NoAck:
            async def async_query(self, command):
                return CollectorAtResponse(command="CLDSRVHOST1", value="", raw="")

            async def async_write(self, command, value):
                return CollectorAtResponse(command="CLDSRVHOST1", value="", raw="")

        with self.assertRaises(CollectorManagementConfirmationError):
            await AtTextCollectorManagementAdapter(lambda: _NoAck()).async_write_endpoint(
                "192.168.8.113,18899,TCP", apply_changes=False
            )

    async def test_at_mismatched_readback_raises_confirmation(self) -> None:
        class _StaleAt:
            async def async_query(self, command):
                return CollectorAtResponse(
                    command="CLDSRVHOST1", value="other.host,18899,TCP", raw=""
                )

            async def async_write(self, command, value):
                return CollectorAtResponse(command=command, value="", raw="")

        with self.assertRaises(CollectorManagementConfirmationError):
            await AtTextCollectorManagementAdapter(lambda: _StaleAt()).async_write_endpoint(
                "192.168.8.113,18899,TCP", apply_changes=False
            )


class AtStatusSemanticsTests(unittest.IsolatedAsyncioTestCase):
    """AT status codes (W000 success / Wxxx reject / empty unconfirmed)."""

    def _apply_adapter(self, intpara_command: str, intpara_value: str):
        class _At:
            async def async_query(self, command):
                return CollectorAtResponse(command="CLDSRVHOST1", value="1.2.3.4,18899,TCP", raw="")

            async def async_write(self, command, value):
                if command == "INTPARA":
                    return CollectorAtResponse(command=intpara_command, value=intpara_value, raw="")
                return CollectorAtResponse(command=command, value=value, raw="")

        return AtTextCollectorManagementAdapter(lambda: _At())

    async def test_intpara_w000_apply_succeeds(self) -> None:
        result = await self._apply_adapter("INTPARA", "W000").async_apply_changes()
        self.assertTrue(result.performed)

    async def test_intpara_w001_apply_rejected_is_command_error(self) -> None:
        with self.assertRaises(CollectorManagementCommandError):
            await self._apply_adapter("INTPARA", "W001").async_apply_changes()

    async def test_intpara_empty_status_is_confirmation_error(self) -> None:
        with self.assertRaises(CollectorManagementConfirmationError):
            await self._apply_adapter("INTPARA", "").async_apply_changes()

    async def test_intpara_foreign_command_is_confirmation_error(self) -> None:
        with self.assertRaises(CollectorManagementConfirmationError):
            await self._apply_adapter("SOMETHINGELSE", "W000").async_apply_changes()

    async def test_cldsrvhost1_w001_write_and_empty_readback_rejected(self) -> None:
        class _At:
            async def async_query(self, command):
                return CollectorAtResponse(command="CLDSRVHOST1", value="", raw="")

            async def async_write(self, command, value):
                return CollectorAtResponse(command="CLDSRVHOST1", value="W001", raw="")

        with self.assertRaises(CollectorManagementConfirmationError):
            await AtTextCollectorManagementAdapter(lambda: _At()).async_write_endpoint(
                "1.2.3.4,18899,TCP", apply_changes=False
            )

    async def test_cldsrvhost1_exact_echo_write_ack_with_empty_readback(self) -> None:
        endpoint = "1.2.3.4,18899,TCP"

        class _At:
            async def async_query(self, command):
                return CollectorAtResponse(command="CLDSRVHOST1", value="", raw="")

            async def async_write(self, command, value):
                return CollectorAtResponse(command="CLDSRVHOST1", value=endpoint, raw="")

        result = await AtTextCollectorManagementAdapter(lambda: _At()).async_write_endpoint(
            endpoint, apply_changes=False
        )
        self.assertEqual(result.confirmation_source, "at_command_echo")

    async def test_intpara_w001_wrapping_write_never_marks_applied(self) -> None:
        # A rejected INTPARA raises: the caller never sees apply_performed=True.
        adapter = self._apply_adapter("INTPARA", "W001")

        class _At:
            async def async_query(self, command):
                return CollectorAtResponse(command="CLDSRVHOST1", value="1.2.3.4,18899,TCP", raw="")

            async def async_write(self, command, value):
                if command == "INTPARA":
                    return CollectorAtResponse(command="INTPARA", value="W001", raw="")
                return CollectorAtResponse(command="CLDSRVHOST1", value=value, raw="")

        with self.assertRaises(CollectorManagementCommandError):
            await AtTextCollectorManagementAdapter(lambda: _At()).async_write_endpoint(
                "1.2.3.4,18899,TCP", apply_changes=True
            )


class ErrorTaxonomyMappingTests(unittest.IsolatedAsyncioTestCase):
    """Malformed responses -> CommandError; delivery failures -> TransportError."""

    async def test_malformed_fc2_payload_is_command_error(self) -> None:
        class _Bad:
            async def async_send_collector(self, *, fcode, payload, devcode, collector_addr):
                return object(), b"\x00"  # too short to parse

        with self.assertRaises(CollectorManagementCommandError):
            await FramedCollectorManagementAdapter(lambda: _Bad()).async_read_endpoint_state()

    async def test_malformed_fc3_payload_is_command_error(self) -> None:
        class _Bad:
            async def async_send_collector(self, *, fcode, payload, devcode, collector_addr):
                if fcode == FC_QUERY_COLLECTOR:
                    return object(), bytes((0, payload[0]))  # valid query
                return object(), b""  # malformed set response

        with self.assertRaises(CollectorManagementCommandError):
            await FramedCollectorManagementAdapter(lambda: _Bad()).async_write_endpoint(
                "1.2.3.4,18899,TCP", apply_changes=False
            )

    async def test_malformed_at_response_is_command_error(self) -> None:
        class _Bad:
            async def async_query(self, command):
                raise CollectorAtError("at_response_invalid")

        with self.assertRaises(CollectorManagementCommandError):
            await AtTextCollectorManagementAdapter(lambda: _Bad()).async_read_endpoint_state()

    async def test_at_wrapped_timeout_is_transport_error(self) -> None:
        class _Bad:
            async def async_query(self, command):
                try:
                    raise asyncio.TimeoutError()
                except asyncio.TimeoutError as exc:
                    raise CollectorAtError("at_response_timeout") from exc

        with self.assertRaises(CollectorManagementTransportError):
            await AtTextCollectorManagementAdapter(lambda: _Bad()).async_read_endpoint_state()

    async def test_direct_timeout_is_transport_error(self) -> None:
        class _Bad:
            async def async_query(self, command):
                raise asyncio.TimeoutError()

        with self.assertRaises(CollectorManagementTransportError):
            await AtTextCollectorManagementAdapter(lambda: _Bad()).async_read_endpoint_state()

    async def test_oserror_is_transport_error(self) -> None:
        class _Bad:
            async def async_send_collector(self, *, fcode, payload, devcode, collector_addr):
                raise OSError("socket gone")

        with self.assertRaises(CollectorManagementTransportError):
            await FramedCollectorManagementAdapter(lambda: _Bad()).async_read_endpoint_state()

    async def test_cancelled_error_propagates_uncaught(self) -> None:
        class _Bad:
            async def async_query(self, command):
                raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await AtTextCollectorManagementAdapter(lambda: _Bad()).async_read_endpoint_state()


class UnavailableCollectorManagementAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_operations_fail_typed(self) -> None:
        adapter = UnavailableCollectorManagementAdapter()
        caps = adapter.capabilities
        self.assertFalse(any((caps.read_endpoint_state, caps.write_endpoint,
                              caps.apply_changes, caps.reboot)))
        for coro in (
            adapter.async_read_endpoint_state(),
            adapter.async_write_endpoint("x", apply_changes=True),
            adapter.async_apply_changes(),
            adapter.async_reboot(),
        ):
            with self.assertRaises(CollectorManagementUnsupportedError):
                await coro


class ErrorHierarchyTests(unittest.TestCase):
    def test_all_management_errors_subclass_base(self) -> None:
        for cls in (
            CollectorManagementUnsupportedError,
            CollectorManagementCommandError,
            CollectorManagementTransportError,
            CollectorManagementConfirmationError,
        ):
            self.assertTrue(issubclass(cls, CollectorManagementError), msg=cls.__name__)


class NoDeadMetadataApiTests(unittest.TestCase):
    def test_management_module_has_no_metadata_api(self) -> None:
        from custom_components.eybond_local.collector import management

        self.assertFalse(hasattr(management, "CollectorMetadataSnapshot"))
        for cls in (
            FramedCollectorManagementAdapter,
            AtTextCollectorManagementAdapter,
            UnavailableCollectorManagementAdapter,
        ):
            self.assertFalse(hasattr(cls, "async_read_metadata"), msg=cls.__name__)


class FactorySelectionTests(unittest.TestCase):
    def _select(self, adapter_id):
        return select_collector_management_adapter(
            adapter_id,
            framed_transport_provider=lambda: _FakeFramedTransport(),
            at_transport_provider=lambda: _FakeAtTransport(),
        )

    def test_framed_id_selects_framed(self) -> None:
        self.assertIsInstance(
            self._select(ADAPTER_COLLECTOR_FRAMED_COMMANDS),
            FramedCollectorManagementAdapter,
        )

    def test_at_id_selects_at(self) -> None:
        self.assertIsInstance(
            self._select(ADAPTER_COLLECTOR_AT_COMMANDS),
            AtTextCollectorManagementAdapter,
        )

    def test_none_and_unknown_select_unavailable(self) -> None:
        for adapter_id in (ADAPTER_NONE, "", "framed_fc4", "raw_passthrough", "garbage"):
            self.assertIsInstance(
                self._select(adapter_id),
                UnavailableCollectorManagementAdapter,
                msg=f"adapter_id={adapter_id!r}",
            )


if __name__ == "__main__":
    unittest.main()
