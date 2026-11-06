"""BLE transport ownership, independent of a local adapter or HA proxy."""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.collector.smartess_ble import (
    BleakSmartEssBleLink, SmartEssBleError, SmartEssBleSession, VENDOR_LAYOUT,
    _connect_ble_client,
)
from test_smartess_ble import _FakeBleLink, _FakeBleakClient


class BleLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def session(self):
        link = _FakeBleLink([VENDOR_LAYOUT.service_uuid])
        return link, SmartEssBleSession(link)

    async def test_notify_failure_still_disconnects_and_clears_session(self):
        link, session = self.session()
        await session.connect()
        with patch.object(link, "stop_notify", new=AsyncMock(side_effect=RuntimeError("notify gone"))):
            with self.assertRaisesRegex(RuntimeError, "notify gone"):
                await session.disconnect()
        self.assertFalse(link.connected)
        self.assertFalse(session.connected)
        with self.assertRaisesRegex(SmartEssBleError, "ble_layout_not_selected"):
            _ = session.layout

    async def test_failed_disconnect_revokes_session_and_notifications(self):
        link, session = self.session()
        await session.connect()
        old_callback = link._notify_callback
        with patch.object(link, "disconnect", new=AsyncMock(side_effect=RuntimeError("proxy offline"))):
            with self.assertRaisesRegex(RuntimeError, "proxy offline"):
                await session.disconnect()
        self.assertFalse(session.connected)
        old_callback(b"old reply")
        self.assertTrue(session._notifications.empty())
        with self.assertRaisesRegex(SmartEssBleError, "ble_not_connected"):
            await session.send_bytes(b"AT+FWVER?")
        self.assertEqual(link.write_calls, [])

    async def test_partial_notify_subscription_is_closed_on_connect_failure(self):
        link, session = self.session()
        original = link.start_notify

        async def partial_start(uuid, callback):
            await original(uuid, callback)
            raise RuntimeError("subscribe failed")

        link.start_notify = partial_start
        with self.assertRaisesRegex(RuntimeError, "subscribe failed"):
            await session.connect()
        self.assertFalse(link.connected)
        self.assertFalse(session.connected)
        self.assertEqual(link.stopped_notify_uuid, VENDOR_LAYOUT.notify_uuid)
        with self.assertRaisesRegex(SmartEssBleError, "ble_layout_not_selected"):
            _ = session.layout

    async def test_old_notification_cannot_enter_reconnected_session(self):
        link, session = self.session()
        await session.connect()
        old_callback = link._notify_callback
        await session.disconnect()
        await session.connect()
        old_callback(b"stale")
        link.push_notification(b"current")
        self.assertEqual(await session.wait_for_notification(timeout=0.1), b"current")
        self.assertTrue(session._notifications.empty())
        await session.disconnect()

    async def test_repeated_cancel_waits_for_disconnect_and_preserves_cancel(self):
        link, session = self.session()
        await session.connect()
        started, release = asyncio.Event(), asyncio.Event()
        original = link.disconnect

        async def held_disconnect():
            started.set()
            await release.wait()
            await original()

        link.disconnect = held_disconnect
        task = asyncio.create_task(session.disconnect())
        await asyncio.wait_for(started.wait(), 0.5)
        try:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
        finally:
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(link.connected)
        self.assertFalse(session.connected)

    async def test_link_reconnect_never_reuses_disconnected_client(self):
        clients = [_FakeBleakClient("AA:BB:CC:DD:EE:FF", [VENDOR_LAYOUT.service_uuid]) for _ in range(2)]
        factory = lambda address: clients.pop(0)
        first, second = clients
        link = BleakSmartEssBleLink(first.address, client_factory=factory)
        await link.connect()
        await link.disconnect()
        await link.connect()
        self.assertFalse(first.connected)
        self.assertTrue(second.connected)
        await link.disconnect()

    async def test_link_service_discovery_failure_closes_owned_client(self):
        client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [])
        client.get_services = AsyncMock(side_effect=RuntimeError("services failed"))
        link = BleakSmartEssBleLink(client.address, client_factory=lambda address: client)
        with self.assertRaisesRegex(RuntimeError, "services failed"):
            await link.connect()
        self.assertFalse(client.connected)
        with self.assertRaisesRegex(SmartEssBleError, "ble_not_connected"):
            await link.write(VENDOR_LAYOUT.write_uuid, b"AT+FWVER?")
        self.assertEqual(client.write_calls, [])

    async def test_failed_link_disconnect_revokes_client_without_claiming_success(self):
        client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [VENDOR_LAYOUT.service_uuid])
        link = BleakSmartEssBleLink(client.address, client_factory=lambda address: client)
        await link.connect()
        client.disconnect = AsyncMock(side_effect=RuntimeError("proxy offline"))
        with self.assertRaisesRegex(RuntimeError, "proxy offline"):
            await link.disconnect()
        with self.assertRaisesRegex(SmartEssBleError, "ble_not_connected"):
            await link.read(VENDOR_LAYOUT.write_uuid)

    async def test_direct_client_connect_failure_closes_partial_connection(self):
        client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [])

        async def partial_connect():
            client.connected = True
            raise RuntimeError("connect failed")

        client.connect = partial_connect
        with self.assertRaisesRegex(RuntimeError, "connect failed"):
            await _connect_ble_client(client)
        self.assertFalse(client.connected)

    async def test_failed_connect_keeps_primary_error_when_cleanup_also_fails(self):
        link, session = self.session()
        link.start_notify = AsyncMock(side_effect=RuntimeError("primary subscribe failure"))
        link.disconnect = AsyncMock(side_effect=ValueError("secondary cleanup failure"))
        with self.assertRaisesRegex(RuntimeError, "primary subscribe failure"):
            await session.connect()
        link.disconnect.assert_awaited_once()
        self.assertFalse(session.connected)

    async def test_cancel_during_notify_setup_closes_partial_connection(self):
        link, session = self.session()
        started = asyncio.Event()

        async def held_subscribe(uuid, callback):
            link._notify_callback = callback
            started.set()
            await asyncio.Future()

        link.start_notify = held_subscribe
        task = asyncio.create_task(session.connect())
        await asyncio.wait_for(started.wait(), 0.5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(link.connected)
        self.assertFalse(session.connected)
        link._notify_callback(b"late")
        self.assertTrue(session._notifications.empty())

    async def test_notify_cleanup_timeout_does_not_skip_disconnect(self):
        link, session = self.session()
        await session.connect()

        async def stalled_stop(uuid):
            await asyncio.Future()

        link.stop_notify = stalled_stop
        with patch("custom_components.eybond_local.collector.smartess_ble._BLE_CLEANUP_TIMEOUT", 0.01):
            with self.assertRaises(TimeoutError):
                await session.disconnect()
        self.assertFalse(link.connected)
        self.assertFalse(session.connected)

    async def test_reconnect_waits_for_old_cleanup_before_subscribing(self):
        link, session = self.session()
        await session.connect()
        original_connect, original_disconnect = link.connect, link.disconnect
        started, release = asyncio.Event(), asyncio.Event()

        async def held_disconnect():
            started.set()
            await release.wait()
            await original_disconnect()

        link.disconnect = held_disconnect
        link.connect = AsyncMock(wraps=original_connect)
        closing = asyncio.create_task(session.disconnect())
        await asyncio.wait_for(started.wait(), 0.5)
        reconnect = asyncio.create_task(session.connect())
        try:
            await asyncio.sleep(0)
            self.assertFalse(reconnect.done())
            link.connect.assert_not_awaited()
        finally:
            release.set()
            await closing
            await reconnect
        self.assertTrue(session.connected)
        link.connect.assert_awaited_once()
        await session.disconnect()

    async def test_link_disconnect_timeout_is_reported_and_never_reuses_client(self):
        client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [VENDOR_LAYOUT.service_uuid])
        link = BleakSmartEssBleLink(client.address, client_factory=lambda address: client)
        await link.connect()

        async def stalled_disconnect():
            await asyncio.Future()

        client.disconnect = stalled_disconnect
        with patch("custom_components.eybond_local.collector.smartess_ble._BLE_CLEANUP_TIMEOUT", 0.01):
            with self.assertRaises(TimeoutError):
                await link.disconnect()
        self.assertIsNone(link._client)
        self.assertTrue(client.connected)  # No claim that the remote slot was released.
