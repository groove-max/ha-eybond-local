"""The production identity sender must not advertise a NAT-internal listener."""
import asyncio
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from custom_components.eybond_local.connection.callback_identity import CallbackIdentityRequest, _ProductionTriggerSender


class AdvertisedRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_udp_payload_uses_override_while_sender_binds_locally(self):
        loop = asyncio.get_running_loop()
        received = asyncio.Queue()

        class Peer(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

            def datagram_received(self, data, addr):
                received.put_nowait((data, addr))
                self.transport.sendto(b"rsp>server=1;", addr)

        transport, _ = await loop.create_datagram_endpoint(Peer, local_addr=("127.0.0.1", 0))
        port = transport.get_extra_info("sockname")[1]
        try:
            for host, advertised_port, expected in (
                ("203.0.113.10", 18899, b"set>server=203.0.113.10:18899;"),
                ("", 0, b"set>server=127.0.0.1:8899;"),
                ("203.0.113.10", 0, b"set>server=203.0.113.10:8899;"),
                ("", 502, b"set>server=127.0.0.1:502;"),
            ):
                request = CallbackIdentityRequest(server_ip="127.0.0.1", tcp_port=8899,
                    udp_port=port, target_ip="127.0.0.1",
                    advertised_server_ip=host, advertised_tcp_port=advertised_port)
                await _ProductionTriggerSender().async_send(request)
                data, addr = await asyncio.wait_for(received.get(), 1)
                self.assertEqual(data, expected)
                self.assertEqual(addr[0], "127.0.0.1")
        finally:
            transport.close()
            await asyncio.sleep(0)


if __name__ == "__main__":
    unittest.main()
