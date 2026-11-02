"""Loopback-only ValueCloud HTTP → raw Modbus → fail-closed learning lab.

Catalogue labels may come from public reports, but identities, credentials and
ALL register mappings are synthetic. This is not a PV3300 hardware emulator or
evidence that a cloud command actually powers an inverter on/off.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
import hashlib
import hmac
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from custom_components.eybond_local.payload.modbus import (
    build_read_holding_request,
    build_write_multiple_request,
    build_write_single_request,
    crc16_modbus,
)
from custom_components.eybond_local.support.shadow_learning.backend import ShadowLearningSeed
from custom_components.eybond_local.support.shadow_learning.proxy import InProcessFailClosedShadowProxyHandler
from custom_components.eybond_local.support.shadow_learning.valuecloud_orchestrator import (
    async_orchestrate_valuecloud_shadow_learning,
)
from custom_components.eybond_local.valuecloud_cloud import ValueCloudSession


class ValueCloudLearningLab:
    """Use production HTTP client, owned dispatch, proxy and correlation logic."""

    pn = "E5000020000000"
    sn = "SYNTHETIC-ONLY"
    session = ValueCloudSession(token="synthetic-token", secret="synthetic-secret")

    def __init__(self, directory: Path, fields: list[dict], *, mapping=None,
                 fault: str = "", fault_index: int = 0, unit: int = 1):
        self.directory = directory
        self.fields = fields
        self.mapping = mapping or {field["id"]: (60000 + i, 16)
                                   for i, field in enumerate(fields)}
        self.unit = unit
        self.fault = fault
        self.fault_index = fault_index
        self.requests = []
        self.nacks = []
        self.collector_traffic = bytearray()
        self.errors = []
        self._servers = []
        self._tasks = set()
        self._writers = []
        self._release = asyncio.Event()
        self._upstream = asyncio.get_running_loop().create_future()
        self.proxy = None

    async def _listen(self, callback):
        async def tracked(reader, writer):
            task = asyncio.current_task()
            self._tasks.add(task)
            self._writers.append(writer)
            try:
                await callback(reader, writer)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.errors.append(exc)
            finally:
                writer.close()
                with suppress(ConnectionError):
                    await writer.wait_closed()
                self._tasks.discard(task)

        server = await asyncio.start_server(tracked, "127.0.0.1", 0)
        self._servers.append(server)
        return server.sockets[0].getsockname()[1]

    async def __aenter__(self):
        try:
            async def upstream(reader, writer):
                self._upstream.set_result((reader, writer))
                await self._release.wait()

            port = await self._listen(upstream)
            seed = ShadowLearningSeed(
                session_id="synthetic-valuecloud-loopback", entry_id="synthetic",
                collector_pn=self.pn, collector_cloud_profile_key="smartess_at",
                collector_cloud_profile_label="Synthetic ValueCloud raw Modbus",
                collector_cloud_profile_source="test", collector_cloud_profile_confidence="high",
                collector_callback_endpoint="127.0.0.1,18899,TCP",
                effective_metadata_snapshot={"protocol_family": "modbus_rtu",
                                             "profile_name": "must_pv_ph18/pv3300.json",
                                             "register_schema_name": "must_pv_ph18/pv3300.json"},
                command_responses={"QID": self.pn}, register_bank={300: 10},
                latest_support_evidence=None, write_response_mode="exception", allow_ack_writes=False,
            )
            self.proxy = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1", upstream_port=port, seed=seed,
                output_path=self.directory / "synthetic-shadow.jsonl",
                expected_session_protocol="at_text",
            )
            await self.proxy.start()
            port = await self._listen(self.proxy.handle_client)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            self._writers.append(writer)
            assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b"AT+DTUPN?\r\n"
            identity = f"AT+DTUPN:{self.pn}\r\n".encode()
            writer.write(identity)
            await writer.drain()
            self.cloud_reader, self.cloud_writer = await asyncio.wait_for(self._upstream, 2)
            self.cloud_writer.write(b"AT+DTUPN?\r\n")
            await self.cloud_writer.drain()
            assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b"AT+DTUPN?\r\n"
            writer.write(identity)
            await writer.drain()
            assert await asyncio.wait_for(self.cloud_reader.readuntil(b"\n"), 2) == identity
            self.cloud_writer.write(build_read_holding_request(self.unit, 300, 1))
            await self.cloud_writer.drain()
            response = await asyncio.wait_for(self.cloud_reader.readexactly(7), 2)
            assert response[:5] == bytes((self.unit, 3, 2, 0, 10))
            assert self.proxy.ready
            assert self.proxy.status()["collector_identity_verified"]

            async def drain_collector():
                while data := await reader.read(1024):
                    self.collector_traffic.extend(data)

            task = asyncio.create_task(drain_collector())
            self._tasks.add(task)
            port = await self._listen(self._http)
            self.base_url = f"http://127.0.0.1:{port}"
            return self
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def __aexit__(self, *_args):
        for server in self._servers:
            server.close()
        self._release.set()
        if self.proxy is not None:
            await self.proxy.stop()
        for writer in self._writers:
            writer.close()
        for writer in self._writers:
            with suppress(ConnectionError):
                await writer.wait_closed()
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for server in self._servers:
            await server.wait_closed()
        if self.errors:
            raise AssertionError(f"Loopback handler failed: {self.errors!r}") from self.errors[0]

    async def _http(self, reader, writer):
        head = (await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 2)).decode()
        line, *lines = head.strip().split("\r\n")
        method, url, _version = line.split()
        parts = urlsplit(url)
        headers = {key.lower(): value.strip() for key, value in
                   (line.split(":", 1) for line in lines)}
        query = {key: values[0] for key, values in parse_qs(parts.query).items()}
        assert method == "GET"
        assert parts.path == "/ppe/api/auth/web/ctrlDevice"
        assert set(query) == {"pn", "sn", "devcode", "devaddr", "id", "val", "datatype"}
        assert query["pn"] == self.pn and query["sn"] == self.sn
        assert query["devcode"] == "6422" and query["devaddr"] == str(self.unit)
        assert headers["token"] == self.session.token
        assert headers["auth"] == self.session.auth
        assert headers["sign"] == hmac.new(self.session.secret.encode(), parts.path.encode(), hashlib.sha256).hexdigest()
        assert headers["project"] == "IOT"
        field = next(field for field in self.fields if field["id"] == query["id"])
        assert query["datatype"] == str(field["datatype"])
        index = len(self.requests)
        self.requests.append(query)
        fault = self.fault if index == self.fault_index else ""
        if fault not in {"timeout_before_wire", "success_without_wire", "disconnect_before_wire"}:
            register, function = self.mapping[query["id"]]
            value = int(float(query["val"]))
            command = (build_write_single_request(self.unit, register, value) if function == 6
                       else build_write_multiple_request(self.unit, register, [value]))
            self.cloud_writer.write(command)
            await self.cloud_writer.drain()
            nack = await asyncio.wait_for(self.cloud_reader.readexactly(5), 2)
            assert nack[:3] == bytes((self.unit, function | 128, 1))
            assert int.from_bytes(nack[-2:], "little") == crc16_modbus(nack[:-2])
            self.nacks.append(nack)
        if fault in {"timeout_before_wire", "timeout_after_wire"}:
            await self._release.wait()
            return
        if fault == "disconnect_before_wire":
            self.cloud_writer.close()
            await self.cloud_writer.wait_closed()
            async with asyncio.timeout(2):
                while self.proxy.ready:
                    await asyncio.sleep(0.01)
        success = fault == "success_without_wire"
        body = json.dumps({"code": 200 if success else 409, "success": success,
                           "message": "synthetic response", "data": {}}).encode()
        writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body)
        await writer.drain()

    async def learn(self):
        async def wait(cursor, seconds):
            return await self.proxy.wait_for_observations_since(cursor, timeout_seconds=seconds)

        return await async_orchestrate_valuecloud_shadow_learning(
            batch_control={"groups": []}, control_strategy={"fields": self.fields},
            device_ctrl={"fields": self.fields[:12]},  # legacy duplicate catalogue
            session=self.session, pn=self.pn, sn=self.sn, devcode=6422, devaddr=self.unit,
            dry_run=False, confirm_cloud_write=True, shadow_session_state="ready", field_ids=[],
            base_url=self.base_url, timeout=0.5 if self.fault else 5,
            correlation_timeout_seconds=0.1,
            observation_cursor=self.proxy.observation_cursor,
            current_observations_since=self.proxy.observations_since,
            wait_for_observations_since=wait, is_session_ready=lambda: self.proxy.ready,
            read_map_snapshot=lambda: self.proxy.read_map,
        )
