#!/usr/bin/env python3
"""Dump arbitrary Modbus holding registers through the EyeBond collector."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.discovery import (  # noqa: E402
    DiscoveryAnnouncer,
)
from custom_components.eybond_local.collector.server import EybondServer  # noqa: E402
from custom_components.eybond_local.const import (  # noqa: E402
    DEFAULT_COLLECTOR_ADDR,
    DEFAULT_COLLECTOR_IP,
    DEFAULT_DISCOVERY_INTERVAL,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_MODBUS_DEVICE_ADDR,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
)
from custom_components.eybond_local.payload.modbus import ModbusSession, to_signed_16  # noqa: E402

SMG_PRESET = [(100, 10), (186, 12), (201, 34), (300, 38), (643, 1)]
SMG_GAP_PRESET = [
    (201, 34),  # full live block, includes uncovered 218/221/222/228/230/231
    (300, 38),  # full config block, includes uncovered 304/311/312/314..319/322/328/330
    (406, 1),
    (420, 1),
    (426, 1),
    (643, 1),
]


def _parse_range(value: str) -> tuple[int, int]:
    try:
        start_raw, count_raw = value.split(":", 1)
        return int(start_raw, 0), int(count_raw, 0)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid range '{value}', expected START:COUNT") from exc


def _words_to_ascii(values: list[int]) -> str:
    chars: list[str] = []
    for value in values:
        for byte in ((value >> 8) & 0xFF, value & 0xFF):
            if byte in (0x00, 0xFF):
                continue
            char = chr(byte)
            if 32 <= byte <= 126:
                chars.append(char)
    return "".join(chars)


def _format_range(start: int, values: list[int]) -> dict[str, object]:
    entries = []
    for offset, value in enumerate(values):
        entries.append(
            {
                "register": start + offset,
                "u16": value,
                "s16": to_signed_16(value),
                "hex": f"0x{value:04X}",
            }
        )
    return {
        "start": start,
        "count": len(values),
        "ascii": _words_to_ascii(values),
        "words": list(values),
        "values": entries,
    }


def _build_fixture_payload(
    *,
    args: argparse.Namespace,
    server: EybondServer,
    formatted_ranges: list[dict[str, object]],
) -> dict[str, object]:
    """Convert a live dump into a reusable offline fixture."""

    collector_info = server.collector_info
    return {
        "fixture_version": 1,
        "name": args.fixture_name or "captured_modbus_fixture",
        "collector": {
            "remote_ip": collector_info.remote_ip,
            "collector_pn": collector_info.collector_pn,
            "last_devcode": collector_info.last_devcode,
            "profile_key": collector_info.profile_key,
            "profile_name": collector_info.profile_name,
        },
        "probe_target": {
            "devcode": args.devcode,
            "collector_addr": args.collector_addr,
            "device_addr": args.device_addr,
        },
        "ranges": [
            {
                "start": int(item["start"]),
                "count": int(item["count"]),
                "values": list(item["words"]),
            }
            for item in formatted_ranges
        ],
    }


async def _wait_for_connection(server: EybondServer, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server.connected:
            return True
        await server.wait_until_connected(timeout=0.75)
    return server.connected


async def _run(args: argparse.Namespace) -> int:
    server = EybondServer(
        host=args.server_ip,
        port=args.tcp_port,
        request_timeout=DEFAULT_REQUEST_TIMEOUT,
        heartbeat_interval=float(args.heartbeat_interval),
    )
    announcer = DiscoveryAnnouncer(
        bind_ip=args.server_ip,
        advertised_server_ip=args.server_ip,
        advertised_server_port=args.tcp_port,
        target_ip=args.collector_ip or args.discovery_target,
        udp_port=args.udp_port,
        interval=float(args.discovery_interval),
    )

    ranges = list(args.range or [])
    if args.smg_preset:
        ranges.extend(SMG_PRESET)
    if args.smg_gap_preset:
        ranges.extend(SMG_GAP_PRESET)
    if not ranges:
        ranges = [(201, 34)]

    try:
        await server.start()
        await announcer.start()
        if not await _wait_for_connection(server, args.timeout):
            print(json.dumps({"error": "waiting_for_collector"}, ensure_ascii=False, indent=2))
            return 1

        await announcer.stop()

        session = ModbusSession(
            server,
            devcode=args.devcode,
            collector_addr=args.collector_addr,
            slave_id=args.device_addr,
        )

        formatted_ranges: list[dict[str, object]] = []
        payload: dict[str, object] = {
            "collector": {
                "remote_ip": server.collector_info.remote_ip,
                "collector_pn": server.collector_info.collector_pn,
                "last_devcode": (
                    f"0x{server.collector_info.last_devcode:04X}"
                    if server.collector_info.last_devcode is not None
                    else None
                ),
            },
            "ranges": [],
        }

        for start, count in ranges:
            values = await session.read_holding(start, count)
            formatted = _format_range(start, values)
            formatted_ranges.append(formatted)
            payload["ranges"].append(formatted)

        if args.fixture_out:
            fixture_payload = _build_fixture_payload(
                args=args,
                server=server,
                formatted_ranges=formatted_ranges,
            )
            args.fixture_out.write_text(
                json.dumps(fixture_payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    finally:
        await announcer.stop()
        await server.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-ip", required=True, help="local IP address for reverse TCP")
    parser.add_argument("--collector-ip", default=DEFAULT_COLLECTOR_IP, help="known collector IP for unicast discovery")
    parser.add_argument("--tcp-port", type=int, default=DEFAULT_TCP_PORT)
    parser.add_argument("--udp-port", type=int, default=DEFAULT_UDP_PORT)
    parser.add_argument("--discovery-target", default=DEFAULT_DISCOVERY_TARGET)
    parser.add_argument("--discovery-interval", type=int, default=DEFAULT_DISCOVERY_INTERVAL)
    parser.add_argument("--heartbeat-interval", type=int, default=DEFAULT_HEARTBEAT_INTERVAL)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--devcode", type=lambda value: int(value, 0), default=0x0001)
    parser.add_argument(
        "--collector-addr",
        type=lambda value: int(value, 0),
        default=DEFAULT_COLLECTOR_ADDR,
    )
    parser.add_argument(
        "--device-addr",
        type=lambda value: int(value, 0),
        default=DEFAULT_MODBUS_DEVICE_ADDR,
    )
    parser.add_argument(
        "--range",
        action="append",
        type=_parse_range,
        help="register range as START:COUNT, can be repeated",
    )
    parser.add_argument("--smg-preset", action="store_true", help="dump common SMG ranges")
    parser.add_argument(
        "--smg-gap-preset",
        action="store_true",
        help="dump SMG ranges that include currently uncovered/auxiliary registers",
    )
    parser.add_argument(
        "--fixture-out",
        type=Path,
        help="optional path to save an offline replay fixture JSON",
    )
    parser.add_argument(
        "--fixture-name",
        default="",
        help="optional fixture display name used with --fixture-out",
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
