#!/usr/bin/env python3
"""Send a raw collector-level EyeBond frame and print the decoded response."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import logging
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.discovery import DiscoveryAnnouncer  # noqa: E402
from custom_components.eybond_local.collector.protocol import (  # noqa: E402
    FC_HEARTBEAT,
    build_heartbeat_payload,
)
from custom_components.eybond_local.collector.server import EybondServer  # noqa: E402


def _parse_int(value: str) -> int:
    return int(value, 0)


def _decode_ascii(payload: bytes) -> str:
    return payload.decode("ascii", errors="replace")


def _resolve_payload(args: argparse.Namespace) -> bytes:
    if args.payload_hex and args.payload_ascii:
        raise SystemExit("use either --payload-hex or --payload-ascii")
    if args.fcode == FC_HEARTBEAT and not args.payload_hex and not args.payload_ascii:
        return build_heartbeat_payload(args.heartbeat_interval)
    if args.payload_hex:
        return bytes.fromhex(args.payload_hex)
    if args.payload_ascii:
        return args.payload_ascii.encode("ascii")
    return b""


async def _run(args: argparse.Namespace) -> int:
    payload = _resolve_payload(args)
    server = EybondServer(
        host=args.server_ip,
        port=args.tcp_port,
        request_timeout=args.request_timeout,
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

    samples: list[dict[str, object]] = []

    try:
        await server.start()
        await announcer.start()
        ok = await server.wait_until_connected(timeout=args.timeout)
        if not ok:
            print(json.dumps({"error": "waiting_for_collector"}, ensure_ascii=False, indent=2))
            return 1

        await announcer.stop()

        for index in range(args.samples):
            try:
                header, response_payload = await server.async_send_collector(
                    fcode=args.fcode,
                    payload=payload,
                    devcode=args.devcode,
                    collector_addr=args.devaddr,
                )
                samples.append(
                    {
                        "sample": index + 1,
                        "request": {
                            "fcode": args.fcode,
                            "devcode": f"0x{args.devcode:04X}",
                            "devaddr": f"0x{args.devaddr:02X}",
                            "payload_hex": payload.hex(),
                            "payload_ascii": _decode_ascii(payload),
                        },
                        "response": {
                            "header": asdict(header),
                            "payload_hex": response_payload.hex(),
                            "payload_ascii": _decode_ascii(response_payload),
                        },
                        "collector": asdict(server.collector_info),
                    }
                )
            except Exception as exc:
                samples.append(
                    {
                        "sample": index + 1,
                        "request": {
                            "fcode": args.fcode,
                            "devcode": f"0x{args.devcode:04X}",
                            "devaddr": f"0x{args.devaddr:02X}",
                            "payload_hex": payload.hex(),
                            "payload_ascii": _decode_ascii(payload),
                        },
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
            if index + 1 < args.samples:
                await asyncio.sleep(args.pause)

        print(json.dumps({"samples": samples}, ensure_ascii=False, indent=2))
        return 0
    finally:
        await announcer.stop()
        await server.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-ip", required=True)
    parser.add_argument("--collector-ip", default="")
    parser.add_argument("--tcp-port", type=int, default=8899)
    parser.add_argument("--udp-port", type=int, default=58899)
    parser.add_argument("--discovery-target", default="255.255.255.255")
    parser.add_argument("--discovery-interval", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--request-timeout", type=float, default=5.0)
    parser.add_argument("--heartbeat-interval", type=int, default=5)
    parser.add_argument("--fcode", type=_parse_int, required=True)
    parser.add_argument("--devcode", type=_parse_int, default=0)
    parser.add_argument("--devaddr", type=_parse_int, default=1)
    parser.add_argument("--payload-hex", default="")
    parser.add_argument("--payload-ascii", default="")
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--pause", type=float, default=1.0)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
