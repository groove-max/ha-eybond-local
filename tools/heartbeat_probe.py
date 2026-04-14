#!/usr/bin/env python3
"""Capture multiple collector heartbeat responses and print decoded fields."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.discovery import DiscoveryAnnouncer  # noqa: E402
from custom_components.eybond_local.collector.protocol import build_heartbeat_request  # noqa: E402
from custom_components.eybond_local.collector.server import EybondServer  # noqa: E402


async def _run(args: argparse.Namespace) -> int:
    server = EybondServer(
        host=args.server_ip,
        port=args.tcp_port,
        request_timeout=5.0,
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
            tid = server._tid.next()
            frame = build_heartbeat_request(tid, args.heartbeat_interval)
            await server._async_write(frame)
            await asyncio.sleep(args.wait_after_send)
            info = asdict(server.collector_info)
            info["sample"] = index + 1
            samples.append(info)

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
    parser.add_argument("--discovery-interval", type=int, default=2)
    parser.add_argument("--heartbeat-interval", type=int, default=5)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--wait-after-send", type=float, default=1.2)
    parser.add_argument("--timeout", type=float, default=12.0)
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
