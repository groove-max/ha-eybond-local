#!/usr/bin/env python3
"""Run one-shot onboarding auto-detection and print structured results."""

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

from custom_components.eybond_local.const import (  # noqa: E402
    DEFAULT_COLLECTOR_IP,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    DRIVER_HINT_AUTO,
)
from custom_components.eybond_local.onboarding.detection import OnboardingDetector  # noqa: E402


async def _run(args: argparse.Namespace) -> int:
    detector = OnboardingDetector(
        server_ip=args.server_ip,
        tcp_port=args.tcp_port,
        udp_port=args.udp_port,
        heartbeat_interval=args.heartbeat_interval,
        driver_hint=args.driver_hint,
    )
    results = await detector.async_auto_detect(
        collector_ip=args.collector_ip,
        discovery_target=args.discovery_target,
        discovery_timeout=args.discovery_timeout,
        connect_timeout=args.connect_timeout,
        heartbeat_timeout=args.heartbeat_timeout,
    )
    payload = [asdict(result) for result in results]
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    if any(result.match is not None for result in results):
        return 0
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-ip", required=True, help="local IP address for reverse TCP")
    parser.add_argument("--collector-ip", default=DEFAULT_COLLECTOR_IP, help="known collector IP for unicast detection")
    parser.add_argument("--discovery-target", default=DEFAULT_DISCOVERY_TARGET)
    parser.add_argument("--tcp-port", type=int, default=DEFAULT_TCP_PORT)
    parser.add_argument("--udp-port", type=int, default=DEFAULT_UDP_PORT)
    parser.add_argument("--heartbeat-interval", type=int, default=DEFAULT_HEARTBEAT_INTERVAL)
    parser.add_argument("--driver-hint", default=DRIVER_HINT_AUTO)
    parser.add_argument("--discovery-timeout", type=float, default=1.5)
    parser.add_argument("--connect-timeout", type=float, default=5.0)
    parser.add_argument("--heartbeat-timeout", type=float, default=2.0)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
