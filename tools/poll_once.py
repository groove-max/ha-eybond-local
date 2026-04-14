#!/usr/bin/env python3
"""Poll the collector once using the same stack as the HA integration."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import logging
from pathlib import Path
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.const import (  # noqa: E402
    DEFAULT_COLLECTOR_IP,
    DEFAULT_DISCOVERY_INTERVAL,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    DRIVER_HINT_AUTO,
)
from custom_components.eybond_local.runtime.hub import EybondHub  # noqa: E402
from custom_components.eybond_local.schema import build_runtime_ui_schema  # noqa: E402


async def _run(args: argparse.Namespace) -> int:
    hub = EybondHub(
        server_ip=args.server_ip,
        collector_ip=args.collector_ip,
        tcp_port=args.tcp_port,
        udp_port=args.udp_port,
        discovery_target=args.discovery_target,
        discovery_interval=args.discovery_interval,
        heartbeat_interval=args.heartbeat_interval,
        driver_hint=args.driver_hint,
    )

    deadline = time.monotonic() + args.timeout
    try:
        await hub.async_start()
        while time.monotonic() < deadline:
            snapshot = await hub.async_refresh()
            if snapshot.connected and snapshot.inverter is not None:
                print(json.dumps(_snapshot_to_json(snapshot, args.full_snapshot), ensure_ascii=False, indent=2, sort_keys=True))
                return 0
            await asyncio.sleep(args.poll_interval)

        snapshot = await hub.async_refresh()
        print(json.dumps(_snapshot_to_json(snapshot, args.full_snapshot), ensure_ascii=False, indent=2, sort_keys=True))
        return 1
    finally:
        await hub.async_stop()


def _snapshot_to_json(snapshot, full_snapshot: bool) -> object:
    if not full_snapshot:
        return snapshot.values

    payload: dict[str, object] = {
        "connected": snapshot.connected,
        "values": snapshot.values,
        "last_error": snapshot.last_error,
    }
    if snapshot.collector is not None:
        payload["collector"] = asdict(snapshot.collector)
    if snapshot.inverter is not None:
        payload["inverter"] = {
            "driver_key": snapshot.inverter.driver_key,
            "protocol_family": snapshot.inverter.protocol_family,
            "model_name": snapshot.inverter.model_name,
            "serial_number": snapshot.inverter.serial_number,
            "probe_target": asdict(snapshot.inverter.probe_target),
            "details": snapshot.inverter.details,
            "capability_groups": [asdict(group) for group in snapshot.inverter.capability_groups],
            "capabilities": [asdict(capability) for capability in snapshot.inverter.capabilities],
            "capability_presets": [asdict(preset) for preset in snapshot.inverter.capability_presets],
        }
        payload["ui_schema"] = build_runtime_ui_schema(snapshot.inverter, snapshot.values)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-ip", required=True, help="local IP address for reverse TCP")
    parser.add_argument("--collector-ip", default=DEFAULT_COLLECTOR_IP, help="known collector IP for unicast discovery")
    parser.add_argument("--tcp-port", type=int, default=DEFAULT_TCP_PORT)
    parser.add_argument("--udp-port", type=int, default=DEFAULT_UDP_PORT)
    parser.add_argument("--discovery-target", default=DEFAULT_DISCOVERY_TARGET)
    parser.add_argument("--discovery-interval", type=int, default=DEFAULT_DISCOVERY_INTERVAL)
    parser.add_argument("--heartbeat-interval", type=int, default=DEFAULT_HEARTBEAT_INTERVAL)
    parser.add_argument("--driver-hint", default=DRIVER_HINT_AUTO)
    parser.add_argument("--poll-interval", type=float, default=float(DEFAULT_POLL_INTERVAL))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--full-snapshot", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
