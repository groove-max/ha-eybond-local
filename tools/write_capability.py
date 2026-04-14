#!/usr/bin/env python3
"""List or write inverter capabilities through the EyeBond local stack."""

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

from custom_components.eybond_local.const import (  # noqa: E402
    DEFAULT_COLLECTOR_IP,
    DEFAULT_DISCOVERY_INTERVAL,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    DRIVER_HINT_AUTO,
)
from custom_components.eybond_local.runtime.hub import EybondHub  # noqa: E402
from custom_components.eybond_local.schema import build_runtime_ui_schema  # noqa: E402


async def _wait_for_snapshot(hub: EybondHub, timeout: float):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snapshot = await hub.async_refresh()
        if snapshot.connected and snapshot.inverter is not None:
            return snapshot
        await asyncio.sleep(1.0)
    return await hub.async_refresh()


def _list_payload(snapshot) -> dict[str, object]:
    inverter = snapshot.inverter
    if inverter is None:
        return {"error": "inverter_not_detected", "values": snapshot.values}
    return build_runtime_ui_schema(inverter, snapshot.values)


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

    try:
        await hub.async_start()
        snapshot = await _wait_for_snapshot(hub, args.timeout)
        if snapshot.inverter is None:
            print(json.dumps({"error": snapshot.last_error or "inverter_not_detected"}, ensure_ascii=False, indent=2))
            return 1

        if args.list:
            print(json.dumps(_list_payload(snapshot), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.preset:
            result = await hub.async_apply_preset(args.preset)
            snapshot = await hub.async_refresh()
            print(
                json.dumps(
                    {
                        "preset": result,
                        "snapshot": snapshot.values if args.full_snapshot else None,
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if not args.key:
            raise SystemExit("--key is required unless --list or --preset is used")
        capability = snapshot.inverter.get_capability(args.key)
        if args.value is None and capability.value_kind != "action":
            raise SystemExit("--value is required for writes unless the capability is an action")

        written_value = await hub.async_write_capability(args.key, args.value)
        snapshot = await hub.async_refresh()
        print(
            json.dumps(
                {
                    "key": args.key,
                    "requested_value": args.value,
                    "written_value": written_value,
                    "current_value": snapshot.values.get(args.key),
                    "snapshot": snapshot.values if args.full_snapshot else None,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    finally:
        await hub.async_stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-ip", required=True, help="local IP address for reverse TCP")
    parser.add_argument("--collector-ip", default=DEFAULT_COLLECTOR_IP)
    parser.add_argument("--tcp-port", type=int, default=DEFAULT_TCP_PORT)
    parser.add_argument("--udp-port", type=int, default=DEFAULT_UDP_PORT)
    parser.add_argument("--discovery-target", default=DEFAULT_DISCOVERY_TARGET)
    parser.add_argument("--discovery-interval", type=int, default=DEFAULT_DISCOVERY_INTERVAL)
    parser.add_argument("--heartbeat-interval", type=int, default=DEFAULT_HEARTBEAT_INTERVAL)
    parser.add_argument("--driver-hint", default=DRIVER_HINT_AUTO)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--preset", default="")
    parser.add_argument("--key", default="")
    parser.add_argument("--value")
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
