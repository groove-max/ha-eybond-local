#!/usr/bin/env python3
"""Probe a SmartESS/EyeBond collector over ICMP, TCP, HTTP and UDP."""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import time
from pathlib import Path
import sys
from urllib import request

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _ping(host: str, count: int = 1, timeout_sec: int = 1) -> dict[str, object]:
    result = subprocess.run(
        ["ping", "-c", str(count), "-W", str(timeout_sec), host],
        capture_output=True,
        text=True,
    )
    return {
        "ok": result.returncode == 0,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }


def _tcp_probe(host: str, port: int, timeout: float) -> dict[str, object]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        return {"port": port, "state": "open"}
    except Exception as exc:
        return {"port": port, "state": "closed", "error": type(exc).__name__}
    finally:
        sock.close()


def _http_probe(url: str, timeout: float) -> dict[str, object]:
    try:
        with request.urlopen(url, timeout=timeout) as response:
            body = response.read(300).decode("utf-8", errors="replace")
            return {
                "url": url,
                "ok": True,
                "status": response.status,
                "snippet": body,
            }
    except Exception as exc:
        return {
            "url": url,
            "ok": False,
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _udp_probe(
    host: str,
    port: int,
    message: bytes,
    timeout: float,
    bind_ip: str | None = None,
    bind_port: int | None = None,
) -> dict[str, object]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        if bind_ip is not None:
            sock.bind((bind_ip, bind_port or 0))
        local_port = sock.getsockname()[1]
        sock.sendto(message, (host, port))
        try:
            data, addr = sock.recvfrom(2048)
            return {
                "message": message.decode("ascii", errors="replace"),
                "local_port": local_port,
                "ok": True,
                "from": addr[0],
                "from_port": addr[1],
                "response": data.decode("ascii", errors="replace"),
            }
        except Exception as exc:
            return {
                "message": message.decode("ascii", errors="replace"),
                "local_port": local_port,
                "ok": False,
                "error": type(exc).__name__,
            }
    finally:
        sock.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector-ip", required=True)
    parser.add_argument("--local-ip")
    parser.add_argument("--tcp-port", type=int, default=8899)
    parser.add_argument("--udp-port", type=int, default=58899)
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--http", action="store_true", help="probe the HTTP UI")
    parser.add_argument("--set-server", action="store_true", help="send set>server probes")
    args = parser.parse_args()

    payload: dict[str, object] = {
        "collector_ip": args.collector_ip,
        "ping": _ping(args.collector_ip),
        "tcp": [
            _tcp_probe(args.collector_ip, port, args.timeout)
            for port in (80, args.tcp_port, args.udp_port, 502)
        ],
    }

    if args.http:
        payload["http"] = [
            _http_probe(f"http://{args.collector_ip}/", args.timeout),
        ]

    if args.set_server:
        if not args.local_ip:
            raise SystemExit("--local-ip is required with --set-server")
        messages = [
            f"set>server={args.local_ip}:{args.tcp_port};".encode(),
            f"set>server={args.local_ip}:{args.tcp_port};\r\n".encode(),
            f"set>server={args.local_ip}:{args.tcp_port};\n".encode(),
        ]
        payload["udp"] = []
        for message in messages:
            payload["udp"].append(
                _udp_probe(
                    args.collector_ip,
                    args.udp_port,
                    message,
                    args.timeout,
                    bind_ip=args.local_ip,
                )
            )
            time.sleep(0.2)

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
