"""Host IPv4-interface discovery shared by EyeBond data-entry flows."""

from __future__ import annotations

import ipaddress
import json
import re
import socket
import subprocess
from typing import Any

_INTERNAL_INTERFACE_NAMES = frozenset({"docker0", "hassio"})
_INTERNAL_INTERFACE_PREFIXES = (
    "br-",
    "cni",
    "docker",
    "flannel",
    "veth",
    "virbr",
)
_IP_ADDR_SHOW_ONELINE = re.compile(
    r"^\d+:\s+(?P<ifname>\S+)\s+inet\s+(?P<ip>\d+\.\d+\.\d+\.\d+)/(?P<prefixlen>\d+)"
    r"(?:\s+brd\s+(?P<broadcast>\d+\.\d+\.\d+\.\d+))?\s+scope\s+(?P<scope>\S+)"
)


def _is_usable_ipv4(ip: str) -> bool:
    return bool(ip) and not ip.startswith(("127.", "169.254."))


def _is_user_selectable_interface(ifname: str) -> bool:
    normalized = str(ifname or "").strip().lower()
    if not normalized:
        return True
    if normalized in _INTERNAL_INTERFACE_NAMES:
        return False
    return not normalized.startswith(_INTERNAL_INTERFACE_PREFIXES)


def get_local_ip() -> str:
    """Return the host's default-route IPv4 address when available."""

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except OSError:
        return ""


def _build_interface_entry(
    *,
    ifname: str,
    ip: str,
    prefixlen: int | None = None,
    broadcast: str = "",
) -> dict[str, str]:
    label = f"{ifname} — {ip}" if ifname else ip
    interface: dict[str, str] = {"name": ifname, "ip": ip, "label": label}
    if prefixlen is not None and 0 < prefixlen <= 32:
        try:
            network = ipaddress.ip_interface(f"{ip}/{prefixlen}").network
        except ValueError:
            network = None
        if network is not None:
            interface["prefixlen"] = str(prefixlen)
            interface["network"] = str(network)
            if prefixlen < 31:
                interface["broadcast"] = str(network.broadcast_address)
    if broadcast:
        interface["broadcast"] = broadcast
    return interface


def _dedupe_interfaces(
    interfaces: list[dict[str, str]],
) -> list[dict[str, str]]:
    deduped: dict[str, dict[str, str]] = {}
    for interface in interfaces:
        deduped.setdefault(interface["ip"], interface)
    return list(deduped.values())


def _parse_json_interfaces(raw: list[dict[str, Any]]) -> list[dict[str, str]]:
    interfaces: list[dict[str, str]] = []
    for item in raw:
        ifname = str(item.get("ifname", "")).strip()
        if ifname and not _is_user_selectable_interface(ifname):
            continue
        for addr in item.get("addr_info", []):
            ip = str(addr.get("local", "")).strip()
            if not ip or addr.get("family") != "inet":
                continue
            if addr.get("scope") not in {"global", "site"}:
                continue
            if not _is_usable_ipv4(ip):
                continue
            try:
                prefixlen = int(addr.get("prefixlen"))
            except (TypeError, ValueError):
                prefixlen = None
            interfaces.append(
                _build_interface_entry(
                    ifname=ifname,
                    ip=ip,
                    prefixlen=prefixlen,
                    broadcast=str(addr.get("broadcast", "")).strip(),
                )
            )
    return _dedupe_interfaces(interfaces)


def _parse_oneline_interfaces(output: str) -> list[dict[str, str]]:
    interfaces: list[dict[str, str]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _IP_ADDR_SHOW_ONELINE.match(line)
        if match is None:
            continue
        ip = str(match.group("ip") or "").strip()
        if not _is_usable_ipv4(ip):
            continue
        ifname = str(match.group("ifname") or "").strip()
        if ifname and not _is_user_selectable_interface(ifname):
            continue
        if str(match.group("scope") or "").strip() not in {"global", "site"}:
            continue
        try:
            prefixlen = int(match.group("prefixlen"))
        except (TypeError, ValueError):
            prefixlen = None
        interfaces.append(
            _build_interface_entry(
                ifname=ifname,
                ip=ip,
                prefixlen=prefixlen,
                broadcast=str(match.group("broadcast") or "").strip(),
            )
        )
    return _dedupe_interfaces(interfaces)


def _ifaddr_interfaces() -> list[dict[str, str]]:
    """Read interfaces through getifaddrs(); needs no `ip` binary in the container."""

    try:
        import ifaddr
    except ImportError:
        return []

    interfaces: list[dict[str, str]] = []
    try:
        adapters = ifaddr.get_adapters()
    except Exception:  # noqa: BLE001 - any libc/ctypes failure means "unknown"
        return []
    for adapter in adapters:
        ifname = str(getattr(adapter, "nice_name", "") or "").strip()
        if ifname and not _is_user_selectable_interface(ifname):
            continue
        for address in getattr(adapter, "ips", ()):
            ip = address.ip
            if not isinstance(ip, str) or not _is_usable_ipv4(ip):
                continue
            interfaces.append(
                _build_interface_entry(
                    ifname=ifname,
                    ip=ip,
                    prefixlen=int(address.network_prefix),
                )
            )
    return _dedupe_interfaces(interfaces)


def list_ipv4_interfaces() -> list[dict[str, str]]:
    """Return active global IPv4 interfaces, or [] when they cannot be enumerated.

    An empty result means "unknown", not "no interfaces": callers must not treat
    it as proof that a configured address has gone away.
    """

    interfaces = _ifaddr_interfaces()
    if interfaces:
        return interfaces

    try:
        output = subprocess.check_output(
            ["ip", "-j", "-4", "addr", "show", "up"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        interfaces = _parse_json_interfaces(json.loads(output))
        if interfaces:
            return interfaces
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        pass

    # BusyBox `ip` (Home Assistant OS / container) has no -j.
    try:
        output = subprocess.check_output(
            ["ip", "-o", "-4", "addr", "show", "up"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return _parse_oneline_interfaces(output)
    except (OSError, subprocess.SubprocessError):
        return []


def get_ipv4_interfaces() -> list[dict[str, str]]:
    """Return active global IPv4 interfaces with human-friendly labels."""

    interfaces = list_ipv4_interfaces()
    if interfaces:
        return interfaces

    fallback_ip = get_local_ip()
    if not fallback_ip:
        return []
    return [{"name": "default", "ip": fallback_ip, "label": fallback_ip}]


__all__ = ["get_ipv4_interfaces", "get_local_ip", "list_ipv4_interfaces"]
