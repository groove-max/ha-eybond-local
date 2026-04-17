#!/usr/bin/env python3
"""Query SmartESS cloud endpoints using APK-compatible request signing."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_evidence import (
    build_smartess_device_bundle_cloud_evidence,
    export_cloud_evidence,
)
from custom_components.eybond_local.smartess_cloud import normalize_device_settings


DEFAULT_BASE_URL = "https://android.shinemonitor.com/public/"
DEFAULT_LANGUAGE = "en"
DEFAULT_APP_ID = "com.eybond.smartclient.ess"
DEFAULT_APP_VERSION = "3.43.3.0"
DEFAULT_COMPANY_KEY = "bnrl_frRFjEz8Mkn"
DEFAULT_DEVICE_TYPE = 2304
DEFAULT_TIMEOUT = 15.0


class SmartEssCloudError(RuntimeError):
    """Raised when the SmartESS cloud request flow fails."""


@dataclass(frozen=True, slots=True)
class ApiEnvelope:
    """One parsed SmartESS cloud response envelope."""

    err: int
    desc: str
    dat: Any
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SessionCredentials:
    """Session credentials returned by SmartESS cloud authentication."""

    token: str
    secret: str
    uid: str = ""
    usr: str = ""
    role: int | None = None
    expire: int | None = None


def _parse_int(value: str) -> int:
    return int(value, 0)


def _sha1_lower(value: str | bytes) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha1(data).hexdigest()


def _normalize_base_url(base_url: str) -> str:
    normalized = str(base_url or DEFAULT_BASE_URL).strip()
    if not normalized:
        normalized = DEFAULT_BASE_URL
    if not normalized.endswith("/"):
        normalized += "/"
    return normalized


def _salt_millis() -> str:
    return str(int(time.time() * 1000))


def _decode_response_body(body: bytes) -> str:
    text = body.decode("utf-8", errors="replace").strip()
    if text.startswith("null(") and text.endswith(")"):
        return text[text.find("(") + 1 : -1]
    return text


def _http_get_json(url: str, *, timeout: float = DEFAULT_TIMEOUT) -> ApiEnvelope:
    request = Request(
        url,
        headers={
            "User-Agent": "okhttp/3.12.1",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = _decode_response_body(response.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace").strip()
        raise SmartEssCloudError(f"http_error:{exc.code}:{detail}") from exc
    except URLError as exc:
        raise SmartEssCloudError(f"network_error:{exc.reason}") from exc

    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SmartEssCloudError(f"invalid_json:{payload[:200]}") from exc

    if not isinstance(raw, dict):
        raise SmartEssCloudError("invalid_envelope:not_an_object")

    return ApiEnvelope(
        err=int(raw.get("err", -1)),
        desc=str(raw.get("desc", "")),
        dat=raw.get("dat"),
        raw=raw,
    )


def _maybe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_login_action(
    *,
    username: str,
    company_key: str = DEFAULT_COMPANY_KEY,
    language: str = DEFAULT_LANGUAGE,
) -> str:
    return (
        f"&action=authSource"
        f"&usr={quote(username, safe='')}"
        f"&company-key={quote(company_key, safe='')}"
        f"&source=1"
        f"&lang={quote(language, safe='')}"
    )


def build_login_url(
    *,
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
    company_key: str = DEFAULT_COMPANY_KEY,
    language: str = DEFAULT_LANGUAGE,
) -> str:
    action = _build_login_action(
        username=username,
        company_key=company_key,
        language=language,
    )
    salt = _salt_millis()
    sign = _sha1_lower(salt + _sha1_lower(password) + action)
    return f"{_normalize_base_url(base_url)}?sign={sign}&salt={salt}{action}"


def login_with_password(
    *,
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
    company_key: str = DEFAULT_COMPANY_KEY,
    language: str = DEFAULT_LANGUAGE,
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[ApiEnvelope, SessionCredentials]:
    envelope = _http_get_json(
        build_login_url(
            username=username,
            password=password,
            base_url=base_url,
            company_key=company_key,
            language=language,
        ),
        timeout=timeout,
    )
    if envelope.err != 0:
        raise SmartEssCloudError(f"login_failed:{envelope.err}:{envelope.desc}")
    if not isinstance(envelope.dat, dict):
        raise SmartEssCloudError("login_failed:missing_dat")

    session = SessionCredentials(
        token=str(envelope.dat.get("token") or "").strip(),
        secret=str(envelope.dat.get("secret") or "").strip(),
        uid=str(envelope.dat.get("uid") or "").strip(),
        usr=str(envelope.dat.get("usr") or "").strip(),
        role=_maybe_int(envelope.dat.get("role")),
        expire=_maybe_int(envelope.dat.get("expire")),
    )
    if not session.token or not session.secret:
        raise SmartEssCloudError("login_failed:missing_token_or_secret")
    return envelope, session


def _build_base_action(
    action: str,
    *,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
) -> str:
    if not action.startswith("&"):
        action = f"&{action}"
    return (
        f"{action}"
        f"&i18n={language}"
        f"&lang={language}"
        f"&source=1"
        f"&_app_client_=android"
        f"&_app_id_={app_id}"
        f"&_app_version_={app_version}"
    )


def build_signed_action_url(
    *,
    action: str,
    session: SessionCredentials,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
) -> str:
    base_action = _build_base_action(
        action,
        language=language,
        app_id=app_id,
        app_version=app_version,
    )
    salt = _salt_millis()
    sign = _sha1_lower(salt + session.secret + session.token + base_action)
    return (
        f"{_normalize_base_url(base_url)}?sign={sign}"
        f"&salt={salt}"
        f"&token={quote(session.token, safe='')}"
        f"{base_action}"
    )


def fetch_signed_action(
    *,
    action: str,
    session: SessionCredentials,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ApiEnvelope:
    envelope = _http_get_json(
        build_signed_action_url(
            action=action,
            session=session,
            base_url=base_url,
            language=language,
            app_id=app_id,
            app_version=app_version,
        ),
        timeout=timeout,
    )
    if envelope.err != 0:
        raise SmartEssCloudError(f"action_failed:{envelope.err}:{envelope.desc}")
    return envelope


def _build_action(action_name: str, parameters: list[tuple[str, Any]]) -> str:
    parts = [f"&action={quote(action_name, safe='')}" ]
    for key, value in parameters:
        if value in (None, ""):
            continue
        parts.append(f"&{quote(str(key), safe='')}={quote(str(value), safe='')}" )
    return "".join(parts)


def build_device_list_action(
    *,
    device_type: int = DEFAULT_DEVICE_TYPE,
    page: int = 0,
    pagesize: int = 10,
    search: str = "",
    pn: str = "",
    status: str = "",
    brand: str = "",
    order_by: str = "",
) -> str:
    parameters: list[tuple[str, Any]] = [("devtype", device_type)]
    if pn:
        parameters.append(("pn", pn))
    parameters.extend(
        [
            ("page", page),
            ("pagesize", pagesize),
            ("search", search),
            ("status", status),
            ("brand", brand),
            ("orderBy", order_by),
        ]
    )
    return _build_action("webQueryDeviceEs", parameters)


def build_device_detail_action(*, pn: str, sn: str, devcode: int, devaddr: int) -> str:
    return _build_action(
        "querySPDeviceLastData",
        [("pn", pn), ("devcode", devcode), ("devaddr", devaddr), ("sn", sn)],
    )


def build_device_settings_action(*, pn: str, sn: str, devcode: int, devaddr: int) -> str:
    return _build_action(
        "webQueryDeviceCtrlField",
        [("pn", pn), ("devcode", devcode), ("devaddr", devaddr), ("sn", sn)],
    )


def build_device_energy_flow_action(*, pn: str, sn: str, devcode: int, devaddr: int) -> str:
    return _build_action(
        "webQueryDeviceEnergyFlowEs",
        [("pn", pn), ("sn", sn), ("devaddr", devaddr), ("devcode", devcode)],
    )


def normalize_device_list(dat: Any) -> dict[str, Any] | None:
    if not isinstance(dat, dict):
        return None
    raw_devices = dat.get("device")
    if not isinstance(raw_devices, list):
        raw_devices = []

    devices: list[dict[str, Any]] = []
    for item in raw_devices:
        if not isinstance(item, dict):
            continue
        devices.append(
            {
                "pn": item.get("pn"),
                "sn": item.get("sn"),
                "devcode": item.get("devcode"),
                "devaddr": item.get("devaddr"),
                "devName": item.get("devName"),
                "devalias": item.get("devalias"),
                "status": item.get("status"),
                "brand": item.get("brand"),
                "usr": item.get("usr"),
                "uid": item.get("uid"),
                "pid": item.get("pid"),
                "devicePicture": item.get("devicePicture"),
            }
        )

    return {
        "page": dat.get("page"),
        "pagesize": dat.get("pagesize"),
        "total": dat.get("total"),
        "device_count": len(devices),
        "devices": devices,
    }


def normalize_device_detail(dat: Any) -> dict[str, Any] | None:
    if not isinstance(dat, dict):
        return None
    pars = dat.get("pars")
    if not isinstance(pars, dict):
        return None

    sections: dict[str, list[dict[str, Any]]] = {}
    section_counts: dict[str, int] = {}
    for key, value in pars.items():
        if not isinstance(value, list):
            continue
        items: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            items.append(
                {
                    "id": item.get("id"),
                    "par": item.get("par"),
                    "val": item.get("val"),
                    "unit": item.get("unit"),
                }
            )
        if not items:
            continue
        sections[key] = items
        section_counts[key] = len(items)

    return {
        "section_counts": section_counts,
        "sections": sections,
    }


def _mask(value: str, *, visible: int = 4) -> str:
    text = str(value or "")
    if not text:
        return ""
    if len(text) <= visible * 2:
        return "*" * len(text)
    return f"{text[:visible]}...{text[-visible:]}"


def session_preview(session: SessionCredentials) -> dict[str, Any]:
    preview = asdict(session)
    preview["token"] = _mask(session.token)
    preview["secret"] = _mask(session.secret)
    return preview


def _resolve_session(args: argparse.Namespace) -> tuple[str, SessionCredentials | None]:
    if getattr(args, "username", "") and getattr(args, "password", ""):
        _, session = login_with_password(
            username=args.username,
            password=args.password,
            base_url=args.base_url,
            company_key=args.company_key,
            language=args.language,
            timeout=args.timeout,
        )
        return "login", session

    token = str(getattr(args, "token", "") or "").strip()
    secret = str(getattr(args, "secret", "") or "").strip()
    if token and secret:
        return "provided", SessionCredentials(token=token, secret=secret)

    return "none", None


def _add_session_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--username", default="")
    parser.add_argument("--password", default="")
    parser.add_argument("--token", default="")
    parser.add_argument("--secret", default="")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--language", default=DEFAULT_LANGUAGE)
    parser.add_argument("--app-id", default=DEFAULT_APP_ID)
    parser.add_argument("--app-version", default=DEFAULT_APP_VERSION)
    parser.add_argument("--company-key", default=DEFAULT_COMPANY_KEY)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)


def _build_command_output(
    *,
    command: str,
    action: str | None,
    params: dict[str, Any],
    session_source: str,
    session: SessionCredentials | None,
    envelope: ApiEnvelope | None,
    normalized: Any,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "request": {
            "command": command,
            "action": action,
            "params": params,
            "session_source": session_source,
        },
        "session": session_preview(session) if session is not None else None,
    }
    if envelope is not None:
        response["response"] = {"err": envelope.err, "desc": envelope.desc}
        response["dat"] = envelope.dat
    if normalized is not None:
        response["normalized"] = normalized
    return response


def _build_response_block(
    *,
    action: str,
    envelope: ApiEnvelope,
    normalized: Any = None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "action": action,
        "response": {"err": envelope.err, "desc": envelope.desc},
        "dat": envelope.dat,
    }
    if normalized is not None:
        response["normalized"] = normalized
    return response


def _export_device_bundle_cloud_evidence(
    bundle_payload: dict[str, Any],
    args: argparse.Namespace,
) -> str:
    config_dir = str(getattr(args, "cloud_evidence_config_dir", "") or "").strip()
    if not config_dir:
        return ""
    evidence = build_smartess_device_bundle_cloud_evidence(
        bundle_payload,
        source="smartess_cloud_probe",
        entry_id=str(getattr(args, "cloud_evidence_entry_id", "") or "").strip(),
        collector_pn=str(getattr(args, "collector_pn", "") or "").strip(),
    )
    path = export_cloud_evidence(config_dir=Path(config_dir), evidence=evidence)
    return str(path)


def _run_login(args: argparse.Namespace) -> dict[str, Any]:
    if not args.username or not args.password:
        raise SmartEssCloudError("login_requires_username_and_password")

    envelope, session = login_with_password(
        username=args.username,
        password=args.password,
        base_url=args.base_url,
        company_key=args.company_key,
        language=args.language,
        timeout=args.timeout,
    )
    return _build_command_output(
        command="login",
        action="authSource",
        params={"username": args.username, "language": args.language},
        session_source="login",
        session=session,
        envelope=ApiEnvelope(err=envelope.err, desc=envelope.desc, dat=None, raw=envelope.raw),
        normalized=None,
    )


def _run_list_devices(args: argparse.Namespace) -> dict[str, Any]:
    session_source, session = _resolve_session(args)
    if session is None:
        raise SmartEssCloudError("list_devices_requires_session_or_login")

    action = build_device_list_action(
        device_type=args.device_type,
        page=args.page,
        pagesize=args.pagesize,
        search=args.search,
        pn=args.pn,
        status=args.status,
        brand=args.brand,
        order_by=args.order_by,
    )
    envelope = fetch_signed_action(
        action=action,
        session=session,
        base_url=args.base_url,
        language=args.language,
        app_id=args.app_id,
        app_version=args.app_version,
        timeout=args.timeout,
    )
    return _build_command_output(
        command="list-devices",
        action="webQueryDeviceEs",
        params={
            "device_type": args.device_type,
            "pn": args.pn,
            "search": args.search,
            "status": args.status,
            "brand": args.brand,
            "order_by": args.order_by,
            "page": args.page,
            "pagesize": args.pagesize,
        },
        session_source=session_source,
        session=session,
        envelope=envelope,
        normalized=normalize_device_list(envelope.dat),
    )


def _run_device_detail(args: argparse.Namespace) -> dict[str, Any]:
    session_source, session = _resolve_session(args)
    if session is None:
        raise SmartEssCloudError("device_detail_requires_session_or_login")

    action = build_device_detail_action(
        pn=args.pn,
        sn=args.sn,
        devcode=args.devcode,
        devaddr=args.devaddr,
    )
    envelope = fetch_signed_action(
        action=action,
        session=session,
        base_url=args.base_url,
        language=args.language,
        app_id=args.app_id,
        app_version=args.app_version,
        timeout=args.timeout,
    )
    return _build_command_output(
        command="device-detail",
        action="querySPDeviceLastData",
        params={
            "pn": args.pn,
            "sn": args.sn,
            "devcode": args.devcode,
            "devaddr": args.devaddr,
        },
        session_source=session_source,
        session=session,
        envelope=envelope,
        normalized=normalize_device_detail(envelope.dat),
    )


def _run_device_settings(args: argparse.Namespace) -> dict[str, Any]:
    session_source, session = _resolve_session(args)
    if session is None:
        raise SmartEssCloudError("device_settings_requires_session_or_login")

    action = build_device_settings_action(
        pn=args.pn,
        sn=args.sn,
        devcode=args.devcode,
        devaddr=args.devaddr,
    )
    envelope = fetch_signed_action(
        action=action,
        session=session,
        base_url=args.base_url,
        language=args.language,
        app_id=args.app_id,
        app_version=args.app_version,
        timeout=args.timeout,
    )
    return _build_command_output(
        command="device-settings",
        action="webQueryDeviceCtrlField",
        params={
            "pn": args.pn,
            "sn": args.sn,
            "devcode": args.devcode,
            "devaddr": args.devaddr,
        },
        session_source=session_source,
        session=session,
        envelope=envelope,
        normalized=normalize_device_settings(envelope.dat),
    )


def _run_energy_flow(args: argparse.Namespace) -> dict[str, Any]:
    session_source, session = _resolve_session(args)
    if session is None:
        raise SmartEssCloudError("energy_flow_requires_session_or_login")

    action = build_device_energy_flow_action(
        pn=args.pn,
        sn=args.sn,
        devcode=args.devcode,
        devaddr=args.devaddr,
    )
    envelope = fetch_signed_action(
        action=action,
        session=session,
        base_url=args.base_url,
        language=args.language,
        app_id=args.app_id,
        app_version=args.app_version,
        timeout=args.timeout,
    )
    return _build_command_output(
        command="energy-flow",
        action="webQueryDeviceEnergyFlowEs",
        params={
            "pn": args.pn,
            "sn": args.sn,
            "devcode": args.devcode,
            "devaddr": args.devaddr,
        },
        session_source=session_source,
        session=session,
        envelope=envelope,
        normalized=None,
    )


def _run_device_bundle(args: argparse.Namespace) -> dict[str, Any]:
    session_source, session = _resolve_session(args)
    if session is None:
        raise SmartEssCloudError("device_bundle_requires_session_or_login")

    list_action = build_device_list_action(
        device_type=args.device_type,
        page=args.page,
        pagesize=args.pagesize,
        search=args.search,
        pn=args.pn,
        status=args.status,
        brand=args.brand,
        order_by=args.order_by,
    )
    detail_action = build_device_detail_action(
        pn=args.pn,
        sn=args.sn,
        devcode=args.devcode,
        devaddr=args.devaddr,
    )
    settings_action = build_device_settings_action(
        pn=args.pn,
        sn=args.sn,
        devcode=args.devcode,
        devaddr=args.devaddr,
    )
    energy_flow_action = build_device_energy_flow_action(
        pn=args.pn,
        sn=args.sn,
        devcode=args.devcode,
        devaddr=args.devaddr,
    )

    list_envelope = fetch_signed_action(
        action=list_action,
        session=session,
        base_url=args.base_url,
        language=args.language,
        app_id=args.app_id,
        app_version=args.app_version,
        timeout=args.timeout,
    )
    detail_envelope = fetch_signed_action(
        action=detail_action,
        session=session,
        base_url=args.base_url,
        language=args.language,
        app_id=args.app_id,
        app_version=args.app_version,
        timeout=args.timeout,
    )
    settings_envelope = fetch_signed_action(
        action=settings_action,
        session=session,
        base_url=args.base_url,
        language=args.language,
        app_id=args.app_id,
        app_version=args.app_version,
        timeout=args.timeout,
    )
    energy_flow_envelope = fetch_signed_action(
        action=energy_flow_action,
        session=session,
        base_url=args.base_url,
        language=args.language,
        app_id=args.app_id,
        app_version=args.app_version,
        timeout=args.timeout,
    )

    normalized_list = normalize_device_list(list_envelope.dat)
    normalized_detail = normalize_device_detail(detail_envelope.dat)
    normalized_settings = normalize_device_settings(settings_envelope.dat)
    payload = {
        "request": {
            "command": "device-bundle",
            "actions": {
                "device_list": "webQueryDeviceEs",
                "device_detail": "querySPDeviceLastData",
                "device_settings": "webQueryDeviceCtrlField",
                "energy_flow": "webQueryDeviceEnergyFlowEs",
            },
            "params": {
                "device_type": args.device_type,
                "pn": args.pn,
                "sn": args.sn,
                "devcode": args.devcode,
                "devaddr": args.devaddr,
                "search": args.search,
                "status": args.status,
                "brand": args.brand,
                "order_by": args.order_by,
                "page": args.page,
                "pagesize": args.pagesize,
            },
            "session_source": session_source,
        },
        "session": session_preview(session),
        "responses": {
            "device_list": _build_response_block(
                action="webQueryDeviceEs",
                envelope=list_envelope,
                normalized=normalized_list,
            ),
            "device_detail": _build_response_block(
                action="querySPDeviceLastData",
                envelope=detail_envelope,
                normalized=normalized_detail,
            ),
            "device_settings": _build_response_block(
                action="webQueryDeviceCtrlField",
                envelope=settings_envelope,
                normalized=normalized_settings,
            ),
            "energy_flow": _build_response_block(
                action="webQueryDeviceEnergyFlowEs",
                envelope=energy_flow_envelope,
            ),
        },
        "normalized": {
            "device_list": normalized_list,
            "device_detail": normalized_detail,
            "device_settings": normalized_settings,
        },
    }
    cloud_evidence_path = _export_device_bundle_cloud_evidence(payload, args)
    if cloud_evidence_path:
        payload["cloud_evidence_path"] = cloud_evidence_path
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    login_parser = subparsers.add_parser("login", help="Authenticate and print a masked session summary.")
    _add_session_args(login_parser)
    login_parser.set_defaults(func=_run_login)

    list_parser = subparsers.add_parser("list-devices", help="Call webQueryDeviceEs.")
    _add_session_args(list_parser)
    list_parser.add_argument("--device-type", type=int, default=DEFAULT_DEVICE_TYPE)
    list_parser.add_argument("--pn", default="")
    list_parser.add_argument("--search", default="")
    list_parser.add_argument("--status", default="")
    list_parser.add_argument("--brand", default="")
    list_parser.add_argument("--order-by", default="")
    list_parser.add_argument("--page", type=int, default=0)
    list_parser.add_argument("--pagesize", type=int, default=10)
    list_parser.set_defaults(func=_run_list_devices)

    detail_parser = subparsers.add_parser("device-detail", help="Call querySPDeviceLastData.")
    _add_session_args(detail_parser)
    detail_parser.add_argument("--pn", required=True)
    detail_parser.add_argument("--sn", required=True)
    detail_parser.add_argument("--devcode", required=True, type=_parse_int)
    detail_parser.add_argument("--devaddr", required=True, type=_parse_int)
    detail_parser.set_defaults(func=_run_device_detail)

    settings_parser = subparsers.add_parser("device-settings", help="Call webQueryDeviceCtrlField.")
    _add_session_args(settings_parser)
    settings_parser.add_argument("--pn", required=True)
    settings_parser.add_argument("--sn", required=True)
    settings_parser.add_argument("--devcode", required=True, type=_parse_int)
    settings_parser.add_argument("--devaddr", required=True, type=_parse_int)
    settings_parser.set_defaults(func=_run_device_settings)

    flow_parser = subparsers.add_parser("energy-flow", help="Call webQueryDeviceEnergyFlowEs.")
    _add_session_args(flow_parser)
    flow_parser.add_argument("--pn", required=True)
    flow_parser.add_argument("--sn", required=True)
    flow_parser.add_argument("--devcode", required=True, type=_parse_int)
    flow_parser.add_argument("--devaddr", required=True, type=_parse_int)
    flow_parser.set_defaults(func=_run_energy_flow)

    bundle_parser = subparsers.add_parser(
        "device-bundle",
        help=(
            "Call webQueryDeviceEs, querySPDeviceLastData, "
            "webQueryDeviceCtrlField, and webQueryDeviceEnergyFlowEs."
        ),
    )
    _add_session_args(bundle_parser)
    bundle_parser.add_argument("--device-type", type=int, default=DEFAULT_DEVICE_TYPE)
    bundle_parser.add_argument("--pn", required=True)
    bundle_parser.add_argument("--sn", required=True)
    bundle_parser.add_argument("--devcode", required=True, type=_parse_int)
    bundle_parser.add_argument("--devaddr", required=True, type=_parse_int)
    bundle_parser.add_argument("--search", default="")
    bundle_parser.add_argument("--status", default="")
    bundle_parser.add_argument("--brand", default="")
    bundle_parser.add_argument("--order-by", default="")
    bundle_parser.add_argument("--page", type=int, default=0)
    bundle_parser.add_argument("--pagesize", type=int, default=50)
    bundle_parser.add_argument("--collector-pn", default="")
    bundle_parser.add_argument("--cloud-evidence-config-dir", default="")
    bundle_parser.add_argument("--cloud-evidence-entry-id", default="")
    bundle_parser.set_defaults(func=_run_device_bundle)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        payload = args.func(args)
    except SmartEssCloudError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())