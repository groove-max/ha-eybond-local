"""Reusable ValueCloud cloud client helpers for runtime support evidence."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "http://api.valueclouds.com/"
DEFAULT_LANGUAGE = "en_US"
DEFAULT_PROJECT = "IOT"
DEFAULT_APP_VERSION = "2.28.1.1"
DEFAULT_TIMEOUT = 15.0
LOGIN_PATH = "ppr/app/login/pub/login"
VALUECLOUD_BATCH_READ_BULK_PATH = "ppe/api/auth/web/batch/control/item/readBulkControl"
VALUECLOUD_BATCH_READ_ALL_PATH = "ppe/api/auth/web/batch/control/item/readAll"
VALUECLOUD_BATCH_SETUP_PATH = "ppe/api/auth/web/batch/control/item/setUp"
VALUECLOUD_CTRL_DEVICE_PATH = "ppe/api/auth/web/ctrlDevice"
VALUECLOUD_QUERY_CTRL_FIELD_KEY_PATH = "ppe/api/auth/web/queryCtrlFieldKey"


class ValueCloudError(RuntimeError):
    """Raised when one ValueCloud cloud request fails."""


@dataclass(frozen=True, slots=True)
class ValueCloudEnvelope:
    """One parsed ValueCloud cloud response envelope."""

    code: int | None
    message: str
    error_message: str
    success: bool | None
    data: Any
    raw: dict[str, Any]
    headers: dict[str, str]


@dataclass(frozen=True, slots=True)
class ValueCloudSession:
    """Session credentials returned by ValueCloud authentication."""

    token: str
    secret: str
    auth: str = ""
    user_id: str = ""
    account: str = ""
    source_endpoint: str = ""


def _normalize_base_url(base_url: str) -> str:
    normalized = str(base_url or DEFAULT_BASE_URL).strip() or DEFAULT_BASE_URL
    if not normalized.endswith("/"):
        normalized += "/"
    return normalized


def _normalize_path(path: str) -> str:
    normalized = str(path or "").strip()
    if not normalized:
        raise ValueCloudError("invalid_path")
    if normalized.startswith("http://") or normalized.startswith("https://"):
        return normalized
    return normalized.lstrip("/")


def _url_for_path(base_url: str, path: str, params: dict[str, Any] | None = None) -> str:
    normalized_path = _normalize_path(path)
    if normalized_path.startswith("http://") or normalized_path.startswith("https://"):
        url = normalized_path
    else:
        url = _normalize_base_url(base_url) + normalized_path
    query = urlencode(
        {
            key: value
            for key, value in (params or {}).items()
            if value not in (None, "")
        }
    )
    return f"{url}?{query}" if query else url


def _headers_for_path(
    path: str,
    *,
    session: ValueCloudSession | None = None,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": f"SmartValue/{app_version}(Android:13)",
        "i18n": language,
        "lang": language,
        "version": f"android;{app_version}",
        "project": DEFAULT_PROJECT,
    }
    if session is not None:
        request_path = "/" + _normalize_path(path).split("?", 1)[0].lstrip("/")
        headers["token"] = session.token
        headers["Auth"] = session.auth
        headers["sign"] = hmac.new(
            session.secret.encode("utf-8"),
            request_path.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
    else:
        headers["token"] = ""
        headers["Auth"] = ""
        headers["sign"] = ""
    return headers


def _decode_response_body(body: bytes) -> str:
    return body.decode("utf-8", errors="replace").strip()


def _http_json(
    *,
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
    session: ValueCloudSession | None = None,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ValueCloudEnvelope:
    url = _url_for_path(base_url, path, params)
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    request = Request(
        url,
        data=data,
        headers=_headers_for_path(
            path,
            session=session,
            language=language,
            app_version=app_version,
        ),
        method=str(method or "GET").upper(),
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = _decode_response_body(response.read())
            response_headers = dict(response.headers.items())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace").strip()
        raise ValueCloudError(f"http_error:{exc.code}:{detail}") from exc
    except URLError as exc:
        raise ValueCloudError(f"network_error:{exc.reason}") from exc

    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueCloudError(f"invalid_json:{payload[:200]}") from exc
    if not isinstance(raw, dict):
        raise ValueCloudError("invalid_envelope:not_an_object")

    return ValueCloudEnvelope(
        code=_maybe_int(raw.get("code")),
        message=str(raw.get("message") or ""),
        error_message=str(raw.get("errorMessage") or ""),
        success=raw.get("success") if isinstance(raw.get("success"), bool) else None,
        data=raw.get("data"),
        raw=raw,
        headers=response_headers,
    )


def _envelope_ok(envelope: ValueCloudEnvelope) -> bool:
    if envelope.success is True:
        return True
    return envelope.code in {0, 200}


def _raise_for_envelope(envelope: ValueCloudEnvelope, *, action: str) -> None:
    if _envelope_ok(envelope):
        return
    code = envelope.code if envelope.code is not None else "unknown"
    detail = envelope.error_message or envelope.message or "unknown"
    raise ValueCloudError(f"action_failed:{code}:{action}:{detail}")


def _maybe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _sha1_lower(value: str | bytes) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha1(data).hexdigest()


def login_with_password(
    *,
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[ValueCloudEnvelope, ValueCloudSession]:
    """Authenticate against ValueCloud and return a signed-request session."""

    account = _clean(username)
    if not account or not password:
        raise ValueCloudError("login_failed:missing_credentials")

    body = {
        "account": account,
        # SmartValue sends Shutil.encryptToSHA(password), i.e. lowercase SHA-1
        # hex of the user-entered password, to the ValueCloud login endpoint.
        "password": _sha1_lower(password),
        "project": DEFAULT_PROJECT,
    }
    envelope = _http_json(
        method="POST",
        path=LOGIN_PATH,
        body=body,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )
    try:
        _raise_for_envelope(envelope, action=LOGIN_PATH)
    except ValueCloudError as exc:
        raise ValueCloudError(f"login_failed:{exc}") from exc
    session = _session_from_login(envelope, account=account, source_endpoint=LOGIN_PATH)
    return envelope, session


def _session_from_login(
    envelope: ValueCloudEnvelope,
    *,
    account: str,
    source_endpoint: str,
) -> ValueCloudSession:
    if not isinstance(envelope.data, dict):
        raise ValueCloudError("login_failed:missing_data")
    token = _clean(envelope.data.get("token"))
    secret = _clean(envelope.data.get("secret"))
    auth = _clean(envelope.data.get("auth")) or _clean(envelope.headers.get("Auth"))
    user_id = _clean(envelope.data.get("userId")) or _clean(envelope.data.get("uid"))
    if not token or not secret:
        raise ValueCloudError("login_failed:missing_token_or_secret")
    return ValueCloudSession(
        token=token,
        secret=secret,
        auth=auth,
        user_id=user_id,
        account=account,
        source_endpoint=source_endpoint,
    )


def session_preview(session: ValueCloudSession) -> dict[str, Any]:
    """Return a privacy-safe ValueCloud session preview for persisted evidence."""

    return {
        "token_prefix": session.token[:6],
        "secret_sha256": hashlib.sha256(session.secret.encode("utf-8")).hexdigest(),
        "auth_prefix": session.auth[:6],
        "user_id": session.user_id,
        "account_hash": hashlib.sha256(session.account.encode("utf-8")).hexdigest()
        if session.account
        else "",
        "source_endpoint": session.source_endpoint,
    }


def fetch_authenticated_envelope(
    *,
    action: str,
    path: str,
    session: ValueCloudSession,
    params: dict[str, Any] | None = None,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ValueCloudEnvelope:
    """Fetch one authenticated ValueCloud GET endpoint and validate the envelope."""

    envelope = _http_json(
        method="GET",
        path=path,
        params=params,
        session=session,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )
    _raise_for_envelope(envelope, action=action)
    return envelope


def post_authenticated_envelope(
    *,
    action: str,
    path: str,
    session: ValueCloudSession,
    body: dict[str, Any] | None = None,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ValueCloudEnvelope:
    """Post one authenticated ValueCloud endpoint and validate the envelope."""

    envelope = _http_json(
        method="POST",
        path=path,
        body=body or {},
        session=session,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )
    _raise_for_envelope(envelope, action=action)
    return envelope


def fetch_batch_control_groups(
    *,
    session: ValueCloudSession,
    pn: str,
    sn: str,
    devcode: int,
    devaddr: int,
    role: int | str = 0,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ValueCloudEnvelope:
    """Fetch ValueCloud batch-control metadata groups for one device."""

    return fetch_authenticated_envelope(
        action="readBulkControl",
        path=VALUECLOUD_BATCH_READ_BULK_PATH,
        params={
            "pn": pn,
            "sn": sn,
            "devcode": devcode,
            "devaddr": devaddr,
            "role": role,
        },
        session=session,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )


def read_batch_control_values(
    *,
    session: ValueCloudSession,
    pn: str,
    sn: str,
    devcode: int,
    devaddr: int,
    control_item_id: Any,
    ids: list[dict[str, Any]],
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ValueCloudEnvelope:
    """Read current values for one ValueCloud batch-control group."""

    return post_authenticated_envelope(
        action="readAll",
        path=VALUECLOUD_BATCH_READ_ALL_PATH,
        body={
            "pn": pn,
            "sn": sn,
            "devcode": devcode,
            "devaddr": devaddr,
            "controlItemId": control_item_id,
            "ids": [
                {
                    "id": item.get("id"),
                    "detailsId": item.get("detailsId"),
                    "order": item.get("order"),
                }
                for item in ids
            ],
        },
        session=session,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )


def setup_batch_control_value(
    *,
    session: ValueCloudSession,
    pn: str,
    sn: str,
    devcode: int,
    devaddr: int,
    control_item_id: Any,
    control_id: Any,
    details_id: Any,
    order: Any,
    value: Any,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ValueCloudEnvelope:
    """Send one ValueCloud batch-control setUp item.

    The official UI posts one group body with an ``ids`` list.  The learning
    runner deliberately sends a single item per request so every cloud write can
    be correlated with exactly one local shadow observation.
    """

    return post_authenticated_envelope(
        action="setUp",
        path=VALUECLOUD_BATCH_SETUP_PATH,
        body={
            "pn": pn,
            "sn": sn,
            "devcode": devcode,
            "devaddr": devaddr,
            "controlItemId": control_item_id,
            "ids": [
                {
                    "id": control_id,
                    "detailsId": details_id,
                    "order": order,
                    "val": value,
                }
            ],
        },
        session=session,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )


def ctrl_device_value(
    *,
    session: ValueCloudSession,
    pn: str,
    sn: str,
    devcode: int,
    devaddr: int,
    control_id: Any,
    value: Any,
    datatype: Any,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ValueCloudEnvelope:
    """Send one legacy ValueCloud ctrlDevice control request."""

    return fetch_authenticated_envelope(
        action="ctrlDevice",
        path=VALUECLOUD_CTRL_DEVICE_PATH,
        params={
            "pn": pn,
            "sn": sn,
            "devcode": devcode,
            "devaddr": devaddr,
            "id": control_id,
            "val": value,
            "datatype": datatype,
        },
        session=session,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )


def query_ctrl_field_key(
    *,
    session: ValueCloudSession,
    pn: str,
    sn: str,
    devcode: int,
    devaddr: int,
    control_id: Any,
    datatype: Any,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> ValueCloudEnvelope:
    """Read one legacy ValueCloud control field value."""

    return fetch_authenticated_envelope(
        action="queryCtrlFieldKey",
        path=VALUECLOUD_QUERY_CTRL_FIELD_KEY_PATH,
        params={
            "pn": pn,
            "sn": sn,
            "devcode": devcode,
            "devaddr": devaddr,
            "id": control_id,
            "datatype": datatype,
        },
        session=session,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )


def fetch_device_bundle_for_collector(
    *,
    username: str,
    password: str,
    collector_pn: str,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Fetch one ValueCloud device evidence bundle by collector PN."""

    _, session = login_with_password(
        username=username,
        password=password,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )
    return fetch_device_bundle_for_collector_with_session(
        session=session,
        collector_pn=collector_pn,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )


def fetch_device_bundle_for_collector_with_session(
    *,
    session: ValueCloudSession,
    collector_pn: str,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Fetch one ValueCloud device evidence bundle using an existing session."""

    normalized_collector_pn = _clean(collector_pn)
    if not normalized_collector_pn:
        raise ValueCloudError("valuecloud_collector_pn_not_available")

    list_params = {
        "page": 1,
        "pageSize": 50,
        "search": normalized_collector_pn,
    }
    list_envelope = fetch_authenticated_envelope(
        action="listTDeviceInfo",
        path="dev/api/auth/app/dev/listTDeviceInfo",
        params=list_params,
        session=session,
        base_url=base_url,
        language=language,
        app_version=app_version,
        timeout=timeout,
    )
    normalized_list = normalize_device_list(list_envelope.data) or {
        "device_count": 0,
        "devices": [],
    }
    if not normalized_list.get("devices"):
        list_params = {"page": 1, "pageSize": 50}
        list_envelope = fetch_authenticated_envelope(
            action="listTDeviceInfo",
            path="dev/api/auth/app/dev/listTDeviceInfo",
            params=list_params,
            session=session,
            base_url=base_url,
            language=language,
            app_version=app_version,
            timeout=timeout,
        )
        normalized_list = normalize_device_list(list_envelope.data) or {
            "device_count": 0,
            "devices": [],
        }

    device = _resolve_bundle_device_identity(
        normalized_list,
        collector_pn=normalized_collector_pn,
    )
    identity_params = {
        "pn": device["pn"],
        "sn": device["sn"],
        "devcode": device["devcode"],
        "devaddr": device["devaddr"],
    }
    pars_params = {
        "uid": device.get("uid") or session.user_id,
        **identity_params,
    }
    control_params = {
        **identity_params,
        "role": device.get("role") or 0,
    }

    responses: dict[str, Any] = {
        "device_list": _build_response_block(
            action="listTDeviceInfo",
            path="dev/api/auth/app/dev/listTDeviceInfo",
            envelope=list_envelope,
            normalized=normalized_list,
        )
    }
    normalized: dict[str, Any] = {
        "device_list": normalized_list,
        "device_identity": device,
    }

    optional_specs = (
        (
            "device_detail",
            "querySPDeviceLastData",
            "ppe/api/auth/app/querySPDeviceLastData",
            identity_params,
            normalize_device_detail,
        ),
        (
            "device_pars",
            "queryDevicePars",
            "ppe/api/auth/web/queryDevicePars",
            pars_params,
            normalize_valuecloud_fields,
        ),
        (
            "control_strategy",
            "queryDeviceCtrlStrategy",
            "ppe/api/auth/web/queryDeviceCtrlStrategy",
            control_params,
            normalize_valuecloud_fields,
        ),
        (
            "device_ctrl",
            "queryDeviceCtrl",
            "ppe/api/auth/web/queryDeviceCtrl",
            identity_params,
            normalize_valuecloud_fields,
        ),
        (
            "batch_control",
            "readBulkControl",
            VALUECLOUD_BATCH_READ_BULK_PATH,
            control_params,
            normalize_batch_control_groups,
        ),
    )
    for key, action, path, params, normalizer in optional_specs:
        response = _fetch_optional_response_block(
            action=action,
            path=path,
            params=params,
            normalizer=normalizer,
            session=session,
            base_url=base_url,
            language=language,
            app_version=app_version,
            timeout=timeout,
        )
        responses[key] = response
        if response.get("status") == "ok":
            normalized[key] = response.get("normalized")

    return {
        "request": {
            "command": "valuecloud-device-bundle",
            "provider": "valuecloud",
            "actions": {
                "device_list": "listTDeviceInfo",
                "device_detail": "querySPDeviceLastData",
                "device_pars": "queryDevicePars",
                "control_strategy": "queryDeviceCtrlStrategy",
                "device_ctrl": "queryDeviceCtrl",
                "batch_control": "readBulkControl",
            },
            "params": {
                "collector_pn": normalized_collector_pn,
                **identity_params,
                "page": list_params.get("page"),
                "pageSize": list_params.get("pageSize"),
                "search": list_params.get("search", ""),
            },
            "session_source": "login",
        },
        "session": session_preview(session),
        "responses": responses,
        "normalized": normalized,
    }


def _fetch_optional_response_block(
    *,
    action: str,
    path: str,
    params: dict[str, Any],
    normalizer,
    session: ValueCloudSession,
    base_url: str,
    language: str,
    app_version: str,
    timeout: float,
) -> dict[str, Any]:
    try:
        envelope = fetch_authenticated_envelope(
            action=action,
            path=path,
            params=params,
            session=session,
            base_url=base_url,
            language=language,
            app_version=app_version,
            timeout=timeout,
        )
    except Exception as exc:
        return {
            "action": action,
            "path": path,
            "status": "error",
            "error": str(exc),
        }
    normalized = normalizer(envelope.data)
    return _build_response_block(
        action=action,
        path=path,
        envelope=envelope,
        normalized=normalized,
    )


def _build_response_block(
    *,
    action: str,
    path: str,
    envelope: ValueCloudEnvelope,
    normalized: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = {
        "action": action,
        "path": path,
        "status": "ok",
        "code": envelope.code,
        "message": envelope.message,
        "errorMessage": envelope.error_message,
        "success": envelope.success,
        "data": envelope.data,
    }
    if normalized is not None:
        response["normalized"] = normalized
    return response


def normalize_device_list(data: Any) -> dict[str, Any] | None:
    """Normalize ValueCloud device-list data into stable evidence fields."""

    if not isinstance(data, dict):
        return None
    raw_devices = data.get("items")
    if not isinstance(raw_devices, list):
        raw_devices = data.get("device")
    if not isinstance(raw_devices, list):
        raw_devices = []

    devices: list[dict[str, Any]] = []
    for item in raw_devices:
        if not isinstance(item, dict):
            continue
        pn = _clean(item.get("pn"))
        sn = (
            _clean(item.get("sn"))
            or _clean(item.get("deviceSn"))
            or _clean(item.get("deviceSN"))
        )
        device = {
            "id": item.get("id"),
            "pn": pn,
            "sn": sn or pn,
            "sn_inferred_from_pn": not sn and bool(pn),
            "devcode": _maybe_int(item.get("devcode")),
            "devaddr": _maybe_int(item.get("devaddr")),
            "uid": _clean(item.get("uid"))
            or _clean(item.get("userId"))
            or _clean(item.get("deviceUserId")),
            "role": _maybe_int(item.get("role")),
            "deviceName": item.get("deviceName") or item.get("devName"),
            "devalias": item.get("devalias"),
            "deviceStatus": item.get("deviceStatus"),
            "deviceOnlineStatus": item.get("deviceOnlineStatus"),
            "devTypeId": item.get("devTypeId"),
            "productName": item.get("productName"),
            "brandName": item.get("brandName") or item.get("brand"),
        }
        devices.append(device)

    return {
        "page": data.get("page"),
        "pageSize": data.get("pageSize") or data.get("pagesize"),
        "total": data.get("total"),
        "device_count": len(devices),
        "devices": devices,
    }


def _resolve_bundle_device_identity(
    normalized_list: dict[str, Any],
    *,
    collector_pn: str,
) -> dict[str, Any]:
    devices = list(normalized_list.get("devices") or [])
    if not devices:
        raise ValueCloudError("collector_device_identity_not_found")

    normalized_collector_pn = _clean(collector_pn)
    matches = [
        device
        for device in devices
        if _pn_matches(normalized_collector_pn, _clean(device.get("pn")))
        or _pn_matches(normalized_collector_pn, _clean(device.get("sn")))
    ]
    candidates = matches or devices
    if len(candidates) != 1:
        raise ValueCloudError(f"collector_device_identity_ambiguous:{len(candidates)}")

    device = candidates[0]
    pn = _clean(device.get("pn"))
    sn = _clean(device.get("sn")) or pn
    devcode = _maybe_int(device.get("devcode"))
    devaddr = _maybe_int(device.get("devaddr"))
    if not pn or not sn or devcode is None or devaddr is None:
        raise ValueCloudError("collector_device_identity_incomplete")
    return {
        **device,
        "pn": pn,
        "sn": sn,
        "devcode": devcode,
        "devaddr": devaddr,
    }


def _pn_matches(requested: str, candidate: str) -> bool:
    return bool(
        requested
        and candidate
        and (
            requested == candidate
            or requested.startswith(candidate)
            or candidate.startswith(requested)
        )
    )


def normalize_device_detail(data: Any) -> dict[str, Any] | None:
    """Normalize ValueCloud querySPDeviceLastData payload sections."""

    if not isinstance(data, dict):
        return None
    pars = data.get("pars")
    if not isinstance(pars, dict):
        pars = {}

    sections: dict[str, list[dict[str, Any]]] = {}
    section_counts: dict[str, int] = {}
    for section, raw_items in pars.items():
        if not isinstance(raw_items, list):
            continue
        items: list[dict[str, Any]] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                continue
            items.append(_normalize_cloud_field(raw_item))
        sections[str(section)] = items
        section_counts[str(section)] = len(items)
    return {
        "section_counts": section_counts,
        "sections": sections,
    }


def normalize_valuecloud_fields(data: Any) -> dict[str, Any] | None:
    """Normalize ValueCloud field/control metadata lists."""

    if isinstance(data, dict):
        raw_fields = data.get("field") or data.get("items") or data.get("list") or data.get("data")
    else:
        raw_fields = data
    if not isinstance(raw_fields, list):
        return None

    fields: list[dict[str, Any]] = []
    current_values = 0
    choice_count = 0
    for raw_item in raw_fields:
        if not isinstance(raw_item, dict):
            continue
        field = _normalize_cloud_field(raw_item)
        fields.append(field)
        if field.get("val") not in (None, "") or field.get("displayValue") not in (None, ""):
            current_values += 1
        if field.get("enumMap") or field.get("item"):
            choice_count += 1

    return {
        "field_count": len(fields),
        "current_values_included": current_values > 0,
        "current_value_count": current_values,
        "choice_field_count": choice_count,
        "fields": fields,
    }


def _normalize_cloud_field(raw_item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": raw_item.get("id"),
        "detailsId": raw_item.get("detailsId"),
        "controlItemId": raw_item.get("controlItemId"),
        "order": raw_item.get("order"),
        "num": raw_item.get("num"),
        "groupNumber": raw_item.get("groupNumber"),
        "name": raw_item.get("name") or raw_item.get("title"),
        "par": raw_item.get("par"),
        "unit": raw_item.get("unit"),
        "val": raw_item.get("val"),
        "displayValue": raw_item.get("displayValue"),
        "hint": raw_item.get("hint"),
        "item": raw_item.get("item"),
        "datatype": raw_item.get("datatype"),
        "dateType": raw_item.get("dateType"),
        "readwrite": raw_item.get("readwrite"),
        "type": raw_item.get("type"),
        "rangeConfigFlag": raw_item.get("rangeConfigFlag"),
        "visable": raw_item.get("visable"),
        "visableDist": raw_item.get("visableDist"),
        "viewable": raw_item.get("viewable"),
        "packet": raw_item.get("packet"),
        "packetname": raw_item.get("packetname"),
        "tag": raw_item.get("tag"),
        "minimum": raw_item.get("minimum"),
        "maximum": raw_item.get("maximum"),
        "enumMap": raw_item.get("enumMap"),
    }


def normalize_batch_control_groups(data: Any) -> dict[str, Any] | None:
    """Normalize ValueCloud readBulkControl metadata into stable groups."""

    raw_groups: Any = data
    if isinstance(data, dict):
        raw_groups = (
            data.get("items")
            or data.get("list")
            or data.get("data")
            or data.get("controlItems")
            or data.get("controlItem")
        )
    if not isinstance(raw_groups, list):
        return None

    groups: list[dict[str, Any]] = []
    parameter_count = 0
    writable_count = 0
    choice_count = 0
    for raw_group in raw_groups:
        if not isinstance(raw_group, dict):
            continue
        raw_parameters = (
            raw_group.get("parameters")
            or raw_group.get("parameter")
            or raw_group.get("items")
            or raw_group.get("field")
            or []
        )
        if not isinstance(raw_parameters, list):
            raw_parameters = []
        parameters: list[dict[str, Any]] = []
        for raw_parameter in raw_parameters:
            if not isinstance(raw_parameter, dict):
                continue
            field = _normalize_cloud_field(raw_parameter)
            if field.get("controlItemId") in (None, ""):
                field["controlItemId"] = raw_group.get("controlItemId") or raw_group.get("id")
            parameters.append(field)
            parameter_count += 1
            if _field_is_writable(field):
                writable_count += 1
            if field.get("item") or field.get("enumMap"):
                choice_count += 1
        groups.append(
            {
                "id": raw_group.get("id"),
                "controlItemId": raw_group.get("controlItemId") or raw_group.get("id"),
                "name": raw_group.get("name") or raw_group.get("title"),
                "date": raw_group.get("date"),
                "parameters": parameters,
                "parameter_count": len(parameters),
            }
        )

    return {
        "group_count": len(groups),
        "parameter_count": parameter_count,
        "writable_parameter_count": writable_count,
        "choice_parameter_count": choice_count,
        "groups": groups,
    }


def _field_is_writable(field: dict[str, Any]) -> bool:
    readwrite = str(field.get("readwrite") or "").strip().upper()
    if readwrite in {"RW", "R/W", "WRITE", "WRITABLE"}:
        return True
    if readwrite in {"R", "RO", "READ", "READONLY", "READ_ONLY"}:
        return False
    value_type = str(field.get("type") or field.get("datatype") or "").strip().lower()
    return "write" in value_type or "ctrl" in value_type or bool(field.get("detailsId"))
