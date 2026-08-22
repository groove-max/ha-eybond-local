"""Read-only DESSMonitor public-API client and normalized evidence models.

This module deliberately does not import :mod:`smartess_cloud`.  Both services
currently speak EyeBond's signed public protocol, but they are separate API
authorities and separate learning sources.  Sharing one provider's execution
code would make a hostname choice silently select parsing and trust policy.

Only read actions are exposed here.  In particular, ``ctrlDevice`` is absent.
The returned bundle is transient learning evidence; credentials and signed
session material are never included in it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import time
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .collector_identity import pn_is_same_identity


DEFAULT_BASE_URL = "https://web.dessmonitor.com/public/"
DEFAULT_LANGUAGE = "en_US"
DEFAULT_APP_ID = "com.demo.test"
DEFAULT_APP_VERSION = "3.6.2.1"
DEFAULT_COMPANY_KEY = "bnrl_frRFjEz8Mkn"
DEFAULT_TIMEOUT = 20.0
DEFAULT_MAX_CONTROL_VALUES = 16
DEFAULT_MAX_RESPONSE_BYTES = 2_000_000
DEFAULT_MAX_METADATA_FIELDS = 512
DEFAULT_MAX_TEXT_LENGTH = 512
_OPTIONAL_METADATA_ACTIONS = frozenset(
    {
        "querySPDeviceLastData",
        "queryDeviceChartField",
        "querySPKeyParameters",
        "queryDeviceCtrlField",
        "queryDeviceCtrlValue",
        "queryDeviceLastRawData",
    }
)
_ERROR_TOKEN_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-"
)


class DessMonitorCloudError(RuntimeError):
    """One safe, typed DESSMonitor request failure."""

    def __init__(self, reason_code: str, *, stage: str = "") -> None:
        _required_error_token(reason_code, "dessmonitor_error_reason_invalid")
        _optional_error_token(stage, "dessmonitor_error_stage_invalid")
        self.reason_code = reason_code
        self.stage = stage
        super().__init__(reason_code)


class DessMonitorActionRejectedError(DessMonitorCloudError):
    """A completed read action returned a definitive provider error."""

    def __init__(self, *, err: int, action: str, desc: str) -> None:
        self.err = err
        self.action = action
        self.desc = _text(desc)
        super().__init__(f"action_failed:{err}:{action}", stage=action)


@dataclass(frozen=True, slots=True)
class DessMonitorApiEnvelope:
    """One parsed DESSMonitor response envelope."""

    err: int
    desc: str = field(repr=False)
    dat: Any = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.err) is not int:
            raise TypeError("dessmonitor_envelope_err_invalid")
        if type(self.desc) is not str:
            raise TypeError("dessmonitor_envelope_desc_invalid")


@dataclass(frozen=True, slots=True)
class DessMonitorSession:
    """Signed-request material; deliberately hidden from repr."""

    token: str = field(repr=False)
    secret: str = field(repr=False)

    def __post_init__(self) -> None:
        _required_token(self.token, "dessmonitor_session_token_invalid")
        _required_token(self.secret, "dessmonitor_session_secret_invalid")


@dataclass(frozen=True, slots=True)
class DessMonitorDeviceIdentity:
    """One exact device behind one collector account record."""

    pn: str
    sn: str
    devcode: int
    devaddr: int

    def __post_init__(self) -> None:
        _required_token(self.pn, "dessmonitor_identity_pn_invalid")
        _required_token(self.sn, "dessmonitor_identity_sn_invalid")
        for value in (self.devcode, self.devaddr):
            if type(value) is not int:
                raise TypeError("dessmonitor_identity_integer_invalid")
            if value < 0:
                raise ValueError("dessmonitor_identity_integer_invalid")

    def to_record(self) -> dict[str, Any]:
        return {
            "pn": self.pn,
            "sn": self.sn,
            "devcode": self.devcode,
            "devaddr": self.devaddr,
        }


@dataclass(frozen=True, slots=True)
class DessMonitorTelemetryField:
    """One normalized labeled value or protocol metadata field."""

    field_id: str
    title: str
    value: str
    unit: str
    section: str
    source_action: str

    def __post_init__(self) -> None:
        _optional_normalized(self.field_id, "dessmonitor_field_id_invalid")
        _required_token(self.title, "dessmonitor_field_title_invalid")
        _optional_normalized(self.value, "dessmonitor_field_value_invalid")
        _optional_normalized(self.unit, "dessmonitor_field_unit_invalid")
        _optional_normalized(self.section, "dessmonitor_field_section_invalid")
        _required_token(self.source_action, "dessmonitor_field_source_invalid")

    def to_record(self) -> dict[str, str]:
        return {
            "field_id": self.field_id,
            "title": self.title,
            "value": self.value,
            "unit": self.unit,
            "section": self.section,
            "source_action": self.source_action,
        }


@dataclass(frozen=True, slots=True)
class DessMonitorControlField:
    """Read-only metadata for one provider-exposed control surface."""

    field_id: str
    title: str
    unit: str = ""
    hint: str = ""
    choices: tuple[tuple[str, str], ...] = ()
    current_value: str = ""

    def __post_init__(self) -> None:
        _required_token(self.field_id, "dessmonitor_control_id_invalid")
        _required_token(self.title, "dessmonitor_control_title_invalid")
        _optional_normalized(self.unit, "dessmonitor_control_unit_invalid")
        _optional_normalized(self.hint, "dessmonitor_control_hint_invalid")
        _optional_normalized(
            self.current_value, "dessmonitor_control_value_invalid"
        )
        if type(self.choices) is not tuple:
            raise TypeError("dessmonitor_control_choices_invalid")
        seen: set[str] = set()
        for choice in self.choices:
            if type(choice) is not tuple or len(choice) != 2:
                raise TypeError("dessmonitor_control_choice_invalid")
            value, label = choice
            _required_token(value, "dessmonitor_control_choice_invalid")
            _required_token(label, "dessmonitor_control_choice_invalid")
            if value in seen:
                raise ValueError("dessmonitor_control_choice_duplicate")
            seen.add(value)

    def to_record(self) -> dict[str, Any]:
        return {
            "field_id": self.field_id,
            "title": self.title,
            "unit": self.unit,
            "hint": self.hint,
            "choices": [
                {"value": value, "label": label}
                for value, label in self.choices
            ],
            "current_value": self.current_value,
        }


@dataclass(frozen=True, slots=True)
class DessMonitorEvidenceBundle:
    """Credential-free, normalized evidence from one read-only API pass."""

    identity: DessMonitorDeviceIdentity
    telemetry_fields: tuple[DessMonitorTelemetryField, ...]
    chart_fields: tuple[DessMonitorTelemetryField, ...]
    key_parameters: tuple[DessMonitorTelemetryField, ...]
    control_fields: tuple[DessMonitorControlField, ...]
    unavailable_actions: tuple[str, ...] = ()
    raw_packet_sha256: str = ""
    raw_packet_length: int = 0

    def __post_init__(self) -> None:
        if type(self.identity) is not DessMonitorDeviceIdentity:
            raise TypeError("dessmonitor_bundle_identity_invalid")
        _typed_tuple(
            self.telemetry_fields,
            DessMonitorTelemetryField,
            "dessmonitor_bundle_telemetry_invalid",
        )
        _typed_tuple(
            self.chart_fields,
            DessMonitorTelemetryField,
            "dessmonitor_bundle_chart_invalid",
        )
        _typed_tuple(
            self.key_parameters,
            DessMonitorTelemetryField,
            "dessmonitor_bundle_parameters_invalid",
        )
        _typed_tuple(
            self.control_fields,
            DessMonitorControlField,
            "dessmonitor_bundle_controls_invalid",
        )
        if type(self.unavailable_actions) is not tuple or any(
            type(action) is not str for action in self.unavailable_actions
        ):
            raise TypeError("dessmonitor_bundle_unavailable_actions_invalid")
        if (
            tuple(sorted(set(self.unavailable_actions)))
            != self.unavailable_actions
            or any(
                action not in _OPTIONAL_METADATA_ACTIONS
                for action in self.unavailable_actions
            )
        ):
            raise ValueError("dessmonitor_bundle_unavailable_actions_invalid")
        if type(self.raw_packet_sha256) is not str:
            raise TypeError("dessmonitor_bundle_raw_digest_invalid")
        if self.raw_packet_sha256 and (
            len(self.raw_packet_sha256) != 64
            or any(ch not in "0123456789abcdef" for ch in self.raw_packet_sha256)
        ):
            raise ValueError("dessmonitor_bundle_raw_digest_invalid")
        if type(self.raw_packet_length) is not int:
            raise TypeError("dessmonitor_bundle_raw_length_invalid")
        if self.raw_packet_length < 0:
            raise ValueError("dessmonitor_bundle_raw_length_invalid")
        if bool(self.raw_packet_sha256) != (self.raw_packet_length > 0):
            raise ValueError("dessmonitor_bundle_raw_evidence_incomplete")
        if self.metadata_field_count > DEFAULT_MAX_METADATA_FIELDS:
            raise ValueError("dessmonitor_bundle_metadata_limit_exceeded")

    @property
    def metadata_field_count(self) -> int:
        return (
            len(self.telemetry_fields)
            + len(self.chart_fields)
            + len(self.key_parameters)
            + len(self.control_fields)
        )

    def to_record(self) -> dict[str, Any]:
        """Return credential-free support evidence (never raw packet content)."""

        return {
            "source": "dessmonitor",
            "identity": self.identity.to_record(),
            "telemetry_fields": [item.to_record() for item in self.telemetry_fields],
            "chart_fields": [item.to_record() for item in self.chart_fields],
            "key_parameters": [item.to_record() for item in self.key_parameters],
            "control_fields": [item.to_record() for item in self.control_fields],
            "unavailable_actions": list(self.unavailable_actions),
            "raw_packet_sha256": self.raw_packet_sha256,
            "raw_packet_length": self.raw_packet_length,
            "metadata_field_count": self.metadata_field_count,
        }


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if (
        not value
        or value != value.strip()
        or len(value) > DEFAULT_MAX_TEXT_LENGTH
    ):
        raise ValueError(reason)
    return value


def _required_error_token(value: object, reason: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > DEFAULT_MAX_TEXT_LENGTH
        or any(char not in _ERROR_TOKEN_CHARS for char in value)
    ):
        raise ValueError(reason)
    return value


def _optional_error_token(value: object, reason: str) -> str:
    if (
        type(value) is not str
        or value != value.strip()
        or len(value) > DEFAULT_MAX_TEXT_LENGTH
        or any(char not in _ERROR_TOKEN_CHARS for char in value)
    ):
        raise ValueError(reason)
    return value


def _optional_normalized(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if value != value.strip() or len(value) > DEFAULT_MAX_TEXT_LENGTH:
        raise ValueError(reason)
    return value


def _provider_session_text(
    value: object,
    *,
    reason: str,
) -> str:
    """Validate required signed-request material without coercion or disclosure."""

    if (
        type(value) is not str
        or value != value.strip()
        or len(value) > DEFAULT_MAX_TEXT_LENGTH
        or not value
    ):
        raise DessMonitorCloudError(reason)
    return value


def _typed_tuple(value: object, item_type: type, reason: str) -> None:
    if type(value) is not tuple:
        raise TypeError(reason)
    if any(type(item) is not item_type for item in value):
        raise TypeError(reason)


def _sha1_lower(value: str | bytes) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha1(data).hexdigest()


def _normalized_base_url(base_url: object) -> str:
    if type(base_url) is not str or not base_url or base_url != base_url.strip():
        raise ValueError("dessmonitor_base_url_invalid")
    if not base_url.startswith("https://"):
        raise ValueError("dessmonitor_base_url_not_https")
    return base_url if base_url.endswith("/") else base_url + "/"


def _salt_millis() -> str:
    return str(int(time.time() * 1000))


def _strict_timeout(timeout: object) -> float:
    if type(timeout) not in {int, float} or isinstance(timeout, bool):
        raise TypeError("dessmonitor_timeout_invalid")
    if timeout <= 0:
        raise ValueError("dessmonitor_timeout_invalid")
    return float(timeout)


def _decode_response_body(body: bytes) -> str:
    text = body.decode("utf-8", errors="replace").strip()
    if text.startswith("null(") and text.endswith(")"):
        return text[text.find("(") + 1 : -1]
    return text


def _http_get_json(url: str, *, timeout: float) -> DessMonitorApiEnvelope:
    request = Request(
        url,
        headers={"User-Agent": "DessMonitor/3.6.2.1", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urlopen(request, timeout=_strict_timeout(timeout)) as response:
            body = response.read(DEFAULT_MAX_RESPONSE_BYTES + 1)
            if len(body) > DEFAULT_MAX_RESPONSE_BYTES:
                raise DessMonitorCloudError("response_too_large")
            payload = _decode_response_body(body)
    except HTTPError as exc:
        raise DessMonitorCloudError(f"http_error:{exc.code}") from exc
    except URLError as exc:
        raise DessMonitorCloudError("network_error") from exc
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DessMonitorCloudError("invalid_json") from exc
    if not isinstance(raw, dict):
        raise DessMonitorCloudError("invalid_envelope")
    raw_err = raw.get("err")
    if type(raw_err) is not int:
        raise DessMonitorCloudError("invalid_envelope_err")
    return DessMonitorApiEnvelope(
        err=raw_err,
        desc=_text(raw.get("desc")),
        dat=raw.get("dat"),
    )


def _login_action(*, username: str, company_key: str) -> str:
    _required_token(username, "dessmonitor_username_invalid")
    _required_token(company_key, "dessmonitor_company_key_invalid")
    return (
        "&action=authSource"
        f"&usr={quote(username, safe='')}"
        f"&company-key={quote(company_key, safe='')}"
        "&source=1"
    )


def build_login_url(
    *,
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
    company_key: str = DEFAULT_COMPANY_KEY,
) -> str:
    _required_token(password, "dessmonitor_password_invalid")
    action = _login_action(username=username, company_key=company_key)
    salt = _salt_millis()
    sign = _sha1_lower(salt + _sha1_lower(password) + action)
    return f"{_normalized_base_url(base_url)}?sign={sign}&salt={salt}{action}"


def login_with_password(
    *,
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
    company_key: str = DEFAULT_COMPANY_KEY,
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[DessMonitorApiEnvelope, DessMonitorSession]:
    stage = "authSource"
    try:
        envelope = _http_get_json(
            build_login_url(
                username=username,
                password=password,
                base_url=base_url,
                company_key=company_key,
            ),
            timeout=timeout,
        )
        if envelope.err != 0:
            raise DessMonitorCloudError(f"login_failed:{envelope.err}")
        if not isinstance(envelope.dat, dict):
            raise DessMonitorCloudError("login_failed:missing_dat")
        session = DessMonitorSession(
            token=_provider_session_text(
                envelope.dat.get("token"),
                reason="invalid_login_session_token",
            ),
            secret=_provider_session_text(
                envelope.dat.get("secret"),
                reason="invalid_login_session_secret",
            ),
        )
    except DessMonitorCloudError as exc:
        if exc.stage:
            raise
        raise DessMonitorCloudError(exc.reason_code, stage=stage) from exc
    return envelope, session


def _action(name: str, parameters: tuple[tuple[str, object], ...] = ()) -> str:
    _required_token(name, "dessmonitor_action_invalid")
    parts = [f"&action={quote(name, safe='')}"]
    for key, value in parameters:
        _required_token(key, "dessmonitor_action_parameter_invalid")
        if value in (None, ""):
            continue
        if type(value) not in {str, int} or isinstance(value, bool):
            raise TypeError("dessmonitor_action_value_invalid")
        parts.append(f"&{quote(key, safe='')}={quote(str(value), safe='')}")
    return "".join(parts)


def build_signed_action_url(
    *,
    action: str,
    session: DessMonitorSession,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
) -> str:
    if type(session) is not DessMonitorSession:
        raise TypeError("dessmonitor_session_invalid")
    _required_token(action, "dessmonitor_action_invalid")
    if not action.startswith("&action="):
        raise ValueError("dessmonitor_action_invalid")
    for value in (language, app_id, app_version):
        _required_token(value, "dessmonitor_client_metadata_invalid")
    suffix = (
        action
        + f"&i18n={quote(language, safe='')}"
        + f"&lang={quote(language, safe='')}"
        + "&source=1"
        + "&_app_client_=android"
        + f"&_app_id_={quote(app_id, safe='')}"
        + f"&_app_version_={quote(app_version, safe='')}"
    )
    salt = _salt_millis()
    sign = _sha1_lower(salt + session.secret + session.token + suffix)
    return (
        f"{_normalized_base_url(base_url)}?sign={sign}&salt={salt}"
        f"&token={quote(session.token, safe='')}{suffix}"
    )


def fetch_signed_action(
    *,
    action: str,
    session: DessMonitorSession,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    timeout: float = DEFAULT_TIMEOUT,
) -> DessMonitorApiEnvelope:
    stage = action.split("&", 2)[1].removeprefix("action=")
    try:
        envelope = _http_get_json(
            build_signed_action_url(
                action=action,
                session=session,
                base_url=base_url,
                language=language,
            ),
            timeout=timeout,
        )
    except DessMonitorCloudError as exc:
        if exc.stage:
            raise
        raise DessMonitorCloudError(exc.reason_code, stage=stage) from exc
    if envelope.err != 0:
        raise DessMonitorActionRejectedError(
            err=envelope.err, action=stage, desc=envelope.desc
        )
    return envelope


def _identity_action(collector_pn: str) -> str:
    _required_token(collector_pn, "dessmonitor_collector_pn_invalid")
    return _action(
        "webQueryDeviceEs",
        (("pn", collector_pn), ("devtype", 2304), ("page", 0), ("pagesize", 50)),
    )


def _device_action(name: str, identity: DessMonitorDeviceIdentity) -> str:
    if type(identity) is not DessMonitorDeviceIdentity:
        raise TypeError("dessmonitor_identity_invalid")
    return _action(
        name,
        (
            ("pn", identity.pn),
            ("sn", identity.sn),
            ("devcode", identity.devcode),
            ("devaddr", identity.devaddr),
        ),
    )


def _resolve_identity(dat: Any, collector_pn: str) -> DessMonitorDeviceIdentity:
    if not isinstance(dat, dict) or not isinstance(dat.get("device"), list):
        raise DessMonitorCloudError("device_identity_missing")
    candidates: list[DessMonitorDeviceIdentity] = []
    for item in dat["device"]:
        if not isinstance(item, dict):
            continue
        pn = item.get("pn")
        sn = item.get("sn")
        devcode = item.get("devcode")
        devaddr = item.get("devaddr")
        if type(pn) is not str or type(sn) is not str:
            continue
        if pn != pn.strip() or sn != sn.strip() or not pn or not sn:
            continue
        if type(devcode) is not int or type(devaddr) is not int:
            continue
        if not pn_is_same_identity(collector_pn, pn):
            continue
        candidates.append(
            DessMonitorDeviceIdentity(
                pn=pn, sn=sn, devcode=devcode, devaddr=devaddr
            )
        )
    if len(candidates) != 1:
        raise DessMonitorCloudError(f"device_identity_ambiguous:{len(candidates)}")
    return candidates[0]


def _text(value: Any) -> str:
    """Normalize one bounded scalar from an untrusted provider payload."""

    if value is None:
        return ""
    if type(value) not in {str, int, float, bool}:
        return ""
    return str(value).strip()[:DEFAULT_MAX_TEXT_LENGTH]


def _normalize_last_data(
    dat: Any, *, limit: int = DEFAULT_MAX_METADATA_FIELDS
) -> tuple[DessMonitorTelemetryField, ...]:
    if not isinstance(dat, dict) or not isinstance(dat.get("pars"), dict):
        return ()
    output: list[DessMonitorTelemetryField] = []
    for section, rows in dat["pars"].items():
        if len(output) >= limit:
            break
        if type(section) is not str or not isinstance(rows, list):
            continue
        for row in rows:
            if len(output) >= limit:
                break
            if not isinstance(row, dict):
                continue
            title = _text(row.get("par") or row.get("name"))
            if not title:
                continue
            output.append(
                DessMonitorTelemetryField(
                    field_id=_text(row.get("id")),
                    title=title,
                    value=_text(row.get("val")),
                    unit=_text(row.get("unit")),
                    section=section.strip(),
                    source_action="querySPDeviceLastData",
                )
            )
    return tuple(output)


def _normalize_chart_fields(
    dat: Any, *, limit: int = DEFAULT_MAX_METADATA_FIELDS
) -> tuple[DessMonitorTelemetryField, ...]:
    rows = dat if isinstance(dat, list) else []
    output: list[DessMonitorTelemetryField] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        field_id = _text(row.get("e0") or row.get("id"))
        title = _text(row.get("e1") or row.get("name"))
        if not field_id or not title:
            continue
        output.append(
            DessMonitorTelemetryField(
                field_id=field_id,
                title=title,
                value="",
                unit=_text(row.get("e3") or row.get("unit")),
                section="chart",
                source_action="queryDeviceChartField",
            )
        )
    return tuple(output)


def _normalize_key_parameters(
    dat: Any, *, limit: int = DEFAULT_MAX_METADATA_FIELDS
) -> tuple[DessMonitorTelemetryField, ...]:
    rows: list[Any] = []
    if isinstance(dat, list):
        rows = dat
    elif isinstance(dat, dict):
        for key in ("field", "pars", "parameters", "dat"):
            candidate = dat.get(key)
            if isinstance(candidate, list):
                rows = candidate
                break
    output: list[DessMonitorTelemetryField] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        field_id = _text(row.get("e0") or row.get("id") or row.get("field"))
        title = _text(row.get("e1") or row.get("par") or row.get("name"))
        if not title:
            continue
        output.append(
            DessMonitorTelemetryField(
                field_id=field_id,
                title=title,
                value="",
                unit=_text(row.get("e3") or row.get("unit")),
                section="key_parameter",
                source_action="querySPKeyParameters",
            )
        )
    return tuple(output)


def _normalize_control_fields(
    dat: Any, *, limit: int = DEFAULT_MAX_METADATA_FIELDS
) -> tuple[DessMonitorControlField, ...]:
    if not isinstance(dat, dict) or not isinstance(dat.get("field"), list):
        return ()
    output: list[DessMonitorControlField] = []
    for row in dat["field"][:limit]:
        if not isinstance(row, dict):
            continue
        field_id = _text(row.get("id"))
        title = _text(row.get("name") or row.get("field") or row.get("title"))
        if not field_id or not title:
            continue
        choices: list[tuple[str, str]] = []
        for choice in row.get("item") if isinstance(row.get("item"), list) else []:
            if not isinstance(choice, dict):
                continue
            value = _text(choice.get("key"))
            label = _text(choice.get("val"))
            if value and label and value not in {item[0] for item in choices}:
                choices.append((value, label))
        output.append(
            DessMonitorControlField(
                field_id=field_id,
                title=title,
                unit=_text(row.get("unit")),
                hint=_text(row.get("hint")),
                choices=tuple(choices),
            )
        )
    return tuple(output)


def _with_control_values(
    fields: tuple[DessMonitorControlField, ...],
    *,
    identity: DessMonitorDeviceIdentity,
    fetch: Callable[[str], DessMonitorApiEnvelope],
    max_values: int,
) -> tuple[tuple[DessMonitorControlField, ...], bool]:
    if type(max_values) is not int or isinstance(max_values, bool) or max_values < 0:
        raise ValueError("dessmonitor_max_control_values_invalid")
    output: list[DessMonitorControlField] = []
    unavailable = False
    for index, item in enumerate(fields):
        current = ""
        if index < max_values:
            try:
                envelope = fetch(
                    _device_action("queryDeviceCtrlValue", identity)
                    + f"&id={quote(item.field_id, safe='')}"
                )
            except DessMonitorCloudError:
                unavailable = True
            else:
                if isinstance(envelope.dat, dict):
                    current = _text(envelope.dat.get("val"))
        output.append(
            DessMonitorControlField(
                field_id=item.field_id,
                title=item.title,
                unit=item.unit,
                hint=item.hint,
                choices=item.choices,
                current_value=current,
            )
        )
    return tuple(output), unavailable


def _raw_digest(dat: Any) -> tuple[str, int]:
    if dat in (None, "", {}, []):
        return "", 0
    encoded = json.dumps(
        dat, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), len(encoded)


def fetch_read_only_evidence(
    *,
    username: str,
    password: str,
    collector_pn: str,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
    max_control_values: int = DEFAULT_MAX_CONTROL_VALUES,
) -> DessMonitorEvidenceBundle:
    """Fetch one bounded DESSMonitor metadata bundle, with zero write actions."""

    _required_token(collector_pn, "dessmonitor_collector_pn_invalid")
    _, session = login_with_password(
        username=username,
        password=password,
        base_url=base_url,
        timeout=timeout,
    )

    return fetch_read_only_evidence_for_session(
        session=session,
        collector_pn=collector_pn,
        base_url=base_url,
        timeout=timeout,
        max_control_values=max_control_values,
    )


def fetch_read_only_evidence_for_session(
    *,
    session: DessMonitorSession,
    collector_pn: str,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
    max_control_values: int = DEFAULT_MAX_CONTROL_VALUES,
    progress: Callable[[str], None] | None = None,
) -> DessMonitorEvidenceBundle:
    """Fetch metadata through one exact already-authenticated session."""

    if type(session) is not DessMonitorSession:
        raise TypeError("dessmonitor_session_invalid")
    _required_token(collector_pn, "dessmonitor_collector_pn_invalid")
    if progress is not None and not callable(progress):
        raise TypeError("dessmonitor_progress_invalid")

    def report(action: str) -> None:
        if progress is not None:
            progress(action)

    def fetch(action: str) -> DessMonitorApiEnvelope:
        return fetch_signed_action(
            action=action,
            session=session,
            base_url=base_url,
            timeout=timeout,
        )

    try:
        identity = _resolve_identity(
            fetch(_identity_action(collector_pn)).dat,
            collector_pn,
        )
    except DessMonitorCloudError as exc:
        if exc.stage:
            raise
        raise DessMonitorCloudError(
            exc.reason_code,
            stage="webQueryDeviceEs",
        ) from exc
    report("webQueryDeviceEs")
    unavailable: set[str] = set()

    def optional_fetch(action_name: str, action: str) -> DessMonitorApiEnvelope:
        try:
            return fetch(action)
        except DessMonitorCloudError:
            unavailable.add(action_name)
            return DessMonitorApiEnvelope(err=0, desc="", dat=None)
        finally:
            report(action_name)

    last_data = optional_fetch(
        "querySPDeviceLastData",
        _device_action("querySPDeviceLastData", identity),
    )
    chart_fields = optional_fetch(
        "queryDeviceChartField",
        _action("queryDeviceChartField", (("devcode", identity.devcode),)),
    )
    key_parameters = optional_fetch(
        "querySPKeyParameters",
        _action("querySPKeyParameters", (("devcode", identity.devcode),)),
    )
    controls = optional_fetch(
        "queryDeviceCtrlField",
        _device_action("queryDeviceCtrlField", identity),
    )
    raw_packet = optional_fetch(
        "queryDeviceLastRawData",
        _device_action("queryDeviceLastRawData", identity),
    )
    remaining = DEFAULT_MAX_METADATA_FIELDS
    telemetry_fields = _normalize_last_data(last_data.dat, limit=remaining)
    remaining -= len(telemetry_fields)
    normalized_chart_fields = _normalize_chart_fields(
        chart_fields.dat, limit=remaining
    )
    remaining -= len(normalized_chart_fields)
    normalized_key_parameters = _normalize_key_parameters(
        key_parameters.dat, limit=remaining
    )
    remaining -= len(normalized_key_parameters)
    normalized_controls, control_values_unavailable = _with_control_values(
        _normalize_control_fields(controls.dat, limit=remaining),
        identity=identity,
        fetch=fetch,
        max_values=max_control_values,
    )
    if control_values_unavailable:
        unavailable.add("queryDeviceCtrlValue")
    report("queryDeviceCtrlValue")
    digest, length = _raw_digest(raw_packet.dat)
    bundle = DessMonitorEvidenceBundle(
        identity=identity,
        telemetry_fields=telemetry_fields,
        chart_fields=normalized_chart_fields,
        key_parameters=normalized_key_parameters,
        control_fields=normalized_controls,
        unavailable_actions=tuple(sorted(unavailable)),
        raw_packet_sha256=digest,
        raw_packet_length=length,
    )
    if bundle.metadata_field_count == 0 and bundle.raw_packet_length == 0:
        raise DessMonitorCloudError(
            "device_metadata_missing",
            stage="metadata_bundle",
        )
    report("metadata_bundle")
    return bundle


__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_MAX_METADATA_FIELDS",
    "DEFAULT_MAX_CONTROL_VALUES",
    "DEFAULT_MAX_RESPONSE_BYTES",
    "DessMonitorActionRejectedError",
    "DessMonitorApiEnvelope",
    "DessMonitorCloudError",
    "DessMonitorControlField",
    "DessMonitorDeviceIdentity",
    "DessMonitorEvidenceBundle",
    "DessMonitorSession",
    "DessMonitorTelemetryField",
    "build_login_url",
    "build_signed_action_url",
    "fetch_read_only_evidence",
    "fetch_read_only_evidence_for_session",
    "fetch_signed_action",
    "login_with_password",
]
