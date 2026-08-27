"""Typed collector-identity probes for one exact TCP session.

This module owns the wire shape of identity challenges, but no socket selection,
callback routing, ownership, or retry policy.  A caller must already hold the
authority to write to one exact session.  No probe changes the collector's
endpoint, UART settings, or durable configuration; FC=1 carries the protocol's
ordinary UTC/heartbeat-interval handshake fields.

The framed probes are deliberately distinct:

* ``framed_fc2`` upgrades a socket that has already volunteered a framed
  heartbeat by reading collector parameter 2;
* ``framed_fc1`` bootstraps a completely silent framed collector with the same
  FC=1 request used by the original EyeBond server protocol;
* ``at_dtupn`` reads the PN from a known or candidate AT-text session.

Only a response correlated to the request's transaction/function code becomes
the strong ``fc1_identity_challenge`` evidence.  An unsolicited FC=1 heartbeat
continues to be weak ``framed_heartbeat`` evidence elsewhere in the transport.
"""

from __future__ import annotations

from dataclasses import dataclass
import re

from ..collector_identity import validated_collector_pn
from .at import build_at_query
from .protocol import (
    FC_HEARTBEAT,
    FC_QUERY_COLLECTOR,
    HEADER_SIZE,
    build_collector_request,
    build_heartbeat_request,
    decode_header,
)

PROBE_FRAMED_FC1 = "framed_fc1"
PROBE_FRAMED_FC2 = "framed_fc2"
PROBE_AT_DTUPN = "at_dtupn"

_PROBE_KINDS = frozenset(
    {PROBE_FRAMED_FC1, PROBE_FRAMED_FC2, PROBE_AT_DTUPN}
)
_AT_DTUPN_RE = re.compile(
    rb"AT\+DTUPN\s*[:=]\s*([A-Za-z0-9][A-Za-z0-9._-]{5,63})(?![A-Za-z0-9._-])"
)
_IDENTITY_TID = 1
_IDENTITY_HEARTBEAT_INTERVAL_SECONDS = 60


@dataclass(frozen=True, slots=True)
class IdentityProbeRequest:
    """One typed identity request and its expected response dialect."""

    session_protocol: str
    probe_kind: str
    payload: bytes
    transaction_id: int = 0
    function_code: int = 0

    def __post_init__(self) -> None:
        if type(self.session_protocol) is not str or self.session_protocol not in {
            "eybond_framed",
            "at_text",
        }:
            raise ValueError("identity_probe_session_protocol_invalid")
        if type(self.probe_kind) is not str or self.probe_kind not in _PROBE_KINDS:
            raise ValueError("identity_probe_kind_invalid")
        if type(self.payload) is not bytes or not self.payload:
            raise ValueError("identity_probe_payload_invalid")
        if type(self.transaction_id) is not int or self.transaction_id < 0:
            raise ValueError("identity_probe_transaction_id_invalid")
        if type(self.function_code) is not int or self.function_code < 0:
            raise ValueError("identity_probe_function_code_invalid")


def default_probe_kind_for_protocol(session_protocol: object) -> str:
    """Return the established known-wire identity query, or ``""``."""

    if session_protocol == "eybond_framed":
        return PROBE_FRAMED_FC2
    if session_protocol == "at_text":
        return PROBE_AT_DTUPN
    return ""


def silent_probe_kind_for_protocol(session_protocol: object) -> str:
    """Return the first-contact query for a fully silent candidate wire."""

    if session_protocol == "eybond_framed":
        return PROBE_FRAMED_FC1
    if session_protocol == "at_text":
        return PROBE_AT_DTUPN
    return ""


def build_identity_probe_request(
    session_protocol: object,
    *,
    probe_kind: object = "",
) -> IdentityProbeRequest | None:
    """Build one strict probe request without coercing protocol authority."""

    if type(session_protocol) is not str:
        return None
    protocol = session_protocol
    if protocol not in {"eybond_framed", "at_text"}:
        return None
    if type(probe_kind) is str and probe_kind == "":
        kind = default_probe_kind_for_protocol(protocol)
    elif type(probe_kind) is str and probe_kind in _PROBE_KINDS:
        kind = probe_kind
    else:
        return None

    if kind == PROBE_AT_DTUPN and protocol == "at_text":
        return IdentityProbeRequest(
            session_protocol=protocol,
            probe_kind=kind,
            payload=build_at_query("DTUPN"),
        )
    if kind == PROBE_FRAMED_FC2 and protocol == "eybond_framed":
        return IdentityProbeRequest(
            session_protocol=protocol,
            probe_kind=kind,
            payload=build_collector_request(
                _IDENTITY_TID,
                b"\x02",
                devcode=1,
                collector_addr=1,
                fcode=FC_QUERY_COLLECTOR,
            ),
            transaction_id=_IDENTITY_TID,
            function_code=FC_QUERY_COLLECTOR,
        )
    if kind == PROBE_FRAMED_FC1 and protocol == "eybond_framed":
        return IdentityProbeRequest(
            session_protocol=protocol,
            probe_kind=kind,
            payload=build_heartbeat_request(
                _IDENTITY_TID,
                _IDENTITY_HEARTBEAT_INTERVAL_SECONDS,
            ),
            transaction_id=_IDENTITY_TID,
            function_code=FC_HEARTBEAT,
        )
    return None


def parse_identity_probe_response(
    request: IdentityProbeRequest,
    response: bytes,
) -> tuple[str, str]:
    """Return ``(full_pn, strong_source)`` for one correlated response."""

    if type(request) is not IdentityProbeRequest or type(response) is not bytes:
        return "", ""

    if request.probe_kind == PROBE_AT_DTUPN:
        match = _AT_DTUPN_RE.search(response)
        if not match:
            return "", ""
        pn = validated_collector_pn(match.group(1).decode("ascii"))
        return (pn, "at_dtupn") if pn else ("", "")

    if len(response) < HEADER_SIZE:
        return "", ""
    try:
        header = decode_header(response[:HEADER_SIZE])
    except Exception:
        return "", ""
    if (
        header.tid != request.transaction_id
        or header.fcode != request.function_code
        or header.total_len != len(response)
        or header.payload_len <= 0
    ):
        return "", ""
    payload = response[HEADER_SIZE:]

    if request.probe_kind == PROBE_FRAMED_FC2:
        if len(payload) < 3 or payload[1] != 2:
            return "", ""
        raw_pn = payload[2:].rstrip(b"\x00")
        source = "fc2_parameter_2"
    elif request.probe_kind == PROBE_FRAMED_FC1:
        raw_pn = payload.rstrip(b"\x00")
        source = "fc1_identity_challenge"
    else:
        return "", ""

    try:
        pn = validated_collector_pn(raw_pn.decode("ascii"))
    except UnicodeDecodeError:
        return "", ""
    return (pn, source) if pn else ("", "")


__all__ = [
    "IdentityProbeRequest",
    "PROBE_AT_DTUPN",
    "PROBE_FRAMED_FC1",
    "PROBE_FRAMED_FC2",
    "build_identity_probe_request",
    "default_probe_kind_for_protocol",
    "parse_identity_probe_response",
    "silent_probe_kind_for_protocol",
]
