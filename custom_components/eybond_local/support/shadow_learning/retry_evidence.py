"""Stricter draft qualification after a previous command could have been sent.

Separate traces/cursors prevent reuse of old records, not late wire delivery.
This check rejects known ambiguity; it is NOT proof of a drained cloud queue.
"""

from collections import defaultdict
from copy import deepcopy
import json

from .pi30_protocol import Pi30ShadowLearningAdapter


def _native_pi30_write(wire: dict) -> str:
    """Keep the whole command as a value, not as the register-like target."""
    raw = wire.get("raw_payload_hex")
    if type(raw) is not str or not 8 <= len(raw) <= 134:
        return ""
    try:
        request = Pi30ShadowLearningAdapter().decode_write_request(bytes.fromhex(raw))
    except ValueError:
        return ""
    if request is None or request.command != wire.get("command"):
        return ""
    if (type(wire.get("devcode")) is not int or not 0 <= wire["devcode"] <= 65535
            or type(wire.get("devaddr")) is not int or not 0 <= wire["devaddr"] <= 255):
        return ""
    return request.command


def qualify_retry_evidence(result: dict) -> dict:
    result = dict(result)
    correlation = deepcopy(result.get("correlation") or {})
    groups = defaultdict(list)
    for item in correlation.get("matched", []):
        groups[item.get("field_id", "")].append(item)
    ambiguous = {item.get("field_id") for item in result.get("results", [])
                 if item.get("observation_count", 0) > 1}
    extra_writes = bool(correlation.get("unmatched_writes"))
    qualified, rejected = [], []
    for field, items in groups.items():
        targets, requested, observed = set(), set(), set()
        native_valid = True
        for item in items:
            wire = item.get("observation", {})
            target_keys = ("protocol", "command", "function_code", "devcode", "devaddr", "unit", "register")
            if wire.get("protocol") == "pi30_ascii":
                command = _native_pi30_write(wire)
                native_valid = native_valid and bool(command)
                target_keys = tuple(key for key in target_keys if key != "command")
                observed.add(command)
            else:
                observed.add(json.dumps(wire.get("values") or wire.get("value", ""), sort_keys=True))
            targets.add(tuple(wire.get(k) for k in target_keys))
            requested.add(str(item.get("requested_value", "")))
        valid = (bool(field) and native_valid and not extra_writes and field not in ambiguous
                 and len(targets) == 1 and len(requested) >= 2 and len(observed) >= 2)
        if valid:
            qualified.extend(items)
        else:
            rejected.extend({**item, "reason": "protected_retry_correlation_unconfirmed"} for item in items)
    correlation["matched"] = qualified
    correlation["matched_count"] = len(qualified)
    correlation["retry_quarantined"] = rejected
    correlation["retry_quarantined_count"] = len(rejected)
    result["correlation"] = correlation
    result["protected_retry"] = True
    return result
