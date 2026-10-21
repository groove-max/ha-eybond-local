"""Provider-neutral numeric evidence for protected cloud command learning.

Probe values are NOT recommended settings or inferred operating limits. They
may only be dispatched by an already protected shadow-learning workflow. Cloud
rejections are evidence, not a reason to bypass interception or try every value.
"""

from decimal import Decimal, InvalidOperation
import re


_NUMBER = r"-?\d+(?:\.\d+)?"
_RANGE = re.compile(rf"({_NUMBER})\s*(?:~|～|–|\bto\b|-)\s*({_NUMBER})", re.I)
# Ambiguous/categorical lists remain exact mappings, with a finite request budget.
# Unobserved choices are never synthesized by the overlay generator.
MAX_LEARNING_CHOICE_VALUES = 32


def decimal_value(value: object) -> Decimal | None:
    text = str(value).strip()
    if len(text) > 32 or not re.fullmatch(_NUMBER, text):
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    return number if number.is_finite() and abs(number) <= 1_000_000_000 else None


def numeric_choice_learning_candidates(choices, *, current="") -> list[dict]:
    """Sample quantitative provider choices, without deriving operating limits.

    Labels must repeat the actual numeric value with one consistent unit. Codes
    such as 0 -> 50Hz are enums, not quantities. Short integer lists stay enums;
    fractional quantities need numeric encoding evidence even in short lists.
    Only existing provider values are used, never interpolated probe values.
    """
    parsed = []
    units = set()
    for raw_value, label in choices:
        value = decimal_value(raw_value)
        match = re.fullmatch(rf"\s*({_NUMBER})\s*([A-Za-z%°/]+)\s*", str(label))
        if value is None or match is None or decimal_value(match[1]) != value:
            return []
        units.add(match[2])
        parsed.append((str(raw_value), value))
    if len(units) != 1 or len({v for _, v in parsed}) < 2:
        return []
    if len(parsed) <= MAX_LEARNING_CHOICE_VALUES and all(v == v.to_integral_value() for _, v in parsed):
        return []
    start = next((item for item in parsed if item[1] == decimal_value(current)), parsed[0])
    selected = [start]
    for item in parsed:
        if item[1] not in {v for _, v in selected}:
            selected.append(item)
        if len(selected) == 3:
            break
    evidence = {"kind": "number", "unit": units.pop(), "sample_source": "provider_choices"}
    return [dict(value=raw, label="", source="numeric_probe", numeric_evidence=dict(evidence))
            for raw, _ in selected]


def numeric_learning_candidates(*, hint="", unit="", current="", fallback="1") -> list[dict]:
    """Return at most two distinct probes, with device-scoped range evidence.

    Only an unambiguous single range is accepted from the provider hint. Voltage
    family, field name and cloud field ID never supply a range or register address.
    """
    matches = list(_RANGE.finditer(str(hint)))
    minimum = maximum = None
    if len(matches) == 1:
        minimum, maximum = (decimal_value(v) for v in matches[0].groups())
        if minimum is None or maximum is None or minimum > maximum:
            minimum = maximum = None
    start = decimal_value(current)
    source = "current"
    if start is None or (minimum is not None and not minimum <= start <= maximum):
        start = minimum if minimum is not None else decimal_value(fallback)
        source = "provider_hint" if minimum is not None else "probe_only"
    if start is None:
        return []
    # Decimal precision can select a second probe in a sub-unit interval;
    # otherwise use a whole-unit separation, not a claimed device step.
    precision = min(start.as_tuple().exponent, 0)
    if minimum is not None:
        precision = min(precision, minimum.as_tuple().exponent, maximum.as_tuple().exponent)
    step = Decimal(1)
    if minimum is not None and maximum - minimum < step:
        step = Decimal(10) ** max(precision, -3)
    second = start + step
    if maximum is not None and second > maximum:
        second = start - step
    candidates = [start]
    if second != start and (minimum is None or minimum <= second <= maximum):
        candidates.append(second)
    evidence = {"kind": "number", "unit": str(unit or ""), "sample_source": source}
    if minimum is not None:
        evidence.update(minimum=str(minimum), maximum=str(maximum), range_source="provider_hint")
    return [dict(value=format(v, "f"), label="", source="numeric_probe",
                 numeric_evidence=dict(evidence)) for v in candidates]


def classify_numeric_encoding(samples: list[dict]) -> dict:
    """Prove the supported unsigned, zero-offset encoding across all samples."""
    evidence = [s.get("numeric_evidence") for s in samples]
    if not evidence or any(not isinstance(e, dict) or e.get("kind") != "number" for e in evidence):
        return {"rejection_reason": "numeric_metadata_inconsistent"}
    if any(e != evidence[0] for e in evidence[1:]):
        return {"rejection_reason": "numeric_metadata_inconsistent"}
    pairs = []
    routes = set()
    for sample in samples:
        observation = sample.get("observation", {})
        routes.add(tuple(observation.get(k) for k in ("register", "function_code", "unit", "devcode", "devaddr")))
        value = decimal_value(sample.get("requested_value"))
        words = sample.get("observation", {}).get("values")
        if value is None or not isinstance(words, (list, tuple)) or len(words) != 1:
            return {"rejection_reason": "unsupported_numeric_encoding"}
        if type(words[0]) is not int or not 0 <= words[0] <= 65535 or value < 0:
            return {"rejection_reason": "unsupported_numeric_encoding"}
        pairs.append((value, words[0]))
    if len(routes) != 1:
        return {"rejection_reason": "numeric_route_inconsistent"}
    if len({v for v, _ in pairs}) < 2:
        return {"rejection_reason": "numeric_needs_distinct_samples"}
    divisors = [d for d in (1, 10, 100, 1000) if all(v * d == raw for v, raw in pairs)]
    if len(divisors) != 1:
        return {"rejection_reason": "numeric_encoding_inconsistent"}
    divisor = divisors[0]
    result = dict(value_kind="scaled_u16" if divisor > 1 else "u16", word_count=1,
                  combine="u16", advanced=False, requires_confirm=True,
                  unsafe_while_running=False, safety_class="setting", confidence="high")
    if divisor > 1:
        result["divisor"] = divisor
    if evidence[0].get("unit"):
        result["unit"] = evidence[0]["unit"]
    for key in ("minimum", "maximum"):
        if key not in evidence[0]:
            continue
        bound = decimal_value(evidence[0][key])
        if bound is None or bound * divisor != (bound * divisor).to_integral_value():
            return {"rejection_reason": "numeric_range_not_representable"}
        raw_bound = int(bound * divisor)
        if not 0 <= raw_bound <= 65535:
            return {"rejection_reason": "numeric_range_not_representable"}
        result[key] = raw_bound
    if result.get("minimum", 0) > result.get("maximum", 65535):
        return {"rejection_reason": "numeric_range_not_representable"}
    return result
