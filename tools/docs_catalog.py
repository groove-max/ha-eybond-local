"""Render reviewed public model records, without copying evidence attachments."""

from __future__ import annotations

import json
from pathlib import Path
import re


def model_records(root: Path) -> list[dict]:
    return [json.loads(path.read_text()) for path in sorted(
        (root / "catalog/inverter_models/models").glob("*.json")
    )]


def model_slug(record: dict) -> str:
    return "devices/" + record["model_key"].replace("_", "-")


def catalog_table(records: list[dict], base: str, language: str) -> str:
    heading = "Model evidence" if language == "en" else "Докази за моделями"
    columns = (
        "Model | Readings | Controls | Status" if language == "en"
        else "Модель (деталі англійською) | Показання | Керування | Стан"
    )
    labels = {
        "confirmed": "підтверджено", "partial": "частково", "none": "немає",
        "unknown": "невідомо", "supported": "підтримується",
        "experimental": "експериментальний", "research": "дослідження",
        "deprecated": "застарілий",
    }
    def label(value: str) -> str:
        return labels.get(value, value) if language == "uk" else value
    lines = [f"## {heading}", "", f"| {columns} |", "|---|---|---|---|"]
    for record in records:
        validation = record["validation"]
        name = f"{record['manufacturer']} {record['model']}"
        link = f"{base}en/{model_slug(record)}/"
        lines.append(
            f"| [{name}]({link}) | {label(validation['telemetry'])} | "
            f"{label(validation['controls'])} | {label(record['lifecycle'])} |"
        )
    return "\n".join(lines) + "\n"


def model_page(record: dict, root: Path, base: str) -> str:
    name = f"{record['manufacturer']} {record['model']}"
    validation = record["validation"]
    lines = [
        f"# {name} with Home Assistant", "",
        record["knowledge_summary"], "",
        "> This is a record of the identified variant, not a guarantee for every",
        "> unit sold under this name. The catalog follows current main; check",
        f"> [release channels]({base}en/installation/) before downloading.", "",
        "## What has been confirmed", "",
        "| Area | Recorded evidence |", "|---|---|",
        f"| Local readings | {validation['telemetry']} |",
        f"| Controls | {validation['controls']} |",
        f"| Hardware evidence | {validation['hardware']} |",
        f"| Catalog lifecycle | {record['lifecycle']} |", "",
        "Controls being listed or exposed does not prove that every setting has",
        "been tested. Read the evidence and limitations below.", "",
    ]
    if record.get("aliases"):
        lines += ["## Names you may see", "", ", ".join(record["aliases"]), ""]
    lines += ["## Recorded variants", ""]
    for variant in record["variants"]:
        lines += [f"- {variant['label']}"]
        if variant.get("known_firmware"):
            lines += ["  Firmware: " + ", ".join(variant["known_firmware"]) + "."]
    lines += ["", "## Capabilities and limits", ""]
    for note in record.get("coverage", {}).get("notes", []):
        lines.append(f"- {note}")
    for note in record.get("known_limitations", []):
        lines.append(f"- {note}")
    lines += ["", "## Next step", ""]
    if record["lifecycle"] == "research":
        lines += [
            "A working local path is not yet established for this recorded model.",
            f"Please [prepare support evidence]({base}en/help/) rather than selecting an unrelated driver.",
        ]
    else:
        lines += [
            f"Start with [installation]({base}en/installation/) and [collector setup]({base}en/getting-started/).",
            f"Compare readings before changing settings. For missing cloud settings, see [learning]({base}en/learning/).",
        ]
    lines += ["", "## Public evidence", ""]
    linked = False
    for key in record.get("source_keys", []):
        source = json.loads((root / "catalog/inverter_models/sources" / f"{key}.json").read_text())
        # Never publish private locations, filenames, attachments or trace data.
        if source.get("visibility") != "public" or not source.get("summary_publishable"):
            continue
        reference = source.get("reference", "")
        issue = re.fullmatch(r"project-issue:(\d+)", reference)
        url = ("https://github.com/groove-max/ha-eybond-local/issues/" + issue[1]
               if issue else reference if reference.startswith("https://") else "")
        if url.startswith("https://"):
            lines.append(f"- [{source.get('title', key)}]({url})")
            linked = True
    if not linked:
        lines.append("The published catalog records the reviewed result; raw device captures are not hosted here.")
    lines += ["", f"[Full catalog reference]({base}en/reference/catalog/)", ""]
    return "\n".join(lines)
