"""Resolve a cloud sensor title to a canonical cross-vendor presentation.

The universal label key is the normalized TITLE (proven across the
dc2376/dc6514/dc6544 corpus: ``eybond_read_N`` is per-protocol, titles are
shared). This loader maps a title — or one of its aliases — to a canonical HA
presentation (device_class / unit / state_class) and a stable semantic key,
independent of register or devcode.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import re


SEMANTIC_TITLES_PATH = (
    Path(__file__).resolve().parents[1] / "protocol_catalogs" / "semantic_titles.json"
)


@dataclass(frozen=True, slots=True)
class SemanticTitleEntry:
    """One canonical title with its HA presentation."""

    semantic_key: str
    canonical_title: str
    kind: str
    device_class: str = ""
    unit: str = ""
    state_class: str = ""


@dataclass(frozen=True, slots=True)
class SemanticTitleCatalog:
    """Title→entry lookup keyed by normalized title and aliases."""

    catalog_version: str
    by_normalized_title: dict[str, SemanticTitleEntry]

    def resolve(self, title: str) -> SemanticTitleEntry | None:
        return self.by_normalized_title.get(normalize_title(title))


def normalize_title(title: str) -> str:
    """Lowercase, collapse non-alphanumerics to single spaces, strip."""

    lowered = str(title or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", " ", lowered).strip()


@lru_cache(maxsize=None)
def load_semantic_title_catalog() -> SemanticTitleCatalog:
    """Load and index the semantic title catalog."""

    raw = json.loads(SEMANTIC_TITLES_PATH.read_text(encoding="utf-8"))
    by_title: dict[str, SemanticTitleEntry] = {}
    for item in raw.get("entries", []):
        if not isinstance(item, dict):
            continue
        ha = item.get("ha") if isinstance(item.get("ha"), dict) else {}
        entry = SemanticTitleEntry(
            semantic_key=str(item.get("semantic_key") or ""),
            canonical_title=str(item.get("canonical_title") or ""),
            kind=str(item.get("kind") or "read"),
            device_class=str(ha.get("device_class") or ""),
            unit=str(ha.get("unit") or ""),
            state_class=str(ha.get("state_class") or ""),
        )
        names = [item.get("canonical_title", "")]
        names.extend(item.get("aliases", []) or [])
        for name in names:
            normalized = normalize_title(str(name))
            if normalized:
                by_title.setdefault(normalized, entry)
    return SemanticTitleCatalog(
        catalog_version=str(raw.get("catalog_version") or ""),
        by_normalized_title=by_title,
    )


def clear_semantic_title_catalog_cache() -> None:
    """Clear the cached semantic title catalog."""

    load_semantic_title_catalog.cache_clear()


def resolve_semantic_title(title: str) -> SemanticTitleEntry | None:
    """Resolve one cloud title to its canonical entry, if known."""

    return load_semantic_title_catalog().resolve(title)
