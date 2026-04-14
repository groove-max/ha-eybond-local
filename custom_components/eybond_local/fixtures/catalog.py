"""Local fixture catalog helpers and conventions."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_CATALOG_DIR = REPO_ROOT / ".local" / "fixtures" / "catalog"
FIXTURE_FILE_NAME = "fixture.json"
FIXTURE_META_FILE_NAME = "meta.json"
FIXTURE_INDEX_FILE_NAME = "index.json"
FIXTURE_META_SCHEMA_VERSION = 1
FIXTURE_INDEX_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class CatalogEntryPaths:
    """Filesystem locations for one catalog entry."""

    slug: str
    directory: Path
    fixture_path: Path
    metadata_path: Path


def slugify_fixture_name(text: str) -> str:
    """Convert free-form fixture text into a stable catalog slug."""

    lowered = text.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return slug or "fixture"


def catalog_entry_paths(slug: str) -> CatalogEntryPaths:
    """Return the standard file layout for one catalog slug."""

    normalized_slug = slugify_fixture_name(slug)
    directory = FIXTURE_CATALOG_DIR / normalized_slug
    return CatalogEntryPaths(
        slug=normalized_slug,
        directory=directory,
        fixture_path=directory / FIXTURE_FILE_NAME,
        metadata_path=directory / FIXTURE_META_FILE_NAME,
    )


def ensure_catalog_dir() -> Path:
    """Ensure the local catalog directory exists and return it."""

    FIXTURE_CATALOG_DIR.mkdir(parents=True, exist_ok=True)
    return FIXTURE_CATALOG_DIR


def catalog_has_entries() -> bool:
    """Return whether the local catalog currently has at least one complete entry."""

    if not FIXTURE_CATALOG_DIR.is_dir():
        return False
    for child in FIXTURE_CATALOG_DIR.iterdir():
        if not child.is_dir():
            continue
        if (child / FIXTURE_FILE_NAME).is_file() and (child / FIXTURE_META_FILE_NAME).is_file():
            return True
    return False


def iter_catalog_entries() -> tuple[CatalogEntryPaths, ...]:
    """Return all catalog entries that have both fixture and metadata files."""

    ensure_catalog_dir()
    entries: list[CatalogEntryPaths] = []
    for child in sorted(FIXTURE_CATALOG_DIR.iterdir()):
        if not child.is_dir():
            continue
        entry = CatalogEntryPaths(
            slug=child.name,
            directory=child,
            fixture_path=child / FIXTURE_FILE_NAME,
            metadata_path=child / FIXTURE_META_FILE_NAME,
        )
        if entry.fixture_path.is_file() and entry.metadata_path.is_file():
            entries.append(entry)
    return tuple(entries)


def load_catalog_metadata(path: CatalogEntryPaths | Path | str) -> dict[str, Any]:
    """Load one catalog metadata document."""

    metadata_path = path.metadata_path if isinstance(path, CatalogEntryPaths) else Path(path)
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def save_catalog_metadata(path: CatalogEntryPaths | Path | str, payload: dict[str, Any]) -> None:
    """Write one catalog metadata document."""

    metadata_path = path.metadata_path if isinstance(path, CatalogEntryPaths) else Path(path)
    metadata_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def rebuild_catalog_index() -> Path:
    """Rebuild the top-level catalog index from entry metadata files."""

    ensure_catalog_dir()
    entries = [load_catalog_metadata(entry) for entry in iter_catalog_entries()]
    index_path = FIXTURE_CATALOG_DIR / FIXTURE_INDEX_FILE_NAME
    index_path.write_text(
        json.dumps(
            {
                "schema_version": FIXTURE_INDEX_SCHEMA_VERSION,
                "entries": entries,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return index_path
