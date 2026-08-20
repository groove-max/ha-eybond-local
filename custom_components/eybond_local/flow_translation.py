"""Shared localized-presentation boundary for EyeBond data-entry flows.

Config and options flows are separate Home Assistant lifecycles, but they read
the same integration and runtime-flow translation bundles.  Keeping that loader
inside ``config_flow.py`` made every future flow extraction depend back on the
god module.  This module owns only presentation data: it performs no collector
I/O, changes no config entry, and carries no flow state beyond the cached bundle
on the consuming flow instance.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache, wraps
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_TRANSLATIONS_DIR = Path(__file__).with_name("translations")
_FLOW_TRANSLATIONS_DIR = Path(__file__).with_name("flow_translations")


def _translation_candidates(language: str) -> list[str]:
    candidates: list[str] = []
    normalized = (language or "").strip()
    if normalized:
        candidates.append(normalized)
        if "-" in normalized:
            candidates.append(normalized.split("-", 1)[0])
        if "_" in normalized:
            candidates.append(normalized.split("_", 1)[0])
    candidates.append("en")
    return candidates


def _load_translation_bundle_from_dir(
    directory: Path,
    language: str,
) -> dict[str, Any]:
    seen: set[str] = set()
    for candidate in _translation_candidates(language):
        if candidate in seen:
            continue
        seen.add(candidate)
        path = directory / f"{candidate}.json"
        if not path.exists():
            continue
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("Failed to load translation bundle: %s", path)
            break
    return {}


def _merge_translation_bundle(
    base: dict[str, Any],
    extra: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(base)
    for key, value in extra.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _merge_translation_bundle(existing, value)
        else:
            merged[key] = value
    return merged


@lru_cache(maxsize=16)
def load_translation_bundle(language: str) -> dict[str, Any]:
    """Load one merged translation bundle for the requested language."""

    bundle = _load_translation_bundle_from_dir(_TRANSLATIONS_DIR, language)
    flow_bundle = _load_translation_bundle_from_dir(
        _FLOW_TRANSLATIONS_DIR,
        language,
    )
    return _merge_translation_bundle(bundle, flow_bundle)


def translation_lookup(bundle: dict[str, Any], key: str) -> Any:
    """Look up a nested translation key inside one bundle."""

    current: Any = bundle
    for part in key.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def selector_option_label(
    bundle: dict[str, Any] | None,
    selector_key: str,
    option_key: str,
    default: str,
) -> str:
    """Resolve one localized selector-option label with a fallback."""

    if not isinstance(bundle, dict):
        return default
    value = translation_lookup(
        bundle,
        f"selector.{selector_key}.options.{option_key}",
    )
    return value if isinstance(value, str) and value else default


def with_translation_bundle(step):
    """Preload one flow translation bundle before rendering localized UI."""

    @wraps(step)
    async def _wrapped(self, *args, **kwargs):
        await self._async_ensure_translation_bundle()
        return await step(self, *args, **kwargs)

    return _wrapped


class TranslationBundleMixin:
    """Shared translation loading helpers for config and options flows."""

    def _flow_language(self) -> str:
        language = str(getattr(self, "context", {}).get("language") or "")
        if not language:
            hass = getattr(self, "hass", None)
            language = str(getattr(getattr(hass, "config", None), "language", "") or "")
        return language or "en"

    async def _async_ensure_translation_bundle(self) -> None:
        language = self._flow_language()
        if getattr(self, "_translation_bundle_language", None) == language:
            cached_bundle = getattr(self, "_translation_bundle", None)
            if isinstance(cached_bundle, dict):
                return

        self._translation_bundle = await self.hass.async_add_executor_job(
            load_translation_bundle,
            language,
        )
        self._translation_bundle_language = language

    def _tr(
        self,
        key: str,
        default: str,
        placeholders: dict[str, Any] | None = None,
    ) -> str:
        bundle: dict[str, Any] = {}
        if getattr(self, "_translation_bundle_language", None) == self._flow_language():
            cached_bundle = getattr(self, "_translation_bundle", None)
            if isinstance(cached_bundle, dict):
                bundle = cached_bundle
        value = translation_lookup(bundle, key)
        text = value if isinstance(value, str) and value else default
        if placeholders:
            try:
                return text.format(**placeholders)
            except (KeyError, ValueError):
                try:
                    return default.format(**placeholders)
                except (KeyError, ValueError):
                    return default
        return text


__all__ = [
    "TranslationBundleMixin",
    "load_translation_bundle",
    "selector_option_label",
    "translation_lookup",
    "with_translation_bundle",
]
