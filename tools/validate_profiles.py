#!/usr/bin/env python3
"""Validate declarative driver profiles and print a compact summary."""

from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.metadata.profile_loader import (  # noqa: E402
    load_driver_profile,
    profile_file_names,
)


def main() -> int:
    profiles: list[dict[str, object]] = []
    for profile_name in profile_file_names():
        profile = load_driver_profile(profile_name)
        profiles.append(
            {
                "file": profile_name,
                "profile_key": profile.key,
                "title": profile.title,
                "groups": len(profile.groups),
                "capabilities": len(profile.capabilities),
                "presets": len(profile.presets),
                "enum_capabilities": [
                    capability.key
                    for capability in profile.capabilities
                    if capability.value_kind == "enum"
                ],
            }
        )

    print(json.dumps({"profiles": profiles}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
