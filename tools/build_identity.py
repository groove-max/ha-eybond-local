#!/usr/bin/env python3
"""Print the offline identity of a component directory, without importing HA."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from custom_components.eybond_local.build_identity import read_build_runtime_values


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--path', type=Path, default=ROOT/'custom_components/eybond_local')
    args = parser.parse_args()
    values = read_build_runtime_values(args.path)
    print(json.dumps(values, indent=2))
    return 0 if values['integration_build_fingerprint_status'] == 'available' else 1


if __name__ == '__main__':
    raise SystemExit(main())
