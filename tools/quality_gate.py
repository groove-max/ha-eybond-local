#!/usr/bin/env python3
"""Run the project quality gate in one command."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from time import perf_counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support.quality_gate import (  # noqa: E402
    REPO_ROOT as PROJECT_ROOT,
    build_quality_gate_steps,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh-generated",
        action="store_true",
        help="rewrite generated docs before running the normal --check steps",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print a JSON report instead of line-oriented text",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="list the configured quality-gate steps without running them",
    )
    args = parser.parse_args()

    steps = build_quality_gate_steps(
        python_executable=sys.executable,
        refresh_generated=args.refresh_generated,
    )

    if args.list:
        for step in steps:
            print(f"{step.key}: {' '.join(step.command)}")
        return 0

    started = perf_counter()
    results: list[dict[str, object]] = []
    ok = True

    for step in steps:
        step_started = perf_counter()
        completed = subprocess.run(
            step.command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        duration_ms = int((perf_counter() - step_started) * 1000)
        result = {
            "key": step.key,
            "title": step.title,
            "command": list(step.command),
            "returncode": completed.returncode,
            "ok": completed.returncode == 0,
            "duration_ms": duration_ms,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        results.append(result)

        if not args.json:
            status = "OK" if completed.returncode == 0 else "FAIL"
            print(f"[{status}] {step.key} ({duration_ms} ms)")
            if completed.returncode != 0:
                if completed.stdout.strip():
                    print(completed.stdout.rstrip())
                if completed.stderr.strip():
                    print(completed.stderr.rstrip(), file=sys.stderr)

        if completed.returncode != 0:
            ok = False
            break

    report = {
        "ok": ok,
        "steps_ran": len(results),
        "steps_total": len(steps),
        "duration_ms": int((perf_counter() - started) * 1000),
        "refresh_generated": args.refresh_generated,
        "results": results,
    }

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    elif ok:
        print(
            f"Quality gate passed: {report['steps_ran']}/{report['steps_total']} steps in {report['duration_ms']} ms."
        )
    else:
        print(
            f"Quality gate failed: step {results[-1]['key']} "
            f"({results[-1]['returncode']}) after {report['steps_ran']}/{report['steps_total']} steps."
        )

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
