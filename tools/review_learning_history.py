#!/usr/bin/env python3
"""Preview/approve reviewed historical learning recovery; never change endpoints.

Maintainer-directed only. Review every run's terminal delivery first. Supply one
evidence file and command count per run, oldest first. Preview does not establish
that review or the completeness of history. Do not publish private local files.
"""
import argparse
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", type=Path, required=True)
    parser.add_argument("--entry-id")
    parser.add_argument("--collector-pn", required=True)
    parser.add_argument("--study-id")
    parser.add_argument("--inspect", action="store_true", help="Read-only, shareable history inventory; no approval")
    parser.add_argument("--evidence-file", type=Path, action="append")
    parser.add_argument("--reviewed-commands", type=int, action="append")
    parser.add_argument("--approve-sha256")
    parser.add_argument("--confirm-all-runs-terminal", action="store_true")
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    sys.path.insert(0, str(args.config_dir))
    from custom_components.eybond_local.support.shadow_learning.reviewed_recovery import ReviewedLearningRecoveryStore
    store = ReviewedLearningRecoveryStore(args.config_dir)
    if args.inspect:
        if args.approve_sha256 or args.confirm_all_runs_terminal:
            parser.error("--inspect cannot approve recovery")
        print(json.dumps(store.inspect(args.collector_pn), indent=2))
        return
    if not args.evidence_file or not args.reviewed_commands:
        parser.error("Preview/approval requires one evidence file and command count per reviewed run")
    hold = store.load(args.collector_pn)
    if hold is None:
        parser.error("No active hold; nothing to approve")
    plan = store.prepare(collector_pn=args.collector_pn, entry_id=args.entry_id or hold.entry_id,
                         study_id=args.study_id or hold.study_id, evidence_files=args.evidence_file,
                         command_counts=args.reviewed_commands)
    digest = store.review_digest(plan)
    print(f"Review SHA256: {digest}")
    print(f"Runs: {len(plan['history'])}; reviewed commands: {sum(args.reviewed_commands)}")
    print(f"Original endpoint (private): {plan['history'][0]['state']['original_endpoint']!r}")
    if args.approve_sha256:
        store.approve(plan=plan, expected_digest=args.approve_sha256,
                      confirmed=args.confirm_all_runs_terminal)
        print("Review receipt recorded. Restart HA for normal, owned endpoint recovery.")
    else:
        print("PREVIEW ONLY: no records changed. Approval requires prior maintainer review.")
    print("No collector, inverter or cloud request was sent by this tool.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, RecursionError) as exc:
        reason = str(exc)
        if not (reason.startswith(("review_", "explicit_terminal_", "cloud_learning_"))
                and reason.replace("_", "").isalnum() and len(reason) <= 100):
            reason = "review_input_unreadable_or_invalid"
        print("Review refused: " + reason, file=sys.stderr)
        raise SystemExit(2) from None
