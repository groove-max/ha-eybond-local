# Issue triage

Use issue state, rather than age alone, to decide whether an issue remains
open. Inactivity does not make a reproducible regression, security problem,
accepted feature request, or maintainer-owned task obsolete.

## Labels

Apply the ordinary type label (`bug`, `enhancement`, `support`, or
`regression`) independently from the workflow status. An issue may have at
most one `status:` label at a time.

| Label | Meaning | Next maintainer action |
| --- | --- | --- |
| `status: needs-info` | A specific answer, reproduction, or current Support Archive is required before work can continue. | State exactly what is missing and wait for the reporter. |
| `status: fixed-awaiting-release` | The required change is implemented locally but is not in a public release. | Keep the issue open until the release is published. |
| `status: awaiting-retest` | A public release contains the change, but a hardware or user retest would still be useful. | Wait for feedback, then either continue the investigation or close as completed. |

The Support Archive issue template applies `support` automatically. Status
labels are always selected by a maintainer after reviewing the issue; no bot
or issue form should infer them from user text.

## State transitions

1. When asking for evidence, remove any previous `status:` label, apply
   `status: needs-info`, and name the exact version, reproduction, or artifact
   required.
2. When the implementation is complete but unreleased, replace the status with
   `status: fixed-awaiting-release`.
3. After publishing the release:
   - close as completed immediately when no user confirmation is required; or
   - replace the status with `status: awaiting-retest` when a real-device check
     would materially improve confidence.
4. When the reporter responds, remove the waiting status before continuing
   triage.

An implemented and released change does not remain unfinished only because the
reporter stopped responding. Close it as completed with the release version and
invite a new Support Archive if the problem persists.

When progress is impossible without requested evidence, a maintainer may close
the issue as not planned after a clear reminder and a reasonable waiting period.
Explain that the issue can be reopened when the missing evidence is available.

## Inactivity review

For now, inactivity review is manual:

- review `status: needs-info` and `status: awaiting-retest` periodically;
- send at most one concise reminder before closing for inactivity;
- do not close issues merely because they are old;
- do not apply these timers to confirmed regressions, security reports,
  accepted roadmap work, or problems reproducible by a maintainer.

No stale action or automatic closing workflow is enabled. Automation may be
added later only for explicitly labelled waiting states, after the manual
process has proven reliable.

## Scope

Keep one independently testable problem or one hardware fingerprint per issue.
If a thread accumulates unrelated models or failures, preserve the discussion
but move follow-up work into separate issues so each one can be resolved and
closed honestly.
