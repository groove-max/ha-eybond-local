# Reviewed recovery of historical learning holds

This is a maintainer-directed repair of **old, stopped** learning histories,
not a user-facing force-clear, cloud command expiry, or new learning method.
Routine new studies continue to use the live completion ledger. Updating alone
does not approve any historical hold.

## Preconditions and review

Review every original and retry attempt, including the actual captured requests,
their refusal responses, and terminal provider results. Counts, HTTP success,
elapsed time, a stopped proxy, and a later successful retry are not substitutes
for terminal evidence. Explicitly establish the completeness of the history;
the inventory cannot discover files that were deleted before inspection.
Mapping quarantine and command completion are separate questions.

The repair binds this human review to the exact on-host hold, entry/PN, original
and retained retry journals, and trace digests. Local traces must have matching
unmasked identity, one session manifest, a stop, and a non-pending final summary.
Share archives can be reviewed evidence but cannot replace those local traces.
Missing/corrupt history, changed traces, new retries or foreign ownership refuse
the operation. Do not delete storage, invent a no-request receipt, or rerun
sensitive controls to obtain a cleaner history.

## Workflow

Use `tools/review_learning_history.py` from the same source revision as the
installed integration, in a Python environment with Home Assistant installed
and access to its real configuration directory. Keep local records and review
receipts private. The script is not included in the HACS component directory;
provide it separately during the supervised repair.

1. Update to a build containing this repair. Keep the working local route and
   do not start new learning while the history is being reviewed.
2. Read-only inventory (replace placeholders locally):

   ```sh
   python tools/review_learning_history.py --config-dir /config --collector-pn COLLECTOR_PN --inspect
   ```

   Only this inventory output is intended for sharing. It contains counts,
   validated timestamps, source names and digests, not PN, endpoint or paths.
3. After reviewing every run, supply one evidence file and reviewed command
   count **per run, oldest first**. A one-run example:

   ```sh
   python tools/review_learning_history.py --config-dir /config --collector-pn COLLECTOR_PN \
     --evidence-file /config/reviewed-evidence.zip --reviewed-commands COMMAND_COUNT
   ```

   This is a no-write preview. Repeat both options for additional runs. The
   entry and original study are derived from the exact active hold; optional
   `--entry-id` and `--study-id` require explicit matches.
4. Only after explicit agreement on that complete review, repeat the preview
   with `--approve-sha256 REVIEW_SHA256 --confirm-all-runs-terminal`.
   The freshly computed plan must match. This creates a private, create-only
   `maintainer_reviewed_terminal_history` receipt; it changes no endpoint and
   does not yet retire the hold. It is not an automatic wire-completion receipt.
5. Restart HA or use the existing learning recovery/stop action. HA drains the
   HTTP work and route under its existing preparation and endpoint ownership,
   revalidates the unchanged history, restores the **original run's** endpoint
   obligation in the current journal, and archives the hold. Ordinary endpoint
   restoration and verification then run. Check cloud telemetry separately;
   retiring protection is not proof that the cloud is receiving data.

If preparation or approval fails, keep the local route and investigate the
specific error; do not replace the requested review hash or edit storage to
make it pass. Approval is a maintainer judgment, not a conclusion produced by
the preview tool or a mechanism to recover arbitrary unknown command histories.

## Crash and transport boundaries

The receipt precedes journal correction, which precedes hold archival. A crash
at either persistence boundary leaves the original endpoint recoverable. An
endpoint restore/readback failure retains a retryable journal; cancellation
drains the owned filesystem operation. The retry owner is preserved, rather
than borrowing the original study's old operation token. No cloud control is
sent and no direct endpoint write exists in the tool.

The review/receipt contract is protocol-neutral, but terminal evidence must be
qualified for the actual provider and transport. It does not turn unsupported
API acceptance into completion. Bridge's separate cloud-write filtering and
endpoint return policy remain independent of this historical-hold repair.
