# Validation workflow

The repository keeps two deliberately different failure boundaries:

- the broad, fast unit suite uses small `homeassistant.*` stubs and exercises
  protocol, policy, parsing, state-machine, and architecture behavior;
- `tests_ha/` installs real Home Assistant and exercises lifecycle integration.

The stub suite is not a substitute for Home Assistant, and the Home Assistant
suite is not broad enough to replace the unit suite.

## Local cadence

Use the smallest mode that matches the change:

```bash
# Inspect the exact changed-file and affected-test selection; run nothing.
python3 tools/validate.py plan

# Syntax, JSON, and whitespace for files changed from HEAD.
python3 tools/validate.py fast

# Fast checks, cheap catalog validators, and mapped unit regressions.
python3 tools/validate.py affected

# The complete stub-based quality gate.
python3 tools/validate.py unit

# Real Home Assistant tests in one already-prepared environment.
python3 tools/validate.py ha --ha-python /path/to/ha-current/bin/python
```

For a multi-commit branch, select affected files relative to its base:

```bash
python3 tools/validate.py affected --base origin/main
```

Run the release gate only before a release or after a cross-cutting lifecycle
change. It intentionally refuses to pass unless both supported HA lanes are
provided:

```bash
python3 tools/validate.py release \
  --ha-lane 2026.2=/path/to/ha-2026.2/bin/python \
  --ha-lane 2026.9=/path/to/ha-2026.9/bin/python
```

The same paths can be supplied through `EYBOND_HA_2026_2_PYTHON` and
`EYBOND_HA_2026_9_PYTHON`.

The current lane is pinned by `requirements-test-ha-2026.9.txt` to a matched
Home Assistant 2026.9.1 test stack. It replaces the 2026.7 lane, not an additional
per-change environment. Device-registry regressions must exercise entry-scoped
identifiers and assert that the integration emits no deprecated-API warnings.

`affected` is intentionally explicit rather than pretending to be a complete
dependency solver. Composition-root families select their architecture and
behavior suites; foundational typed models, telemetry, collector wire and
cloud-client boundaries have pinned mappings whose selection is itself tested.
Use `plan` to review that mapping before a long run. A green affected run is a
checkpoint, not a release substitute.

Do not repeat the complete unit suite or both HA lanes after every corrective.
Use `fast` after mechanical edits, focused files while diagnosing, `affected`
once the batch stabilizes, the current HA lane for lifecycle changes, and the
full unit/release gate once at the terminal boundary. This preserves both
failure boundaries without multiplying the same wall-clock waits.

## CI cadence

Every push and pull request runs the full stub-based quality gate and the
current HA lane. The older compatibility lane runs on the nightly schedule and
manual dispatch. Release validation still means both lanes; compatibility has
only been moved out of the inner development loop, not deleted.

## Pull request review evidence

Use the [acceptance criteria](../../CONTRIBUTING.md#pull-request-acceptance) for
the merge decision. CI is one input, not the decision itself.

- Record the PR head SHA, current target SHA, and exact tree tested. A green
  head-only run does not establish compatibility with changes made to the target
  since the PR branched. Check the merge base and validate the combined result
  in an isolated worktree when necessary.
- Select checks by impact using the cadence above. Shared listener or lifecycle
  changes need relevant real-HA checks and connection/ownership regressions;
  catalog-only changes do not require repeating unrelated live-device tests.
- For a suspected regression, run the same reproducer on the target baseline and
  the proposed result. Preserve a sanitized regression test. Existing tests
  passing does not override a failing new counterexample.
- Record skips, environment limitations, untested paths, and the provenance of
  hardware observations. Do not describe a snapshot or replay as a long live run,
  or a captured write as proof that a device accepted the setting.
- Resolve known blockers before acceptance. Recheck the head/target revisions
  before merging; changed relevant code requires renewed qualification.
