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
  --ha-lane 2026.7=/path/to/ha-2026.7/bin/python
```

The same paths can be supplied through `EYBOND_HA_2026_2_PYTHON` and
`EYBOND_HA_2026_7_PYTHON`.

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
