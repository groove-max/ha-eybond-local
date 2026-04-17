# Tools

Command-line utilities for probing live hardware, working with local fixtures, validating the project, and refreshing generated documentation.

> All commands assume the current working directory is the repository root.

---

## I want to talk to a live device

Read a one-shot full snapshot from the inverter:

```bash
python3 tools/poll_once.py \
  --server-ip <ha_host_ip> \
  --collector-ip <collector_ip> \
  --full-snapshot
```

Run the same auto-detection that the Home Assistant config flow uses, but from the terminal:

```bash
python3 tools/detect_onboarding.py \
  --server-ip <ha_host_ip> \
  --collector-ip <collector_ip>
```

Read the collector's heartbeat and profile data:

```bash
python3 tools/heartbeat_probe.py \
  --server-ip <ha_host_ip> \
  --collector-ip <collector_ip> \
  --samples 3
```

Send a raw collector function-code probe (low-level debugging):

```bash
python3 tools/collector_fc_probe.py \
  --server-ip <ha_host_ip> \
  --collector-ip <collector_ip> \
  --fcode 1
```

> `smartess_cloud_probe.py` is a maintainer and support tool. Normal Home Assistant setup does not require it.
> Passing `--password` on the command line can leave credentials in your shell history, so use it only on a trusted machine and clean up history afterward if needed.

Query the SmartESS cloud with the same signing scheme the Android app uses:

```bash
python3 tools/smartess_cloud_probe.py list-devices \
  --username <smartess_user> \
  --password <smartess_password> \
  --pn <collector_pn> \
  --pagesize 50
```

Fetch the cloud-backed live detail payload for one device identity:

```bash
python3 tools/smartess_cloud_probe.py device-detail \
  --username <smartess_user> \
  --password <smartess_password> \
  --pn <collector_pn> \
  --sn <device_sn> \
  --devcode 0x0102 \
  --devaddr 0x05
```

Fetch the device list plus detail/settings/energy-flow for one identity in a single run:

```bash
python3 tools/smartess_cloud_probe.py device-bundle \
  --username <smartess_user> \
  --password <smartess_password> \
  --pn <collector_pn> \
  --sn <device_sn> \
  --devcode 0x0102 \
  --devaddr 0x05
```

Write the same SmartESS bundle into Home Assistant's local `cloud_evidence` store so the next support bundle/archive export can pick it up automatically:

```bash
python3 tools/smartess_cloud_probe.py device-bundle \
  --username <smartess_user> \
  --password <smartess_password> \
  --pn <cloud_device_pn> \
  --sn <device_sn> \
  --devcode 0x0102 \
  --devaddr 0x05 \
  --collector-pn <local_collector_pn> \
  --cloud-evidence-config-dir /config
```

If the local entry uses the same `collector_pn`, the next advanced `Export Support Bundle` JSON export or `Create Support Archive` action will include that cloud evidence automatically.

Saved cloud-evidence files stay under `/config/eybond_local/cloud_evidence/` until you remove them manually. EyeBond Local automatically reuses the latest matching file for the entry.

Capture Modbus registers through the collector transport:

```bash
python3 tools/modbus_dump.py \
  --server-ip <ha_host_ip> \
  --collector-ip <collector_ip> \
  --smg-preset
```

List writable capabilities or execute a write locally:

```bash
python3 tools/write_capability.py \
  --server-ip <ha_host_ip> \
  --collector-ip <collector_ip> \
  --list
```

---

## I want to work with fixtures

Fixture catalogs are local-only and live under `.local/fixtures/catalog/`. They are intentionally ignored by git.

Replay a saved fixture without live hardware:

```bash
python3 tools/replay_fixture.py \
  --fixture /path/to/fixture.json \
  --full-snapshot
```

Anonymize a captured fixture before sharing it (strips serials, IPs, MAC addresses):

```bash
python3 tools/anonymize_fixture.py \
  --input /path/to/fixture.json \
  --output /path/to/fixture_anon.json
```

Import a fixture into the local catalog:

```bash
python3 tools/import_fixture.py \
  --input /path/to/fixture_anon.json \
  --slug my-device-capture \
  --title "My Device Capture"
```

Validate the local catalog and rebuild its index:

```bash
python3 tools/validate_fixture_catalog.py --rebuild-index
```

---

## I want to validate the project

Run the unit test suite:

```bash
python3 -m unittest discover -s tests -v
```

Validate declarative profiles:

```bash
python3 tools/validate_profiles.py
```

Run the full public quality gate (tests + profile validation + checked-in doc freshness):

```bash
python3 tools/quality_gate.py
```

Refresh generated docs first, then run the quality gate:

```bash
python3 tools/quality_gate.py --refresh-generated
```

## I want to cut a release

Render the GitHub release body for a version directly from `CHANGELOG.md`:

```bash
python3 tools/render_release_notes.py v0.1.43
```

Write the release body to a file for `gh release create --notes-file`:

```bash
python3 tools/render_release_notes.py v0.1.43 --output .local/release-notes/v0.1.43.md
```

The full maintainer flow is documented in [docs/RELEASING.md](../docs/RELEASING.md).

---

## I want to refresh generated documentation

Checked-in generated reports live under `docs/generated/`. Most contributors don't need to run these by hand — `quality_gate.py --refresh-generated` does it all in one go. Use the individual commands when iterating on a specific public report.

| Report | Command |
|---|---|
| Support overview | `python3 tools/export_support_overview.py --format markdown --output docs/generated/SUPPORT_OVERVIEW.generated.md` |
| SMG support matrix | `python3 tools/export_support_matrix.py --profile smg_modbus.json --format markdown --output docs/generated/SMG_SUPPORT_MATRIX.generated.md` |

## I want fixture-derived debug reports

These reports are intentionally local-only. Store them under `.local/generated/` or another ignored path.

| Report | Command |
|---|---|
| Fixture coverage | `python3 tools/export_fixture_coverage.py --format markdown --output .local/generated/FIXTURE_COVERAGE.generated.md` |
| Fixture validation | `python3 tools/export_fixture_validation.py --format markdown --output .local/generated/FIXTURE_VALIDATION.generated.md` |
| Evidence index | `python3 tools/export_evidence_index.py --format markdown --output .local/generated/EVIDENCE_INDEX.generated.md` |
| Release readiness | `python3 tools/export_release_readiness.py --format markdown --output .local/generated/RELEASE_READINESS.generated.md` |
