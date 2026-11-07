# Install and update EyeBond Local

[Українська](INSTALLATION.uk.md)

EyeBond Local is a custom Home Assistant integration, not an app/add-on. HACS is
optional. You need a supported EyeBond-compatible collector, working network
access and a backup before changing an existing installation.

## Choose the version first

| Channel | Current publication | Use it when |
|---|---|---|
| Current release | [0.3.0-rc.1](https://github.com/groove-max/ha-eybond-local/releases/tag/v0.3.0-rc.1) | You want the tested 0.3 release snapshot; it remains an RC with documented qualification limits. No beta switch is required. |
| Older releases | [0.1.53](https://github.com/groove-max/ha-eybond-local/releases/tag/v0.1.53), [0.3.0-beta.5](https://github.com/groove-max/ha-eybond-local/releases/tag/v0.3.0-beta.5) | You deliberately need a historical version; newer fixes are absent. |
| Current `main` | Unreleased 0.3 development | A maintainer asks you to check a newer fix, or you deliberately choose development code. |
| Cloud Bridge 0.4 | Separate development | Not part of these installation instructions or the public 0.3 feature set. |

GitHub's **Latest release** points to RC1. It is deliberately published without
the prerelease flag so HACS can offer it in the ordinary release channel. The
name still means release candidate, not final 0.3.0; read its known limitations.

RC1 is not a superset of newer `main` snapshots. Experimental 099E/VPHB telemetry,
new Anenji 4200 controls from #88 and query-only BLE inspection remain on `main`.
If you installed `main` for those features, keep that build rather than replace
it with RC1. The `main` manifest may still say beta.5: report the commit/source
ID as well as the version. These guides can describe newer development features;
check the [release notes](https://github.com/groove-max/ha-eybond-local/releases)
for the scope of your installed version.

## Install with HACS

1. Install HACS separately if you do not already use it.
2. Open HACS, then **Custom repositories** in its menu.
3. Add `https://github.com/groove-max/ha-eybond-local`, category **Integration**.
4. Find **EyeBond Local**, select the intended version and download it. RC1 does
   not require enabling beta versions; older prereleases may need that option.
5. Restart Home Assistant, then **Settings → Devices & Services → Add Integration → EyeBond Local**.

[Open repository in HACS](https://my.home-assistant.io/redirect/hacs_repository/?owner=groove-max&repository=ha-eybond-local&category=integration).
This is a custom repository, not a claim of inclusion in HACS's default list.

## Manual installation

1. Download the ZIP for your chosen release from the links above.
2. Copy only `custom_components/eybond_local/` into your HA configuration's
   `custom_components/` directory.
3. Verify the final path is `config/custom_components/eybond_local/manifest.json`.
4. Fully restart HA, then add the integration.

Do not nest another repository or `custom_components` folder inside it. Keep
backups **outside** `custom_components`: renaming a backup directory there does
not prevent HA from discovering its unchanged manifest.

## Testing main

Back up HA, download the [main ZIP](https://github.com/groove-max/ha-eybond-local/archive/refs/heads/main.zip),
and replace the complete existing `eybond_local` directory with the one from
the archive. Do not mix files from different versions. Fully restart HA;
reloading the entry does not reload every Python module. Do not delete/re-add
devices merely to update the code. Include the tested commit in your report.

After restarting, open **Configure**: **Integration build** shows the version,
the commit when archive metadata is available, and a short `sha256:` source ID.
These are integration identifiers, not the collector's firmware version. The
same identity is in the startup log and Support Archive. GitHub archive/HACS
archive installations carry the commit automatically. A raw file copy or HACS
fallback download can lack it; the source ID still distinguishes the component
files. Local edits change that ID even when the embedded commit stays the same.
The identity is captured at startup: fully restart HA after replacing files.

A manually installed main snapshot does not automatically follow future main
commits. Published HACS updates and main snapshots are different channels.

## Updating and going back

Keep a HA backup and note the previously installed version. Update through HACS
or replace the full directory manually, then restart and check readings.
To go back, reinstall the chosen published version, restart, and restore your
backup if necessary. Replacing code does not itself resolve a learning
protection hold or undo an endpoint change: follow the recovery status shown
by the integration, not a forced reset.

Next: [first connection](GETTING_STARTED.md) and [compatibility](COMPATIBILITY.md).
