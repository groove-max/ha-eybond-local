# Releasing

This project uses a changelog-first release flow.

The source of truth for release notes is [CHANGELOG.md](../../CHANGELOG.md), not the GitHub release form.

## Publishing code for manual testing without a release

Use this path when real-device feedback is needed before a public release:

1. Assign the next candidate version in
   [manifest.json](../../custom_components/eybond_local/manifest.json), while
   keeping its notes under `Unreleased` in the changelog.
2. Run the quality gate and the current real-Home-Assistant lane, then deploy
   that exact tree to the maintainer's test Home Assistant and verify entry
   setup, live entities, and logs.
3. Push the tested commit to `main`, but do not create or push a version tag and
   do not create a GitHub release.
4. Direct testers to the `main` branch instructions in the public README. Ask
   them to include the exact commit and a new Support Archive with feedback.

HACS release users do not receive this build automatically. Before the actual
release, rerun the complete release workflow below from the final tree.

## Release Workflow

1. Keep incoming work under `## [Unreleased]` in [CHANGELOG.md](../../CHANGELOG.md)
   while the next target version is not assigned yet.
2. When cutting a release, decide the target version and verify
   [custom_components/eybond_local/manifest.json](../../custom_components/eybond_local/manifest.json)
   uses the same version.
   - If that version has not been published yet, it is OK to keep adding release
     notes to the existing version section.
   - If that version has already been published, bump the manifest and move the
     relevant notes into a new changelog section.
3. Update the changelog section date to the actual release date.
4. Refresh the public generated catalog without running the test suite:

```bash
python3 tools/model_catalog.py \
  --format markdown \
  --output docs/generated/INVERTER_MODEL_CATALOG.generated.md
```

5. Run the release validation with both supported real-Home-Assistant lanes:

```bash
python3 tools/validate.py release \
  --ha-lane 2026.2=/path/to/ha-2026.2/bin/python \
  --ha-lane 2026.9=/path/to/ha-2026.9/bin/python
```

   This first requires a populated local fixture catalog and a blocker-free
   release-readiness replay, then runs the complete quality gate once, including
   the public-documentation link/index check. Commit any refreshed file under
   `docs/generated/` before tagging.
6. Push the prepared commit or branch and confirm the HACS Validation and
   Hassfest jobs pass. These online checks validate the repository as HACS and
   Home Assistant currently see it.
7. Render the GitHub release body from the matching changelog section:

```bash
python3 tools/render_release_notes.py vX.Y.Z --output .local/release-notes/vX.Y.Z.md
```

8. Create and push the tag:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

9. Publish the GitHub release from the rendered notes:

```bash
gh release create vX.Y.Z \
  --title "vX.Y.Z" \
  --notes-file .local/release-notes/vX.Y.Z.md
```

Replace `X.Y.Z` with the same version used in the manifest and changelog.

## Build identity in development and release archives

`custom_components/eybond_local/BUILD_INFO.txt` is a committed template marked
`export-subst` in `.gitattributes`. Archive a **commit or tag**, not a bare Git
tree: Git expands the full SHA and commit date without a self-referential version
commit or a bot modifying main after each push. GitHub source archives use
`git archive`; HACS normally downloads and extracts that archive. Its file-by-file
fallback can leave the template unexpanded. See [Git archive attributes](https://git-scm.com/docs/gitattributes#_creating_an_archive)
and [GitHub archive guarantees](https://docs.github.com/en/repositories/working-with-files/using-files/downloading-source-code-archives).

The integration also computes `sha256-source-v1` from sorted relative paths and
bytes of `.py`, `.json`, `.yaml`, `.yml` files, including the manifest and profiles.
Build metadata, bytecode and hidden/cache directories are excluded. No Git binary,
network call or user-specific HA configuration is required. Fingerprinting runs
in an executor and is retained for the process lifetime; restart after updates.
Missing/invalid metadata is not guessed. Fingerprint read/size failures report
unavailable diagnostics rather than preventing integration setup.

The commit is a declaration, never a clean-tree guarantee. To compare a reported
fingerprint, run `python3 tools/build_identity.py --path /path/to/eybond_local`
against the same component tree. An archive and its source checkout have the
same fingerprint; local source changes differ even if commit metadata remains.
Version and fingerprint still identify raw-copy installations without a commit.
The settings screen/log use short IDs; Support Archives retain the full hashes.

Archive audits must expect the one substituted metadata file to differ from its
Git blob; verify its SHA against the archived commit, not by accepting arbitrary
differences. Do not carry an old deployment's BUILD_INFO file into a new build.

## Writing Good Release Notes

- Keep release notes user-facing.
- Prefer grouped bullets under `Added`, `Changed`, `Fixed`, and `Docs`.
- Mention breaking changes or required reconfiguration explicitly.
- Call out newly supported hardware, controls, diagnostics, or important workflow changes.
- Keep purely internal refactors out unless they affect users, maintainers, or release safety.

## Maintainer Checklist

The maintainer checklist lives in [.github/ISSUE_TEMPLATE/release_checklist.md](../../.github/ISSUE_TEMPLATE/release_checklist.md).
