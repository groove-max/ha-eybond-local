# Releasing

This project uses a changelog-first release flow.

The source of truth for release notes is [CHANGELOG.md](../../CHANGELOG.md), not the GitHub release form.

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
  --ha-lane 2026.7=/path/to/ha-2026.7/bin/python
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

## Writing Good Release Notes

- Keep release notes user-facing.
- Prefer grouped bullets under `Added`, `Changed`, `Fixed`, and `Docs`.
- Mention breaking changes or required reconfiguration explicitly.
- Call out newly supported hardware, controls, diagnostics, or important workflow changes.
- Keep purely internal refactors out unless they affect users, maintainers, or release safety.

## Maintainer Checklist

The maintainer checklist lives in [.github/ISSUE_TEMPLATE/release_checklist.md](../../.github/ISSUE_TEMPLATE/release_checklist.md).
