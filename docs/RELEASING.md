# Releasing

This project uses a changelog-first release flow.

The source of truth for release notes is [CHANGELOG.md](../CHANGELOG.md), not the GitHub release form.

## Release Workflow

1. Keep incoming work under `## [Unreleased]` in [CHANGELOG.md](../CHANGELOG.md).
2. When cutting a release, move the relevant notes into a new version section whose number matches [custom_components/eybond_local/manifest.json](../custom_components/eybond_local/manifest.json).
3. Run the public validation gate:

```bash
python3 tools/quality_gate.py
```

4. If public generated docs changed, refresh and commit them before tagging:

```bash
python3 tools/quality_gate.py --refresh-generated
```

5. Render the GitHub release body from the matching changelog section:

```bash
python3 tools/render_release_notes.py v0.1.43 --output .local/release-notes/v0.1.43.md
```

6. Create and push the tag:

```bash
git tag v0.1.43
git push origin v0.1.43
```

7. Publish the GitHub release from the rendered notes:

```bash
gh release create v0.1.43 \
  --title "v0.1.43" \
  --notes-file .local/release-notes/v0.1.43.md
```

## Writing Good Release Notes

- Keep release notes user-facing.
- Prefer grouped bullets under `Added`, `Changed`, `Fixed`, and `Docs`.
- Mention breaking changes or required reconfiguration explicitly.
- Call out newly supported hardware, controls, diagnostics, or important workflow changes.
- Keep purely internal refactors out unless they affect users, maintainers, or release safety.

## Maintainer Checklist

The maintainer checklist lives in [.github/ISSUE_TEMPLATE/release_checklist.md](../.github/ISSUE_TEMPLATE/release_checklist.md).