---
name: Release Checklist
about: Maintainer checklist for publishing a new GitHub release and keeping HACS metadata consistent.
title: "release: "
labels: maintenance
assignees: groove-max
---

## Version And Metadata

- [ ] Update `CHANGELOG.md` and confirm the release scope is reflected in the target version section.
- [ ] Verify the version in `custom_components/eybond_local/manifest.json`.
- [ ] Verify `hacs.json`, `README.md`, and public docs still reflect current installation/support guidance.
- [ ] Verify `LICENSE` and repository metadata still match the intended publication state.

## Validation

- [ ] Run `python3 tools/quality_gate.py` locally.
- [ ] If generated reports changed, run `python3 tools/quality_gate.py --refresh-generated` and commit the updated files under `docs/generated/`.
- [ ] Confirm GitHub Actions are green for HACS validation, Hassfest, and Quality Gate.

## Release Notes

- [ ] Render release notes from `CHANGELOG.md` with `python3 tools/render_release_notes.py vX.Y.Z --output .local/release-notes/vX.Y.Z.md`.
- [ ] Summarize user-visible changes.
- [ ] Call out any breaking changes or required reconfiguration.
- [ ] Mention newly supported hardware, controls, or diagnostics if applicable.
- [ ] Mention known limitations that still require Support Archive reports.

## GitHub Release

- [ ] Create or verify the Git tag.
- [ ] Publish the GitHub release.
- [ ] Make sure the release title and tag are aligned.
- [ ] Use the rendered release notes file as the GitHub release body.

## Post-Release

- [ ] Confirm the repository still installs as a HACS custom repository.
- [ ] Confirm issue templates and CODEOWNERS still match the current maintainer workflow.
- [ ] Close or retarget any issues that were resolved by the release.