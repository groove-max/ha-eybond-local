# Graphify workflow

Graphify is a local architecture-navigation aid for this repository. Its
generated artifacts live in `graphify-out/`, are excluded from Git, and may
describe an uncommitted working tree. They are not release artifacts or a
substitute for source inspection and tests.

## Corpus

`.graphifyignore` is the source of truth. The graph intentionally includes:

- production source under `custom_components/eybond_local/`;
- selected architecture, ownership, recovery, and lifecycle tests;
- real Home Assistant lifecycle tests in `tests_ha/`;
- handwritten architecture and operational documentation;
- project tooling when it participates in executable workflows.

It intentionally excludes generated catalogs, localization copies, fixtures,
binary assets, Graphify/Codex instructions, GitHub process templates, and most
behavioral tests. This keeps inferred relationships focused on runtime
authority rather than repeated prose or test payloads.

## Installation

The project is validated with Graphify 0.9.46 or newer:

```bash
uv tool install --upgrade graphifyy
graphify install --platform codex
graphify --version
```

The tracked `.codex/skills/graphify/` copy makes the workflow available from a
fresh checkout. When upgrading Graphify, update that copy from the same package
and commit the package-supplied delta separately from generated graph output.

## Refresh policy

Refresh after an architectural batch, before using the graph for a broad
review, or whenever a query exposes a stale symbol:

```bash
graphify update .
```

Use the destructive-refresh guard override after deleted or relocated code,
large refactors, or a corpus change:

```bash
graphify update . --force
```

`update` refreshes code structurally without an LLM. Documentation changes are
semantic input and require a deliberate full extraction. Do not run a full
semantic rebuild merely because installation or release prose changed.

For a deliberate full rebuild, select and configure the LLM backend first, then
run:

```bash
graphify extract . --force --backend <backend>
```

Do not install Graphify Git hooks for this repository. They make commits and
checkouts unexpectedly rewrite a large local artifact and do not match the
batch-oriented development workflow.

## Validation

After a refresh:

```bash
graphify check-update .
graphify diagnose multigraph --json
graphify benchmark graphify-out/graph.json
graphify god-nodes --top 15
```

The multigraph diagnostic reports information loss caused by collapsing
parallel relationships. It is evidence to interpret, not an automatic failure:
external-import dangling edges and repeated source relationships can be valid.

Before relying on a result, run at least one project-specific smoke query:

```bash
graphify query "How does collector admission hand off an observed session to runtime ownership?" --budget 1200
graphify path "CollectorAdmissionTransaction" "EybondLocalCoordinator"
```

Treat inferred edges as hypotheses. Confirm security, ownership, persistence,
wire-authority, and lifecycle conclusions in the source and load-bearing tests.
