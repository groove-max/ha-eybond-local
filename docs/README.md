# Documentation

This directory is the public documentation set for EyeBond Local.

## Start Here

- [Main README](../README.md) — installation, setup, user-facing modes, troubleshooting, and support flow
- [Collector Management](COLLECTOR_MANAGEMENT.md) — collector mode, runtime settings, Wi-Fi change, restart, and proxy capture basics
- [Collector Proxy Capture](PROXY_CAPTURE.md) — when to use proxy mode, how to start it, how the timer works, and how to restore the original server if needed
- [Remote / NAT Setup Guide](REMOTE_SETUP.md) — only for collectors that are remote, routed through VPN, or behind port forwarding
- [SMG Support Matrix](SMG_SUPPORT_MATRIX.md) — deeper SMG-family compatibility notes
- [ESP EyeBond Collector](https://github.com/groove-max/esp-eybond-collector) — community firmware bridge for inverters without a factory collector (detected and supported automatically; see [Collector Management](COLLECTOR_MANAGEMENT.md#virtual-bridge-collectors-esp-eybond-collector))

Ukrainian readers can also use the [Ukrainian README](../README.uk.md).

## Generated Status Docs

Generated reports live under [generated/](generated/) and are refreshed by the quality gate:

- [Support Overview](generated/SUPPORT_OVERVIEW.generated.md) — implementation-level coverage for shipped runtime profiles; not a commercial device matrix
- [Generated SMG Support Matrix](generated/SMG_SUPPORT_MATRIX.generated.md) — current declarative support snapshot

## Public Vs Maintainer-Only Material

The files under `docs/` are the public user and contributor docs.

Maintainer-only local notes, private utilities, and release scratch material live under `.local/` and are intentionally outside this public docs surface.

If you are extending or maintaining the project, use [../CONTRIBUTING.md](../CONTRIBUTING.md) for the contributor workflow.
