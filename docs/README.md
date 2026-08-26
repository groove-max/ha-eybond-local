# Documentation

All EyeBond Local documentation is publicly readable, but it serves two
different audiences. Use the first section for installation and day-to-day
operation. The maintainer references describe implementation boundaries and
release work; they are not required to use the integration.

## User guides

- [Main README](../README.md) — installation, setup, connection settings, troubleshooting, and support flow
- [Testing the unreleased main branch](../README.md#testing-the-unreleased-main-branch) — manual installation of a maintainer-requested test build
- [Setup and Discovery](user/SETUP_AND_DISCOVERY.md) — collector-first setup, scan results, address confirmation, background discovery, and manual setup
- [Runtime Detection and Entities](user/RUNTIME_AND_INVERTER.md) — driver detection, Fast and Full protocol checks, polling, controls, and entity availability
- [Collector Management](user/COLLECTOR_MANAGEMENT.md) — cloud connection profiles, Wi-Fi, restart, UART, virtual bridges, and proxy-capture basics
- [Device Learning](user/DEVICE_LEARNING.md) — read-only cloud evidence and active verification of extra sensors and controls
- [Diagnostic Commands](user/DIAGNOSTIC_COMMANDS.md) — advanced, developer-provided read/write scenarios and shareable results
- [Support Archive](user/SUPPORT_ARCHIVE.md) — what to attach when asking for hardware support
- [Collector Proxy Capture](user/PROXY_CAPTURE.md) — use this only when support asks for a temporary capture
- [Remote / NAT Setup Guide](user/REMOTE_SETUP.md) — only for collectors outside the normal Home Assistant network
- [Inverter Model Catalog](generated/INVERTER_MODEL_CATALOG.generated.md) — supported and partially supported inverter models
- [Interface Screenshot Guide](user/INTERFACE_SCREENSHOTS.md) — examples from current and earlier interfaces, with notes about controls that moved
- [ESP EyeBond Collector](https://github.com/groove-max/esp-eybond-collector) — community firmware bridge for inverters without a factory collector (detected and supported automatically; see [Collector Management](user/COLLECTOR_MANAGEMENT.md#virtual-bridge-collectors))

Ukrainian readers can also use the [Ukrainian README](../README.uk.md).

## Contributing and maintenance

If you are extending or maintaining the project, use [../CONTRIBUTING.md](../CONTRIBUTING.md).

- [Adding Drivers](maintainer/ADDING_DRIVERS.md) — driver structure, registration, tests, and documentation updates
- [Validation](maintainer/VALIDATION.md) — focused tests, quality gate, and Home Assistant compatibility lanes
- [Releasing](maintainer/RELEASING.md) — maintainer-only release preparation and publication checklist
- [Issue Triage](maintainer/ISSUE_TRIAGE.md) — manual status labels, evidence requests, retest handling, and closing rules
- [Graphify](maintainer/GRAPHIFY.md) — regenerating and querying the local architecture knowledge graph
- [Graphify Architecture Audit](maintainer/GRAPHIFY_ARCHITECTURE_AUDIT.md) — findings and follow-up work from the current graph snapshot

## Architecture and design references

- [Collector Connection Architecture](architecture/CONNECTION_ARCHITECTURE.md) — maintainer reference for the connection axes, session/PN ownership, `callback_on_demand`, endpoint ownership rules, and what must not be reintroduced.
- [Cloud Learning Architecture](architecture/CLOUD_LEARNING_ARCHITECTURE.md) — provider-neutral learning methods, typed API sources, trust boundaries, and read-only metadata evidence.
- [Typed Telemetry Migration](architecture/TYPED_TELEMETRY.md) — staged replacement of the broad runtime value mapping with a strict measurement contract.
