# Documentation

This is the public documentation for EyeBond Local.

## Start here

- [Main README](../README.md) — installation, setup, connection settings, troubleshooting, and support flow
- [Collector Management](COLLECTOR_MANAGEMENT.md) — how the collector connects, runtime settings, Wi-Fi change, restart, and proxy capture basics
- [Device Learning](DEVICE_LEARNING.md) — how to discover extra sensors and controls for partially supported devices
- [Support Archive](SUPPORT_ARCHIVE.md) — what to attach when asking for hardware support
- [Collector Proxy Capture](PROXY_CAPTURE.md) — use this only when support asks for a temporary capture
- [Remote / NAT Setup Guide](REMOTE_SETUP.md) — only for collectors outside the normal Home Assistant network
- [Inverter Model Catalog](generated/INVERTER_MODEL_CATALOG.generated.md) — supported and partially supported inverter models
- [ESP EyeBond Collector](https://github.com/groove-max/esp-eybond-collector) — community firmware bridge for inverters without a factory collector (detected and supported automatically; see [Collector Management](COLLECTOR_MANAGEMENT.md#virtual-bridge-collectors))

Ukrainian readers can also use the [Ukrainian README](../README.uk.md).

## For contributors

If you are extending or maintaining the project, use [../CONTRIBUTING.md](../CONTRIBUTING.md).

- [Collector Connection Architecture](CONNECTION_ARCHITECTURE.md) — maintainer reference for the connection axes, session/PN ownership, `callback_on_demand`, endpoint ownership rules, and what must not be reintroduced.
- [Typed Telemetry Migration](TYPED_TELEMETRY.md) — staged replacement of the broad runtime value mapping with a strict measurement contract.
