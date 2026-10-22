# Connect your inverter to Home Assistant

[Українська](GETTING_STARTED.uk.md)

[Install EyeBond Local](INSTALLATION.md), connect the collector to its Wi-Fi
network and keep its inverter cable/settings unchanged for the first test.

## Find and add the collector

1. Open **Settings → Devices & Services → Add Integration → EyeBond Local**.
2. Choose the HA network interface that can reach the collector and start a scan.
3. Select an identified collector. If its address needs confirmation, follow
   that step rather than repeatedly creating new setup flows.
4. Add the collector and allow runtime detection to identify the inverter.

![Collector discovery](../images/setup-03-detected-devices.png)

The collector and inverter normally become separate HA devices. A collector
being online does not yet prove that the inverter is speaking a supported
protocol. Give detection time and inspect its status before changing drivers.

## If the scan finds nothing

Check the chosen interface and collector Wi-Fi. For another routed subnet,
use **Add known collector IPs to scan** or manual setup: broadcasts normally do
not cross routers. In manual setup, the advertised callback address must be
reachable **from the collector**, not merely from your browser.

UDP discovery normally uses port 58899; the collector then opens a TCP callback
to HA's listener, normally 8899. A successful UDP response alone does not prove
the TCP return path or identity handshake. Do not expose ports to the Internet
just to make a local scan work; use a VPN for remote setups where practical.

[Detailed discovery](SETUP_AND_DISCOVERY.md) · [Routed / NAT networks](REMOTE_SETUP.md)

## Check readings before controls

Compare battery voltage, PV and load with the inverter display. Leave automatic
polling enabled initially. A stale or unavailable reading needs diagnosis,
not a faster polling interval.

- **Read-only:** no local writes.
- **Auto:** verified controls for a sufficiently confident match.
- **Full Control:** available controls including untested ones; not a guarantee
  of support or permission to try arbitrary electrical settings.

Change only values you understand and verify the inverter's read-back. If a
setting is missing, [learning](LEARNING.md) may help; you do not need to enable
every entity or run learning on a working installation.

## Choose a connection mode deliberately

**Cloud + Home Assistant** keeps the vendor endpoint and adds a local connection
on compatible collectors. **Home Assistant only** explicitly changes the
collector endpoint and stops vendor-cloud access. Review the restoration option
before switching. Cloud Bridge is separate 0.4 development, not the same mode.

Normal local monitoring needs no cloud password. Optional cloud analysis and
learning ask for credentials separately. See [collector management](COLLECTOR_MANAGEMENT.md).

## Next steps

- [Troubleshooting](TROUBLESHOOTING.md)
- [Runtime sensors and control modes](RUNTIME_AND_INVERTER.md)
- [Optional dashboard card](https://github.com/groove-max/ha-eybond-local-card)
- [Getting useful support](HELP.md)
