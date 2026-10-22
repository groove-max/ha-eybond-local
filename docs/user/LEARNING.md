# Shadow Learning: add missing inverter controls

[Українська](LEARNING.uk.md)

If local readings work but a setting is missing, **Write Controls** learning can
help identify the commands used by the cloud app and create locally confirmed
controls for this device. Availability depends on the profile, transport and
cloud provider. It is not required for ordinary local monitoring.

## Choose the right task

| Task | Result |
|---|---|
| Read-only / Analyze device data | Cloud metadata and evidence for diagnosis. Does not automatically add entities or controls. |
| Write Controls / Verify additional local controls | Protected active verification; eligible, locally confirmed items can be applied to the device. |

For a missing cloud setting, choose active verification when offered; read-only
analysis is not a required preliminary step. A captured command is not proof
of every possible value, all settings or compatibility with another inverter.

## Before active learning

- Confirm stable local readings and sufficient HA memory. The current test
  workflow requires 400 MiB of available RAM, not merely total installed RAM.
- Close the vendor app and avoid concurrent changes from another integration.
- Accept the endpoint change and possible interruption of cloud access.
- Keep a way to check the inverter and recover its connection if needed.
- Do not run the test while interruption would put critical loads at risk.

The intended test writes are intercepted by the protected workflow rather than
forwarded as ordinary cloud control. Do not bypass its readiness checks or
assume that every failure is safe to retry immediately.

## Run and review

Open **Configure → Expand device support** and select active control
verification. Names can vary by version/language. Choose the offered cloud
source, read the notices and sign in for this session. The password is not
saved. Some accounts work through both SmartESS and DESSMonitor; use only a
source the wizard offers and confirms for the device.

Review the result before applying additions. Learned controls are local to this
HA device until their evidence is reviewed for the built-in catalog.

## If the cloud uses another protocol

Some inverters respond to PI30 locally while the cloud uses Modbus. A confirmed
compatibility check may offer another profile, for example 0925. It does not
switch automatically, translate arbitrary commands, or prove that all writes
work. Compare the candidate readings before choosing it; stay on the existing
profile if no compatible map is confirmed. Learning must be supported by the
chosen profile separately from telemetry.

## Recovery and protected retry

In current 0.3 test code, automatic restoration requires verified completion
for every sent test command. If any result is uncertain, the collector can
remain local, **even after a successful scan or HA restart**. Local telemetry
and supported controls remain available, but vendor-cloud access may stay off
pending reviewed recovery. This is not Cloud Bridge.

Use a protected retry only when the wizard offers it. Do not delete recovery
files, force a cloud endpoint, or repeatedly restart to remove the hold.
If preparation fails, retain the status and make one [Support Archive](SUPPORT_ARCHIVE.md).
The [detailed learning reference](DEVICE_LEARNING.md) covers provider/transport
limitations and the exact recovery states. Published beta.5 may predate newer
workflow fixes: [check the installed channel](INSTALLATION.md).
