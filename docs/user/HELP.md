# Get help with your inverter

[Українська](HELP.uk.md)

Use an existing issue for the same device/problem when possible. Otherwise
[open a support issue](https://github.com/groove-max/ha-eybond-local/issues/new/choose).
Both English and Ukrainian reports are welcome.

## Include useful evidence

- Inverter's retail model, collector model and firmware if known.
- Home Assistant version, integration version and release/beta/main channel.
- Main commit if installed manually; the manifest version alone may be unchanged.
- What works, what fails, and whether the vendor app still updates.
- Time and time zone for an outage or screenshot.
- A fresh **Support Archive** from **Configure → Diagnostics and service tools → Create support archive**.

If setup has not completed, include its exact error and available diagnostics;
do not invent a device entry just to create an archive. For a missing setting,
include its cloud name and allowed choices/range. Do not guess register addresses.

## Privacy and access

Review archives and screenshots before uploading. Redaction reduces exposure
but is not permission to post credentials or personal data. Never publish
cloud passwords, HA access tokens or remote-login details in an issue.
Remote access is optional and must be arranged privately and explicitly.

## Which tool do I need?

Start with the archive. Read-only analysis can add cloud evidence, while active
[Write Controls](LEARNING.md) is a different workflow with connection and
recovery precautions. Run [proxy capture](PROXY_CAPTURE.md) or
[diagnostic commands](DIAGNOSTIC_COMMANDS.md) only when support requests them.
Do not repeatedly reproduce a disruptive failure just to obtain more logs.

See the [full Support Archive reference](SUPPORT_ARCHIVE.md) for export options
and what the archive contains.
