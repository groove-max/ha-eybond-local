# Collector Proxy Capture

Proxy capture is an advanced support tool. Most users do not need it for normal monitoring or control.

Use it only when a developer asks you to collect a temporary capture from the collector.

## What it is for

Proxy capture records a short communication session so a developer can understand why a collector or inverter behaves differently from expected.

It can help when:

- the collector connects but the inverter is not identified;
- the vendor app or cloud sees data but Home Assistant does not;
- a model needs extra evidence before support can be added;
- a developer asks for a capture in a GitHub issue.

For most problems, start with **Create support archive** first. Proxy capture is the next step only when the archive is not enough.

## Before you start

Make sure:

- the collector has stable Wi-Fi;
- Home Assistant can already reach the collector;
- you are ready to keep the capture short;
- you can check that the vendor app still works afterward if you use one.

If you are not sure, stop and create a Support Archive instead.

## How to start

You can start proxy capture from the collector device page:

1. Open the EyeBond collector device in Home Assistant.
2. Set **Proxy Mode Duration** to the requested number of minutes.
3. Press **Start Traffic Capture**.
4. Reproduce the problem, or follow the developer’s instructions.
5. Press **Stop Traffic Capture**, or wait for the timer to finish.

<p align="center"><img src="images/proxy-capture-settings.png" alt="Collector settings with proxy mode controls" width="520"></p>

You can also open:

1. **Settings → Devices & Services**
2. **EyeBond Local**
3. **Configure**
4. **Diagnostics and service tools**
5. **Collector traffic capture**

This screen is useful when you want to watch the live capture status or download the result immediately.

<p align="center"><img src="images/proxy-capture-running.png" alt="Running proxy capture session with timer and live log" width="720"></p>

## Timer behavior

Proxy capture is temporary.

When the timer ends, Home Assistant stops the capture automatically and tries to restore the collector’s normal connection path.

You can:

- stop the capture early;
- reset the timer if the developer asks for a longer capture;
- change the duration while the capture is running.

Refreshing the live log does not extend the timer.

## Downloading the result

After the capture finishes, the same screen shows a **Saved result** download link.

<p align="center"><img src="images/proxy-capture-result.png" alt="Finished proxy capture session with saved result download" width="720"></p>

Download the ZIP and attach it to the GitHub issue together with a short note about what you did during the capture.

If the developer also asks for a normal Support Archive, create it separately from **Configure → Diagnostics and service tools → Create support archive**.

## Restoring cloud/app access

Normally, EyeBond Local restores the collector automatically after capture.

If the vendor app stops showing live data afterward:

1. Open the collector device page.
2. Press the restore cloud/app access action shown for this collector.
3. Wait a few minutes for the collector to reconnect.
4. Check the vendor app again.

If the restore action is unavailable or the collector still does not recover, do not repeat captures. Create a Support Archive and report the issue.
