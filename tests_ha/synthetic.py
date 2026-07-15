"""Synthetic identifiers for the real-Home-Assistant suite.

HARD RULE: only synthetic values here. `tests/test_no_real_identifiers.py`
scans every tracked file, so any PN-shaped token must already be on that
allowlist. Never paste a value from a real installation.
"""

from __future__ import annotations

# On the project's synthetic PN allowlist (tests/test_no_real_identifiers.py).
SYNTHETIC_COLLECTOR_PN = "E5000099990001"
SYNTHETIC_OTHER_COLLECTOR_PN = "E5000099990002"

# RFC 5737 TEST-NET-1: reserved for documentation, never routable.
SYNTHETIC_SERVER_IP = "192.0.2.10"
SYNTHETIC_COLLECTOR_IP = "192.0.2.55"
SYNTHETIC_BROADCAST = "192.0.2.255"
SYNTHETIC_NETWORK = "192.0.2.0/24"
