"""Backward-compatible re-export of the neutral polling contract.

The polling contract (``PollPolicy`` + the neutral ``DEFAULT_POLL_POLICY``) lives
in the provider/driver-agnostic top-level :mod:`..poll_policy` module. Concrete
driver-specific policies are NOT re-exported here: they live in their driver
modules and are declared via ``InverterDriver.poll_policy`` / ``poll_policy_for``.

This shim only keeps existing ``runtime.poll_policy`` imports of the neutral
contract working.
"""

from __future__ import annotations

from ..poll_policy import DEFAULT_POLL_POLICY, PollPolicy

__all__ = ["DEFAULT_POLL_POLICY", "PollPolicy"]
