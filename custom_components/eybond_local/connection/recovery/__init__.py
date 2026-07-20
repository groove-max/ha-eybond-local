"""Neutral recovery execution authority.

The controlled reset/restart/reconnect recovery engine, its verifiers, the
observed-session restart channel, and the recovery-contract terminal merge live
here -- in the neutral ``connection`` layer, NOT in ``onboarding``. Config-flow /
onboarding UI, runtime, and strategy-transition all consume this package; it
imports nothing from ``config_flow``, ``runtime`` or ``onboarding``.
"""
