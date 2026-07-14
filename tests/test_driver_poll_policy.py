"""Driver-owned polling policy contract (Phase-1 review).

Proves the polling policy is declared BY the driver (not by a runtime catalog),
that a model-dependent driver actually receives the detected inverter, and that
the neutral resolver forwards the inverter through to the driver.
"""

from __future__ import annotations

from pathlib import Path
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.drivers import registry
from custom_components.eybond_local.drivers.base import InverterDriver
from custom_components.eybond_local.drivers.pi30 import PI30_POLL_POLICY, Pi30Driver
from custom_components.eybond_local.drivers.smg import SMG_MODBUS_POLL_POLICY
from custom_components.eybond_local.poll_policy import DEFAULT_POLL_POLICY, PollPolicy


class _FixedDriver(InverterDriver):
    key = "fixed_test_driver"
    name = "Fixed"
    probe_targets = ()
    measurements = ()
    poll_policy = PollPolicy(min_auto_interval=7.0, max_auto_interval=70.0)

    async def async_probe(self, transport, target):
        return None

    async def async_read_values(self, transport, inverter, **kwargs):
        return {}

    async def async_write_capability(self, transport, inverter, capability_key, value):
        return None


class _ModelAwareDriver(InverterDriver):
    key = "model_aware_test_driver"
    name = "ModelAware"
    probe_targets = ()
    measurements = ()
    _FAST = PollPolicy(min_auto_interval=3.0, max_auto_interval=30.0)
    _SLOW = PollPolicy(min_auto_interval=30.0, max_auto_interval=300.0)

    def poll_policy_for(self, inverter=None):
        if str(getattr(inverter, "variant_key", "") or "") == "fast":
            return self._FAST
        return self._SLOW

    async def async_probe(self, transport, target):
        return None

    async def async_read_values(self, transport, inverter, **kwargs):
        return {}

    async def async_write_capability(self, transport, inverter, capability_key, value):
        return None


class DriverPollPolicyContractTests(unittest.TestCase):
    def test_base_returns_declared_class_attribute(self) -> None:
        driver = _FixedDriver()
        # Default contract: the method returns the declared class attribute.
        self.assertIs(driver.poll_policy_for(), driver.poll_policy)
        self.assertEqual(driver.poll_policy_for().min_auto_interval, 7.0)
        # A driver with no declared policy inherits the neutral default.

        class _Bare(_FixedDriver):
            key = "bare_test_driver"
            poll_policy = DEFAULT_POLL_POLICY

        self.assertIs(_Bare().poll_policy_for(), DEFAULT_POLL_POLICY)

    def test_model_specific_driver_receives_inverter(self) -> None:
        driver = _ModelAwareDriver()
        fast = types.SimpleNamespace(variant_key="fast")
        slow = types.SimpleNamespace(variant_key="default")
        self.assertEqual(driver.poll_policy_for(fast).min_auto_interval, 3.0)
        self.assertEqual(driver.poll_policy_for(slow).min_auto_interval, 30.0)
        # No inverter => the model-agnostic branch.
        self.assertEqual(driver.poll_policy_for(None).min_auto_interval, 30.0)

    def test_resolver_forwards_inverter_to_driver(self) -> None:
        # The registry resolver must pass the detected inverter through to the
        # driver's poll_policy_for (proves the arg reaches the driver end-to-end).
        received = {}

        class _Recorder:
            key = "recorder_test_driver"

            def poll_policy_for(self, inverter=None):
                received["inverter"] = inverter
                return DEFAULT_POLL_POLICY

        original = registry.get_driver
        registry.get_driver = lambda key: (
            _Recorder() if key == "recorder_test_driver" else original(key)
        )
        try:
            sentinel = object()
            registry.poll_policy_for_driver_key("recorder_test_driver", inverter=sentinel)
        finally:
            registry.get_driver = original
        self.assertIs(received.get("inverter"), sentinel)

    def test_resolver_forwards_inverter_selecting_model_policy(self) -> None:
        # End-to-end through the real resolver with a model-aware driver.
        original = registry.get_driver
        driver = _ModelAwareDriver()
        registry.get_driver = lambda key: (
            driver if key == driver.key else original(key)
        )
        try:
            fast = types.SimpleNamespace(variant_key="fast")
            policy = registry.poll_policy_for_driver_key(driver.key, inverter=fast)
            self.assertEqual(policy.min_auto_interval, 3.0)
            slow_policy = registry.poll_policy_for_driver_key(driver.key, inverter=None)
            self.assertEqual(slow_policy.min_auto_interval, 30.0)
        finally:
            registry.get_driver = original

    def test_concrete_driver_policies_live_in_driver_modules(self) -> None:
        # The concrete driver policies are owned by their driver modules and the
        # neutral contract does not export them.
        self.assertIs(Pi30Driver().poll_policy_for(), PI30_POLL_POLICY)
        self.assertEqual(PI30_POLL_POLICY.min_auto_interval, 2.0)
        self.assertEqual(SMG_MODBUS_POLL_POLICY.min_auto_interval, 3.0)

        import custom_components.eybond_local.poll_policy as neutral

        for forbidden in (
            "PI30_POLL_POLICY",
            "SMG_MODBUS_POLL_POLICY",
            "FAST_MODBUS_POLL_POLICY",
            "ASCII_POLL_POLICY",
        ):
            self.assertFalse(
                hasattr(neutral, forbidden),
                f"neutral poll_policy must not export {forbidden}",
            )


class VariantSerialStabilityTests(unittest.TestCase):
    """The 'family has no stable serial' rule is driver/model policy, not runtime.

    The registry only DISPATCHES to the owning driver; it holds no variant set.
    """

    def test_smartess_0925_family_has_no_stable_serial(self) -> None:
        inverter = types.SimpleNamespace(variant_key="smartess_0925")
        self.assertFalse(registry.serial_is_stable("smartess_local", inverter))

    def test_non_0925_smartess_variant_has_stable_serial(self) -> None:
        inverter = types.SimpleNamespace(variant_key="smartess_0912")
        self.assertTrue(registry.serial_is_stable("smartess_local", inverter))

    def test_other_drivers_default_stable(self) -> None:
        for driver_key in ("modbus_smg", "pi30", "", "unknown_driver"):
            self.assertTrue(
                registry.serial_is_stable(driver_key, types.SimpleNamespace(variant_key=""))
            )

    def test_registry_holds_no_variant_literal(self) -> None:
        import inspect

        self.assertNotIn("smartess_0925", inspect.getsource(registry))


if __name__ == "__main__":
    unittest.main()
