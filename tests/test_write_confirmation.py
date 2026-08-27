from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.write_confirmation import (  # noqa: E402
    CapabilityWriteConfirmation,
    WRITE_CONFIRMATION_DIAGNOSTIC_KEY,
    load_write_confirmation,
    start_write_confirmation,
    store_write_confirmation,
    write_confirmation_diagnostics,
)


class CapabilityWriteConfirmationTests(unittest.TestCase):
    def test_typed_trace_records_mismatch_then_observed_value(self) -> None:
        state: dict[str, object] = {}
        trace = start_write_confirmation(
            state,
            capability_key="secondary_output_priority",
            value_key="secondary_output_priority",
            requested_value="SBU",
            expected_value="SBU",
            requested_words=(2,),
        ).with_immediate_observation(
            value="OFF",
            words=(0,),
            matched=False,
        )
        store_write_confirmation(state, trace)
        trace = trace.with_poll_observation(value="OFF", matched=False)
        trace = trace.with_poll_observation(value="SBU", matched=True)
        store_write_confirmation(state, trace)

        diagnostic = write_confirmation_diagnostics(state)[
            WRITE_CONFIRMATION_DIAGNOSTIC_KEY
        ]
        self.assertEqual(diagnostic["first_full_poll_value"], "OFF")
        self.assertEqual(diagnostic["latest_full_poll_value"], "SBU")
        self.assertEqual(diagnostic["full_poll_observation_count"], 2)
        self.assertEqual(
            diagnostic["convergence"],
            "requested_value_observed_after_mismatch",
        )

    def test_trace_freezes_after_requested_value_is_observed(self) -> None:
        trace = CapabilityWriteConfirmation(
            capability_key="secondary_output_priority",
            value_key="secondary_output_priority",
            requested_value="SBU",
            expected_value="SBU",
            requested_words=(2,),
        ).with_poll_observation(value="SBU", matched=True)

        after_external_change = trace.with_poll_observation(value="OFF", matched=False)

        self.assertIs(after_external_change, trace)
        self.assertEqual(after_external_change.latest_poll_value, "SBU")
        self.assertEqual(after_external_change.poll_observation_count, 1)

    def test_unavailable_immediate_read_uses_bounded_error_marker(self) -> None:
        trace = CapabilityWriteConfirmation(
            capability_key="secondary_output_priority",
            value_key="secondary_output_priority",
            requested_value="SBU",
            expected_value="SBU",
            requested_words=(2,),
        ).with_immediate_unavailable(
            error=RuntimeError("request_timeout:192.0.2.1:secret"),
        )

        self.assertEqual(trace.immediate_status, "unavailable")
        self.assertEqual(trace.immediate_error, "RuntimeError:request_timeout")
        self.assertNotIn("192.0.2.1", str(trace.diagnostics()))

    def test_load_fails_closed_on_duck_or_malformed_state(self) -> None:
        valid = CapabilityWriteConfirmation(
            capability_key="secondary_output_priority",
            value_key="secondary_output_priority",
            requested_value="SBU",
            expected_value="SBU",
            requested_words=(2,),
        )

        class Subclass(CapabilityWriteConfirmation):
            pass

        for malformed in (object(), {"capability_key": "secondary_output_priority"}, Subclass(
            capability_key="secondary_output_priority",
            value_key="secondary_output_priority",
            requested_value="SBU",
            expected_value="SBU",
            requested_words=(2,),
        )):
            with self.subTest(malformed=type(malformed).__name__):
                state = {"capability_write_confirmation": malformed}
                self.assertIsNone(load_write_confirmation(state))
                self.assertEqual(write_confirmation_diagnostics(state), {})

        state = {"capability_write_confirmation": valid}
        self.assertIs(load_write_confirmation(state), valid)

    def test_constructor_rejects_untrusted_shapes(self) -> None:
        base = {
            "capability_key": "secondary_output_priority",
            "value_key": "secondary_output_priority",
            "requested_value": "SBU",
            "expected_value": "SBU",
            "requested_words": (2,),
        }
        invalid_cases = (
            {**base, "capability_key": " padded "},
            {**base, "requested_words": [2]},
            {**base, "requested_words": (True,)},
            {**base, "requested_value": object()},
            {**base, "poll_observation_count": True},
        )
        for values in invalid_cases:
            with self.subTest(values=values):
                with self.assertRaises((TypeError, ValueError)):
                    CapabilityWriteConfirmation(**values)


if __name__ == "__main__":
    unittest.main()
