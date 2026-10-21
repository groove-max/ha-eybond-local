"""Memory-blocked learning has actionable localized UI, not protocol guesses."""
from unittest.mock import AsyncMock

import pytest

from test_ha_config_flow import collector_entry
from test_ha_cloud_learning_dispatch import dispatch_context
from test_ha_protected_learning_retry import retry_context


@pytest.mark.parametrize(("language", "phrase"), [
    ("en", "available RAM"), ("ru", "доступной RAM"), ("uk", "доступної RAM"),
])
@pytest.mark.parametrize("held", [False, True])
async def test_memory_failure_details_use_real_translation_bundle(hass, collector_entry, language, phrase, held):
    from custom_components.eybond_local.options_flow import EybondLocalOptionsFlow
    from custom_components.eybond_local.flows.common.translation import load_translation_bundle

    flow = EybondLocalOptionsFlow(collector_entry)
    flow.hass = hass
    flow._flow_language = lambda: language
    flow._translation_bundle_language = language
    flow._translation_bundle = await hass.async_add_executor_job(load_translation_bundle, language)
    reason = flow._control_discovery_failure_reason(RuntimeError(
        "shadow_learning_preflight_blocked:insufficient_memory:184MiB"
    ))
    flow._shadow_learning_state = {"discovery": {
        "status": "error", "reason": "cloud_learning_protection_held" if held else reason,
        "diagnostics": {"primary_failure_reason": reason,
                        "memory": {"available_mib": 184, "required_mib": 400}},
    }, "protocol_diagnostics": {"stale": True}}
    message = flow._control_discovery_error_detail()
    assert phrase in message
    assert "184 MiB" in message and "400 MiB" in message
    # Persisted malformed diagnostics are not rendered or allowed to crash UI.
    flow._shadow_learning_state["discovery"]["diagnostics"] = "private-secret"
    assert "private-secret" not in flow._control_discovery_error_detail()


async def test_low_memory_protected_retry_preserves_original_hold(retry_context, monkeypatch):
    from custom_components.eybond_local.runtime.coordinator import learning_retry

    ctx = retry_context
    c = ctx.coordinator
    monkeypatch.setattr(learning_retry, "read_available_memory_mib", lambda: 184)
    prove = AsyncMock()
    monkeypatch.setattr(learning_retry, "_prove_local_route", prove)
    with pytest.raises(RuntimeError, match="shadow_learning_preflight_blocked:insufficient_memory:184MiB"):
        await learning_retry.async_start_protected_retry(c, cloud_source="smartess", confirm_cloud_outage=True)
    prove.assert_not_awaited()
    assert await c._async_cloud_learning_hold_reason()
