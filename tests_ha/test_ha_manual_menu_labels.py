"""Manual result labels survive missing frontend translations in real HA.

These presentation-only tests do not open a collector session. A mapping of
action IDs to explicit labels is HA's supported alternative to frontend lookup;
the repair suite separately exercises selection through HA's flow manager.
"""

from copy import deepcopy
import json
from unittest.mock import patch

import pytest
from homeassistant.data_entry_flow import FlowResultType

from custom_components.eybond_local.config_flow import EybondLocalConfigFlow
from custom_components.eybond_local.connection.callback_identity import (
    SilentSessionBootstrapOffer,
)
from custom_components.eybond_local.flows.common.translation import (
    load_translation_bundle,
)
from custom_components.eybond_local.models import OnboardingResult
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_SERVER_IP


ACTIONS = (
    "manual_bootstrap_framed", "manual_bootstrap_at",
    "manual_probe_again", "manual_edit_settings",
)
STEPS = ("manual_confirm", "reconfigure_confirm")
LOADER = "custom_components.eybond_local.flows.common.translation.load_translation_bundle"


def manual_result_flow(hass, language, step_id):
    flow = EybondLocalConfigFlow()
    flow.hass = hass
    flow.context = {"language": language}
    flow._manual_config = {
        "server_ip": SYNTHETIC_SERVER_IP,
        "collector_ip": SYNTHETIC_COLLECTOR_IP,
        "tcp_port": 8899,
    }
    flow._manual_result = OnboardingResult(
        connection_mode="manual", last_error="callback_session_silent",
    )
    flow._callback_continuation._silent_offer = SilentSessionBootstrapOffer(
        "listener-synthetic-repair",
    )
    if step_id == "reconfigure_confirm":
        flow._repair_entry_id = "synthetic-legacy-entry"
    return flow


@pytest.mark.parametrize("step_id", STEPS)
@pytest.mark.parametrize("language", ("en", "ru", "uk", "ru-RU", "de"))
async def test_manual_result_includes_localized_labels_without_frontend_lookup(
    hass, language, step_id,
):
    flow = manual_result_flow(hass, language, step_id)
    result = await flow.async_step_manual_confirm()
    assert result["type"] is FlowResultType.MENU
    assert result["step_id"] == step_id
    # Round-trip the actual wire field: labels must travel in the flow response,
    # not depend on hass.localize having the new bootstrap keys in its cache.
    options = json.loads(json.dumps(result["menu_options"]))
    assert isinstance(options, dict)
    assert list(options) == list(ACTIONS)
    bundle = await hass.async_add_executor_job(load_translation_bundle, language)
    expected = bundle["config"]["step"][step_id]["menu_options"]
    assert options == {action: expected[action] for action in ACTIONS}
    assert all(label.strip() and label != action for action, label in options.items())
    for action in ACTIONS:
        assert result["data_schema"]({"next_step_id": action}) == {"next_step_id": action}


@pytest.mark.parametrize("step_id", STEPS)
@pytest.mark.parametrize("missing", ("absent", "empty", "whitespace", "invalid", "bundle"))
async def test_manual_result_has_readable_fallback_for_incomplete_backend_translation(
    hass, step_id, missing,
):
    bundle = deepcopy(await hass.async_add_executor_job(load_translation_bundle, "ru"))
    labels = bundle["config"]["step"][step_id]["menu_options"]
    for action in ACTIONS[:2]:
        if missing == "absent":
            labels.pop(action)
        else:
            labels[action] = {"empty": "", "whitespace": " \n ", "invalid": None,
                              "bundle": ""}[missing]
    if missing == "bundle":
        bundle = {}
    flow = manual_result_flow(hass, "ru", step_id)
    with patch(LOADER, return_value=bundle):
        result = await flow.async_step_manual_confirm()
    options = result["menu_options"]
    assert isinstance(options, dict)
    assert list(options) == list(ACTIONS)
    assert options[ACTIONS[0]] == "Silent collector: query with EyeBond framed protocol"
    assert options[ACTIONS[1]] == "Silent collector: query with AT command protocol"
    if missing != "bundle":
        assert options[ACTIONS[2]] == labels[ACTIONS[2]]
        assert options[ACTIONS[3]] == labels[ACTIONS[3]]
    assert all(label.strip() and label != action for action, label in options.items())
    assert not hass.config_entries.async_entries("eybond_local")


async def test_manual_result_uses_ha_language_when_no_user_language(hass):
    hass.config.language = "uk"
    flow = manual_result_flow(hass, "", "manual_confirm")
    result = await flow.async_step_manual_confirm()
    bundle = await hass.async_add_executor_job(load_translation_bundle, "uk")
    assert result["menu_options"] == {
        action: bundle["config"]["step"]["manual_confirm"]["menu_options"][action]
        for action in ACTIONS
    }


@pytest.mark.parametrize("language", ("en", "ru", "uk"))
@pytest.mark.parametrize("case,expected_actions", (
    ("unverified", ACTIONS[2:]),
    ("save", ("manual_save", *ACTIONS[2:])),
    ("cloud", (*ACTIONS[2:], "manual_smartess_cloud_assist")),
    ("inbound", (*ACTIONS[2:], "manual_enable_background_discovery")),
))
async def test_other_manual_result_actions_keep_order_labels_and_ids(
    hass, language, case, expected_actions,
):
    flow = manual_result_flow(hass, language, "manual_confirm")
    flow._manual_result = OnboardingResult(
        connection_mode="manual",
        last_error="inbound_awaiting_session" if case == "inbound" else "",
    )
    # Test presentation of already-decided eligibility, not admission authority.
    with (
        patch.object(flow, "_manual_entry_ready_to_save", return_value=case == "save"),
        patch.object(flow, "_can_offer_smartess_cloud_assist", return_value=case == "cloud"),
    ):
        result = await flow.async_step_manual_confirm()
    bundle = await hass.async_add_executor_job(load_translation_bundle, language)
    labels = bundle["config"]["step"]["manual_confirm"]["menu_options"]
    assert list(result["menu_options"]) == list(expected_actions)
    assert result["menu_options"] == {action: labels[action] for action in expected_actions}
