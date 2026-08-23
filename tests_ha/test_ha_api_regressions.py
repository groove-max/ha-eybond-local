"""Regressions for real-Home-Assistant API defects found by this suite.

Each test here pins a concrete defect that the stub-based unit suite is
structurally unable to see, because the stubs do not model the real Home
Assistant base classes or component graph.
"""

from __future__ import annotations

import json
from pathlib import Path

from homeassistant.config_entries import ConfigFlow
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.config_flow import EybondLocalConfigFlow
from custom_components.eybond_local.const import DOMAIN, SERVICE_RUN_DIAGNOSTIC_COMMANDS
from synthetic import SYNTHETIC_COLLECTOR_PN

MANIFEST = (
    Path(__file__).resolve().parents[1]
    / "custom_components"
    / "eybond_local"
    / "manifest.json"
)


async def test_manifest_declares_http_dependency(hass: HomeAssistant) -> None:
    """Regression: `async_setup` registers an HTTP view, so `http` is required.

    `support/download.py` calls `hass.http.register_view(...)`. Without `http` in
    the manifest dependencies Home Assistant does not guarantee it is set up
    first, `hass.http` is None, and the whole integration bootstrap raises
    `AttributeError: 'NoneType' object has no attribute 'register_view'`.
    """

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert "http" in manifest.get("dependencies", []), (
        "manifest must declare the `http` dependency because async_setup "
        "registers an HTTP view"
    )


async def test_domain_bootstrap_registers_http_view(hass: HomeAssistant) -> None:
    """Regression (behavioral): the bootstrap completes with `http` available."""

    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    # `hass.http` exists because the declared dependency was set up first, so the
    # support-package download view registered without raising.
    assert hass.http is not None


async def test_diagnostic_service_schema_accepts_download_link_flag(
    hass: HomeAssistant,
) -> None:
    """The real HA service layer must not reject the documented download flag."""

    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    calls: list[dict[str, object]] = []

    class Coordinator:
        async def async_run_diagnostic_commands(self, **kwargs):
            calls.append(dict(kwargs))
            return {
                "success": True,
                "output": "ok\n",
                "results": [],
                "context": {},
                "started_at": "start",
                "finished_at": "finish",
                "result_path": "/config/eybond_local/diagnostic_runs/result.json",
                "download_url": "https://ha.example/api/eybond_local/diagnostic_run/signed",
            }

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Diagnostic entry",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={},
    )
    entry.add_to_hass(hass)
    entry.runtime_data = Coordinator()

    response = await hass.services.async_call(
        DOMAIN,
        SERVICE_RUN_DIAGNOSTIC_COMMANDS,
        {
            "entry_id": entry.entry_id,
            "commands": "read 171 1\n",
            "publish_download_copy": True,
        },
        blocking=True,
        return_response=True,
    )

    assert response is not None
    assert response["success"] is True
    assert calls[0]["publish_download_copy"] is True


async def test_diagnostic_download_view_serves_only_entry_redacted_file(
    hass: HomeAssistant,
    hass_client,
) -> None:
    """The real HA HTTP router serves the private shareable result, not raw data."""

    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Diagnostic entry",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={},
    )
    entry.add_to_hass(hass)
    filename = f"diagnostic_{entry.entry_id}_20260823T125052279675Z.share.json"
    root = Path(hass.config.config_dir) / "eybond_local" / "diagnostic_runs"
    root.mkdir(parents=True, exist_ok=True)
    (root / filename).write_text('{"redacted": true}', encoding="utf-8")
    raw_filename = filename.removesuffix(".share.json") + ".json"
    (root / raw_filename).write_text('{"private": true}', encoding="utf-8")

    client = await hass_client()
    response = await client.get(
        f"/api/{DOMAIN}/diagnostic_run/{entry.entry_id}/{filename}"
    )
    assert response.status == 200
    assert await response.json() == {"redacted": True}
    assert "attachment" in response.headers["Content-Disposition"]

    raw_response = await client.get(
        f"/api/{DOMAIN}/diagnostic_run/{entry.entry_id}/{raw_filename}"
    )
    assert raw_response.status == 404


def test_config_flow_does_not_shadow_ha_reconfigure_property() -> None:
    """Regression: never assign to a name HA's ConfigFlow defines as a property.

    Home Assistant's ConfigFlow exposes `_reconfigure_entry_id` as a READ-ONLY
    property. An earlier revision assigned `self._reconfigure_entry_id = ""` in
    `__init__`, which raises `AttributeError: property ... has no setter` and
    broke EVERY config-flow instantiation on real Home Assistant.
    """

    # The Home Assistant property this used to collide with still exists, so the
    # hazard is real and this guard stays meaningful.
    assert isinstance(
        getattr(ConfigFlow, "_reconfigure_entry_id", None), property
    ), "HA no longer defines _reconfigure_entry_id; revisit this guard"

    # Instantiating our flow must not touch any read-only HA property.
    flow = EybondLocalConfigFlow()
    assert flow is not None

    # No attribute our flow assigns in __init__ may shadow a HA-owned property.
    ha_properties = {
        name
        for klass in type(flow).__mro__
        if klass is not EybondLocalConfigFlow
        for name, value in vars(klass).items()
        if isinstance(value, property) and value.fset is None
    }
    assigned = set(vars(flow))
    collisions = assigned & ha_properties
    assert not collisions, (
        f"config flow assigns to read-only Home Assistant properties: {collisions}"
    )


async def test_config_flow_can_be_started_by_real_flow_manager(
    hass: HomeAssistant,
) -> None:
    """Regression (behavioral): the flow instantiates and serves a step."""

    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )
    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)
