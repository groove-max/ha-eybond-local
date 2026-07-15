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

from custom_components.eybond_local.config_flow import EybondLocalConfigFlow
from custom_components.eybond_local.const import DOMAIN

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
