"""The custom integration loads under a REAL Home Assistant.

Catches manifest/handler/platform declaration problems and import-time breakage
against the actual Home Assistant API -- none of which the stub-based unit suite
can see.
"""

from __future__ import annotations

import json
from pathlib import Path

from homeassistant.config_entries import HANDLERS
from homeassistant.core import HomeAssistant
from homeassistant.loader import async_get_integration
from homeassistant.setup import async_setup_component

from custom_components.eybond_local.const import DOMAIN, PLATFORMS

MANIFEST = (
    Path(__file__).resolve().parents[1]
    / "custom_components"
    / "eybond_local"
    / "manifest.json"
)


async def test_real_homeassistant_package_is_imported() -> None:
    """Guard: this suite must exercise the real HA, never the tests/ stubs."""

    import homeassistant
    import homeassistant.config_entries as real_config_entries

    # A stub module has no real package path; the real one is installed in
    # site-packages and exposes the genuine ConfigEntry machinery.
    assert getattr(homeassistant, "__file__", None) is not None
    assert "site-packages" in str(homeassistant.__file__)
    assert hasattr(real_config_entries, "ConfigEntryState")
    assert hasattr(real_config_entries, "ConfigFlow")


async def test_integration_manifest_is_discovered(hass: HomeAssistant) -> None:
    """Home Assistant's loader finds and parses our manifest."""

    integration = await async_get_integration(hass, DOMAIN)

    assert integration.domain == DOMAIN
    assert integration.config_flow is True
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert integration.version is not None
    assert manifest["domain"] == DOMAIN


async def test_config_flow_handler_is_registered(hass: HomeAssistant) -> None:
    """The config flow handler resolves through HA's real handler registry."""

    integration = await async_get_integration(hass, DOMAIN)
    await integration.async_get_platform("config_flow")

    assert DOMAIN in HANDLERS
    handler = HANDLERS[DOMAIN]
    assert handler.VERSION >= 1
    # The options flow is served by a real staticmethod on the handler.
    assert hasattr(handler, "async_get_options_flow")


async def test_declared_platforms_import_cleanly(hass: HomeAssistant) -> None:
    """Every declared platform module imports against the real HA API."""

    integration = await async_get_integration(hass, DOMAIN)
    for platform in PLATFORMS:
        module = await integration.async_get_platform(platform)
        assert module is not None


async def test_domain_setup_registers_services(hass: HomeAssistant) -> None:
    """`async_setup` runs under real HA and registers the domain services."""

    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    assert DOMAIN in hass.data
    assert hass.services.async_services().get(DOMAIN)
