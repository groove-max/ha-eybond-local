"""Build identity is available through the actual HA setup/options lifecycle."""
from homeassistant.data_entry_flow import FlowResultType
from custom_components.eybond_local.build_identity import build_runtime_values, build_label
from test_ha_config_flow import collector_entry


async def test_setup_logs_build_and_options_expose_same_identity(hass, collector_entry, fake_runtime, caplog):
    caplog.set_level('INFO', logger='custom_components.eybond_local')
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    values = await hass.async_add_executor_job(build_runtime_values)
    label = build_label(values)
    assert 'EyeBond Local integration build: ' + label in caplog.text
    flow = await hass.config_entries.options.async_init(collector_entry.entry_id)
    assert flow['type'] is FlowResultType.MENU
    assert flow['description_placeholders']['integration_build'] == label
    hass.config_entries.options.async_abort(flow['flow_id'])
    assert await hass.config_entries.async_unload(collector_entry.entry_id)
    await hass.async_block_till_done()
