# SMG Support Matrix

This document summarizes the current support level for the SMG-family default runtime path in `eybond_local`.

## Source Of Truth

- runtime driver: `custom_components/eybond_local/drivers/smg.py`
- shared family profile base: `custom_components/eybond_local/profiles/modbus_smg/family_base.json`
- verified default capability profile: `custom_components/eybond_local/profiles/smg_modbus.json` (shim -> `profiles/modbus_smg/default.json`)
- read-only family fallback profile: `custom_components/eybond_local/profiles/modbus_smg/family_fallback.json`
- model-specific Anenji profile: `custom_components/eybond_local/profiles/modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json`
- default register schema: `custom_components/eybond_local/register_schemas/modbus_smg/models/smg_6200.json`
- model-specific Anenji schema: `custom_components/eybond_local/register_schemas/modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json`
- generated export: [generated/SMG_SUPPORT_MATRIX.generated.md](generated/SMG_SUPPORT_MATRIX.generated.md)

Regenerate the machine-readable Markdown export for the verified default SMG runtime profile with:

```bash
python3 tools/export_support_matrix.py \
  --profile smg_modbus.json \
  --format markdown \
  --output docs/generated/SMG_SUPPORT_MATRIX.generated.md
```

Inspect the model-specific Anenji capability matrix with:

```bash
python3 tools/export_support_matrix.py \
  --profile modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json \
  --format markdown
```

## Human Support Categories

- `tested`
  - verified on a real `SMG 6200` using live reads and successful write or no-op write checks
  - exposed automatically in `auto` control mode when detection confidence is `high`
- `untested`
  - implemented in schema and driver, but not yet confirmed on real hardware
  - exposed only in `full` control mode
- `runtime-restricted`
  - implemented, but visibility or editability depends on live inverter state
- `observed blocked`
  - attempted on real hardware, but the inverter rejected the write path

These human categories map approximately to machine-readable profile metadata like this:

- `tested` -> `validation_state=tested`
- `untested` -> `validation_state=untested`
- `runtime-restricted` -> `support_tier=conditional`
- `observed blocked` -> `support_tier=blocked`

## Runtime Paths

The SMG family now has three distinct built-in runtime paths.

| Runtime path | When it is used | What users should expect |
|---|---|---|
| Verified default (`SMG 6200`) | Rated-power `6200` devices that match the known default SMG layout | Full monitoring and the tested default SMG write surface. This is the path covered by the generated matrix export below. |
| Model-specific Anenji (`anenji_anj_11kw_48v_wifi_p`) | Devices that match the validated Anenji protocol-4 anchors | Built-in monitoring is broader than the default SMG path, including PV1/PV2, inverter date/time, and native PV counters. The writable surface is implemented but still untested, so normal `auto` mode keeps it hidden. |
| Read-only family fallback (`family_fallback`) | Devices that clearly look SMG-family, but do not match a verified model-specific binding | Monitoring remains available, but built-in writes stay disabled. Support workflow and exported archives explicitly label this state as `Read-only unverified SMG family`. |

## Verified Default SMG Diagnostics

Live verification on the currently checked Sandisolar-backed SMG 6200 path supports keeping these extra diagnostics:

- keep as useful diagnostics: `program_version`, `protocol_number`, `device_type`, `battery_type`, `warning_mask_i`, `dry_contact_mode`, `automatic_mains_output_enabled`
- keep as hidden-by-default diagnostics for now: `rated_cell_count`, `max_discharge_current_protection`
- suppress when it is only placeholder data: `device_name`

The current driver also backfills missing probe-only SMG details during normal refresh, so these surviving diagnostics remain available after a Home Assistant restart instead of dropping to `unavailable` permanently.

## Anenji Model-Specific Additions

The rest of this document focuses on the verified default SMG 6200 write surface. The built-in Anenji runtime path adds these notable model-specific behaviors on top of the generic family support:

- read-side PV channel telemetry: `pv1_voltage`, `pv1_current`, `pv1_power`, `pv2_voltage`, `pv2_current`, `pv2_power`
- read-side system/config telemetry from the `677+` window, including `input_mode`, `remote_switch`, `ground_relay_enabled`, and `lithium_battery_activation_time`
- read-side inverter clock decoding from `696..701` as `inverter_date` and `inverter_time`
- native PV counters from `702` and `703..704` as `pv_generation_day` and `pv_generation_sum`
- an untested 47-capability control surface, exposed only in manual `full` control mode
- a dedicated `Sync Inverter Clock` tooling button that writes the current Home Assistant local date/time through the same untested clock registers when the model-specific write path is enabled

## Auto-Exposed Tested Controls

These are the safest parts of the verified default SMG 6200 write surface. In `auto + high confidence`, these are the controls that can appear by default.

### Output

| Capability | Register | Notes |
|---|---:|---|
| `output_source_priority` | `301` | Tested. Marked unsafe while running, so warnings are shown. |

### Charging

| Capability | Register | Notes |
|---|---:|---|
| `charge_source_priority` | `331` | Tested. |
| `max_charge_current` | `332` | Tested. |
| `max_ac_charge_current` | `333` | Tested. Editable only when utility charging is allowed by current policy. |

### Battery

| Capability | Register | Notes |
|---|---:|---|
| `battery_equalization_mode` | `313` | Tested. |
| `battery_overvoltage_protection_voltage` | `323` | Tested. |
| `battery_bulk_voltage` | `324` | Tested. |
| `battery_float_voltage` | `325` | Tested. |
| `battery_redischarge_voltage` | `326` | Tested. |
| `battery_under_voltage` | `327` | Tested. |
| `battery_under_voltage_off_grid` | `329` | Tested. |
| `battery_equalization_voltage` | `334` | Tested. Visible only when equalization is enabled. |
| `battery_equalization_time` | `335` | Tested. Visible only when equalization is enabled. |
| `battery_equalization_timeout` | `336` | Tested. Visible only when equalization is enabled. |
| `battery_equalization_interval` | `337` | Tested. Visible only when equalization is enabled. |
| `low_dc_protection_soc_grid_mode` | `341` | Tested. Reverse-engineered from live app changes. |
| `solar_battery_utility_return_soc_threshold` | `342` | Tested. Reverse-engineered from live app changes. |
| `low_dc_cutoff_soc` | `343` | Tested. Reverse-engineered from live app changes. |

### System

| Capability | Register | Notes |
|---|---:|---|
| `input_voltage_range` | `302` | Tested. |
| `buzzer_mode` | `303` | Tested. |
| `lcd_backlight_mode` | `305` | Tested. |
| `lcd_auto_return_mode` | `306` | Tested. |
| `overload_restart_mode` | `308` | Tested. |
| `over_temperature_restart_mode` | `309` | Tested. |
| `turn_on_mode` | `406` | Tested. |

## Implemented But Not Yet Promoted

These controls exist in the profile and driver, but are still untested or intentionally not auto-exposed.

| Capability | Register | Why Not Auto-Exposed Yet |
|---|---:|---|
| `output_mode` | `300` | Untested and high-impact. Requires safe configuration mode. |
| `output_rating_voltage` | `320` | Untested and high-impact. Requires safe configuration mode. |
| `output_rating_frequency` | `321` | Untested and high-impact. Requires safe configuration mode. |
| `remote_turn_on` | `420` | Action-style control, not yet live-validated. |
| `remote_shutdown` | `420` | Action-style control, not yet live-validated. |
| `exit_fault_mode` | `426` | Action-style control, only relevant in fault mode, not yet live-validated. |

## Observed Blocked On Real Hardware

| Capability | Register | Observed Behavior |
|---|---:|---|
| `power_saving_mode` | `307` | Returned `exception_code:7` |
| `overload_bypass_mode` | `310` | Returned `exception_code:7` |

At the moment these should be treated as firmware-locked or mode-restricted for the observed SMG 6200 variant.

## Runtime Gates That Matter

The following runtime conditions affect whether a control is visible or editable:

- `configuration_safe_mode`
- `battery_connected`
- `charging_inactive`
- `utility_charging_allowed`
- `battery_equalization_enabled`
- `remote_control_enabled`
- `fault_mode`

## Read Coverage Summary

Current SMG read coverage in the driver:

- status block: `100`, `108`
- live block: `201..217`, `219`, `220`, `223..227`, `229`, `232..234`
- config block: `300..310`, `313..316`, `320..338`, `341..343`, `351`
- auxiliary or model registers: `171..184`, `406`, `420`, `626..644`

Known uncovered candidates inside otherwise known SMG ranges:

- live block: `218`, `221`, `222`, `228`, `230`, `231`
- config block: `304`, `311`, `312`, `317..319`, `328`, `330`, `339`, `340`

These are not blockers for current Home Assistant functionality, but they remain candidates for future reverse engineering.
