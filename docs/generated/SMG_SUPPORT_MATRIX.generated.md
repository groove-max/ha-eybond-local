# Support Matrix: SMG / Modbus

> Generated from declarative profile metadata. Do not edit this export manually.

- `profile_key`: `smg_modbus`
- capabilities: `33`
- validation states: `{'tested': 25, 'untested': 8}`
- support tiers: `{'blocked': 2, 'conditional': 24, 'standard': 7}`

| Capability | Register | Group | Validation | Tier | Notes |
|---|---:|---|---|---|---|
| `battery_equalization_mode` | `313` | `battery` | `tested` | `conditional` |  |
| `battery_overvoltage_protection_voltage` | `323` | `battery` | `tested` | `conditional` |  |
| `battery_bulk_voltage` | `324` | `battery` | `tested` | `conditional` |  |
| `battery_float_voltage` | `325` | `battery` | `tested` | `conditional` |  |
| `battery_redischarge_voltage` | `326` | `battery` | `tested` | `conditional` |  |
| `battery_under_voltage` | `327` | `battery` | `tested` | `conditional` |  |
| `battery_under_voltage_off_grid` | `329` | `battery` | `tested` | `conditional` |  |
| `low_dc_protection_soc_grid_mode` | `341` | `battery` | `tested` | `conditional` |  |
| `solar_battery_utility_return_soc_threshold` | `342` | `battery` | `tested` | `conditional` |  |
| `low_dc_cutoff_soc` | `343` | `battery` | `tested` | `conditional` |  |
| `battery_equalization_voltage` | `334` | `battery` | `tested` | `conditional` |  |
| `battery_equalization_time` | `335` | `battery` | `tested` | `conditional` |  |
| `battery_equalization_timeout` | `336` | `battery` | `tested` | `conditional` |  |
| `battery_equalization_interval` | `337` | `battery` | `tested` | `conditional` |  |
| `charge_source_priority` | `331` | `charging` | `tested` | `conditional` |  |
| `max_charge_current` | `332` | `charging` | `tested` | `conditional` |  |
| `max_ac_charge_current` | `333` | `charging` | `tested` | `conditional` |  |
| `output_source_priority` | `301` | `output` | `tested` | `conditional` |  |
| `output_rating_voltage` | `320` | `output` | `untested` | `conditional` |  |
| `output_rating_frequency` | `321` | `output` | `untested` | `conditional` |  |
| `output_mode` | `300` | `system` | `untested` | `conditional` |  |
| `input_voltage_range` | `302` | `system` | `tested` | `standard` |  |
| `buzzer_mode` | `303` | `system` | `tested` | `standard` |  |
| `lcd_backlight_mode` | `305` | `system` | `tested` | `standard` |  |
| `lcd_auto_return_mode` | `306` | `system` | `tested` | `standard` |  |
| `power_saving_mode` | `307` | `system` | `untested` | `blocked` | Observed on the real SMG 6200 to return Modbus exception_code:7 during write attempts. |
| `overload_restart_mode` | `308` | `system` | `tested` | `standard` |  |
| `over_temperature_restart_mode` | `309` | `system` | `tested` | `standard` |  |
| `overload_bypass_mode` | `310` | `system` | `untested` | `blocked` | Observed on the real SMG 6200 to return Modbus exception_code:7 during write attempts. |
| `turn_on_mode` | `406` | `system` | `tested` | `standard` |  |
| `remote_turn_on` | `420` | `system` | `untested` | `conditional` |  |
| `remote_shutdown` | `420` | `system` | `untested` | `conditional` |  |
| `exit_fault_mode` | `426` | `system` | `untested` | `conditional` |  |
