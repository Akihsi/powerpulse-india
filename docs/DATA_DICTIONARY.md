# Data dictionary

Source: Hunt & Bloomfield (2025), Zenodo record 14983362, CC BY 4.0. Read `results/schema_report.md` (created by step 0)
for the exact columns and row counts of the files you downloaded and copy those into the report.

## Raw files used

| File | Layout | Meaning |
|---|---|---|
| `corrected_daily_energy_met_MU.csv` | `Date` + one column per state | Electricity supplied to the state that day, MU (million units = GWh), quality-controlled for transcription errors |
| `corrected_peak_met_MW.csv` | same | Highest power supplied to the state that day, MW |
| `peak_unmet_MW.csv` | same | Largest shortfall of supplied power versus demand that day, MW (raw version; no corrected file is published) |
| `<State>.csv` (e.g. `Tamil_Nadu.csv`) | `date` + weather columns | Daily minimum, mean and maximum of 2 m temperature, 2 m dewpoint, 10 m wind (u, v), surface solar radiation, total cloud cover and UTCI for that state |

Non-state columns in the national files (for example utility entities) are dropped in step 1 and listed in the quality report.

## Cleaned tables (data/processed, Parquet partitioned by `year`)

### demand_long.parquet - one row per state and day

| Column | Type | Description |
|---|---|---|
| date | date | Day |
| state | string | State or union territory name |
| energy_met_mu | double | Energy supplied, MU. Values <= 0 set to null |
| peak_met_mw | double | Peak power supplied, MW. Values <= 0 set to null |
| peak_unmet_mw | double | Peak shortfall, MW. Negative values set to null |
| region_code, region | string | Grid region (NR/Northern, ER/Eastern, WR/Western, SR/Southern, NER/North-Eastern, I/Islands), as defined by the dataset authors |
| year, month | int | From `date` |
| season | string | Winter (Dec-Feb), Summer (Mar-May), Monsoon (Jun-Sep), Post-monsoon (Oct-Nov) |
| day_of_week | int | 1 = Monday ... 7 = Sunday |
| is_weekend | boolean | Saturday or Sunday |

### weather_clean.parquet - one row per state and day

Column names are shortened by `clean_column_name()`: `2m_temperature_*` becomes `temp_*`, `2m_dewpoint_temperature_*` becomes `dewpoint_*`,
`10m_u/v_component_of_wind_*` becomes `wind_u_*`/`wind_v_*`, `surface_solar_radiation_downwards_*` becomes `solar_*`,
`total_cloud_cover_*` becomes `cloud_*`. Suffixes `_min`, `_mean`, `_max` are the daily statistics.

| Group | Unit after cleaning | Note |
|---|---|---|
| temp_*, dewpoint_*, utci_* | degrees Celsius | Converted from Kelvin when the column average is above 150 (checked from the data) |
| wind_u_*, wind_v_* | m/s | `wind_speed_mean` = sqrt(u^2 + v^2) is added |
| solar_* | W/m2 | Source value divided by 3600 (same convention as the dataset authors' code) |
| cloud_* | fraction 0-1 | As published |

### demand_weather.parquet

`demand_long` left-joined with `weather_clean` on (`date`, `state`). Ladakh has demand data but no weather file, so its weather columns are null.

## Result tables (results/*.csv)

| File | Content |
|---|---|
| q1_annual_state_demand | state, year: total and average daily energy, reported days, `is_full_year`, year-over-year growth % |
| q2_top_peak_days_by_state | 10 highest peak days per state with season and mean temperature |
| q2_peak_season_share | share of each state's 30 highest peaks falling in each season |
| q2_national_peak_days | days with the highest sum of state peaks |
| q3_unmet_by_state_year / q3_unmet_by_region_year | shortfall days, average and maximum unmet MW, % of demand |
| q4_temp_demand_correlation | per-state correlation of temperature with demand (raw and relative) |
| q4_demand_by_temperature_bin | relative demand per 5 degC temperature band and region |
| q5_monthly_state_year / q5_season_by_region / q5_weekend_effect | seasonal and weekday patterns |
| x_region_monthly / x_national_daily | helper tables for the dashboard |
| query_catalog | list of all queries with technique and run time |
| benchmarks_summary / benchmarks_raw / benchmark_environment | benchmark results and the machine they were measured on |

**Relative demand** = a day's energy divided by the state's average daily energy in the same year (1.0 = an average day). It removes long-term growth and state size.
