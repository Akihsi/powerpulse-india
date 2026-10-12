# Raw data schema report (real mode)
- Folder: `/Users/nitingupta/Desktop/projects/powerpulse-india/data/raw`
- CSV files found: 39 (136.6 MB total)

## National files (one Date column + one column per state)
- `corrected_daily_energy_met_MU.csv` (807.6 kB): 3,955 rows, 2013-01-01 to 2024-04-28, 36 state columns; non-state columns that the pipeline drops: ['AMNSIL', 'Damodar Valley Corporation', 'Total']
  - Unique dates: 3,900
  - Exact duplicate rows: 43
  - Duplicate-date groups: 55
  - Missing calendar dates within observed range: 236
  - Entirely empty state columns: ['Andaman and Nicobar Islands', 'Lakshadweep']
  - State columns with negative values (cell counts): {'Sikkim': 1, 'Tripura': 2}
  - State columns with null values (cell counts): {'Andaman and Nicobar Islands': 3955, 'Chhattisgarh': 1, 'Dadra and Nagar Haveli': 695, 'Daman and Diu': 695, 'Lakshadweep': 3955, 'Telangana': 423}
- `corrected_peak_met_MW.csv` (1.0 MB): 3,955 rows, 2013-01-01 to 2024-04-28, 36 state columns; non-state columns that the pipeline drops: ['AMNSIL', 'Damodar Valley Corporation', 'Total']
  - Unique dates: 3,900
  - Exact duplicate rows: 43
  - Duplicate-date groups: 55
  - Missing calendar dates within observed range: 236
  - Entirely empty state columns: ['Andaman and Nicobar Islands', 'Lakshadweep']
  - State columns with negative values (cell counts): none
  - State columns with null values (cell counts): {'Andaman and Nicobar Islands': 3955, 'Chhattisgarh': 1, 'Dadra and Nagar Haveli': 695, 'Daman and Diu': 695, 'Lakshadweep': 3955, 'Telangana': 423}
- `peak_unmet_MW.csv` (677.3 kB): 3,955 rows, 2013-01-01 to 2024-04-28, 36 state columns; non-state columns that the pipeline drops: ['AMNSIL', 'Damodar Valley Corporation', 'Total']
  - Unique dates: 3,900
  - Exact duplicate rows: 47
  - Duplicate-date groups: 55
  - Missing calendar dates within observed range: 236
  - Entirely empty state columns: ['Andaman and Nicobar Islands', 'Lakshadweep']
  - State columns with negative values (cell counts): {'Andhra Pradesh': 1, 'Arunachal Pradesh': 1, 'Assam': 1, 'Bihar': 6, 'Delhi': 4, 'Gujarat': 1, 'Himachal Pradesh': 7, 'Jammu and Kashmir': 5, 'Jharkhand': 6, 'Karnataka': 1, 'Kerala': 5, 'Maharashtra': 1, 'Manipur': 1, 'Meghalaya': 2, 'Mizoram': 2, 'Nagaland': 1, 'Odisha': 12, 'Puducherry': 3, 'Punjab': 6, 'Telangana': 2, 'Tripura': 1, 'Uttar Pradesh': 7, 'Uttarakhand': 3, 'West Bengal': 4}
  - State columns with null values (cell counts): {'Andaman and Nicobar Islands': 3955, 'Andhra Pradesh': 3, 'Chhattisgarh': 1, 'Dadra and Nagar Haveli': 695, 'Daman and Diu': 695, 'Karnataka': 3, 'Kerala': 3, 'Lakshadweep': 3955, 'Puducherry': 4, 'Tamil Nadu': 3, 'Telangana': 426}

## State weather files (36 found)
- Total weather rows across all state files: **595,812**
- Columns (22) and inferred types:
  - `date` : date
  - `10m_u_component_of_wind_min` : double
  - `10m_u_component_of_wind_mean` : double
  - `10m_u_component_of_wind_max` : double
  - `10m_v_component_of_wind_min` : double
  - `10m_v_component_of_wind_mean` : double
  - `10m_v_component_of_wind_max` : double
  - `2m_temperature_min` : double
  - `2m_temperature_mean` : double
  - `2m_temperature_max` : double
  - `2m_dewpoint_temperature_min` : double
  - `2m_dewpoint_temperature_mean` : double
  - `2m_dewpoint_temperature_max` : double
  - `surface_solar_radiation_downwards_min` : double
  - `surface_solar_radiation_downwards_mean` : double
  - `surface_solar_radiation_downwards_max` : double
  - `total_cloud_cover_min` : double
  - `total_cloud_cover_mean` : double
  - `total_cloud_cover_max` : double
  - `utci_min` : double
  - `utci_mean` : double
  - `utci_max` : double

| File | Rows | First date | Last date |
|---|---:|---|---|
| Andaman_and_Nicobar.csv | 16,422 | 1979-01-01 | 2023-12-17 |
| Andhra_Pradesh.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Arunachal_Pradesh.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Assam.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Bihar.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Chandigarh.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Chhattisgarh.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Dadra_and_Nagar_Haveli.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Daman_and_Diu.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Goa.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Gujarat.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Haryana.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Himachal_Pradesh.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Jammu_and_Kashmir.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Jharkhand.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Karnataka.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Kerala.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Lakshadweep.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Madhya_Pradesh.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Maharashtra.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Manipur.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Meghalaya.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Mizoram.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| NCT_of_Delhi.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Nagaland.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Odisha.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Puducherry.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Punjab.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Rajasthan.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Sikkim.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Tamil_Nadu.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Telangana.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Tripura.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Uttar_Pradesh.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| Uttarakhand.csv | 16,554 | 1979-01-01 | 2024-04-27 |
| West_Bengal.csv | 16,554 | 1979-01-01 | 2024-04-27 |

- Columns with missing values: {'10m_u_component_of_wind_min': 35, '10m_u_component_of_wind_mean': 35, '10m_u_component_of_wind_max': 35, '10m_v_component_of_wind_min': 36, '10m_v_component_of_wind_mean': 36, '10m_v_component_of_wind_max': 36, '2m_temperature_min': 36, '2m_temperature_mean': 36, '2m_temperature_max': 36, '2m_dewpoint_temperature_min': 36, '2m_dewpoint_temperature_mean': 36, '2m_dewpoint_temperature_max': 36, 'surface_solar_radiation_downwards_min': 71, 'surface_solar_radiation_downwards_mean': 71, 'surface_solar_radiation_downwards_max': 71, 'total_cloud_cover_min': 36, 'total_cloud_cover_mean': 36, 'total_cloud_cover_max': 36, 'utci_min': 3151, 'utci_mean': 3151, 'utci_max': 3151}
