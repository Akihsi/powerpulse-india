# PowerPulse India

**Big Data-Driven Electricity Demand and Weather Pattern Analysis Across Indian States**
(PySpark, Spark SQL, Parquet, Streamlit, Plotly)

The project processes daily electricity supplied to Indian states (2013 to early 2025) together with
daily state weather, runs five Spark analyses, measures Spark's behaviour with real benchmarks, and shows
everything in a dashboard.

Dataset: Hunt, K. & Bloomfield, H. (2025). *Daily electricity demand data for Indian states (2014-2024)*.
Zenodo. https://doi.org/10.5281/zenodo.14983362 (licence CC BY 4.0, cite it in the report).

## Quick start

You need Python 3.10+ and Java 11, 17 or 21 (`java -version` must work).

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python run_all.py --download         # downloads ~140 MB, runs steps 0-3
streamlit run dashboard/app.py       # opens the dashboard in the browser
```

No internet or no download yet? Test everything with **synthetic** data first:
`python run_all.py --sample`, then `POWERPULSE_MODE=sample streamlit run dashboard/app.py`.
Synthetic results are written to separate folders (`results_sample/`, `data/sample_*`) and the dashboard
shows a warning banner. **Never put sample numbers in the report.**

Prefer to download by hand? Get the files from the Zenodo page, put them in `data/raw/`, then run `python run_all.py`.
Only the 3 national files (`corrected_daily_energy_met_MU.csv`, `corrected_peak_met_MW.csv`, `peak_unmet_MW.csv`)
and the per-state weather files are used.

## Pipeline

| Step | Script | What it does | Main output |
|---|---|---|---|
| 0 | `src/00_inspect_data.py` | Schemas, row counts, date ranges, missing values | `results/schema_report.md` |
| 1 | `src/01_ingest_clean.py` | Unpivot the wide national files, validate values, convert units, join demand with weather, save partitioned Parquet | `data/processed/*.parquet`, `results/data_quality_report.json` |
| 2 | `src/02_analytics.py` | The five Spark queries (DataFrame API and Spark SQL) | `results/q1_*.csv ... q5_*.csv`, `results/query_catalog.csv` |
| 3 | `src/03_benchmarks.py` | Five measured experiments (below) | `results/benchmarks_summary.csv`, `results/benchmark_environment.json` |
| - | `dashboard/app.py` | Streamlit dashboard reading the result tables | - |

### The five analytical queries

| Query | Technique | Question answered |
|---|---|---|
| Q1 Annual state demand | Spark SQL `GROUP BY`, window `lag` | How much does each state receive per year and how fast does it grow? |
| Q2 Peak-demand analysis | `row_number()` window, group-by | Which days are the peaks, and in which season do they happen? |
| Q3 Unmet peak demand | Filter, conditional aggregation | How often and how severely is peak demand not met, by state and region? |
| Q4 Demand versus weather | De-trending window, `corr()`, binning, join | How are temperature and demand associated? |
| Q5 Seasons and weekdays | Date functions, pivot | How do demand levels differ by month, season and weekday/weekend? |

### The benchmark experiments (Step 3)

A. CSV vs Parquet (read time and size) | B. Caching | C. Shuffle partitions | D. Pandas vs Spark | E. Scale-up stress test.
All numbers are measured on the machine that runs the script. Experiment E replicates the same data
x1, x2, x4 to see how both tools scale; it is labelled as a stress test and does not change the size of the project's real dataset.

## Folder layout

```
powerpulse-india/
  run_all.py            one command for the whole pipeline
  requirements.txt
  src/                  config, helpers, steps 0-3, downloader, synthetic data generator
  dashboard/app.py      Streamlit dashboard
  docs/DATA_DICTIONARY.md
  data/raw/             Zenodo CSVs (not committed)
  data/processed/       Parquet tables (created by step 1)
  results/              CSV/JSON outputs used by the dashboard and the report
```

## Be honest about these points in the report

* **Size.** About 140 MB of CSV is a moderate dataset. Do not call it a petabyte-scale system. The correct claim is:
  the pipeline uses distributed-processing techniques and would scale out by changing the Spark master (e.g. a cluster), not the code.
* **Spark vs Pandas.** On this size Pandas is often faster, especially once Spark's start-up time is counted. Report what you measured and explain why
  (JVM start-up and task scheduling are fixed costs; Spark pays off when data outgrows one machine's memory).
* **Association, not causation.** Temperature and demand move together through the year, but so do farming, festivals and holidays.
* **Unmet demand** is reported by grid operators; a zero can also mean "not recorded".
* **Weather** is a state-level average and cannot describe local conditions.
* **Partial years.** 2013 may have gaps and 2025 ends in March. Year comparisons only use state-years with at least 330 reported days.
* **Sum of state peaks** (Q2) is not the same as the simultaneous national peak.

## Things to check first on the real data

This code was written without being able to download the Zenodo files, so the column layout was taken from the dataset authors' own code
(https://github.com/kieranmrhunt/india-renewable) and then tested on synthetic files in the same layout. Do this first:

1. Run `python src/00_inspect_data.py` and read `results/schema_report.md`.
2. Confirm the weather column names include a daily mean temperature (the code looks for `2m_temperature_mean`, or any column with "temperature" and "mean").
   If step 2 stops with `Could not find a column ...`, the message lists the available columns; adjust the name in `src/02_analytics.py`.
3. Open `results/data_quality_report.json` after step 1 and check: dropped non-state columns, number of invalid values, rows without weather.
4. Check that the results look sensible (for example Delhi, Haryana and Rajasthan should peak in summer).

## Troubleshooting

| Problem | Fix |
|---|---|
| `JAVA_HOME is not set` / `java: command not found` | Install JDK 17 and restart the terminal. |
| Windows error mentioning `HADOOP_HOME` / `winutils` when writing Parquet | Easiest: run the project in **WSL2 (Ubuntu)** or in **Google Colab** (`!pip install pyspark streamlit plotly`, upload the folder). Alternative: install winutils for Hadoop 3 and set `HADOOP_HOME`. |
| `OutOfMemoryError` | `export POWERPULSE_DRIVER_MEMORY=4g` (Windows: `set POWERPULSE_DRIVER_MEMORY=4g`). |
| Python worker version mismatch | Run inside one virtual environment; the code already points Spark workers to the same Python. |
| Dashboard says "No results found" | Run `python run_all.py` (or `--sample`) first, and start the dashboard in the same mode. |
| Download fails | Download the files manually from the Zenodo page into `data/raw/`. |
