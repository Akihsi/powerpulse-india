"""Step 3 - Big Data benchmarks. Every number here is MEASURED on the machine that runs it.

Experiments (all use the same aggregation: average temperature per state and year):
  A  File format        : CSV (schema inferred) vs CSV (schema given) vs Parquet
  B  Caching            : repeated query without cache vs with .cache()
  C  Shuffle partitions : 1, 2, 4, 8, 16, 64, 200 (adaptive query execution switched off to expose the effect)
  D  Pandas vs Spark    : same job, same data, end to end (read + aggregate)
  E  Scale-up test      : data replicated x1, x2, x4 (x8 with --scale-max 8) - a STRESS TEST on copied data,
                          clearly labelled as such; it does not describe the project's real dataset

    python src/03_benchmarks.py [--runs 3] [--scale-max 4]

Outputs: results/benchmarks_raw.csv, results/benchmarks_summary.csv, results/benchmark_environment.json
Run it on a quiet machine (close browsers etc.) and report the numbers you actually get, including
the ones where Spark is slower than Pandas - on a few hundred MB that is the normal, expected outcome.
"""
from __future__ import annotations

import argparse
import os
import platform
import shutil
import statistics
import sys
import time

import pandas as pd
from pyspark.sql import functions as F

import config
from common import banner, find_column, get_spark, write_json

RAW_ROWS: list[dict] = []


def record(experiment: str, variant: str, run: int, seconds: float, rows: int | None = None, note: str = "") -> None:
    RAW_ROWS.append({"experiment": experiment, "variant": variant, "run": run, "seconds": round(seconds, 4),
                     "rows": rows, "note": note})
    print(f"    {experiment:<14} {variant:<34} run {run}: {seconds:7.3f}s")


def time_it(fn):
    t0 = time.perf_counter()
    out = fn()
    return time.perf_counter() - t0, out


def spark_job(df, temp_col):
    """The benchmark query. .collect() forces Spark to really execute it."""
    return (df.groupBy("state", "year").agg(F.avg(temp_col).alias("avg_temp"), F.count("*").alias("n"))).collect()


def pandas_job(pdf, temp_col):
    return pdf.groupby(["state", "year"]).agg(avg_temp=(temp_col, "mean"), n=(temp_col, "size"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=3, help="timed repetitions per variant")
    ap.add_argument("--scale-max", type=int, default=4, choices=[1, 2, 4, 8], help="largest replication factor in experiment E")
    args = ap.parse_args()
    runs = args.runs

    banner(f"STEP 3 - Benchmarks ({config.MODE} mode, {runs} runs each)")
    weather_pq = config.PROC_DIR / "weather_clean.parquet"
    if not weather_pq.exists():
        raise SystemExit("Run `python src/01_ingest_clean.py` first.")

    t0 = time.perf_counter()
    spark = get_spark("PowerPulse-benchmarks")
    startup = time.perf_counter() - t0
    record("startup", "Spark session start-up (JVM)", 1, startup, note="one-off cost Pandas does not have")

    sdf = spark.read.parquet(str(weather_pq))
    temp = find_column(sdf.columns, ["temp", "mean"], ["dewpoint"], exact=["temp_mean"])
    n_rows = sdf.count()
    n_cols = len(sdf.columns)
    print(f"  benchmark table: weather_clean, {n_rows:,} rows x {n_cols} columns, temperature column = {temp}")

    bench_dir = config.PROC_DIR / "_bench"
    shutil.rmtree(bench_dir, ignore_errors=True)
    bench_dir.mkdir(parents=True)

    # ---------------------------------------------------------------- A  file format
    banner("A  CSV vs Parquet")
    csv_dir = bench_dir / "weather_csv"
    sdf.write.mode("overwrite").option("header", True).csv(str(csv_dir))
    schema = sdf.schema
    csv_bytes = sum(p.stat().st_size for p in csv_dir.glob("*.csv"))
    pq_bytes = sum(p.stat().st_size for p in weather_pq.rglob("*.parquet"))
    print(f"    size on disk: CSV {csv_bytes / 1e6:.1f} MB | Parquet {pq_bytes / 1e6:.1f} MB")
    record("A_format", "size on disk CSV (MB)", 1, csv_bytes / 1e6, note="value is MB, not seconds")
    record("A_format", "size on disk Parquet (MB)", 1, pq_bytes / 1e6, note="value is MB, not seconds")

    readers = {
        "CSV (inferSchema)": lambda: spark.read.csv(str(csv_dir), header=True, inferSchema=True),
        "CSV (explicit schema)": lambda: spark.read.csv(str(csv_dir), header=True, schema=schema),
        "Parquet": lambda: spark.read.parquet(str(weather_pq)),
    }
    for name, make in readers.items():
        spark_job(make(), temp)  # warm-up (JIT, file listing)
        for r in range(1, runs + 1):
            secs, _ = time_it(lambda: spark_job(make(), temp))
            record("A_format", name, r, secs, n_rows)

    # ---------------------------------------------------------------- B  caching
    banner("B  Caching")
    base = spark.read.parquet(str(weather_pq))
    spark_job(base, temp)  # warm-up
    for r in range(1, runs + 3):
        secs, _ = time_it(lambda: spark_job(base, temp))
        record("B_cache", "no cache (repeat query)", r, secs, n_rows)
    cached = spark.read.parquet(str(weather_pq)).cache()
    secs, _ = time_it(lambda: spark_job(cached, temp))
    record("B_cache", "with cache - 1st run (fills cache)", 1, secs, n_rows)
    for r in range(1, runs + 3):
        secs, _ = time_it(lambda: spark_job(cached, temp))
        record("B_cache", "with cache - later runs", r, secs, n_rows)
    cached.unpersist()

    # ---------------------------------------------------------------- C  shuffle partitions
    banner("C  Shuffle partitions (adaptive query execution off)")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    sdf_c = spark.read.parquet(str(weather_pq)).cache()
    sdf_c.count()
    for parts in (1, 2, 4, 8, 16, 64, 200):
        spark.conf.set("spark.sql.shuffle.partitions", str(parts))
        spark_job(sdf_c, temp)  # warm-up
        for r in range(1, runs + 1):
            secs, _ = time_it(lambda: spark_job(sdf_c, temp))
            record("C_partitions", f"shuffle.partitions={parts}", r, secs, n_rows)
    sdf_c.unpersist()
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    # ---------------------------------------------------------------- D  pandas vs spark
    banner("D  Pandas vs Spark (read + aggregate, end to end)")
    cols = ["state", "year", temp]

    def pandas_parquet():
        return pandas_job(pd.read_parquet(weather_pq, columns=["state", "year", temp]), temp)

    def pandas_csv():
        files = sorted(csv_dir.glob("*.csv"))
        return pandas_job(pd.concat((pd.read_csv(f, usecols=cols) for f in files), ignore_index=True), temp)

    def spark_parquet():
        return spark_job(spark.read.parquet(str(weather_pq)), temp)

    def spark_csv():
        return spark_job(spark.read.csv(str(csv_dir), header=True, schema=schema), temp)

    for name, fn in {"Pandas + Parquet": pandas_parquet, "Spark + Parquet": spark_parquet,
                     "Pandas + CSV": pandas_csv, "Spark + CSV": spark_csv}.items():
        fn()  # warm-up
        for r in range(1, runs + 1):
            secs, _ = time_it(fn)
            record("D_pandas_spark", name, r, secs, n_rows)

    # ---------------------------------------------------------------- E  scale-up (replicated copies)
    banner("E  Scale-up stress test (replicated copies of the same data)")
    factor = 1
    base_df = spark.read.parquet(str(weather_pq)).select(*cols)
    while factor <= args.scale_max:
        out = bench_dir / f"scale_x{factor}"
        big = base_df
        for _ in range(factor - 1):
            big = big.unionByName(base_df)
        big.write.mode("overwrite").parquet(str(out))
        rows = factor * n_rows
        label = f"x{factor} ({rows:,} rows)"
        for name, fn in {
            "Spark": lambda: spark_job(spark.read.parquet(str(out)), temp),
            "Pandas": lambda: pandas_job(pd.read_parquet(out, columns=cols), temp),
        }.items():
            fn()
            for r in range(1, runs + 1):
                secs, _ = time_it(fn)
                record("E_scale", f"{name} {label}", r, secs, rows, note="replicated copy - stress test only")
        factor *= 2

    shutil.rmtree(bench_dir, ignore_errors=True)

    # ---------------------------------------------------------------- save
    raw = pd.DataFrame(RAW_ROWS)
    raw.to_csv(config.RESULTS_DIR / "benchmarks_raw.csv", index=False)
    summ = (
        raw.groupby(["experiment", "variant"], sort=False)
        .agg(median_s=("seconds", "median"), min_s=("seconds", "min"), max_s=("seconds", "max"),
             runs=("seconds", "count"), rows=("rows", "first"), note=("note", "first"))
        .reset_index()
    )
    summ.to_csv(config.RESULTS_DIR / "benchmarks_summary.csv", index=False)

    try:
        import psutil  # optional
        ram = round(psutil.virtual_memory().total / 1e9, 1)
    except Exception:  # noqa: BLE001
        ram = None
    write_json({
        "python": sys.version.split()[0], "pyspark": spark.version, "pandas": pd.__version__,
        "os": platform.platform(), "cpu_cores": os.cpu_count(), "ram_gb": ram,
        "spark_master": spark.sparkContext.master, "driver_memory": os.environ.get("POWERPULSE_DRIVER_MEMORY", "2g"),
        "benchmark_rows": n_rows, "benchmark_columns": n_cols, "timed_runs_per_variant": runs,
        "data_is_synthetic": (config.RAW_DIR / "SYNTHETIC.txt").exists(),
    }, config.RESULTS_DIR / "benchmark_environment.json")

    print("\nSUMMARY (median seconds)")
    print(summ[["experiment", "variant", "median_s"]].to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
