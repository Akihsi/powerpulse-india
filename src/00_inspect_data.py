"""Step 0 - Inspect the raw files (schemas, row counts, date ranges, missing values).

Writes results/schema_report.md. Use its contents for the "Data description" section of
the report and to fill in docs/DATA_DICTIONARY.md.

    python src/00_inspect_data.py
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F

import config
from common import banner, get_spark


def human(nbytes: int) -> str:
    return f"{nbytes / 1e6:.1f} MB" if nbytes >= 1e6 else f"{nbytes / 1e3:.1f} kB"


def main() -> None:
    banner(f"STEP 0 - Inspect raw data ({config.MODE} mode) in {config.RAW_DIR}")
    if not config.RAW_DIR.exists() or not any(config.RAW_DIR.glob("*.csv")):
        raise SystemExit(
            f"No CSV files in {config.RAW_DIR}.\n"
            "Run `python src/download_data.py` first (or `python run_all.py --sample` to test with synthetic data)."
        )
    spark = get_spark("PowerPulse-inspect")
    lines = [f"# Raw data schema report ({config.MODE} mode)\n"]
    if (config.RAW_DIR / "SYNTHETIC.txt").exists():
        lines.append("> **SYNTHETIC SAMPLE DATA - not real. Do not quote these numbers.**\n")

    all_csv = sorted(config.RAW_DIR.glob("*.csv"))
    total = sum(p.stat().st_size for p in all_csv)
    lines.append(f"- Folder: `{config.RAW_DIR}`\n- CSV files found: {len(all_csv)} ({human(total)} total)\n")

    # ---- national wide files
    lines.append("\n## National files (one Date column + one column per state)\n")
    for name in (config.ENERGY_FILE, config.PEAK_MET_FILE, config.PEAK_UNMET_FILE):
        p = config.RAW_DIR / name
        if not p.exists():
            lines.append(f"- `{name}`: **MISSING**\n")
            print(f"  ! missing {name}")
            continue
        df = spark.read.csv(str(p), header=True, inferSchema=True)
        date_col = next(c for c in df.columns if c.strip().lower() == "date")
        d = F.to_date(F.col(date_col).cast("string"))
        stats = df.agg(F.count("*").alias("rows"), F.min(d).alias("first"), F.max(d).alias("last")).first()
        known = [c for c in df.columns if c.strip() in config.REGIONS]
        extra = [c for c in df.columns if c.strip() != date_col.strip() and c.strip() not in config.REGIONS]
        unique_dates = df.select(F.to_date(F.col(f"`{date_col}`").cast("string")).alias("_date")).agg(
            F.countDistinct("_date").alias("n")
        ).first()["n"]
        exact_duplicate_rows = int(stats["rows"] - df.dropDuplicates().count())
        date_df = df.select(F.to_date(F.col(f"`{date_col}`").cast("string")).alias("_date"))
        duplicate_date_groups = date_df.groupBy("_date").count().filter(
            F.col("_date").isNotNull() & (F.col("count") > 1)
        ).count()
        span_days = None
        missing_calendar_dates = None
        if stats["first"] is not None and stats["last"] is not None and unique_dates is not None:
            span_days = (stats["last"] - stats["first"]).days + 1
            missing_calendar_dates = int(span_days - unique_dates)
        numeric_cols = list(known)
        quality_exprs = []
        for i, c in enumerate(numeric_cols):
            value = F.col(f"`{c}`").cast("double")
            quality_exprs.append(F.sum(value.isNull().cast("long")).alias(f"_null_{i}"))
            quality_exprs.append(F.sum((value < 0).cast("long")).alias(f"_negative_{i}"))
        quality_row = df.agg(*quality_exprs).first().asDict() if quality_exprs else {}
        null_counts = {
            c: int(quality_row.get(f"_null_{i}") or 0)
            for i, c in enumerate(numeric_cols)
            if int(quality_row.get(f"_null_{i}") or 0) > 0
        }
        negative_counts = {
            c: int(quality_row.get(f"_negative_{i}") or 0)
            for i, c in enumerate(numeric_cols)
            if int(quality_row.get(f"_negative_{i}") or 0) > 0
        }
        all_null_states = [c for c in numeric_cols if null_counts.get(c, 0) == int(stats["rows"])]
        lines.append(
            f"- `{name}` ({human(p.stat().st_size)}): {stats['rows']:,} rows, {stats['first']} to {stats['last']}, "
            f"{len(known)} state columns; non-state columns that the pipeline drops: {extra or 'none'}\n"
            f"  - Unique dates: {unique_dates:,}\n"
            f"  - Exact duplicate rows: {exact_duplicate_rows:,}\n"
            f"  - Duplicate-date groups: {duplicate_date_groups:,}\n"
            f"  - Missing calendar dates within observed range: {missing_calendar_dates}\n"
            f"  - Entirely empty state columns: {all_null_states or 'none'}\n"
            f"  - State columns with negative values (cell counts): {negative_counts or 'none'}\n"
            f"  - State columns with null values (cell counts): {null_counts or 'none'}\n"
        )
        print(f"  {name}: rows={stats['rows']:,}, unique_dates={unique_dates:,}, duplicate_date_groups={duplicate_date_groups}, exact_duplicate_rows={exact_duplicate_rows}")

    # ---- state weather files
    files = config.state_weather_files()
    lines.append(f"\n## State weather files ({len(files)} found)\n")
    if files:
        w = spark.read.csv([str(p) for p in files], header=True, inferSchema=True)
        w = w.select("*", F.col("_metadata.file_name").alias("_file"))
        date_col = next(c for c in w.columns if c.strip().lower() == "date")
        wd = F.to_date(F.col(date_col).cast("string"))
        per_file = (
            w.groupBy("_file")
            .agg(F.count("*").alias("rows"), F.min(wd).alias("first"), F.max(wd).alias("last"))
            .orderBy("_file")
            .collect()
        )
        total_rows = sum(r["rows"] for r in per_file)
        lines.append(f"- Total weather rows across all state files: **{total_rows:,}**\n")
        lines.append(f"- Columns ({len(w.columns) - 1}) and inferred types:\n")
        for name, dtype in w.dtypes:
            if name != "_file":
                lines.append(f"  - `{name}` : {dtype}\n")
        lines.append("\n| File | Rows | First date | Last date |\n|---|---:|---|---|\n")
        for r in per_file:
            lines.append(f"| {r['_file']} | {r['rows']:,} | {r['first']} | {r['last']} |\n")
        print(f"  weather: {len(files)} files, {total_rows:,} rows, {len(w.columns) - 1} columns")
        w.drop("_file").printSchema()

        num_cols = [c for c, t in w.dtypes if t in ("double", "int", "bigint", "float") and c != "_file"]
        nulls = w.agg(*[F.sum(F.col(f"`{c}`").isNull().cast("int")).alias(c) for c in num_cols]).first().asDict()
        bad = {k: v for k, v in nulls.items() if v}
        lines.append(f"\n- Columns with missing values: {bad or 'none'}\n")
    else:
        lines.append("- No state weather files found.\n")

    out = config.RESULTS_DIR / "schema_report.md"
    out.write_text("".join(lines), encoding="utf-8")
    print(f"\nWrote {out}")
    spark.stop()


if __name__ == "__main__":
    main()
