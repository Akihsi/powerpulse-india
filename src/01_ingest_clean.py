"""Step 1 - Ingest and clean with PySpark, then store as partitioned Parquet.



Input : data/raw/*.csv (Zenodo files)

Output: data/processed/demand_long.parquet     one row per (date, state): energy, peak met, peak unmet

        data/processed/weather_clean.parquet   one row per (date, state): weather variables

        data/processed/demand_weather.parquet  demand joined with weather (the analysis table)

        results/data_quality_report.json       counts of what was dropped / fixed



    python src/01_ingest_clean.py

"""

from __future__ import annotations



from pyspark.sql import Window  # noqa: F401

from pyspark.sql import functions as F



import config

from common import banner, clean_column_name, get_spark, timer, write_json





def load_wide(spark, filename: str, value_name: str):
    """Read a wide national file and conservatively resolve duplicate dates.

    Exact duplicate rows are removed. For duplicate dates, values that agree
    are retained (including complementary nulls); only individual state/date
    cells with conflicting non-null values are set to NULL. All source rows
    for dates with conflicts are written to an audit CSV. No conflicting
    value is selected or averaged arbitrarily.
    """
    path = config.RAW_DIR / filename
    if not path.exists():
        raise SystemExit(f"Missing {path}. Run `python src/download_data.py` first.")

    df = spark.read.csv(str(path), header=True, inferSchema=True)
    df = df.toDF(*[c.strip() for c in df.columns])
    date_col = next((c for c in df.columns if c.lower() == "date"), None)
    if date_col is None:
        raise SystemExit(f"No Date column found in {path}")

    state_cols = [c for c in df.columns if c in config.REGIONS]
    # Keep the source's Total series independently when present. It is never
    # recomputed from state values and remains a separate series downstream.
    value_cols = state_cols + (["Total"] if "Total" in df.columns and "Total" not in state_cols else [])
    if not value_cols:
        raise SystemExit(f"No recognised state columns found in {path}. Check config.REGIONS.")
    dropped = [c for c in df.columns if c != date_col and c not in value_cols]

    wide = df.select(
        F.to_date(F.col(f"`{date_col}`").cast("string")).alias("date"),
        *[
            F.nanvl(F.col(f"`{c}`").cast("double"), F.lit(None).cast("double")).alias(c)
            for c in value_cols
        ],
    )

    invalid_dates = wide.filter(F.col("date").isNull())
    if invalid_dates.limit(1).count():
        audit_path = config.RESULTS_DIR / f"{path.stem}_invalid_dates"
        (invalid_dates.coalesce(1).write.mode("overwrite")
         .option("header", True).csv(str(audit_path)))
        raise SystemExit(f"Invalid or unparseable dates in {filename}; review {audit_path}")

    original_rows = wide.count()
    exact_duplicate_rows = original_rows - wide.dropDuplicates().count()
    wide = wide.dropDuplicates()

    duplicate_dates = (
        wide.groupBy("date").count()
        .filter(F.col("count") > 1)
        .select("date")
    )
    duplicate_date_groups = duplicate_dates.count()
    conflict_date_count = 0
    conflict_cell_count = 0

    if duplicate_date_groups:
        count_exprs = [
            F.countDistinct(F.col(c)).alias(f"_n_{i}")
            for i, c in enumerate(value_cols)
        ]
        value_exprs = [
            F.first(F.col(c), ignorenulls=True).alias(f"_v_{i}")
            for i, c in enumerate(value_cols)
        ]
        grouped = wide.groupBy("date").agg(*(count_exprs + value_exprs))

        conflict_condition = None
        for i in range(len(value_cols)):
            condition = F.col(f"_n_{i}") > 1
            conflict_condition = condition if conflict_condition is None else (
                conflict_condition | condition
            )
        conflicts = grouped.filter(conflict_condition).select("date")
        conflict_date_count = conflicts.count()

        if conflict_date_count:
            audit_path = config.RESULTS_DIR / f"{path.stem}_duplicate_date_conflicts"
            (wide.join(conflicts, "date", "inner").orderBy("date")
             .coalesce(1).write.mode("overwrite")
             .option("header", True).csv(str(audit_path)))

        # Count conflicting date/value cells from the per-date aggregate rows.
        # Collect only the compact count columns (roughly one row per date), not
        # the full source data. Avoid nesting a second aggregate over aliases
        # that are themselves produced by countDistinct.
        count_columns = [f"_n_{i}" for i in range(len(value_cols))]
        conflict_cell_count = sum(
            1
            for row in grouped.select(*count_columns).collect()
            for column in count_columns
            if row[column] is not None and row[column] > 1
        )

        # Preserve a value only if all non-null observations agree. For a
        # conflicting state/date cell, retain NULL rather than choosing a row.
        resolved_cols = [
            F.when(F.col(f"_n_{i}") <= 1, F.col(f"_v_{i}"))
             .otherwise(F.lit(None).cast("double")).alias(c)
            for i, c in enumerate(value_cols)
        ]
        wide = grouped.select("date", *resolved_cols)

    diagnostics = {
        "raw_rows": original_rows,
        "exact_duplicate_rows_removed": exact_duplicate_rows,
        "duplicate_date_groups_before_resolution": duplicate_date_groups,
        "conflicting_date_groups_audited": conflict_date_count,
        "conflicting_state_date_cells_set_to_null": conflict_cell_count,
        "unique_dates_after_resolution": wide.select("date").distinct().count(),
    }
    long = wide.unpivot("date", value_cols, "state", value_name)
    long = long.withColumn(
        value_name, F.nanvl(F.col(value_name), F.lit(None).cast("double"))
    )
    return long, dropped, diagnostics


def season_expr(month_col):

    return (

        F.when(month_col.isin(12, 1, 2), "Winter")

        .when(month_col.isin(3, 4, 5), "Summer")

        .when(month_col.isin(6, 7, 8, 9), "Monsoon")

        .otherwise("Post-monsoon")

    )





def main() -> None:

    banner(f"STEP 1 - Ingest & clean ({config.MODE} mode)")

    spark = get_spark("PowerPulse-ingest")

    report: dict = {"mode": config.MODE, "raw_dir": str(config.RAW_DIR)}

    times: dict = {}



    # -------------------------------------------------------------- demand (3 wide files -> long)

    with timer("read + unpivot the 3 national files", times):

        energy, drop1, diag1 = load_wide(spark, config.ENERGY_FILE, "energy_met_mu")

        peak, drop2, diag2 = load_wide(spark, config.PEAK_MET_FILE, "peak_met_mw")

        unmet, drop3, diag3 = load_wide(spark, config.PEAK_UNMET_FILE, "peak_unmet_mw")

        report["national_file_diagnostics"] = {

            config.ENERGY_FILE: diag1,

            config.PEAK_MET_FILE: diag2,

            config.PEAK_UNMET_FILE: diag3,

        }

        demand = energy.join(peak, ["date", "state"], "outer").join(unmet, ["date", "state"], "outer")

        demand = demand.cache()

        report["demand_rows_raw_long"] = demand.count()

    report["non_state_columns_dropped"] = sorted(set(drop1 + drop2 + drop3))

    print(f"  dropped non-state columns: {report['non_state_columns_dropped']}")



    # Preserve suspicious source values before the existing cleaning policy changes them.

    # Negative/zero values are flagged for audit; they are not silently rewritten in the source.

    invalid_condition = (

        (F.col("energy_met_mu").isNotNull() & (F.col("energy_met_mu") <= 0))

        | (F.col("peak_met_mw").isNotNull() & (F.col("peak_met_mw") <= 0))

        | (F.col("peak_unmet_mw").isNotNull() & (F.col("peak_unmet_mw") < 0))

    )

    invalid_rows = demand.filter(invalid_condition).select(

        "date", "state", "energy_met_mu", "peak_met_mw", "peak_unmet_mw"

    )

    invalid_count = invalid_rows.count()

    report["invalid_measurement_rows_audited"] = invalid_count

    if invalid_count:

        invalid_path = config.RESULTS_DIR / "invalid_electricity_measurements"

        invalid_rows.coalesce(1).write.mode("overwrite").option("header", True).csv(str(invalid_path))



    # validity rules: energy and peak supplied must be > 0, unmet must be >= 0

    invalid = demand.agg(

        F.sum((F.col("energy_met_mu") <= 0).cast("int")).alias("energy_non_positive"),

        F.sum((F.col("peak_met_mw") <= 0).cast("int")).alias("peak_met_non_positive"),

        F.sum((F.col("peak_unmet_mw") < 0).cast("int")).alias("peak_unmet_negative"),

    ).first().asDict()

    report["invalid_values_set_to_null"] = {k: int(v or 0) for k, v in invalid.items()}

    demand = (

        demand.withColumn("energy_met_mu", F.when(F.col("energy_met_mu") > 0, F.col("energy_met_mu")))

        .withColumn("peak_met_mw", F.when(F.col("peak_met_mw") > 0, F.col("peak_met_mw")))

        .withColumn("peak_unmet_mw", F.when(F.col("peak_unmet_mw") >= 0, F.col("peak_unmet_mw")))

    )

    # a (date, state) row with no metric at all carries no information

    demand = demand.where(

        F.col("energy_met_mu").isNotNull() | F.col("peak_met_mw").isNotNull() | F.col("peak_unmet_mw").isNotNull()

    )

    observed_states = {r["state"] for r in demand.select("state").distinct().collect()}

    report["states_with_no_usable_electricity_data"] = sorted(set(config.REGIONS) - observed_states)

    print("  states/UTs without usable electricity data:", report["states_with_no_usable_electricity_data"] or "none")



    region_df = spark.createDataFrame(

        [(s, r, config.REGION_NAMES[r]) for s, r in config.REGIONS.items()],

        ["state", "region_code", "region"],

    )

    month = F.month("date")

    demand = (

        demand.join(F.broadcast(region_df), "state", "left")

        .withColumn("year", F.year("date"))

        .withColumn("month", month)

        .withColumn("season", season_expr(month))

        .withColumn("day_of_week", (F.dayofweek("date") + 5) % 7 + 1)      # 1=Mon ... 7=Sun

        .withColumn("is_weekend", (F.col("day_of_week") >= 6))

    )



    with timer("write demand_long.parquet", times):

        demand.write.mode("overwrite").partitionBy("year").parquet(str(config.PROC_DIR / "demand_long.parquet"))

    demand = spark.read.parquet(str(config.PROC_DIR / "demand_long.parquet"))

    n_demand = demand.count()

    report["demand_rows_clean"] = n_demand

    dr = demand.agg(F.min("date").alias("first"), F.max("date").alias("last"),

                    F.countDistinct("state").alias("states")).first()

    report["demand_first_date"], report["demand_last_date"] = str(dr["first"]), str(dr["last"])
    report["demand_states"] = demand.where(F.col("state") != "Total").select("state").distinct().count()

    report["demand_missing_values"] = demand.agg(*[

        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ("energy_met_mu", "peak_met_mw", "peak_unmet_mw")

    ]).first().asDict()

    print(f"  demand_long: {n_demand:,} rows, {dr['states']} states, {dr['first']} -> {dr['last']}")



    # -------------------------------------------------------------- weather (one file per state)

    files = config.state_weather_files()

    if not files:

        raise SystemExit(f"No state weather files in {config.RAW_DIR}.")

    with timer("read the per-state weather files", times):

        stems = spark.createDataFrame(

            [(p.name, config.state_from_stem(p.stem)) for p in files], ["_file", "state"]

        )

        w = spark.read.csv([str(p) for p in files], header=True, inferSchema=True)

        w = w.select("*", F.col("_metadata.file_name").alias("_file"))

        w = w.toDF(*[clean_column_name(c) if c != "_file" else c for c in w.columns])

        w = w.join(F.broadcast(stems), "_file").drop("_file")

        w = w.withColumn("date", F.to_date(F.col("date").cast("string"))).where(F.col("date").isNotNull())

        report["weather_files"] = len(files)

        report["weather_rows_raw"] = w.cache().count()



    # unit fixes: Kelvin -> Celsius for temperature-like columns (detected from the data, not assumed)

    temp_like = [c for c in w.columns if any(k in c for k in ("temp", "dewpoint", "utci"))]

    means = w.agg(*[F.avg(c).alias(c) for c in temp_like]).first().asDict() if temp_like else {}

    converted = [c for c, m in means.items() if m is not None and m > 150]

    for c in converted:

        w = w.withColumn(c, F.col(c) - F.lit(273.15))

    # solar radiation: accumulated J/m2 -> mean W/m2 (same convention as the dataset authors' code)

    solar_cols = [c for c in w.columns if "solar" in c]

    for c in solar_cols:

        w = w.withColumn(c, F.col(c) / F.lit(3600.0))

    report["weather_kelvin_to_celsius"] = converted

    report["weather_solar_converted_J_to_W_per_m2"] = solar_cols

    print(f"  converted to Celsius: {len(converted)} columns; solar columns converted: {len(solar_cols)}")



    # wind speed from u and v components (mean)

    if "wind_u_mean" in w.columns and "wind_v_mean" in w.columns:

        w = w.withColumn("wind_speed_mean", F.sqrt(F.col("wind_u_mean") ** 2 + F.col("wind_v_mean") ** 2))



    # Remove exact weather duplicates only. Never arbitrarily select one of two

    # different weather observations for the same date and state.

    before = w.count()

    w = w.dropDuplicates()

    report["weather_exact_duplicate_rows_removed"] = before - w.count()

    duplicate_keys = (

        w.groupBy("date", "state").count()

        .filter(F.col("count") > 1)

        .select("date", "state")

    )

    if duplicate_keys.limit(1).count():

        audit_path = config.RESULTS_DIR / "weather_duplicate_conflicts"

        (w.join(duplicate_keys, ["date", "state"], "inner")

         .orderBy("state", "date").coalesce(1)

         .write.mode("overwrite").option("header", True).csv(str(audit_path)))

        raise SystemExit(

            f"Conflicting weather rows share the same date/state key. Review {audit_path}."

        )

    w = w.withColumn("year", F.year("date"))



    with timer("write weather_clean.parquet", times):

        w.write.mode("overwrite").partitionBy("year").parquet(str(config.PROC_DIR / "weather_clean.parquet"))

    w = spark.read.parquet(str(config.PROC_DIR / "weather_clean.parquet"))

    report["weather_rows_clean"] = w.count()

    report["weather_columns"] = [c for c in w.columns if c not in ("date", "state", "year")]

    num_cols = [c for c, t in w.dtypes if t == "double"]

    report["weather_missing_values"] = {

        k: int(v) for k, v in w.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in num_cols]).first().asDict().items() if v

    }



    # -------------------------------------------------------------- join demand + weather

    with timer("join demand with weather", times):

        w_join = w.drop("year")

        joined = demand.join(w_join, ["date", "state"], "left")

        demand_key_count = demand.select("date", "state").distinct().count()

        joined_key_count = joined.select("date", "state").distinct().count()

        joined_row_count = joined.count()

        if joined_row_count != demand.count() or joined_key_count != demand_key_count:

            raise SystemExit(

                "Demand-weather join changed the demand row count. Check duplicate date/state keys "

                "in the cleaned demand or weather table before continuing."

            )

        report["demand_date_state_keys"] = demand_key_count

        report["joined_date_state_keys"] = joined_key_count

        joined.repartition("year").write.mode("overwrite").partitionBy("year").parquet(

            str(config.PROC_DIR / "demand_weather.parquet")

        )

    joined = spark.read.parquet(str(config.PROC_DIR / "demand_weather.parquet"))

    temp_probe = next((c for c in joined.columns if c.startswith("temp_")), None)

    report["joined_rows"] = joined.count()

    if temp_probe:

        report["joined_rows_without_weather"] = joined.where(F.col(temp_probe).isNull()).count()

        report["states_without_weather_file"] = sorted(

            r["state"] for r in joined.groupBy("state").agg(F.count(temp_probe).alias("n")).where("n = 0").collect()

        )

    report["step_times_seconds"] = {k: round(v, 2) for k, v in times.items()}

    report["data_is_synthetic"] = (config.RAW_DIR / "SYNTHETIC.txt").exists()



    write_json(report, config.RESULTS_DIR / "data_quality_report.json")

    print(f"\n  joined table: {report['joined_rows']:,} rows -> {config.PROC_DIR / 'demand_weather.parquet'}")

    print(f"  quality report -> {config.RESULTS_DIR / 'data_quality_report.json'}")

    spark.stop()





if __name__ == "__main__":

    main()
