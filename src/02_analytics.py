"""Step 2 - Spark analytics (DataFrame API + Spark SQL).

Reads data/processed/demand_weather.parquet and writes small result tables to results/*.csv
(the dashboard and the report use these). Every query is timed and listed in
results/query_catalog.csv together with the Big Data technique it demonstrates.

    python src/02_analytics.py
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

import config
from common import banner, find_column, get_spark, timer, write_json

CATALOG: list[dict] = []


def save(name: str, df, query: str, technique: str, what: str) -> pd.DataFrame:
    """Run the Spark query, bring the (small) result to pandas and write it as CSV."""
    import time

    t0 = time.perf_counter()
    pdf = df.toPandas()
    secs = time.perf_counter() - t0
    pdf.to_csv(config.RESULTS_DIR / f"{name}.csv", index=False)
    CATALOG.append({"query": query, "output_file": f"{name}.csv", "technique": technique,
                    "what_it_answers": what, "result_rows": len(pdf), "seconds": round(secs, 2)})
    print(f"  [{secs:6.2f}s] {name}.csv  ({len(pdf):,} rows)")
    return pdf


def main() -> None:
    banner(f"STEP 2 - Spark analytics ({config.MODE} mode)")
    spark = get_spark("PowerPulse-analytics")
    path = config.PROC_DIR / "demand_weather.parquet"
    if not path.exists():
        raise SystemExit("Run `python src/01_ingest_clean.py` first.")
    d = spark.read.parquet(str(path)).cache()
    analysis_rows = d.count()
    print(f"  analysis table: {analysis_rows:,} rows, {len(d.columns)} columns")

    # Guard against duplicate date-state keys inflating state/day counts.
    duplicate_key_count = (
        d.groupBy("date", "state").count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_key_count:
        raise SystemExit(
            f"Analysis table has {duplicate_key_count} duplicate date/state keys. "
            "Rerun the corrected ingestion step before analytics."
        )
    available_states = d.select("state").distinct().count()
    print(f"  validated unique date-state keys; states represented: {available_states}")

    # weather columns are located by name pattern so the script keeps working if the exact set differs
    temp = find_column(d.columns, ["temp", "mean"], ["dewpoint"], exact=["temp_mean"])
    temp_max = find_column(d.columns, ["temp", "max"], ["dewpoint"], exact=["temp_max"])
    meta = {"temp_mean_col": temp, "temp_max_col": temp_max, "min_days_full_year": config.MIN_DAYS_FULL_YEAR}
    write_json(meta, config.RESULTS_DIR / "meta.json")
    d.createOrReplaceTempView("demand")
    MIN_DAYS = config.MIN_DAYS_FULL_YEAR

    # ------------------------------------------------------------------ Q1 annual state demand
    # Technique: Spark SQL group-by aggregation + window function (year-over-year growth)
    banner("Q1  Annual electricity supplied by state")
    q1 = spark.sql("""
        SELECT state, region_code, region, year,
               ROUND(SUM(energy_met_mu), 1)  AS total_energy_mu,
               ROUND(AVG(energy_met_mu), 2)  AS avg_daily_mu,
               COUNT(energy_met_mu)          AS n_days
        FROM demand
        GROUP BY state, region_code, region, year
    """)
    w_state = Window.partitionBy("state").orderBy("year")
    complete = F.col("n_days") >= MIN_DAYS
    q1 = (
        q1.withColumn("is_full_year", complete)
        .withColumn("prev_full", F.lag(F.when(complete, F.col("avg_daily_mu"))).over(w_state))
        .withColumn(
            "yoy_growth_pct",
            F.when(complete & F.col("prev_full").isNotNull(), F.round((F.col("avg_daily_mu") / F.col("prev_full") - 1) * 100, 2)),
        )
        .drop("prev_full")
        .orderBy("state", "year")
    )
    save("q1_annual_state_demand", q1, "Q1 Annual state demand", "Spark SQL GROUP BY + window lag (YoY)",
         "How much electricity each state received per year and how fast it grows (only years with >= "
         f"{MIN_DAYS} reported days count as full years)")

    # ------------------------------------------------------------------ Q2 peak demand analysis
    # Technique: ranking window functions
    banner("Q2  Peak-demand analysis")
    w_rank = Window.partitionBy("state").orderBy(F.col("peak_met_mw").desc())
    ranked = d.where(F.col("peak_met_mw").isNotNull()).withColumn("rank", F.row_number().over(w_rank))
    top10 = ranked.where("rank <= 10").select("state", "region_code", "rank", "date", "season", "peak_met_mw",
                                              F.col(temp).alias("temp_mean_c")).orderBy("state", "rank")
    save("q2_top_peak_days_by_state", top10, "Q2 Top 10 peak days per state", "Window function row_number()",
         "The ten days with the highest peak power supplied in each state, with the day's mean temperature")

    top30 = ranked.where("rank <= 30")
    season_share = (
        top30.groupBy("state", "season").count()
        .withColumn("share_pct", F.round(F.col("count") / F.sum("count").over(Window.partitionBy("state")) * 100, 1))
        .orderBy("state", "season")
    )
    save("q2_peak_season_share", season_share, "Q2 Season of each state's 30 highest peaks",
         "Window function + group-by", "In which season does each state hit its highest peaks")

    nat = d.groupBy("date").agg(F.sum("peak_met_mw").alias("sum_of_state_peaks_mw"), F.count("peak_met_mw").alias("n_states"))
    max_states = nat.agg(F.max("n_states")).first()[0]
    nat_top = nat.where(F.col("n_states") >= 0.8 * max_states).orderBy(F.col("sum_of_state_peaks_mw").desc()).limit(15)
    if nat_top.filter(F.col("n_states") > available_states).limit(1).count():
        raise SystemExit("National peak-day output has more states than the analysis dataset.")
    save("q2_national_peak_days", nat_top, "Q2 Highest all-India days (sum of state peaks)", "Group-by + filter + sort",
         "Days with the highest SUM of state-level peaks. Note: state peaks happen at different hours, so this is "
         "not the same as the simultaneous national peak")

    # ------------------------------------------------------------------ Q3 unmet peak demand
    # Technique: conditional aggregation
    banner("Q3  Unmet peak demand (supply shortfall)")
    unmet_ratio = F.col("peak_unmet_mw") / (F.col("peak_met_mw") + F.col("peak_unmet_mw"))
    q3 = (
        d.where(F.col("peak_unmet_mw").isNotNull() & F.col("peak_met_mw").isNotNull())
        .groupBy("region_code", "region", "state", "year")
        .agg(
            F.count("*").alias("days_reported"),
            F.sum((F.col("peak_unmet_mw") > 0).cast("int")).alias("days_with_shortfall"),
            F.round(F.avg("peak_unmet_mw"), 1).alias("avg_unmet_mw"),
            F.max("peak_unmet_mw").alias("max_unmet_mw"),
            F.round(F.avg(unmet_ratio) * 100, 3).alias("avg_unmet_pct_of_demand"),
        )
        .withColumn("shortfall_days_pct", F.round(F.col("days_with_shortfall") / F.col("days_reported") * 100, 1))
        .orderBy("state", "year")
    )
    save("q3_unmet_by_state_year", q3, "Q3 Unmet peak demand by state and year", "Filtering + conditional aggregation",
         "How often and how severely each state's peak demand was not fully met")

    q3_region = (
        d.where(F.col("peak_unmet_mw").isNotNull() & F.col("peak_met_mw").isNotNull())
        .groupBy("region_code", "region", "year")
        .agg(F.round(F.sum("peak_unmet_mw"), 0).alias("total_unmet_mw_days"),
             F.round(F.avg(unmet_ratio) * 100, 3).alias("avg_unmet_pct_of_demand"))
        .orderBy("region_code", "year")
    )
    save("q3_unmet_by_region_year", q3_region, "Q3 Unmet peak demand by grid region", "Group-by aggregation",
         "Regional shortfall pattern over the years")

    # ------------------------------------------------------------------ Q4 demand vs weather
    # Technique: window function to remove growth trend + corr() aggregate + binning
    banner("Q4  Demand versus weather")
    wy = Window.partitionBy("state", "year")
    base = (
        d.where(F.col(temp).isNotNull() & F.col("energy_met_mu").isNotNull())
        .withColumn("n_year", F.count("energy_met_mu").over(wy))
        .where(F.col("n_year") >= MIN_DAYS)                       # full years only
        .withColumn("rel_energy", F.col("energy_met_mu") / F.avg("energy_met_mu").over(wy))   # 1.0 = that year's average day
    )
    corr_state = (
        base.groupBy("state", "region_code", "region")
        .agg(
            F.count("*").alias("n_days"),
            F.round(F.avg(temp), 2).alias("avg_temp_c"),
            F.round(F.corr(temp, "energy_met_mu"), 3).alias("corr_temp_energy_raw"),
            F.round(F.corr(temp, "rel_energy"), 3).alias("corr_temp_relative_energy"),
        )
        .orderBy(F.col("corr_temp_relative_energy").desc())
    )
    save("q4_temp_demand_correlation", corr_state, "Q4 Temperature-demand correlation per state",
         "Window function (de-trending) + corr() aggregate",
         "Observed association between daily mean temperature and demand. 'relative' divides each day by the state's "
         "average of that year to remove growth and state size. Association only - not proof of causation")

    bins = (
        base.withColumn("temp_bin_c", (F.floor(F.col(temp) / 5) * 5).cast("int"))
        .groupBy("region_code", "region", "temp_bin_c")
        .agg(F.round(F.avg("rel_energy"), 4).alias("avg_relative_demand"), F.count("*").alias("n_days"))
        .where("n_days >= 20")
        .orderBy("region_code", "temp_bin_c")
    )
    save("q4_demand_by_temperature_bin", bins, "Q4 Relative demand by temperature band", "Join-derived feature + binning + group-by",
         "Average relative demand in 5 degC temperature bands for each grid region")

    # ------------------------------------------------------------------ Q5 seasonal comparisons
    # Technique: date functions, Spark SQL
    banner("Q5  Seasonal and calendar patterns")
    q5_month = spark.sql(f"""
        SELECT state, region_code, year, month,
               ROUND(AVG(energy_met_mu), 2) AS avg_daily_mu,
               ROUND(AVG({temp}), 2)        AS avg_temp_c,
               COUNT(energy_met_mu)         AS n_days
        FROM demand
        GROUP BY state, region_code, year, month
    """).orderBy("state", "year", "month")
    save("q5_monthly_state_year", q5_month, "Q5 Monthly demand by state and year", "Spark SQL + date functions",
         "Month-by-month average daily demand for every state and year (heatmaps)")

    q5_season = (
        base.groupBy("region_code", "region", "season")
        .agg(F.round(F.avg("rel_energy"), 4).alias("avg_relative_demand"),
             F.round(F.avg(temp), 2).alias("avg_temp_c"), F.count("*").alias("n_days"))
        .orderBy("region_code", "season")
    )
    save("q5_season_by_region", q5_season, "Q5 Season comparison by region", "Group-by on derived season column",
         "How much busier than an average day is each season in each region")

    wk = (
        base.groupBy("state", "region_code")
        .pivot("is_weekend", [False, True])
        .agg(F.avg("rel_energy"))
        .withColumnRenamed("false", "weekday_rel").withColumnRenamed("true", "weekend_rel")
        .withColumn("weekend_vs_weekday_pct", F.round((F.col("weekend_rel") / F.col("weekday_rel") - 1) * 100, 2))
        .orderBy("weekend_vs_weekday_pct")
    )
    save("q5_weekend_effect", wk, "Q5 Weekday vs weekend demand", "Pivot aggregation",
         "How much lower (or higher) demand is on weekends than on weekdays in each state")

    # ------------------------------------------------------------------ extras for the dashboard
    banner("Extra tables for the dashboard")
    region_monthly = (
        d.groupBy("region_code", "region", "year", "month")
        .agg(F.round(F.sum("energy_met_mu"), 1).alias("total_energy_mu"))
        .orderBy("region_code", "year", "month")
    )
    save("x_region_monthly", region_monthly, "Extra: Region monthly totals", "Group-by", "Monthly energy per grid region")

    w30 = Window.orderBy("date").rowsBetween(-29, 0)
    nat_daily = (
        d.groupBy("date").agg(F.round(F.sum("energy_met_mu"), 1).alias("total_energy_mu"),
                              F.count("energy_met_mu").alias("n_states"))
        .withColumn("rolling_30d_mu", F.round(F.avg("total_energy_mu").over(w30), 1))
        .orderBy("date")
    )
    save("x_national_daily", nat_daily, "Extra: All-India daily total with 30-day rolling mean", "Window function (rolling average)",
         "All-India daily energy supplied (sum over states) and its 30-day rolling mean")

    kpi = d.agg(
        F.round(F.sum("energy_met_mu") / 1000, 1).alias("total_energy_twh"),
        F.countDistinct("state").alias("states"),
        F.min("date").alias("first_date"), F.max("date").alias("last_date"),
        F.count("*").alias("rows"),
    ).first().asDict()
    kpi["data_is_synthetic"] = (config.RAW_DIR / "SYNTHETIC.txt").exists()
    write_json(kpi, config.RESULTS_DIR / "kpi_summary.json")
    pd.DataFrame(CATALOG).to_csv(config.RESULTS_DIR / "query_catalog.csv", index=False)
    print(f"\n  query catalog -> {config.RESULTS_DIR / 'query_catalog.csv'}")
    spark.stop()


if __name__ == "__main__":
    main()
