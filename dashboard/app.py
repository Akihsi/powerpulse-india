"""PowerPulse India - Streamlit dashboard.

    streamlit run dashboard/app.py
    POWERPULSE_MODE=sample streamlit run dashboard/app.py     # synthetic test data (Windows: set POWERPULSE_MODE=sample)

The dashboard only reads the small CSV tables in results/ (produced by Spark in step 2/3) and,
for the scatter plot, one state's rows from the Parquet table. It does not need Spark.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import config  # noqa: E402
from common import read_json  # noqa: E402

R = config.RESULTS_DIR
INK, AMBER, TEAL, GREY = "#1D2433", "#E39B00", "#127C8A", "#6B7280"
REGION_COLORS = {"Northern": "#E39B00", "Western": "#C2410C", "Southern": "#127C8A",
                 "Eastern": "#6D5BD0", "North-Eastern": "#2F9E5B", "Islands": "#8A8F98"}
SEASON_ORDER = config.SEASON_ORDER

st.set_page_config(page_title="PowerPulse India", page_icon="⚡", layout="wide")
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', system-ui, sans-serif; }
    h1, h2, h3 { letter-spacing: -0.01em; color: #1D2433; }
    [data-testid="stMetric"] { border-left: 4px solid #E39B00; padding: 4px 0 4px 14px; }
    [data-testid="stMetricLabel"] p { color: #6B7280; }
    footer { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data
def load(name: str) -> pd.DataFrame | None:
    p = R / f"{name}.csv"
    return pd.read_csv(p) if p.exists() else None


def style(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(template="plotly_white", height=height, margin=dict(l=10, r=10, t=40, b=10),
                      font=dict(family="DM Sans, sans-serif", color=INK),
                      legend=dict(orientation="h", y=-0.2), title_font_size=15)
    return fig


kpi = read_json(R / "kpi_summary.json")
q1 = load("q1_annual_state_demand")
if kpi is None or q1 is None:
    st.title("PowerPulse India")
    st.error(f"No results found in `{R}`. Run `python run_all.py` (or `python run_all.py --sample` to test) first.")
    st.stop()

if kpi.get("data_is_synthetic"):
    st.warning("SYNTHETIC TEST DATA - these charts only prove the code works. None of these numbers are real.", icon="⚠️")

st.title("PowerPulse India")
st.caption("Daily electricity supplied to Indian states, with weather - analysed with PySpark. "
           f"{kpi['first_date']} to {kpi['last_date']} | {kpi['states']} states and UTs")

# ---- sidebar filters --------------------------------------------------------------------------
full = q1[q1["is_full_year"]]
all_regions = sorted(q1["region"].dropna().unique())
with st.sidebar:
    st.header("Filters")
    sel_regions = st.multiselect("Grid regions", all_regions, default=all_regions)
    ymin, ymax = int(full["year"].min()), int(full["year"].max())
    years = st.slider("Years (full years only)", ymin, ymax, (ymin, ymax)) if ymin < ymax else (ymin, ymax)
    st.caption("A year counts as 'full' for a state when it has at least "
               f"{config.MIN_DAYS_FULL_YEAR} reported days.")

f1 = full[full["region"].isin(sel_regions) & full["year"].between(*years)]
if f1.empty:
    st.info("No data for this filter selection.")
    st.stop()

tab_over, tab_state, tab_weather, tab_peak, tab_season, tab_bench, tab_about = st.tabs(
    ["Overview", "State trends", "Weather vs demand", "Peaks and shortfall", "Seasons and weekdays", "Spark benchmarks", "About the data"]
)

# ================================================================================ Overview
with tab_over:
    last_year = int(f1["year"].max())
    yr_tot = f1.groupby("year")["total_energy_mu"].sum()
    growth = (yr_tot.iloc[-1] / yr_tot.iloc[-2] - 1) * 100 if len(yr_tot) > 1 else None
    top_state = f1[f1["year"] == last_year].sort_values("total_energy_mu", ascending=False).iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"All-India supply, {last_year}", f"{yr_tot.iloc[-1] / 1000:,.0f} TWh",
              f"{growth:+.1f}% vs previous year" if growth is not None else None)
    c2.metric("Largest state", top_state["state"], f"{top_state['total_energy_mu'] / 1000:,.0f} TWh in {last_year}", delta_color="off")
    c3.metric("States and UTs in selection", f"{f1['state'].nunique()}")
    c4.metric("Rows analysed by Spark", f"{kpi['rows']:,}")

    nat = load("x_national_daily")
    if nat is not None:
        nat = nat.assign(date=pd.to_datetime(nat["date"]))
        nat = nat[nat["n_states"] >= nat["n_states"].max() * 0.8]
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=nat["date"], y=nat["total_energy_mu"], name="Daily total", line=dict(color="#F3D18A", width=1)))
        fig.add_trace(go.Scatter(x=nat["date"], y=nat["rolling_30d_mu"], name="30-day average", line=dict(color=AMBER, width=2.5)))
        fig.update_layout(title="All-India electricity supplied per day (sum of states, MU = GWh)", yaxis_title="MU per day")
        st.plotly_chart(style(fig), width="stretch")

    a, b = st.columns(2)
    with a:
        top = f1[f1["year"] == last_year].nlargest(12, "total_energy_mu").sort_values("total_energy_mu")
        fig = px.bar(top, x="total_energy_mu", y="state", color="region", color_discrete_map=REGION_COLORS, orientation="h",
                     title=f"Top 12 states by energy supplied, {last_year}", labels={"total_energy_mu": "MU", "state": ""})
        st.plotly_chart(style(fig, 420), width="stretch")
    with b:
        reg = f1.groupby(["year", "region"], as_index=False)["total_energy_mu"].sum()
        fig = px.area(reg, x="year", y="total_energy_mu", color="region", color_discrete_map=REGION_COLORS,
                      title="Energy supplied by grid region", labels={"total_energy_mu": "MU per year"})
        st.plotly_chart(style(fig, 420), width="stretch")

# ================================================================================ State trends
with tab_state:
    states = sorted(f1["state"].unique())
    default = list(f1[f1["year"] == f1["year"].max()].nlargest(4, "total_energy_mu")["state"])
    chosen = st.multiselect("Compare states", states, default=default)
    if chosen:
        d = f1[f1["state"].isin(chosen)]
        fig = px.line(d, x="year", y="avg_daily_mu", color="state", markers=True,
                      title="Average daily energy supplied (MU per day)", labels={"avg_daily_mu": "MU per day"})
        st.plotly_chart(style(fig), width="stretch")
        g = d.pivot(index="year", columns="state", values="yoy_growth_pct").round(1)
        st.subheader("Year-over-year growth (%)")
        st.dataframe(g, width="stretch")

        one = st.selectbox("Monthly pattern for", chosen)
        m = load("q5_monthly_state_year")
        if m is not None:
            md = m[(m["state"] == one) & (m["n_days"] >= 20)].pivot(index="year", columns="month", values="avg_daily_mu")
            fig = px.imshow(md, aspect="auto", color_continuous_scale="YlOrBr", labels=dict(x="Month", y="Year", color="MU/day"),
                            title=f"{one}: average daily demand by month and year")
            st.plotly_chart(style(fig, 420), width="stretch")

# ================================================================================ Weather
with tab_weather:
    st.caption("Everything on this tab shows an observed association between temperature and demand. "
               "It does not prove that temperature causes the change: seasons, holidays, farming and industry also vary through the year.")
    corr = load("q4_temp_demand_correlation")
    bins = load("q4_demand_by_temperature_bin")
    if corr is not None:
        corr = corr[corr["region"].isin(sel_regions)].sort_values("corr_temp_relative_energy")
        fig = px.bar(corr, x="corr_temp_relative_energy", y="state", color="region", color_discrete_map=REGION_COLORS, orientation="h",
                     title="Correlation between daily temperature and relative demand",
                     labels={"corr_temp_relative_energy": "Correlation (-1 to +1)", "state": ""})
        st.plotly_chart(style(fig, max(380, 18 * len(corr))), width="stretch")
        st.caption("Relative demand = a day's demand divided by that state's average for the same year, so growth over time and state size do not distort the comparison.")
    if bins is not None:
        bb = bins[bins["region"].isin(sel_regions)]
        fig = px.line(bb, x="temp_bin_c", y="avg_relative_demand", color="region", markers=True,
                      color_discrete_map=REGION_COLORS, title="Relative demand by temperature band (5 °C bands)",
                      labels={"temp_bin_c": "Daily mean temperature, lower edge of band (°C)", "avg_relative_demand": "Relative demand (1.0 = average day)"})
        st.plotly_chart(style(fig), width="stretch")

    meta = read_json(R / "meta.json", {})
    pq = config.PROC_DIR / "demand_weather.parquet"
    if pq.exists() and meta:
        tcol = meta["temp_mean_col"]
        pick = st.selectbox("Daily scatter for state", sorted(f1["state"].unique()), key="scatter_state")
        sd = pd.read_parquet(pq, columns=["date", "state", "energy_met_mu", tcol], filters=[("state", "==", pick)]).dropna()
        sd["date"] = pd.to_datetime(sd["date"])
        sd["year"] = sd["date"].dt.year
        cnt = sd.groupby("year")["energy_met_mu"].transform("count")
        sd = sd[cnt >= config.MIN_DAYS_FULL_YEAR].copy()
        sd["relative_demand"] = sd["energy_met_mu"] / sd.groupby("year")["energy_met_mu"].transform("mean")
        if not sd.empty:
            fig = px.scatter(sd, x=tcol, y="relative_demand", color="year", opacity=0.55, color_continuous_scale="YlOrBr",
                             title=f"{pick}: each dot is one day", labels={tcol: "Daily mean temperature (°C)", "relative_demand": "Relative demand"})
            st.plotly_chart(style(fig, 440), width="stretch")

# ================================================================================ Peaks and shortfall
with tab_peak:
    pk = load("q2_top_peak_days_by_state")
    ss = load("q2_peak_season_share")
    if pk is not None:
        pick = st.selectbox("State", sorted(pk["state"].unique()), key="peak_state")
        left, right = st.columns([3, 2])
        with left:
            t = pk[pk["state"] == pick][["rank", "date", "season", "peak_met_mw", "temp_mean_c"]].rename(
                columns={"peak_met_mw": "peak supplied (MW)", "temp_mean_c": "mean temp (°C)"})
            st.subheader(f"{pick}: 10 highest peak days")
            st.dataframe(t.round(1), hide_index=True, width="stretch")
        with right:
            if ss is not None:
                s = ss[ss["state"] == pick]
                fig = px.bar(s, x="season", y="share_pct", category_orders={"season": SEASON_ORDER}, color_discrete_sequence=[AMBER],
                             title="Season of the 30 highest peaks (%)", labels={"share_pct": "%", "season": ""})
                st.plotly_chart(style(fig, 320), width="stretch")
    npk = load("q2_national_peak_days")
    if npk is not None:
        st.subheader("Highest all-India days")
        st.caption("Sum of state-level daily peaks. States peak at different hours, so this is not the single simultaneous national peak.")
        st.dataframe(npk.round(0), hide_index=True, width="stretch")

    ur = load("q3_unmet_by_region_year")
    us = load("q3_unmet_by_state_year")
    st.subheader("Unmet peak demand")
    if ur is not None:
        fig = px.line(ur[ur["region"].isin(sel_regions)], x="year", y="avg_unmet_pct_of_demand", color="region", markers=True,
                      color_discrete_map=REGION_COLORS, title="Average unmet peak demand, % of demand",
                      labels={"avg_unmet_pct_of_demand": "% of peak demand"})
        st.plotly_chart(style(fig), width="stretch")
    if us is not None:
        agg = (us[us["region"].isin(sel_regions)].groupby(["state", "region"], as_index=False)
               .agg(days=("days_reported", "sum"), shortfall=("days_with_shortfall", "sum")))
        agg["shortfall_days_pct"] = (agg["shortfall"] / agg["days"] * 100).round(1)
        fig = px.bar(agg.nlargest(15, "shortfall_days_pct").sort_values("shortfall_days_pct"), x="shortfall_days_pct", y="state",
                     color="region", color_discrete_map=REGION_COLORS, orientation="h",
                     title="States with the highest share of days with an unmet peak",
                     labels={"shortfall_days_pct": "% of reported days", "state": ""})
        st.plotly_chart(style(fig, 440), width="stretch")

# ================================================================================ Seasons
with tab_season:
    se = load("q5_season_by_region")
    wk = load("q5_weekend_effect")
    if se is not None:
        fig = px.bar(se[se["region"].isin(sel_regions)], x="season", y="avg_relative_demand", color="region", barmode="group",
                     category_orders={"season": SEASON_ORDER}, color_discrete_map=REGION_COLORS,
                     title="Relative demand by season (1.0 = average day of the year)", labels={"avg_relative_demand": "Relative demand", "season": ""})
        fig.update_yaxes(range=[0.85, None])
        st.plotly_chart(style(fig), width="stretch")
        st.caption("Winter = Dec-Feb, Summer = Mar-May, Monsoon = Jun-Sep, Post-monsoon = Oct-Nov.")
    if wk is not None:
        reg_lookup = q1.drop_duplicates("state").set_index("state")["region"]
        wk = wk.assign(region=wk["state"].map(reg_lookup))
        wk = wk[wk["region"].isin(sel_regions)].sort_values("weekend_vs_weekday_pct")
        fig = px.bar(wk, x="weekend_vs_weekday_pct", y="state", color="region", color_discrete_map=REGION_COLORS, orientation="h",
                     title="Weekend demand compared with weekdays (%)", labels={"weekend_vs_weekday_pct": "% difference", "state": ""})
        st.plotly_chart(style(fig, max(380, 18 * len(wk))), width="stretch")

# ================================================================================ Benchmarks
with tab_bench:
    bs = load("benchmarks_summary")
    env = read_json(R / "benchmark_environment.json")
    if bs is None:
        st.info("Run `python src/03_benchmarks.py` to measure Spark performance on your machine.")
    else:
        if env:
            st.caption(f"Measured on: {env['os']} | {env['cpu_cores']} CPU cores | RAM {env['ram_gb']} GB | Python {env['python']} | "
                       f"PySpark {env['pyspark']} | pandas {env['pandas']} | {env['benchmark_rows']:,} rows x {env['benchmark_columns']} columns | "
                       f"median of {env['timed_runs_per_variant']} runs")
        st.write("All times are measured, not estimated. On a dataset of this size Pandas is often faster than Spark; "
                 "the point of the experiments is to show how the Big Data techniques behave and where Spark starts to pay off.")
        names = {"A_format": "A. File format", "B_cache": "B. Caching", "C_partitions": "C. Shuffle partitions",
                 "D_pandas_spark": "D. Pandas vs Spark", "E_scale": "E. Scale-up stress test (replicated data)"}
        for key, title in names.items():
            d = bs[(bs["experiment"] == key) & (~bs["variant"].str.contains("size on disk"))]
            if d.empty:
                continue
            fig = px.bar(d, x="variant", y="median_s", error_y=d["max_s"] - d["median_s"], error_y_minus=d["median_s"] - d["min_s"],
                         color_discrete_sequence=[TEAL], title=title, labels={"median_s": "Seconds (median, bars show min-max)", "variant": ""})
            st.plotly_chart(style(fig, 340), width="stretch")
        sizes = bs[bs["variant"].str.contains("size on disk")]
        if not sizes.empty:
            st.write("**Storage size:** " + " | ".join(f"{r['variant']}: {r['median_s']:.1f}" for _, r in sizes.iterrows()))
        with st.expander("Raw summary table"):
            st.dataframe(bs, hide_index=True, width="stretch")

    qc = load("query_catalog")
    if qc is not None:
        st.subheader("Spark queries in this project")
        st.dataframe(qc[["query", "technique", "result_rows", "seconds"]], hide_index=True, width="stretch")

# ================================================================================ About
with tab_about:
    dq = read_json(R / "data_quality_report.json", {})
    st.markdown(f"**Dataset.** {config.DATASET_CITATION}")
    st.markdown(
        "**What the numbers mean.** *MU* (million units) equals GWh. 'Energy met' is electricity supplied to a state in a day. "
        "'Peak met' is the highest power supplied in a day (MW). 'Peak unmet' is the largest shortfall against demand that day (MW). "
        "Weather variables are state-level daily minima, means and maxima from reanalysis data."
    )
    st.markdown(
        "**Limitations.**\n"
        "- The dataset is moderate in size (about 140 MB of CSV), not a production-scale data lake.\n"
        "- Unmet demand is only recorded when the grid operators report it, so zero may also mean 'not recorded'.\n"
        "- Temperature and demand are both seasonal; correlations show association, not cause.\n"
        "- Weather is an area-weighted state average and cannot capture local conditions in a large state.\n"
        "- Ladakh has demand data but no weather file in this dataset."
    )
    if dq:
        with st.expander("Data-quality report from the cleaning step"):
            st.json(dq)
