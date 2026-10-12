"""Shared helpers: Spark session, timing, small utilities."""
from __future__ import annotations

import json
import os
import re
import sys
import time
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402


def get_spark(app: str = "PowerPulse", shuffle_partitions: int = 8):
    """Local Spark session that uses every CPU core of this machine."""
    # Make Spark workers use the same Python as this script (avoids version-mismatch errors)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.driver.memory", os.environ.get("POWERPULSE_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@contextmanager
def timer(label: str, store: dict | None = None):
    t0 = time.perf_counter()
    yield
    dt = time.perf_counter() - t0
    if store is not None:
        store[label] = dt
    print(f"  [{dt:7.2f}s] {label}")


def banner(text: str) -> None:
    print("\n" + "=" * 70 + f"\n{text}\n" + "=" * 70)


def write_json(obj, path: Path) -> None:
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def clean_column_name(name: str) -> str:
    """Make ERA5-style names Spark-friendly: '2m_temperature_mean' -> 'temp_mean'."""
    c = name.strip().lower()
    for old, new in [
        ("2m_dewpoint_temperature", "dewpoint"),
        ("2m_temperature", "temp"),
        ("10m_u_component_of_wind", "wind_u"),
        ("10m_v_component_of_wind", "wind_v"),
        ("surface_solar_radiation_downwards", "solar"),
        ("total_cloud_cover", "cloud"),
    ]:
        c = c.replace(old, new)
    c = re.sub(r"[^0-9a-z]+", "_", c).strip("_")
    return f"c_{c}" if c[:1].isdigit() else c


def find_column(columns, required: list[str], forbidden: list[str] | None = None, exact: list[str] | None = None) -> str:
    """Pick a column by name. Tries exact names first, then 'contains all of `required`'."""
    forbidden = forbidden or []
    for e in exact or []:
        if e in columns:
            return e
    for c in columns:
        lc = c.lower()
        if all(r in lc for r in required) and not any(f in lc for f in forbidden):
            return c
    raise KeyError(
        f"Could not find a column containing {required} (excluding {forbidden}). "
        f"Available columns: {list(columns)}. Run `python src/00_inspect_data.py` and "
        f"adjust the names in src/02_analytics.py."
    )
