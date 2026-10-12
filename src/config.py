"""Central configuration for PowerPulse India.

Everything that other scripts need to agree on lives here: folder locations,
file names from the Zenodo dataset, the state -> region mapping and season rules.

Two modes (set with the POWERPULSE_MODE environment variable, or `run_all.py --sample`):
  real   (default) -> reads data/raw,        writes data/processed,        results/
  sample           -> reads data/sample_raw, writes data/sample_processed, results_sample/
Sample mode uses SYNTHETIC data only to test the code. Never report its numbers.
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODE = os.environ.get("POWERPULSE_MODE", "real").lower()
_prefix = "sample_" if MODE == "sample" else ""

RAW_DIR = Path(os.environ.get("POWERPULSE_RAW_DIR", ROOT / "data" / f"{_prefix}raw"))
PROC_DIR = ROOT / "data" / f"{_prefix}processed"
RESULTS_DIR = ROOT / ("results_sample" if MODE == "sample" else "results")

for _d in (PROC_DIR, RESULTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---- Dataset (Hunt & Bloomfield, 2025, Zenodo) -------------------------------------
ZENODO_RECORD = "14983362"
DATASET_CITATION = (
    "Hunt, K. & Bloomfield, H. (2025). Daily electricity demand data for Indian "
    "states (2014-2024) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.14983362 "
    "(CC BY 4.0)"
)

# National "wide" files: one Date column + one column per state
ENERGY_FILE = "corrected_daily_energy_met_MU.csv"     # energy supplied, MU (= GWh)
PEAK_MET_FILE = "corrected_peak_met_MW.csv"           # peak power supplied, MW
PEAK_UNMET_FILE = "peak_unmet_MW.csv"                 # peak shortfall vs demand, MW

# ---- States and grid regions -------------------------------------------------------
# Region codes follow the dataset authors' own notebook.
REGION_NAMES = {
    "NR": "Northern", "ER": "Eastern", "WR": "Western",
    "SR": "Southern", "NER": "North-Eastern", "I": "Islands",
}
REGIONS = {
    "Assam": "NER", "Dadra and Nagar Haveli": "WR", "Maharashtra": "WR",
    "Madhya Pradesh": "WR", "Arunachal Pradesh": "NER", "Bihar": "ER",
    "Tripura": "NER", "Lakshadweep": "I", "Haryana": "NR", "Daman and Diu": "WR",
    "Delhi": "NR", "Meghalaya": "NER", "Andhra Pradesh": "SR", "Sikkim": "ER",
    "Manipur": "NER", "Rajasthan": "NR", "Kerala": "SR", "Nagaland": "NER",
    "Jammu and Kashmir": "NR", "Jharkhand": "ER", "Tamil Nadu": "SR",
    "Gujarat": "WR", "Uttar Pradesh": "NR", "Karnataka": "SR", "Ladakh": "NR",
    "Mizoram": "NER", "Andaman and Nicobar Islands": "I", "Goa": "WR",
    "Himachal Pradesh": "NR", "West Bengal": "ER", "Telangana": "SR",
    "Chandigarh": "NR", "Uttarakhand": "NR", "Chhattisgarh": "WR",
    "Odisha": "ER", "Punjab": "NR", "Puducherry": "SR",
}

# State weather files are named like Tamil_Nadu.csv; two names differ from the grid names.
_FILE_NAME_OVERRIDES = {"NCT_of_Delhi": "Delhi", "Andaman_and_Nicobar": "Andaman and Nicobar Islands"}


def state_from_stem(stem: str) -> str:
    """'Tamil_Nadu' -> 'Tamil Nadu', 'NCT_of_Delhi' -> 'Delhi'."""
    return _FILE_NAME_OVERRIDES.get(stem, stem.replace("_", " "))


def stem_from_state(state: str) -> str:
    inv = {v: k for k, v in _FILE_NAME_OVERRIDES.items()}
    return inv.get(state, state.replace(" ", "_"))


def state_weather_files(raw_dir: Path | None = None) -> list[Path]:
    """All per-state weather CSVs found in the raw folder (national files are skipped)."""
    raw_dir = raw_dir or RAW_DIR
    return [p for p in sorted(raw_dir.glob("*.csv")) if state_from_stem(p.stem) in REGIONS]


# A state-year with fewer reported days than this is treated as a partial year
MIN_DAYS_FULL_YEAR = 330

SEASON_ORDER = ["Winter", "Summer", "Monsoon", "Post-monsoon"]
# Winter Dec-Feb | Summer (pre-monsoon) Mar-May | Monsoon Jun-Sep | Post-monsoon Oct-Nov
