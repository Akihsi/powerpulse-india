"""Run the whole PowerPulse pipeline with one command.

    python run_all.py                 # real data (data/raw/ must contain the Zenodo files)
    python run_all.py --download      # download the Zenodo files first, then run everything
    python run_all.py --sample        # SYNTHETIC test data - only to check the code works
    python run_all.py --skip-bench    # skip the (slower) benchmark step

Afterwards start the dashboard:  streamlit run dashboard/app.py
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(script: str, *extra: str, env=None) -> None:
    print(f"\n>>> python {script} {' '.join(extra)}".rstrip())
    t0 = time.perf_counter()
    rc = subprocess.call([sys.executable, str(ROOT / script), *extra], env=env, cwd=ROOT)
    if rc != 0:
        sys.exit(f"\nStep failed: {script} (exit code {rc}). Read the error above; see README 'Troubleshooting'.")
    print(f"<<< finished in {time.perf_counter() - t0:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", action="store_true", help="use synthetic test data")
    ap.add_argument("--download", action="store_true", help="download the real dataset first")
    ap.add_argument("--skip-bench", action="store_true")
    ap.add_argument("--runs", default="3", help="timed runs per benchmark variant")
    a = ap.parse_args()

    env = os.environ.copy()
    env["POWERPULSE_MODE"] = "sample" if a.sample else "real"
    if a.sample:
        run("src/make_sample_data.py", env=env)
    elif a.download:
        run("src/download_data.py", env=env)

    run("src/00_inspect_data.py", env=env)
    run("src/01_ingest_clean.py", env=env)
    run("src/02_analytics.py", env=env)
    if not a.skip_bench:
        run("src/03_benchmarks.py", "--runs", a.runs, env=env)

    mode_hint = "POWERPULSE_MODE=sample " if a.sample else ""
    print(f"\nDone. Start the dashboard with:\n    {mode_hint}streamlit run dashboard/app.py")
    if a.sample:
        print("(Windows PowerShell: $env:POWERPULSE_MODE='sample'; streamlit run dashboard/app.py)")


if __name__ == "__main__":
    main()
