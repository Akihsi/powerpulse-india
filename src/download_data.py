"""Download the dataset from Zenodo into data/raw/ (needs internet; ~140 MB).

    python src/download_data.py

Only the files the pipeline needs are fetched: the 3 national files and one weather file per
state. Files already present (with a matching checksum) are skipped, so it is safe to re-run.
If the download fails, open https://zenodo.org/records/14983362 in a browser, download the
files by hand and put them in data/raw/ - the rest of the pipeline works the same.
"""
from __future__ import annotations

import hashlib
import json
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402

HEADERS = {"User-Agent": "powerpulse-india-student-project/1.0"}


def wanted_names() -> list[str]:
    names = [config.ENERGY_FILE, config.PEAK_MET_FILE, config.PEAK_UNMET_FILE]
    names += [f"{config.stem_from_state(s)}.csv" for s in config.REGIONS if s != "Ladakh"]  # no Ladakh file in dataset
    return names


def md5_of(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_listing() -> dict[str, dict]:
    """Ask the Zenodo API for file names, download links and md5 checksums (best effort)."""
    try:
        req = urllib.request.Request(f"https://zenodo.org/api/records/{config.ZENODO_RECORD}", headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as r:
            rec = json.load(r)
        files = rec.get("files", [])
        out = {}
        for f in files:
            name = f.get("key") or f.get("filename")
            link = (f.get("links") or {}).get("self") or (f.get("links") or {}).get("download")
            checksum = (f.get("checksum") or "").replace("md5:", "")
            if name:
                out[name] = {"url": link, "md5": checksum}
        return out
    except Exception as e:  # noqa: BLE001
        print(f"  (could not read the Zenodo file listing: {e}; using direct links without checksums)")
        return {}


def download(url: str, dest: Path) -> None:
    req = urllib.request.Request(url, headers=HEADERS)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(req, timeout=120) as r, tmp.open("wb") as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
    tmp.replace(dest)


def main() -> None:
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    listing = fetch_listing()
    names = wanted_names()
    failed = []
    for i, name in enumerate(names, 1):
        dest = config.RAW_DIR / name
        info = listing.get(name, {})
        expected = info.get("md5", "")
        if dest.exists() and (not expected or md5_of(dest) == expected):
            print(f"[{i:2d}/{len(names)}] have  {name}")
            continue
        url = info.get("url") or f"https://zenodo.org/records/{config.ZENODO_RECORD}/files/{name}?download=1"
        try:
            print(f"[{i:2d}/{len(names)}] fetch {name} ...", end=" ", flush=True)
            download(url, dest)
            if expected and md5_of(dest) != expected:
                print("CHECKSUM MISMATCH")
                failed.append(name)
            else:
                print(f"ok ({dest.stat().st_size / 1e6:.1f} MB)")
        except Exception as e:  # noqa: BLE001
            print(f"FAILED ({e})")
            failed.append(name)
    if failed:
        print(f"\n{len(failed)} file(s) failed: {failed}\nDownload them manually from "
              f"https://zenodo.org/records/{config.ZENODO_RECORD} into {config.RAW_DIR}")
        sys.exit(1)
    print(f"\nAll {len(names)} files are in {config.RAW_DIR}\nCite as: {config.DATASET_CITATION}")


if __name__ == "__main__":
    main()
