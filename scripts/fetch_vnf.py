#!/usr/bin/env python3
"""Fetch VIIRS Nightfire profiles for Permian Basin flares."""
import csv
import io
import os
import re
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

INDEX_URL = "https://eogdata.mines.edu/wwwdata/downloads/VNF_multiyear_2012-2021/multiyear_201204_202405_monthly.zip"
PROFILES_URL = "https://eogdata.mines.edu/wwwdata/downloads/vnf_profiles/profiles_multiyear"

# Permian Basin bounding box
PERMIAN = {"lat_min": 30.0, "lat_max": 33.5, "lon_min": -104.5, "lon_max": -100.0}
WORKERS = 8


def eog_session() -> requests.Session:
    """Authenticate with EOG via OIDC."""
    s = requests.Session()
    email = os.environ["EOG_EMAIL"]
    password = os.environ["EOG_PASSWORD"]

    r = s.get(PROFILES_URL, allow_redirects=True)
    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")
    if not form:
        return s  # already authenticated or no login needed

    action = form.get("action", r.url)
    data = {i["name"]: i.get("value", "") for i in form.find_all("input") if i.get("name")}
    data["username"] = email
    data["password"] = password
    r = s.post(action, data=data, allow_redirects=True)

    # Handle possible consent form
    if "consent" in r.text.lower():
        soup = BeautifulSoup(r.text, "html.parser")
        form = soup.find("form")
        if form:
            action = form.get("action", r.url)
            data = {i["name"]: i.get("value", "") for i in form.find_all("input") if i.get("name")}
            r = s.post(action, data=data, allow_redirects=True)

    return s


def find_permian_flares(index_path: Path) -> set[int]:
    """Read VNF index CSV, return flare IDs within Permian bounding box."""
    flares = set()
    with open(index_path) as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                lat = float(row["Lat_GMTCO"])
                lon = float(row["Lon_GMTCO"])
            except (ValueError, KeyError):
                continue
            if (PERMIAN["lat_min"] <= lat <= PERMIAN["lat_max"] and
                    PERMIAN["lon_min"] <= lon <= PERMIAN["lon_max"]):
                flares.add(int(float(row["Flare_ID"])))
    return flares


def download_index(session: requests.Session, out_dir: Path) -> Path:
    """Download and extract VNF monthly index."""
    import zipfile
    zip_path = out_dir / "vnf_index.zip"
    csv_path = out_dir / "vnf_index.csv"

    if csv_path.exists():
        return csv_path

    print("Downloading VNF index...")
    r = session.get(INDEX_URL, stream=True)
    r.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

    with zipfile.ZipFile(zip_path) as zf:
        # Extract the CSV (usually one file inside)
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        with zf.open(names[0]) as src, open(csv_path, "wb") as dst:
            dst.write(src.read())
    zip_path.unlink()
    return csv_path


def download_profile(session: requests.Session, flare_id: int, out_dir: Path) -> str:
    """Download a single flare profile CSV."""
    fname = f"site_{flare_id}_multiyear_vnf_series.csv"
    out_path = out_dir / fname
    if out_path.exists():
        return f"skip {flare_id}"

    url = f"{PROFILES_URL}/{fname}"
    r = session.get(url, timeout=60)
    if r.status_code == 200:
        out_path.write_bytes(r.content)
        return f"ok {flare_id}"
    return f"fail {flare_id} ({r.status_code})"


def main():
    data_dir = Path("data")
    profiles_dir = data_dir / "vnf_profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)

    session = eog_session()

    # Get index and find Permian flares
    index_path = download_index(session, data_dir)
    flare_ids = find_permian_flares(index_path)
    print(f"Found {len(flare_ids)} Permian flares in index")

    # Filter to not-yet-downloaded
    existing = {int(m.group(1)) for p in profiles_dir.glob("site_*.csv")
                if (m := re.match(r"site_(\d+)", p.stem))}
    todo = flare_ids - existing
    print(f"{len(existing)} already downloaded, {len(todo)} remaining")

    if not todo:
        return

    # Download profiles
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(download_profile, session, fid, profiles_dir): fid
                   for fid in todo}
        for f in as_completed(futures):
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(todo)}")

    print(f"Done. {len(list(profiles_dir.glob('site_*.csv')))} profiles total")


if __name__ == "__main__":
    main()
