#!/usr/bin/env python3
"""Fetch VIIRS Nightfire profiles for Permian Basin flares from EOG.

Scrapes the EOG directory listing for flare IDs, peeks at each profile's
first row for lat/lon, filters to Permian bounding box, downloads full CSVs.
Requires EOG_EMAIL and EOG_PASSWORD in .env.
"""
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

PROFILES_URL = "https://eogdata.mines.edu/wwwdata/downloads/vnf_profiles/profiles_multiyear"
PERMIAN_BBOX = (30.0, 33.5, -104.5, -100.0)  # lat_min, lat_max, lon_min, lon_max


def eog_session() -> tuple[requests.Session, str]:
    """Authenticate with EOG via OIDC. Returns (session, listing_html)."""
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"

    r = s.get(f"{PROFILES_URL}/", allow_redirects=True)
    if r.ok and "site_" in r.text:
        return s, r.text

    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")
    if not form:
        return s, ""

    action = form.get("action", r.url)
    if not action.startswith("http"):
        action = urljoin(r.url, action)

    data = {i["name"]: i.get("value", "") for i in form.find_all("input") if i.get("name")}
    for inp in form.find_all("input"):
        t = (inp.get("type") or "text").lower()
        n = (inp.get("name") or "").lower()
        if t in ("text", "email") or n in ("username", "email"):
            data[inp["name"]] = os.environ["EOG_EMAIL"]
        elif t == "password" or n in ("password", "credential"):
            data[inp["name"]] = os.environ["EOG_PASSWORD"]

    r = s.post(action, data=data, allow_redirects=True)

    # Handle consent page
    if r.ok and "<form" in r.text.lower():
        soup2 = BeautifulSoup(r.text, "html.parser")
        form2 = soup2.find("form")
        if form2 and form2.get("action"):
            a2 = form2["action"]
            if not a2.startswith("http"):
                a2 = urljoin(r.url, a2)
            d2 = {i["name"]: i.get("value", "") for i in form2.find_all("input") if i.get("name")}
            s.post(a2, data=d2, allow_redirects=True)

    return s, ""


def peek_location(session: requests.Session, fid: int) -> tuple[float, float] | None:
    """Range-request first bytes of a profile to extract lat/lon."""
    try:
        r = session.get(f"{PROFILES_URL}/site_{fid}_multiyear_vnf_series.csv",
                        headers={"Range": "bytes=0-511"}, timeout=30)
        if r.status_code not in (200, 206):
            return None
        lines = r.text.split("\n")
        if len(lines) < 2:
            return None
        h, d = lines[0].split(","), lines[1].split(",")
        lat, lon = float(d[h.index("Lat_GMTCO")]), float(d[h.index("Lon_GMTCO")])
        return (lat, lon) if -90 <= lat <= 90 and -180 <= lon <= 180 else None
    except (requests.RequestException, ValueError, IndexError):
        return None


def read_profile_location(path: Path) -> tuple[float, float] | None:
    """Read lat/lon from first data row of an existing profile CSV."""
    try:
        with open(path) as f:
            h = f.readline().strip().split(",")
            d = f.readline().strip().split(",")
            return float(d[h.index("Lat_GMTCO")]), float(d[h.index("Lon_GMTCO")])
    except (ValueError, IndexError, OSError):
        return None


def download_profile(session: requests.Session, fid: int, out_dir: Path) -> bool:
    """Download a single VNF profile CSV. Returns success."""
    out = out_dir / f"site_{fid}.csv"
    if out.exists():
        return True
    try:
        r = session.get(f"{PROFILES_URL}/site_{fid}_multiyear_vnf_series.csv", timeout=120)
        if r.ok and "Date_Mscan" in r.text[:500]:
            out.write_text(r.text)
            return True
    except requests.RequestException:
        pass
    return False


def in_permian(lat: float, lon: float) -> bool:
    return PERMIAN_BBOX[0] <= lat <= PERMIAN_BBOX[1] and PERMIAN_BBOX[2] <= lon <= PERMIAN_BBOX[3]


def main():
    profiles_dir = Path("data/vnf_profiles")
    profiles_dir.mkdir(parents=True, exist_ok=True)

    print("Authenticating with EOG...")
    session, listing = eog_session()
    if not listing or "site_" not in listing:
        r = session.get(f"{PROFILES_URL}/", allow_redirects=False)
        if r.status_code in (301, 302):
            sys.exit("Auth failed. Check EOG_EMAIL/EOG_PASSWORD in .env")
        listing = r.text
    print("  Authenticated")

    all_ids = {int(m.group(1)) for m in re.finditer(r"site_(\d+)_multiyear_vnf_series\.csv", listing)}
    print(f"  {len(all_ids)} flares in directory")

    existing = {int(m.group(1)) for p in profiles_dir.glob("site_*.csv")
                if (m := re.match(r"site_(\d+)", p.stem))}

    # Build location map: existing profiles first, then peek at unknowns
    locs = {}
    for fid in existing:
        loc = read_profile_location(profiles_dir / f"site_{fid}.csv")
        if loc:
            locs[fid] = loc

    unknown = [fid for fid in all_ids if fid not in locs]
    if unknown:
        print(f"  Peeking at {len(unknown)} profiles for lat/lon...")
        with ThreadPoolExecutor(max_workers=16) as pool:
            futs = {pool.submit(peek_location, session, fid): fid for fid in unknown}
            for i, fut in enumerate(as_completed(futs), 1):
                loc = fut.result()
                if loc:
                    locs[futs[fut]] = loc
                if i % 500 == 0:
                    print(f"\r  {i}/{len(unknown)} peeked", end="", flush=True)
        print(f"\r  {len(unknown)} peeked, {len(locs)} located")

    permian = {fid for fid, (lat, lon) in locs.items() if in_permian(lat, lon)}
    to_dl = sorted(permian - existing)
    print(f"  {len(permian)} Permian flares, {len(to_dl)} to download")

    if not to_dl:
        print("All Permian profiles up to date.")
        return

    ok = fail = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(download_profile, session, fid, profiles_dir): fid for fid in to_dl}
        for fut in as_completed(futs):
            if fut.result():
                ok += 1
            else:
                fail += 1
            if (ok + fail) % 100 == 0:
                print(f"\r  {ok+fail}/{len(to_dl)} ({fail} failed)", end="", flush=True)
    print(f"\n  Done: {ok} downloaded, {fail} failed")


if __name__ == "__main__":
    main()
