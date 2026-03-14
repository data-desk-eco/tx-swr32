#!/usr/bin/env python3
"""Fetch RRC R-3 gas processing plant data and extract facility locations.

Downloads all available monthly R-3 JSON files and unions the facilities
to get complete coverage (each monthly file only contains that month's filers).
"""

import csv
import io
import json
import zipfile

import httpx
from bs4 import BeautifulSoup

R3_PAGE = "https://www.rrc.texas.gov/resource-center/research/data-sets-available-for-download/r-3-gas-processing-plants-report/"
OUT = "data/r3_facilities.csv"


def fetch_r3_urls(client):
    """Scrape the R-3 download page for all available JSON zip URLs."""
    resp = client.get(R3_PAGE)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    urls = []
    for a in soup.select("a[href*=r3dataload]"):
        href = a["href"]
        if not href.startswith("http"):
            href = "https://www.rrc.texas.gov" + href
        urls.append(href)
    if not urls:
        raise RuntimeError("No R-3 download links found")
    return urls


def parse_r3(data, facilities):
    """Extract unique active facilities with coordinates from R-3 JSON.

    Merges into the existing facilities dict, preferring the first occurrence
    (most recent month is downloaded first).
    """
    reports = data.get("R3Report", data.get(" R3Report ", []))

    for r in reports:
        serial = r.get("Serial Number", "").strip()
        if not serial or serial in facilities:
            continue

        lat = float(r.get("Latitude") or 0)
        lon = float(r.get("Longtitude") or r.get("Longitude") or 0)

        # Skip zero/dummy coords
        if lat == 0 or lon == 0:
            continue

        # Check facility status from Facility Information array
        fi = r.get("Facility Information", [])
        statuses = [f.get("Facility Status", "") for f in fi]
        if all(s == "Decommissioned" for s in statuses) and statuses:
            continue

        facilities[serial] = {
            "serial_number": serial,
            "facility_name": r.get("Facility Name", "").strip(),
            "plant_type": r.get("Plant Type", "").strip(),
            "latitude": lat,
            "longitude": lon,
        }


def main():
    client = httpx.Client(follow_redirects=True, timeout=60)
    urls = fetch_r3_urls(client)
    print(f"Found {len(urls)} monthly R-3 files")

    facilities = {}
    for url in urls:
        label = url.rsplit("/", 1)[-1]
        print(f"  {label}")
        resp = client.get(url)
        resp.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        json_name = zf.namelist()[0]
        data = json.loads(zf.read(json_name))
        parse_r3(data, facilities)

    print(f"Extracted {len(facilities)} unique active facilities with coordinates")

    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["serial_number", "facility_name", "plant_type", "latitude", "longitude"])
        w.writeheader()
        w.writerows(facilities.values())

    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
