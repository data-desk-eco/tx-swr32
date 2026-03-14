#!/usr/bin/env python3
"""Fetch methane plume data from Carbon Mapper API and IMEO/UNEP GeoJSON."""

import csv
import io
import json
import sys
from pathlib import Path

import httpx

IMEO_GEOJSON = Path("data/imeo_plumes.geojson")
IMEO_URL = "https://methanedata.unep.org"

# Permian Basin bounding box
WEST, SOUTH, EAST, NORTH = -105, 30, -99.5, 33.5

CM_URL = "https://api.carbonmapper.org/api/v1/catalog/plume-csv"
CM_BBOX_PARAMS = "&".join(f"bbox={v}" for v in [WEST, SOUTH, EAST, NORTH])
PAGE_SIZE = 50000

CM_OUT_FIELDS = [
    "plume_id", "plume_latitude", "plume_longitude", "datetime",
    "ipcc_sector", "emission_auto", "emission_uncertainty_auto", "platform",
]

IMEO_FIELDS = [
    "plume_id", "source_name", "satellite", "date",
    "latitude", "longitude", "emission_rate", "emission_uncertainty", "sector",
]


def fetch_carbon_mapper():
    out = Path("data/plumes_cm.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    client = httpx.Client(timeout=180, follow_redirects=True)
    total = 0
    offset = 0
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CM_OUT_FIELDS)
        w.writeheader()
        while True:
            url = f"{CM_URL}?plume_gas=CH4&{CM_BBOX_PARAMS}&limit={PAGE_SIZE}&offset={offset}"
            print(f"  CM: fetching offset={offset}...", file=sys.stderr)
            resp = client.get(url)
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            rows = list(reader)
            if not rows:
                break
            for row in rows:
                w.writerow({k: row.get(k, "") for k in CM_OUT_FIELDS})
                total += 1
            offset += len(rows)
            if len(rows) < PAGE_SIZE:
                break
    client.close()
    print(f"Carbon Mapper: {total} plumes -> {out}", file=sys.stderr)


def filter_imeo():
    out = Path("data/plumes_imeo.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    if not IMEO_GEOJSON.exists():
        print(f"IMEO GeoJSON not found at {IMEO_GEOJSON}", file=sys.stderr)
        print(f"  Download from {IMEO_URL} and save as {IMEO_GEOJSON}", file=sys.stderr)
        with open(out, "w", newline="") as f:
            csv.writer(f).writerow(IMEO_FIELDS)
        return

    print("  IMEO: reading GeoJSON...", file=sys.stderr)
    with open(IMEO_GEOJSON) as f:
        data = json.load(f)

    total = 0
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(IMEO_FIELDS)
        for feat in data["features"]:
            props = feat["properties"]
            lat = props.get("lat")
            lon = props.get("lon")
            if lat is None or lon is None:
                continue
            lat, lon = float(lat), float(lon)
            if not (SOUTH <= lat <= NORTH and WEST <= lon <= EAST):
                continue
            w.writerow([
                props.get("id_plume", ""),
                props.get("source_name", ""),
                props.get("satellite", ""),
                props.get("tile_date", ""),
                lat, lon,
                props.get("ch4_fluxrate") or "",
                props.get("ch4_fluxrate_std") or "",
                props.get("sector", ""),
            ])
            total += 1

    print(f"IMEO: {total} Permian plumes -> {out}", file=sys.stderr)


if __name__ == "__main__":
    fetch_carbon_mapper()
    filter_imeo()
