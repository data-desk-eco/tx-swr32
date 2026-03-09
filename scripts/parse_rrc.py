#!/usr/bin/env python3
"""Parse RRC EBCDIC files into CSVs for DuckDB loading."""
import csv
import gzip
import sys
from pathlib import Path

PERMIAN_DISTRICTS = {"07", "08"}  # 07 covers 7C, 08 covers 8A in permits
WELLBORE_RECLEN = 247
P5_RECLEN = 350


def ebcdic(data: bytes) -> str:
    return data.decode("cp500").strip()


def signed_decimal(data: bytes, decimal_places: int) -> float | None:
    """Parse EBCDIC zoned decimal (sign in last byte upper nibble)."""
    if not data or all(b == 0 for b in data):
        return None
    value = 0
    for b in data:
        value = value * 10 + (b & 0x0F)
    negative = (data[-1] & 0xF0) == 0xD0
    result = value / (10 ** decimal_places)
    return -result if negative else result


def parse_wellbore(gz_path: Path, out_dir: Path):
    """Parse wellbore EBCDIC -> wells.csv.

    Two passes: first collect locations (type 13), then stream completions
    (type 02) filtered to Permian districts directly to CSV.
    """
    # Pass 1: collect all locations and operator numbers
    locations = {}
    operator_nos = {}
    current_api = None
    with gzip.open(gz_path, "rb") as f:
        while (rec := f.read(WELLBORE_RECLEN)) and len(rec) == WELLBORE_RECLEN:
            rtype = ebcdic(rec[0:2])
            if rtype == "01":
                current_api = ebcdic(rec[2:5]) + ebcdic(rec[5:10])
                op_no = ebcdic(rec[28:34])
                if op_no and op_no != "000000":
                    operator_nos[current_api] = op_no
            elif rtype == "13" and current_api:
                lat = signed_decimal(rec[132:142], 7)
                lon = signed_decimal(rec[142:152], 7)
                if lat and lon:
                    locations[current_api] = (lat, -abs(lon))

    # Pass 2: stream completions to CSV
    out_path = out_dir / "wells.csv"
    current_api = None
    seen = set()
    with gzip.open(gz_path, "rb") as f, open(out_path, "w", newline="") as fout:
        w = csv.writer(fout)
        w.writerow(["api", "oil_gas_code", "lease_district", "lease_number",
                     "well_number", "operator_no", "latitude", "longitude"])
        while (rec := f.read(WELLBORE_RECLEN)) and len(rec) == WELLBORE_RECLEN:
            rtype = ebcdic(rec[0:2])
            if rtype == "01":
                current_api = ebcdic(rec[2:5]) + ebcdic(rec[5:10])
            elif rtype == "02" and current_api:
                og = ebcdic(rec[2:3])
                if og not in ("O", "G"):
                    continue
                district = ebcdic(rec[3:5])
                if district not in PERMIAN_DISTRICTS:
                    continue
                lease = ebcdic(rec[5:10])
                key = (current_api, og, lease)
                if key in seen:
                    continue
                seen.add(key)
                loc = locations.get(current_api)
                op = operator_nos.get(current_api, "")
                w.writerow([current_api, og, district, lease, ebcdic(rec[10:16]),
                            op, loc[0] if loc else "", loc[1] if loc else ""])

    print(f"Wrote {len(seen)} Permian wells to {out_path}")


def parse_p5(gz_path: Path, out_dir: Path):
    """Parse P-5 org EBCDIC -> operators.csv."""
    out_path = out_dir / "operators.csv"
    count = 0
    with open(out_path, "w", newline="") as fout, gzip.open(gz_path, "rb") as f:
        w = csv.writer(fout)
        w.writerow(["operator_number", "operator_name", "status"])
        while (rec := f.read(P5_RECLEN)) and len(rec) == P5_RECLEN:
            if rec[0:2].decode("cp500") != "A ":
                continue
            w.writerow([ebcdic(rec[2:8]), ebcdic(rec[8:40]), ebcdic(rec[41:42])])
            count += 1
    print(f"Wrote {count} operators to {out_path}")


if __name__ == "__main__":
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    for gz, parser in [("dbf900.ebc.gz", parse_wellbore), ("orf850.ebc.gz", parse_p5)]:
        path = data_dir / gz
        if path.exists():
            parser(path, data_dir)
        else:
            print(f"Missing {path}, skipping")
