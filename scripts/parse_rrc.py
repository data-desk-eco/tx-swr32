#!/usr/bin/env python3
"""Parse RRC EBCDIC files into CSVs for DuckDB loading."""
import csv
import gzip
import sys
from pathlib import Path

PERMIAN_DISTRICTS = {"07", "08"}  # 07 covers 7C, 08 covers 8A in permits
WELLBORE_RECLEN = 247
P4_RECLEN = 92
P5_RECLEN = 350


def ebcdic(data: bytes) -> str:
    return data.decode("cp500").strip()


def ebcdic_int(data: bytes) -> int | None:
    s = data.decode("cp500").strip()
    return int(s) if s and s.isdigit() else None


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


def parse_p4(gz_path: Path) -> dict:
    """Parse P-4 Schedule EBCDIC -> dict of (og, district, lease_rrcid) -> operator_no.

    P-4 root records (type 01, 92 bytes) contain the CURRENT operator for each lease.
    This is more accurate than the wellbore file which has the drilling-era operator.
    """
    operators = {}
    count = 0
    with gzip.open(gz_path, "rb") as f:
        while (rec := f.read(P4_RECLEN)) and len(rec) == P4_RECLEN:
            if rec[0:2].decode("cp500") != "01":
                continue
            og = rec[2:3].decode("cp500").strip()
            district = ebcdic_int(rec[3:5])
            lease_rrcid = ebcdic_int(rec[5:11])
            operator_no = ebcdic_int(rec[20:26])
            if og and district is not None and lease_rrcid and operator_no:
                operators[(og, district, lease_rrcid)] = f"{operator_no:06d}"
                count += 1
    print(f"Parsed {count} P-4 lease operators")
    return operators


def parse_wellbore(gz_path: Path, out_dir: Path, p4_operators: dict):
    """Parse wellbore EBCDIC -> wells.csv.

    Three passes:
      1. Collect locations (type 13) and wellbore operators (type 01)
      2. Collect wellid records (type 21) — bridges API to P-4 lease identifiers
      3. Stream completions (type 02) to CSV with P-4 current operators

    Type 21 (wellid) is needed because gas wells use a different district/lease
    numbering in P-4 than in type 02 completion records.
    """
    # Pass 1: collect locations and wellbore operator numbers (fallback)
    locations = {}
    wb_operator_nos = {}
    current_api = None
    with gzip.open(gz_path, "rb") as f:
        while (rec := f.read(WELLBORE_RECLEN)) and len(rec) == WELLBORE_RECLEN:
            rtype = ebcdic(rec[0:2])
            if rtype == "01":
                current_api = ebcdic(rec[2:5]) + ebcdic(rec[5:10])
                op_no = ebcdic(rec[28:34])
                if op_no and op_no != "000000":
                    wb_operator_nos[current_api] = op_no
            elif rtype == "13" and current_api:
                lat = signed_decimal(rec[132:142], 7)
                lon = signed_decimal(rec[142:152], 7)
                if lat and lon:
                    locations[current_api] = (lat, -abs(lon))

    # Pass 2: collect wellid records (type 21) — the bridge to P-4
    # Maps API to P-4 lookup keys: (og_code, p4_district, lease_rrcid)
    api_p4_keys = {}
    current_api = None
    with gzip.open(gz_path, "rb") as f:
        while (rec := f.read(WELLBORE_RECLEN)) and len(rec) == WELLBORE_RECLEN:
            rtype = ebcdic(rec[0:2])
            if rtype == "01":
                current_api = ebcdic(rec[2:5]) + ebcdic(rec[5:10])
            elif rtype == "21" and current_api:
                og = ebcdic(rec[2:3])
                if og == "O":
                    # Oil: district at bytes 3-5, lease_number at bytes 5-10
                    p4_district = ebcdic_int(rec[3:5])
                    lease_rrcid = ebcdic_int(rec[5:10])
                elif og == "G":
                    # Gas: combined district+rrcid at bytes 3-9 (6 digits)
                    combined = rec[3:9].decode("cp500").strip()
                    if combined and combined.isdigit():
                        p4_district = int(combined[0:2])
                        lease_rrcid = int(combined)
                    else:
                        continue
                else:
                    continue
                if p4_district is not None and lease_rrcid:
                    p4_key = (og, p4_district, lease_rrcid)
                    if current_api not in api_p4_keys:
                        api_p4_keys[current_api] = []
                    api_p4_keys[current_api].append(p4_key)

    print(f"Collected {len(api_p4_keys)} APIs with P-4 bridge keys (type 21)")

    # Pass 3: stream completions to CSV
    out_path = out_dir / "wells.csv"
    current_api = None
    seen = set()
    p4_hits = 0
    wb_hits = 0
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

                # Look up current operator from P-4
                op = ""

                # Try 1: type 21 wellid bridge (most accurate, handles gas districts)
                p4_keys = api_p4_keys.get(current_api, [])
                for p4_key in p4_keys:
                    p4_op = p4_operators.get(p4_key)
                    if p4_op:
                        op = p4_op
                        p4_hits += 1
                        break

                # Try 2: direct type 02 match (works for oil, some gas)
                if not op:
                    district_int = int(district)
                    lease_int = int(lease) if lease.isdigit() else None
                    if lease_int is not None:
                        p4_op = p4_operators.get((og, district_int, lease_int))
                        if p4_op:
                            op = p4_op
                            p4_hits += 1

                # Try 3: wellbore operator (drilling-era, last resort)
                if not op:
                    op = wb_operator_nos.get(current_api, "")
                    if op:
                        wb_hits += 1

                loc = locations.get(current_api)
                w.writerow([current_api, og, district, lease, ebcdic(rec[10:16]),
                            op, loc[0] if loc else "", loc[1] if loc else ""])

    print(f"Wrote {len(seen)} Permian wells to {out_path}")
    print(f"  P-4 operators (current): {p4_hits}")
    print(f"  Wellbore operators (fallback): {wb_hits}")


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

    # Parse P-4 first (needed by wellbore parser for current operators)
    p4_path = data_dir / "p4f606.ebc.gz"
    p4_operators = parse_p4(p4_path) if p4_path.exists() else {}
    if not p4_operators:
        print("WARNING: No P-4 data — using wellbore operators (may be stale)")

    # Parse wellbore with P-4 operator lookup
    wb_path = data_dir / "dbf900.ebc.gz"
    if wb_path.exists():
        parse_wellbore(wb_path, data_dir, p4_operators)
    else:
        print(f"Missing {wb_path}, skipping")

    # Parse P-5 org
    p5_path = data_dir / "orf850.ebc.gz"
    if p5_path.exists():
        parse_p5(p5_path, data_dir)
    else:
        print(f"Missing {p5_path}, skipping")
