#!/usr/bin/env python3
"""Parse downloaded SWR 32 detail page HTML into structured CSVs.

Reads: data/raw_html/{filing_no}.html
Writes:
  data/permit_details.csv      — filing metadata (one row per filing)
  data/permit_properties.csv   — properties/leases (one row per property per filing)
  data/flare_locations.csv     — flare/vent GPS locations (replaces existing)
  data/permit_attachments.csv  — attachment index

Optimised: builds an id→text index per page for O(1) lookups instead of
repeated full-tree searches.
"""
import csv
import re
import sys
from pathlib import Path

from lxml import etree


def build_index(html: str) -> tuple[dict[str, str], etree._Element]:
    """Parse HTML and build id→text_content index for all labelled elements."""
    parser = etree.HTMLParser()
    tree = etree.fromstring(html.encode("utf-8"), parser)

    idx: dict[str, str] = {}
    for el in tree.iter():
        eid = el.get("id")
        if eid:
            idx[eid] = (el.text or "").strip()
    return idx, tree


def parse_filing_metadata(idx: dict[str, str], tree: etree._Element, filing_no: str) -> dict:
    """Extract filing metadata using the known label ID patterns."""
    p = "pbviewForm:"

    # Exception reasons from the datalist
    reasons = []
    for el in tree.iter():
        eid = el.get("id") or ""
        if "pbexcprsn_list" in eid:
            for li in el.iter("li"):
                text = "".join(li.itertext()).strip()
                text = re.sub(r"^\d+\.\s*", "", text)
                if text:
                    reasons.append(text)
            break

    # Dates are in <span> siblings, not labels — scan for them
    effective_date = ""
    expiration_date = ""
    for el in tree.iter():
        eid = el.get("id") or ""
        if eid == f"{p}j_idt60":  # "Requested Effective Date:" label
            sib = el.getnext()
            if sib is not None:
                effective_date = (sib.text or "").strip()
        elif eid == f"{p}j_idt62":  # "Requested Expiration Date:" label
            sib = el.getnext()
            if sib is not None:
                expiration_date = (sib.text or "").strip()

    return {
        "filing_no": filing_no,
        "exception_number": idx.get(f"{p}j_idt24", ""),
        "sequence_number": idx.get(f"{p}j_idt26", ""),
        "exception_status": idx.get(f"{p}j_idt29", ""),
        "operator": idx.get(f"{p}j_idt33", ""),
        "submitted_date": idx.get(f"{p}j_idt36", ""),
        "filing_type": idx.get(f"{p}j_idt38", ""),
        "prior_exception_no": idx.get(f"{p}j_idt31", ""),
        "cumulative_days_authorized": idx.get(f"{p}j_idt40", ""),
        "site_name": idx.get(f"{p}j_idt48", ""),
        "hearing_requested": idx.get(f"{p}j_idt53", ""),
        "is_h8_shutdown": idx.get(f"{p}j_idt56", ""),
        "permanent_exception_requested": idx.get(f"{p}j_idt58", ""),
        "requested_effective_date": effective_date,
        "requested_expiration_date": expiration_date,
        "number_of_days": idx.get(f"{p}j_idt65", ""),
        "every_day_of_month": idx.get(f"{p}j_idt70", ""),
        "days_per_month": idx.get(f"{p}j_idt72", ""),
        "connected_to_gathering_system": idx.get(f"{p}j_idt75", ""),
        "distance_to_nearest_pipeline": idx.get(f"{p}j_idt77", ""),
        "exception_reasons": "; ".join(reasons),
    }


def parse_properties(idx: dict[str, str], filing_no: str) -> list[dict]:
    """Extract property list using known ID patterns."""
    properties = []
    p = "pbviewForm:pbactiveprop:"
    prop_idx = 0

    while True:
        pp = f"{p}{prop_idx}:"
        type_id = f"{pp}j_idt85"
        if type_id not in idx:
            break

        properties.append({
            "filing_no": filing_no,
            "property_type": idx.get(type_id, ""),
            "district": idx.get(f"{pp}j_idt89", ""),
            "property_id": idx.get(f"{pp}j_idt93", ""),
            "lease_name": idx.get(f"{pp}j_idt97", ""),
            "requested_release_rate_mcf_day": idx.get(f"{pp}j_idt102", ""),
            "gas_measurement_method": idx.get(f"{pp}j_idt108", ""),
        })

        # Nested commingle sub-properties
        sub_idx = 0
        while True:
            sp = f"{pp}pbcmnglprop:{sub_idx}:"
            sub_type_id = f"{sp}j_idt117"
            if sub_type_id not in idx:
                break

            properties.append({
                "filing_no": filing_no,
                "property_type": idx.get(sub_type_id, ""),
                "district": idx.get(f"{sp}j_idt121", ""),
                "property_id": idx.get(f"{sp}j_idt125", ""),
                "lease_name": idx.get(f"{sp}j_idt129", ""),
                "requested_release_rate_mcf_day": idx.get(f"{sp}j_idt133", ""),
                "gas_measurement_method": "",
            })
            sub_idx += 1

        prop_idx += 1

    return properties


def parse_flare_locations(idx: dict[str, str], filing_no: str) -> list[dict]:
    """Extract flare/vent locations using known ID patterns."""
    locations = []
    p = "pbviewForm:pbactivefv:"
    fv_idx = 0

    while True:
        fp = f"{p}{fv_idx}:"
        name_id = f"{fp}j_idt144"
        if name_id not in idx:
            break

        lat_text = idx.get(f"{fp}j_idt242", "")
        lon_text = idx.get(f"{fp}j_idt246", "")
        lat = ""
        lon = ""
        if lat_text and lon_text:
            try:
                lat = float(lat_text)
                lon = float(lon_text)
            except ValueError:
                pass

        locations.append({
            "filing_no": filing_no,
            "name": idx.get(name_id, ""),
            "county": idx.get(f"{fp}j_idt148", ""),
            "district": idx.get(f"{fp}j_idt152", ""),
            "release_type": idx.get(f"{fp}j_idt160", ""),
            "release_height_ft": idx.get(f"{fp}j_idt164", ""),
            "gps_datum": idx.get(f"{fp}j_idt172", ""),
            "latitude": lat,
            "longitude": lon,
            "h2s_area": idx.get(f"{fp}j_idt251", ""),
            "h2s_concentration_ppm": idx.get(f"{fp}j_idt259", ""),
            "h2s_distance_ft": idx.get(f"{fp}j_idt262", ""),
            "h2s_public_area_type": idx.get(f"{fp}j_idt265", ""),
            "other_public_area": idx.get(f"{fp}j_idt268", ""),
            "facility_type": "",
        })
        fv_idx += 1

    return locations


def parse_attachments(tree: etree._Element, filing_no: str) -> list[dict]:
    """Extract attachment metadata from the attachment table."""
    attachments = []

    # Find the tbody by ID
    for el in tree.iter():
        eid = el.get("id") or ""
        if "attachmentTable_data" in eid:
            for tr in el.iter("tr"):
                tds = list(tr.iter("td"))
                if len(tds) < 3:
                    continue
                filename = "".join(tds[0].itertext()).strip()
                file_size = "".join(tds[1].itertext()).strip()
                file_type = "".join(tds[2].itertext()).strip()

                url = ""
                for btn in tr.iter("button"):
                    onclick = btn.get("onclick", "")
                    m = re.search(r"window\.open\('([^']+)'", onclick)
                    if m:
                        url = m.group(1).replace("\\/", "/")

                if filename:
                    attachments.append({
                        "filing_no": filing_no,
                        "filename": filename,
                        "file_size": file_size,
                        "file_type": file_type,
                        "url": url,
                    })
            break

    return attachments


DETAILS_FIELDS = [
    "filing_no", "exception_number", "sequence_number", "exception_status",
    "operator", "submitted_date", "filing_type",
    "prior_exception_no", "cumulative_days_authorized",
    "site_name", "hearing_requested", "is_h8_shutdown",
    "permanent_exception_requested",
    "requested_effective_date", "requested_expiration_date",
    "number_of_days", "every_day_of_month", "days_per_month",
    "connected_to_gathering_system", "distance_to_nearest_pipeline",
    "exception_reasons",
]

PROPERTIES_FIELDS = [
    "filing_no", "property_type", "district", "property_id",
    "lease_name", "requested_release_rate_mcf_day", "gas_measurement_method",
]

LOCATIONS_FIELDS = [
    "filing_no", "name", "county", "district", "release_type",
    "release_height_ft", "gps_datum", "latitude", "longitude",
    "h2s_area", "h2s_concentration_ppm", "h2s_distance_ft",
    "h2s_public_area_type", "other_public_area", "facility_type",
]

ATTACHMENTS_FIELDS = [
    "filing_no", "filename", "file_size", "file_type", "url",
]


def main():
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    html_dir = data_dir / "raw_html"

    html_files = sorted(html_dir.glob("*.html"))
    if not html_files:
        print("No HTML files found in", html_dir)
        return

    print(f"Parsing {len(html_files)} HTML files...", flush=True)

    all_details = []
    all_properties = []
    all_locations = []
    all_attachments = []
    errors = 0

    for i, html_path in enumerate(html_files):
        filing_no = html_path.stem
        try:
            html = html_path.read_text(encoding="utf-8")
            idx, tree = build_index(html)

            all_details.append(parse_filing_metadata(idx, tree, filing_no))
            all_properties.extend(parse_properties(idx, filing_no))
            all_locations.extend(parse_flare_locations(idx, filing_no))
            all_attachments.extend(parse_attachments(tree, filing_no))
        except Exception as e:
            print(f"  Error parsing {filing_no}: {e}", flush=True)
            errors += 1

        if (i + 1) % 1000 == 0:
            print(f"  Parsed {i + 1}/{len(html_files)}...", flush=True)

    # Write CSVs
    def write_csv(path, fieldnames, rows):
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"  {path}: {len(rows)} rows")

    write_csv(data_dir / "permit_details.csv", DETAILS_FIELDS, all_details)
    write_csv(data_dir / "permit_properties.csv", PROPERTIES_FIELDS, all_properties)
    write_csv(data_dir / "flare_locations.csv", LOCATIONS_FIELDS, all_locations)
    write_csv(data_dir / "permit_attachments.csv", ATTACHMENTS_FIELDS, all_attachments)

    print(f"Done. {errors} errors." if errors else "Done.", flush=True)


if __name__ == "__main__":
    main()
