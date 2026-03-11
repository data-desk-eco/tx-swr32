#!/usr/bin/env python3
"""Parse downloaded SWR 32 detail page HTML into structured CSVs.

Reads: data/raw_html/{filing_no}.html
Writes:
  data/permit_details.csv      — filing metadata (one row per filing)
  data/permit_properties.csv   — properties/leases (one row per property per filing)
  data/flare_locations.csv     — flare/vent GPS locations (replaces existing)
  data/permit_attachments.csv  — attachment index
"""
import csv
import re
import sys
from pathlib import Path

from bs4 import BeautifulSoup


def label_after(soup: BeautifulSoup, header_text: str, prefix: str) -> str:
    """Find label containing header_text with given ID prefix, return the next sibling's text.

    Values can be in <label> (most fields) or <span> (dates) tags.
    """
    header = soup.find("label", id=lambda x: x and prefix in x,
                       string=lambda s: s and header_text in s)
    if not header:
        return ""
    # Try next label with matching prefix first
    sibling = header.find_next("label", id=lambda x: x and prefix in x)
    if sibling:
        text = sibling.get_text(strip=True)
        # If the next label is actually another header (contains ":"), check for span value first
        if text and not text.endswith(":"):
            return text
    # Fall back to next sibling span (used for dates)
    next_sib = header.find_next_sibling()
    if next_sib:
        return next_sib.get_text(strip=True)
    return ""


def label_value(soup: BeautifulSoup, label_id: str) -> str:
    """Get text of a label by exact ID."""
    el = soup.find("label", id=label_id)
    return el.get_text(strip=True) if el else ""


def parse_filing_metadata(soup: BeautifulSoup, filing_no: str) -> dict:
    """Extract filing metadata from the Filing Information and Exception Information sections."""
    prefix = "pbviewForm:"

    def lv(header_text):
        return label_after(soup, header_text, prefix)

    # Exception reasons from the datalist
    reasons = []
    reasons_panel = soup.find(id=lambda x: x and "pbexcprsn" in str(x))
    if reasons_panel:
        for li in reasons_panel.find_all("li"):
            text = li.get_text(strip=True)
            # Strip leading number like "1. " or "2. "
            text = re.sub(r"^\d+\.\s*", "", text)
            if text:
                reasons.append(text)

    return {
        "filing_no": filing_no,
        "exception_number": lv("Exception Number:"),
        "sequence_number": lv("Sequence Number:"),
        "exception_status": lv("Exception Status:"),
        "operator": lv("Operator:"),
        "submitted_date": lv("Submitted Date:"),
        "filing_type": lv("Filing Type:"),
        "prior_exception_no": lv("Prior Exception No:"),
        "cumulative_days_authorized": lv("Cumulative Days Authorized"),
        "site_name": lv("Site Name:"),
        "hearing_requested": lv("Hearing Requested:"),
        "is_h8_shutdown": lv("shut-down of a gas plant"),
        "permanent_exception_requested": lv("Permanent Exception Requested"),
        "requested_effective_date": lv("Requested Effective Date:"),
        "requested_expiration_date": lv("Requested Expiration Date:"),
        "number_of_days": lv("Number of Days"),
        "every_day_of_month": lv("Every day of the calendar month:"),
        "days_per_month": lv("Days per month:"),
        "connected_to_gathering_system": lv("connected to a gas gathering"),
        "distance_to_nearest_pipeline": lv("Distance to nearest pipeline:"),
        "exception_reasons": ";".join(reasons),
    }


def parse_properties(soup: BeautifulSoup, filing_no: str) -> list[dict]:
    """Extract property list. Handles both top-level properties and nested commingle sub-properties."""
    properties = []
    prop_idx = 0

    while True:
        prefix = f"pbactiveprop:{prop_idx}:"
        if not soup.find(id=lambda x: x and prefix in str(x)):
            break

        prop_type = label_after(soup, "Property Type:", prefix)
        district = label_after(soup, "District:", prefix)
        prop_id = label_after(soup, "Property ID:", prefix)
        lease_name = label_after(soup, "Lease Name:", prefix)

        # For commingle permits, the release rate is at the top level
        total_rate = label_after(soup, "Total Requested Release Rate", prefix)
        gas_measurement = label_after(soup, "Gas Measurement Method", prefix)

        properties.append({
            "filing_no": filing_no,
            "property_type": prop_type,
            "district": district,
            "property_id": prop_id,
            "lease_name": lease_name,
            "requested_release_rate_mcf_day": total_rate,
            "gas_measurement_method": gas_measurement,
        })

        # Check for nested commingle sub-properties (pbcmnglprop)
        sub_idx = 0
        while True:
            sub_prefix = f"pbactiveprop:{prop_idx}:pbcmnglprop:{sub_idx}:"
            if not soup.find(id=lambda x: x and sub_prefix in str(x)):
                break

            sub_type = label_after(soup, "Property Type:", sub_prefix)
            sub_district = label_after(soup, "District:", sub_prefix)
            sub_id = label_after(soup, "Property ID:", sub_prefix)
            sub_name = label_after(soup, "Lease Name:", sub_prefix)
            sub_rate = label_after(soup, "Requested Release Rate", sub_prefix)

            properties.append({
                "filing_no": filing_no,
                "property_type": sub_type,
                "district": sub_district,
                "property_id": sub_id,
                "lease_name": sub_name,
                "requested_release_rate_mcf_day": sub_rate,
                "gas_measurement_method": "",
            })
            sub_idx += 1

        prop_idx += 1

    return properties


def parse_flare_locations(soup: BeautifulSoup, filing_no: str) -> list[dict]:
    """Extract flare/vent locations."""
    locations = []
    idx = 0

    while True:
        prefix = f"pbactivefv:{idx}:"
        if not soup.find(id=lambda x: x and prefix in str(x)):
            break

        def lv(header_text):
            return label_after(soup, header_text, prefix)

        lat_text = lv("Degrees (Latitude)")
        lon_text = lv("Degrees (Longitude)")

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
            "name": lv("Flare or Vent Name"),
            "county": lv("County"),
            "district": lv("District"),
            "release_type": lv("Release Type"),
            "release_height_ft": lv("Release Height"),
            "gps_datum": lv("GPS Datum"),
            "latitude": lat,
            "longitude": lon,
            "h2s_area": lv("subject to SWR 36"),
            "h2s_concentration_ppm": lv("H2S Concentration"),
            "h2s_distance_ft": lv("distance to public area"),
            "h2s_public_area_type": lv("Public Area Type"),
            "other_public_area": lv("Other Public Area"),
            "facility_type": lv("Facility Type"),
        })
        idx += 1

    return locations


def parse_attachments(soup: BeautifulSoup, filing_no: str) -> list[dict]:
    """Extract attachment metadata from the attachment table."""
    attachments = []

    tbody = soup.find(id=lambda x: x and "attachmentTable_data" in str(x))
    if not tbody:
        return attachments

    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        filename = tds[0].get_text(strip=True)
        file_size = tds[1].get_text(strip=True)
        file_type = tds[2].get_text(strip=True)

        # Extract download URL from button onclick
        url = ""
        btn = tr.find("button", onclick=True)
        if btn:
            m = re.search(r"window\.open\('([^']+)'", btn.get("onclick", ""))
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
            soup = BeautifulSoup(html, "html.parser")

            all_details.append(parse_filing_metadata(soup, filing_no))
            all_properties.extend(parse_properties(soup, filing_no))
            all_locations.extend(parse_flare_locations(soup, filing_no))
            all_attachments.extend(parse_attachments(soup, filing_no))
        except Exception as e:
            print(f"  Error parsing {filing_no}: {e}", flush=True)
            errors += 1

        if (i + 1) % 500 == 0:
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
