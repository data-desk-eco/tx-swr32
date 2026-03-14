#!/usr/bin/env python3
"""Scrape SWR 32 exception permit metadata from the Texas RRC public query tool.

Paginates through search results at:
  https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml

Outputs data/filings.csv (tab-delimited). Resumable — skips filings already
in the output file.
"""

import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

from rrc_common import get_viewstate

BASE = "https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml"
SEARCH_FROM = "01/01/2019"
ROWS_PER_PAGE = 10
TIMEOUT = 120
MAX_RETRIES = 3

COLUMNS = [
    "excep_seq", "submittal_dt", "filing_no", "status", "filing_type",
    "operator_no", "operator_name", "property", "effective_dt",
    "expiration_dt", "fv_district",
]


def log(msg: str):
    print(f"  {msg}", file=sys.stderr, flush=True)


def get_total(text: str) -> int:
    m = re.search(r"out of (\d+) records", text)
    return int(m.group(1)) if m else 0


def parse_rows(xml_text: str) -> list[list[str]]:
    """Extract table rows from JSF AJAX response."""
    html = xml_text
    try:
        root = ET.fromstring(xml_text)
        for update in root.findall(".//{http://java.sun.com/jsf/ajax}update") or root.findall(".//update"):
            if update.text and "gridcell" in update.text:
                html = update.text
                break
    except ET.ParseError:
        for block in re.findall(r"<!\[CDATA\[(.*?)\]\]>", xml_text, re.DOTALL):
            if "gridcell" in block:
                html = block
                break

    soup = BeautifulSoup(html, "html.parser")
    cells = [td.get_text(strip=True) for td in soup.find_all("td", attrs={"role": "gridcell"})]

    # 12 cells per row: skip cell 0 (actions button), take cells 1-11
    rows = []
    for i in range(0, len(cells), 12):
        if i + 11 < len(cells):
            rows.append(cells[i + 1 : i + 12])
    return rows


def init_and_search(s: requests.Session, today: str) -> tuple[str, int, str]:
    """Initialize session, run search, return (response_text, total, viewstate)."""
    r = s.get(BASE, timeout=TIMEOUT)
    vs = get_viewstate(r.text)
    if not vs:
        r = s.get(BASE, timeout=TIMEOUT)
        vs = get_viewstate(r.text)

    r = s.post(BASE, data={
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": "pbqueryForm:searchExceptions",
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render": "pbqueryForm:pQueryTable",
        "pbqueryForm:searchExceptions": "pbqueryForm:searchExceptions",
        "pbqueryForm": "pbqueryForm",
        "javax.faces.ViewState": vs,
        "pbqueryForm:filingTypeList_focus": "",
        "pbqueryForm:filingTypeList_input": "",
        "pbqueryForm:permanentException_focus": "",
        "pbqueryForm:permanentException_input": "",
        "pbqueryForm:swr32h8_focus": "",
        "pbqueryForm:swr32h8_input": "",
        "pbqueryForm:propertyTypeList_focus": "",
        "pbqueryForm:propertyTypeList_input": "",
        "pbqueryForm:submittalDateFrom_input": SEARCH_FROM,
        "pbqueryForm:submittalDateTo_input": today,
    }, headers={
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
    }, timeout=TIMEOUT)

    vs = get_viewstate(r.text) or vs
    total = get_total(r.text)
    return r.text, total, vs


def paginate(s: requests.Session, vs: str, first: int, today: str) -> requests.Response:
    """Fetch a single page of results with retry."""
    for attempt in range(MAX_RETRIES):
        try:
            r = s.post(BASE, data={
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "pbqueryForm:pQueryTable",
                "javax.faces.partial.execute": "pbqueryForm:pQueryTable",
                "javax.faces.partial.render": "pbqueryForm:pQueryTable",
                "javax.faces.behavior.event": "page",
                "javax.faces.partial.event": "page",
                "pbqueryForm:pQueryTable_pagination": "true",
                "pbqueryForm:pQueryTable_first": str(first),
                "pbqueryForm:pQueryTable_rows": str(ROWS_PER_PAGE),
                "pbqueryForm:pQueryTable_encodeFeature": "true",
                "pbqueryForm:pQueryTable_rppDD": str(ROWS_PER_PAGE),
                "pbqueryForm": "pbqueryForm",
                "javax.faces.ViewState": vs,
                "pbqueryForm:filingTypeList_focus": "",
                "pbqueryForm:filingTypeList_input": "",
                "pbqueryForm:permanentException_focus": "",
                "pbqueryForm:permanentException_input": "",
                "pbqueryForm:swr32h8_focus": "",
                "pbqueryForm:swr32h8_input": "",
                "pbqueryForm:propertyTypeList_focus": "",
                "pbqueryForm:propertyTypeList_input": "",
                "pbqueryForm:submittalDateFrom_input": SEARCH_FROM,
                "pbqueryForm:submittalDateTo_input": today,
            }, headers={
                "Faces-Request": "partial/ajax",
                "X-Requested-With": "XMLHttpRequest",
            }, timeout=TIMEOUT)
            return r
        except (requests.Timeout, requests.ConnectionError) as e:
            log(f"RETRY {attempt + 1}/{MAX_RETRIES}: {e}")
            time.sleep(5 * (attempt + 1))
    raise requests.Timeout(f"Failed after {MAX_RETRIES} retries")


def main():
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    out = data_dir / "filings.csv"
    data_dir.mkdir(parents=True, exist_ok=True)
    # Use yesterday to avoid "future date" error from RRC server (Central time)
    today = (date.today() - timedelta(days=1)).strftime("%m/%d/%Y")

    # Load existing filings for dedup/resume
    seen = set()
    if out.exists():
        with open(out) as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) > 2 and parts[2] != "filing_no":
                    seen.add(parts[2])
        log(f"Resuming: {len(seen)} filings already scraped")

    s = requests.Session()
    s.headers.update({"User-Agent": "tx-swr32/1.0"})

    log("Initializing session...")
    search_text, total, vs = init_and_search(s, today)
    total_pages = (total + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE
    log(f"Found {total} records ({total_pages} pages)")

    # Open file for appending (write header if new)
    write_header = not out.exists() or out.stat().st_size == 0
    added = 0

    with open(out, "a") as f:
        if write_header:
            f.write("\t".join(COLUMNS) + "\n")

        def add_rows(rows):
            nonlocal added
            for row in rows:
                filing_no = row[2] if len(row) > 2 else None
                if filing_no and filing_no not in seen:
                    seen.add(filing_no)
                    f.write("\t".join(row) + "\n")
                    added += 1

        # First page
        add_rows(parse_rows(search_text))
        f.flush()
        log(f"Page 1/{total_pages}")

        # Remaining pages
        reinit_count = 0
        for page in range(1, total_pages):
            first = page * ROWS_PER_PAGE

            try:
                r = paginate(s, vs, first, today)
            except requests.Timeout:
                log(f"WARN: timeout on page {page + 1}, reinitializing...")
                search_text, _, vs = init_and_search(s, today)
                reinit_count += 1
                if reinit_count >= 5:
                    log("ERROR: too many reinits, stopping")
                    break
                continue

            if not r.text or "ViewExpiredException" in r.text:
                log(f"WARN: expired session on page {page + 1}, reinitializing...")
                search_text, _, vs = init_and_search(s, today)
                reinit_count += 1
                if reinit_count >= 5:
                    log("ERROR: too many reinits, stopping")
                    break
                continue

            reinit_count = 0
            new_vs = get_viewstate(r.text)
            if new_vs:
                vs = new_vs
            add_rows(parse_rows(r.text))

            if (page + 1) % 50 == 0:
                f.flush()
                log(f"Page {page + 1}/{total_pages} ({added} new, {len(seen)} total)")

    log(f"Done: {added} new filings ({len(seen)} total) -> {out}")


if __name__ == "__main__":
    main()
