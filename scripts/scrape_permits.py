#!/usr/bin/env python3
"""Scrape SWR 32 exception permit metadata from the Texas RRC public query tool.

Paginates through search results at:
  https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml

Outputs data/filings.csv (tab-delimited).
"""

import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

BASE = "https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml"
SEARCH_FROM = "01/01/2019"
ROWS_PER_PAGE = 10
TIMEOUT = 60

COLUMNS = [
    "excep_seq", "submittal_dt", "filing_no", "status", "filing_type",
    "operator_no", "operator_name", "property", "effective_dt",
    "expiration_dt", "fv_district",
]


def log(msg: str):
    print(f"  {msg}", file=sys.stderr, flush=True)


def get_viewstate(text: str) -> str:
    m = re.search(r'name="javax\.faces\.ViewState"[^/]*value="([^"]*)"', text)
    if m:
        return m.group(1)
    m = re.search(r'javax\.faces\.ViewState:0">(.*?)]]', text)
    return m.group(1).replace("<![CDATA[", "") if m else ""


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
        # Fallback: extract from CDATA blocks directly
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


def main():
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    out = data_dir / "filings.csv"
    data_dir.mkdir(parents=True, exist_ok=True)
    # Use yesterday to avoid "future date" error from RRC server (Central time)
    today = (date.today() - timedelta(days=1)).strftime("%m/%d/%Y")

    s = requests.Session()
    s.headers.update({"User-Agent": "tx-swr32/1.0"})

    # Init session
    log("Initializing session...")
    r = s.get(BASE, timeout=TIMEOUT)
    vs = get_viewstate(r.text)
    if not vs:
        log("WARN: empty ViewState, retrying...")
        r = s.get(BASE, timeout=TIMEOUT)
        vs = get_viewstate(r.text)

    # Search
    log(f"Searching {SEARCH_FROM} to {today}...")
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
    total_pages = (total + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE
    log(f"Found {total} records ({total_pages} pages)")

    # Collect all rows
    seen = set()
    all_rows = []

    def add_rows(rows):
        for row in rows:
            filing_no = row[2] if len(row) > 2 else None
            if filing_no and filing_no not in seen:
                seen.add(filing_no)
                all_rows.append(row)

    # First page
    add_rows(parse_rows(r.text))
    log(f"Page 1/{total_pages}")

    # Remaining pages
    fail_count = 0
    for page in range(1, total_pages):
        first = page * ROWS_PER_PAGE
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

        if not r.text or "ViewExpiredException" in r.text:
            log(f"WARN: bad response on page {page + 1}, reinitializing...")
            r2 = s.get(BASE, timeout=TIMEOUT)
            vs = get_viewstate(r2.text)
            # Re-search
            s.post(BASE, data={
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "pbqueryForm:searchExceptions",
                "javax.faces.partial.execute": "@all",
                "javax.faces.partial.render": "pbqueryForm:pQueryTable",
                "pbqueryForm:searchExceptions": "pbqueryForm:searchExceptions",
                "pbqueryForm": "pbqueryForm",
                "javax.faces.ViewState": vs,
                "pbqueryForm:submittalDateFrom_input": SEARCH_FROM,
                "pbqueryForm:submittalDateTo_input": today,
            }, headers={
                "Faces-Request": "partial/ajax",
                "X-Requested-With": "XMLHttpRequest",
            }, timeout=TIMEOUT)
            fail_count += 1
            if fail_count >= 5:
                log("ERROR: too many failures, stopping")
                break
            continue

        fail_count = 0
        new_vs = get_viewstate(r.text)
        if new_vs:
            vs = new_vs
        add_rows(parse_rows(r.text))

        if (page + 1) % 50 == 0:
            log(f"Page {page + 1}/{total_pages} ({len(all_rows)} filings)")

    # Write output
    with open(out, "w") as f:
        f.write("\t".join(COLUMNS) + "\n")
        for row in all_rows:
            f.write("\t".join(row) + "\n")

    log(f"Done: {len(all_rows)} filings -> {out}")


if __name__ == "__main__":
    main()
