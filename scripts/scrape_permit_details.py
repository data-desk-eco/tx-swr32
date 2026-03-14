#!/usr/bin/env python3
"""Download raw HTML detail pages and attachments for all SWR 32 filings.

Saves:
  data/raw_html/{filing_no}.html — full detail page HTML
  data/attachments/{filing_no}/ — all attached files (PDFs, XLSX, etc.)

Resumable: skips filings with existing HTML files.
Prioritises Permian districts (7B, 7C, 08, 8A) then scrapes the rest.
"""
import csv
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from rrc_common import get_viewstate

BASE = "https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml"
DETAIL = "https://webapps.rrc.state.tx.us/swr32/pbfiling.xhtml?action=open"
WORKERS = 8
PERMIAN_DISTRICTS = {"7B", "7C", "08", "8A", "8"}
TIMEOUT = 120

progress_lock = threading.Lock()
progress = {"done": 0, "html": 0, "attachments": 0, "errors": 0, "total": 0, "start": 0.0}


def log(msg: str):
    print(msg, flush=True)


def find_view_button(search_xml: str) -> str | None:
    from xml.etree import ElementTree as ET
    try:
        root = ET.fromstring(search_xml)
        updates = root.findall(".//update") or root.findall(".//{http://java.sun.com/jsf/ajax}update")
        for update in updates:
            fragment = update.text or ""
            if "View Application" not in fragment:
                continue
            soup = BeautifulSoup(fragment, "html.parser")
            for a in soup.find_all("a"):
                if "View Application" in a.get_text():
                    onclick = a.get("onclick", "")
                    m = re.search(r"'(pbqueryForm:pQueryTable:0:[^']+)'", onclick)
                    if m:
                        return m.group(1)
    except ET.ParseError:
        pass
    return None


def find_attachment_urls(html: str) -> list[dict]:
    """Extract attachment download URLs from button onclick handlers.

    Attachments are PrimeFaces buttons with onclick like:
      window.open('https://webapps.rrc.state.tx.us/dpimages/r/8232849','_blank')

    The attachment table also has filename, size, and type columns.
    """
    soup = BeautifulSoup(html, "html.parser")
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

        # Find the download URL from the button onclick
        btn = tr.find("button", onclick=True)
        url = None
        if btn:
            m = re.search(r"window\.open\('([^']+)'", btn.get("onclick", ""))
            if m:
                url = m.group(1).replace("\\/", "/")

        if url and filename:
            attachments.append({
                "url": url,
                "filename": filename,
                "file_size": file_size,
                "file_type": file_type,
            })

    return attachments


def download_attachment(session: requests.Session, att: dict, dest_dir: Path) -> str | None:
    """Download a single attachment. Returns filename on success."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    url = att["url"]
    fname = att["filename"]
    fname = re.sub(r'[/\\<>:"|?*]', '_', fname)

    try:
        r = session.get(url, timeout=TIMEOUT, stream=True)
        r.raise_for_status()

        # Use Content-Disposition filename if available
        cd = r.headers.get("Content-Disposition", "")
        m = re.search(r'filename="?([^";\n]+)', cd)
        if m:
            fname = re.sub(r'[/\\<>:"|?*]', '_', m.group(1).strip())

        dest = dest_dir / fname
        with open(dest, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        return fname
    except Exception:
        return None


def scrape_one(filing_no: str, html_dir: Path, att_dir: Path) -> tuple[bool, int]:
    """Scrape a single filing. Returns (got_html, n_attachments)."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (research; tx-swr32-scraper)",
    })

    # Step 1: GET search page for ViewState
    r = s.get(BASE, timeout=TIMEOUT)
    vs = get_viewstate(r.text)

    # Step 2: POST search with filing number
    r = s.post(BASE, data={
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": "pbqueryForm:searchExceptions",
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render": "pbqueryForm:pQueryTable",
        "pbqueryForm:searchExceptions": "pbqueryForm:searchExceptions",
        "pbqueryForm": "pbqueryForm",
        "javax.faces.ViewState": vs,
        "pbqueryForm:filingNumber_input": filing_no,
        "pbqueryForm:filingNumber_hinput": filing_no,
        "pbqueryForm:filingTypeList_focus": "",
        "pbqueryForm:filingTypeList_input": "",
        "pbqueryForm:permanentException_focus": "",
        "pbqueryForm:permanentException_input": "",
        "pbqueryForm:swr32h8_focus": "",
        "pbqueryForm:swr32h8_input": "",
        "pbqueryForm:propertyTypeList_focus": "",
        "pbqueryForm:propertyTypeList_input": "",
    }, headers={
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
    }, timeout=TIMEOUT)
    vs = get_viewstate(r.text) or vs

    # Step 3: Click "View Application"
    btn = find_view_button(r.text)
    if not btn:
        return False, 0

    r = s.post(BASE, data={
        "pbqueryForm": "pbqueryForm",
        "javax.faces.ViewState": vs,
        btn: btn,
    }, allow_redirects=True, timeout=TIMEOUT)

    html = r.text
    # The detail page form is pbviewForm (public view) not pbfilingForm
    if "pbactivefv" not in html and "pbviewForm" not in html:
        html = s.get(DETAIL, timeout=TIMEOUT).text

    # Verify we got a real detail page
    if "Filing Information" not in html and "pbviewForm" not in html:
        return False, 0

    # Save raw HTML
    html_path = html_dir / f"{filing_no}.html"
    html_path.write_text(html, encoding="utf-8")

    # Download attachments — URLs are direct dpimages links, no session needed
    atts = find_attachment_urls(html)
    n_downloaded = 0
    for att in atts:
        fname = download_attachment(s, att, att_dir / filing_no)
        if fname:
            n_downloaded += 1

    return True, n_downloaded


def worker_fn(filing_no: str, html_dir: Path, att_dir: Path):
    t0 = time.time()
    got_html = False
    n_att = 0
    error = False

    for attempt in range(3):
        try:
            got_html, n_att = scrape_one(filing_no, html_dir, att_dir)
            break
        except Exception as e:
            if attempt == 2:
                error = True
                log(f"  #{filing_no} FAILED after 3 attempts: {e}")
            time.sleep(2 * (attempt + 1))

    elapsed = time.time() - t0
    with progress_lock:
        progress["done"] += 1
        if got_html:
            progress["html"] += 1
        progress["attachments"] += n_att
        if error:
            progress["errors"] += 1
        d = progress["done"]
        total = progress["total"]
        h = progress["html"]
        a = progress["attachments"]
        errs = progress["errors"]
        rate = d / (time.time() - progress["start"]) if time.time() > progress["start"] else 0
        eta = (total - d) / rate if rate > 0 else 0

    if d % 25 == 0 or n_att > 0 or error:
        log(f"  [{d}/{total}] #{filing_no} html={'Y' if got_html else 'N'} att={n_att} ({elapsed:.0f}s) | {h} pages {a} files | {rate:.1f}/s | ETA {eta/60:.0f}m | {errs} err")


def main():
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    filings_csv = data_dir / "filings.csv"
    html_dir = data_dir / "raw_html"
    att_dir = data_dir / "attachments"
    html_dir.mkdir(parents=True, exist_ok=True)

    # Load all filings
    with open(filings_csv) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    # Split into Permian-first ordering
    permian = []
    other = []
    for r in rows:
        districts = r.get("fv_district", "").strip()
        if not districts or any(d.strip() in PERMIAN_DISTRICTS for d in districts.split(",")):
            permian.append(r["filing_no"])
        else:
            other.append(r["filing_no"])

    all_filings = permian + other
    log(f"Total: {len(all_filings)} filings ({len(permian)} Permian, {len(other)} other)")

    # Resumability: skip filings with existing HTML
    done = {p.stem for p in html_dir.glob("*.html")}
    remaining = [fn for fn in all_filings if fn not in done]
    log(f"{len(remaining)} to scrape ({len(done)} already done)")

    if not remaining:
        log("All done!")
        return

    progress["total"] = len(remaining)
    progress["start"] = time.time()
    log(f"Launching {WORKERS} workers")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(worker_fn, fn, html_dir, att_dir): fn for fn in remaining}
        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - progress["start"]
    log(f"Done: {progress['html']} pages, {progress['attachments']} attachments, {progress['errors']} errors ({elapsed/60:.0f}m)")


if __name__ == "__main__":
    main()
