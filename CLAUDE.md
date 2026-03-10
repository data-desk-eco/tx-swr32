# tx-swr32

Dark flaring analysis for the Permian Basin. Matches VIIRS Nightfire satellite flare detections to SWR 32 permitted flare locations, then checks permit dates to identify unpermitted ("dark") flaring.

## Layout

- `scripts/scrape_permits.py` — SWR 32 permit metadata scraper (paginates RRC web app)
- `scripts/scrape_flare_locations.py` — scrapes GPS coordinates from permit detail pages
- `scripts/download_rrc.py` — downloads wellbore, P-4, and P-5 EBCDIC files from RRC MFT (Playwright)
- `scripts/parse_rrc.py` — parses EBCDIC to `wells.csv` + `operators.csv` (three-pass streaming with P-4 operator lookup)
- `scripts/fetch_vnf.py` — fetches VNF profiles from EOG (needs `.env` with EOG credentials)
- `scripts/fetch_plumes.py` — fetches Carbon Mapper + IMEO methane plume data for Permian Basin
- `queries/schema.sql` — DuckDB table definitions (all tables)
- `queries/flaring.sql` — loads flaring data, matches VNF flares to permit locations, checks permit dates
- `queries/plumes.sql` — loads CM + IMEO plumes, well attribution, VNF cross-reference

## Key details

- **Methodology**: each VNF flare site is matched to the nearest SWR 32 permit location within 1.5km. Operator attributed from the permit filing. For each detection-day, if any nearby permit covers the date, it's "permitted"; otherwise "dark". Sites near multiple operators' permits are flagged as "contested" and excluded from operator rankings.
- **Attribution confidence**: `sole` (only one operator nearby), `majority` (attributed operator has >50% of nearby permits), `contested` (multiple operators, none dominant). Operator chart filters to sole+majority.
- **VNF load**: uses `all_varchar=true` instead of `auto_detect` on the 1,700 profile CSVs — much faster.
- **Exclusions**: non-upstream facilities (EPA GHGRP gas plants, compressor stations, refineries) within 1.5km are excluded. Gas Plant permits excluded at load time.
- **Wells/operators**: still loaded for plumes.sql (methane plume→well attribution), not used in dark flaring analysis.
- **EBCDIC layout**: wellbore file (dbf900) has 247-byte records. Type 01 = root, type 02 = completion, type 13 = location, type 21 = wellid. P-4 Schedule (p4f606) has current operators.
- **Methane plumes**: Carbon Mapper (Tanager-1) + IMEO/MARS (multi-satellite). Fetched via API and filtered to Permian bbox. Matched to nearest well within 1km, cross-referenced with VNF ±1 day to classify as unlit/flaring/wellpad/unmatched.
- **IMEO source**: `data/imeo_plumes.geojson` — manual download from methanedata.unep.org (no API available, 403s automated access).

## Commands

- `uv` manages Python deps
- `make db` — full pipeline (download, parse, load, analyse)
- `make plumes` — fetch latest CM + IMEO plume data
- `make clean` — removes derived data (keeps raw downloads)
- `duckdb data/dark_flaring.duckdb` — query results interactively

