# tx-swr32

Dark flaring analysis for the Permian Basin. Combines SWR 32 flaring permits, RRC well data, and VIIRS Nightfire satellite detections in a DuckDB database to find unpermitted flaring.

## Layout

- `scripts/scrape_permits.py` — SWR 32 permit metadata scraper (paginates RRC web app)
- `scripts/download_rrc.py` — downloads wellbore, P-4, and P-5 EBCDIC files from RRC MFT (Playwright)
- `scripts/parse_rrc.py` — parses EBCDIC to `wells.csv` + `operators.csv` (three-pass streaming with P-4 operator lookup)
- `scripts/fetch_vnf.py` — fetches VNF profiles from EOG (needs `.env` with EOG credentials)
- `scripts/fetch_plumes.py` — fetches Carbon Mapper + IMEO methane plume data for Permian Basin
- `queries/schema.sql` — DuckDB table definitions (all tables)
- `queries/flaring.sql` — loads flaring data, spatial join (VNF→wells), permit matching
- `queries/plumes.sql` — loads CM + IMEO plumes, well attribution, VNF cross-reference

## Key details

- **Districts**: EBCDIC uses numeric codes (07, 08); permits use alphanumeric (7C, 8A, 08). The `district_match` macro in `dark_flaring.sql` handles the mapping.
- **EBCDIC layout**: wellbore file (dbf900) has 247-byte records. Type 01 = root (API + drilling-era operator), type 02 = completion (OG code + district + lease at bytes 2-10), type 13 = location (lat/lon at bytes 132-152 as zoned decimal), type 21 = wellid (bridges API to P-4 lease identifiers). Types 02, 13, and 21 inherit API from preceding type 01.
- **P-4 Schedule** (p4f606): 92-byte records. Type 01 root has the *current* operator for each lease, keyed by (OG code, district, lease_rrcid). This is preferred over the wellbore's drilling-era operator.
- **Operator lookup chain**: type 21 wellid → P-4 root → P-5 org. Falls back to direct type 02 → P-4 match, then to wellbore operator.
- **VNF load**: uses `all_varchar=true` instead of `auto_detect` on the 1,700 profile CSVs — much faster.
- **Spatial join**: matches VNF detections to nearest well within ~1km using `ST_DWithin` with a coarse bounding-box pre-filter.
- **Permit matching**: spatial (via scraped flare location GPS) + operator-based (if well operator has any active permit in compatible district). Follows Earthworks "benefit of the doubt" methodology.
- **Methane plumes**: Carbon Mapper (Tanager-1) + IMEO/MARS (multi-satellite). Fetched via API and filtered to Permian bbox. Matched to nearest well within 1km, cross-referenced with VNF ±1 day to classify as unlit/flaring/wellpad/unmatched.
- **IMEO source**: `data/imeo_plumes.geojson` — manual download from methanedata.unep.org (no API available, 403s automated access).

## Commands

- `uv` manages Python deps
- `make db` — full pipeline (download, parse, load, analyse)
- `make plumes` — fetch latest CM + IMEO plume data
- `make clean` — removes derived data (keeps raw downloads)
- `duckdb data/dark_flaring.duckdb` — query results interactively

## TODO: fix operator and field district matching

Current operator attribution is incomplete. Gas wells (94% of our data) use different district numbering in the P-4 Schedule (districts 09, 10) than in the wellbore completion records (districts 07, 08). Only ~20% of wells have type 21 (wellid) bridge records that map between the two systems. This means major Permian operators like Diamondback E&P (merged with Endeavor) and Pioneer Natural Resources are largely missing from our operator data despite having thousands of P-4 leases.

The RRC bulk data files contain the information needed to fix this — we likely need to:
- Download additional RRC field/district mapping files, or parse more record types from the existing EBCDIC files to build a complete wellbore-district → P-4-district crosswalk
- The nom-de-plume project (`~/Research/nom-de-plume`) has a working three-table join (wellbore type 21 → P-4 root → P-5 org) that correctly resolves current operators — but it relies on type 21 coverage which is incomplete for our Permian subset
- Investigate whether the RRC publishes a district mapping table or whether the P-4 info records (type 02, which track operator changes over time) can fill the gap
- Consider parsing the full wellbore type 21 record set without filtering to Permian districts, then joining to P-4 across all districts to find the correct current operator for each API
