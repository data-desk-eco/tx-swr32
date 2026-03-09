# tx-swr32

Dark flaring analysis for the Permian Basin. Combines SWR 32 flaring permits, RRC well data, and VIIRS Nightfire satellite detections in a DuckDB database to find unpermitted flaring.

## Layout

- `scrape.sh` — SWR 32 permit scraper (bash, hits RRC web app)
- `scripts/download_rrc.py` — downloads wellbore + P-5 EBCDIC files from RRC MFT (Playwright)
- `scripts/parse_rrc.py` — parses EBCDIC to `wells.csv` + `operators.csv` (two-pass streaming)
- `scripts/fetch_vnf.py` — fetches VNF profiles from EOG (needs `.env` with EOG credentials)
- `queries/schema.sql` — DuckDB table definitions
- `queries/load.sql` — loads CSVs into DuckDB, aggregates VNF to daily
- `queries/dark_flaring.sql` — spatial join (VNF→wells) + permit matching

## Key details

- **Districts**: EBCDIC uses numeric codes (07, 08); permits use alphanumeric (7C, 8A, 08). The `district_match` macro in `dark_flaring.sql` handles the mapping.
- **EBCDIC layout**: wellbore file (dbf900) has 247-byte records. Type 01 = root (API), type 02 = completion (OG code + district + lease at bytes 2-10), type 13 = location (lat/lon at bytes 132-152 as zoned decimal). Types 02 and 13 inherit API from preceding type 01.
- **VNF load**: uses `all_varchar=true` instead of `auto_detect` on the 1,700 profile CSVs — much faster.
- **Spatial join**: matches VNF detections to nearest well within ~750m using `ST_DWithin` with a coarse bounding-box pre-filter.

## Commands

- `uv` manages Python deps
- `make db` — full pipeline (download, parse, load, analyse)
- `make clean` — removes derived data (keeps raw downloads)
- `duckdb data/dark_flaring.duckdb` — query results interactively
