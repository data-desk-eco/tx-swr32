# gaslight

Dark flaring analysis for the Permian Basin. Matches VIIRS Nightfire satellite flare detections to SWR 32 permitted flare locations and oil/gas lease footprints, then cross-references permit dates and self-reported flaring volumes to identify unpermitted ("dark") flaring.

## Layout

- `scripts/scrape_permits.py` — SWR 32 permit metadata scraper
- `scripts/scrape_permit_details.py` — downloads permit detail HTML pages
- `scripts/parse_permit_details.py` — parses HTML to CSVs (permit_details, permit_properties, flare_locations)
- `scripts/download_rrc.py` — downloads EBCDIC files from RRC MFT (Playwright) + OTLS survey polygons from ArcGIS Online
- `scripts/parse_rrc.py` — parses EBCDIC to `wells.csv` + `operators.csv` (Permian districts 6E/7B/7C/08/8A)
- `scripts/fetch_vnf.py` — fetches VNF profiles from EOG
- `scripts/fetch_plumes.py` — fetches Carbon Mapper + IMEO methane plume data
- `queries/load.sql` → `rrc.sql` → `flaring.sql` → `export.sql` — layered SQL pipeline
- `web/` — interactive map (MapLibre GL + DuckDB WASM, zero npm deps)

## Architecture

Three-schema database design:

- **`raw`** — staging area, faithful load of source files (CSVs, shapefiles, DSVs)
- **`rrc`** — Texas oil & gas foundation tables derived from RRC data (permits, leases, production, well-survey joins). Designed to support future analysis beyond flaring.
- **`flaring`** — Permian Basin dark flaring analysis (VNF sites, spatial matching, plume attribution, operator scorecards)

Pipeline: `load → rrc → flaring → export`

## Methodology

1. **Dark flaring**: VNF flare sites matched to SWR 32 permit locations and RRC wells within 375m (VIIRS M-band pixel radius). For each detection-day, if any nearby permit covers the date, it's "permitted"; otherwise "dark".
2. **Lease matching**: spatial via `rrc.leases` (union of OTLS survey polygons containing each lease's wells). Wells are spatial-joined to OTLS surveys (`rrc.well_surveys`), then survey polygons are unioned per lease. VNF sites within a lease footprint (`ST_Contains`) get allocated to that lease. Vertically stacked leases (different depth intervals) share surface geometry.
3. **Reported flaring**: PDQ gas disposition data (code 04 = vented/flared) cross-referenced with permit coverage to estimate unpermitted volumes.
4. **Operator attribution**: combined evidence from permits and wells within pixel radius. Prefers operators with permit filings, then most evidence (wells + permits), then closest distance. Confidence: `sole`/`majority`/`contested`.
5. **Exclusions**: EPA GHGRP non-upstream facilities within 1.5km; Gas Plant permits filtered out.
6. **Plume attribution**: Carbon Mapper + IMEO methane plumes matched to wells and VNF sites within 1km. Classified as flaring/unlit/wellpad/unmatched.

## Key details

- **EBCDIC districts**: numeric codes mapped to alphanumeric via `rrc.district_map` (08→7B, 09→7C, 10→08, 11→8A)
- **Permits**: `rrc.permits` merges raw filings + detail pages with parsed dates, eliminating repeated COALESCE patterns downstream.
- **OTLS surveys**: statewide shapefile from ArcGIS Online (`survALLp.shp`), loaded in full.
- **Lease footprints**: `rrc.leases` — union of OTLS survey polygons per lease. Leases spanning >10km extent excluded as data errors.
- **IMEO source**: `data/imeo_plumes.geojson` — manual download from methanedata.unep.org (no API).
- **Permit coverage**: `rrc.permit_leases` maps each SWR 32 filing to its underlying leases.
- **Permian bbox**: 30-33.5N, 100-104.5W (applied in `flaring` schema, not at load time). Texas-only: sites above 32°N must be east of -103.064° (TX-NM border) to exclude New Mexico.
- **Match radius**: 375m (VIIRS M-band pixel radius = 750m / 2). Bounding box pre-filter ±0.005° (~500m).
- **Well matching**: `flaring.site_well_matches` spatial-joins RRC wells within pixel radius alongside permits.
- **VIIRS pixel squares**: 750m squares generated client-side in the web app for visual review of spatial matching.

## Commands

- `make db` — full pipeline (load → rrc → flaring → export)
- `make refresh` — rebuild DB from scratch
- `make export` — re-export parquets for web app
- `make vendor` — download vendored JS deps
- `make serve` — dev server on :8080
- `make plumes` — fetch latest plume data
- `make clean` — removes derived data
- `make help` — list all targets
- `duckdb data/dark_flaring.duckdb` — query interactively
