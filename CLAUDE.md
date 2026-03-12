# gaslight

Flaring analysis for the Permian Basin. Matches VIIRS Nightfire satellite flare detections and Sentinel-2 imagery to SWR 32 permitted flare locations, oil/gas lease footprints, and methane plume observations.

## Layout

- `scripts/scrape_permits.py` — SWR 32 permit metadata scraper
- `scripts/scrape_permit_details.py` — downloads permit detail HTML pages
- `scripts/parse_permit_details.py` — parses HTML to CSVs (permit_details, permit_properties, flare_locations)
- `scripts/download_rrc.py` — downloads EBCDIC files from RRC MFT (Playwright) + OTLS survey polygons from ArcGIS Online
- `scripts/parse_rrc.py` — parses EBCDIC to `wells.csv` + `operators.csv` (Permian districts 6E/7B/7C/08/8A)
- `scripts/fetch_vnf.py` — fetches VNF profiles from EOG
- `scripts/fetch_plumes.py` — fetches Carbon Mapper + IMEO methane plume data
- `queries/load.sql` → `rrc.sql` → `export.sql` — SQL pipeline (load → normalise → export parquets)
- `web/` — interactive map (MapLibre GL + DuckDB WASM, zero npm deps)
- `web/app.js` — main app: map setup, feature detail panels, S2 enhancement UI, shared helpers (`$`, `openDetail`, `fmtCoords`, color ramps, `renderTimeline`)
- `web/db.js` — DuckDB WASM wrapper: all data queries, operator attribution index, bbox helpers
- `web/drawer.js` — sliding data drawer (tabbed table view of flares/permits/plumes/wells with sort, viewport-sync, keyboard nav)
- `web/enhance.js` — Sentinel-2 "Enhance" feature (spawns s2-flares worker for single-flare deep analysis)
- `web/vendor/s2-flares/` — shared Sentinel-2 detection library (git submodule)

## Architecture

Two-schema database design, with analytical work done client-side in DuckDB WASM:

- **`raw`** — staging area, faithful load of source files (CSVs, shapefiles, DSVs)
- **`rrc`** — Texas oil & gas foundation tables derived from RRC data (permits, leases, production, well-survey joins)

Pipeline: `load → rrc → export` (export writes normalised parquets for the web app)

Client-side (DuckDB WASM): operator attribution, plume display, operator search — all computed live from the exported parquets.

### Web app structure

Single-page app with no build step and zero npm dependencies. MapLibre GL and DuckDB WASM are vendored.

- **app.js** — entry point. Initialises map, loads data, binds UI. Contains shared utilities: `$` (DOM lookup), `openDetail` (detail panel lifecycle), `fmtCoords`, color ramp functions (`b12Color`, `mwColor`), `renderTimeline` (shared SVG chart builder for both VNF sparklines and S2 timelines), and geo constants (`LAT_PER_M`, `lonPerM`).
- **db.js** — DuckDB WASM interface. Loads parquets, exposes typed query functions. Shared helpers: `bboxDeltas` (lat/lon deltas from radius), `tableWhere` (WHERE clause builder for table queries). Builds `flare_operators` in-memory table at startup for O(1) operator lookups.
- **drawer.js** — data drawer with tabbed tables, column sorting, keyboard navigation (j/k/h/l/g/G), viewport-synced queries.
- **enhance.js** — manages s2-flares Web Worker lifecycle, localStorage caching, cluster state.
- **style.css** — all styling via CSS custom properties. `.btn-action` base class for action buttons. `.glass` / `.panel` for frosted-glass panels.

## Methodology

1. **Flare detection**: VNF flare sites matched to SWR 32 permit locations and RRC wells within 375m (VIIRS M-band pixel radius).
2. **Lease matching**: spatial via `rrc.leases` (union of OTLS survey polygons containing each lease's wells). Wells are spatial-joined to OTLS surveys (`rrc.well_surveys`), then survey polygons are unioned per lease. VNF sites within a lease footprint (`ST_Contains`) get allocated to that lease. Vertically stacked leases (different depth intervals) share surface geometry.
3. **Operator attribution** (client-side, DuckDB WASM): combined evidence from permits and wells within 375m. Prefers operators with permit filings, then most evidence (wells + permits), then closest distance. Confidence: `sole`/`majority`/`contested`. Pre-computed for all flares at startup (`flare_operators` in-memory table); S2 clusters use live spatial queries.
4. **Exclusions**: EPA GHGRP non-upstream facilities within 1.5km; Gas Plant permits filtered out.
5. **Sentinel-2 enhancement**: Per-flare deep analysis using s2-flares library. Searches Sentinel-2 archive (last year) over a 750m bbox, runs detection at 20m resolution, clusters results incrementally after each image. Accessed via "Enhance with Sentinel-2" button in flare detail panel. Each S2 cluster is a first-class map feature with its own detail card (B12 stats, timeline chart, permit coverage) and deep link (`#s2=HASH`). Clusters get a deterministic hash ID based on anchor position. Enhancement runs in the background — navigating away doesn't cancel it; only the explicit "Stop Analysis" button does. Stopped analyses resume from where they left off (worker skips already-processed dates). Results cached to localStorage with a `complete` flag distinguishing finished vs partial runs.
6. **S2 pixel overlay**: Clicking a detection date in an S2 cluster detail card fetches the Sentinel-2 B12 COG via STAC, reads a 250m window around the cluster, and renders hot pixels (>0.6 reflectance) on the map with a magma colormap and nearest-neighbour resampling.

## Key details

- **EBCDIC districts**: numeric codes mapped to alphanumeric via `rrc.district_map` (08→7B, 09→7C, 10→08, 11→8A)
- **Permits**: `rrc.permits` merges raw filings + detail pages with parsed dates, eliminating repeated COALESCE patterns downstream.
- **OTLS surveys**: statewide shapefile from ArcGIS Online (`survALLp.shp`), loaded in full.
- **Lease footprints**: `rrc.leases` — union of OTLS survey polygons per lease. Leases spanning >10km extent excluded as data errors.
- **IMEO source**: `data/imeo_plumes.geojson` — manual download from methanedata.unep.org (no API).
- **Permit coverage**: `rrc.permit_leases` maps each SWR 32 filing to its underlying leases.
- **Permian bbox**: 30–33.5°N, 100–104.5°W (applied at export time via `in_permian()` macro). Texas-only: sites above 32°N must be east of -103.064° (TX-NM border) to exclude New Mexico.
- **Match radius**: 375m (VIIRS M-band pixel radius = 750m / 2). Bounding box pre-filter ±0.0034° (~375m).
- **VIIRS pixel squares**: 750m squares generated client-side in the web app for visual review of spatial matching.
- **Deep linking**: all params in the hash alongside MapLibre's map position. `#map=zoom/lat/lon&vnf=ID` opens VNF detail, `#map=…&vnf=ID&mode=s2` starts S2 enhancement, `#map=…&s2=HASH` opens an S2 cluster detail card.
- **Colors**: defined centrally as CSS custom properties (`--color-flare`, `--color-permit`, `--color-plume`, `--color-well`) in `:root`; JS reads them via `getComputedStyle`. Color ramps for intensity (`b12Color`, `mwColor`) are shared functions in app.js.

## Commands

- `make db` — full pipeline (load → rrc → export)
- `make refresh` — rebuild DB from scratch
- `make export` — re-export parquets for web app
- `make vendor` — download vendored JS deps
- `make serve` — dev server on :8080
- `make plumes` — fetch latest plume data
- `make clean` — removes derived data
- `make help` — list all targets
- `duckdb data/data.duckdb` — query interactively
