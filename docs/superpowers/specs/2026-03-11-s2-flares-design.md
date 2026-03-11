# s2-flares: Sentinel-2 flare detection as a shared module

## Problem

VIIRS Nightfire detections have 750m spatial uncertainty (M-band pixel size). When investigating a specific VNF flare site in gaslight, there's no way to determine the precise location of the flare source(s) within that pixel. Burnoff already solves this — it detects flares at 20m resolution using Sentinel-2 SWIR imagery — but its detection logic is coupled to its P2P distributed processing architecture.

## Solution

Extract burnoff's Sentinel-2 detection algorithm into a standalone module (`s2-flares`) designed as a clean library from first principles. Both burnoff and gaslight adapt to the module's API — the module is not shaped by either project's existing code structure.

## Module design: s2-flares

### Repository structure

```
s2-flares/
├── index.js       # public API: detect(), searchSTAC(), clusterDetections()
├── detect.js      # single-image detection: band math, connected components, cluster filtering
├── stac.js        # STAC catalog search + S2 item parsing
├── cog.js         # COG reading: open, read windows, band extraction via geotiff.js
├── cluster.js     # cross-date spatial merge
├── geo.js         # coordinate transforms (WGS84 ↔ UTM, meters ↔ degrees)
├── worker.js      # Web Worker entry point (thin: imports index.js, handles messages)
├── test/          # self-contained tests (run against known S2 scenes)
└── vendor/
    └── geotiff.js
```

### Design principles

1. **Library-first**: s2-flares is a library with a clean public API. It knows nothing about P2P, CRDTs, IndexedDB, or MapLibre. Consumers handle their own orchestration.
2. **Streaming results**: the core `detect()` async generator yields results as they're found. Consumers decide how to present them.
3. **Cancellable**: all long-running operations accept an `AbortSignal`.
4. **Self-contained COG handling**: `cog.js` owns all geotiff.js interaction. Detection code works with typed arrays, not GeoTIFF objects.
5. **Separation of concerns**: `detect.js` is pure computation (band math on arrays). `cog.js` handles I/O. `stac.js` handles catalog queries. `cluster.js` handles temporal aggregation.

### Public API

#### `detect(bbox, start, end, options?)` → AsyncGenerator

The primary entry point. Searches the STAC catalog, processes each S2 image, and yields results as they're found.

```js
import { detect } from 's2-flares';

for await (const event of detect([west, south, east, north], '2020-01-01', '2026-01-01', { signal })) {
    switch (event.type) {
        case 'image-start':    // { item, date, cloudCover }
        case 'detections':     // { features: [...], date, cloudFree }
        case 'image-done':     // { date, blocksProcessed }
        case 'progress':       // { imagesProcessed, imagesTotal }
    }
}
```

Each detection feature: `{ lon, lat, max_b12, avg_b12, pixels, date }`.

#### `clusterDetections(detections, observations?)` → clusters

Cross-date spatial merge. Pure function, no I/O.

```js
import { clusterDetections } from 's2-flares';

const clusters = clusterDetections(allDetections, {
    // observations: optional Map<dateString, { cloudFree: boolean }> for persistence calc
    // mergeDistance: default 135m
    // minDates: default 4
    // minAvgB12: default 0.85
});
```

Each cluster: `{ lon, lat, max_b12, avg_b12, detection_count, date_count, first_date, last_date, persistence, seasonal }`.

#### `searchSTAC(bbox, start, end, options?)` → AsyncGenerator<Item>

For consumers that want to manage image processing themselves.

```js
import { searchSTAC } from 's2-flares';

for await (const item of searchSTAC(bbox, start, end, { signal })) {
    // item: { id, date, cloudCover, mgrs, bands: { b12, b11, b8a, scl } }
    // bands are URLs, not opened yet
}
```

#### `detectImage(item, bbox, options?)` → detections

Process a single S2 image. Handles COG opening, block enumeration, band reading, detection, and dedup internally.

```js
import { detectImage } from 's2-flares';

const { detections, cloudFree } = await detectImage(item, bbox, { signal });
```

#### Web Worker wrapper

`worker.js` is a thin message adapter over the async generator API:

```
→ { type: 'detect', bbox, start, end }
← { type: 'detections', features: [...], date }
← { type: 'progress', done, total }
← { type: 'clusters', features: [...] }
← { type: 'error', message }
← { type: 'done' }
→ { type: 'cancel' }
```

The worker collects all detections and observation metadata internally, runs `clusterDetections()` after the last image, then emits `clusters` before `done`.

### Detection algorithm

The core band math and filtering logic is proven — thresholds and constants are preserved from burnoff. But the code is restructured for clarity:

**`detect.js`** — pure functions operating on typed arrays (no GeoTIFF objects, no I/O):

1. `screenClouds(scl)` → cloud mask + cloud fraction
2. `toReflectance(dn)` → reflectance array (L2A offset: `(DN - 1000) / 10000`)
3. `fuseFilter(b12, b11, b8a, cloudMask)` → candidate mask (brightness, contrast, thermal)
4. `findClusters(mask, b12)` → connected components via BFS (4-connectivity)
5. `filterClusters(clusters, b12)` → quality filtering (peak, size, peakedness, halo rejection)
6. `detectBlock(b12, b11, b8a, scl, meta)` → detections for one 256×256 block

**`cog.js`** — all geotiff.js interaction isolated here:

1. `openCOG(url)` → image handle with metadata (bbox, dimensions, UTM zone)
2. `readWindow(image, [x0, y0, x1, y1])` → Uint16Array
3. `openS2Bands(item, bbox)` → opens needed bands, computes block windows, returns iterator

**Constants** — exported from `detect.js` as named exports:

| Constant | Value | Purpose |
|----------|-------|---------|
| B12_MIN | 0.30 | Brightness threshold |
| B11_MIN | 0.20 | Brightness threshold |
| PEAK_B12_MIN | 0.50 | Min peak for cluster |
| CONTRAST_RATIO | 3.0 | Peak/median contrast |
| PEAKEDNESS_MIN | 1.15 | Point source check |
| MAX_PIXELS | 80 | Max cluster size |
| MAX_CLOUD | 0.75 | Skip threshold |
| MERGE_DISTANCE | 135 | Cross-date merge (m) |
| MIN_DATES | 4 | Cluster persistence |
| MIN_AVG_B12 | 0.85 | Cluster intensity |

### Cross-date clustering

`cluster.js` — pure function, no external state:

- Grid-indexed anchor-based merge (default 135m)
- Sort detections by max_b12 descending
- Merge within grid cells; create new anchor if no nearby match
- Filter: ≥ minDates distinct dates, avg B12 ≥ minAvgB12
- Seasonal flag: all detections fall within April–August (solar glint false positives)
- Persistence: detection dates / cloud-free observation dates (if observations provided; omitted otherwise)

### Module format

ES module syntax (`export`/`import`). `worker.js` uses `importScripts` with a compatibility shim for the module files (standard pattern for Web Workers without build tooling). The `vendor/geotiff.js` is loaded via `importScripts` in worker context.

### Tests

`test/` directory with deterministic tests:

- Unit tests for `detect.js` pure functions (synthetic raster data)
- Integration test against a known S2 scene with verified flare locations
- Cluster tests with fixed detection arrays

## Changes to burnoff

Burnoff adapts its architecture to consume s2-flares as a library. The detection algorithm moves out of burnoff entirely.

**Before:**
```
web/detect.js   — monolithic worker: STAC + COG + detection + P2P partitioning
web/utm.js      — coordinate transforms
web/app.js      — clustering logic inline (~100 lines)
```

**After:**
```
web/vendor/s2-flares/     — git submodule
web/detect-worker.js      — P2P worker harness that calls s2-flares functions
web/app.js                — clustering calls s2-flares/cluster.js
```

### What burnoff's detect-worker.js does

Burnoff's P2P architecture distributes work at the block level across peers. The new worker:

1. Receives block assignments from main thread (existing P2P dispatch, unchanged)
2. Calls `s2-flares/cog.js` to open bands and read windows
3. Calls `s2-flares/detect.js` `detectBlock()` for the actual detection
4. Posts results back to main thread (existing message format, unchanged)

The P2P partitioning, CRDT sync, IndexedDB persistence, WebRTC mesh, and awareness protocol stay in burnoff's own code — they're orchestration concerns that don't belong in the library.

### What changes in burnoff's app.js

- Cross-date clustering replaced with `import { clusterDetections } from 's2-flares'`
- STAC search can optionally migrate to `s2-flares/searchSTAC()` (low priority — existing code works)
- `utm.js` replaced by `s2-flares/geo.js`
- Terminal naming logic stays in burnoff (consumer-specific)

## Changes to gaslight

### New files

**`web/enhance.js`** (~100–150 lines): orchestrator module.

- `enhance(flare)` — main entry point
  - Computes 750m bbox from flare lat/lon
  - Date range: flare's `first_detected` to `last_detected`
  - Spawns `s2-flares/worker.js`
  - On `detections`: converts to GeoJSON, appends to `s2-detections` map source (live)
  - On `progress`: updates progress indicator in detail panel
  - On `clusters`: replaces raw detections with clustered points
  - On `error`: shows error state in detail panel
  - On `done`: finalizes UI
- `cancelEnhance()` — terminates worker, clears S2 map sources. Called automatically by `closeDetail()` and when switching to a different flare.

### New map layers

- **Source `s2-detections`**: GeoJSON, updated live during detection
- **Layer `s2-detection-points`** (circle): raw S2 detections within the pixel square
  - Radius: 4–8px scaled by max_b12
  - Color: white-hot palette (distinct from VNF red-yellow ramp)
  - Visible during/after enhance
- **Layer `s2-cluster-points`** (circle): final merged clusters (replaces raw detections when done)
  - Radius: scaled by detection_count
  - Color: by persistence

### Detail panel additions

When viewing a flare:

- **Enhance button**: below sparkline. "Enhance with Sentinel-2". Disabled during detection.
- **Progress**: "Processing image 12 of 87..." — live updates.
- **Results summary**: "3 sources found, 142 observations, 2020–2025".
- **Per-cluster list**: clickable rows (max B12, detection count, persistence). Click zooms to cluster on map.

### Vendor addition

```
web/vendor/s2-flares/  ← git submodule
```

## UX flow

1. User clicks a VNF flare → detail panel opens (existing)
2. "Enhance with Sentinel-2" button visible below sparkline
3. Click Enhance:
   - Button → progress state ("Searching Sentinel-2 archive...")
   - Map zooms to fit pixel square if needed
   - Detection points appear live inside pixel square as images are processed
   - Progress updates in detail panel
4. Detection completes:
   - Raw points replaced by merged clusters
   - Button → "Enhanced" (completed)
   - Summary: "3 sources found, 142 observations, 2020–2025"
   - Cluster list in detail panel
5. Click cluster row → highlight on map, show stats
6. Close panel or click different flare → clear S2 results, cancel worker
7. Re-enhance same flare → re-runs (no cache in gaslight)

## Sequencing

1. **Create s2-flares repo** with the clean library structure
2. **Build s2-flares** — port detection logic from burnoff into the new module structure, write tests
3. **Refactor burnoff** — replace inline detection with s2-flares submodule, validate everything works
4. **Integrate into gaslight** — add submodule, build enhance.js, add UI
5. **Test end-to-end** — known VNF flare, run enhance, verify S2 detections

## Open questions

- **Date range**: flare's `first_detected`–`last_detected`, or always back to 2020? Former is faster; latter catches flares predating VNF.
- **Rate limiting**: Element84 STAC has no published limits. Backoff needed — years of data for one pixel could mean 200+ images.
- **Result caching**: ephemeral (simpler) vs IndexedDB (avoids re-processing)?
- **Cluster thresholds for small areas**: burnoff's defaults (≥4 dates, avg B12 ≥ 0.85) are tuned for large viewport scans. A 750m pixel may need relaxed thresholds since we're looking at a known flare location — even 1–2 detections are informative. Consider a `mode: 'enhance'` option that relaxes filtering.
