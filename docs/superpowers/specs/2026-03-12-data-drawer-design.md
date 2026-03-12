# Data Drawer

Draggable left-side panel showing raw parquet table contents via DuckDB WASM, filtered to the current map viewport. Provides a spreadsheet-style data inspector alongside the map.

## Layout & Interaction

- **Data drawer**: full-height panel on the left edge, default closed (0 width). A vertical drag handle sits on its right edge, always visible at the left screen edge.
- **Dragging**: grabbing the handle slides the drawer open. The `#map` div's left margin/width adjusts to match the drawer width, pushing the map right. MapLibre's `map.resize()` is called on `pointerup` only (not during drag — avoids janky tile re-renders).
- **Max width**: `calc(100vw - 400px)` — leaves a usable mobile-width map.
- **Min width**: ~300px when open (enough for a readable table). Below that threshold, snaps closed to 0px.
- **Closed state**: drawer is 0px wide. Handle is a thin vertical square (6px wide, 48px tall, square corners) visible at the screen edge, centered vertically.
- **Existing left panel**: unchanged. It remains `position: fixed` over the map. When the map narrows, the left panel stays in place — no repositioning needed.
- **Mobile** (<768px): drawer is hidden entirely. No drag handle shown.

## Data & Tabs

- **Tabs**: one per currently visible map layer — **flares, permits, plumes, wells**. Tabs appear/disappear as layers are toggled on/off. Detections and leases are excluded (they are per-flare detail data without independent geometry). If no layers are visible, drawer shows an empty state.
- **Active tab**: queries DuckDB with a bounding-box WHERE clause using `map.getBounds()`. Query fires on `moveend` (debounced) and on tab switch.
- **Row limit**: queries use `LIMIT 1000`. Footer shows "1,000 of 47,382 in view" when capped, or "142 in view" when under the limit. The `COUNT(*)` runs as a separate fast query to get the true total.
- **Table**: renders all columns from the parquet table for the active tab. Horizontally scrollable if columns overflow. Rows are the raw DuckDB query result — no GeoJSON transformation. All columns shown unfiltered (this is a raw data inspector — the "cool" is seeing the actual parquet contents).
- **Sorting**: clicking a column header sorts by that column (ASC/DESC toggle). Sort is applied in the DuckDB query via ORDER BY, not client-side.

## Selection & Bidirectional Linking

- **Click row → map**: flies to the feature's lat/lon and opens the right-side detail card (reusing existing `showFlareDetail`, `showPermitDetail`, etc.). The clicked row highlights with the layer's color as a left border accent + subtle background tint.
- **Click map → table**: when a detail card opens from a map click, the corresponding row in the data table highlights and scrolls into view (if the drawer is open and the matching tab is active). If the selected feature is not in the current query result (e.g., row limit exceeded), no highlight is shown.
- **Highlight style**: left border in the layer's map color + subtle background tint on the row.
- **Row identity**: flares keyed by `flare_id`, permits by `latitude+longitude+name`, plumes by `plume_id`, wells by `api`.

## Styling

- **Drawer background**: solid dark (`#16213e` palette), not frosted glass — it's a separate panel, not an overlay.
- **Border**: subtle right border (1px, muted) where drawer meets map.
- **Drag handle**: square shape (no border-radius), centered vertically on the drawer's right edge. Visible in both open and closed states. Cursor changes to `col-resize` on hover.
- **Table**: compact rows, monospace for numeric columns, standard font for text. Alternating row backgrounds for readability. Sticky header row.
- **Tabs**: horizontal tab bar at top of drawer. Active tab has bottom border accent in the layer's color. Inactive tabs are muted.
- **Layer colors**: each tab/table uses the same color already assigned to that layer on the map.
- **Transitions**: drawer width animates with CSS transition when snapping open/closed. During active drag, no transition (immediate response).

## Implementation

- **New file**: `web/drawer.js` — ES module owning drawer DOM, drag logic, tab management, DuckDB queries, and selection state. Exports an `init(map)` function called from `app.js` after map loads.
- **DOM**: drawer markup injected by `drawer.js`, not in `index.html`. No DOM reparenting of `#map` — instead, drawer is `position: fixed; left: 0` and `#map`'s style is updated (`marginLeft` / `width`) to accommodate the drawer width. This avoids reparenting a live MapLibre canvas.
- **DuckDB queries**: new generic query function in `db.js` (e.g., `queryTableRaw(tableName, bounds, options)`) that takes a table name, optional bbox, optional ORDER BY, and LIMIT/OFFSET. Returns plain row objects. A companion `queryTableCount(tableName, bounds)` returns just the count for the footer.
- **Drag logic**: pointer events on the handle (`pointerdown` → `pointermove` → `pointerup`). During drag, updates drawer width and map margin directly (CSS only, no `map.resize()`). On `pointerup`, calls `map.resize()` once and triggers a table refresh.
- **Map integration**: `moveend` listener fires the active tab's query. Drawer listens to the layer toggle checkboxes directly via DOM event listeners (observes `change` events on the existing checkbox inputs), keeping it decoupled from `app.js`.
- **Selection sync**: drawer exposes `highlight(layerType, id)` for map→table direction. Row clicks call existing detail functions from `app.js` — `drawer.js` imports or receives these as callbacks during `init()`.
- **No new dependencies**: vanilla JS, DOM APIs, existing DuckDB WASM instance.
