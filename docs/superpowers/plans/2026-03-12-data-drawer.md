# Data Drawer Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a draggable left-side data drawer that shows raw parquet table contents via DuckDB WASM, filtered to the current map viewport, with bidirectional selection linking to the map.

**Architecture:** New `web/drawer.js` ES module handles all drawer logic. Two new query functions in `web/db.js` (`queryTableRaw`, `queryTableCount`) provide generic bbox-filtered table access. The drawer is `position: fixed` on the left; `#map`'s `marginLeft`/`width` adjusts to accommodate. No DOM reparenting of MapLibre's canvas.

**Tech Stack:** Vanilla JS, DuckDB WASM (already loaded), MapLibre GL JS, CSS

**Spec:** `docs/superpowers/specs/2026-03-12-data-drawer-design.md`

---

## Chunk 1: DuckDB Query Functions

### Task 1: Add generic table query functions to db.js

**Files:**
- Modify: `web/db.js`

These two functions power the drawer's data display. They query any parquet table with optional bbox filtering, sorting, and pagination.

- [ ] **Step 1: Add `queryTableRaw` function**

Add to the end of `web/db.js` (before the final empty line):

```javascript
export async function queryTableRaw(table, { bounds, orderBy, orderDir = 'ASC', limit = 1000 } = {}) {
    const allowed = new Set(['flares', 'permits', 'plumes', 'wells']);
    if (!allowed.has(table)) throw new Error(`Unknown table: ${table}`);

    const latCol = table === 'flares' ? 'lat' : 'latitude';
    const lonCol = table === 'flares' ? 'lon' : 'longitude';

    let where = 'WHERE 1=1';
    if (table === 'permits') where += ' AND latitude IS NOT NULL AND longitude IS NOT NULL';
    if (bounds) {
        where += ` AND ${latCol} BETWEEN ${bounds.south} AND ${bounds.north}`;
        where += ` AND ${lonCol} BETWEEN ${bounds.west} AND ${bounds.east}`;
    }

    let order = '';
    if (orderBy) {
        const dir = orderDir === 'DESC' ? 'DESC' : 'ASC';
        order = `ORDER BY "${orderBy}" ${dir}`;
    }

    // Cast date columns to VARCHAR for display
    const dateSelect = table === 'permits'
        ? `SELECT * REPLACE (
               CAST(earliest_effective AS VARCHAR) AS earliest_effective,
               CAST(latest_expiration AS VARCHAR) AS latest_expiration
           )`
        : table === 'plumes'
        ? `SELECT * REPLACE (CAST(date AS VARCHAR) AS date)`
        : 'SELECT *';

    const result = await query(`${dateSelect} FROM '${table}.parquet' ${where} ${order} LIMIT ${limit}`);
    return rows(result);
}

export async function queryTableCount(table, { bounds } = {}) {
    const allowed = new Set(['flares', 'permits', 'plumes', 'wells']);
    if (!allowed.has(table)) throw new Error(`Unknown table: ${table}`);

    const latCol = table === 'flares' ? 'lat' : 'latitude';
    const lonCol = table === 'flares' ? 'lon' : 'longitude';

    let where = 'WHERE 1=1';
    if (table === 'permits') where += ' AND latitude IS NOT NULL AND longitude IS NOT NULL';
    if (bounds) {
        where += ` AND ${latCol} BETWEEN ${bounds.south} AND ${bounds.north}`;
        where += ` AND ${lonCol} BETWEEN ${bounds.west} AND ${bounds.east}`;
    }

    const result = await query(`SELECT COUNT(*) AS cnt FROM '${table}.parquet' ${where}`);
    const r = rows(result);
    return r[0]?.cnt || 0;
}
```

- [ ] **Step 2: Verify in browser console**

Open the app at localhost:8080. In the browser console:

```javascript
const db = await import('./db.js?v=4');
const r = await db.queryTableRaw('flares', { limit: 5 });
console.log(r); // Should show 5 flare row objects with all columns
const c = await db.queryTableCount('flares');
console.log(c); // Should show total flare count
```

Expected: array of 5 plain objects and a number.

- [ ] **Step 3: Commit**

```bash
git add web/db.js
git commit -m "feat: add queryTableRaw and queryTableCount for drawer"
```

---

## Chunk 2: Drawer Module — DOM, Drag, and Tabs

### Task 2: Create drawer.js with DOM structure and drag handle

**Files:**
- Create: `web/drawer.js`

The drawer module creates its DOM, handles drag resize, and manages tab state. It's a single file that owns all drawer concerns.

- [ ] **Step 1: Create `web/drawer.js` with DOM injection and drag logic**

```javascript
import { queryTableRaw, queryTableCount } from './db.js?v=4';

const LAYERS = {
    flares:  { label: 'Flares',  color: '#ffaa44', latCol: 'lat',       lonCol: 'lon',       idCol: 'flare_id' },
    permits: { label: 'Permits', color: '#00ccff', latCol: 'latitude',  lonCol: 'longitude',  idCol: null },
    plumes:  { label: 'Plumes',  color: '#ff44ff', latCol: 'latitude',  lonCol: 'longitude',  idCol: 'plume_id' },
    wells:   { label: 'Wells',   color: 'rgba(220,220,230,0.8)', latCol: 'latitude', lonCol: 'longitude', idCol: 'api' },
};

const MIN_WIDTH = 300;
const HANDLE_WIDTH = 6;

let map = null;
let drawerEl = null;
let handleEl = null;
let tableEl = null;
let tabBarEl = null;
let footerEl = null;
let drawerWidth = 0;
let activeTab = null;
let currentRows = [];
let currentTotalCount = 0;
let selectedId = null;
let sortCol = null;
let sortDir = 'ASC';

// Callbacks set by app.js
let onRowClick = null;

export function init(mapInstance, { onSelect } = {}) {
    map = mapInstance;
    onRowClick = onSelect || null;
    createDOM();
    bindDrag();
    bindLayerToggles();
    bindMapEvents();
}

function createDOM() {
    drawerEl = document.createElement('div');
    drawerEl.id = 'data-drawer';
    drawerEl.style.cssText = `
        position: fixed; left: 0; top: 0; bottom: 0;
        width: 0; overflow: hidden;
        background: #16213e; z-index: 5;
        display: flex; flex-direction: column;
        border-right: 1px solid rgba(255,255,255,0.1);
    `;

    // Tab bar
    tabBarEl = document.createElement('div');
    tabBarEl.style.cssText = `
        display: flex; border-bottom: 1px solid rgba(255,255,255,0.1);
        flex-shrink: 0; min-height: 32px;
    `;
    drawerEl.appendChild(tabBarEl);

    // Table container (scrollable)
    const tableContainer = document.createElement('div');
    tableContainer.style.cssText = 'flex: 1; overflow: auto;';
    tableEl = document.createElement('table');
    tableEl.style.cssText = `
        width: 100%; border-collapse: collapse;
        font-size: 11px; color: #ccc;
    `;
    tableContainer.appendChild(tableEl);
    drawerEl.appendChild(tableContainer);

    // Footer
    footerEl = document.createElement('div');
    footerEl.style.cssText = `
        padding: 6px 10px; color: #666; font-size: 11px;
        border-top: 1px solid rgba(255,255,255,0.1); flex-shrink: 0;
    `;
    drawerEl.appendChild(footerEl);

    // Drag handle
    handleEl = document.createElement('div');
    handleEl.id = 'drawer-handle';
    handleEl.style.cssText = `
        position: fixed; left: 0; top: 50%; transform: translateY(-50%);
        width: ${HANDLE_WIDTH}px; height: 48px;
        background: rgba(255,255,255,0.15); cursor: col-resize;
        z-index: 6; transition: background 0.15s;
    `;
    handleEl.addEventListener('mouseenter', () => { handleEl.style.background = 'rgba(255,255,255,0.3)'; });
    handleEl.addEventListener('mouseleave', () => { handleEl.style.background = 'rgba(255,255,255,0.15)'; });

    document.body.appendChild(drawerEl);
    document.body.appendChild(handleEl);

    // Hide on mobile
    const mq = window.matchMedia('(max-width: 768px)');
    const toggle = () => {
        const mobile = mq.matches;
        handleEl.style.display = mobile ? 'none' : '';
        if (mobile && drawerWidth > 0) setDrawerWidth(0);
    };
    mq.addEventListener('change', toggle);
    toggle();
}

function setDrawerWidth(w) {
    drawerWidth = w;
    drawerEl.style.width = w + 'px';
    handleEl.style.left = w + 'px';
    const mapEl = document.getElementById('map');
    mapEl.style.marginLeft = w + 'px';
    mapEl.style.width = `calc(100% - ${w}px)`;
}

function bindDrag() {
    let startX = 0;
    let startWidth = 0;
    let dragging = false;

    handleEl.addEventListener('pointerdown', e => {
        e.preventDefault();
        handleEl.setPointerCapture(e.pointerId);
        startX = e.clientX;
        startWidth = drawerWidth;
        dragging = true;
        drawerEl.style.transition = 'none';
        document.getElementById('map').style.transition = 'none';
    });

    handleEl.addEventListener('pointermove', e => {
        if (!dragging) return;
        const maxW = window.innerWidth - 400;
        const newW = Math.max(0, Math.min(maxW, startWidth + (e.clientX - startX)));
        setDrawerWidth(newW);
    });

    const endDrag = () => {
        if (!dragging) return;
        dragging = false;
        drawerEl.style.transition = 'width 0.2s';
        document.getElementById('map').style.transition = 'margin-left 0.2s, width 0.2s';

        // Snap closed if below min
        if (drawerWidth > 0 && drawerWidth < MIN_WIDTH) {
            setDrawerWidth(0);
        }

        map.resize();

        // If just opened, set initial tab and load data
        if (drawerWidth >= MIN_WIDTH && !activeTab) {
            activateFirstTab();
        }
        if (drawerWidth >= MIN_WIDTH) {
            refreshTable();
        }
    };

    handleEl.addEventListener('pointerup', endDrag);
    handleEl.addEventListener('pointercancel', endDrag);
}

function bindLayerToggles() {
    // app.js label click sets cb.checked programmatically then calls setLayerVisibility.
    // We dispatch a synthetic 'change' event from app.js (see Task 3) so the drawer
    // only needs to listen for 'change'.
    for (const row of document.querySelectorAll('.toggle-row[data-layer]')) {
        const cb = row.querySelector('input');
        cb.addEventListener('change', () => {
            updateTabs();
            if (drawerWidth >= MIN_WIDTH) refreshTable();
        });
    }
}

function bindMapEvents() {
    let debounceTimer = null;
    map.on('moveend', () => {
        if (drawerWidth < MIN_WIDTH) return;
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => refreshTable(), 150);
    });
}

function getVisibleLayers() {
    const visible = [];
    for (const row of document.querySelectorAll('.toggle-row[data-layer]')) {
        const layer = row.dataset.layer;
        const cb = row.querySelector('input');
        if (cb.checked && LAYERS[layer]) visible.push(layer);
    }
    return visible;
}

function updateTabs() {
    const visible = getVisibleLayers();
    tabBarEl.innerHTML = '';

    if (visible.length === 0) {
        activeTab = null;
        tableEl.innerHTML = '';
        footerEl.textContent = '';
        return;
    }

    // If active tab is no longer visible, switch to first
    if (!activeTab || !visible.includes(activeTab)) {
        activeTab = visible[0];
        sortCol = null;
        sortDir = 'ASC';
    }

    for (const layer of visible) {
        const tab = document.createElement('div');
        const info = LAYERS[layer];
        tab.textContent = info.label;
        tab.style.cssText = `
            padding: 6px 12px; cursor: pointer; font-size: 11px;
            color: ${layer === activeTab ? info.color : '#666'};
            border-bottom: 2px solid ${layer === activeTab ? info.color : 'transparent'};
            transition: color 0.15s;
        `;
        tab.addEventListener('click', () => {
            if (activeTab === layer) return;
            activeTab = layer;
            sortCol = null;
            sortDir = 'ASC';
            updateTabs();
            refreshTable();
        });
        tabBarEl.appendChild(tab);
    }
}

function activateFirstTab() {
    const visible = getVisibleLayers();
    if (visible.length > 0) {
        activeTab = visible[0];
        sortCol = null;
        sortDir = 'ASC';
        updateTabs();
    }
}

async function refreshTable() {
    if (!activeTab || drawerWidth < MIN_WIDTH) return;

    const b = map.getBounds();
    const bounds = { south: b.getSouth(), north: b.getNorth(), west: b.getWest(), east: b.getEast() };

    const [data, count] = await Promise.all([
        queryTableRaw(activeTab, { bounds, orderBy: sortCol, orderDir: sortDir, limit: 1000 }),
        queryTableCount(activeTab, { bounds }),
    ]);

    currentRows = data;
    currentTotalCount = count;
    renderTable(data, count);
}

function renderTable(data, totalCount) {
    if (!data.length) {
        tableEl.innerHTML = '<tr><td style="padding: 20px; color: #666; text-align: center;">No data in view</td></tr>';
        footerEl.textContent = '0 in view';
        return;
    }

    const info = LAYERS[activeTab];
    const cols = Object.keys(data[0]);

    // Header
    let html = '<thead><tr>';
    for (const col of cols) {
        const isSorted = sortCol === col;
        const arrow = isSorted ? (sortDir === 'ASC' ? ' ↑' : ' ↓') : '';
        html += `<th data-col="${col}" style="
            position: sticky; top: 0; background: #1a1a2e;
            padding: 4px 8px; text-align: left; cursor: pointer;
            color: ${isSorted ? '#fff' : '#888'}; font-size: 10px;
            font-weight: normal; white-space: nowrap; border-bottom: 1px solid rgba(255,255,255,0.1);
            user-select: none;
        ">${col}${arrow}</th>`;
    }
    html += '</tr></thead><tbody>';

    // Rows
    for (let i = 0; i < data.length; i++) {
        const row = data[i];
        const rowId = getRowId(row, activeTab);
        const isSelected = selectedId != null && rowId === selectedId;
        const bgColor = i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)';

        html += `<tr data-idx="${i}" style="
            cursor: pointer; background: ${isSelected ? info.color + '15' : bgColor};
            border-left: 3px solid ${isSelected ? info.color : 'transparent'};
        ">`;
        for (const col of cols) {
            const v = row[col];
            const display = v == null ? '' : String(v);
            const isNum = typeof v === 'number';
            html += `<td style="
                padding: 3px 8px; white-space: nowrap;
                font-family: ${isNum ? "'SF Mono', 'Menlo', monospace" : 'inherit'};
                font-size: 11px; color: #bbb;
            ">${display}</td>`;
        }
        html += '</tr>';
    }
    html += '</tbody>';
    tableEl.innerHTML = html;

    // Footer
    if (totalCount > data.length) {
        footerEl.textContent = `${data.length.toLocaleString()} of ${totalCount.toLocaleString()} in view`;
    } else {
        footerEl.textContent = `${totalCount.toLocaleString()} in view`;
    }

    // Bind header click for sorting
    tableEl.querySelectorAll('th[data-col]').forEach(th => {
        th.addEventListener('click', () => {
            const col = th.dataset.col;
            if (sortCol === col) {
                sortDir = sortDir === 'ASC' ? 'DESC' : 'ASC';
            } else {
                sortCol = col;
                sortDir = 'ASC';
            }
            refreshTable();
        });
    });

    // Bind row click for selection
    tableEl.querySelectorAll('tbody tr[data-idx]').forEach(tr => {
        tr.addEventListener('click', () => {
            const idx = Number(tr.dataset.idx);
            const row = currentRows[idx];
            if (!row) return;

            const info = LAYERS[activeTab];
            const lat = Number(row[info.latCol]);
            const lon = Number(row[info.lonCol]);

            selectedId = getRowId(row, activeTab);
            renderTable(currentRows, currentTotalCount); // re-render to update highlight

            if (onRowClick) onRowClick(activeTab, row);
            map.flyTo({ center: [lon, lat], zoom: Math.max(map.getZoom(), 13) });
        });
    });
}

function getRowId(row, table) {
    const info = LAYERS[table];
    if (info.idCol) return String(row[info.idCol]);
    // Permits: composite key
    return `${row.latitude}_${row.longitude}_${row.name}`;
}

// Public: highlight a row from map selection (or clear with null)
export function highlight(layerType, id) {
    if (drawerWidth < MIN_WIDTH) return;

    if (layerType == null || id == null) {
        selectedId = null;
        if (activeTab) renderTable(currentRows, currentTotalCount);
        return;
    }

    // Map layer names to drawer tab names
    const tabMap = {
        'flares-layer': 'flares', 'flare-pixels-fill': 'flares', 'flare-pixels-layer': 'flares',
        'permits-layer': 'permits', 'plumes-layer': 'plumes', 'wells-layer': 'wells',
    };
    const tab = tabMap[layerType] || layerType;

    selectedId = String(id);

    // If the matching tab is active, re-render to show highlight
    if (tab === activeTab) {
        const idx = currentRows.findIndex(r => getRowId(r, activeTab) === selectedId);
        renderTable(currentRows, currentTotalCount);
        if (idx >= 0) {
            const tr = tableEl.querySelector(`tr[data-idx="${idx}"]`);
            if (tr) tr.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
        }
    }
}
```

- [ ] **Step 2: Verify DOM injection in browser**

Temporarily add to app.js after `map.once('idle', updateStats);`:

```javascript
import('./drawer.js').then(d => d.init(map));
```

Open the app. Verify:
- A thin square handle (6px × 48px) appears at the left edge of the screen
- Dragging it right reveals the dark drawer panel
- Map resizes when you release the handle
- Tabs show for visible layers (Flares, Permits by default)
- Dragging below 300px snaps closed

Remove the temporary import after verifying.

- [ ] **Step 3: Commit**

```bash
git add web/drawer.js
git commit -m "feat: add data drawer module with drag, tabs, and table rendering"
```

---

## Chunk 3: Integration with app.js and Bidirectional Selection

### Task 3: Wire drawer into app.js with selection linking

**Files:**
- Modify: `web/app.js`

Connect the drawer to the app's lifecycle and enable bidirectional selection.

- [ ] **Step 1: Add drawer import and fix label click change event**

At the top of `web/app.js`, after the enhance import (line 2):

```javascript
import * as drawer from './drawer.js';
```

In `bindUI()`, the `.filter-label` click handler (lines 358-361) sets `cb.checked` programmatically without firing a `change` event. Add a synthetic dispatch so the drawer's listener fires. Change:

```javascript
        row.querySelector('.filter-label').addEventListener('click', () => {
            cb.checked = !cb.checked;
            setLayerVisibility(layer, cb.checked);
        });
```

To:

```javascript
        row.querySelector('.filter-label').addEventListener('click', () => {
            cb.checked = !cb.checked;
            cb.dispatchEvent(new Event('change'));
        });
```

- [ ] **Step 2: Initialize drawer after map loads**

In the `map.on('load', ...)` handler, after `map.once('idle', updateStats);` (line 71), add:

```javascript
    drawer.init(map, {
        onSelect: (layer, row) => {
            const info = {
                flares: { latCol: 'lat', lonCol: 'lon' },
                permits: { latCol: 'latitude', lonCol: 'longitude' },
                plumes: { latCol: 'latitude', lonCol: 'longitude' },
                wells: { latCol: 'latitude', lonCol: 'longitude' },
            }[layer];
            if (!info) return;

            // Build a mock feature to reuse existing detail functions
            const lat = Number(row[info.latCol]);
            const lon = Number(row[info.lonCol]);
            const feature = {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [lon, lat] },
                properties: row,
                layer: { id: `${layer}-layer` },
            };
            overlappingFeatures = [feature];
            overlapIndex = 0;
            showFeatureDetail(feature);
        }
    });
```

- [ ] **Step 3: Add drawer highlight calls to map click handler**

In the `map.on('click', ...)` handler inside `bindUI()`, after `showFeatureDetail(features[0]);` (line 444), add:

```javascript
        // Sync selection to drawer
        const f = features[0];
        const layerId = f.layer.id;
        if (layerId.startsWith('flare')) {
            drawer.highlight('flares-layer', String(f.properties.flare_id));
        } else if (layerId.startsWith('permits')) {
            drawer.highlight('permits-layer', `${f.properties.latitude}_${f.properties.longitude}_${f.properties.name}`);
        } else if (layerId.startsWith('plumes')) {
            drawer.highlight('plumes-layer', String(f.properties.plume_id));
        } else if (layerId.startsWith('wells')) {
            drawer.highlight('wells-layer', String(f.properties.api));
        }
```

- [ ] **Step 4: Clear drawer highlight on detail close**

In the `closeDetail()` function, after `overlapIndex = 0;` (line 527), add:

```javascript
    drawer.highlight(null, null);
```

- [ ] **Step 5: Verify bidirectional linking**

Open the app, drag the drawer open:
1. Click a row in the flares table → map should fly to that flare and open its detail card
2. Click a flare on the map → the corresponding row in the drawer should highlight and scroll into view
3. Close the detail card → highlight should clear

- [ ] **Step 6: Commit**

```bash
git add web/app.js
git commit -m "feat: integrate data drawer with map selection"
```

---

## Chunk 4: Cache-bust imports and final polish

### Task 4: Update import version strings and add .gitignore entry

**Files:**
- Modify: `web/app.js` (import path)
- Modify: `web/drawer.js` (import path)
- Modify: `.gitignore`

- [ ] **Step 1: Update db.js import version in app.js**

The app uses cache-busting query strings on imports (e.g., `./db.js?v=4`). Update app.js line 1 to increment the version if needed, and ensure `drawer.js` import uses a version string:

In `web/app.js`, change the drawer import to:
```javascript
import * as drawer from './drawer.js?v=1';
```

- [ ] **Step 2: Add .superpowers/ to .gitignore**

Check if `.superpowers/` is already in `.gitignore`. If not, add it:

```bash
echo '.superpowers/' >> .gitignore
```

- [ ] **Step 3: Test full flow end-to-end**

1. `make serve` and open localhost:8080
2. Drag handle to open drawer — tabs show Flares and Permits (default visible layers)
3. Pan map — table data updates to show only items in viewport
4. Toggle Wells on — "Wells" tab appears
5. Switch to Wells tab — shows well data in view
6. Toggle Wells off — tab disappears, switches to next available
7. Click a table row — map flies to location, detail card opens, row highlights
8. Click a map feature — matching table row highlights and scrolls into view
9. Sort by a column header — data re-queries with ORDER BY
10. Drag drawer below 300px — snaps closed
11. On mobile viewport (<768px) — no handle visible

- [ ] **Step 4: Commit**

```bash
git add web/app.js web/drawer.js .gitignore
git commit -m "feat: finalize data drawer integration"
```
