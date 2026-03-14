const _css = k => getComputedStyle(document.documentElement).getPropertyValue(k).trim();

const LAYERS = {
    flares:  { label: 'Flares',  color: () => _css('--color-flare'),  latCol: 'lat',       lonCol: 'lon',       idCol: 'flare_id' },
    permits: { label: 'Permits', color: () => _css('--color-permit'), latCol: 'latitude',  lonCol: 'longitude',  idCol: null },
    plumes:  { label: 'Plumes',  color: () => _css('--color-plume'),  latCol: 'latitude',  lonCol: 'longitude',  idCol: 'plume_id' },
    wells:   { label: 'Wells',   color: () => _css('--color-well'),   latCol: 'latitude', lonCol: 'longitude', idCol: 'api' },
    infra:   { label: 'Infrastructure', color: () => _css('--color-infra'), latCol: 'latitude', lonCol: 'longitude', idCol: 'serial_number' },
};

const MIN_WIDTH = 300;
const MAX_ROWS = 1000;

function resetSelection() {
    sortCol = null; sortDir = 'ASC'; selectedIdx = -1; selectedId = null;
}

function rowCoords(row, info) {
    return [Number(row[info.latCol]), Number(row[info.lonCol])];
}

function esc(v) {
    return String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

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
let selectedIdx = -1;
let sortCol = null;
let sortDir = 'ASC';

// Feature data pushed from app.js after each load
const allData = {};

// Callbacks set by app.js
let onRowClick = null;

export function init(mapInstance, { onSelect } = {}) {
    map = mapInstance;
    onRowClick = onSelect || null;
    createDOM();
    bindDrag();
    bindLayerToggles();
    bindMapEvents();
    bindKeyboard();
}

// Called by app.js after loading data for a layer
export function setData(layer, features) {
    allData[layer] = features.map(f => f.properties);
    if (layer === activeTab && drawerWidth >= MIN_WIDTH) refreshTable();
}

function createDOM() {
    drawerEl = document.createElement('div');
    drawerEl.id = 'data-drawer';

    tabBarEl = document.createElement('div');
    tabBarEl.className = 'drawer-tab-bar';
    drawerEl.appendChild(tabBarEl);

    const tableContainer = document.createElement('div');
    tableContainer.className = 'drawer-table-wrap';
    tableEl = document.createElement('table');
    tableEl.className = 'drawer-table';
    tableContainer.appendChild(tableEl);
    drawerEl.appendChild(tableContainer);

    footerEl = document.createElement('div');
    footerEl.className = 'drawer-footer';
    drawerEl.appendChild(footerEl);

    handleEl = document.createElement('div');
    handleEl.id = 'drawer-handle';
    handleEl.innerHTML = '<div class="handle-bar"></div><div class="handle-label">data table</div>';

    document.body.appendChild(drawerEl);
    document.body.appendChild(handleEl);
}

function setDrawerWidth(w) {
    drawerWidth = w;
    drawerEl.style.width = w + 'px';
    handleEl.style.left = w + 'px';

    // Map stays full-width; satellite tiles render under the drawer.
    // setPadding shifts the logical centre so pan/zoom feel correct.
    map.setPadding({ left: w });

    // Keep left panel in sync
    const leftPanel = document.getElementById('left-panel');
    if (leftPanel) leftPanel.style.left = (w + 16) + 'px';
}

function bindDrag() {
    let startX = 0;
    let startWidth = 0;
    let dragging = false;

    // Pre-render table on hover so data is ready before drag starts
    let preloaded = false;
    handleEl.addEventListener('pointerenter', () => {
        if (preloaded || drawerWidth >= MIN_WIDTH) return;
        preloaded = true;
        if (!activeTab) activateFirstTab();
        refreshTable();
    });
    handleEl.addEventListener('pointerleave', () => { preloaded = false; });

    handleEl.addEventListener('pointerdown', e => {
        e.preventDefault();
        handleEl.setPointerCapture(e.pointerId);
        startX = e.clientX;
        startWidth = drawerWidth;
        dragging = true;
        drawerEl.style.transition = 'none';

        if (!activeTab) activateFirstTab();
    });

    let refreshRAF = null;

    handleEl.addEventListener('pointermove', e => {
        if (!dragging) return;
        const maxW = window.innerWidth - 400;
        const newW = Math.max(0, Math.min(maxW, startWidth + (e.clientX - startX)));
        setDrawerWidth(newW);

        // Live-render table while dragging, throttled to one per frame
        if (newW >= MIN_WIDTH && !refreshRAF) {
            refreshRAF = requestAnimationFrame(() => {
                refreshRAF = null;
                refreshTable();
            });
        }
    });

    const endDrag = () => {
        if (!dragging) return;
        dragging = false;
        if (refreshRAF) { cancelAnimationFrame(refreshRAF); refreshRAF = null; }
        drawerEl.style.transition = 'width 0.2s';

        if (drawerWidth > 0 && drawerWidth < MIN_WIDTH) {
            setDrawerWidth(0);
        } else if (drawerWidth >= MIN_WIDTH) {
            refreshTable();
        }
    };

    handleEl.addEventListener('pointerup', endDrag);
    handleEl.addEventListener('pointercancel', endDrag);
}

function bindLayerToggles() {
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

function bindKeyboard() {
    document.addEventListener('keydown', e => {
        if (drawerWidth < MIN_WIDTH || !activeTab) return;
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

        const visible = getVisibleLayers();

        switch (e.key) {
            case 'j':
            case 'ArrowDown':
                e.preventDefault();
                selectRow(selectedIdx + 1);
                break;
            case 'k':
            case 'ArrowUp':
                e.preventDefault();
                selectRow(selectedIdx - 1);
                break;
            case 'h':
            case 'ArrowLeft': {
                e.preventDefault();
                const ci = visible.indexOf(activeTab);
                if (ci > 0) {
                    activeTab = visible[ci - 1];
                    resetSelection();
                    updateTabs();
                    refreshTable();
                }
                break;
            }
            case 'l':
            case 'ArrowRight': {
                e.preventDefault();
                const ci = visible.indexOf(activeTab);
                if (ci < visible.length - 1) {
                    activeTab = visible[ci + 1];
                    resetSelection();
                    updateTabs();
                    refreshTable();
                }
                break;
            }
            case 'Enter': {
                if (selectedIdx >= 0 && selectedIdx < currentRows.length) {
                    e.preventDefault();
                    const row = currentRows[selectedIdx];
                    const [lat, lon] = rowCoords(row, LAYERS[activeTab]);
                    if (onRowClick) onRowClick(activeTab, row);
                    map.flyTo({ center: [lon, lat], zoom: Math.max(map.getZoom(), 13) });
                }
                break;
            }
            case 'g':
                e.preventDefault();
                selectRow(0);
                break;
            case 'G':
                e.preventDefault();
                selectRow(currentRows.length - 1);
                break;
        }
    });
}

function selectRow(idx) {
    if (currentRows.length === 0) return;
    idx = Math.max(0, Math.min(currentRows.length - 1, idx));
    selectedIdx = idx;
    selectedId = getRowId(currentRows[idx], activeTab);
    renderTable(currentRows, currentTotalCount);
    const tr = tableEl.querySelector(`tr[data-idx="${idx}"]`);
    if (tr) tr.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
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

    if (!activeTab || !visible.includes(activeTab)) {
        activeTab = visible[0];
        resetSelection();
    }

    for (const layer of visible) {
        const tab = document.createElement('div');
        const info = LAYERS[layer];
        tab.className = 'drawer-tab' + (layer === activeTab ? ' active' : '');
        tab.textContent = info.label;
        if (layer === activeTab) {
            tab.style.color = info.color();
            tab.style.borderBottomColor = info.color();
        }
        tab.addEventListener('click', () => {
            if (activeTab === layer) return;
            activeTab = layer;
            resetSelection();
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
        resetSelection();
        updateTabs();
    }
}

// Filter + sort the already-loaded data client-side
function refreshTable() {
    if (!activeTab || drawerWidth < MIN_WIDTH) return;

    const rows = allData[activeTab] || [];
    const info = LAYERS[activeTab];
    const b = map.getBounds();
    const south = b.getSouth(), north = b.getNorth(), west = b.getWest(), east = b.getEast();

    // Viewport filter
    let filtered = rows.filter(r => {
        const [lat, lon] = rowCoords(r, info);
        return lat >= south && lat <= north && lon >= west && lon <= east;
    });

    const totalCount = filtered.length;

    // Sort
    if (sortCol) {
        const dir = sortDir === 'DESC' ? -1 : 1;
        filtered.sort((a, b) => {
            const va = a[sortCol], vb = b[sortCol];
            if (va == null && vb == null) return 0;
            if (va == null) return 1;
            if (vb == null) return -1;
            return (va < vb ? -1 : va > vb ? 1 : 0) * dir;
        });
    }

    // Paginate
    if (filtered.length > MAX_ROWS) filtered = filtered.slice(0, MAX_ROWS);

    currentRows = filtered;
    currentTotalCount = totalCount;
    renderTable(filtered, totalCount);
}

function renderTable(data, totalCount) {
    if (!data.length) {
        tableEl.innerHTML = '<tr><td class="drawer-empty">No data in view</td></tr>';
        footerEl.textContent = '0 in view';
        return;
    }

    const info = LAYERS[activeTab];
    const cols = Object.keys(data[0]);

    let html = '<thead><tr>';
    for (const col of cols) {
        const isSorted = sortCol === col;
        const arrow = isSorted ? (sortDir === 'ASC' ? ' \u2191' : ' \u2193') : '';
        html += `<th data-col="${col}" class="${isSorted ? 'sorted' : ''}">${col}${arrow}</th>`;
    }
    html += '</tr></thead><tbody>';

    for (let i = 0; i < data.length; i++) {
        const row = data[i];
        const rowId = getRowId(row, activeTab);
        const isSelected = selectedId != null && rowId === selectedId;

        html += `<tr data-idx="${i}"${isSelected ? ' class="selected"' : ''}>`;
        for (const col of cols) {
            const v = row[col];
            html += `<td>${v == null ? '' : esc(v)}</td>`;
        }
        html += '</tr>';
    }
    html += '</tbody>';
    tableEl.innerHTML = html;

    if (totalCount > data.length) {
        footerEl.textContent = `${data.length.toLocaleString()} of ${totalCount.toLocaleString()} in view`;
    } else {
        footerEl.textContent = `${totalCount.toLocaleString()} in view`;
    }

    // Header click for sorting
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

    // Row click for selection
    tableEl.querySelectorAll('tbody tr[data-idx]').forEach(tr => {
        tr.addEventListener('click', () => {
            const idx = Number(tr.dataset.idx);
            const row = currentRows[idx];
            if (!row) return;

            const [lat, lon] = rowCoords(row, LAYERS[activeTab]);

            selectedIdx = idx;
            selectedId = getRowId(row, activeTab);
            renderTable(currentRows, currentTotalCount);

            if (onRowClick) onRowClick(activeTab, row);
            map.flyTo({ center: [lon, lat], zoom: Math.max(map.getZoom(), 13) });
        });
    });
}

function getRowId(row, table) {
    const info = LAYERS[table];
    if (info.idCol) return String(row[info.idCol]);
    const lat = row.latitude ?? row.lat;
    const lon = row.longitude ?? row.lon;
    return `${lat}_${lon}_${row.name ?? row.operator ?? ''}`;
}

export function highlight(layerType, id) {
    if (drawerWidth < MIN_WIDTH) return;

    if (layerType == null || id == null) {
        selectedId = null;
        selectedIdx = -1;
        if (activeTab) renderTable(currentRows, currentTotalCount);
        return;
    }

    const tabMap = {
        'flares-layer': 'flares', 'flare-pixels-fill': 'flares', 'flare-pixels-layer': 'flares',
        'permits-layer': 'permits', 'plumes-layer': 'plumes', 'wells-layer': 'wells', 'infra-layer': 'infra',
    };
    const tab = tabMap[layerType] || layerType;

    selectedId = String(id);

    if (tab === activeTab) {
        selectedIdx = currentRows.findIndex(r => getRowId(r, activeTab) === selectedId);
        renderTable(currentRows, currentTotalCount);
        if (selectedIdx >= 0) {
            const tr = tableEl.querySelector(`tr[data-idx="${selectedIdx}"]`);
            if (tr) tr.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
        }
    }
}
