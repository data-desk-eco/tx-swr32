import * as db from './db.js?v=7';
import { enhance, cancelEnhance, setUpdateCallback, getState, loadAllCached, getCluster, isEnhancing } from './enhance.js?v=4';
import * as drawer from './drawer.js?v=2';
import { searchSTAC } from './vendor/s2-flares/stac.js';
import { openCOG } from './vendor/s2-flares/cog.js';
import { wgs84ToUtm, utmToWgs84, utmParams } from './vendor/s2-flares/geo.js';

const _css = k => getComputedStyle(document.documentElement).getPropertyValue(k).trim();
const COLORS = {
    flare: _css('--color-flare'),
    permit: _css('--color-permit'),
    plume: _css('--color-plume'),
    well: _css('--color-well'),
    infra: _css('--color-infra'),
};

// Geo constants
const LAT_PER_M = 1 / 110540;
const LON_PER_M = lat => 1 / (111320 * Math.cos(lat * Math.PI / 180));

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

// Color ramps
function b12Color(b) {
    return b < 0.3 ? '#660800' : b < 0.5 ? '#991100' : b < 0.7 ? '#cc2200' : b < 0.9 ? '#ff4422' : b < 1.2 ? '#ff8844' : '#ffcc44';
}
function mwColor(mw) {
    return mw < 0.3 ? '#660800' : mw < 0.6 ? '#991100' : mw < 0.9 ? '#cc3300' : mw < 1.3 ? '#ff5522' : mw < 2 ? '#ff8844' : mw < 4 ? '#ffcc66' : '#ffeeaa';
}

function fmtCoords(lat, lon) {
    return `${Number(lat).toFixed(4)}, ${Number(lon).toFixed(4)}`;
}

// DOM cache for detail panel
const $ = id => document.getElementById(id);
function openDetail(title, lat, lon, body) {
    $('detail-title').textContent = title;
    $('detail-coords').textContent = fmtCoords(lat, lon);
    $('intensity-chart').innerHTML = '';
    removeS2Badge();
    $('detail-body').innerHTML = Array.isArray(body)
        ? body.flat(Infinity).filter(Boolean).join('')
        : body;
    const panel = $('detail-panel');
    panel.classList.remove('hidden');
    panel.scrollTop = 0;
}

let layerState = { flares: true, permits: true, plumes: false, wells: false, infra: false };
let operatorFilter = '';
let overlappingFeatures = [];
let overlapIndex = 0;
let flareFeatures = [];

const map = new maplibregl.Map({
    container: 'map',
    style: {
        version: 8,
        glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
        sources: {
            satellite: {
                type: 'raster',
                tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
                tileSize: 256
            },
            labels: {
                type: 'vector',
                url: 'https://tiles.openfreemap.org/planet'
            }
        },
        layers: [
            { id: 'basemap', type: 'raster', source: 'satellite', paint: { 'raster-saturation': -1, 'raster-brightness-max': 0.65 } },
            {
                id: 'state-borders', type: 'line', source: 'labels', 'source-layer': 'boundary',
                filter: ['==', ['get', 'admin_level'], 4],
                paint: { 'line-color': 'rgba(255,255,255,0.15)', 'line-width': 0.5 }
            },
            {
                id: 'place-labels', type: 'symbol', source: 'labels', 'source-layer': 'place',
                filter: ['in', ['get', 'class'], ['literal', ['city', 'town']]],
                layout: { 'text-field': ['get', 'name:en'], 'text-font': ['Noto Sans Regular'], 'text-size': 11, 'text-anchor': 'center' },
                paint: { 'text-color': 'rgba(255,255,255,0.5)', 'text-halo-color': 'rgba(0,0,0,0.7)', 'text-halo-width': 1 }
            }
        ]
    },
    center: [-102.5, 31.8],
    zoom: 7,
    minZoom: 7,
    maxBounds: [[-107, 29], [-98, 35]],
    projection: 'globe',
    hash: 'map'
});


// Start DuckDB init immediately — runs in parallel with map tile loading
const dbReady = db.init();

map.on('load', async () => {
    $('stat-sites').textContent = 'Loading...';

    await dbReady;

    addEmptySources();
    addLayers();
    bindUI();
    await refreshFlares();
    // Tier 1: start loading permits + plumes in background
    db.loadTier1();
    await loadPermits();
    loadCachedS2();
    updateMapCentre();
    handleDeepLink();
    // Stats use queryRenderedFeatures — wait for first idle after data loads
    map.once('idle', updateStats);

    drawer.init(map, {
        onSelect: (layer, row) => {
            const info = {
                flares: { latCol: 'lat', lonCol: 'lon' },
                permits: { latCol: 'latitude', lonCol: 'longitude' },
                plumes: { latCol: 'latitude', lonCol: 'longitude' },
                wells: { latCol: 'latitude', lonCol: 'longitude' },
                infra: { latCol: 'latitude', lonCol: 'longitude' },
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

});

function syncDrawer(f) {
    const lid = f.layer.id;
    const p = f.properties;
    if (lid.startsWith('flare')) drawer.highlight('flares-layer', String(p.flare_id));
    else if (lid.startsWith('permits')) drawer.highlight('permits-layer', `${p.latitude}_${p.longitude}_${p.name}`);
    else if (lid.startsWith('plumes')) drawer.highlight('plumes-layer', String(p.plume_id));
    else if (lid.startsWith('wells')) drawer.highlight('wells-layer', String(p.api));
    else if (lid.startsWith('infra')) drawer.highlight('infra-layer', String(p.serial_number));
}

function featureKey(f) {
    const p = f.properties;
    if (f.layer.id === 's2-points' && p.id) return `s2:${p.id}`;
    if (p.flare_id != null) return `flare:${p.flare_id}`;
    if (p.plume_id != null) return `plume:${p.plume_id}`;
    if (p.api != null) return `well:${p.api}`;
    if (p.serial_number != null && f.layer.id.startsWith('infra')) return `infra:${p.serial_number}`;
    if (p.name != null && p.latitude != null) return `permit:${p.latitude}_${p.longitude}_${p.name}`;
    return `${f.layer.id}:${f.id}`;
}

function addEmptySources() {
    const empty = { type: 'FeatureCollection', features: [] };
    map.addSource('texas', { type: 'geojson', data: 'data/texas.geojson' });
    map.addSource('flares', { type: 'geojson', data: empty });
    map.addSource('permits', { type: 'geojson', data: empty });
    map.addSource('plumes', { type: 'geojson', data: empty });
    map.addSource('wells', { type: 'geojson', data: empty });
    map.addSource('infra', { type: 'geojson', data: empty });
    map.addSource('flare-pixels', { type: 'geojson', data: empty });
    map.addSource('s2-detections', { type: 'geojson', data: empty });
}

function addWellImage() {
    const size = 24;
    const canvas = document.createElement('canvas');
    canvas.width = size;
    canvas.height = size;
    const ctx = canvas.getContext('2d');
    ctx.strokeStyle = '#fff';
    ctx.lineWidth = 5;
    ctx.lineCap = 'round';
    ctx.beginPath();
    ctx.moveTo(4, 4); ctx.lineTo(size - 4, size - 4);
    ctx.moveTo(size - 4, 4); ctx.lineTo(4, size - 4);
    ctx.stroke();
    const imgData = ctx.getImageData(0, 0, size, size);
    map.addImage('well-x', { width: size, height: size, data: imgData.data }, { sdf: true });
}

function addInfraImage() {
    const size = 24;
    const canvas = document.createElement('canvas');
    canvas.width = size;
    canvas.height = size;
    const ctx = canvas.getContext('2d');
    ctx.strokeStyle = '#fff';
    ctx.lineWidth = 4;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.beginPath();
    ctx.moveTo(size / 2, 4);
    ctx.lineTo(size - 4, size - 4);
    ctx.lineTo(4, size - 4);
    ctx.closePath();
    ctx.stroke();
    const imgData = ctx.getImageData(0, 0, size, size);
    map.addImage('infra-triangle', { width: size, height: size, data: imgData.data }, { sdf: true });
}

function addLayers() {
    // Flare radius: scale on total_rh_mw (MW)
    const flareRadius = [
        'interpolate', ['linear'],
        ['coalesce', ['get', 'total_rh_mw'], 0],
        0, 2, 10, 4, 50, 7, 200, 12, 1000, 20, 5000, 32
    ];

    map.addLayer({
        id: 'texas-border', type: 'line', source: 'texas',
        paint: { 'line-color': 'rgba(255,255,255,0.2)', 'line-width': 1 }
    });

    // Permit radius: sqrt-ish scale on max_release_rate_mcf_day (huge range, 3–680K)
    const permitRadius = [
        'interpolate', ['linear'],
        ['coalesce', ['get', 'max_release_rate_mcf_day'], 0],
        0, 1.5, 100, 2, 1000, 3.5, 5000, 6, 25000, 10, 100000, 16
    ];

    // Wells: fixed-size X markers, visible at z10+
    // Color by combined score: sqrt(intensity% × ln(1 + flared_mcf))
    // Weights both how wasteful (intensity) and how much gas (volume)
    addWellImage();
    const wellScore = ['sqrt', ['*',
        ['coalesce', ['get', 'flaring_intensity_pct'], 0],
        ['ln', ['+', 1, ['coalesce', ['get', 'flared_mcf'], 0]]]
    ]];
    const wellColor = [
        'interpolate', ['linear'], wellScore,
        0, '#555566',
        1, '#660800',
        4, '#991100',
        8, '#cc2200',
        12, '#ff4422',
        16, '#ff8844',
        22, '#ffcc44',
        30, '#ffeeaa'
    ];
    map.addLayer({
        id: 'wells-layer', type: 'symbol', source: 'wells',
        minzoom: 10,
        layout: {
            visibility: 'none',
            'icon-image': 'well-x',
            'icon-size': 0.4,
            'icon-allow-overlap': true,
            'icon-ignore-placement': true,
        },
        paint: {
            'icon-color': wellColor,
            'icon-opacity': 0.85,
        }
    });

    // Infrastructure: R-3 gas processing facilities (triangle marker)
    addInfraImage();
    map.addLayer({
        id: 'infra-layer', type: 'symbol', source: 'infra',
        layout: {
            visibility: 'none',
            'icon-image': 'infra-triangle',
            'icon-size': 0.45,
            'icon-allow-overlap': true,
            'icon-ignore-placement': true,
        },
        paint: {
            'icon-color': COLORS.infra,
            'icon-opacity': 0.85,
        }
    });

    map.addLayer({
        id: 'permits-layer', type: 'circle', source: 'permits',
        paint: {
            'circle-radius': permitRadius,
            'circle-color': COLORS.permit,
            'circle-opacity': 0.25,
            'circle-stroke-width': 1,
            'circle-stroke-color': COLORS.permit
        }
    });

    map.addLayer({
        id: 'plumes-layer', type: 'circle', source: 'plumes',
        layout: { visibility: 'none' },
        paint: { 'circle-radius': PLUME_RADIUS, 'circle-color': COLORS.plume, 'circle-opacity': 0.25, 'circle-stroke-width': 1, 'circle-stroke-color': COLORS.plume }
    });

    // Flare stroke color ramp by avg_rh_mw (p25=0.5, p50=0.8, p75=1.3, p90=2.1)
    const flareColorRamp = [
        'interpolate', ['linear'],
        ['coalesce', ['get', 'avg_rh_mw'], 0],
        0, '#660800', 0.3, '#991100', 0.6, '#cc2200', 0.9, '#ff4422', 1.3, '#ff8844', 2, '#ffcc44', 4, '#ffeeaa'
    ];

    // VIIRS M-band pixel footprint (750m square) — invisible fill for click target
    map.addLayer({
        id: 'flare-pixels-fill', type: 'fill', source: 'flare-pixels',
        paint: { 'fill-color': 'transparent' }
    });

    // Dashed outline — fades in as the flare dot fades out (zoom 13→15)
    map.addLayer({
        id: 'flare-pixels-layer', type: 'line', source: 'flare-pixels',
        paint: {
            'line-color': 'rgba(255,255,255,0.8)',
            'line-width': 1,
            'line-dasharray': [3, 2],
            'line-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 1]
        }
    });

    // Label above pixel square (positioned at top-left corner)
    // Text scales with zoom so it stays proportional to the 750m box on the ground.
    // At z15 750m ≈ 200px, we want ~11px text. Doubling per zoom: 11 / 2^(15-z).
    map.addSource('flare-pixel-labels', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
    map.addLayer({
        id: 'flare-pixels-label', type: 'symbol', source: 'flare-pixel-labels',
        layout: {
            'text-field': 'FLARE DETECTION AREA',
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['exponential', 2], ['zoom'], 13, 2.75, 15, 11, 17, 44],
            'text-anchor': 'bottom-left',
            'text-max-width': 999,
            'text-offset': [-0.1, -0.3]
        },
        minzoom: 13,
        paint: {
            'text-color': 'rgba(255,255,255,0.8)',
            'text-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 1]
        }
    });

    map.addLayer({
        id: 'flares-layer', type: 'circle', source: 'flares',
        paint: {
            'circle-radius': flareRadius,
            'circle-color': flareColorRamp,
            'circle-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0.25, 15, 0],
            'circle-stroke-width': 1.5,
            'circle-stroke-color': flareColorRamp,
            'circle-stroke-opacity': ['interpolate', ['linear'], ['zoom'], 13, 1, 15, 0]
        }
    });

    // Sentinel-2 detection points (visible during/after enhance)
    // Styled like VNF flares: hollow circles scaled by intensity
    const s2ColorRamp = [
        'interpolate', ['linear'],
        ['coalesce', ['get', 'max_b12'], 0],
        0.3, '#660800', 0.5, '#991100', 0.7, '#cc2200', 0.9, '#ff4422', 1.2, '#ff8844', 1.5, '#ffcc44'
    ];
    map.addLayer({
        id: 's2-points',
        type: 'circle',
        source: 's2-detections',
        paint: {
            'circle-radius': ['interpolate', ['linear'],
                ['coalesce', ['get', 'max_b12'], 0],
                0.3, 3, 0.6, 5, 1.0, 8, 1.5, 12],
            'circle-color': s2ColorRamp,
            'circle-opacity': 0.25,
            'circle-stroke-width': 1.5,
            'circle-stroke-color': s2ColorRamp,
        },
    });

}

const PLUME_RADIUS = ['interpolate', ['linear'], ['coalesce', ['get', 'emission_rate'], 100], 10, 3, 500, 8, 5000, 18];

// Generate 750m square polygons and top-left label points from flare data
function flarePixelData(flareGeoJson) {
    const HALF_M = 375; // half of 750m pixel
    const squares = [];
    const labels = [];
    for (const f of flareGeoJson.features) {
        const [lon, lat] = f.geometry.coordinates;
        const dLat = HALF_M * LAT_PER_M;
        const dLon = HALF_M * LON_PER_M(lat);
        squares.push({
            type: 'Feature',
            geometry: {
                type: 'Polygon',
                coordinates: [[
                    [lon - dLon, lat - dLat],
                    [lon + dLon, lat - dLat],
                    [lon + dLon, lat + dLat],
                    [lon - dLon, lat + dLat],
                    [lon - dLon, lat - dLat]
                ]]
            },
            properties: f.properties
        });
        labels.push({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [lon - dLon, lat + dLat] },
            properties: f.properties
        });
    }
    return {
        squares: { type: 'FeatureCollection', features: squares },
        labels: { type: 'FeatureCollection', features: labels }
    };
}

async function refreshFlares() {
    const data = await db.queryFlares({ operator: operatorFilter || undefined });
    flareFeatures = data.features;
    map.getSource('flares').setData(data);
    const px = flarePixelData(data);
    map.getSource('flare-pixels').setData(px.squares);
    map.getSource('flare-pixel-labels').setData(px.labels);
    drawer.setData('flares', data.features);
}

async function loadPermits() {
    if (!layerState.permits) return;
    const data = await db.queryPermits({ operator: operatorFilter || undefined });
    map.getSource('permits').setData(data);
    drawer.setData('permits', data.features);
}

async function loadPlumes() {
    if (!layerState.plumes) return;
    const data = await db.queryPlumes();
    map.getSource('plumes').setData(data);
    drawer.setData('plumes', data.features);
}

async function loadInfra() {
    if (!layerState.infra) return;
    const data = await db.queryFacilities();
    map.getSource('infra').setData(data);
    drawer.setData('infra', data.features);
}

const WELLS_MIN_ZOOM = 10;

function updateWellsToggle() {
    const row = document.querySelector('.toggle-row[data-layer="wells"]');
    if (!row) return;
    const tooFar = map.getZoom() < WELLS_MIN_ZOOM;
    row.classList.toggle('disabled', tooFar);
    row.querySelector('input').disabled = tooFar;
}

async function loadWells() {
    if (!layerState.wells || map.getZoom() < WELLS_MIN_ZOOM) return;
    const b = map.getBounds();
    const bounds = { south: b.getSouth(), north: b.getNorth(), west: b.getWest(), east: b.getEast() };
    const data = await db.queryWells({ operator: operatorFilter || undefined, bounds });
    map.getSource('wells').setData(data);
    drawer.setData('wells', data.features);
}


function loadCachedS2() {
    const clusters = loadAllCached(); // also rebuilds clusterIndex
    if (clusters.length === 0) return;
    const fc = {
        type: 'FeatureCollection',
        features: clusters.map(d => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [d.lon, d.lat] },
            properties: {
                id: d.id, lon: d.lon, lat: d.lat,
                max_b12: d.max_b12, avg_b12: d.avg_b12,
                detection_count: d.detection_count, date_count: d.date_count,
                first_date: d.first_date, last_date: d.last_date,
                flare_id: d.flare_id,
            },
        })),
    };
    map.getSource('s2-detections').setData(fc);
}

function updateMapCentre() {
    const c = map.getCenter();
    $('map-centre').textContent = `${c.lat.toFixed(3)}, ${c.lng.toFixed(3)}`;
}

function updateStats() {
    const features = map.queryRenderedFeatures({ layers: ['flares-layer'] });
    const sites = features.length;
    const totalMw = features.reduce((s, f) => s + (Number(f.properties.total_rh_mw) || 0), 0);
    $('stat-sites').textContent = sites.toLocaleString();
    $('stat-mw').textContent = sites > 0 ? Math.round(totalMw).toLocaleString() : '--';
}

const LAYER_MAP = {
    flares: ['flares-layer', 'flare-pixels-fill', 'flare-pixels-layer', 'flare-pixels-label', 's2-points'],
    permits: ['permits-layer'],
    plumes: ['plumes-layer'],
    wells: ['wells-layer'],
    infra: ['infra-layer']
};

function setLayerVisibility(layer, visible) {
    layerState[layer] = visible;
    const vis = visible ? 'visible' : 'none';
    for (const id of LAYER_MAP[layer]) {
        map.setLayoutProperty(id, 'visibility', vis);
    }
    if (visible) {
        if (layer === 'permits') loadPermits();
        if (layer === 'plumes') loadPlumes();
        if (layer === 'wells') loadWells();
        if (layer === 'infra') loadInfra();
    }
}

const ALL_CLICK_LAYERS = [
    'flares-layer',
    'flare-pixels-fill',
    'flare-pixels-layer',
    's2-points',
    'permits-layer',
    'plumes-layer',
    'wells-layer',
    'infra-layer'
];

function bindUI() {
    $('collapse-toggle').addEventListener('click', () => {
        $('left-panel').classList.toggle('collapsed');
    });
    for (const row of document.querySelectorAll('.toggle-row[data-layer]')) {
        const layer = row.dataset.layer;
        const cb = row.querySelector('input');
        cb.addEventListener('change', () => setLayerVisibility(layer, cb.checked));
        row.querySelector('.filter-label').addEventListener('click', () => {
            cb.checked = !cb.checked;
            cb.dispatchEvent(new Event('change'));
        });
    }

    let searchTimeout;
    $('operator-search').addEventListener('input', e => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            operatorFilter = e.target.value.trim();
            refreshFlares();
            loadPermits();
            loadWells();
        }, 300);
    });

    $('detail-close').addEventListener('click', closeDetail);
    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDetail(); });

    // Overlap navigation
    $('overlap-prev').addEventListener('click', () => {
        if (overlappingFeatures.length < 2) return;
        overlapIndex = (overlapIndex - 1 + overlappingFeatures.length) % overlappingFeatures.length;
        showFeatureDetail(overlappingFeatures[overlapIndex]);
    });
    $('overlap-next').addEventListener('click', () => {
        if (overlappingFeatures.length < 2) return;
        overlapIndex = (overlapIndex + 1) % overlappingFeatures.length;
        showFeatureDetail(overlappingFeatures[overlapIndex]);
    });

    // Click-through: query all visible layers at click point
    map.on('click', e => {
        const tolerance = 10;
        const bbox = [
            [e.point.x - tolerance, e.point.y - tolerance],
            [e.point.x + tolerance, e.point.y + tolerance]
        ];
        const activeLayers = ALL_CLICK_LAYERS.filter(l =>
            map.getLayer(l) && map.getLayoutProperty(l, 'visibility') !== 'none'
        );
        const raw = map.queryRenderedFeatures(bbox, { layers: activeLayers });

        if (raw.length === 0) {
            closeDetail();
            return;
        }

        // Deduplicate: pixel squares share flare_id with point layer — keep point, drop pixel dupes
        const PIXEL_LAYERS = new Set(['flare-pixels-fill', 'flare-pixels-layer']);
        const seen = new Set();
        const features = [];
        // Prefer point features: process non-pixel layers first
        const sorted = [...raw].sort((a, b) => (PIXEL_LAYERS.has(a.layer.id) ? 1 : 0) - (PIXEL_LAYERS.has(b.layer.id) ? 1 : 0));
        for (const f of sorted) {
            const key = featureKey(f);
            if (seen.has(key)) continue;
            seen.add(key);
            features.push(f);
        }

        if (features.length === 0) {
            closeDetail();
            return;
        }

        // Sort by distance to click (use properties for polygons)
        const featureCenter = f => {
            if (f.geometry.type === 'Point') return f.geometry.coordinates;
            return [Number(f.properties.lon), Number(f.properties.lat)];
        };
        features.sort((a, b) => {
            const [aLng, aLat] = featureCenter(a);
            const [bLng, bLat] = featureCenter(b);
            return Math.hypot(aLng - e.lngLat.lng, aLat - e.lngLat.lat)
                 - Math.hypot(bLng - e.lngLat.lng, bLat - e.lngLat.lat);
        });

        overlappingFeatures = features;
        overlapIndex = 0;
        showFeatureDetail(features[0]);

        syncDrawer(features[0]);
    });

    // Cursor changes for interactive layers
    for (const id of ALL_CLICK_LAYERS) {
        map.on('mouseenter', id, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', id, () => { map.getCanvas().style.cursor = ''; });
    }

    map.on('move', updateMapCentre);
    map.on('moveend', () => {
        updateStats();
        updateWellsToggle();
        loadWells();
    });
}

// Hash param helpers — coexist with MapLibre's #map=zoom/lat/lon
function updateHash(params) {
    const hash = location.hash.replace(/^#/, '');
    const mapPart = hash.split('&').find(p => p.startsWith('map='));
    const parts = mapPart ? [mapPart] : [];
    for (const [k, v] of Object.entries(params)) {
        if (v != null) parts.push(`${k}=${encodeURIComponent(v)}`);
    }
    history.replaceState(null, '', location.pathname + location.search + '#' + parts.join('&'));
}

function handleDeepLink() {
    const hash = location.hash.replace(/^#/, '');
    const params = {};
    for (const part of hash.split('&')) {
        const eq = part.indexOf('=');
        if (eq > 0) params[part.slice(0, eq)] = decodeURIComponent(part.slice(eq + 1));
    }

    if (params.s2) {
        const cluster = getCluster(params.s2);
        if (cluster) {
            updateHash({ s2: cluster.id });
            map.flyTo({ center: [cluster.lon, cluster.lat], zoom: 16 });
            showS2ClusterDetail(cluster);
        }
        return;
    }

    const flareId = params.vnf;
    if (!flareId) return;

    const feature = flareFeatures.find(f => String(f.properties.flare_id) === flareId);
    if (!feature) return;

    const [lon, lat] = feature.geometry.coordinates;
    updateHash({ vnf: flareId, mode: params.mode || undefined });
    map.flyTo({ center: [lon, lat], zoom: 14 });

    if (params.mode === 's2') {
        showEnhanceDetail(feature);
    } else {
        feature.layer = { id: 'flares-layer' };
        showFeatureDetail(feature);
    }
}

function removeS2Badge() {
    const badge = $('s2-badge') || $('detail-badge');
    if (badge) { badge.id = 'detail-badge'; badge.classList.add('hidden'); }
}

// ---------------------------------------------------------------------------
// Selection visual state — dims map, highlights selected + associated features
// ---------------------------------------------------------------------------

// Default opacity values for each layer (must match addLayers paint values).
// Used by activateSelection/deactivateSelection to dim/restore layers.
const LAYER_DEFAULTS = {
    'flares-layer': {
        'circle-stroke-opacity': ['interpolate', ['linear'], ['zoom'], 13, 1, 15, 0],
        'circle-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0.25, 15, 0],
    },
    'flare-pixels-layer': {
        'line-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 1],
    },
    'flare-pixels-label': {
        'text-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 1],
    },
    'permits-layer': { 'circle-stroke-opacity': 1, 'circle-opacity': 0.25 },
    'plumes-layer': { 'circle-stroke-opacity': 1, 'circle-opacity': 0.25 },
    'wells-layer': { 'icon-opacity': 0.85 },
    'infra-layer': { 'icon-opacity': 0.85 },
    's2-points': { 'circle-stroke-opacity': 1, 'circle-opacity': 0.25 },
};

// How much to scale each property type when dimmed (unselected).
// Pixel boxes/labels use 0 — only the selected flare's detection area is shown.
const DIM_RATIOS = {
    'circle-stroke-opacity': 0.15, 'circle-opacity': 0.2,
    'icon-opacity': 0.18,
    'line-opacity': 0, 'text-opacity': 0,
};

// Build a paint expression: matched features stay at base, rest are dimmed.
// For zoom-based interpolations, data-driven case is embedded inside each stop
// (MapLibre requires data expressions inside zoom interpolations, not outside).
function dimPaint(base, ratio, match) {
    if (typeof base === 'number') {
        if (!match) return base * ratio;
        return ['case', match, base, base * ratio];
    }
    // Zoom interpolation: ['interpolate', ['linear'], ['zoom'], z0, v0, z1, v1, ...]
    const result = [base[0], base[1], base[2]];
    for (let i = 3; i < base.length; i += 2) {
        const z = base[i], v = base[i + 1];
        result.push(z);
        result.push(!match ? v * ratio : ['case', match, v, v * ratio]);
    }
    return result;
}

function activateSelection({ flareId, permitProps, plumeId, wellApi, infraSerial } = {}) {
    $('map-dim-overlay').classList.add('active');

    const flareMatch = flareId != null ? ['==', ['get', 'flare_id'], flareId] : null;
    const matches = {
        'flares-layer': flareMatch,
        'flare-pixels-layer': flareMatch,
        'flare-pixels-label': flareMatch,
        'permits-layer': permitProps ? ['all',
            ['==', ['get', 'latitude'], Number(permitProps.latitude)],
            ['==', ['get', 'longitude'], Number(permitProps.longitude)],
            ['==', ['get', 'name'], permitProps.name]] : null,
        'plumes-layer': plumeId != null ? ['==', ['get', 'plume_id'], plumeId] : null,
        'wells-layer': wellApi != null ? ['==', ['get', 'api'], wellApi] : null,
        'infra-layer': infraSerial != null ? ['==', ['get', 'serial_number'], infraSerial] : null,
        's2-points': flareMatch,
    };

    for (const [layerId, defaults] of Object.entries(LAYER_DEFAULTS)) {
        if (!map.getLayer(layerId)) continue;
        const match = matches[layerId];
        for (const [prop, base] of Object.entries(defaults)) {
            map.setPaintProperty(layerId, prop, dimPaint(base, DIM_RATIOS[prop], match));
        }
    }
}

function deactivateSelection() {
    $('map-dim-overlay').classList.remove('active');
    for (const [layerId, defaults] of Object.entries(LAYER_DEFAULTS)) {
        if (!map.getLayer(layerId)) continue;
        for (const [prop, base] of Object.entries(defaults)) {
            map.setPaintProperty(layerId, prop, base);
        }
    }
}

function closeDetail() {
    removeS2Badge();
    closeS2Pixels();
    updateHash({});
    $('detail-panel').classList.add('hidden');
    deactivateSelection();
    overlappingFeatures = [];
    overlapIndex = 0;
    drawer.highlight(null, null);
}

// ---------------------------------------------------------------------------
// S2 pixel overlay — renders B12 COG pixels on the map (magma colormap)
// ---------------------------------------------------------------------------

// Magma-ish colormap: black → purple → red → orange → yellow
const MAGMA_STOPS = [
    [0.0, 0, 0, 4],
    [0.1, 15, 4, 56],
    [0.2, 58, 12, 108],
    [0.3, 101, 21, 132],
    [0.4, 143, 36, 130],
    [0.5, 186, 55, 112],
    [0.6, 221, 82, 83],
    [0.7, 245, 119, 56],
    [0.8, 254, 164, 37],
    [0.9, 253, 210, 59],
    [1.0, 252, 255, 164],
];

function magmaColor(t) {
    t = Math.max(0, Math.min(1, t));
    for (let i = 1; i < MAGMA_STOPS.length; i++) {
        if (t <= MAGMA_STOPS[i][0]) {
            const [t0, r0, g0, b0] = MAGMA_STOPS[i - 1];
            const [t1, r1, g1, b1] = MAGMA_STOPS[i];
            const f = (t - t0) / (t1 - t0);
            return [
                Math.round(r0 + f * (r1 - r0)),
                Math.round(g0 + f * (g1 - g0)),
                Math.round(b0 + f * (b1 - b0)),
            ];
        }
    }
    return [252, 255, 164];
}

function utmBoundsToWgs84(utmBounds, epsg) {
    const { zone, isNorth } = utmParams(epsg);
    const sw = utmToWgs84(utmBounds[0], utmBounds[1], zone, isNorth);
    const ne = utmToWgs84(utmBounds[2], utmBounds[3], zone, isNorth);
    return [sw[0], sw[1], ne[0], ne[1]]; // [west, south, east, north]
}

function closeS2Pixels() {
    if (map.getLayer('cog-layer')) map.removeLayer('cog-layer');
    if (map.getSource('cog-source')) map.removeSource('cog-source');
}

async function loadS2Pixels(det, clusterLon, clusterLat) {
    closeS2Pixels();

    const buffer = 250; // meters around detection
    const epsg = det.epsg;
    if (!epsg || !det.cog_b12) return;

    const { zone, isNorth } = utmParams(epsg);
    const [utmX, utmY] = wgs84ToUtm(clusterLon, clusterLat, zone, isNorth);
    const utmBounds = [utmX - buffer, utmY - buffer, utmX + buffer, utmY + buffer];

    // Mark active event
    document.querySelectorAll('.s2-event-item').forEach(el => el.classList.remove('active'));
    const activeEl = document.querySelector(`.s2-event-item[data-date="${det.date}"]`);
    activeEl?.classList.add('active', 'loading');

    try {
        const b12Meta = await openCOG(det.cog_b12);
        const { image, bbox: imgBbox, width, height, resX, resY } = b12Meta;
        const [imgMinX, imgMinY, imgMaxX, imgMaxY] = imgBbox;

        const x0 = Math.max(0, Math.floor((utmBounds[0] - imgMinX) / resX));
        const y0 = Math.max(0, Math.floor((imgMaxY - utmBounds[3]) / resY));
        const x1 = Math.min(width, Math.ceil((utmBounds[2] - imgMinX) / resX));
        const y1 = Math.min(height, Math.ceil((imgMaxY - utmBounds[1]) / resY));

        const windowWidth = x1 - x0, windowHeight = y1 - y0;
        if (windowWidth <= 0 || windowHeight <= 0) throw new Error('Outside image bounds');

        const actualUtmBounds = [imgMinX + x0 * resX, imgMaxY - y1 * resY, imgMinX + x1 * resX, imgMaxY - y0 * resY];
        const bounds = utmBoundsToWgs84(actualUtmBounds, epsg);
        if (!bounds) throw new Error('Could not convert bounds');

        const rasters = await image.readRasters({
            window: [x0, y0, x1, y1],
            width: Math.min(windowWidth, 256),
            height: Math.min(windowHeight, 256)
        });

        const data = rasters[0];
        const w = rasters.width, h = rasters.height;
        const canvas = document.createElement('canvas');
        canvas.width = w;
        canvas.height = h;
        const ctx = canvas.getContext('2d');
        const imgData = ctx.createImageData(w, h);

        // B12 reflectance conversion: raw DN → reflectance, then hot-pixel filter
        const B12_SCALE = 0.0001, B12_OFFSET = -0.1, B12_THRESHOLD = 0.6, B12_CEILING = 1.5;
        for (let i = 0; i < data.length; i++) {
            const v = data[i] * B12_SCALE + B12_OFFSET;
            if (v <= B12_THRESHOLD) {
                imgData.data[i * 4 + 3] = 0;
            } else {
                const t = Math.min(1, (v - B12_THRESHOLD) / (B12_CEILING - B12_THRESHOLD));
                const [r, g, b] = magmaColor(t);
                imgData.data[i * 4] = r;
                imgData.data[i * 4 + 1] = g;
                imgData.data[i * 4 + 2] = b;
                imgData.data[i * 4 + 3] = 255;
            }
        }
        ctx.putImageData(imgData, 0, 0);

        closeS2Pixels(); // remove any added while fetching

        const coords = [
            [bounds[0], bounds[3]], [bounds[2], bounds[3]],
            [bounds[2], bounds[1]], [bounds[0], bounds[1]]
        ];

        map.addSource('cog-source', {
            type: 'image',
            url: canvas.toDataURL(),
            coordinates: coords
        });
        map.addLayer({
            id: 'cog-layer', type: 'raster', source: 'cog-source',
            paint: { 'raster-opacity': 1, 'raster-resampling': 'nearest' }
        }, 's2-points');

        activeEl?.classList.remove('loading');
    } catch (err) {
        console.error('Failed to load S2 COG:', err);
        activeEl?.classList.remove('loading');
    }
}

function showFeatureDetail(feature) {
    removeS2Badge();
    const layer = feature.layer.id;
    const p = feature.properties;

    // Build selection opts: each layer type identifies its selected feature differently
    const selOpts = {};
    if (p.flare_id != null) selOpts.flareId = p.flare_id;
    if (layer.startsWith('permits-')) selOpts.permitProps = { latitude: p.latitude, longitude: p.longitude, name: p.name };
    if (layer.startsWith('plumes-')) selOpts.plumeId = p.plume_id;
    if (layer.startsWith('wells-')) selOpts.wellApi = p.api;
    if (layer.startsWith('infra-')) selOpts.infraSerial = p.serial_number;
    activateSelection(selOpts);

    if (layer === 's2-points') {
        // Don't cancel enhance — let it run in background
        const cluster = getCluster(feature.properties.id);
        if (cluster) showS2ClusterDetail(cluster);
    } else {
        if (layer.startsWith('flare')) showFlareDetail(feature);
        else {
            updateHash({});
            if (layer.startsWith('permits-')) showPermitDetail(feature);
            else if (layer.startsWith('plumes-')) showPlumeDetail(feature);
            else if (layer.startsWith('wells-')) showWellDetail(feature);
            else if (layer.startsWith('infra-')) showInfraDetail(feature);
        }
    }

    updateOverlapNav();
}

function updateOverlapNav() {
    const layer0 = overlappingFeatures[0]?.layer?.id || '';
    const group = layer0.startsWith('permits-') ? 'permits-' : layer0.startsWith('plumes-') ? 'plumes-' : null;
    const nav = $('overlap-nav');
    if (group) {
        const grouped = overlappingFeatures.filter(f => f.layer.id.startsWith(group));
        if (grouped.length > 1) {
            overlappingFeatures = grouped;
            nav.classList.remove('hidden');
            $('overlap-count').textContent = `${overlapIndex + 1} / ${overlappingFeatures.length}`;
            return;
        }
    }
    nav.classList.add('hidden');
}

// ---------------------------------------------------------------------------
// Card building blocks — return HTML strings, compose via arrays in openDetail
// ---------------------------------------------------------------------------

function field(label, value) {
    return `<div class="detail-field"><span class="detail-field-label">${label}</span><span class="detail-field-value">${value}</span></div>`;
}

const card = {
    // Stats grid: [{value, unit, id?}, ...]
    stats: (items) => `<div class="stats-grid">${items.map(
        i => `<div class="stat"><div class="stat-big"${i.id ? ` id="${i.id}"` : ''}>${i.value}</div><div class="stat-unit">${i.unit}</div></div>`
    ).join('')}</div>`,

    // Key-value field row: [label, value] pairs. Falsy pairs and null values are filtered.
    fields: (...pairs) => {
        const html = pairs.filter(p => p && p[1] != null).map(([l, v]) => field(l, v)).join('');
        return html ? `<div class="detail-row">${html}</div>` : '';
    },

    // Section header
    header: (text) => `<div class="section-header">${text}</div>`,

    // Async placeholder — filled later via $('id').innerHTML = ...
    section: (id) => `<div id="${id}"></div>`,
};

// ---------------------------------------------------------------------------
// Shared data helpers for detail cards
// ---------------------------------------------------------------------------

function mergeRanges(filings) {
    const ranges = filings
        .filter(f => f.effective_dt)
        .map(f => [f.effective_dt, f.expiration_dt || '9999-12-31'])
        .sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
    if (ranges.length === 0) return [];
    const merged = [ranges[0].slice()];
    for (let i = 1; i < ranges.length; i++) {
        const prev = merged[merged.length - 1];
        if (ranges[i][0] <= prev[1]) {
            if (ranges[i][1] > prev[1]) prev[1] = ranges[i][1];
        } else {
            merged.push(ranges[i].slice());
        }
    }
    return merged;
}

function computeCoverage(filings, firstDetected, lastDetected) {
    if (!firstDetected || !lastDetected || filings.length === 0) return null;
    const merged = mergeRanges(filings);
    if (merged.length === 0) return { status: 'uncovered', gaps: null };
    const gaps = [];
    let cursor = firstDetected;
    for (const [start, end] of merged) {
        if (start > cursor && start <= lastDetected) gaps.push([cursor, start]);
        if (end > cursor) cursor = end;
    }
    if (cursor < lastDetected) gaps.push([cursor, lastDetected]);
    const firstCovered = merged.some(([s, e]) => s <= firstDetected && e >= firstDetected);
    const lastCovered = merged.some(([s, e]) => s <= lastDetected && e >= lastDetected);
    if (firstCovered && lastCovered && gaps.length === 0) return { status: 'covered', gaps: null };
    return { status: gaps.length > 0 ? 'gap' : 'partial', gaps };
}

function coverageLabel(coverage) {
    if (!coverage) return null;
    const labels = {
        covered: '<span class="permit-covered">Covered</span>',
        gap: '<span class="permit-uncovered">Gap in coverage</span>',
        partial: '<span class="permit-uncovered">Partial</span>',
    };
    return labels[coverage.status] || '<span class="permit-uncovered">Uncovered</span>';
}

// Shared attribution block — facility match takes precedence over operator
function attributionHtml(facs, op, filings, firstDate, lastDate) {
    if (facs.length > 0) {
        const f = facs[0];
        return [
            card.fields(
                ['Facility', f.facility_name],
                f.plant_type && ['Type', f.plant_type],
                ['Distance', (f.distance_km * 1000).toFixed(0) + 'm'],
            ),
            card.fields(
                ['First detected', formatDate(firstDate)],
                ['Last detected', formatDate(lastDate)],
            ),
        ].join('');
    }
    if (!op) return '';
    const confidence = op.confidence ? op.confidence.charAt(0).toUpperCase() + op.confidence.slice(1) : null;
    const distKm = op.nearest_permit_km != null ? Number(op.nearest_permit_km) : null;
    const coverage = computeCoverage(filings, firstDate, lastDate);
    const gapHtml = coverage?.gaps?.length
        ? coverage.gaps.map(([a, b]) => `${formatDate(a)} – ${formatDate(b)}`).join('<br>')
        : null;
    return [
        card.fields(
            ['Operator', op.operator_name || 'N/A'],
            confidence != null && ['Confidence', confidence],
            ['Nearest permit', op.permit_name || 'None'],
            ['Distance', distKm != null ? distKm.toFixed(2) + ' km' : 'N/A'],
            coverageLabel(coverage) && ['Coverage', coverageLabel(coverage)],
            gapHtml && ['Gaps', gapHtml],
        ),
        card.fields(
            ['First detected', formatDate(firstDate)],
            ['Last detected', formatDate(lastDate)],
        ),
    ].join('');
}

// ---------------------------------------------------------------------------
// Detail cards
// ---------------------------------------------------------------------------

function showFlareDetail(feature) {
    const p = feature.properties;
    updateHash({ vnf: p.flare_id });

    openDetail(`Flare ${p.flare_id}`, p.lat, p.lon, [
        card.stats([
            { value: num(p.total_rh_mw), unit: 'total MW' },
            { value: num(p.detection_days), unit: 'detection days' },
        ]),
        card.section('vnf-operator-section'),
        card.section('vnf-lease-section'),
    ]);

    Promise.all([
        db.queryNearbyFacilities(Number(p.lat), Number(p.lon)),
        db.queryOperator(p.flare_id, Number(p.lat), Number(p.lon)),
        db.queryPermitFilings(Number(p.lat), Number(p.lon)),
    ]).then(([facs, op, filings]) => {
        const el = $('vnf-operator-section');
        if (el) el.innerHTML = attributionHtml(facs, op, filings, p.first_detected, p.last_detected);
    }).catch(() => {});

    db.queryLeases(p.flare_id).then(leases => {
        const el = $('vnf-lease-section');
        if (!el || leases.length === 0) return;
        el.innerHTML = leases.map(l => {
            const name = l.lease_name || `${l.lease_district}-${l.lease_number}`;
            const flared = Number(l.reported_flared_mcf) || 0;
            return card.fields(
                ['Lease', name],
                l.lease_operator && ['Operator', l.lease_operator],
                ['Wells', num(l.well_count)],
                flared > 0 && ['Reported flared', flared.toLocaleString() + ' MCF'],
            );
        }).join('');
    }).catch(() => {});

    db.queryDetections(p.flare_id).then(detections => {
        renderSparkline(detections);
        const btn = document.createElement('button');
        btn.className = 'btn-action enhance-btn';
        btn.textContent = 'Enhance with Sentinel-2';
        btn.addEventListener('click', () => showEnhanceDetail(feature));
        $('intensity-chart').appendChild(btn);
    }).catch(() => {});
}

function showPermitDetail(feature) {
    const p = feature.properties;
    const rate = Number(p.max_release_rate_mcf_day);
    openDetail(p.name || 'Permit location', p.latitude, p.longitude, [
        card.stats([
            { value: rate > 0 ? rate.toLocaleString() : 'N/A', unit: 'max Mcf/day' },
            { value: Number(p.n_filings), unit: 'filings', id: 'permit-filings-count' },
        ]),
        card.fields(
            ['Operator', p.operator_name || 'N/A'],
            ['County', p.county || 'N/A'],
            ['District', p.district || 'N/A'],
            ['Release type', p.release_type || 'N/A'],
        ),
        card.section('permit-filings-section'),
    ]);

    db.queryPermitFilings(Number(p.latitude), Number(p.longitude), { radiusKm: 0.01, name: p.name, operator: p.operator_name }).then(filings => {
        const el = $('permit-filings-section');
        if (!el || filings.length === 0) return;
        const countEl = $('permit-filings-count');
        if (countEl) countEl.textContent = filings.length;
        el.innerHTML = `<div class="filings-list">${filings.map(f =>
            card.fields(
                ['Effective', formatDate(f.effective_dt)],
                ['Expiration', formatDate(f.expiration_dt)],
                f.status && ['Status', f.status],
                f.exception_reasons && ['Reasons', f.exception_reasons],
            )
        ).join('')}</div>`;
    }).catch(() => {});
}

function plumeUrl(source, id) {
    if (source === 'cm') return `https://data.carbonmapper.org/?plume_id=${encodeURIComponent(id)}`;
    if (source === 'imeo') return `https://methanedata.unep.org`;
    return null;
}

function showPlumeDetail(feature) {
    const p = feature.properties;
    const url = plumeUrl(p.source, p.plume_id);
    openDetail(p.plume_id, p.latitude, p.longitude, [
        card.stats([
            { value: Number(p.emission_rate).toLocaleString(), unit: 'kg/hr' },
            { value: `&plusmn;${Number(p.emission_uncertainty || 0).toLocaleString()}`, unit: 'uncertainty' },
        ]),
        card.fields(
            ['Source', p.source],
            ['Satellite', p.satellite || 'N/A'],
            ['Date', formatDate(p.date)],
            ['Sector', p.sector || 'N/A'],
        ),
    ]);
    if (url) $('detail-title').innerHTML = `<a href="${url}" target="_blank" rel="noopener" style="color: inherit; text-decoration: none;">${p.plume_id}</a>`;
}

function showWellDetail(feature) {
    const p = feature.properties;
    const flared = Number(p.flared_mcf) || 0;
    const produced = Number(p.produced_mcf) || 0;
    const intensity = p.flaring_intensity_pct != null ? Number(p.flaring_intensity_pct).toFixed(1) + '%' : 'N/A';
    const leaseId = p.lease_district && p.lease_number ? `${p.lease_district}-${p.lease_number}` : null;
    const leaseName = p.lease_name || leaseId;
    const hasLease = leaseId != null;

    openDetail(`Well ${p.api}`, p.latitude, p.longitude, [
        card.fields(
            ['Operator', p.operator_name || 'N/A'],
            ['Type', p.oil_gas_code === 'O' ? 'Oil' : p.oil_gas_code === 'G' ? 'Gas' : p.oil_gas_code || 'N/A'],
            ['District', p.lease_district || 'N/A'],
            ['Well #', p.well_number || 'N/A'],
        ),
        hasLease && [
            card.header(`Lease ${leaseName}`),
            card.fields(
                ['Gas flared (MCF)', flared > 0 ? flared.toLocaleString() : 'None reported'],
                ['Gas produced (MCF)', produced > 0 ? produced.toLocaleString() : 'None reported'],
                ['Flaring intensity', intensity],
            ),
            `<div id="well-lease-chart" class="intensity-chart"></div>`,
            card.header('Gatherers & Purchasers'),
            `<div id="well-gatherers" class="gatherer-list loading-placeholder">Loading\u2026</div>`,
        ],
    ]);

    if (hasLease) {
        db.queryLeaseMonthly(p.lease_district, p.lease_number).then(monthly => {
            const el = document.getElementById('well-lease-chart');
            if (!el || monthly.length === 0) return;
            renderLeaseChartIn(el, monthly);
        }).catch(() => {});

        db.queryGatherers(p.lease_district, p.lease_number).then(rows => {
            const el = document.getElementById('well-gatherers');
            if (!el || rows.length === 0) {
                if (el) el.innerHTML = '<span class="dim">No records</span>';
                return;
            }
            const current = rows.filter(r => r.is_current === '1');
            const currentKeys = new Set(current.map(r => r.type + '|' + r.gpn_name));
            const historical = rows.filter(r => r.is_current !== '1' && !currentKeys.has(r.type + '|' + r.gpn_name));
            const fmtDate = d => d ? d.slice(0, 7) : '';
            const renderRows = (list) => list.map(r => {
                const parts = [r.type];
                if (r.percentage) parts.push(r.percentage + '%');
                if (r.first_date) {
                    const from = fmtDate(r.first_date);
                    const to = fmtDate(r.last_date);
                    parts.push(from === to || !to ? from : `${from} – ${to}`);
                }
                return `<div class="gatherer-row">
                    <span class="gatherer-name">${r.gpn_name}</span>
                    <span class="gatherer-meta">${parts.join(' · ')}</span>
                </div>`;
            }).join('');
            el.innerHTML =
                (current.length ? renderRows(current) : '<div class="gatherer-row dim">None active</div>') +
                (historical.length ? `<details class="gatherer-history"><summary>${historical.length} historical</summary>${renderRows(historical)}</details>` : '');
        }).catch(() => {});
    }
}

function showInfraDetail(feature) {
    const p = feature.properties;
    openDetail(p.facility_name || 'Facility', p.latitude, p.longitude, [
        card.fields(
            ['Serial #', p.serial_number],
            p.plant_type && ['Type', p.plant_type],
        ),
    ]);
}

function showEnhanceDetail(feature) {
    const p = feature.properties;
    updateHash({ vnf: p.flare_id, mode: 's2' });
    activateSelection({ flareId: p.flare_id });

    openDetail(`Sentinel-2 · Flare ${p.flare_id}`, p.lat, p.lon, [
        `<div id="s2-stop-section">
            <button class="btn-action stop-analysis-btn" id="s2-stop-btn">Stop Analysis</button>
        </div>`,
        card.section('s2-cluster-list'),
    ]);
    $('s2-stop-btn').addEventListener('click', () => {
        cancelEnhance(map);
        $('s2-stop-section').innerHTML = '';
    });
    const badge = $('detail-badge');
    badge.className = 'status-badge s2';
    badge.id = 's2-badge';
    badge.textContent = 'Enhancing';
    badge.classList.remove('hidden');
    $('overlap-nav').classList.add('hidden');

    setUpdateCallback((s) => {
        const s2b = $('s2-badge');
        if (!s2b) return;

        if (s.enhancing) {
            s2b.textContent = s.progress?.total
                ? `${s.progress.done} / ${s.progress.total}${s.progress.skipped ? ` (${s.progress.skipped} cached)` : ''}`
                : 'Searching...';
        } else if (s.error) {
            s2b.className = 'status-badge excluded';
            s2b.textContent = 'Failed';
        }

        if (!s.enhancing) $('s2-stop-section')?.replaceChildren();

        if (s.clusters?.length) {
            if (!s.enhancing) {
                s2b.textContent = `${s.clusters.length} source${s.clusters.length !== 1 ? 's' : ''}`;
            }
            const list = $('s2-cluster-list');
            if (list) {
                list.className = 'enhance-results';
                list.innerHTML = s.clusters.map(c =>
                    `<div class="enhance-cluster" data-id="${c.id}">
                        <span class="cluster-dot" style="background:${b12Color(c.max_b12)}"></span>
                        B12 ${c.max_b12.toFixed(2)} · ${c.detection_count} det · ${c.first_date}${c.first_date !== c.last_date ? ` – ${c.last_date}` : ''}
                    </div>`
                ).join('');
                list.querySelectorAll('.enhance-cluster').forEach(el => {
                    el.addEventListener('click', () => {
                        const cluster = getCluster(el.dataset.id);
                        if (cluster) {
                            map.flyTo({ center: [cluster.lon, cluster.lat], zoom: 17 });
                            showS2ClusterDetail(cluster);
                        }
                    });
                });
            }
        }
    });

    enhance(feature, map);
}

function showS2ClusterDetail(cluster) {
    closeS2Pixels();
    updateHash({ s2: cluster.id });
    activateSelection({ flareId: cluster.flare_id });

    const dets = (cluster.detections || []).slice().sort((a, b) => b.date.localeCompare(a.date));
    const eventListHtml = dets.map(d => `<div class="s2-event-item" data-date="${d.date}">
            <span class="s2-event-dot" style="background:${b12Color(d.max_b12)}"></span>
            <span class="s2-event-date">${formatDate(d.date)}</span>
            <span class="s2-event-b12">B12 ${d.max_b12.toFixed(2)}</span>
        </div>`).join('');

    openDetail(`S2 Source ${cluster.id}`, cluster.lat, cluster.lon, [
        card.stats([
            { value: cluster.max_b12.toFixed(2), unit: 'peak B12' },
            { value: cluster.avg_b12.toFixed(2), unit: 'mean B12' },
        ]),
        card.fields(
            ['First detected', formatDate(cluster.first_date)],
            ['Last detected', formatDate(cluster.last_date)],
        ),
        card.section('s2-permit-section'),
        `<div id="s2-event-list" class="s2-event-list">${eventListHtml}</div>`,
    ]);
    const badge = $('detail-badge');
    badge.className = 'status-badge s2';
    badge.textContent = `${cluster.detection_count} det`;
    badge.classList.remove('hidden');
    $('overlap-nav').classList.add('hidden');

    document.querySelectorAll('.s2-event-item').forEach(el => {
        el.addEventListener('click', async () => {
            const date = el.dataset.date;
            const dLat = 400 * LAT_PER_M;
            const dLon = 400 * LON_PER_M(cluster.lat);
            const bbox = [cluster.lon - dLon, cluster.lat - dLat, cluster.lon + dLon, cluster.lat + dLat];

            el.classList.add('loading');
            try {
                let item = null;
                for await (const it of searchSTAC(bbox, date, date)) { item = it; break; }
                if (!item?.bands?.b12 || !item.epsg) { el.classList.remove('loading'); return; }
                await loadS2Pixels({ date, cog_b12: item.bands.b12, epsg: item.epsg }, cluster.lon, cluster.lat);
            } catch (err) {
                console.error('STAC lookup failed:', err);
                el.classList.remove('loading');
            }
        });
    });

    if (cluster.detections?.length) renderS2Chart(cluster.detections);

    const dates = (cluster.detections || []).map(d => d.date).filter(Boolean).sort();
    const firstDate = dates[0] || null, lastDate = dates[dates.length - 1] || null;
    Promise.all([
        db.queryNearbyFacilities(cluster.lat, cluster.lon),
        db.queryOperatorByLocation(cluster.lat, cluster.lon),
        db.queryPermitFilings(cluster.lat, cluster.lon),
    ]).then(([facs, op, filings]) => {
        const el = $('s2-permit-section');
        if (el) el.innerHTML = attributionHtml(facs, op, filings, firstDate, lastDate);
    }).catch(() => {});
}

// Shared timeline chart builder
function renderTimeline(detections, { valueKey, colorFn, scaleFn, gridStyle = 'months' } = {}) {
    const container = $('intensity-chart');
    if (!detections?.length) { container.innerHTML = ''; return; }

    const M = { top: 8, right: 8, bottom: 16, left: 8 };
    const width = container.clientWidth || 400, height = 100;
    const innerW = width - M.left - M.right;
    const innerH = height - M.top - M.bottom;

    const dates = detections.map(d => new Date(d.date).getTime());
    const minDate = Math.min(...dates), maxDate = Math.max(...dates);
    const dateRange = maxDate - minDate || 1;

    let svg = `<svg viewBox="0 0 ${width} ${height}">`;
    svg += `<line x1="${M.left}" y1="${height - M.bottom}" x2="${width - M.right}" y2="${height - M.bottom}" stroke="rgba(255,255,255,0.15)" stroke-width="1"/>`;

    const xOf = t => M.left + ((t - minDate) / dateRange) * innerW;

    if (gridStyle === 'months') {
        // Month gridlines with labels
        const charW = 5, minGap = 30;
        const startD = new Date(minDate), endD = new Date(maxDate);
        const startX = M.left, endX = width - M.right;
        const startLabel = `${MONTHS[startD.getMonth()]} ${startD.getFullYear()}`;
        svg += `<text x="${startX}" y="${height - 2}" fill="rgba(255,255,255,0.35)" font-size="9" text-anchor="start">${startLabel}</text>`;
        let lastLabelX = startX + startLabel.length * charW;
        const firstMonth = new Date(startD);
        firstMonth.setDate(1);
        firstMonth.setMonth(firstMonth.getMonth() + 1);
        for (let d = new Date(firstMonth); d <= endD; d.setMonth(d.getMonth() + 1)) {
            const x = xOf(d.getTime());
            const isJan = d.getMonth() === 0;
            svg += `<line x1="${x}" y1="${M.top}" x2="${x}" y2="${height - M.bottom}" stroke="rgba(255,255,255,${isJan ? 0.15 : 0.06})" stroke-width="1"/>`;
            const label = isJan ? `${MONTHS[0]} ${d.getFullYear()}` : MONTHS[d.getMonth()];
            const labelW = label.length * charW;
            if (x - labelW / 2 > lastLabelX + minGap && x + labelW / 2 < endX - minGap) {
                svg += `<text x="${x}" y="${height - 2}" fill="rgba(255,255,255,${isJan ? 0.4 : 0.25})" font-size="9" text-anchor="middle">${label}</text>`;
                lastLabelX = x + labelW / 2;
            }
        }
        const endLabel = `${MONTHS[endD.getMonth()]} ${endD.getFullYear()}`;
        if (endLabel !== startLabel && endX - endLabel.length * charW > lastLabelX + minGap)
            svg += `<text x="${endX}" y="${height - 2}" fill="rgba(255,255,255,0.35)" font-size="9" text-anchor="end">${endLabel}</text>`;
    } else {
        // Year-only gridlines
        const firstYear = new Date(minDate).getFullYear(), lastYear = new Date(maxDate).getFullYear();
        for (let y = firstYear + 1; y <= lastYear; y++) {
            const x = xOf(new Date(y, 0, 1).getTime());
            svg += `<line x1="${x}" y1="${M.top}" x2="${x}" y2="${height - M.bottom}" stroke="rgba(255,255,255,0.08)" stroke-width="1"/>`;
            svg += `<text x="${x}" y="${height - 2}" fill="rgba(255,255,255,0.3)" font-size="10" text-anchor="middle">${y}</text>`;
        }
    }

    detections.forEach(det => {
        const x = xOf(new Date(det.date).getTime());
        const t = scaleFn(det);
        const y = M.top + innerH - t * innerH;
        svg += `<circle class="chart-dot" cx="${x}" cy="${y}" r="2" fill="${colorFn(det[valueKey])}" opacity="0.8"/>`;
    });

    svg += '</svg>';
    container.innerHTML = svg;
}

function renderS2Chart(detections) {
    const maxVal = Math.max(1.5, ...detections.map(d => d.max_b12));
    renderTimeline(detections, {
        valueKey: 'max_b12', colorFn: b12Color, gridStyle: 'months',
        scaleFn: d => Math.min(1, d.max_b12 / maxVal),
    });
}

function renderSparkline(detections) {
    const vals = detections.map(d => d.rh_mw).filter(v => v > 0);
    const lo = 0.1, hi = Math.max(10, ...vals);
    const logLo = Math.log(lo), logRange = Math.log(hi) - logLo;
    renderTimeline(detections, {
        valueKey: 'rh_mw', colorFn: mwColor, gridStyle: 'years',
        scaleFn: d => d.rh_mw > 0 ? Math.max(0, Math.min(1, (Math.log(Math.max(lo, d.rh_mw)) - logLo) / logRange)) : 0,
    });
}

function renderLeaseChartIn(container, monthly) {
    if (!monthly?.length) { container.innerHTML = ''; return; }

    const M = { top: 4, right: 8, bottom: 14, left: 8 };
    const width = container.clientWidth || 400, chartH = 80, legendH = 20;
    const height = chartH + legendH;
    const innerW = width - M.left - M.right;
    const innerH = chartH - M.top - M.bottom;

    const dates = monthly.map(d => new Date(d.date).getTime());
    const minDate = Math.min(...dates), maxDate = Math.max(...dates);
    const dateRange = maxDate - minDate || 1;
    const xOf = t => M.left + ((t - minDate) / dateRange) * innerW;

    const maxProd = Math.max(1, ...monthly.map(d => Math.max(d.produced_mcf || 0, d.flared_mcf || 0)));
    const yOf = v => M.top + innerH - (Math.min(v, maxProd) / maxProd) * innerH;

    let svg = `<svg viewBox="0 0 ${width} ${height}">`;
    svg += `<line x1="${M.left}" y1="${chartH - M.bottom}" x2="${width - M.right}" y2="${chartH - M.bottom}" stroke="rgba(255,255,255,0.15)" stroke-width="1"/>`;

    const startD = new Date(minDate), endD = new Date(maxDate);
    const firstYear = startD.getFullYear(), lastYear = endD.getFullYear();

    // Year gridlines — track positions to avoid overlapping end labels
    const charW = 6, minGap = 30;
    let firstYearX = Infinity, lastYearX = -Infinity;
    for (let y = firstYear + 1; y <= lastYear; y++) {
        const jan = new Date(y, 0, 1).getTime();
        if (jan <= minDate || jan >= maxDate) continue;
        const x = xOf(jan);
        if (x < firstYearX) firstYearX = x;
        if (x > lastYearX) lastYearX = x;
        svg += `<line x1="${x}" y1="${M.top}" x2="${x}" y2="${chartH - M.bottom}" stroke="rgba(255,255,255,0.08)" stroke-width="1"/>`;
        svg += `<text x="${x}" y="${chartH - 1}" fill="rgba(255,255,255,0.3)" font-size="11" text-anchor="middle">${y}</text>`;
    }

    // Start/end date labels — skip if too close to a year gridline label
    const fmtLabel = d => `${MONTHS[d.getMonth()]} ${d.getFullYear()}`;
    const startLabel = fmtLabel(startD), endLabel = fmtLabel(endD);
    const startLabelEnd = M.left + startLabel.length * charW;
    const endLabelStart = (width - M.right) - endLabel.length * charW;
    if (startLabelEnd + minGap < firstYearX)
        svg += `<text x="${M.left}" y="${chartH - 1}" fill="rgba(255,255,255,0.3)" font-size="11" text-anchor="start">${startLabel}</text>`;
    if (endLabel !== startLabel && endLabelStart - minGap > lastYearX)
        svg += `<text x="${width - M.right}" y="${chartH - 1}" fill="rgba(255,255,255,0.3)" font-size="11" text-anchor="end">${endLabel}</text>`;

    const prodPoints = monthly.map(d => {
        const x = xOf(new Date(d.date).getTime());
        return `${x},${yOf(d.produced_mcf || 0)}`;
    });
    const baseline = `${xOf(dates[dates.length - 1])},${yOf(0)} ${xOf(dates[0])},${yOf(0)}`;
    svg += `<polygon points="${prodPoints.join(' ')} ${baseline}" fill="rgba(255,255,255,0.08)"/>`;
    svg += `<polyline points="${prodPoints.join(' ')}" fill="none" stroke="rgba(255,255,255,0.3)" stroke-width="1"/>`;

    const flaredPoints = monthly.map(d => {
        const x = xOf(new Date(d.date).getTime());
        return `${x},${yOf(d.flared_mcf || 0)}`;
    });
    svg += `<polygon points="${flaredPoints.join(' ')} ${baseline}" fill="rgba(255,100,50,0.2)"/>`;
    svg += `<polyline points="${flaredPoints.join(' ')}" fill="none" stroke="${COLORS.flare}" stroke-width="1.5"/>`;

    const legendY = chartH + 14;
    const mid = width / 2;
    svg += `<line x1="${mid - 68}" y1="${legendY - 3}" x2="${mid - 56}" y2="${legendY - 3}" stroke="${COLORS.flare}" stroke-width="2"/>`;
    svg += `<text x="${mid - 53}" y="${legendY}" fill="${COLORS.flare}" font-size="10">Flared</text>`;
    svg += `<line x1="${mid + 10}" y1="${legendY - 3}" x2="${mid + 22}" y2="${legendY - 3}" stroke="rgba(255,255,255,0.3)" stroke-width="2"/>`;
    svg += `<text x="${mid + 25}" y="${legendY}" fill="rgba(255,255,255,0.4)" font-size="10">Produced</text>`;

    svg += '</svg>';
    container.innerHTML = svg;
}

function num(v) {
    const n = Number(v);
    return isNaN(n) || v == null ? '--' : n.toLocaleString();
}

function formatDate(d) {
    if (!d || d === 'null') return 'N/A';
    return String(d).slice(0, 10);
}
