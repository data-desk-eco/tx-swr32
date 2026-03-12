import * as db from './db.js?v=3';
import { enhance, cancelEnhance, setUpdateCallback, getState, loadAllCached, getCluster, isEnhancing } from './enhance.js?v=3';

const COLORS = {
    flare: '#ffaa44',
    permit: '#00ccff',
    plume: '#ff44ff',
    well: 'rgba(220,220,230,0.8)'
};

let layerState = { flares: true, permits: true, plumes: false, wells: false };
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
    minZoom: 5,
    maxBounds: [[-110, 26], [-95, 37]],
    projection: 'globe',
    hash: 'map'
});


map.on('load', async () => {
    document.getElementById('stat-sites').textContent = 'Loading...';

    await db.init();

    addEmptySources();
    addLayers();
    await refreshFlares();
    await loadPermits();
    loadCachedS2();
    bindUI();
    updateMapCentre();
    handleDeepLink();
    // Stats use queryRenderedFeatures — wait for first idle after data loads
    map.once('idle', updateStats);
});

function addEmptySources() {
    const empty = { type: 'FeatureCollection', features: [] };
    map.addSource('texas', { type: 'geojson', data: 'data/texas.geojson' });
    map.addSource('flares', { type: 'geojson', data: empty });
    map.addSource('permits', { type: 'geojson', data: empty });
    map.addSource('plumes', { type: 'geojson', data: empty });
    map.addSource('wells', { type: 'geojson', data: empty });
    map.addSource('flare-pixels', { type: 'geojson', data: empty });
    map.addSource('s2-detections', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
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

    map.addLayer({
        id: 'wells-layer', type: 'circle', source: 'wells',
        layout: { visibility: 'none' },
        paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 7, 1.5, 10, 3, 14, 5],
            'circle-color': COLORS.well,
            'circle-opacity': 0.25,
            'circle-stroke-width': 1,
            'circle-stroke-color': COLORS.well
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
        paint: { 'circle-radius': plumeRadius(), 'circle-color': COLORS.plume, 'circle-opacity': 0.25, 'circle-stroke-width': 1, 'circle-stroke-color': COLORS.plume }
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
        filter: ['!=', ['get', 'near_excluded_facility'], true],
        paint: { 'fill-color': 'transparent' }
    });

    // Dashed outline
    map.addLayer({
        id: 'flare-pixels-layer', type: 'line', source: 'flare-pixels',
        filter: ['!=', ['get', 'near_excluded_facility'], true],
        paint: {
            'line-color': 'rgba(255,240,150,0.8)',
            'line-width': 1,
            'line-dasharray': [3, 2]
        }
    });

    // Label above pixel square (positioned at top-left corner)
    map.addSource('flare-pixel-labels', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
    map.addLayer({
        id: 'flare-pixels-label', type: 'symbol', source: 'flare-pixel-labels',
        filter: ['!=', ['get', 'near_excluded_facility'], true],
        layout: {
            'text-field': 'FLARE DETECTION AREA',
            'text-font': ['Noto Sans Regular'],
            'text-size': 11,
            'text-anchor': 'bottom-left',
            'text-max-width': 999,
            'text-offset': [-0.1, -0.3]
        },
        minzoom: 13,
        paint: {
            'text-color': 'rgba(255,240,150,0.8)',
            'text-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 1]
        }
    });

    map.addLayer({
        id: 'flares-layer', type: 'circle', source: 'flares',
        filter: ['!=', ['get', 'near_excluded_facility'], true],
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

function plumeRadius() {
    return ['interpolate', ['linear'], ['coalesce', ['get', 'emission_rate'], 100], 10, 3, 500, 6, 5000, 12];
}

// Generate 750m square polygons and top-left label points from flare data
function flarePixelData(flareGeoJson) {
    const HALF_M = 375; // half of 750m pixel
    const squares = [];
    const labels = [];
    for (const f of flareGeoJson.features) {
        const [lon, lat] = f.geometry.coordinates;
        const dLat = HALF_M / 110540;
        const dLon = HALF_M / (111320 * Math.cos(lat * Math.PI / 180));
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
}

async function loadPermits() {
    if (!layerState.permits) return;
    const data = await db.queryPermits({ operator: operatorFilter || undefined });
    map.getSource('permits').setData(data);
}

async function loadPlumes() {
    if (!layerState.plumes) return;
    const data = await db.queryPlumes();
    map.getSource('plumes').setData(data);
}

async function loadWells() {
    if (!layerState.wells) return;
    const data = await db.queryWells({ operator: operatorFilter || undefined });
    map.getSource('wells').setData(data);
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
    document.getElementById('map-centre').textContent = `${c.lat.toFixed(3)}, ${c.lng.toFixed(3)}`;
}

function updateStats() {
    const features = map.queryRenderedFeatures({ layers: ['flares-layer'] });
    const sites = features.length;
    const totalMw = features.reduce((s, f) => s + (Number(f.properties.total_rh_mw) || 0), 0);
    document.getElementById('stat-sites').textContent = sites.toLocaleString();
    document.getElementById('stat-mw').textContent = sites > 0 ? Math.round(totalMw).toLocaleString() : '--';
}

const LAYER_MAP = {
    flares: ['flares-layer', 'flare-pixels-fill', 'flare-pixels-layer', 'flare-pixels-label'],
    permits: ['permits-layer'],
    plumes: ['plumes-layer'],
    wells: ['wells-layer']
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
    }
}

const ALL_CLICK_LAYERS = [
    'flares-layer',
    'flare-pixels-fill',
    'flare-pixels-layer',
    's2-points',
    'permits-layer',
    'plumes-layer',
    'wells-layer'
];

function bindUI() {
    document.getElementById('collapse-toggle').addEventListener('click', () => {
        document.getElementById('left-panel').classList.toggle('collapsed');
    });
    for (const row of document.querySelectorAll('.toggle-row[data-layer]')) {
        const layer = row.dataset.layer;
        const cb = row.querySelector('input');
        cb.addEventListener('change', () => setLayerVisibility(layer, cb.checked));
        row.querySelector('.filter-label').addEventListener('click', () => {
            cb.checked = !cb.checked;
            setLayerVisibility(layer, cb.checked);
        });
    }

    let searchTimeout;
    document.getElementById('operator-search').addEventListener('input', e => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            operatorFilter = e.target.value.trim();
            refreshFlares();
            loadPermits();
            loadWells();
        }, 300);
    });

    document.getElementById('detail-close').addEventListener('click', closeDetail);
    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDetail(); });

    // Overlap navigation
    document.getElementById('overlap-prev').addEventListener('click', () => {
        if (overlappingFeatures.length < 2) return;
        overlapIndex = (overlapIndex - 1 + overlappingFeatures.length) % overlappingFeatures.length;
        showFeatureDetail(overlappingFeatures[overlapIndex]);
    });
    document.getElementById('overlap-next').addEventListener('click', () => {
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
            const isS2 = f.layer.id === 's2-points';
            const key = isS2 && f.properties.id
                ? `s2:${f.properties.id}`
                : f.properties.flare_id != null
                    ? `flare:${f.properties.flare_id}`
                    : `${f.layer.id}:${f.id}`;
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
    });

    // Cursor changes for interactive layers
    for (const id of ALL_CLICK_LAYERS) {
        map.on('mouseenter', id, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', id, () => { map.getCanvas().style.cursor = ''; });
    }

    map.on('move', updateMapCentre);
    map.on('moveend', updateStats);
}

function updateFlareUrl(flareId, mode) {
    const url = new URL(window.location);
    url.searchParams.delete('s2');
    if (flareId != null) {
        url.searchParams.set('flare', flareId);
        if (mode) url.searchParams.set('mode', mode);
        else url.searchParams.delete('mode');
    } else {
        url.searchParams.delete('flare');
        url.searchParams.delete('mode');
    }
    history.replaceState(null, '', url);
}

function updateS2Url(s2Id) {
    const url = new URL(window.location);
    url.searchParams.delete('flare');
    url.searchParams.delete('mode');
    if (s2Id != null) url.searchParams.set('s2', s2Id);
    else url.searchParams.delete('s2');
    history.replaceState(null, '', url);
}

function handleDeepLink() {
    const params = new URLSearchParams(window.location.search);

    const s2Id = params.get('s2');
    if (s2Id) {
        const cluster = getCluster(s2Id);
        if (cluster) {
            map.flyTo({ center: [cluster.lon, cluster.lat], zoom: 16 });
            showS2ClusterDetail(cluster);
        }
        return;
    }

    const flareId = params.get('flare');
    if (!flareId) return;

    const feature = flareFeatures.find(f => String(f.properties.flare_id) === flareId);
    if (!feature) return;

    const [lon, lat] = feature.geometry.coordinates;
    map.flyTo({ center: [lon, lat], zoom: 14 });

    if (params.get('mode') === 's2') {
        showEnhanceDetail(feature);
    } else {
        feature.layer = { id: 'flares-layer' };
        showFeatureDetail(feature);
    }
}

function removeS2Badge() {
    const badge = document.getElementById('s2-badge') || document.getElementById('detail-badge');
    if (badge) {
        badge.id = 'detail-badge';
        badge.classList.add('hidden');
    }
}

function closeDetail() {
    cancelEnhance(map);
    removeS2Badge();
    updateFlareUrl(null);
    updateS2Url(null);
    document.getElementById('detail-panel').classList.add('hidden');
    overlappingFeatures = [];
    overlapIndex = 0;
}

function showFeatureDetail(feature) {
    removeS2Badge();
    const layer = feature.layer.id;
    if (layer === 's2-points') {
        // Don't cancel enhance — let it run in background
        const cluster = getCluster(feature.properties.id);
        if (cluster) showS2ClusterDetail(cluster);
    } else {
        cancelEnhance(map);
        if (layer.startsWith('flare')) showFlareDetail(feature);
        else {
            updateFlareUrl(null);
            if (layer.startsWith('permits-')) showPermitDetail(feature);
            else if (layer.startsWith('plumes-')) showPlumeDetail(feature);
            else if (layer.startsWith('wells-')) showWellDetail(feature);
        }
    }

    // Update overlap nav — only for overlapping permits
    const permitFeatures = overlappingFeatures.filter(f => f.layer.id.startsWith('permits-'));
    const nav = document.getElementById('overlap-nav');
    if (permitFeatures.length > 1) {
        overlappingFeatures = permitFeatures;
        nav.classList.remove('hidden');
        document.getElementById('overlap-count').textContent = `${overlapIndex + 1} / ${overlappingFeatures.length}`;
    } else {
        nav.classList.add('hidden');
    }
}

function field(label, value) {
    return `<div class="detail-field"><span class="detail-field-label">${label}</span><span class="detail-field-value">${value}</span></div>`;
}

function flareStatus(p) {
    const isExcluded = p.near_excluded_facility === true || p.near_excluded_facility === 'true';
    if (isExcluded) return { status: 'excluded', label: 'Excluded' };
    return { status: null, label: null };
}

function permitInfoFromVnf(p) {
    return {
        operator: p.operator_name,
        confidence: p.confidence ? p.confidence.charAt(0).toUpperCase() + p.confidence.slice(1) : null,
        permitName: [...new Set([p.site_name, p.permit_name].filter(Boolean))].join(', ') || null,
        distanceKm: p.nearest_permit_km != null ? Number(p.nearest_permit_km) : null,
        firstDetected: p.first_detected,
        lastDetected: p.last_detected,
    };
}

function permitInfoFromS2(detections, nearbyPermits) {
    if (!detections.length) return null;

    // Find the nearest permit to any detection
    let bestPermit = null, bestDist = Infinity;
    for (const d of detections) {
        const np = nearestPermit(d.lat, d.lon, nearbyPermits);
        if (np && np.distance_km < bestDist) {
            bestDist = np.distance_km;
            bestPermit = np;
        }
    }

    const dates = detections.map(d => d.date).filter(Boolean).sort();
    const firstDetected = dates[0] || null;
    const lastDetected = dates[dates.length - 1] || null;

    let covered = null;
    if (bestPermit && bestDist <= 0.375) {
        covered = firstDetected && bestPermit.earliest_effective <= firstDetected
            && (!bestPermit.latest_expiration || bestPermit.latest_expiration >= lastDetected);
    }

    return {
        operator: bestPermit?.operator_name || null,
        permitName: bestPermit && bestDist <= 0.375 ? (bestPermit.name || 'Unnamed') : null,
        distanceKm: bestPermit && bestDist <= 0.375 ? bestDist : null,
        firstDetected,
        lastDetected,
        covered,
        permitDates: bestPermit && bestDist <= 0.375
            ? (bestPermit.earliest_effective || '?') + ' → ' + (bestPermit.latest_expiration || 'open')
            : null,
    };
}

function permitCoverageHtml(info) {
    if (!info) return '';
    const coverageLabel = info.covered === true
        ? '<span class="permit-covered">covered</span>'
        : info.covered === false
            ? '<span class="permit-uncovered">uncovered</span>'
            : null;
    return `
        <div class="detail-row">
            ${field('Operator', info.operator || 'N/A')}
            ${info.confidence != null ? field('Confidence', info.confidence) : ''}
            ${field('Nearest permit', info.permitName || 'None')}
            ${field('Distance', info.distanceKm != null ? info.distanceKm.toFixed(2) + ' km' : 'N/A')}
            ${coverageLabel ? field('Coverage', coverageLabel) : ''}
            ${info.permitDates ? field('Permit dates', info.permitDates) : ''}
        </div>
        <div class="detail-row">
            ${field('First detected', formatDate(info.firstDetected))}
            ${field('Last detected', formatDate(info.lastDetected))}
        </div>
    `;
}

async function showFlareDetail(feature) {
    const p = feature.properties;
    const panel = document.getElementById('detail-panel');
    const { status, label: statusLabel } = flareStatus(p);
    updateFlareUrl(p.flare_id);

    document.getElementById('detail-title').textContent = `Flare ${p.flare_id}`;
    document.getElementById('detail-coords').textContent = `${Number(p.lat).toFixed(4)}, ${Number(p.lon).toFixed(4)}`;
    removeS2Badge();
    const badge = document.getElementById('detail-badge');
    if (status) {
        badge.className = `status-badge ${status}`;
        badge.textContent = statusLabel;
        badge.classList.remove('hidden');
    }

    let leaseHtml = '';
    try {
        const leases = await db.queryLeases(p.flare_id);
        if (leases.length > 0) {
            const names = [...new Set(leases.map(l => l.lease_name).filter(Boolean))];
            if (names.length > 0) {
                leaseHtml = '<div class="detail-row lease-row">' + field('Leases', names.join(', ')) + '</div>';
            } else {
                leaseHtml = '<div class="detail-row lease-row">' + field('Leases', `${leases.length} matched (unnamed)`) + '</div>';
            }
        }
    } catch { /* lease query failed, skip */ }

    document.getElementById('intensity-chart').innerHTML = '';
    document.getElementById('detail-body').innerHTML = `
        <div class="stats-grid">
            <div class="stat"><div class="stat-big">${num(p.total_rh_mw)}</div><div class="stat-unit">total MW</div></div>
            <div class="stat"><div class="stat-big">${num(p.detection_days)}</div><div class="stat-unit">detection days</div></div>
        </div>
        ${permitCoverageHtml(permitInfoFromVnf(p))}
        ${leaseHtml}
    `;
    panel.classList.remove('hidden');

    // Load sparkline async, then append enhance button after chart renders
    db.queryDetections(p.flare_id).then(detections => {
        renderSparkline(detections);

        // Enhance button — appended after sparkline so it's not overwritten
        const chartContainer = document.getElementById('intensity-chart');
        const enhanceBtn = document.createElement('button');
        enhanceBtn.className = 'enhance-btn';
        enhanceBtn.textContent = 'Enhance with Sentinel-2';
        enhanceBtn.addEventListener('click', () => {
            showEnhanceDetail(feature);
        });
        chartContainer.appendChild(enhanceBtn);
    }).catch(() => {});
}

function showPermitDetail(feature) {
    const p = feature.properties;
    const rate = Number(p.max_release_rate_mcf_day);
    document.getElementById('intensity-chart').innerHTML = '';
    removeS2Badge();
    document.getElementById('detail-title').textContent = p.name || 'Permit location';
    document.getElementById('detail-coords').textContent = `${Number(p.latitude).toFixed(4)}, ${Number(p.longitude).toFixed(4)}`;
    document.getElementById('detail-body').innerHTML = `
        <div class="stats-grid">
            <div class="stat"><div class="stat-big">${rate > 0 ? rate.toLocaleString() : 'N/A'}</div><div class="stat-unit">max Mcf/day</div></div>
            <div class="stat"><div class="stat-big">${Number(p.n_filings)}</div><div class="stat-unit">filings</div></div>
        </div>
        <div class="detail-row">
            ${field('Operator', p.operator_name || 'N/A')}
            ${field('County', p.county || 'N/A')}
            ${field('District', p.district || 'N/A')}
            ${field('Release type', p.release_type || 'N/A')}
        </div>
        <div class="detail-row">
            ${field('Earliest effective', formatDate(p.earliest_effective))}
            ${field('Latest expiration', formatDate(p.latest_expiration))}
        </div>
        ${p.exception_reasons ? `<div class="detail-row">${field('Reasons', p.exception_reasons.split('; ').join(' / '))}</div>` : ''}
    `;
    document.getElementById('detail-panel').classList.remove('hidden');
}

function plumeUrl(source, id) {
    if (source === 'cm') return `https://data.carbonmapper.org/?plume_id=${encodeURIComponent(id)}`;
    if (source === 'imeo') return `https://methanedata.unep.org`;
    return null;
}

function showPlumeDetail(feature) {
    const p = feature.properties;
    document.getElementById('intensity-chart').innerHTML = '';
    removeS2Badge();
    const url = plumeUrl(p.source, p.plume_id);
    const titleEl = document.getElementById('detail-title');
    if (url) {
        titleEl.innerHTML = `<a href="${url}" target="_blank" rel="noopener" style="color: inherit; text-decoration: none;">${p.plume_id}</a>`;
    } else {
        titleEl.textContent = p.plume_id;
    }
    document.getElementById('detail-coords').textContent = `${Number(p.latitude).toFixed(4)}, ${Number(p.longitude).toFixed(4)}`;
    document.getElementById('detail-body').innerHTML = `
        <div class="stats-grid">
            <div class="stat"><div class="stat-big">${Number(p.emission_rate).toLocaleString()}</div><div class="stat-unit">kg/hr</div></div>
            <div class="stat"><div class="stat-big">&plusmn;${Number(p.emission_uncertainty || 0).toLocaleString()}</div><div class="stat-unit">uncertainty</div></div>
        </div>
        <div class="detail-row">
            ${field('Source', p.source)}
            ${field('Satellite', p.satellite || 'N/A')}
            ${field('Date', formatDate(p.date))}
            ${field('Sector', p.sector || 'N/A')}
        </div>
    `;
    document.getElementById('detail-panel').classList.remove('hidden');
}

function showWellDetail(feature) {
    const p = feature.properties;
    document.getElementById('intensity-chart').innerHTML = '';
    removeS2Badge();
    document.getElementById('detail-title').textContent = `Well ${p.api}`;
    document.getElementById('detail-coords').textContent = `${Number(p.latitude).toFixed(4)}, ${Number(p.longitude).toFixed(4)}`;
    document.getElementById('detail-body').innerHTML = `
        <div class="detail-row">
            ${field('Operator', p.operator_name || 'N/A')}
            ${field('Type', p.oil_gas_code === 'O' ? 'Oil' : p.oil_gas_code === 'G' ? 'Gas' : p.oil_gas_code || 'N/A')}
            ${field('District', p.lease_district || 'N/A')}
            ${field('Lease', p.lease_number || 'N/A')}
            ${field('Well #', p.well_number || 'N/A')}
        </div>
    `;
    document.getElementById('detail-panel').classList.remove('hidden');
}

function distKm(lat1, lon1, lat2, lon2) {
    const cosLat = Math.cos(lat1 * Math.PI / 180);
    return 111.32 * Math.sqrt((lat2 - lat1) ** 2 * (110.54 / 111.32) ** 2 + ((lon2 - lon1) * cosLat) ** 2);
}

function nearestPermit(lat, lon, permits) {
    let best = null, bestDist = Infinity;
    for (const p of permits) {
        const d = distKm(lat, lon, p.latitude, p.longitude);
        if (d < bestDist) { bestDist = d; best = p; }
    }
    return best ? { ...best, distance_km: bestDist } : null;
}

function showEnhanceDetail(feature) {
    const p = feature.properties;
    const panel = document.getElementById('detail-panel');
    updateFlareUrl(p.flare_id, 's2');

    document.getElementById('detail-title').textContent = `Sentinel-2 · Flare ${p.flare_id}`;
    document.getElementById('detail-coords').textContent = `${Number(p.lat).toFixed(4)}, ${Number(p.lon).toFixed(4)}`;
    removeS2Badge();
    const badge = document.getElementById('detail-badge');
    badge.className = 'status-badge s2';
    badge.id = 's2-badge';
    badge.textContent = 'Enhancing';
    badge.classList.remove('hidden');

    document.getElementById('overlap-nav').classList.add('hidden');
    document.getElementById('intensity-chart').innerHTML = '';
    document.getElementById('detail-body').innerHTML = `
        <div id="s2-stop-section"></div>
        <div id="s2-cluster-list"></div>
    `;
    panel.classList.remove('hidden');

    function renderStopButton() {
        const section = document.getElementById('s2-stop-section');
        if (!section) return;
        if (isEnhancing()) {
            if (!section.querySelector('.stop-analysis-btn')) {
                const btn = document.createElement('button');
                btn.className = 'stop-analysis-btn';
                btn.textContent = 'Stop Analysis';
                btn.addEventListener('click', () => {
                    cancelEnhance(map);
                    btn.remove();
                });
                section.appendChild(btn);
            }
        } else {
            section.innerHTML = '';
        }
    }

    // Wire up live updates before starting (cache path fires synchronously)
    setUpdateCallback((s) => {
        const s2b = document.getElementById('s2-badge');
        if (!s2b) return;

        if (s.enhancing) {
            s2b.textContent = s.progress?.total
                ? `${s.progress.done} / ${s.progress.total}${s.progress.skipped ? ` (${s.progress.skipped} cached)` : ''}`
                : 'Searching...';
        } else if (s.error) {
            s2b.className = 'status-badge excluded';
            s2b.textContent = 'Failed';
        }

        renderStopButton();

        // Update cluster list (live during enhancement and on completion)
        if (s.clusters?.length) {
            if (!s.enhancing) {
                s2b.textContent = `${s.clusters.length} source${s.clusters.length !== 1 ? 's' : ''}`;
            }
            const list = document.getElementById('s2-cluster-list');
            if (list) {
                list.className = 'enhance-results';
                list.innerHTML = s.clusters.map(c =>
                    `<div class="enhance-cluster" data-id="${c.id}">
                        <div class="cluster-chart"></div>
                        <div class="cluster-header">
                            <span class="cluster-dot"></span>
                            B12 ${c.max_b12.toFixed(2)} · ${c.detection_count} det · ${c.first_date}${c.first_date !== c.last_date ? ` – ${c.last_date}` : ''}
                        </div>
                    </div>`
                ).join('');
                list.querySelectorAll('.enhance-cluster').forEach(el => {
                    const c = getCluster(el.dataset.id);
                    if (c?.detections?.length) {
                        renderS2Chart(c.detections, el.querySelector('.cluster-chart'));
                    }
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

    // Start enhancement (may resolve from cache synchronously)
    enhance(feature, map);
}

function showS2ClusterDetail(cluster) {
    removeS2Badge();
    const panel = document.getElementById('detail-panel');
    updateS2Url(cluster.id);

    document.getElementById('detail-title').textContent = `S2 Source ${cluster.id}`;
    document.getElementById('detail-coords').textContent = `${cluster.lat.toFixed(4)}, ${cluster.lon.toFixed(4)}`;

    const badge = document.getElementById('detail-badge');
    badge.className = 'status-badge s2';
    badge.textContent = `${cluster.detection_count} det`;
    badge.classList.remove('hidden');

    document.getElementById('overlap-nav').classList.add('hidden');
    document.getElementById('intensity-chart').innerHTML = '';
    document.getElementById('detail-body').innerHTML = `
        <div class="stats-grid">
            <div class="stat"><div class="stat-big">${cluster.max_b12.toFixed(2)}</div><div class="stat-unit">peak B12</div></div>
            <div class="stat"><div class="stat-big">${cluster.avg_b12.toFixed(2)}</div><div class="stat-unit">mean B12</div></div>
        </div>
        <div class="detail-row">
            ${field('First detected', formatDate(cluster.first_date))}
            ${field('Last detected', formatDate(cluster.last_date))}
        </div>
        <div id="s2-permit-section"></div>
    `;

    // Timeline chart from cluster detections
    if (cluster.detections?.length) {
        renderS2Chart(cluster.detections);
    }

    // Stop button if enhancement is running in background
    if (isEnhancing()) {
        const stopBtn = document.createElement('button');
        stopBtn.className = 'stop-analysis-btn';
        stopBtn.textContent = 'Stop Analysis';
        stopBtn.addEventListener('click', () => {
            cancelEnhance(map);
            stopBtn.remove();
        });
        document.getElementById('detail-body').appendChild(stopBtn);
    }

    // Permit coverage
    db.queryNearbyPermits(cluster.lat, cluster.lon).then(permits => {
        const info = permitInfoFromS2(cluster.detections || [], permits);
        const el = document.getElementById('s2-permit-section');
        if (el) el.innerHTML = permitCoverageHtml(info);
    }).catch(() => {});

    // Link to parent VNF flare
    if (cluster.flare_id != null) {
        const parent = flareFeatures.find(f => f.properties.flare_id === cluster.flare_id);
        if (parent) {
            const link = document.createElement('div');
            link.className = 'detail-row';
            link.innerHTML = `<a href="?flare=${cluster.flare_id}" class="vnf-link">← VNF Flare ${cluster.flare_id}</a>`;
            link.querySelector('a').addEventListener('click', (e) => {
                e.preventDefault();
                parent.layer = { id: 'flares-layer' };
                showFeatureDetail(parent);
            });
            document.getElementById('detail-body').appendChild(link);
        }
    }

    panel.classList.remove('hidden');
}

function renderS2Chart(detections, target) {
    const container = target || document.getElementById('intensity-chart');
    if (!detections?.length) { container.innerHTML = ''; return; }

    const compact = target != null; // inline chart in cluster list
    const margin = compact
        ? { top: 4, right: 4, bottom: 12, left: 4 }
        : { top: 8, right: 8, bottom: 16, left: 8 };
    const width = container.clientWidth || 400, height = compact ? 48 : 100;
    const innerW = width - margin.left - margin.right;
    const innerH = height - margin.top - margin.bottom;

    const dates = detections.map(d => new Date(d.date).getTime());
    const minDate = Math.min(...dates);
    const maxDate = Math.max(...dates);
    const dateRange = maxDate - minDate || 1;

    const vals = detections.map(d => d.max_b12);
    const maxVal = Math.max(1.5, ...vals);

    let svg = `<svg viewBox="0 0 ${width} ${height}">`;
    svg += `<line x1="${margin.left}" y1="${height - margin.bottom}" x2="${width - margin.right}" y2="${height - margin.bottom}" stroke="rgba(255,255,255,0.15)" stroke-width="1"/>`;

    // Month gridlines with labels
    const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const fontSize = compact ? 7 : 9;
    const charW = compact ? 4 : 5;
    const startD = new Date(minDate), endD = new Date(maxDate);
    const firstMonth = new Date(startD);
    firstMonth.setDate(1);
    firstMonth.setMonth(firstMonth.getMonth() + 1);
    const minLabelGap = compact ? 20 : 30;
    const startX = margin.left, endX = width - margin.right;
    let lastLabelX = startX; // track rightmost label position
    // Start date label at left edge
    const startLabel = `${MONTHS[startD.getMonth()]} ${startD.getFullYear()}`;
    svg += `<text x="${startX}" y="${height - 2}" fill="rgba(255,255,255,0.35)" font-size="${fontSize}" text-anchor="start">${startLabel}</text>`;
    lastLabelX = startX + startLabel.length * charW;
    for (let d = new Date(firstMonth); d <= endD; d.setMonth(d.getMonth() + 1)) {
        const t = d.getTime();
        const x = margin.left + ((t - minDate) / dateRange) * innerW;
        const isJan = d.getMonth() === 0;
        svg += `<line x1="${x}" y1="${margin.top}" x2="${x}" y2="${height - margin.bottom}" stroke="rgba(255,255,255,${isJan ? 0.15 : 0.06})" stroke-width="1"/>`;
        const label = isJan ? `${MONTHS[0]} ${d.getFullYear()}` : MONTHS[d.getMonth()];
        const labelW = label.length * charW;
        if (x - labelW / 2 > lastLabelX + minLabelGap && x + labelW / 2 < endX - minLabelGap) {
            svg += `<text x="${x}" y="${height - 2}" fill="rgba(255,255,255,${isJan ? 0.4 : 0.25})" font-size="${fontSize}" text-anchor="middle">${label}</text>`;
            lastLabelX = x + labelW / 2;
        }
    }
    // End date label at right edge (skip if too close to last gridline label)
    const endLabel = `${MONTHS[endD.getMonth()]} ${endD.getFullYear()}`;
    if (endLabel !== startLabel && endX - endLabel.length * charW > lastLabelX + minLabelGap) {
        svg += `<text x="${endX}" y="${height - 2}" fill="rgba(255,255,255,0.35)" font-size="${fontSize}" text-anchor="end">${endLabel}</text>`;
    }

    // B12 detection dots
    const dotR = compact ? 1.5 : 2;
    detections.forEach(det => {
        const date = new Date(det.date).getTime();
        const x = margin.left + ((date - minDate) / dateRange) * innerW;
        const t = Math.min(1, det.max_b12 / maxVal);
        const y = margin.top + innerH - t * innerH;
        const b = det.max_b12;
        const color = b < 0.3 ? '#660800' : b < 0.5 ? '#991100' : b < 0.7 ? '#cc2200' : b < 0.9 ? '#ff4422' : b < 1.2 ? '#ff8844' : '#ffcc44';
        svg += `<circle class="chart-dot" cx="${x}" cy="${y}" r="${dotR}" fill="${color}" opacity="0.8"/>`;
    });

    svg += '</svg>';
    container.innerHTML = svg;
}

function renderSparkline(detections) {
    const container = document.getElementById('intensity-chart');
    if (!detections?.length) { container.innerHTML = ''; return; }

    const margin = { top: 8, right: 8, bottom: 16, left: 8 };
    const width = container.clientWidth || 400, height = 100;
    const innerW = width - margin.left - margin.right;
    const innerH = height - margin.top - margin.bottom;

    const dates = detections.map(d => new Date(d.date).getTime());
    const minDate = Math.min(...dates);
    const maxDate = Math.max(...dates);
    const dateRange = maxDate - minDate || 1;

    // Log scale for rh_mw (MW)
    const vals = detections.map(d => d.rh_mw).filter(v => v > 0);
    const lo = 0.1, hi = Math.max(10, ...vals);

    let svg = `<svg viewBox="0 0 ${width} ${height}">`;
    svg += `<line x1="${margin.left}" y1="${height - margin.bottom}" x2="${width - margin.right}" y2="${height - margin.bottom}" stroke="rgba(255,255,255,0.15)" stroke-width="1"/>`;

    // Year gridlines
    const firstYear = new Date(minDate).getFullYear();
    const lastYear = new Date(maxDate).getFullYear();
    for (let y = firstYear + 1; y <= lastYear; y++) {
        const jan1 = new Date(y, 0, 1).getTime();
        const x = margin.left + ((jan1 - minDate) / dateRange) * innerW;
        svg += `<line x1="${x}" y1="${margin.top}" x2="${x}" y2="${height - margin.bottom}" stroke="rgba(255,255,255,0.08)" stroke-width="1"/>`;
        svg += `<text x="${x}" y="${height - 2}" fill="rgba(255,255,255,0.3)" font-size="10" text-anchor="middle">${y}</text>`;
    }

    // Detection dots — small for dense data
    detections.forEach(det => {
        const date = new Date(det.date).getTime();
        const x = margin.left + ((date - minDate) / dateRange) * innerW;
        const val = det.rh_mw || 0;
        const t = val > 0 ? Math.max(0, Math.min(1, (Math.log(Math.max(lo, val)) - Math.log(lo)) / (Math.log(hi) - Math.log(lo)))) : 0;
        const y = margin.top + innerH - t * innerH;
        const mw = det.rh_mw || 0;
        const color = mw < 0.3 ? '#660800' : mw < 0.6 ? '#991100' : mw < 0.9 ? '#cc3300' : mw < 1.3 ? '#ff5522' : mw < 2 ? '#ff8844' : mw < 4 ? '#ffcc66' : '#ffeeaa';
        svg += `<circle class="chart-dot" cx="${x}" cy="${y}" r="2" fill="${color}" opacity="0.8"/>`;
    });

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
