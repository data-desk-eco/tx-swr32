import * as db from './db.js';

const COLORS = {
    dark: '#ff6b35',
    permitted: '#2ecc71',
    excluded: '#666',
    permit: '#3498db',
    plumeFlaring: '#f39c12',
    plumeUnlit: '#e67e22',
    plumeOther: '#9b59b6'
};

let layerState = { flares: true, permits: false, plumes: false };
let darkOnly = false;
let operatorFilter = '';

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
    hash: 'map'
});

const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, maxWidth: '240px', className: 'flare-popup' });

map.on('load', async () => {
    document.getElementById('stat-sites').textContent = 'Loading...';

    await db.init();

    addEmptySources();
    addLayers();
    await refreshFlares();
    await updateStats();
    bindUI();
});

function addEmptySources() {
    const empty = { type: 'FeatureCollection', features: [] };
    map.addSource('flares', { type: 'geojson', data: empty });
    map.addSource('permits', { type: 'geojson', data: empty });
    map.addSource('plumes', { type: 'geojson', data: empty });
}

function addLayers() {
    // Flare radius: scale on total_rh_mw for area-proportional sizing
    const flareRadius = [
        'interpolate', ['linear'],
        ['coalesce', ['get', 'total_rh_mw'], 1],
        1, 3, 50, 7, 200, 12, 1000, 20, 5000, 30
    ];

    // Permit radius: scale on max_release_rate_mcf_day
    const permitRadius = [
        'interpolate', ['linear'],
        ['coalesce', ['get', 'max_release_rate_mcf_day'], 0],
        0, 2, 10, 3, 50, 5, 200, 8, 1000, 12
    ];

    map.addLayer({
        id: 'permits-layer', type: 'circle', source: 'permits',
        layout: { visibility: 'none' },
        paint: {
            'circle-radius': permitRadius,
            'circle-color': 'transparent',
            'circle-stroke-width': 1.5,
            'circle-stroke-color': COLORS.permit,
            'circle-stroke-opacity': 0.6
        }
    });

    map.addLayer({
        id: 'plumes-other', type: 'circle', source: 'plumes',
        layout: { visibility: 'none' },
        filter: ['!', ['in', ['get', 'classification'], ['literal', ['flaring', 'unlit']]]],
        paint: { 'circle-radius': plumeRadius(), 'circle-color': 'transparent', 'circle-stroke-width': 1.5, 'circle-stroke-color': COLORS.plumeOther, 'circle-stroke-opacity': 0.6 }
    });
    map.addLayer({
        id: 'plumes-unlit', type: 'circle', source: 'plumes',
        layout: { visibility: 'none' },
        filter: ['==', ['get', 'classification'], 'unlit'],
        paint: { 'circle-radius': plumeRadius(), 'circle-color': 'transparent', 'circle-stroke-width': 1.5, 'circle-stroke-color': COLORS.plumeUnlit, 'circle-stroke-opacity': 0.7 }
    });
    map.addLayer({
        id: 'plumes-flaring', type: 'circle', source: 'plumes',
        layout: { visibility: 'none' },
        filter: ['==', ['get', 'classification'], 'flaring'],
        paint: { 'circle-radius': plumeRadius(), 'circle-color': 'transparent', 'circle-stroke-width': 1.5, 'circle-stroke-color': COLORS.plumeFlaring, 'circle-stroke-opacity': 0.7 }
    });

    map.addLayer({
        id: 'flares-excluded', type: 'circle', source: 'flares',
        filter: ['==', ['get', 'near_excluded_facility'], true],
        paint: { 'circle-radius': flareRadius, 'circle-color': 'transparent', 'circle-stroke-width': 1.5, 'circle-stroke-color': COLORS.excluded, 'circle-stroke-opacity': 0.5 }
    });
    map.addLayer({
        id: 'flares-permitted', type: 'circle', source: 'flares',
        filter: ['all', ['!=', ['get', 'near_excluded_facility'], true], ['<=', ['get', 'dark_pct'], 50]],
        paint: { 'circle-radius': flareRadius, 'circle-color': 'transparent', 'circle-stroke-width': 1.5, 'circle-stroke-color': COLORS.permitted, 'circle-stroke-opacity': 0.8 }
    });
    map.addLayer({
        id: 'flares-dark', type: 'circle', source: 'flares',
        filter: ['all', ['!=', ['get', 'near_excluded_facility'], true], ['>', ['get', 'dark_pct'], 50]],
        paint: { 'circle-radius': flareRadius, 'circle-color': 'transparent', 'circle-stroke-width': 1.5, 'circle-stroke-color': COLORS.dark, 'circle-stroke-opacity': 0.9 }
    });
}

function plumeRadius() {
    return ['interpolate', ['linear'], ['coalesce', ['get', 'emission_rate'], 100], 10, 3, 500, 6, 5000, 12];
}

async function refreshFlares() {
    const data = await db.queryFlares({ operator: operatorFilter || undefined, darkOnly });
    map.getSource('flares').setData(data);
}

async function loadPermits() {
    if (!layerState.permits) return;
    const data = await db.queryPermits();
    map.getSource('permits').setData(data);
}

async function loadPlumes() {
    if (!layerState.plumes) return;
    const data = await db.queryPlumes();
    map.getSource('plumes').setData(data);
}

async function updateStats() {
    const stats = await db.queryStats();
    document.getElementById('stat-sites').textContent = stats.total_sites?.toLocaleString() || '--';
    document.getElementById('stat-dark-rate').textContent = stats.avg_dark_pct != null ? stats.avg_dark_pct + '%' : '--';
    document.getElementById('stat-mw').textContent = stats.total_mw?.toLocaleString() || '--';
}

const LAYER_MAP = {
    flares: ['flares-dark', 'flares-permitted', 'flares-excluded'],
    permits: ['permits-layer'],
    plumes: ['plumes-flaring', 'plumes-unlit', 'plumes-other']
};

const LEGEND_MAP = {
    permits: ['legend-permits'],
    plumes: ['legend-plumes-flaring', 'legend-plumes-unlit', 'legend-plumes-other']
};

function setLayerVisibility(layer, visible) {
    layerState[layer] = visible;
    const vis = visible ? 'visible' : 'none';
    for (const id of LAYER_MAP[layer]) {
        map.setLayoutProperty(id, 'visibility', vis);
    }
    for (const id of (LEGEND_MAP[layer] || [])) {
        document.getElementById(id).style.display = visible ? '' : 'none';
    }

    if (visible) {
        if (layer === 'permits') loadPermits();
        if (layer === 'plumes') loadPlumes();
    }
}

function bindUI() {
    document.getElementById('collapse-toggle').addEventListener('click', () => {
        document.getElementById('left-panel').classList.toggle('collapsed');
    });
    document.getElementById('legend-collapse').addEventListener('click', () => {
        document.getElementById('legend').classList.toggle('collapsed');
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

    const darkCb = document.querySelector('#dark-only-toggle input');
    darkCb.addEventListener('change', () => { darkOnly = darkCb.checked; refreshFlares(); });

    let searchTimeout;
    document.getElementById('operator-search').addEventListener('input', e => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            operatorFilter = e.target.value.trim();
            refreshFlares();
        }, 300);
    });

    document.getElementById('detail-close').addEventListener('click', closeDetail);
    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDetail(); });

    const clickLayers = ['flares-dark', 'flares-permitted', 'flares-excluded'];
    for (const id of clickLayers) {
        map.on('click', id, e => showFlareDetail(e.features[0]));
        map.on('mouseenter', id, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', id, () => { map.getCanvas().style.cursor = ''; });
    }

    map.on('click', 'permits-layer', e => showPermitDetail(e.features[0]));
    map.on('mouseenter', 'permits-layer', () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'permits-layer', () => { map.getCanvas().style.cursor = ''; });

    for (const id of ['plumes-flaring', 'plumes-unlit', 'plumes-other']) {
        map.on('click', id, e => showPlumeDetail(e.features[0]));
        map.on('mouseenter', id, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', id, () => { map.getCanvas().style.cursor = ''; });
    }

    map.on('mousemove', 'flares-dark', e => showTooltip(e));
    map.on('mousemove', 'flares-permitted', e => showTooltip(e));
    map.on('mouseleave', 'flares-dark', () => popup.remove());
    map.on('mouseleave', 'flares-permitted', () => popup.remove());
}

function showTooltip(e) {
    const p = e.features[0].properties;
    popup.setLngLat(e.lngLat).setHTML(
        `<strong>${p.operator_name}</strong><br>` +
        `${p.dark_pct}% dark · ${Number(p.total_rh_mw).toLocaleString()} MW`
    ).addTo(map);
}

function closeDetail() {
    document.getElementById('detail-panel').classList.add('hidden');
}

function field(label, value) {
    return `<div class="detail-field"><span class="detail-field-label">${label}</span><span class="detail-field-value">${value}</span></div>`;
}

async function showFlareDetail(feature) {
    const p = feature.properties;
    const panel = document.getElementById('detail-panel');
    const isDark = p.dark_pct > 50;
    const isExcluded = p.near_excluded_facility === true || p.near_excluded_facility === 'true';
    const status = isExcluded ? 'excluded' : (isDark ? 'dark' : 'permitted');
    const statusLabel = isExcluded ? 'Excluded' : (isDark ? `${p.dark_pct}% dark` : `${100 - p.dark_pct}% permitted`);

    document.getElementById('detail-title').textContent = `Flare ${p.flare_id}`;
    document.getElementById('detail-coords').textContent = `${Number(p.lat).toFixed(4)}, ${Number(p.lon).toFixed(4)}`;

    let leaseHtml = '';
    try {
        const leases = await db.queryFlareLeases(p.flare_id);
        if (leases.length > 0) {
            const names = [...new Set(leases.map(l => l.lease_name).filter(Boolean))];
            if (names.length > 0) {
                leaseHtml = '<div class="detail-row">' + field('Leases', names.join(', ')) + '</div>';
            } else {
                leaseHtml = '<div class="detail-row">' + field('Leases', `${leases.length} matched (unnamed)`) + '</div>';
            }
        }
    } catch { /* lease query failed, skip */ }

    document.getElementById('detail-body').innerHTML = `
        <div class="detail-row" style="padding-top:0">
            <span class="status-badge ${status}">${statusLabel}</span>
        </div>
        <div class="stats-grid">
            <div class="stat"><div class="stat-big">${Number(p.avg_rh_mw).toFixed(1)}</div><div class="stat-unit">avg MW</div></div>
            <div class="stat"><div class="stat-big">${Number(p.dark_days).toLocaleString()}/${Number(p.total_days).toLocaleString()}</div><div class="stat-unit">dark/total days</div></div>
        </div>
        <div class="detail-row">
            ${field('Operator', p.operator_name)}
            ${field('Confidence', p.confidence || 'N/A')}
            ${field('Nearest permit', [p.site_name, p.permit_name].filter(Boolean).join(', ') || 'None')}
        </div>
        <div class="detail-row">
            ${field('First detected', formatDate(p.first_detected))}
            ${field('Last detected', formatDate(p.last_detected))}
        </div>
        ${leaseHtml}
    `;
    panel.classList.remove('hidden');
}

function showPermitDetail(feature) {
    const p = feature.properties;
    const rate = Number(p.max_release_rate_mcf_day);
    const days = Number(p.total_permitted_days);
    document.getElementById('detail-title').textContent = p.name || 'Permit location';
    document.getElementById('detail-coords').textContent = `${Number(p.latitude).toFixed(4)}, ${Number(p.longitude).toFixed(4)}`;
    document.getElementById('detail-body').innerHTML = `
        <div class="stats-grid">
            <div class="stat"><div class="stat-big">${rate > 0 ? rate.toLocaleString() : 'N/A'}</div><div class="stat-unit">max Mcf/day</div></div>
            <div class="stat"><div class="stat-big">${Number(p.n_filings)}</div><div class="stat-unit">filings</div></div>
            <div class="stat"><div class="stat-big">${days > 0 ? days.toLocaleString() : 'N/A'}</div><div class="stat-unit">permitted days</div></div>
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
    `;
    document.getElementById('detail-panel').classList.remove('hidden');
}

function showPlumeDetail(feature) {
    const p = feature.properties;
    document.getElementById('detail-title').textContent = `Plume ${p.plume_id}`;
    document.getElementById('detail-coords').textContent = `${Number(p.latitude).toFixed(4)}, ${Number(p.longitude).toFixed(4)}`;
    document.getElementById('detail-body').innerHTML = `
        <div class="detail-row" style="padding-top:0">
            <span class="status-badge ${p.classification === 'unlit' ? 'unlit' : ''}">${p.classification}</span>
        </div>
        <div class="stats-grid">
            <div class="stat"><div class="stat-big">${Number(p.emission_rate).toLocaleString()}</div><div class="stat-unit">kg/hr</div></div>
            <div class="stat"><div class="stat-big">&plusmn;${Number(p.emission_uncertainty || 0).toLocaleString()}</div><div class="stat-unit">uncertainty</div></div>
        </div>
        <div class="detail-row">
            ${field('Source', p.source)}
            ${field('Satellite', p.satellite || 'N/A')}
            ${field('Date', formatDate(p.date))}
            ${field('Sector', p.sector || 'N/A')}
            ${p.vnf_flare_id ? field('VNF match', `Flare ${p.vnf_flare_id} (${Number(p.vnf_distance_km).toFixed(3)} km)`) : ''}
        </div>
    `;
    document.getElementById('detail-panel').classList.remove('hidden');
}

function formatDate(d) {
    if (!d || d === 'null') return 'N/A';
    return String(d).slice(0, 10);
}
