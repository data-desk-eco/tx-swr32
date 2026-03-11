// enhance.js — Sentinel-2 flare enhancement for gaslight
// Spawns s2-flares Web Worker, streams detections to map, runs clustering on completion.

let worker = null;
let state = { enhancing: false, progress: null, detections: [], clusters: null, error: null };
let onUpdate = null;  // callback for UI updates

const CACHE_PREFIX = 's2:';

function cacheKey(flareId) { return CACHE_PREFIX + flareId; }

function saveCache(flareId, detections, clusters) {
    try {
        localStorage.setItem(cacheKey(flareId), JSON.stringify({ detections, clusters }));
    } catch { /* quota exceeded — ignore */ }
}

function loadCache(flareId) {
    try {
        const raw = localStorage.getItem(cacheKey(flareId));
        return raw ? JSON.parse(raw) : null;
    } catch { return null; }
}

export function setUpdateCallback(fn) { onUpdate = fn; }

function setMapDetections(map, detections) {
    const fc = {
        type: 'FeatureCollection',
        features: detections.map(d => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [d.lon, d.lat] },
            properties: d,
        })),
    };
    map.getSource('s2-detections')?.setData(fc);
}

export function enhance(flare, map) {
    cancelEnhance(map);

    const p = flare.properties;
    const lon = Number(p.lon);
    const lat = Number(p.lat);

    // 750m pixel bbox (same math as flarePixelData in app.js)
    const dLat = 375 / 110540;
    const dLon = 375 / (111320 * Math.cos(lat * Math.PI / 180));
    const bbox = [lon - dLon, lat - dLat, lon + dLon, lat + dLat];

    // Zoom to pixel square
    map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 80, maxZoom: 17 });

    // Check cache first
    const cached = loadCache(p.flare_id);
    if (cached) {
        state = { enhancing: false, progress: null, detections: cached.detections, clusters: cached.clusters, error: null };
        setMapDetections(map, cached.clusters || cached.detections);
        onUpdate?.(state);
        return;
    }

    const end = p.last_detected;
    // Cap to last year to keep image count manageable
    const oneYearBefore = new Date(new Date(end).getTime() - 365 * 86400000).toISOString().slice(0, 10);
    const start = p.first_detected > oneYearBefore ? p.first_detected : oneYearBefore;

    state = { enhancing: true, progress: { done: 0, total: null }, detections: [], clusters: null, error: null };
    onUpdate?.(state);

    // Module worker — ES module imports work natively
    worker = new Worker('vendor/s2-flares/worker.js', { type: 'module' });

    worker.onmessage = (e) => {
        const msg = e.data;
        switch (msg.type) {
            case 'detections':
                // Clip to request bbox and append to map source live
                const clipped = msg.features.filter(d =>
                    d.lon >= bbox[0] && d.lon <= bbox[2] && d.lat >= bbox[1] && d.lat <= bbox[3]);
                state.detections.push(...clipped);
                setMapDetections(map, state.detections);
                onUpdate?.(state);
                break;

            case 'progress':
                state.progress = { done: msg.done, total: msg.total };
                onUpdate?.(state);
                break;

            case 'clusters':
                state.clusters = msg.features.filter(c =>
                    c.lon >= bbox[0] && c.lon <= bbox[2] && c.lat >= bbox[1] && c.lat <= bbox[3]);
                setMapDetections(map, state.clusters);
                onUpdate?.(state);
                break;

            case 'error':
                state.error = msg.message;
                state.enhancing = false;
                onUpdate?.(state);
                break;

            case 'done':
                state.enhancing = false;
                saveCache(p.flare_id, state.detections, state.clusters);
                onUpdate?.(state);
                worker?.terminate();
                worker = null;
                break;
        }
    };

    worker.onerror = (err) => {
        state.error = err.message;
        state.enhancing = false;
        onUpdate?.(state);
    };

    // Relaxed thresholds for single-pixel enhance: even 1 detection is informative
    worker.postMessage({ type: 'detect', bbox, start, end, clusterOptions: { minDates: 1, minAvgB12: 0.5 } });
}

export function cancelEnhance(map) {
    if (worker) {
        worker.postMessage({ type: 'cancel' });
        worker.terminate();
        worker = null;
    }
    state = { enhancing: false, progress: null, detections: [], clusters: null, error: null };
    // Clear map source
    const empty = { type: 'FeatureCollection', features: [] };
    map?.getSource('s2-detections')?.setData(empty);
    onUpdate?.(state);
}

export function getState() { return state; }
