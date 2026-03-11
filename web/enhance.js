// enhance.js — Sentinel-2 flare enhancement for gaslight
// Spawns s2-flares Web Worker, streams detections to map, runs clustering on completion.

let worker = null;
let state = { enhancing: false, progress: null, clusters: null, error: null };
let onUpdate = null;  // callback for UI updates

export function setUpdateCallback(fn) { onUpdate = fn; }

export function enhance(flare, map) {
    cancelEnhance(map);

    const p = flare.properties;
    const lon = Number(p.lon);
    const lat = Number(p.lat);

    // 750m pixel bbox (same math as flarePixelData in app.js)
    const dLat = 375 / 110540;
    const dLon = 375 / (111320 * Math.cos(lat * Math.PI / 180));
    const bbox = [lon - dLon, lat - dLat, lon + dLon, lat + dLat];

    const end = p.last_detected;
    // Cap to last year to keep image count manageable
    const oneYearBefore = new Date(new Date(end).getTime() - 365 * 86400000).toISOString().slice(0, 10);
    const start = p.first_detected > oneYearBefore ? p.first_detected : oneYearBefore;

    state = { enhancing: true, progress: { done: 0, total: null }, clusters: null, error: null };
    onUpdate?.(state);

    // Module worker — ES module imports work natively
    worker = new Worker('vendor/s2-flares/worker.js', { type: 'module' });
    const allDetections = [];

    worker.onmessage = (e) => {
        const msg = e.data;
        switch (msg.type) {
            case 'detections':
                // Append to map source live
                allDetections.push(...msg.features);
                const fc = {
                    type: 'FeatureCollection',
                    features: allDetections.map(d => ({
                        type: 'Feature',
                        geometry: { type: 'Point', coordinates: [d.lon, d.lat] },
                        properties: d,
                    })),
                };
                map.getSource('s2-detections')?.setData(fc);
                break;

            case 'progress':
                state.progress = { done: msg.done, total: msg.total };
                onUpdate?.(state);
                break;

            case 'clusters':
                state.clusters = msg.features;
                // Replace raw detections with clusters on map
                const clusterFc = {
                    type: 'FeatureCollection',
                    features: msg.features.map(c => ({
                        type: 'Feature',
                        geometry: { type: 'Point', coordinates: [c.lon, c.lat] },
                        properties: c,
                    })),
                };
                map.getSource('s2-detections')?.setData(clusterFc);
                onUpdate?.(state);
                break;

            case 'error':
                state.error = msg.message;
                state.enhancing = false;
                onUpdate?.(state);
                break;

            case 'done':
                state.enhancing = false;
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

    // Zoom to pixel square if not already in view
    map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 80, maxZoom: 17 });
}

export function cancelEnhance(map) {
    if (worker) {
        worker.postMessage({ type: 'cancel' });
        worker.terminate();
        worker = null;
    }
    state = { enhancing: false, progress: null, clusters: null, error: null };
    // Clear map source
    const empty = { type: 'FeatureCollection', features: [] };
    map?.getSource('s2-detections')?.setData(empty);
    onUpdate?.(state);
}

export function getState() { return state; }
