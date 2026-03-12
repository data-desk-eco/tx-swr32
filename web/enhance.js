// enhance.js — Sentinel-2 flare enhancement for gaslight
// Spawns s2-flares Web Worker, streams detections to map, clusters incrementally.

let worker = null;
let state = { enhancing: false, progress: null, detections: [], clusters: null, error: null };
let onUpdate = null;  // callback for UI updates

const CACHE_PREFIX = 's2:';
const clusterIndex = new Map(); // id → cluster object (with detections)

function clusterHash(lat, lon) {
    const s = `${lat.toFixed(4)},${lon.toFixed(4)}`;
    let h = 0;
    for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
    return (h >>> 0).toString(36);
}

function cacheKey(flareId) { return CACHE_PREFIX + flareId; }

function saveCache(flareId, detections, clusters, processedDates, complete = false) {
    const data = JSON.stringify({ detections, clusters: clusters ?? null, processedDates: [...processedDates], complete });
    const key = cacheKey(flareId);
    try {
        localStorage.setItem(key, data);
    } catch {
        // Quota exceeded — evict oldest s2 entries and retry
        const s2Keys = [];
        for (let i = 0; i < localStorage.length; i++) {
            const k = localStorage.key(i);
            if (k?.startsWith(CACHE_PREFIX)) s2Keys.push(k);
        }
        const toRemove = Math.max(1, Math.ceil(s2Keys.length / 2));
        for (let i = 0; i < toRemove && i < s2Keys.length; i++) {
            localStorage.removeItem(s2Keys[i]);
        }
        try { localStorage.setItem(key, data); } catch { /* still full */ }
    }
}

function loadCache(flareId) {
    try {
        const raw = localStorage.getItem(cacheKey(flareId));
        return raw ? JSON.parse(raw) : null;
    } catch { return null; }
}

// Load all cached S2 clusters from localStorage — returns flat array with flare_id
// Also rebuilds clusterIndex for lookups by id
export function loadAllCached() {
    const all = [];
    clusterIndex.clear();
    for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (!k?.startsWith(CACHE_PREFIX)) continue;
        const flareId = k.slice(CACHE_PREFIX.length);
        try {
            const cached = JSON.parse(localStorage.getItem(k));
            const clusters = cached.clusters || [];
            for (const c of clusters) {
                // Backfill id for old cache entries that predate cluster hashing
                if (!c.id) c.id = clusterHash(c.lat, c.lon);
                const enriched = { ...c, flare_id: Number(flareId) };
                all.push(enriched);
                clusterIndex.set(enriched.id, enriched);
            }
        } catch { /* skip corrupt entries */ }
    }
    return all;
}

// Look up a cluster by id (from cache or live state)
export function getCluster(id) { return clusterIndex.get(id); }

export function setUpdateCallback(fn) { onUpdate = fn; }

let activeFlareId = null;

function refreshS2Source(map) {
    // Merge global cached S2 clusters with live clusters for the active flare
    const cached = loadAllCached(); // rebuilds clusterIndex with cached data
    // Remove cached entries for active flare (replaced by live state)
    const filtered = activeFlareId != null
        ? cached.filter(d => d.flare_id !== activeFlareId)
        : cached;
    // Add live clusters for active flare
    const live = activeFlareId != null && state.clusters
        ? state.clusters.map(d => {
            const enriched = { ...d, flare_id: activeFlareId };
            clusterIndex.set(d.id, enriched);
            return enriched;
        })
        : [];
    const all = [...filtered, ...live];
    const fc = {
        type: 'FeatureCollection',
        features: all.map(d => ({
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
    map.getSource('s2-detections')?.setData(fc);
}

export function enhance(flare, map) {
    cancelEnhance(map);

    const p = flare.properties;
    activeFlareId = Number(p.flare_id);
    const lon = Number(p.lon);
    const lat = Number(p.lat);

    // 750m pixel bbox (same math as flarePixelData in app.js)
    const dLat = 375 / 110540;
    const dLon = 375 / (111320 * Math.cos(lat * Math.PI / 180));
    const bbox = [lon - dLon, lat - dLat, lon + dLon, lat + dLat];

    // Zoom to pixel square
    map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 80, maxZoom: 17 });

    // Check cache — if fully complete, use directly; otherwise seed and continue
    const cached = loadCache(p.flare_id);
    if (cached?.complete && cached?.clusters) {
        // Backfill ids for old cache entries
        for (const c of cached.clusters) {
            if (!c.id) c.id = clusterHash(c.lat, c.lon);
        }
        state = { enhancing: false, progress: null, detections: cached.detections, clusters: cached.clusters, error: null };
        refreshS2Source(map);
        onUpdate?.(state);
        return;
    }

    // Seed with cached data (partial or old-format complete without flag)
    const cachedDetections = cached?.detections || [];
    const cachedClusters = cached?.clusters || null;
    if (cachedClusters) {
        for (const c of cachedClusters) {
            if (!c.id) c.id = clusterHash(c.lat, c.lon);
        }
    }
    const processedDates = new Set(cached?.processedDates || []);

    const end = p.last_detected;
    // Cap to last year to keep image count manageable
    const oneYearBefore = new Date(new Date(end).getTime() - 365 * 86400000).toISOString().slice(0, 10);
    const start = p.first_detected > oneYearBefore ? p.first_detected : oneYearBefore;

    state = { enhancing: true, progress: { done: 0, total: null }, detections: [...cachedDetections], clusters: cachedClusters, error: null };
    refreshS2Source(map);
    onUpdate?.(state);

    // Module worker — ES module imports work natively
    worker = new Worker('vendor/s2-flares/worker.js', { type: 'module' });

    worker.onmessage = (e) => {
        const msg = e.data;
        switch (msg.type) {
            case 'detections':
                // Clip to request bbox and append
                const clipped = msg.features.filter(d =>
                    d.lon >= bbox[0] && d.lon <= bbox[2] && d.lat >= bbox[1] && d.lat <= bbox[3]);
                state.detections.push(...clipped);
                onUpdate?.(state);
                break;

            case 'image-done':
                // Track all processed dates (including those with no detections)
                processedDates.add(msg.date);
                saveCache(p.flare_id, state.detections, state.clusters, processedDates);
                break;

            case 'progress':
                state.progress = { done: msg.done, total: msg.total, skipped: msg.skipped || 0 };
                onUpdate?.(state);
                break;

            case 'clusters':
                state.clusters = msg.features.filter(c =>
                    c.lon >= bbox[0] && c.lon <= bbox[2] && c.lat >= bbox[1] && c.lat <= bbox[3]);
                refreshS2Source(map);
                onUpdate?.(state);
                break;

            case 'error':
                state.error = msg.message;
                state.enhancing = false;
                onUpdate?.(state);
                break;

            case 'done':
                state.enhancing = false;
                saveCache(p.flare_id, state.detections, state.clusters, processedDates, true);
                refreshS2Source(map);
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
    const skipDates = processedDates.size > 0 ? [...processedDates] : undefined;
    const priorDetections = cachedDetections.length > 0 ? cachedDetections : undefined;
    worker.postMessage({ type: 'detect', bbox, start, end, clusterOptions: { minDates: 1, minAvgB12: 0.5 }, skipDates, priorDetections });
}

export function cancelEnhance(map) {
    if (worker) {
        worker.postMessage({ type: 'cancel' });
        worker.terminate();
        worker = null;
    }
    state = { enhancing: false, progress: null, detections: [], clusters: null, error: null };
    activeFlareId = null;
    // Restore global cached S2 data
    refreshS2Source(map);
    onUpdate?.(state);
}

export function getState() { return state; }
export function isEnhancing() { return state.enhancing; }
