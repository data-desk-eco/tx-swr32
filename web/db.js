let db = null;
let conn = null;
let _initPromise = null;

export async function init() {
    if (_initPromise) return _initPromise;
    _initPromise = _init();
    return _initPromise;
}

async function _init() {
    const duckdb = await import('./vendor/duckdb/duckdb-browser.mjs');
    const base = new URL('.', import.meta.url).href;
    const mainModule = base + 'vendor/duckdb/duckdb-eh.wasm';
    const mainWorker = base + 'vendor/duckdb/duckdb-browser-eh.worker.js';
    const workerBlob = new Blob([`importScripts("${mainWorker}");`], { type: 'text/javascript' });
    const worker = new Worker(URL.createObjectURL(workerBlob));
    db = new duckdb.AsyncDuckDB({ log: () => {} }, worker);
    await db.instantiate(mainModule);
    conn = await db.connect();

    const files = ['flares', 'flare_leases', 'permits', 'plumes', 'detections', 'wells'];
    await Promise.all(files.map(async name => {
        const resp = await fetch(`data/${name}.parquet`);
        const buf = await resp.arrayBuffer();
        await db.registerFileBuffer(`${name}.parquet`, new Uint8Array(buf));
    }));
}

async function query(sql) {
    if (!conn) throw new Error('DB not initialized');
    return conn.query(sql);
}

function rows(result) {
    const out = [];
    for (let i = 0; i < result.numRows; i++) {
        const row = result.get(i);
        const obj = {};
        for (const field of result.schema.fields) {
            const v = row[field.name];
            obj[field.name] = typeof v === 'bigint' ? Number(v) : v;
        }
        out.push(obj);
    }
    return out;
}

export async function queryFlares({ operator } = {}) {
    let where = 'WHERE 1=1';
    if (operator) where += ` AND lower(operator_name) LIKE '%${operator.toLowerCase().replace(/'/g, "''")}%'`;

    const result = await query(`
        SELECT flare_id, lat, lon, detection_days, dark_days, total_days, dark_pct,
            total_rh_mw, avg_rh_mw, operator_name, confidence,
            nearest_permit_km, permit_name, site_name,
            first_detected, last_detected, near_excluded_facility
        FROM 'flares.parquet' ${where}
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [Number(r.lon), Number(r.lat)] },
            properties: r
        }))
    };
}

export async function queryDetections(flareId) {
    const result = await query(`
        SELECT date, rh_mw, is_dark
        FROM 'detections.parquet'
        WHERE flare_id = ${Number(flareId)}
        ORDER BY date
    `);
    return rows(result);
}

export async function queryFlareLeases(flareId) {
    const result = await query(`
        SELECT lease_district, lease_number, oil_gas_code, well_count,
            reported_flared_mcf, unpermitted_flared_mcf,
            permitted_days, total_days, lease_operator, lease_name
        FROM 'flare_leases.parquet'
        WHERE flare_id = ${Number(flareId)}
    `);
    return rows(result);
}

export async function queryPermits({ operator } = {}) {
    let where = 'WHERE latitude IS NOT NULL AND longitude IS NOT NULL';
    if (operator) where += ` AND lower(operator_name) LIKE '%${operator.toLowerCase().replace(/'/g, "''")}%'`;
    const result = await query(`
        SELECT latitude, longitude, name, county, district,
            release_type, operator_name, n_filings,
            CAST(earliest_effective AS VARCHAR) AS earliest_effective,
            CAST(latest_expiration AS VARCHAR) AS latest_expiration,
            max_release_rate_mcf_day, total_permitted_days, exception_reasons
        FROM 'permits.parquet'
        ${where}
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [Number(r.longitude), Number(r.latitude)] },
            properties: r
        }))
    };
}

export async function queryPlumes() {
    const result = await query(`
        SELECT plume_id, latitude, longitude, source, satellite,
            CAST(date AS VARCHAR) AS date,
            emission_rate, emission_uncertainty, sector, classification,
            operator_name, vnf_flare_id, vnf_distance_km
        FROM 'plumes.parquet'
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [Number(r.longitude), Number(r.latitude)] },
            properties: r
        }))
    };
}

export async function queryWells({ operator } = {}) {
    let where = 'WHERE 1=1';
    if (operator) where += ` AND lower(operator_name) LIKE '%${operator.toLowerCase().replace(/'/g, "''")}%'`;
    const result = await query(`
        SELECT api, oil_gas_code, lease_district, lease_number, well_number,
            operator_name, latitude, longitude
        FROM 'wells.parquet'
        ${where}
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [Number(r.longitude), Number(r.latitude)] },
            properties: r
        }))
    };
}

