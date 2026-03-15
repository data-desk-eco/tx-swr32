let db = null;
let conn = null;
let _initPromise = null;
const _loaded = new Set();
const _loading = new Map();
let _log = () => {}; // boot log callback
let _status = () => {}; // boot status callback
export function onLog(fn) { _log = fn; }
export function onStatus(fn) { _status = fn; }

const MATCH_RADIUS_KM = 0.375;
const BBOX_DELTA = 0.0034; // ~375m in degrees latitude

// Reusable SQL fragment for great-circle distance (km) between two lat/lon pairs
function distSql(latA, lonA, latB, lonB) {
    return `111.32 * sqrt(power((${latA} - ${latB}) * cos(radians(${latB})), 2) + power(${lonA} - ${lonB}, 2))`;
}

// Reusable SQL fragment for bounding-box pre-filter
function bboxSql(latCol, lonCol, lat, lon) {
    return `${latCol} BETWEEN ${lat} - ${BBOX_DELTA} AND ${lat} + ${BBOX_DELTA}
             AND ${lonCol} BETWEEN ${lon} - (${BBOX_DELTA} / cos(radians(${lat}))) AND ${lon} + (${BBOX_DELTA} / cos(radians(${lat})))`;
}

// Tier 0: needed for first paint (37K flares)
// Tier 1: visible layers loaded right after first paint (~750K total)
// Tier 2: deferred until first query (detections 667K, gatherers 308K, production 288K, leases 44K)
const TIER0 = ['flares'];
const TIER1 = ['permits', 'plumes', 'facilities', 'wells'];

function _fmtSize(bytes) {
    return bytes < 1024 ? bytes + ' B'
        : bytes < 1048576 ? (bytes / 1024).toFixed(0) + ' KB'
        : (bytes / 1048576).toFixed(1) + ' MB';
}

async function _loadParquet(name) {
    if (_loaded.has(name)) return;
    if (_loading.has(name)) return _loading.get(name);
    const p = (async () => {
        _log(`fetch  data/${name}.parquet`);
        const resp = await fetch(`data/${name}.parquet`);
        const buf = await resp.arrayBuffer();
        _log(`load   ${name}.parquet (${_fmtSize(buf.byteLength)})`);
        await db.registerFileBuffer(`${name}.parquet`, new Uint8Array(buf));
        _loaded.add(name);
    })();
    _loading.set(name, p);
    return p;
}

// Ensure a parquet is loaded before querying it — no-ops if already loaded
async function need(...names) {
    await Promise.all(names.map(n => _loadParquet(n)));
}

export async function init() {
    if (_initPromise) return _initPromise;
    _initPromise = _init();
    return _initPromise;
}

// Start fetching tier 0 parquets immediately (before WASM is even requested)
const _prefetched = new Map();
for (const name of TIER0) {
    _prefetched.set(name, fetch(`data/${name}.parquet`).then(r => r.arrayBuffer()));
}

async function _init() {
    _status('importing duckdb module...');
    _log('import duckdb-browser.mjs');
    const duckdb = await import('./vendor/duckdb/duckdb-browser.mjs');
    _log('import duckdb-browser.mjs done');
    const base = new URL('.', import.meta.url).href;
    const mainModule = base + 'vendor/duckdb/duckdb-eh.wasm';
    const mainWorker = base + 'vendor/duckdb/duckdb-browser-eh.worker.js';
    _log('spawn  duckdb worker');
    const workerBlob = new Blob([`importScripts("${mainWorker}");`], { type: 'text/javascript' });
    const worker = new Worker(URL.createObjectURL(workerBlob));
    db = new duckdb.AsyncDuckDB({ log: () => {} }, worker);
    _status('downloading duckdb wasm (34 MB)...');
    _log('fetch  duckdb-eh.wasm (34 MB)');
    await db.instantiate(mainModule);
    _status('starting wasm runtime...');
    _log('instantiate wasm runtime');
    conn = await db.connect();
    _log('connect to duckdb');
    _status('loading flare data...');
    // Register prefetched tier 0 parquets (fetches started at module load)
    await Promise.all(TIER0.map(async n => {
        const buf = await _prefetched.get(n);
        _log(`load   ${n}.parquet (${_fmtSize(buf.byteLength)})`);
        await db.registerFileBuffer(`${n}.parquet`, new Uint8Array(buf));
        _loaded.add(n);
    }));
}

// Load tier 1 files in background (call after first paint)
export function loadTier1() {
    TIER1.forEach(n => _loadParquet(n));
}

function bboxDeltas(lat, radiusKm) {
    return {
        dLat: radiusKm / 110.54,
        dLon: radiusKm / (111.32 * Math.cos(lat * Math.PI / 180)),
    };
}

let _indexReady = false;
let _indexPromise = null;

export function isOperatorIndexReady() { return _indexReady; }

export async function buildOperatorIndex() {
    if (_indexPromise) return _indexPromise;
    _indexPromise = _buildOperatorIndex();
    return _indexPromise;
}

async function _buildOperatorIndex() {
    await need('permits', 'wells');
    const dist = distSql('p.latitude', 'p.longitude', 'f.lat', 'f.lon');
    const wDist = distSql('w.latitude', 'w.longitude', 'f.lat', 'f.lon');
    await conn.query(`
        CREATE TABLE IF NOT EXISTS flare_operators AS
        WITH permit_ops AS (
            SELECT f.flare_id, p.operator_name, 'permit' AS source,
                COUNT(DISTINCT p.name) AS n_records,
                MIN(${dist}) AS min_distance_km,
                FIRST(p.name ORDER BY ${dist}) AS nearest_permit_name
            FROM 'flares.parquet' f
            JOIN 'permits.parquet' p
              ON ${bboxSql('p.latitude', 'p.longitude', 'f.lat', 'f.lon')}
            WHERE ${dist} <= ${MATCH_RADIUS_KM}
            GROUP BY 1, 2
        ),
        well_ops AS (
            SELECT f.flare_id, w.operator_name, 'well' AS source,
                COUNT(DISTINCT w.api) AS n_records,
                MIN(${wDist}) AS min_distance_km
            FROM 'flares.parquet' f
            JOIN 'wells.parquet' w
              ON ${bboxSql('w.latitude', 'w.longitude', 'f.lat', 'f.lon')}
            WHERE w.operator_name IS NOT NULL
              AND ${wDist} <= ${MATCH_RADIUS_KM}
            GROUP BY 1, 2
        ),
        all_ops AS (
            SELECT flare_id, operator_name, SUM(n_records) AS total_records,
                MIN(min_distance_km) AS min_distance_km,
                bool_or(source = 'permit') AS has_permit
            FROM (
                SELECT flare_id, operator_name, source, n_records, min_distance_km FROM permit_ops
                UNION ALL
                SELECT flare_id, operator_name, source, n_records, min_distance_km FROM well_ops
            ) combined
            GROUP BY 1, 2
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY flare_id
                ORDER BY has_permit DESC, total_records DESC, min_distance_km
            ) AS rn
            FROM all_ops
        ),
        best AS (SELECT * FROM ranked WHERE rn = 1),
        agg AS (
            SELECT b.flare_id,
                COUNT(DISTINCT a.operator_name) AS n_operators,
                SUM(CASE WHEN a.operator_name = b.operator_name THEN a.total_records ELSE 0 END) * 1.0
                  / SUM(a.total_records) AS own_share
            FROM best b JOIN all_ops a USING (flare_id)
            GROUP BY 1
        ),
        nearest_permit AS (
            SELECT DISTINCT ON (flare_id) flare_id,
                nearest_permit_name AS permit_name,
                min_distance_km AS nearest_permit_km
            FROM permit_ops
            ORDER BY flare_id, min_distance_km
        )
        SELECT b.flare_id, b.operator_name,
            CASE WHEN a.n_operators = 1 THEN 'sole'
                 WHEN a.own_share > 0.5 THEN 'majority'
                 ELSE 'contested'
            END AS confidence,
            np.permit_name, np.nearest_permit_km
        FROM best b
        LEFT JOIN agg a USING (flare_id)
        LEFT JOIN nearest_permit np USING (flare_id)
    `);
    _indexReady = true;
}

async function query(sql) {
    if (!conn) throw new Error('DB not initialized');
    return conn.query(sql);
}

function rows(result) {
    const n = result.numRows;
    if (n === 0) return [];
    const fields = result.schema.fields;
    // Column-based extraction — reads each column array once instead of per-row proxy access
    const columns = fields.map(f => {
        const col = result.getChild(f.name);
        const arr = col.toArray();
        return { name: f.name, arr, bigint: arr.length > 0 && typeof arr[0] === 'bigint' };
    });
    const out = new Array(n);
    for (let i = 0; i < n; i++) {
        const obj = {};
        for (const { name, arr, bigint } of columns) {
            const v = arr[i];
            obj[name] = bigint ? Number(v) : v;
        }
        out[i] = obj;
    }
    return out;
}

export async function queryFlares({ operator } = {}) {
    let where = '';
    if (operator) {
        await buildOperatorIndex();
        const op = operator.toLowerCase().replace(/'/g, "''");
        where = `JOIN flare_operators fo USING (flare_id) WHERE lower(fo.operator_name) LIKE '%${op}%'`;
    }
    const result = await query(`
        SELECT f.flare_id, f.lat AS _lat, f.lon AS _lon,
            round(f.lat, 2) AS lat, round(f.lon, 2) AS lon,
            f.detection_days, f.total_rh_mw, f.avg_rh_mw,
            f.first_detected, f.last_detected
        FROM 'flares.parquet' f ${where}
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}

export async function queryDetections(flareId) {
    await need('detections');
    const result = await query(`
        SELECT date, rh_mw
        FROM 'detections.parquet'
        WHERE flare_id = ${Number(flareId)}
        ORDER BY date
    `);
    return rows(result);
}

export async function queryLeases(flareId) {
    await need('leases');
    const result = await query(`
        SELECT lease_district, lease_number, oil_gas_code, well_count,
            reported_flared_mcf, lease_operator, lease_name
        FROM 'leases.parquet'
        WHERE flare_id = ${Number(flareId)}
    `);
    return rows(result);
}

export async function queryLeaseMonthly(leaseDistrict, leaseNumber) {
    await need('production');
    const ld = leaseDistrict.replace(/'/g, "''");
    const ln = String(leaseNumber).replace(/'/g, "''");
    const result = await query(`
        SELECT date, flared_mcf, produced_mcf
        FROM 'production.parquet'
        WHERE lease_district = '${ld}'
          AND lease_number = '${ln}'
        ORDER BY date
    `);
    return rows(result);
}

export async function queryPermits({ operator } = {}) {
    await need('permits');
    let where = 'WHERE latitude IS NOT NULL AND longitude IS NOT NULL';
    if (operator) where += ` AND lower(operator_name) LIKE '%${operator.toLowerCase().replace(/'/g, "''")}%'`;
    const result = await query(`
        SELECT latitude AS _lat, longitude AS _lon,
            round(latitude, 2) AS latitude, round(longitude, 2) AS longitude,
            name, county, district,
            release_type, operator_name,
            count(*) AS n_filings,
            MIN(effective_dt) AS earliest_effective,
            MAX(expiration_dt) AS latest_expiration,
            MAX(release_rate_mcf_day) AS max_release_rate_mcf_day
        FROM 'permits.parquet'
        ${where}
        GROUP BY _lat, _lon, name, county, district,
            release_type, operator_name
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}

export async function queryPermitFilings(lat, lon, { radiusKm = MATCH_RADIUS_KM, name, operator } = {}) {
    await need('permits');
    const dist = distSql('latitude', 'longitude', lat, lon);
    const bbox = bboxSql('latitude', 'longitude', lat, lon);
    let where = `${bbox} AND ${dist} <= ${radiusKm}`;
    if (name) where += ` AND name = '${name.replace(/'/g, "''")}'`;
    if (operator) where += ` AND operator_name = '${operator.replace(/'/g, "''")}'`;
    const result = await query(`
        SELECT filing_no, name, operator_name, district, county, release_type,
            status, effective_dt, expiration_dt, release_rate_mcf_day,
            exception_reasons, latitude, longitude,
            ${dist} AS distance_km
        FROM 'permits.parquet'
        WHERE ${where}
        ORDER BY effective_dt
    `);
    return rows(result);
}

export async function queryOperator(flareId, lat, lon) {
    if (_indexReady) {
        const result = await query(`
            SELECT operator_name, confidence, permit_name,
                nearest_permit_km
            FROM flare_operators
            WHERE flare_id = ${Number(flareId)}
        `);
        const r = rows(result);
        return r.length > 0 ? r[0] : null;
    }
    // Index still building — fast single-point lookup
    return queryOperatorByLocation(lat, lon);
}

export async function queryOperatorByLocation(lat, lon) {
    await need('permits', 'wells');
    const dist = distSql('latitude', 'longitude', lat, lon);
    const bbox = bboxSql('latitude', 'longitude', lat, lon);
    const result = await query(`
        WITH permit_ops AS (
            SELECT operator_name, 'permit' AS source,
                COUNT(DISTINCT name) AS n_records,
                MIN(${dist}) AS min_distance_km,
                FIRST(name ORDER BY ${dist}) AS nearest_permit_name
            FROM 'permits.parquet'
            WHERE ${bbox} AND ${dist} <= ${MATCH_RADIUS_KM}
            GROUP BY operator_name
        ),
        well_ops AS (
            SELECT operator_name, 'well' AS source,
                COUNT(DISTINCT api) AS n_records,
                MIN(${dist}) AS min_distance_km
            FROM 'wells.parquet'
            WHERE ${bbox} AND operator_name IS NOT NULL
              AND ${dist} <= ${MATCH_RADIUS_KM}
            GROUP BY operator_name
        ),
        all_ops AS (
            SELECT operator_name, SUM(n_records) AS total_records,
                MIN(min_distance_km) AS min_distance_km,
                bool_or(source = 'permit') AS has_permit
            FROM (
                SELECT operator_name, source, n_records, min_distance_km FROM permit_ops
                UNION ALL
                SELECT operator_name, source, n_records, min_distance_km FROM well_ops
            ) combined
            GROUP BY operator_name
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                ORDER BY has_permit DESC, total_records DESC, min_distance_km
            ) AS rn
            FROM all_ops
        ),
        best AS (SELECT * FROM ranked WHERE rn = 1),
        agg AS (
            SELECT COUNT(DISTINCT a.operator_name) AS n_operators,
                SUM(CASE WHEN a.operator_name = b.operator_name THEN a.total_records ELSE 0 END) * 1.0
                  / SUM(a.total_records) AS own_share
            FROM best b, all_ops a
        ),
        nearest_permit AS (
            SELECT nearest_permit_name, min_distance_km AS nearest_permit_km
            FROM permit_ops
            ORDER BY min_distance_km
            LIMIT 1
        )
        SELECT b.operator_name,
            CASE WHEN a.n_operators = 1 THEN 'sole'
                 WHEN a.own_share > 0.5 THEN 'majority'
                 ELSE 'contested'
            END AS confidence,
            np.nearest_permit_name AS permit_name,
            np.nearest_permit_km
        FROM best b, agg a
        LEFT JOIN nearest_permit np ON true
    `);
    const r = rows(result);
    return r.length > 0 ? r[0] : null;
}

export async function queryNearbyPermits(lat, lon, radiusKm = 0.75) {
    await need('permits');
    const { dLat, dLon } = bboxDeltas(lat, radiusKm);
    const result = await query(`
        SELECT name, operator_name, district, county, release_type,
            effective_dt, expiration_dt, latitude, longitude
        FROM 'permits.parquet'
        WHERE latitude BETWEEN ${lat - dLat} AND ${lat + dLat}
          AND longitude BETWEEN ${lon - dLon} AND ${lon + dLon}
    `);
    return rows(result);
}

export async function queryNearestPermit(lat, lon, radiusKm = MATCH_RADIUS_KM) {
    await need('permits');
    const dist = distSql('latitude', 'longitude', lat, lon);
    const bbox = bboxSql('latitude', 'longitude', lat, lon);
    const result = await query(`
        SELECT name, operator_name, district, county, release_type,
            effective_dt, expiration_dt,
            latitude, longitude,
            ${dist} AS distance_km
        FROM 'permits.parquet'
        WHERE ${bbox}
        ORDER BY distance_km
        LIMIT 1
    `);
    const r = rows(result);
    return r.length > 0 ? r[0] : null;
}

export async function queryFacilities() {
    await need('facilities');
    const result = await query(`
        SELECT serial_number, facility_name, plant_type,
            latitude AS _lat, longitude AS _lon,
            round(latitude, 2) AS latitude, round(longitude, 2) AS longitude
        FROM 'facilities.parquet'
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}

export async function queryPlumes() {
    await need('plumes');
    const result = await query(`
        SELECT plume_id, latitude AS _lat, longitude AS _lon,
            round(latitude, 2) AS latitude, round(longitude, 2) AS longitude,
            source, satellite,
            CAST(date AS VARCHAR) AS date,
            emission_rate, emission_uncertainty, sector
        FROM 'plumes.parquet'
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}

export async function queryNearbyFacilities(lat, lon, radiusKm = 5) {
    await need('facilities');
    const { dLat, dLon } = bboxDeltas(lat, radiusKm);
    const dist = distSql('latitude', 'longitude', lat, lon);
    const result = await query(`
        SELECT serial_number, facility_name, plant_type, latitude, longitude,
            ${dist} AS distance_km
        FROM 'facilities.parquet'
        WHERE latitude BETWEEN ${lat - dLat} AND ${lat + dLat}
          AND longitude BETWEEN ${lon - dLon} AND ${lon + dLon}
          AND ${dist} <= ${radiusKm}
        ORDER BY distance_km
    `);
    return rows(result);
}

export async function queryGatherers(leaseDistrict, leaseNumber) {
    await need('gatherers');
    const ld = leaseDistrict.replace(/'/g, "''");
    const ln = String(leaseNumber).replace(/'/g, "''");
    const result = await query(`
        SELECT type, gpn_name, percentage, is_current, first_date, last_date
        FROM 'gatherers.parquet'
        WHERE district = '${ld}'
          AND lease_number = LPAD('${ln}', 6, '0')
        ORDER BY is_current DESC, last_date DESC NULLS LAST, type, percentage DESC
    `);
    return rows(result);
}

export async function queryWells({ operator, bounds } = {}) {
    await need('wells');
    const conditions = [];
    if (bounds) {
        conditions.push(`latitude BETWEEN ${bounds.south} AND ${bounds.north}`);
        conditions.push(`longitude BETWEEN ${bounds.west} AND ${bounds.east}`);
    }
    if (operator) conditions.push(`lower(operator_name) LIKE '%${operator.toLowerCase().replace(/'/g, "''")}%'`);
    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';
    const result = await query(`
        SELECT api, oil_gas_code, lease_district, lease_number, well_number,
            operator_name, latitude AS _lat, longitude AS _lon,
            round(latitude, 2) AS latitude, round(longitude, 2) AS longitude,
            flared_mcf, produced_mcf, flaring_intensity_pct, lease_name
        FROM 'wells.parquet'
        ${where}
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}
