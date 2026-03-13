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

    const files = ['flares', 'leases', 'permits', 'plumes', 'detections', 'wells', 'lease_footprints', 'lease_monthly'];
    await Promise.all(files.map(async name => {
        const resp = await fetch(`data/${name}.parquet`);
        const buf = await resp.arrayBuffer();
        await db.registerFileBuffer(`${name}.parquet`, new Uint8Array(buf));
    }));
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
    // Pre-compute operator attribution for all flares (permits+wells scoring)
    await conn.query(`
        CREATE TABLE IF NOT EXISTS flare_operators AS
        WITH permit_ops AS (
            SELECT f.flare_id, p.operator_name, 'permit' AS source,
                COUNT(DISTINCT p.name) AS n_records,
                MIN(111.32 * sqrt(
                    power((p.latitude - f.lat) * cos(radians(f.lat)), 2) +
                    power(p.longitude - f.lon, 2)
                )) AS min_distance_km,
                FIRST(p.name ORDER BY 111.32 * sqrt(
                    power((p.latitude - f.lat) * cos(radians(f.lat)), 2) +
                    power(p.longitude - f.lon, 2)
                )) AS nearest_permit_name
            FROM 'flares.parquet' f
            JOIN 'permits.parquet' p
              ON p.latitude BETWEEN f.lat - 0.0034 AND f.lat + 0.0034
             AND p.longitude BETWEEN f.lon - (0.0034 / cos(radians(f.lat)))
                                 AND f.lon + (0.0034 / cos(radians(f.lat)))
            WHERE 111.32 * sqrt(
                power((p.latitude - f.lat) * cos(radians(f.lat)), 2) +
                power(p.longitude - f.lon, 2)
            ) <= 0.375
            GROUP BY 1, 2
        ),
        well_ops AS (
            SELECT f.flare_id, w.operator_name, 'well' AS source,
                COUNT(DISTINCT w.api) AS n_records,
                MIN(111.32 * sqrt(
                    power((w.latitude - f.lat) * cos(radians(f.lat)), 2) +
                    power(w.longitude - f.lon, 2)
                )) AS min_distance_km
            FROM 'flares.parquet' f
            JOIN 'wells.parquet' w
              ON w.latitude BETWEEN f.lat - 0.0034 AND f.lat + 0.0034
             AND w.longitude BETWEEN f.lon - (0.0034 / cos(radians(f.lat)))
                                 AND f.lon + (0.0034 / cos(radians(f.lat)))
            WHERE w.operator_name IS NOT NULL
              AND 111.32 * sqrt(
                power((w.latitude - f.lat) * cos(radians(f.lat)), 2) +
                power(w.longitude - f.lon, 2)
            ) <= 0.375
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
    let where = '';
    if (operator) {
        await buildOperatorIndex();
        const op = operator.toLowerCase().replace(/'/g, "''");
        where = `JOIN flare_operators fo USING (flare_id) WHERE lower(fo.operator_name) LIKE '%${op}%'`;
    }
    const result = await query(`
        SELECT f.flare_id, f.lat, f.lon, f.detection_days,
            f.total_rh_mw, f.avg_rh_mw,
            f.first_detected, f.last_detected, f.near_excluded_facility
        FROM 'flares.parquet' f ${where}
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
        SELECT date, rh_mw
        FROM 'detections.parquet'
        WHERE flare_id = ${Number(flareId)}
        ORDER BY date
    `);
    return rows(result);
}

export async function queryLeases(flareId) {
    const result = await query(`
        SELECT lease_district, lease_number, oil_gas_code, well_count,
            reported_flared_mcf, lease_operator, lease_name
        FROM 'leases.parquet'
        WHERE flare_id = ${Number(flareId)}
    `);
    return rows(result);
}

export async function queryLeaseMonthly(leaseDistrict, leaseNumber) {
    const ld = leaseDistrict.replace(/'/g, "''");
    const ln = String(leaseNumber).replace(/'/g, "''");
    const result = await query(`
        SELECT date, flared_mcf, produced_mcf
        FROM 'lease_monthly.parquet'
        WHERE lease_district = '${ld}'
          AND lease_number = '${ln}'
        ORDER BY date
    `);
    return rows(result);
}

export async function queryLeaseFootprints() {
    const result = await query(`
        SELECT lease_count, flaring_intensity_pct, leases, geometry
        FROM 'lease_footprints.parquet'
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { geometry, ...props } = r;
            return {
                type: 'Feature',
                geometry: JSON.parse(geometry),
                properties: props
            };
        })
    };
}

export async function queryPermits({ operator } = {}) {
    let where = 'WHERE latitude IS NOT NULL AND longitude IS NOT NULL';
    if (operator) where += ` AND lower(operator_name) LIKE '%${operator.toLowerCase().replace(/'/g, "''")}%'`;
    const result = await query(`
        SELECT latitude, longitude, name, county, district,
            release_type, operator_name,
            count(*) AS n_filings,
            MIN(effective_dt) AS earliest_effective,
            MAX(expiration_dt) AS latest_expiration,
            MAX(release_rate_mcf_day) AS max_release_rate_mcf_day
        FROM 'permits.parquet'
        ${where}
        GROUP BY latitude, longitude, name, county, district,
            release_type, operator_name
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

export async function queryPermitFilings(lat, lon, { radiusKm = 0.375, name, operator } = {}) {
    const { dLat, dLon } = bboxDeltas(lat, radiusKm);
    let where = `latitude BETWEEN ${lat - dLat} AND ${lat + dLat}
          AND longitude BETWEEN ${lon - dLon} AND ${lon + dLon}
          AND 111.32 * sqrt(
              power((latitude - ${lat}) * cos(radians(${lat})), 2) +
              power(longitude - (${lon}), 2)
          ) <= ${radiusKm}`;
    if (name) where += ` AND name = '${name.replace(/'/g, "''")}'`;
    if (operator) where += ` AND operator_name = '${operator.replace(/'/g, "''")}'`;
    const result = await query(`
        SELECT filing_no, name, operator_name, district, county, release_type,
            status, effective_dt, expiration_dt, release_rate_mcf_day,
            exception_reasons, latitude, longitude,
            111.32 * sqrt(
                power((latitude - ${lat}) * cos(radians(${lat})), 2) +
                power(longitude - (${lon}), 2)
            ) AS distance_km
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
    const { dLat, dLon } = bboxDeltas(lat, 0.375);
    const result = await query(`
        WITH permit_ops AS (
            SELECT operator_name, 'permit' AS source,
                COUNT(DISTINCT name) AS n_records,
                MIN(111.32 * sqrt(
                    power((latitude - ${lat}) * cos(radians(${lat})), 2) +
                    power(longitude - (${lon}), 2)
                )) AS min_distance_km,
                FIRST(name ORDER BY 111.32 * sqrt(
                    power((latitude - ${lat}) * cos(radians(${lat})), 2) +
                    power(longitude - (${lon}), 2)
                )) AS nearest_permit_name
            FROM 'permits.parquet'
            WHERE latitude BETWEEN ${lat - dLat} AND ${lat + dLat}
              AND longitude BETWEEN ${lon - dLon} AND ${lon + dLon}
              AND 111.32 * sqrt(
                  power((latitude - ${lat}) * cos(radians(${lat})), 2) +
                  power(longitude - (${lon}), 2)
              ) <= 0.375
            GROUP BY operator_name
        ),
        well_ops AS (
            SELECT operator_name, 'well' AS source,
                COUNT(DISTINCT api) AS n_records,
                MIN(111.32 * sqrt(
                    power((latitude - ${lat}) * cos(radians(${lat})), 2) +
                    power(longitude - (${lon}), 2)
                )) AS min_distance_km
            FROM 'wells.parquet'
            WHERE latitude BETWEEN ${lat - dLat} AND ${lat + dLat}
              AND longitude BETWEEN ${lon - dLon} AND ${lon + dLon}
              AND operator_name IS NOT NULL
              AND 111.32 * sqrt(
                  power((latitude - ${lat}) * cos(radians(${lat})), 2) +
                  power(longitude - (${lon}), 2)
              ) <= 0.375
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

export async function queryNearestPermit(lat, lon, radiusKm = 0.375) {
    const { dLat, dLon } = bboxDeltas(lat, radiusKm);
    const result = await query(`
        SELECT name, operator_name, district, county, release_type,
            effective_dt, expiration_dt,
            latitude, longitude,
            111.32 * sqrt(
                power((latitude - ${lat}) * cos(radians(${lat})), 2) +
                power(longitude - (${lon}), 2)
            ) AS distance_km
        FROM 'permits.parquet'
        WHERE latitude BETWEEN ${lat - dLat} AND ${lat + dLat}
          AND longitude BETWEEN ${lon - dLon} AND ${lon + dLon}
        ORDER BY distance_km
        LIMIT 1
    `);
    const r = rows(result);
    return r.length > 0 ? r[0] : null;
}

export async function queryPlumes() {
    const result = await query(`
        SELECT plume_id, latitude, longitude, source, satellite,
            CAST(date AS VARCHAR) AS date,
            emission_rate, emission_uncertainty, sector
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

export async function queryWells({ operator, bounds } = {}) {
    let where = 'WHERE 1=1';
    if (bounds) {
        where += ` AND latitude BETWEEN ${bounds.south} AND ${bounds.north}`;
        where += ` AND longitude BETWEEN ${bounds.west} AND ${bounds.east}`;
    }
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



