-- dark_flaring.sql: Identify VNF detections without valid SWR 32 permits
LOAD spatial;

-- Step 1: Match VNF detections to nearest well (within 750m ≈ 0.0075°)
CREATE OR REPLACE TABLE vnf_matched AS
WITH nearest AS (
    SELECT
        v.flare_id,
        v.date,
        v.rh_mw,
        v.temp_k,
        v.lat AS vnf_lat,
        v.lon AS vnf_lon,
        w.api,
        w.oil_gas_code,
        w.lease_district,
        w.lease_number,
        w.well_number,
        ST_Distance_Sphere(v.geom, w.geom) / 1000.0 AS distance_km,
        ROW_NUMBER() OVER (
            PARTITION BY v.flare_id, v.date
            ORDER BY ST_Distance_Sphere(v.geom, w.geom)
        ) AS rn
    FROM vnf v
    JOIN wells w ON w.geom IS NOT NULL
        AND w.longitude BETWEEN v.lon - 0.015 AND v.lon + 0.015
        AND w.latitude  BETWEEN v.lat - 0.015 AND v.lat + 0.015
        AND ST_DWithin(v.geom, w.geom, 0.0075)
    WHERE v.detected = true
)
SELECT * EXCLUDE (rn) FROM nearest WHERE rn = 1;

-- Step 2: Join to permits — find valid SWR 32 coverage
CREATE OR REPLACE TABLE dark_flares AS
SELECT
    m.*,
    o.operator_name,
    p.filing_no AS permit_filing_no,
    p.status AS permit_status,
    p.effective_dt AS permit_effective,
    p.expiration_dt AS permit_expiration,
    p.property AS permit_property,
    CASE WHEN p.filing_no IS NOT NULL THEN false ELSE true END AS is_dark
FROM vnf_matched m
LEFT JOIN operators o
    ON LPAD(o.operator_number, 6, '0') = (
        -- Get operator from well via lease lookup
        -- For now, join via the permit's operator since we have operator_no there
        SELECT LPAD(CAST(pp.operator_no AS VARCHAR), 6, '0')
        FROM permits pp
        WHERE pp.lease_district = m.lease_district
          AND pp.lease_number = m.lease_number
        LIMIT 1
    )
LEFT JOIN permits p
    ON p.lease_district = m.lease_district
    AND p.lease_number = m.lease_number
    AND p.status = 'Approved'
    AND TRY_STRPTIME(p.effective_dt, '%m/%d/%Y') <= m.date
    AND (p.expiration_dt = ''
         OR TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y') >= m.date);

-- Summary view
CREATE OR REPLACE VIEW dark_flaring_summary AS
SELECT
    is_dark,
    count(*) AS detection_days,
    count(DISTINCT flare_id) AS flare_sites,
    round(avg(rh_mw), 2) AS avg_rh_mw,
    round(sum(rh_mw), 0) AS total_rh_mw,
    min(date) AS earliest,
    max(date) AS latest
FROM dark_flares
GROUP BY is_dark;

-- Top dark flare sites by cumulative radiant heat
CREATE OR REPLACE VIEW top_dark_flares AS
SELECT
    flare_id,
    lease_district,
    lease_number,
    operator_name,
    vnf_lat,
    vnf_lon,
    count(*) AS detection_days,
    round(sum(rh_mw), 1) AS total_rh_mw,
    min(date) AS first_seen,
    max(date) AS last_seen
FROM dark_flares
WHERE is_dark = true
GROUP BY flare_id, lease_district, lease_number, operator_name, vnf_lat, vnf_lon
ORDER BY total_rh_mw DESC;
