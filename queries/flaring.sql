LOAD spatial;

SET VARIABLE permit_radius = 1000;
SET VARIABLE plume_radius = 1000;
SET VARIABLE start_date = '2021-01-01'::DATE;
SET VARIABLE lat_min = 30.0;
SET VARIABLE lat_max = 33.5;
SET VARIABLE lon_min = -104.5;
SET VARIABLE lon_max = -100.0;

-- VNF flare sites: one row per site with exclusion flag
CREATE OR REPLACE TABLE flaring.sites AS
SELECT
    f.flare_id, f.lat, f.lon, f.geom,
    f.first_detected, f.last_detected, f.detection_days,
    EXISTS (
        SELECT 1 FROM raw.excluded_facilities ef
        WHERE ef.geom IS NOT NULL
          AND ef.longitude BETWEEN f.lon - 0.015 AND f.lon + 0.015
          AND ef.latitude  BETWEEN f.lat - 0.015 AND f.lat + 0.015
    ) AS near_excluded_facility
FROM (
    SELECT flare_id,
        AVG(lat) AS lat, AVG(lon) AS lon,
        ST_Point(AVG(lon), AVG(lat)) AS geom,
        MIN(date) AS first_detected, MAX(date) AS last_detected,
        COUNT(*) AS detection_days
    FROM raw.vnf WHERE detected AND date >= getvariable('start_date')
    GROUP BY flare_id
) f;

CREATE INDEX idx_sites_geom ON flaring.sites USING RTREE (geom);

-- Upstream permit locations in the Permian (excludes Gas Plant)
CREATE OR REPLACE TABLE flaring.permit_locations AS
SELECT fl.*
FROM raw.flare_locations fl
WHERE fl.filing_no NOT IN (SELECT filing_no FROM raw.permits WHERE property_type = 'Gas Plant')
  AND COALESCE(fl.facility_type, '') NOT ILIKE '%gas plant%'
  AND fl.latitude BETWEEN getvariable('lat_min') AND getvariable('lat_max')
  AND fl.longitude BETWEEN getvariable('lon_min') AND getvariable('lon_max');

CREATE INDEX idx_permit_loc_geom ON flaring.permit_locations USING RTREE (geom);

-- Site ↔ permit matches within 1km
CREATE OR REPLACE TABLE flaring.site_permit_matches AS
SELECT
    f.flare_id, fl.filing_no,
    ST_Distance_Sphere(f.geom, fl.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY f.flare_id ORDER BY ST_Distance_Sphere(f.geom, fl.geom)) AS rank
FROM flaring.sites f
JOIN flaring.permit_locations fl ON fl.geom IS NOT NULL
    AND fl.longitude BETWEEN f.lon - 0.03 AND f.lon + 0.03
    AND fl.latitude  BETWEEN f.lat - 0.03 AND f.lat + 0.03
    AND ST_Distance_Sphere(f.geom, fl.geom) < getvariable('permit_radius')
WHERE NOT f.near_excluded_facility;

-- Permit coverage with date validation
-- Includes Submitted/Hearing Pending per Earthworks methodology
CREATE OR REPLACE TABLE flaring.site_permit_coverage AS
SELECT
    sm.flare_id, sm.filing_no, sm.distance_km, sm.rank,
    p.operator_name, p.operator_no,
    p.property, p.property_type, p.lease_district, p.lease_number,
    p.status AS permit_status, p.effective_dt, p.expiration_dt,
    p.filing_type, p.site_name, p.exception_reasons
FROM flaring.site_permit_matches sm
JOIN rrc.permits p ON p.filing_no = sm.filing_no
WHERE p.status IN ('Approved', 'Submitted', 'Hearing Pending', 'Resubmitted')
  AND p.property_type != 'Gas Plant';

-- Operator attribution per site (from nearest permit)
CREATE OR REPLACE TABLE flaring.site_operators AS
WITH nearest AS (
    SELECT flare_id, filing_no, distance_km, operator_name, operator_no, lease_district
    FROM flaring.site_permit_coverage WHERE rank = 1
),
nearby_ops AS (
    SELECT sm.flare_id,
        n.operator_name AS attributed_operator,
        p.operator_name AS nearby_operator,
        COUNT(DISTINCT sm.filing_no) AS n_permits
    FROM flaring.site_permit_matches sm
    JOIN rrc.permits p ON p.filing_no = sm.filing_no
    JOIN nearest n USING (flare_id)
    GROUP BY 1, 2, 3
),
agg AS (
    SELECT flare_id, attributed_operator,
        COUNT(DISTINCT nearby_operator) AS n_operators,
        SUM(CASE WHEN nearby_operator = attributed_operator THEN n_permits ELSE 0 END) * 1.0
          / SUM(n_permits) AS own_share
    FROM nearby_ops GROUP BY 1, 2
)
SELECT n.flare_id, n.operator_name, n.operator_no,
    n.filing_no AS nearest_filing_no, n.distance_km AS nearest_permit_km,
    n.lease_district,
    CASE WHEN a.n_operators = 1 THEN 'sole'
         WHEN a.own_share > 0.5 THEN 'majority'
         ELSE 'contested'
    END AS confidence
FROM nearest n LEFT JOIN agg a USING (flare_id);

-- Site ↔ lease matches (flare within OTLS lease boundary)
CREATE OR REPLACE TABLE flaring.site_leases AS
SELECT fs.flare_id, ll.lease_district, ll.lease_number, ll.oil_gas_code, ll.well_count
FROM flaring.sites fs
JOIN rrc.leases ll
    ON fs.lon BETWEEN ST_XMin(ll.geom) AND ST_XMax(ll.geom)
    AND fs.lat BETWEEN ST_YMin(ll.geom) AND ST_YMax(ll.geom)
    AND ST_Contains(ll.geom, fs.geom)
WHERE NOT fs.near_excluded_facility;

-- Dark flare detection (site × day)
CREATE OR REPLACE TABLE flaring.dark_flares AS
WITH matched AS (
    SELECT
        v.flare_id, v.date, v.rh_mw, v.temp_k,
        v.lat AS vnf_lat, v.lon AS vnf_lon,
        so.operator_name, so.operator_no,
        so.nearest_filing_no AS loc_permit,
        so.nearest_permit_km AS permit_distance_km,
        so.lease_district AS permit_lease_district,
        so.confidence,
        spc.filing_no IS NULL AS is_dark,
        ROW_NUMBER() OVER (
            PARTITION BY v.flare_id, v.date
            ORDER BY (spc.filing_no IS NOT NULL) DESC
        ) AS rn
    FROM raw.vnf v
    JOIN flaring.sites fs USING (flare_id)
    JOIN flaring.site_operators so USING (flare_id)
    LEFT JOIN (
        flaring.site_permit_matches sm
        JOIN flaring.site_permit_coverage spc
            ON spc.flare_id = sm.flare_id AND spc.filing_no = sm.filing_no
    ) ON sm.flare_id = v.flare_id
        AND spc.effective_dt <= v.date
        AND (spc.expiration_dt IS NULL OR spc.expiration_dt >= v.date)
    WHERE v.detected AND NOT fs.near_excluded_facility
      AND v.date >= getvariable('start_date')
)
SELECT * EXCLUDE (rn) FROM matched WHERE rn = 1;

-- VNF detection-days allocated to leases (weighted when site spans multiple)
CREATE OR REPLACE TABLE flaring.lease_allocation AS
WITH detection_leases AS (
    SELECT v.flare_id, v.date, v.rh_mw,
        COALESCE(df.is_dark, TRUE) AS is_dark,
        COALESCE(df.operator_name, so.operator_name) AS operator_name,
        sl.lease_district, sl.lease_number
    FROM raw.vnf v
    JOIN flaring.sites fs USING (flare_id)
    JOIN flaring.site_leases sl ON sl.flare_id = v.flare_id
    LEFT JOIN flaring.dark_flares df ON df.flare_id = v.flare_id AND df.date = v.date
    LEFT JOIN flaring.site_operators so ON so.flare_id = v.flare_id
    WHERE v.detected AND NOT fs.near_excluded_facility
      AND v.date >= getvariable('start_date')
),
with_weights AS (
    SELECT *, 1.0 / COUNT(*) OVER (PARTITION BY flare_id, date) AS weight
    FROM detection_leases
)
SELECT flare_id, date, is_dark, operator_name,
    lease_district, lease_number,
    rh_mw * weight AS allocated_rh_mw, weight
FROM with_weights;

-- Monthly lease-level flaring summary (VNF + reported + permit coverage)
CREATE OR REPLACE TABLE flaring.lease_flaring AS
WITH months AS (
    SELECT DISTINCT date_trunc('month', date)::DATE AS month
    FROM flaring.dark_flares
),
reported AS (
    SELECT district AS lease_district, lease_number,
        make_date(year, month, 1) AS month,
        operator_name, lease_name, total_flared_mcf
    FROM rrc.production
    WHERE district IN ('7B','7C','08','8A')
      AND year >= extract(year FROM getvariable('start_date'))
),
vnf AS (
    SELECT lease_district, lease_number,
        date_trunc('month', date)::DATE AS month,
        count(*) AS vnf_detection_days,
        round(sum(allocated_rh_mw), 2) AS vnf_rh_mw,
        count(*) FILTER (WHERE is_dark) AS vnf_dark_days,
        round(sum(allocated_rh_mw) FILTER (WHERE is_dark), 2) AS vnf_dark_rh_mw
    FROM flaring.lease_allocation GROUP BY 1, 2, 3
),
permit_days AS (
    SELECT lease_district, lease_number, month, days_in_month,
        count(DISTINCT covered_day) AS covered_days
    FROM (
        SELECT plm.lease_district, plm.lease_number, m.month,
            dayofmonth(m.month + INTERVAL 1 MONTH - INTERVAL 1 DAY) AS days_in_month,
            UNNEST(generate_series(
                GREATEST(p.effective_dt, m.month),
                LEAST(p.expiration_dt, (m.month + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE),
                INTERVAL 1 DAY
            ))::DATE AS covered_day
        FROM rrc.permit_leases plm
        JOIN rrc.permits p ON p.filing_no = plm.filing_no
        CROSS JOIN months m
        WHERE p.status IN ('Approved', 'Submitted', 'Hearing Pending', 'Resubmitted')
          AND p.effective_dt <= (m.month + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE
          AND p.expiration_dt >= m.month
          AND GREATEST(p.effective_dt, m.month)
              <= LEAST(p.expiration_dt, (m.month + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE)
    ) GROUP BY 1, 2, 3, 4
)
SELECT
    COALESCE(r.lease_district, v.lease_district) AS lease_district,
    COALESCE(r.lease_number, v.lease_number) AS lease_number,
    COALESCE(r.month, v.month) AS month,
    r.operator_name, r.lease_name,
    COALESCE(r.total_flared_mcf, 0) AS reported_flared_mcf,
    COALESCE(v.vnf_detection_days, 0) AS vnf_detection_days,
    COALESCE(v.vnf_rh_mw, 0) AS vnf_rh_mw,
    COALESCE(v.vnf_dark_days, 0) AS vnf_dark_days,
    COALESCE(v.vnf_dark_rh_mw, 0) AS vnf_dark_rh_mw,
    COALESCE(pd.covered_days * 1.0 / pd.days_in_month, 0) AS permit_coverage,
    COALESCE(pd.covered_days, 0) AS permit_days,
    round(COALESCE(r.total_flared_mcf, 0) * (1 - COALESCE(pd.covered_days * 1.0 / pd.days_in_month, 0)), 0) AS unpermitted_flared_mcf
FROM reported r
FULL OUTER JOIN vnf v
    ON v.lease_district = r.lease_district
    AND LPAD(v.lease_number, 6, '0') = LPAD(r.lease_number, 6, '0')
    AND v.month = r.month
LEFT JOIN permit_days pd
    ON pd.lease_district = COALESCE(r.lease_district, v.lease_district)
    AND LPAD(pd.lease_number, 6, '0') = LPAD(COALESCE(r.lease_number, v.lease_number), 6, '0')
    AND pd.month = COALESCE(r.month, v.month);

-- Plume ↔ well matches within 1km (excluding non-upstream facilities)
CREATE OR REPLACE TABLE flaring.plume_wells AS
SELECT
    p.plume_id,
    w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number, w.operator_no,
    ST_Distance_Sphere(p.geom, w.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY p.plume_id ORDER BY ST_Distance_Sphere(p.geom, w.geom)) AS rank
FROM raw.plumes p
JOIN raw.wells w ON w.geom IS NOT NULL
    AND w.longitude BETWEEN p.longitude - 0.02 AND p.longitude + 0.02
    AND w.latitude  BETWEEN p.latitude  - 0.02 AND p.latitude  + 0.02
    AND ST_Distance_Sphere(p.geom, w.geom) < getvariable('plume_radius')
WHERE NOT EXISTS (
    SELECT 1 FROM raw.excluded_facilities ef
    WHERE ef.geom IS NOT NULL
      AND ef.longitude BETWEEN p.longitude - 0.015 AND p.longitude + 0.015
      AND ef.latitude  BETWEEN p.latitude  - 0.015 AND p.latitude  + 0.015
);

-- Plume ↔ VNF site matches within 1km
CREATE OR REPLACE TABLE flaring.plume_sites AS
SELECT
    p.plume_id, p.date AS plume_date,
    fs.flare_id, fs.lat AS vnf_lat, fs.lon AS vnf_lon,
    ST_Distance_Sphere(p.geom, fs.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY p.plume_id ORDER BY ST_Distance_Sphere(p.geom, fs.geom)) AS rank
FROM raw.plumes p
JOIN flaring.sites fs
    ON fs.lon BETWEEN p.longitude - 0.02 AND p.longitude + 0.02
    AND fs.lat BETWEEN p.latitude - 0.02 AND p.latitude + 0.02
    AND ST_Distance_Sphere(p.geom, fs.geom) < getvariable('plume_radius')
WHERE NOT fs.near_excluded_facility;

-- Attributed plumes: classified as flaring/unlit/wellpad/unmatched
CREATE OR REPLACE TABLE flaring.plumes AS
WITH plume_vnf AS (
    SELECT * FROM (
        SELECT psm.plume_id, psm.flare_id, psm.distance_km AS vnf_distance_km,
            v.rh_mw, v.date IS NOT NULL AS flare_detected,
            ROW_NUMBER() OVER (PARTITION BY psm.plume_id ORDER BY (v.date IS NOT NULL) DESC, psm.distance_km) AS rn
        FROM flaring.plume_sites psm
        LEFT JOIN raw.vnf v ON v.flare_id = psm.flare_id AND v.detected
            AND v.date BETWEEN psm.plume_date - INTERVAL 1 DAY AND psm.plume_date + INTERVAL 1 DAY
        WHERE psm.rank = 1
    ) WHERE rn = 1
)
SELECT
    p.plume_id, p.source, p.satellite, p.date, p.latitude, p.longitude,
    p.emission_rate, p.emission_uncertainty, p.sector,
    pw.api, pw.oil_gas_code, pw.lease_district, pw.lease_number, pw.operator_no,
    pw.distance_km AS well_distance_km,
    o.operator_name,
    pv.flare_id AS vnf_flare_id, pv.vnf_distance_km,
    COALESCE(pv.flare_detected, false) AS flare_detected, pv.rh_mw AS vnf_rh_mw,
    CASE
        WHEN pv.flare_id IS NOT NULL AND NOT COALESCE(pv.flare_detected, false) THEN 'unlit'
        WHEN pv.flare_id IS NOT NULL AND pv.flare_detected THEN 'flaring'
        WHEN pw.api IS NOT NULL THEN 'wellpad'
        ELSE 'unmatched'
    END AS classification
FROM raw.plumes p
LEFT JOIN (SELECT * FROM flaring.plume_wells WHERE rank = 1) pw USING (plume_id)
LEFT JOIN plume_vnf pv USING (plume_id)
LEFT JOIN raw.operators o ON LPAD(o.operator_number, 6, '0') = LPAD(pw.operator_no, 6, '0');

-- Summary views

CREATE OR REPLACE VIEW flaring.plume_summary AS
SELECT classification, source,
    count(*) AS plume_count,
    round(avg(emission_rate), 1) AS avg_emission_rate,
    round(sum(emission_rate), 0) AS total_emission_rate,
    min(date) AS earliest, max(date) AS latest
FROM flaring.plumes GROUP BY 1, 2 ORDER BY 1, 2;

CREATE OR REPLACE VIEW flaring.operator_scorecard AS
WITH vnf AS (
    SELECT COALESCE(operator_name, 'Unknown') AS operator,
        count(DISTINCT flare_id) AS vnf_sites,
        count(*) AS total_detection_days,
        count(*) FILTER (WHERE is_dark) AS dark_detection_days,
        round(100.0 * count(*) FILTER (WHERE is_dark) / count(*), 1) AS pct_dark,
        round(sum(rh_mw), 0) AS total_rh_mw,
        round(sum(rh_mw) FILTER (WHERE is_dark), 0) AS dark_rh_mw
    FROM flaring.dark_flares
    WHERE confidence IN ('sole', 'majority')
    GROUP BY 1
),
reported AS (
    SELECT operator_name AS operator,
        round(sum(total_flared_mcf) / 1e6, 2) AS reported_bcf,
        round(sum(total_gas_prod_mcf) / 1e6, 2) AS produced_bcf,
        round(100.0 * sum(total_flared_mcf) / NULLIF(sum(total_gas_prod_mcf), 0), 2) AS flare_intensity_pct
    FROM rrc.production
    WHERE district IN ('7B','7C','08','8A')
      AND year >= (SELECT extract(year FROM min(date)) FROM flaring.dark_flares)
    GROUP BY 1
),
plumes AS (
    SELECT operator_name AS operator,
        count(*) AS plume_count,
        round(sum(emission_rate), 0) AS plume_total_kg_hr,
        count(*) FILTER (WHERE classification = 'unlit') AS unlit_plumes
    FROM flaring.plumes WHERE operator_name IS NOT NULL
    GROUP BY 1
)
SELECT v.operator,
    v.vnf_sites, v.total_rh_mw, v.dark_rh_mw, v.pct_dark,
    v.total_detection_days, v.dark_detection_days,
    r.reported_bcf, r.produced_bcf, r.flare_intensity_pct,
    p.plume_count, p.plume_total_kg_hr, p.unlit_plumes
FROM vnf v
LEFT JOIN reported r ON r.operator = v.operator
LEFT JOIN plumes p ON p.operator = v.operator
WHERE v.total_rh_mw >= 200
ORDER BY v.total_rh_mw DESC;
