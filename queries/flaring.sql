LOAD spatial;

SET VARIABLE permit_radius = 375;  -- VIIRS M-band pixel radius (750m / 2)
SET VARIABLE plume_radius = 1000;
SET VARIABLE start_date = '2021-01-01'::DATE;
SET VARIABLE lat_min = 30.0;
SET VARIABLE lat_max = 33.5;
SET VARIABLE lon_min = -104.5;
SET VARIABLE lon_max = -100.0;
SET VARIABLE nm_border_lon = -103.064;  -- TX-NM border longitude (above 32°N)

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
) f
WHERE f.lat BETWEEN getvariable('lat_min') AND getvariable('lat_max')
  AND f.lon BETWEEN getvariable('lon_min') AND getvariable('lon_max')
  AND (f.lat <= 32.0 OR f.lon >= getvariable('nm_border_lon'));

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

-- Site ↔ permit matches within pixel radius (375m)
CREATE OR REPLACE TABLE flaring.site_permit_matches AS
SELECT
    f.flare_id, fl.filing_no, fl.name AS location_name,
    ST_Distance_Sphere(f.geom, fl.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY f.flare_id ORDER BY ST_Distance_Sphere(f.geom, fl.geom)) AS rank
FROM flaring.sites f
JOIN flaring.permit_locations fl ON fl.geom IS NOT NULL
    AND fl.longitude BETWEEN f.lon - 0.005 AND f.lon + 0.005
    AND fl.latitude  BETWEEN f.lat - 0.005 AND f.lat + 0.005
    AND ST_Distance_Sphere(f.geom, fl.geom) < getvariable('permit_radius')
WHERE NOT f.near_excluded_facility;

-- Site ↔ well matches within pixel radius (375m)
CREATE OR REPLACE TABLE flaring.site_well_matches AS
SELECT
    f.flare_id, w.api, w.operator_no,
    o.operator_name,
    ST_Distance_Sphere(f.geom, w.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY f.flare_id ORDER BY ST_Distance_Sphere(f.geom, w.geom)) AS rank
FROM flaring.sites f
JOIN raw.wells w ON w.geom IS NOT NULL
    AND w.longitude BETWEEN f.lon - 0.005 AND f.lon + 0.005
    AND w.latitude  BETWEEN f.lat - 0.005 AND f.lat + 0.005
    AND ST_Distance_Sphere(f.geom, w.geom) < getvariable('permit_radius')
LEFT JOIN raw.operators o ON LPAD(o.operator_number, 6, '0') = LPAD(w.operator_no, 6, '0')
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

-- Operator attribution per site (from nearest permit + wells within pixel)
CREATE OR REPLACE TABLE flaring.site_operators AS
WITH
-- All nearby operators from permits
permit_ops AS (
    SELECT sm.flare_id, p.operator_name, 'permit' AS source,
        COUNT(DISTINCT sm.filing_no) AS n_records,
        MIN(sm.distance_km) AS min_distance_km
    FROM flaring.site_permit_matches sm
    JOIN rrc.permits p ON p.filing_no = sm.filing_no
    GROUP BY 1, 2
),
-- All nearby operators from wells
well_ops AS (
    SELECT wm.flare_id, wm.operator_name, 'well' AS source,
        COUNT(DISTINCT wm.api) AS n_records,
        MIN(wm.distance_km) AS min_distance_km
    FROM flaring.site_well_matches wm
    WHERE wm.operator_name IS NOT NULL
    GROUP BY 1, 2
),
-- Combined: all operators near each site with total evidence count
all_ops AS (
    SELECT flare_id, operator_name, SUM(n_records) AS total_records,
        MIN(min_distance_km) AS min_distance_km,
        bool_or(source = 'permit') AS has_permit
    FROM (SELECT flare_id, operator_name, source, n_records, min_distance_km FROM permit_ops
          UNION ALL
          SELECT flare_id, operator_name, source, n_records, min_distance_km FROM well_ops) combined
    GROUP BY 1, 2
),
-- Pick best operator per site: prefer operators with permits, then most evidence, then closest
ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY flare_id
        ORDER BY has_permit DESC, total_records DESC, min_distance_km
    ) AS rn
    FROM all_ops
),
best AS (SELECT * FROM ranked WHERE rn = 1),
-- Confidence: how dominant is the attributed operator?
agg AS (
    SELECT b.flare_id,
        COUNT(DISTINCT a.operator_name) AS n_operators,
        SUM(CASE WHEN a.operator_name = b.operator_name THEN a.total_records ELSE 0 END) * 1.0
          / SUM(a.total_records) AS own_share
    FROM best b JOIN all_ops a USING (flare_id)
    GROUP BY 1
),
-- Nearest permit info (may be null if matched only via wells)
nearest_permit AS (
    SELECT spc.flare_id, spc.filing_no, spc.distance_km AS nearest_permit_km,
        spc.operator_name AS permit_operator, spc.operator_no, spc.lease_district,
        sm.location_name AS nearest_permit_name
    FROM flaring.site_permit_coverage spc
    JOIN flaring.site_permit_matches sm ON sm.flare_id = spc.flare_id AND sm.filing_no = spc.filing_no
    WHERE spc.rank = 1
)
SELECT b.flare_id, b.operator_name,
    COALESCE(np.operator_no, (
        SELECT wm.operator_no FROM flaring.site_well_matches wm
        WHERE wm.flare_id = b.flare_id AND wm.operator_name = b.operator_name
        ORDER BY wm.distance_km LIMIT 1
    )) AS operator_no,
    np.filing_no AS nearest_filing_no, np.nearest_permit_km,
    np.nearest_permit_name,
    np.lease_district,
    CASE WHEN a.n_operators = 1 THEN 'sole'
         WHEN a.own_share > 0.5 THEN 'majority'
         ELSE 'contested'
    END AS confidence,
    b.has_permit AS has_nearby_permit,
    b.total_records AS nearby_records
FROM best b
LEFT JOIN agg a USING (flare_id)
LEFT JOIN nearest_permit np USING (flare_id);

-- Site ↔ lease matches (flare within OTLS lease boundary)
CREATE OR REPLACE TABLE flaring.site_leases AS
SELECT fs.flare_id, ll.lease_district, ll.lease_number, ll.oil_gas_code, ll.well_count
FROM flaring.sites fs
JOIN rrc.leases ll
    ON fs.lon BETWEEN ST_XMin(ll.geom) AND ST_XMax(ll.geom)
    AND fs.lat BETWEEN ST_YMin(ll.geom) AND ST_YMax(ll.geom)
    AND ST_Contains(ll.geom, fs.geom)
WHERE NOT fs.near_excluded_facility;

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
WHERE p.latitude BETWEEN getvariable('lat_min') AND getvariable('lat_max')
  AND p.longitude BETWEEN getvariable('lon_min') AND getvariable('lon_max')
  AND (p.latitude <= 32.0 OR p.longitude >= getvariable('nm_border_lon'))
  AND NOT EXISTS (
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
WHERE p.latitude BETWEEN getvariable('lat_min') AND getvariable('lat_max')
  AND p.longitude BETWEEN getvariable('lon_min') AND getvariable('lon_max')
  AND (p.latitude <= 32.0 OR p.longitude >= getvariable('nm_border_lon'))
  AND NOT fs.near_excluded_facility;

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
LEFT JOIN raw.operators o ON LPAD(o.operator_number, 6, '0') = LPAD(pw.operator_no, 6, '0')
WHERE p.latitude BETWEEN getvariable('lat_min') AND getvariable('lat_max')
  AND p.longitude BETWEEN getvariable('lon_min') AND getvariable('lon_max')
  AND (p.latitude <= 32.0 OR p.longitude >= getvariable('nm_border_lon'));

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
    SELECT COALESCE(so.operator_name, 'Unknown') AS operator,
        count(DISTINCT fs.flare_id) AS vnf_sites,
        sum(fs.detection_days) AS total_detection_days,
        round(sum(d.total_rh_mw), 0) AS total_rh_mw
    FROM flaring.sites fs
    LEFT JOIN flaring.site_operators so USING (flare_id)
    LEFT JOIN (
        SELECT flare_id, sum(rh_mw) AS total_rh_mw
        FROM raw.vnf WHERE detected GROUP BY flare_id
    ) d USING (flare_id)
    WHERE so.confidence IN ('sole', 'majority')
    GROUP BY 1
),
reported AS (
    SELECT operator_name AS operator,
        round(sum(total_flared_mcf) / 1e6, 2) AS reported_bcf,
        round(sum(total_gas_prod_mcf) / 1e6, 2) AS produced_bcf,
        round(100.0 * sum(total_flared_mcf) / NULLIF(sum(total_gas_prod_mcf), 0), 2) AS flare_intensity_pct
    FROM rrc.production
    WHERE district IN ('7B','7C','08','8A')
      AND year >= extract(year FROM getvariable('start_date'))
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
    v.vnf_sites, v.total_rh_mw,
    v.total_detection_days,
    r.reported_bcf, r.produced_bcf, r.flare_intensity_pct,
    p.plume_count, p.plume_total_kg_hr, p.unlit_plumes
FROM vnf v
LEFT JOIN reported r ON r.operator = v.operator
LEFT JOIN plumes p ON p.operator = v.operator
WHERE v.total_rh_mw >= 200
ORDER BY v.total_rh_mw DESC;
