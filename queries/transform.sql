LOAD spatial;

-- ============================================================
-- Entity tables: flare sites, detections, spatial matches
-- ============================================================

-- Match radii
SET VARIABLE permit_radius = 0.015;   -- 1.5km for VNF ↔ permit location
SET VARIABLE plume_radius = 0.01;     -- 1km for plume ↔ well/VNF

-- Flare sites: one row per VNF site with exclusion flag
CREATE OR REPLACE TABLE flare_sites AS
SELECT
    f.flare_id,
    f.lat,
    f.lon,
    f.geom,
    f.first_detected,
    f.last_detected,
    f.detection_days,
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
    FROM raw.vnf WHERE detected
    GROUP BY flare_id
) f;

CREATE INDEX IF NOT EXISTS idx_flare_sites_geom ON flare_sites USING RTREE (geom);

-- Upstream flare locations (exclude Gas Plant permits)
CREATE OR REPLACE TABLE flare_locations AS
SELECT fl.*
FROM raw.flare_locations fl
WHERE fl.filing_no::VARCHAR NOT IN (
    SELECT filing_no FROM raw.permits WHERE property_type = 'Gas Plant'
);

CREATE INDEX IF NOT EXISTS idx_flare_locations_geom ON flare_locations USING RTREE (geom);

-- ============================================================
-- Spatial matching: flare sites ↔ permit locations
-- ============================================================

-- All matches within 1.5km (not just nearest)
CREATE OR REPLACE TABLE site_permit_matches AS
SELECT
    f.flare_id,
    fl.filing_no,
    ST_Distance_Sphere(f.geom, fl.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY f.flare_id ORDER BY ST_Distance_Sphere(f.geom, fl.geom)) AS rank
FROM flare_sites f
JOIN flare_locations fl ON fl.geom IS NOT NULL
    AND fl.longitude BETWEEN f.lon - 0.03 AND f.lon + 0.03
    AND fl.latitude  BETWEEN f.lat - 0.03 AND f.lat + 0.03
    AND ST_DWithin(f.geom, fl.geom, getvariable('permit_radius'))
WHERE NOT f.near_excluded_facility;

-- Permit coverage: which permits cover which sites, with parsed dates
-- Includes Submitted/Hearing Pending (benefit of the doubt per Earthworks methodology)
CREATE OR REPLACE TABLE site_permit_coverage AS
SELECT
    sm.flare_id,
    sm.filing_no,
    sm.distance_km,
    sm.rank,
    p.operator_name,
    p.operator_no,
    p.property,
    p.property_type,
    p.lease_district,
    p.lease_number,
    p.status AS permit_status,
    TRY_STRPTIME(p.effective_dt, '%m/%d/%Y')::DATE AS effective_dt,
    TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y')::DATE AS expiration_dt
FROM site_permit_matches sm
JOIN raw.permits p ON p.filing_no = sm.filing_no
WHERE p.status IN ('Approved', 'Submitted', 'Hearing Pending', 'Resubmitted')
  AND p.property_type != 'Gas Plant';

-- Operator attribution per site (from nearest permit location)
CREATE OR REPLACE TABLE site_operators AS
WITH nearest AS (
    SELECT flare_id, filing_no, distance_km, operator_name, operator_no, lease_district
    FROM site_permit_coverage
    WHERE rank = 1
),
nearby_ops AS (
    SELECT
        sm.flare_id,
        n.operator_name AS attributed_operator,
        p.operator_name AS nearby_operator,
        COUNT(DISTINCT sm.filing_no) AS n_permits
    FROM site_permit_matches sm
    JOIN raw.permits p ON p.filing_no = sm.filing_no
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
SELECT
    n.flare_id,
    n.operator_name,
    n.operator_no,
    n.filing_no AS nearest_filing_no,
    n.distance_km AS nearest_permit_km,
    n.lease_district,
    CASE
        WHEN a.n_operators = 1 THEN 'sole'
        WHEN a.own_share > 0.5 THEN 'majority'
        ELSE 'contested'
    END AS confidence
FROM nearest n
LEFT JOIN agg a USING (flare_id);

-- ============================================================
-- Spatial matching: plumes ↔ wells and VNF sites
-- ============================================================

-- Plume ↔ well matches within 1km (excluding plumes near non-upstream facilities)
CREATE OR REPLACE TABLE plume_well_matches AS
SELECT
    p.plume_id,
    w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number, w.operator_no,
    ST_Distance_Sphere(p.geom, w.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY p.plume_id ORDER BY ST_Distance_Sphere(p.geom, w.geom)) AS rank
FROM raw.plumes p
JOIN raw.wells w ON w.geom IS NOT NULL
    AND w.longitude BETWEEN p.longitude - 0.02 AND p.longitude + 0.02
    AND w.latitude  BETWEEN p.latitude  - 0.02 AND p.latitude  + 0.02
    AND ST_DWithin(p.geom, w.geom, getvariable('plume_radius'))
WHERE NOT EXISTS (
    SELECT 1 FROM raw.excluded_facilities ef
    WHERE ef.geom IS NOT NULL
      AND ef.longitude BETWEEN p.longitude - 0.015 AND p.longitude + 0.015
      AND ef.latitude  BETWEEN p.latitude  - 0.015 AND p.latitude  + 0.015
);

-- Plume ↔ VNF site matches within 1km (excluding sites near non-upstream facilities)
CREATE OR REPLACE TABLE plume_site_matches AS
SELECT
    p.plume_id,
    p.date AS plume_date,
    fs.flare_id,
    fs.lat AS vnf_lat, fs.lon AS vnf_lon,
    ST_Distance_Sphere(p.geom, fs.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY p.plume_id ORDER BY ST_Distance_Sphere(p.geom, fs.geom)) AS rank
FROM raw.plumes p
JOIN flare_sites fs
    ON fs.lon BETWEEN p.longitude - 0.02 AND p.longitude + 0.02
    AND fs.lat BETWEEN p.latitude - 0.02 AND p.latitude + 0.02
    AND ST_DWithin(p.geom, fs.geom, getvariable('plume_radius'))
WHERE NOT fs.near_excluded_facility;
