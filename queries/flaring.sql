LOAD spatial;

-- SWR 32 permits (parse "Oil Lease-08-43066" -> type, district, number)
INSERT INTO permits
SELECT * REPLACE (replace(operator_name, '&amp;', '&') AS operator_name),
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 1) END,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 2) END,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 3) END
FROM read_csv('data/filings.csv', delim='\t', header=true, all_varchar=true);

-- Wells (still needed for plumes.sql)
INSERT INTO wells
SELECT *, CASE WHEN latitude != 0 AND longitude != 0
               THEN ST_Point(longitude, latitude) END
FROM read_csv('data/wells.csv', header=true, auto_detect=true);

-- Operators (still needed for plumes.sql)
INSERT INTO operators
SELECT * FROM read_csv('data/operators.csv', header=true, auto_detect=true);

-- Flare locations (permitted flare GPS coordinates)
-- Exclude Gas Plant permits (midstream) at load time
INSERT INTO flare_locations
SELECT fl.*, CASE WHEN fl.latitude != 0 AND fl.longitude != 0
                  THEN ST_Point(fl.longitude, fl.latitude) END
FROM read_csv('data/flare_locations.csv', header=true, auto_detect=true) fl
WHERE fl.filing_no::VARCHAR NOT IN (
    SELECT filing_no FROM permits WHERE property_type = 'Gas Plant'
);

-- VNF: read profiles with explicit types (avoids auto_detect on 1700 files)
-- Filter to permit era (Q4 2023+) — no permits exist before this date
SET VARIABLE vnf_start = '2023-10-01';

INSERT INTO vnf
SELECT flare_id, AVG(lat), AVG(lon), date,
    BOOL_OR(cloud = 0) AS clear,
    BOOL_OR(cloud = 0 AND temp != 999999) AS detected,
    AVG(CASE WHEN cloud = 0 AND temp != 999999 THEN rh END),
    AVG(CASE WHEN cloud = 0 AND temp != 999999 THEN temp END),
    COUNT(*), NULL
FROM (
    SELECT CAST(regexp_extract(filename, 'site_(\d+)', 1) AS INTEGER) AS flare_id,
           Date_Mscan::DATE AS date, Lat_GMTCO::DOUBLE AS lat, Lon_GMTCO::DOUBLE AS lon,
           Cloud_Mask::INTEGER AS cloud, Temp_BB::DOUBLE AS temp, RH::DOUBLE AS rh
    FROM read_csv('data/vnf_profiles/site_*.csv', filename=true, union_by_name=true,
                  ignore_errors=true, all_varchar=true, header=true)
    WHERE Sunlit::INTEGER = 0
      AND Date_Mscan::DATE >= getvariable('vnf_start')
)
GROUP BY flare_id, date;

-- Non-upstream facility exclusion zones (EPA GHGRP: gas plants, compressor stations, etc.)
INSERT INTO excluded_facilities
SELECT *, CASE WHEN latitude != 0 AND longitude != 0
               THEN ST_Point(longitude, latitude) END
FROM read_csv('data/excluded_facilities.csv', header=true, auto_detect=true);

-- Geometry + indexes
UPDATE vnf SET geom = ST_Point(lon, lat) WHERE lat IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_wells_geom ON wells USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_vnf_geom ON vnf USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_flare_loc_geom ON flare_locations USING RTREE (geom);

-- Match radius: 1.5km (0.015°) — permitted GPS is the actual flare stack,
-- VNF geolocation error is ~375m, so 1.5km is conservative
SET VARIABLE permit_radius = 0.015;

-- Distinct flare site locations (one row per flare_id)
CREATE OR REPLACE TEMP TABLE flare_sites_all AS
SELECT flare_id, AVG(lat) AS lat, AVG(lon) AS lon, ST_Point(AVG(lon), AVG(lat)) AS geom
FROM vnf WHERE detected GROUP BY flare_id;

-- Exclude VNF sites within 1.5km of non-upstream facilities
CREATE OR REPLACE TEMP TABLE flare_sites AS
SELECT f.* FROM flare_sites_all f
WHERE NOT EXISTS (
    SELECT 1 FROM excluded_facilities ef
    WHERE ef.geom IS NOT NULL
      AND ef.longitude BETWEEN f.lon - 0.015 AND f.lon + 0.015
      AND ef.latitude  BETWEEN f.lat - 0.015 AND f.lat + 0.015
);

-- Match each flare site to ALL permit locations within 1.5km
CREATE OR REPLACE TEMP TABLE site_permit_loc_match AS
SELECT f.flare_id, fl.filing_no,
       ST_Distance_Sphere(f.geom, fl.geom) / 1000.0 AS distance_km,
       ROW_NUMBER() OVER (PARTITION BY f.flare_id ORDER BY ST_Distance_Sphere(f.geom, fl.geom)) AS rn
FROM flare_sites f
JOIN flare_locations fl ON fl.geom IS NOT NULL
    AND fl.longitude BETWEEN f.lon - 0.03 AND f.lon + 0.03
    AND fl.latitude  BETWEEN f.lat - 0.03 AND f.lat + 0.03
    AND ST_DWithin(f.geom, fl.geom, getvariable('permit_radius'));

-- Nearest permit info for each matched site (for operator attribution)
CREATE OR REPLACE TEMP TABLE site_permit_info AS
SELECT sl.flare_id, sl.filing_no, sl.distance_km,
       p.operator_name, p.operator_no,
       p.property AS permit_property, p.lease_district AS permit_lease_district
FROM site_permit_loc_match sl
JOIN permits p ON p.filing_no = sl.filing_no
WHERE sl.rn = 1;

-- Attribution confidence: how many operators have permits near each flare site?
-- 'sole' = only one operator's permits nearby
-- 'majority' = attributed operator has >50% of nearby permits
-- 'contested' = multiple operators, none dominant
CREATE OR REPLACE TEMP TABLE site_confidence AS
WITH nearby_ops AS (
    SELECT sl.flare_id,
        sp.operator_name AS attributed,
        p.operator_name AS nearby_operator,
        count(DISTINCT sl.filing_no) AS n_permits
    FROM site_permit_loc_match sl
    JOIN permits p ON p.filing_no = sl.filing_no
    JOIN site_permit_info sp USING (flare_id)
    GROUP BY 1, 2, 3
),
agg AS (
    SELECT flare_id, attributed,
        count(DISTINCT nearby_operator) AS n_operators,
        sum(CASE WHEN nearby_operator = attributed THEN n_permits ELSE 0 END) * 1.0
          / sum(n_permits) AS own_share
    FROM nearby_ops GROUP BY 1, 2
)
SELECT flare_id,
    CASE WHEN n_operators = 1 THEN 'sole'
         WHEN own_share > 0.5 THEN 'majority'
         ELSE 'contested'
    END AS confidence
FROM agg;

-- Pre-parse permit dates (include Submitted/Hearing Pending for benefit of the doubt)
-- Following Earthworks methodology: give operators every benefit of the doubt
CREATE OR REPLACE TEMP TABLE permits_parsed AS
SELECT filing_no, property, lease_district, lease_number, operator_no,
    TRY_STRPTIME(effective_dt, '%m/%d/%Y')::DATE AS eff_date,
    TRY_STRPTIME(expiration_dt, '%m/%d/%Y')::DATE AS exp_date
FROM permits
WHERE status IN ('Approved', 'Submitted', 'Hearing Pending', 'Resubmitted')
  AND property_type != 'Gas Plant';

-- For each flare detection-day, check if ANY permit location within 1.5km
-- has an active permit covering that date (benefit of the doubt)
-- Deduplicate: one row per (flare_id, date), preferring permitted over dark
CREATE OR REPLACE TABLE dark_flares AS
WITH matched AS (
    SELECT v.flare_id, v.date, v.rh_mw, v.temp_k, v.lat AS vnf_lat, v.lon AS vnf_lon,
        sp.operator_name, sp.operator_no, sp.permit_property, sp.permit_lease_district,
        sp.filing_no AS loc_permit, sp.distance_km AS permit_distance_km,
        sc.confidence,
        pp.filing_no AS permit_filing_no,
        pp.eff_date AS permit_effective,
        pp.exp_date AS permit_expiration,
        pp.filing_no IS NULL AS is_dark,
        ROW_NUMBER() OVER (
            PARTITION BY v.flare_id, v.date
            ORDER BY (pp.filing_no IS NOT NULL) DESC
        ) AS rn
    FROM vnf v
    JOIN flare_sites fs USING (flare_id)
    JOIN site_permit_info sp USING (flare_id)
    LEFT JOIN site_confidence sc USING (flare_id)
    LEFT JOIN (
        site_permit_loc_match sl
        JOIN permits_parsed pp ON pp.filing_no = sl.filing_no
    ) ON sl.flare_id = v.flare_id
        AND pp.eff_date <= v.date
        AND (pp.exp_date IS NULL OR pp.exp_date >= v.date)
    WHERE v.detected
)
SELECT * EXCLUDE (rn) FROM matched WHERE rn = 1;

CREATE OR REPLACE VIEW dark_flaring_summary AS
SELECT is_dark,
    count(*) AS detection_days, count(DISTINCT flare_id) AS flare_sites,
    round(avg(rh_mw), 2) AS avg_rh_mw, round(sum(rh_mw), 0) AS total_rh_mw,
    min(date) AS earliest, max(date) AS latest
FROM dark_flares GROUP BY is_dark;

CREATE OR REPLACE VIEW top_dark_flares AS
SELECT flare_id,
    permit_lease_district AS lease_district,
    operator_no, operator_name AS operator, confidence,
    round(avg(vnf_lat), 4) AS lat, round(avg(vnf_lon), 4) AS lon,
    count(*) AS detection_days, round(sum(rh_mw), 1) AS total_rh_mw,
    strftime(min(date), '%Y-%m-%d') AS first_seen, strftime(max(date), '%Y-%m-%d') AS last_seen
FROM dark_flares WHERE is_dark
GROUP BY flare_id, 2, operator_no, operator, confidence
ORDER BY total_rh_mw DESC;
