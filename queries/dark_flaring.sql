LOAD spatial;

-- Match VNF detections to nearest well within ~750m
CREATE OR REPLACE TABLE vnf_matched AS
WITH nearest AS (
    SELECT v.flare_id, v.date, v.rh_mw, v.temp_k, v.lat AS vnf_lat, v.lon AS vnf_lon,
           w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number, w.operator_no,
           ST_Distance_Sphere(v.geom, w.geom) / 1000.0 AS distance_km,
           ROW_NUMBER() OVER (PARTITION BY v.flare_id, v.date
                              ORDER BY ST_Distance_Sphere(v.geom, w.geom)) AS rn
    FROM vnf v
    JOIN wells w ON w.geom IS NOT NULL
        AND w.longitude BETWEEN v.lon - 0.015 AND v.lon + 0.015
        AND w.latitude  BETWEEN v.lat - 0.015 AND v.lat + 0.015
        AND ST_DWithin(v.geom, w.geom, 0.0075)
    WHERE v.detected
)
SELECT * EXCLUDE (rn) FROM nearest WHERE rn = 1;

-- District mapping: wells use numeric (07,08), permits use alphanumeric (7C,8A,08)
CREATE OR REPLACE MACRO district_match(well_d, permit_d) AS
    permit_d = well_d
    OR (well_d = '08' AND permit_d IN ('08', '8A'))
    OR (well_d = '07' AND permit_d IN ('07', '7C', '7B'));

-- Join to permits — find valid SWR 32 coverage
CREATE OR REPLACE TABLE dark_flares AS
SELECT m.*,
    COALESCE(
        (SELECT o.operator_name FROM operators o
         WHERE LPAD(o.operator_number, 6, '0') = LPAD(m.operator_no, 6, '0') LIMIT 1),
        (SELECT o.operator_name FROM operators o
         WHERE LPAD(o.operator_number, 6, '0') = (
             SELECT LPAD(pp.operator_no, 6, '0') FROM permits pp
             WHERE pp.lease_number = m.lease_number
               AND district_match(m.lease_district, pp.lease_district) LIMIT 1
         ) LIMIT 1)
    ) AS operator_name,
    p.filing_no AS permit_filing_no,
    p.status AS permit_status,
    p.effective_dt AS permit_effective,
    p.expiration_dt AS permit_expiration,
    p.property AS permit_property,
    p.filing_no IS NULL AS is_dark
FROM vnf_matched m
LEFT JOIN permits p
    ON p.lease_number = m.lease_number
    AND district_match(m.lease_district, p.lease_district)
    AND p.status = 'Approved'
    AND TRY_STRPTIME(p.effective_dt, '%m/%d/%Y') <= m.date
    AND (p.expiration_dt = '' OR TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y') >= m.date);

CREATE OR REPLACE VIEW dark_flaring_summary AS
SELECT is_dark,
    count(*) AS detection_days, count(DISTINCT flare_id) AS flare_sites,
    round(avg(rh_mw), 2) AS avg_rh_mw, round(sum(rh_mw), 0) AS total_rh_mw,
    min(date) AS earliest, max(date) AS latest
FROM dark_flares GROUP BY is_dark;

CREATE OR REPLACE VIEW top_dark_flares AS
SELECT flare_id, lease_district, lease_number, operator_no,
    COALESCE(operator_name, 'OP#' || operator_no) AS operator_name,
    round(avg(vnf_lat), 4) AS vnf_lat, round(avg(vnf_lon), 4) AS vnf_lon,
    count(*) AS detection_days, round(sum(rh_mw), 1) AS total_rh_mw,
    min(date) AS first_seen, max(date) AS last_seen
FROM dark_flares WHERE is_dark
GROUP BY flare_id, lease_district, lease_number, operator_no, operator_name
ORDER BY total_rh_mw DESC;
