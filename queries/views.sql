LOAD spatial;

-- ============================================================
-- Dark flaring analysis
-- ============================================================

-- For each detection-day, check if ANY nearby permit covers that date
-- Deduplicate: one row per (flare_id, date), preferring permitted over dark
CREATE OR REPLACE TABLE dark_flares AS
WITH matched AS (
    SELECT
        v.flare_id, v.date, v.rh_mw, v.temp_k, v.lat AS vnf_lat, v.lon AS vnf_lon,
        so.operator_name, so.operator_no,
        so.nearest_filing_no AS loc_permit, so.nearest_permit_km AS permit_distance_km,
        so.lease_district AS permit_lease_district,
        so.confidence,
        spc.filing_no AS permit_filing_no,
        spc.effective_dt AS permit_effective,
        spc.expiration_dt AS permit_expiration,
        spc.filing_no IS NULL AS is_dark,
        ROW_NUMBER() OVER (
            PARTITION BY v.flare_id, v.date
            ORDER BY (spc.filing_no IS NOT NULL) DESC
        ) AS rn
    FROM raw.vnf v
    JOIN flare_sites fs USING (flare_id)
    JOIN site_operators so USING (flare_id)
    LEFT JOIN (
        site_permit_matches sm
        JOIN site_permit_coverage spc ON spc.filing_no = sm.filing_no
    ) ON sm.flare_id = v.flare_id
        AND spc.effective_dt <= v.date
        AND (spc.expiration_dt IS NULL OR spc.expiration_dt >= v.date)
    WHERE v.detected
      AND NOT fs.near_excluded_facility
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

-- ============================================================
-- Plume attribution
-- ============================================================

-- Check if nearest VNF site had a detection within +-1 day of plume
CREATE OR REPLACE TABLE plume_attributed AS
WITH plume_vnf_detection AS (
    SELECT
        psm.plume_id, psm.flare_id, psm.distance_km AS vnf_distance_km,
        v.date AS vnf_date, v.rh_mw, v.temp_k,
        CASE WHEN v.date IS NOT NULL THEN true ELSE false END AS flare_detected
    FROM plume_site_matches psm
    LEFT JOIN raw.vnf v ON v.flare_id = psm.flare_id
        AND v.detected
        AND v.date BETWEEN psm.plume_date - INTERVAL 1 DAY AND psm.plume_date + INTERVAL 1 DAY
    WHERE psm.rank = 1
),
plume_vnf AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plume_id ORDER BY flare_detected DESC) AS rn
        FROM plume_vnf_detection
    ) WHERE rn = 1
),
plume_nearest_well AS (
    SELECT * FROM plume_well_matches WHERE rank = 1
)
SELECT
    p.plume_id, p.source, p.satellite, p.date, p.latitude, p.longitude,
    p.emission_rate, p.emission_uncertainty, p.sector,
    pw.api, pw.oil_gas_code, pw.lease_district, pw.lease_number, pw.operator_no,
    pw.distance_km AS well_distance_km,
    o.operator_name,
    pv.flare_id AS vnf_flare_id,
    pv.vnf_distance_km,
    COALESCE(pv.flare_detected, false) AS flare_detected,
    pv.rh_mw AS vnf_rh_mw,
    CASE
        WHEN pv.flare_id IS NOT NULL AND NOT COALESCE(pv.flare_detected, false) THEN 'unlit'
        WHEN pv.flare_id IS NOT NULL AND COALESCE(pv.flare_detected, false) THEN 'flaring'
        WHEN pw.api IS NOT NULL THEN 'wellpad'
        ELSE 'unmatched'
    END AS classification
FROM raw.plumes p
LEFT JOIN plume_nearest_well pw USING (plume_id)
LEFT JOIN plume_vnf pv USING (plume_id)
LEFT JOIN raw.operators o ON LPAD(o.operator_number, 6, '0') = LPAD(pw.operator_no, 6, '0');

CREATE OR REPLACE VIEW plume_summary AS
SELECT classification, source,
    count(*) AS plume_count,
    count(DISTINCT COALESCE(api, plume_id)) AS sites,
    round(avg(emission_rate), 1) AS avg_emission_rate,
    round(sum(emission_rate), 0) AS total_emission_rate,
    min(date) AS earliest, max(date) AS latest
FROM plume_attributed
GROUP BY classification, source
ORDER BY classification, source;

CREATE OR REPLACE VIEW unlit_flares AS
SELECT plume_id, date, latitude, longitude, emission_rate, emission_uncertainty,
    source, satellite, operator_name, api, vnf_flare_id, vnf_distance_km,
    well_distance_km
FROM plume_attributed
WHERE classification = 'unlit'
ORDER BY emission_rate DESC;

CREATE OR REPLACE VIEW top_plume_operators AS
SELECT COALESCE(operator_name, 'Unknown') AS operator,
    count(*) AS plume_count,
    count(DISTINCT api) AS well_sites,
    round(sum(emission_rate), 0) AS total_emission_kg_hr,
    round(avg(emission_rate), 1) AS avg_emission_kg_hr,
    sum(CASE WHEN classification = 'unlit' THEN 1 ELSE 0 END) AS unlit_count,
    sum(CASE WHEN classification = 'flaring' THEN 1 ELSE 0 END) AS flaring_count
FROM plume_attributed
WHERE api IS NOT NULL
GROUP BY 1
ORDER BY total_emission_kg_hr DESC;
