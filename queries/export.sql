LOAD spatial;

SET VARIABLE start_date = '2021-01-01'::DATE;
SET VARIABLE lat_min = 30.0;
SET VARIABLE lat_max = 33.5;
SET VARIABLE lon_min = -104.5;
SET VARIABLE lon_max = -100.0;
SET VARIABLE nm_border_lon = -103.064;  -- TX-NM border longitude (above 32°N)

-- Normalize lease numbers to 6-digit zero-padded strings
CREATE OR REPLACE MACRO normalize_lease(n) AS LPAD(n, 6, '0');

-- Reusable Permian bbox filter (lat, lon columns vary by table)
CREATE OR REPLACE MACRO in_permian(lat, lon) AS
    lat BETWEEN getvariable('lat_min') AND getvariable('lat_max')
    AND lon BETWEEN getvariable('lon_min') AND getvariable('lon_max')
    AND (lat <= 32.0 OR lon >= getvariable('nm_border_lon'));

-- VNF flare sites (one row per site, Permian bbox)
CREATE OR REPLACE TEMP TABLE sites AS
SELECT
    f.flare_id, f.lat, f.lon, f.geom,
    f.first_detected, f.last_detected, f.detection_days
FROM (
    SELECT flare_id,
        AVG(lat) AS lat, AVG(lon) AS lon,
        ST_Point(AVG(lon), AVG(lat)) AS geom,
        MIN(date) AS first_detected, MAX(date) AS last_detected,
        COUNT(*) AS detection_days
    FROM raw.vnf WHERE detected AND date >= getvariable('start_date')
    GROUP BY flare_id
) f
WHERE in_permian(f.lat, f.lon);

-- Flares parquet
COPY (
    SELECT
        fs.flare_id, fs.lat, fs.lon, fs.detection_days,
        CAST(fs.first_detected AS VARCHAR) AS first_detected,
        CAST(fs.last_detected AS VARCHAR) AS last_detected,
        round(d.total_rh_mw, 1) AS total_rh_mw,
        round(d.avg_rh_mw, 2) AS avg_rh_mw
    FROM sites fs
    LEFT JOIN (
        SELECT flare_id,
            sum(rh_mw) AS total_rh_mw,
            avg(rh_mw) FILTER (WHERE rh_mw > 0) AS avg_rh_mw
        FROM raw.vnf WHERE detected
        GROUP BY flare_id
    ) d USING (flare_id)
) TO 'web/data/flares.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Detections parquet (per-flare daily timeseries)
COPY (
    SELECT v.flare_id, CAST(v.date AS VARCHAR) AS date,
        round(v.rh_mw, 2) AS rh_mw
    FROM raw.vnf v
    JOIN sites fs USING (flare_id)
    WHERE v.detected AND v.date >= fs.first_detected
) TO 'web/data/detections.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Lease production aggregates (shared by leases + wells parquets)
-- Filtered to start_date+ to match VNF analysis window
-- Flaring numerator: from disposition table (months with flaring only)
-- Production denominator: from full production table (ALL months, not just flaring months)
CREATE OR REPLACE TEMP TABLE lease_production AS
WITH flared AS (
    SELECT district, lease_number,
        sum(total_flared_mcf) AS total_flared_mcf,
        mode(operator_name) AS operator_name,
        mode(lease_name) AS lease_name
    FROM rrc.production
    WHERE district IN ('6E','7B','7C','08','8A')
      AND make_date(year, month, 1) >= getvariable('start_date')
    GROUP BY 1, 2
),
produced AS (
    SELECT dm.rrc_district AS district, lp.lease_no AS lease_number,
        sum(lp.lease_gas_prod_vol + lp.lease_csgd_prod_vol) AS total_gas_prod_mcf
    FROM raw.lease_production lp
    JOIN rrc.district_map dm ON dm.pdq_district = lp.district_no
    WHERE dm.rrc_district IN ('6E','7B','7C','08','8A')
      AND make_date(lp.cycle_year::INT, lp.cycle_month::INT, 1) >= getvariable('start_date')
    GROUP BY 1, 2
)
SELECT f.district, f.lease_number,
    f.total_flared_mcf,
    COALESCE(p.total_gas_prod_mcf, f.total_flared_mcf) AS total_gas_prod_mcf,
    f.operator_name, f.lease_name
FROM flared f
LEFT JOIN produced p
    ON p.district = f.district
    AND normalize_lease(p.lease_number) = normalize_lease(f.lease_number);

-- Wells near flare sites (within 375m bbox pre-filter, reused by leases/wells/gatherers/production)
CREATE OR REPLACE TEMP TABLE flare_wells AS
SELECT DISTINCT w.api, w.oil_gas_code, w.lease_district, w.lease_number
FROM sites fs
JOIN raw.wells w
    ON w.longitude BETWEEN fs.lon - 0.0034 AND fs.lon + 0.0034
    AND w.latitude BETWEEN fs.lat - 0.0034 AND fs.lat + 0.0034
    AND w.latitude != 0 AND w.longitude != 0;

-- Distinct leases linked to flare sites (for filtering large tables)
CREATE OR REPLACE TEMP TABLE flare_leases AS
SELECT DISTINCT lease_district, normalize_lease(lease_number) AS lease_number
FROM flare_wells;

-- Leases parquet (flare ↔ lease matches via nearby wells within 375m)
COPY (
    WITH fl AS (
        SELECT DISTINCT fs.flare_id, w.oil_gas_code, w.lease_district, w.lease_number
        FROM sites fs
        JOIN raw.wells w
            ON w.longitude BETWEEN fs.lon - 0.0034 AND fs.lon + 0.0034
            AND w.latitude BETWEEN fs.lat - 0.0034 AND fs.lat + 0.0034
            AND w.latitude != 0 AND w.longitude != 0
    )
    SELECT
        fl.flare_id, fl.lease_district, fl.lease_number, fl.oil_gas_code,
        COALESCE(wc.well_count, 0) AS well_count,
        round(COALESCE(lp.total_flared_mcf, 0), 0) AS reported_flared_mcf,
        lp.operator_name AS lease_operator, lp.lease_name
    FROM fl
    LEFT JOIN (
        SELECT oil_gas_code, lease_district, lease_number, count(*) AS well_count
        FROM raw.wells WHERE latitude != 0 GROUP BY 1, 2, 3
    ) wc USING (oil_gas_code, lease_district, lease_number)
    LEFT JOIN lease_production lp
        ON lp.district = fl.lease_district
        AND normalize_lease(lp.lease_number) = normalize_lease(fl.lease_number)
) TO 'web/data/leases.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Lease monthly production time series (for sparklines in well detail cards)
-- Only for flare-linked leases with reported flaring, filtered to start_date+
COPY (
    SELECT
        p.district AS lease_district, p.lease_number,
        make_date(p.year, p.month, 1)::VARCHAR AS date,
        round(p.total_flared_mcf, 0)::INT AS flared_mcf,
        round(CASE WHEN p.total_gas_prod_mcf > 0 THEN p.total_gas_prod_mcf
                   ELSE p.total_disposed_mcf END, 0)::INT AS produced_mcf
    FROM rrc.production p
    JOIN lease_production lp
        ON lp.district = p.district
        AND normalize_lease(lp.lease_number) = normalize_lease(p.lease_number)
    SEMI JOIN flare_leases fl
        ON fl.lease_district = p.district
        AND fl.lease_number = normalize_lease(p.lease_number)
    WHERE p.district IN ('6E','7B','7C','08','8A')
      AND lp.total_flared_mcf > 0
      AND make_date(p.year, p.month, 1) >= getvariable('start_date')
    ORDER BY p.district, p.lease_number, p.year, p.month
) TO 'web/data/production.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Permits parquet (one row per filing, excludes gas plants)
COPY (
    SELECT
        fl.filing_no, fl.latitude, fl.longitude, fl.name, fl.county, fl.district,
        fl.release_type, p.operator_name, p.status,
        CAST(p.effective_dt AS VARCHAR) AS effective_dt,
        CAST(p.expiration_dt AS VARCHAR) AS expiration_dt,
        round(COALESCE(plm.release_rate_mcf_day, 0), 0) AS release_rate_mcf_day,
        p.exception_reasons
    FROM raw.flare_locations fl
    JOIN rrc.permits p ON p.filing_no = fl.filing_no
    LEFT JOIN (
        SELECT filing_no, sum(TRY_CAST(requested_release_rate_mcf_day AS DOUBLE)) AS release_rate_mcf_day
        FROM rrc.permit_leases GROUP BY filing_no
    ) plm ON plm.filing_no = fl.filing_no
    WHERE fl.latitude IS NOT NULL AND fl.longitude IS NOT NULL
      AND fl.filing_no NOT IN (SELECT filing_no FROM raw.permits WHERE property_type = 'Gas Plant')
      AND COALESCE(fl.facility_type, '') NOT ILIKE '%gas plant%'
      AND in_permian(fl.latitude, fl.longitude)
) TO 'web/data/permits.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Plumes parquet
COPY (
    SELECT plume_id, source, satellite, date, latitude, longitude,
        round(emission_rate, 1) AS emission_rate,
        round(emission_uncertainty, 1) AS emission_uncertainty,
        sector
    FROM raw.plumes
    WHERE in_permian(latitude, longitude)
) TO 'web/data/plumes.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Wells parquet (only wells near flare sites, with per-lease flaring metrics)
COPY (
    SELECT w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number,
        COALESCE(o.operator_name, 'Unknown') AS operator_name,
        w.latitude, w.longitude,
        round(COALESCE(lp.total_flared_mcf, 0), 0) AS flared_mcf,
        round(COALESCE(lp.total_gas_prod_mcf, 0), 0) AS produced_mcf,
        CASE WHEN lp.total_gas_prod_mcf > 0
             THEN round(100.0 * lp.total_flared_mcf / lp.total_gas_prod_mcf, 1)
             ELSE NULL END AS flaring_intensity_pct,
        lp.lease_name
    FROM raw.wells w
    SEMI JOIN flare_wells fw USING (api)
    LEFT JOIN raw.operators o ON o.operator_number = w.operator_no
    LEFT JOIN lease_production lp
        ON lp.district = w.lease_district
        AND normalize_lease(lp.lease_number) = normalize_lease(w.lease_number)
    WHERE w.latitude != 0 AND w.longitude != 0
        AND in_permian(w.latitude, w.longitude)
) TO 'web/data/wells.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Gatherer/purchaser/nominator parquet (only flare-linked leases, deduplicated per lease×type×name×current)
COPY (
    SELECT
        g.oil_gas_code, g.district, normalize_lease(g.lease_rrcid::VARCHAR) AS lease_number,
        CASE g.type_code WHEN 'G' THEN 'Gatherer' WHEN 'H' THEN 'Purchaser' WHEN 'I' THEN 'Nominator' ELSE g.type_code END AS type,
        MAX(round(g.percentage * 100, 2)) AS percentage,
        g.gpn_number,
        COALESCE(o.operator_name, 'Unknown (' || g.gpn_number || ')') AS gpn_name,
        g.is_current::VARCHAR AS is_current,
        MIN(NULLIF(g.effective_date, '')) AS first_date,
        MAX(NULLIF(g.effective_date, '')) AS last_date
    FROM raw.gatherers g
    SEMI JOIN flare_leases fl
        ON fl.lease_district = g.district
        AND fl.lease_number = normalize_lease(g.lease_rrcid::VARCHAR)
    LEFT JOIN raw.operators o ON normalize_lease(o.operator_number::VARCHAR) = normalize_lease(g.gpn_number)
    WHERE g.district IN ('6E','7B','7C','08','8A')
    GROUP BY g.oil_gas_code, g.district, g.lease_rrcid, g.type_code, g.gpn_number, o.operator_name, g.is_current
) TO 'web/data/gatherers.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- R-3 gas processing facilities parquet
COPY (
    SELECT serial_number, facility_name, plant_type, latitude, longitude
    FROM raw.excluded_facilities
    WHERE in_permian(latitude, longitude)
) TO 'web/data/facilities.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
