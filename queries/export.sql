LOAD spatial;

SET VARIABLE start_date = '2021-01-01'::DATE;
SET VARIABLE lat_min = 30.0;
SET VARIABLE lat_max = 33.5;
SET VARIABLE lon_min = -104.5;
SET VARIABLE lon_max = -100.0;
SET VARIABLE nm_border_lon = -103.064;  -- TX-NM border longitude (above 32°N)

-- Reusable Permian bbox filter (lat, lon columns vary by table)
CREATE OR REPLACE MACRO in_permian(lat, lon) AS
    lat BETWEEN getvariable('lat_min') AND getvariable('lat_max')
    AND lon BETWEEN getvariable('lon_min') AND getvariable('lon_max')
    AND (lat <= 32.0 OR lon >= getvariable('nm_border_lon'));

-- VNF flare sites (one row per site, Permian bbox, exclusion flag)
CREATE OR REPLACE TEMP TABLE sites AS
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
WHERE in_permian(f.lat, f.lon);

-- Flares parquet
COPY (
    SELECT
        fs.flare_id, fs.lat, fs.lon, fs.detection_days,
        CAST(fs.first_detected AS VARCHAR) AS first_detected,
        CAST(fs.last_detected AS VARCHAR) AS last_detected,
        fs.near_excluded_facility,
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

-- Lease production aggregates (shared by leases parquet and footprints export)
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
    AND LPAD(p.lease_number, 6, '0') = LPAD(f.lease_number, 6, '0');

-- Leases parquet (flare ↔ lease matches via OTLS boundaries)
COPY (
    SELECT
        sl.flare_id, sl.lease_district, sl.lease_number, sl.oil_gas_code, sl.well_count,
        round(COALESCE(lp.total_flared_mcf, 0), 0) AS reported_flared_mcf,
        lp.operator_name AS lease_operator, lp.lease_name
    FROM (
        SELECT fs.flare_id, ll.lease_district, ll.lease_number, ll.oil_gas_code, ll.well_count
        FROM sites fs
        JOIN rrc.leases ll
            ON fs.lon BETWEEN ST_XMin(ll.geom) AND ST_XMax(ll.geom)
            AND fs.lat BETWEEN ST_YMin(ll.geom) AND ST_YMax(ll.geom)
            AND ST_Contains(ll.geom, fs.geom)
        WHERE NOT fs.near_excluded_facility
    ) sl
    LEFT JOIN lease_production lp
        ON lp.district = sl.lease_district
        AND LPAD(lp.lease_number, 6, '0') = LPAD(sl.lease_number, 6, '0')
) TO 'web/data/leases.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Lease footprints parquet (merged by surface area to avoid opacity stacking)
-- Vertically stacked leases share the same OTLS survey abstracts but have different
-- lease numbers (depth intervals). Group by (district, abstract set) to flatten them
-- into one polygon per unique surface area, averaging flaring intensity across stacks.
COPY (
    WITH lease_abstracts AS (
        SELECT ws.lease_district, ws.lease_number,
            list(DISTINCT ws.abstract_n ORDER BY ws.abstract_n) AS abstracts
        FROM rrc.well_surveys ws
        GROUP BY 1, 2
    ),
    surface_groups AS (
        SELECT la.lease_district, la.abstracts,
            ST_Union_Agg(l.geom) AS geom,
            COUNT(*) AS lease_count,
            SUM(lp.total_flared_mcf) AS total_flared,
            SUM(lp.total_gas_prod_mcf) AS total_produced,
            list({
                d: la.lease_district,
                n: la.lease_number,
                name: lp.lease_name,
                op: lp.operator_name,
                wells: l.well_count,
                flared: round(lp.total_flared_mcf, 0)::INT,
                produced: round(lp.total_gas_prod_mcf, 0)::INT
            } ORDER BY lp.total_flared_mcf DESC) AS leases
        FROM lease_abstracts la
        JOIN rrc.leases l
            ON l.lease_district = la.lease_district
            AND l.lease_number = la.lease_number
        JOIN lease_production lp
            ON lp.district = la.lease_district
            AND LPAD(lp.lease_number, 6, '0') = LPAD(la.lease_number, 6, '0')
        WHERE la.lease_district IN ('6E','7B','7C','08','8A')
            AND lp.total_flared_mcf > 0
        GROUP BY la.lease_district, la.abstracts
    )
    SELECT lease_count,
        CASE WHEN total_produced > 0
             THEN round(100.0 * total_flared / total_produced, 1)
             ELSE NULL END AS flaring_intensity_pct,
        to_json(leases)::VARCHAR AS leases,
        ST_AsGeoJSON(ST_Simplify(geom, 0.001)) AS geometry
    FROM surface_groups
) TO 'web/data/lease_footprints.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Lease monthly production time series (for sparklines in lease detail)
-- Only for leases that appear in lease_footprints (have flaring), filtered to start_date+
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
        AND LPAD(lp.lease_number, 6, '0') = LPAD(p.lease_number, 6, '0')
    WHERE p.district IN ('6E','7B','7C','08','8A')
      AND lp.total_flared_mcf > 0
      AND make_date(p.year, p.month, 1) >= getvariable('start_date')
    ORDER BY p.district, p.lease_number, p.year, p.month
) TO 'web/data/lease_monthly.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

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

-- Wells parquet
COPY (
    SELECT w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number,
        COALESCE(o.operator_name, 'Unknown') AS operator_name,
        w.latitude, w.longitude
    FROM raw.wells w
    LEFT JOIN raw.operators o ON o.operator_number = w.operator_no
    WHERE w.latitude != 0 AND w.longitude != 0
        AND in_permian(w.latitude, w.longitude)
) TO 'web/data/wells.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
