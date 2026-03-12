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

-- Leases parquet (flare ↔ lease matches via OTLS boundaries)
COPY (
    SELECT
        sl.flare_id, sl.lease_district, sl.lease_number, sl.oil_gas_code, sl.well_count,
        round(COALESCE(lf.reported_mcf, 0), 0) AS reported_flared_mcf,
        lf.operator_name AS lease_operator, lf.lease_name
    FROM (
        SELECT fs.flare_id, ll.lease_district, ll.lease_number, ll.oil_gas_code, ll.well_count
        FROM sites fs
        JOIN rrc.leases ll
            ON fs.lon BETWEEN ST_XMin(ll.geom) AND ST_XMax(ll.geom)
            AND fs.lat BETWEEN ST_YMin(ll.geom) AND ST_YMax(ll.geom)
            AND ST_Contains(ll.geom, fs.geom)
        WHERE NOT fs.near_excluded_facility
    ) sl
    LEFT JOIN (
        SELECT lease_district, lease_number,
            sum(total_flared_mcf) AS reported_mcf,
            mode(operator_name) AS operator_name,
            mode(lease_name) AS lease_name
        FROM rrc.production
        WHERE district IN ('7B','7C','08','8A')
        GROUP BY 1, 2
    ) lf ON LPAD(lf.lease_district, 2, '0') = LPAD(sl.lease_district, 2, '0')
        AND LPAD(lf.lease_number, 6, '0') = LPAD(sl.lease_number, 6, '0')
) TO 'web/data/leases.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

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
