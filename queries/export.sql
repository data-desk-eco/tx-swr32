LOAD spatial;

COPY (
    SELECT
        fs.flare_id, fs.lat, fs.lon, fs.detection_days,
        CAST(fs.first_detected AS VARCHAR) AS first_detected,
        CAST(fs.last_detected AS VARCHAR) AS last_detected,
        fs.near_excluded_facility,
        COALESCE(so.operator_name, 'Unknown') AS operator_name,
        so.confidence,
        round(so.nearest_permit_km, 3) AS nearest_permit_km,
        fl.name AS permit_name, p.site_name,
        d.dark_days, d.total_days,
        round(100.0 * d.dark_days / NULLIF(d.total_days, 0), 1) AS dark_pct,
        round(d.total_rh_mw, 1) AS total_rh_mw,
        round(d.avg_rh_mw, 2) AS avg_rh_mw
    FROM flaring.sites fs
    LEFT JOIN flaring.site_operators so USING (flare_id)
    LEFT JOIN (
        SELECT DISTINCT ON (filing_no) filing_no, name
        FROM flaring.permit_locations ORDER BY filing_no
    ) fl ON fl.filing_no = so.nearest_filing_no
    LEFT JOIN rrc.permits p ON p.filing_no = so.nearest_filing_no
    LEFT JOIN (
        SELECT flare_id,
            count(*) FILTER (WHERE is_dark) AS dark_days,
            count(*) AS total_days,
            sum(rh_mw) AS total_rh_mw,
            avg(rh_mw) FILTER (WHERE rh_mw > 0) AS avg_rh_mw
        FROM flaring.dark_flares GROUP BY flare_id
    ) d USING (flare_id)
) TO 'web/data/flares.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
    SELECT
        sl.flare_id, sl.lease_district, sl.lease_number, sl.oil_gas_code, sl.well_count,
        round(COALESCE(lf.reported_mcf, 0), 0) AS reported_flared_mcf,
        round(COALESCE(lf.unpermitted_mcf, 0), 0) AS unpermitted_flared_mcf,
        COALESCE(lf.permitted_days, 0) AS permitted_days,
        COALESCE(lf.total_days, 0) AS total_days,
        lf.operator_name AS lease_operator, lf.lease_name
    FROM flaring.site_leases sl
    LEFT JOIN (
        SELECT lease_district, lease_number,
            sum(reported_flared_mcf) AS reported_mcf,
            sum(unpermitted_flared_mcf) AS unpermitted_mcf,
            sum(permit_days) AS permitted_days,
            count(*) * 30 AS total_days,
            mode(operator_name) AS operator_name,
            mode(lease_name) AS lease_name
        FROM flaring.lease_flaring GROUP BY 1, 2
    ) lf ON LPAD(lf.lease_district, 2, '0') = LPAD(sl.lease_district, 2, '0')
        AND LPAD(lf.lease_number, 6, '0') = LPAD(sl.lease_number, 6, '0')
) TO 'web/data/flare_leases.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
    SELECT
        fl.latitude, fl.longitude, fl.name, fl.county, fl.district, fl.release_type,
        p.operator_name,
        count(DISTINCT fl.filing_no) AS n_filings,
        min(p.effective_dt) AS earliest_effective,
        max(p.expiration_dt) AS latest_expiration,
        round(max(COALESCE(plm.release_rate_mcf_day, 0)), 0) AS max_release_rate_mcf_day,
        sum(GREATEST(COALESCE(p.expiration_dt - p.effective_dt, 0), 0)) AS total_permitted_days
    FROM flaring.permit_locations fl
    JOIN rrc.permits p ON p.filing_no = fl.filing_no
    LEFT JOIN (
        SELECT filing_no, sum(TRY_CAST(requested_release_rate_mcf_day AS DOUBLE)) AS release_rate_mcf_day
        FROM rrc.permit_leases GROUP BY filing_no
    ) plm ON plm.filing_no = fl.filing_no
    WHERE fl.latitude IS NOT NULL AND fl.longitude IS NOT NULL
    GROUP BY fl.latitude, fl.longitude, fl.name, fl.county, fl.district,
        fl.release_type, p.operator_name
) TO 'web/data/permits.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
    SELECT plume_id, source, satellite, date, latitude, longitude,
        round(emission_rate, 1) AS emission_rate,
        round(emission_uncertainty, 1) AS emission_uncertainty,
        sector, classification, operator_name,
        vnf_flare_id, round(vnf_distance_km, 3) AS vnf_distance_km
    FROM flaring.plumes
    WHERE latitude BETWEEN 30.0 AND 33.5 AND longitude BETWEEN -104.5 AND -100.0
) TO 'web/data/plumes.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
    SELECT v.flare_id, CAST(v.date AS VARCHAR) AS date,
        round(v.rh_mw, 2) AS rh_mw,
        COALESCE(df.is_dark, TRUE) AS is_dark
    FROM raw.vnf v
    JOIN flaring.sites fs USING (flare_id)
    LEFT JOIN flaring.dark_flares df ON df.flare_id = v.flare_id AND df.date = v.date
    WHERE v.detected AND v.date >= fs.first_detected
) TO 'web/data/detections.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
