LOAD spatial;

COPY (
    SELECT
        fs.flare_id, fs.lat, fs.lon,
        fs.detection_days,
        CAST(fs.first_detected AS VARCHAR) AS first_detected,
        CAST(fs.last_detected AS VARCHAR) AS last_detected,
        fs.near_excluded_facility,
        COALESCE(so.operator_name, 'Unknown') AS operator_name,
        so.confidence,
        round(so.nearest_permit_km, 3) AS nearest_permit_km,
        fl.name AS permit_name,
        pd.site_name,
        d.dark_days, d.total_days,
        round(100.0 * d.dark_days / NULLIF(d.total_days, 0), 1) AS dark_pct,
        round(d.total_rh_mw, 1) AS total_rh_mw,
        round(d.avg_rh_mw, 2) AS avg_rh_mw
    FROM flare_sites fs
    LEFT JOIN site_operators so USING (flare_id)
    LEFT JOIN (
        SELECT DISTINCT ON (filing_no) filing_no, name
        FROM flare_locations ORDER BY filing_no
    ) fl ON fl.filing_no = so.nearest_filing_no
    LEFT JOIN raw.permit_details pd ON pd.filing_no = so.nearest_filing_no
    LEFT JOIN (
        SELECT flare_id,
            sum(CASE WHEN is_dark THEN 1 ELSE 0 END) AS dark_days,
            count(*) AS total_days,
            sum(rh_mw) AS total_rh_mw,
            avg(rh_mw) AS avg_rh_mw
        FROM dark_flares GROUP BY flare_id
    ) d USING (flare_id)
) TO 'web/data/flares.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
    SELECT
        slm.flare_id,
        slm.lease_district, slm.lease_number, slm.oil_gas_code,
        slm.well_count,
        round(COALESCE(lf.reported_mcf, 0), 0) AS reported_flared_mcf,
        round(COALESCE(lf.unpermitted_mcf, 0), 0) AS unpermitted_flared_mcf,
        COALESCE(lf.permitted_days, 0) AS permitted_days,
        COALESCE(lf.total_days, 0) AS total_days,
        lf.operator_name AS lease_operator,
        lf.lease_name
    FROM site_lease_matches slm
    LEFT JOIN (
        SELECT lease_district, lease_number,
            sum(reported_flared_mcf) AS reported_mcf,
            sum(unpermitted_flared_mcf) AS unpermitted_mcf,
            sum(permit_days) AS permitted_days,
            count(*) * 30 AS total_days,
            mode(operator_name) AS operator_name,
            mode(lease_name) AS lease_name
        FROM lease_flaring
        GROUP BY lease_district, lease_number
    ) lf ON LPAD(lf.lease_district, 2, '0') = LPAD(slm.lease_district, 2, '0')
        AND LPAD(lf.lease_number, 6, '0') = LPAD(slm.lease_number, 6, '0')
) TO 'web/data/flare_leases.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
    SELECT
        fl.latitude, fl.longitude,
        fl.name, fl.county, fl.district,
        fl.release_type,
        p.operator_name,
        count(DISTINCT fl.filing_no) AS n_filings,
        min(TRY_STRPTIME(p.effective_dt, '%m/%d/%Y'))::DATE AS earliest_effective,
        max(TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y'))::DATE AS latest_expiration,
        round(max(COALESCE(plm.release_rate_mcf_day, 0)), 0) AS max_release_rate_mcf_day,
        sum(GREATEST(COALESCE(
            TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y')::DATE
            - TRY_STRPTIME(p.effective_dt, '%m/%d/%Y')::DATE, 0), 0)) AS total_permitted_days
    FROM flare_locations fl
    JOIN raw.permits p ON p.filing_no = fl.filing_no
    LEFT JOIN (
        SELECT filing_no, sum(TRY_CAST(requested_release_rate_mcf_day AS DOUBLE)) AS release_rate_mcf_day
        FROM permit_lease_map GROUP BY filing_no
    ) plm ON plm.filing_no = fl.filing_no
    WHERE fl.latitude IS NOT NULL AND fl.longitude IS NOT NULL
    GROUP BY fl.latitude, fl.longitude, fl.name, fl.county, fl.district,
        fl.release_type, p.operator_name
) TO 'web/data/permits.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
    SELECT
        plume_id, source, satellite, date, latitude, longitude,
        round(emission_rate, 1) AS emission_rate,
        round(emission_uncertainty, 1) AS emission_uncertainty,
        sector, classification,
        operator_name,
        vnf_flare_id,
        round(vnf_distance_km, 3) AS vnf_distance_km
    FROM plume_attributed
) TO 'web/data/plumes.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
