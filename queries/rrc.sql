LOAD spatial;

-- PDQ district_no (numeric) → RRC district ID (alphanumeric)
CREATE OR REPLACE TABLE rrc.district_map AS
SELECT * FROM (VALUES
    ('01','01'), ('02','02'), ('03','03'), ('04','04'), ('05','05'), ('06','06'),
    ('07','6E'), ('08','7B'), ('09','7C'), ('10','08'), ('11','8A'), ('12','8B'),
    ('13','09'), ('14','10')
) AS t(pdq_district, rrc_district);

-- Permits: merged filings + detail pages with parsed dates
CREATE OR REPLACE TABLE rrc.permits AS
SELECT
    p.filing_no, p.excep_seq, p.operator_no,
    COALESCE(pd.operator, p.operator_name) AS operator_name,
    p.property, p.property_type, p.lease_district, p.lease_number, p.fv_district,
    COALESCE(pd.exception_status, p.status) AS status,
    pd.filing_type, pd.site_name, pd.exception_reasons,
    COALESCE(TRY_STRPTIME(pd.requested_effective_date, '%m/%d/%Y'),
             TRY_STRPTIME(p.effective_dt, '%m/%d/%Y'))::DATE AS effective_dt,
    COALESCE(TRY_STRPTIME(pd.requested_expiration_date, '%m/%d/%Y'),
             TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y'))::DATE AS expiration_dt
FROM raw.permits p
LEFT JOIN raw.permit_details pd ON pd.filing_no = p.filing_no;

-- Permit → lease mapping (flattens commingle permits to underlying leases)
CREATE OR REPLACE TABLE rrc.permit_leases AS
SELECT pp.filing_no, pp.property_type,
    pp.district AS lease_district, pp.property_id AS lease_number,
    pp.lease_name, pp.requested_release_rate_mcf_day
FROM raw.permit_properties pp
WHERE pp.property_type IN ('Oil Lease', 'Gas Lease', 'Drilling Permit')
  AND pp.property_id IS NOT NULL AND pp.property_id != '';

-- Well → OTLS survey spatial join
CREATE OR REPLACE TABLE rrc.well_surveys AS
SELECT DISTINCT
    w.oil_gas_code, w.lease_district, w.lease_number, w.api,
    s.abstract_n, s.abstract_l, s.survey_name, s.block, s.section
FROM raw.wells w
JOIN raw.surveys s ON ST_Contains(s.geom, ST_Point(w.longitude, w.latitude))
WHERE w.latitude != 0 AND w.longitude != 0;

-- Lease boundaries: union of OTLS survey polygons per lease
-- Leases spanning >10km excluded as data errors
CREATE OR REPLACE TABLE rrc.leases AS
WITH lease_surveys AS (
    SELECT DISTINCT ws.oil_gas_code, ws.lease_district, ws.lease_number,
        ws.abstract_n, s.geom
    FROM rrc.well_surveys ws
    JOIN raw.surveys s ON s.abstract_n = ws.abstract_n
),
agg AS (
    SELECT oil_gas_code, lease_district, lease_number,
        COUNT(DISTINCT abstract_n) AS survey_count,
        ST_Union_Agg(geom) AS geom
    FROM lease_surveys GROUP BY 1, 2, 3
)
SELECT a.oil_gas_code, a.lease_district, a.lease_number,
    a.survey_count, wc.well_count, a.geom
FROM agg a
JOIN (
    SELECT oil_gas_code, lease_district, lease_number, count(*) AS well_count
    FROM raw.wells WHERE latitude != 0 GROUP BY 1, 2, 3
) wc USING (oil_gas_code, lease_district, lease_number)
WHERE greatest(ST_XMax(a.geom) - ST_XMin(a.geom),
               ST_YMax(a.geom) - ST_YMin(a.geom)) * 111 < 10;

CREATE INDEX idx_leases_geom ON rrc.leases USING RTREE (geom);

-- Monthly reported flaring by lease
CREATE OR REPLACE TABLE rrc.production AS
SELECT
    gd.oil_gas_code, dm.rrc_district AS district,
    gd.lease_no AS lease_number,
    gd.cycle_year::INT AS year, gd.cycle_month::INT AS month,
    gd.operator_no, gd.operator_name, gd.lease_name, gd.field_name,
    COALESCE(gd.lease_gas_dispcd04_vol, 0) AS gas_flared_mcf,
    COALESCE(gd.lease_csgd_dispcde04_vol, 0) AS csgd_flared_mcf,
    COALESCE(gd.lease_gas_dispcd04_vol, 0) + COALESCE(gd.lease_csgd_dispcde04_vol, 0) AS total_flared_mcf,
    COALESCE(gd.lease_gas_total_vol, 0) + COALESCE(gd.lease_csgd_total_vol, 0) AS total_gas_prod_mcf
FROM raw.gas_disposition gd
LEFT JOIN rrc.district_map dm ON dm.pdq_district = gd.district_no;
