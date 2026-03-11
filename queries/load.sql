INSTALL spatial; LOAD spatial;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS rrc;
CREATE SCHEMA IF NOT EXISTS flaring;

-- SWR 32 permits (parse "Oil Lease-08-43066" → type, district, number)
CREATE OR REPLACE TABLE raw.permits AS
SELECT * REPLACE (replace(operator_name, '&amp;', '&') AS operator_name),
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 1) END AS property_type,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 2) END AS lease_district,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 3) END AS lease_number
FROM read_csv('data/filings.csv', delim='\t', header=true, all_varchar=true);

-- Wells
CREATE OR REPLACE TABLE raw.wells AS
SELECT *,
    CASE WHEN latitude != 0 AND longitude != 0 THEN ST_Point(longitude, latitude) END AS geom
FROM read_csv('data/wells.csv', header=true, auto_detect=true);

-- Operators
CREATE OR REPLACE TABLE raw.operators AS
SELECT * FROM read_csv('data/operators.csv', header=true, auto_detect=true);

-- Permit details (parsed from detail HTML pages)
CREATE OR REPLACE TABLE raw.permit_details AS
SELECT * FROM read_csv('data/permit_details.csv', header=true, all_varchar=true);

-- Permit properties (leases per filing)
CREATE OR REPLACE TABLE raw.permit_properties AS
SELECT * FROM read_csv('data/permit_properties.csv', header=true, all_varchar=true);

-- Flare locations (permitted flare GPS coordinates)
CREATE OR REPLACE TABLE raw.flare_locations AS
SELECT * REPLACE (latitude::DOUBLE AS latitude, longitude::DOUBLE AS longitude),
    CASE WHEN latitude::DOUBLE != 0 AND longitude::DOUBLE != 0
         THEN ST_Point(longitude::DOUBLE, latitude::DOUBLE) END AS geom
FROM read_csv('data/flare_locations.csv', header=true, all_varchar=true);

-- VNF: aggregate individual site profiles (nighttime, per site × day)
CREATE OR REPLACE TABLE raw.vnf AS
WITH profiles AS (
    SELECT
        flare_id::INTEGER AS flare_id,
        Date_Mscan::DATE AS date,
        Lat_GMTCO::DOUBLE AS lat, Lon_GMTCO::DOUBLE AS lon,
        Cloud_Mask::INTEGER AS cloud_mask,
        QF_Detect::INTEGER AS qf_detect,
        CASE WHEN RH < 999999 THEN RH::DOUBLE END AS rh,
        CASE WHEN Temp_BB < 999999 THEN Temp_BB::DOUBLE END AS temp_bb
    FROM read_csv('data/vnf_profiles/site_*.csv',
        header=true, auto_detect=true, filename=false)
    WHERE Sunlit = 0
)
SELECT flare_id,
    AVG(lat) AS lat, AVG(lon) AS lon, date,
    BOOL_OR(cloud_mask = 0) AS clear,
    BOOL_OR(qf_detect > 0 AND qf_detect < 999999) AS detected,
    MAX(CASE WHEN qf_detect > 0 AND qf_detect < 999999 THEN rh END) AS rh_mw,
    MAX(CASE WHEN qf_detect > 0 AND qf_detect < 999999 THEN temp_bb END) AS temp_k,
    COUNT(*) AS n_passes,
    ST_Point(AVG(lon), AVG(lat)) AS geom
FROM profiles
GROUP BY flare_id, date;

-- PDQ gas disposition (only rows with flaring/venting, code 04)
CREATE OR REPLACE TABLE raw.gas_disposition AS
SELECT
    OIL_GAS_CODE AS oil_gas_code, DISTRICT_NO AS district_no, LEASE_NO AS lease_no,
    CYCLE_YEAR AS cycle_year, CYCLE_MONTH AS cycle_month,
    OPERATOR_NO AS operator_no, FIELD_NO AS field_no,
    TRY_CAST(LEASE_GAS_DISPCD04_VOL AS DOUBLE) AS lease_gas_dispcd04_vol,
    TRY_CAST(LEASE_CSGD_DISPCDE04_VOL AS DOUBLE) AS lease_csgd_dispcde04_vol,
    COALESCE(TRY_CAST(LEASE_GAS_DISPCD01_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_GAS_DISPCD02_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_GAS_DISPCD03_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_GAS_DISPCD04_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_GAS_DISPCD05_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_GAS_DISPCD06_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_GAS_DISPCD07_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_GAS_DISPCD08_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_GAS_DISPCD09_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_GAS_DISPCD99_VOL AS DOUBLE),0) AS lease_gas_total_vol,
    COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE01_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE02_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE03_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE04_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE05_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE06_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE07_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE08_VOL AS DOUBLE),0) + COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE99_VOL AS DOUBLE),0) AS lease_csgd_total_vol,
    DISTRICT_NAME AS district_name, LEASE_NAME AS lease_name,
    OPERATOR_NAME AS operator_name, FIELD_NAME AS field_name
FROM read_csv('data/pdq/OG_LEASE_CYCLE_DISP_DATA_TABLE.dsv',
    delim='}', header=true, all_varchar=true, ignore_errors=true)
WHERE COALESCE(TRY_CAST(LEASE_GAS_DISPCD04_VOL AS DOUBLE), 0) > 0
   OR COALESCE(TRY_CAST(LEASE_CSGD_DISPCDE04_VOL AS DOUBLE), 0) > 0;

-- PDQ lease summary master
CREATE OR REPLACE TABLE raw.pdq_leases AS
SELECT
    OIL_GAS_CODE AS oil_gas_code, DISTRICT_NO AS district_no, LEASE_NO AS lease_no,
    OPERATOR_NO AS operator_no, FIELD_NO AS field_no,
    DISTRICT_NAME AS district_name, LEASE_NAME AS lease_name,
    OPERATOR_NAME AS operator_name, FIELD_NAME AS field_name,
    CYCLE_YEAR_MONTH_MIN::VARCHAR AS cycle_year_month_min,
    CYCLE_YEAR_MONTH_MAX::VARCHAR AS cycle_year_month_max
FROM read_csv('data/pdq/OG_SUMMARY_MASTER_LARGE_DATA_TABLE.dsv',
    delim='}', header=true, all_varchar=true, ignore_errors=true);

-- EPA GHGRP non-upstream facility exclusion zones
CREATE OR REPLACE TABLE raw.excluded_facilities AS
SELECT *,
    CASE WHEN latitude != 0 AND longitude != 0 THEN ST_Point(longitude, latitude) END AS geom
FROM read_csv('data/excluded_facilities.csv', header=true, auto_detect=true);

-- Methane plumes (Carbon Mapper + IMEO)
CREATE OR REPLACE TABLE raw.plumes AS
SELECT
    plume_id, 'cm' AS source, platform AS satellite,
    datetime::DATE AS date,
    plume_latitude::DOUBLE AS latitude, plume_longitude::DOUBLE AS longitude,
    emission_auto::DOUBLE AS emission_rate,
    emission_uncertainty_auto::DOUBLE AS emission_uncertainty,
    CASE
        WHEN ipcc_sector ILIKE '%oil%' OR ipcc_sector ILIKE '%gas%' THEN 'og'
        WHEN ipcc_sector ILIKE '%coal%' THEN 'coal'
        WHEN ipcc_sector ILIKE '%waste%' THEN 'waste'
        WHEN ipcc_sector IS NOT NULL AND ipcc_sector != '' THEN 'other'
    END AS sector,
    ST_Point(plume_longitude::DOUBLE, plume_latitude::DOUBLE) AS geom
FROM read_csv('data/plumes_cm.csv', header=true, all_varchar=true, quote='"')
WHERE plume_latitude IS NOT NULL AND plume_longitude IS NOT NULL;

INSERT INTO raw.plumes
SELECT
    plume_id, 'imeo' AS source, satellite, date,
    latitude, longitude, emission_rate, emission_uncertainty,
    CASE
        WHEN sector ILIKE '%oil%' OR sector ILIKE '%gas%' OR sector = 'og' THEN 'og'
        WHEN sector ILIKE '%coal%' THEN 'coal'
        WHEN sector ILIKE '%waste%' THEN 'waste'
        WHEN sector IS NOT NULL AND sector != '' THEN 'other'
    END,
    ST_Point(longitude, latitude)
FROM read_csv('data/plumes_imeo.csv', header=true, auto_detect=true)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- OTLS survey polygons (statewide)
CREATE OR REPLACE TABLE raw.surveys AS
SELECT ABSTRACT_N AS abstract_n, ABSTRACT_L AS abstract_l,
    LEVEL1_SUR AS survey_name, LEVEL2_BLO AS block, LEVEL3_SUR AS section,
    LEFT(ABSTRACT_N, 3) AS county_fips, geom
FROM ST_Read('data/survALLp.shp');

-- Spatial indexes
CREATE INDEX idx_wells_geom ON raw.wells USING RTREE (geom);
CREATE INDEX idx_vnf_geom ON raw.vnf USING RTREE (geom);
CREATE INDEX idx_flare_loc_geom ON raw.flare_locations USING RTREE (geom);
CREATE INDEX idx_plumes_geom ON raw.plumes USING RTREE (geom);
CREATE INDEX idx_surveys_geom ON raw.surveys USING RTREE (geom);
