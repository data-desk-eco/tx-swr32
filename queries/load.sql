LOAD spatial;

-- SWR 32 permits (parse "Oil Lease-08-43066" -> type, district, number)
INSERT INTO raw.permits
SELECT * REPLACE (replace(operator_name, '&amp;', '&') AS operator_name),
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 1) END,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 2) END,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 3) END
FROM read_csv('data/filings.csv', delim='\t', header=true, all_varchar=true);

-- Wells (Permian, with locations)
INSERT INTO raw.wells
SELECT *, CASE WHEN latitude != 0 AND longitude != 0
               THEN ST_Point(longitude, latitude) END
FROM read_csv('data/wells.csv', header=true, auto_detect=true);

-- Operators
INSERT INTO raw.operators
SELECT * FROM read_csv('data/operators.csv', header=true, auto_detect=true);

-- Permit details (filing metadata from detail pages)
INSERT INTO raw.permit_details
SELECT * FROM read_csv('data/permit_details.csv', header=true, all_varchar=true);

-- Permit properties (leases/permits per filing from detail pages)
INSERT INTO raw.permit_properties
SELECT * FROM read_csv('data/permit_properties.csv', header=true, all_varchar=true);

-- Flare locations (all permitted flare GPS coordinates, including Gas Plant)
INSERT INTO raw.flare_locations
SELECT fl.*, CASE WHEN fl.latitude != 0 AND fl.longitude != 0
                  THEN ST_Point(fl.longitude, fl.latitude) END
FROM read_csv('data/flare_locations.csv', header=true, auto_detect=true) fl;

-- PDQ: lease-level gas disposition (vented/flared volumes)
-- Only load rows where gas was flared/vented (DISPCD04 > 0) or casinghead gas flared (DISPCDE04 > 0)
-- District mapping: 08=7B, 09=7C, 10=08, 11=8A, 13=09, 14=10
INSERT INTO raw.gas_disposition
SELECT
    OIL_GAS_CODE, DISTRICT_NO, LEASE_NO, CYCLE_YEAR, CYCLE_MONTH,
    OPERATOR_NO, FIELD_NO,
    NULLIF(LEASE_GAS_DISPCD04_VOL, '')::DOUBLE,
    NULLIF(LEASE_CSGD_DISPCDE04_VOL, '')::DOUBLE,
    COALESCE(NULLIF(LEASE_GAS_DISPCD01_VOL,'')::DOUBLE,0) + COALESCE(NULLIF(LEASE_GAS_DISPCD02_VOL,'')::DOUBLE,0)
      + COALESCE(NULLIF(LEASE_GAS_DISPCD03_VOL,'')::DOUBLE,0) + COALESCE(NULLIF(LEASE_GAS_DISPCD04_VOL,'')::DOUBLE,0)
      + COALESCE(NULLIF(LEASE_GAS_DISPCD05_VOL,'')::DOUBLE,0) + COALESCE(NULLIF(LEASE_GAS_DISPCD06_VOL,'')::DOUBLE,0)
      + COALESCE(NULLIF(LEASE_GAS_DISPCD07_VOL,'')::DOUBLE,0) + COALESCE(NULLIF(LEASE_GAS_DISPCD08_VOL,'')::DOUBLE,0)
      + COALESCE(NULLIF(LEASE_GAS_DISPCD09_VOL,'')::DOUBLE,0) + COALESCE(NULLIF(LEASE_GAS_DISPCD99_VOL,'')::DOUBLE,0),
    COALESCE(NULLIF(LEASE_CSGD_DISPCDE01_VOL,'')::DOUBLE,0) + COALESCE(NULLIF(LEASE_CSGD_DISPCDE02_VOL,'')::DOUBLE,0)
      + COALESCE(NULLIF(LEASE_CSGD_DISPCDE03_VOL,'')::DOUBLE,0) + COALESCE(NULLIF(LEASE_CSGD_DISPCDE04_VOL,'')::DOUBLE,0)
      + COALESCE(NULLIF(LEASE_CSGD_DISPCDE05_VOL,'')::DOUBLE,0) + COALESCE(NULLIF(LEASE_CSGD_DISPCDE06_VOL,'')::DOUBLE,0)
      + COALESCE(NULLIF(LEASE_CSGD_DISPCDE07_VOL,'')::DOUBLE,0) + COALESCE(NULLIF(LEASE_CSGD_DISPCDE08_VOL,'')::DOUBLE,0)
      + COALESCE(NULLIF(LEASE_CSGD_DISPCDE99_VOL,'')::DOUBLE,0),
    DISTRICT_NAME, LEASE_NAME, OPERATOR_NAME, FIELD_NAME
FROM read_csv('data/pdq/OG_LEASE_CYCLE_DISP_DATA_TABLE.dsv',
    delim='}', header=true, all_varchar=true, ignore_errors=true)
WHERE (NULLIF(LEASE_GAS_DISPCD04_VOL, '') IS NOT NULL AND LEASE_GAS_DISPCD04_VOL != '0')
   OR (NULLIF(LEASE_CSGD_DISPCDE04_VOL, '') IS NOT NULL AND LEASE_CSGD_DISPCDE04_VOL != '0');

-- PDQ: lease summary master (for lease name/operator lookups)
INSERT INTO raw.pdq_leases
SELECT
    OIL_GAS_CODE, DISTRICT_NO, LEASE_NO, OPERATOR_NO, FIELD_NO,
    DISTRICT_NAME, LEASE_NAME, OPERATOR_NAME, FIELD_NAME,
    CYCLE_YEAR_MONTH_MIN::VARCHAR, CYCLE_YEAR_MONTH_MAX::VARCHAR
FROM read_csv('data/pdq/OG_SUMMARY_MASTER_LARGE_DATA_TABLE.dsv',
    delim='}', header=true, all_varchar=true, ignore_errors=true);

-- VNF: read profiles with explicit types (avoids auto_detect on 1700 files)
-- Filter to permit era (Q4 2023+) and nighttime (sunlit=0)
SET VARIABLE vnf_start = '2023-10-01';

INSERT INTO raw.vnf
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

UPDATE raw.vnf SET geom = ST_Point(lon, lat) WHERE lat IS NOT NULL;

-- Non-upstream facility exclusion zones (EPA GHGRP)
INSERT INTO raw.excluded_facilities
SELECT *, CASE WHEN latitude != 0 AND longitude != 0
               THEN ST_Point(longitude, latitude) END
FROM read_csv('data/excluded_facilities.csv', header=true, auto_detect=true);

-- Carbon Mapper plumes
INSERT INTO raw.plumes
SELECT
    plume_id,
    'cm' AS source,
    platform AS satellite,
    datetime::DATE AS date,
    plume_latitude::DOUBLE AS latitude,
    plume_longitude::DOUBLE AS longitude,
    emission_auto::DOUBLE AS emission_rate,
    emission_uncertainty_auto::DOUBLE AS emission_uncertainty,
    CASE
        WHEN ipcc_sector ILIKE '%oil%' OR ipcc_sector ILIKE '%gas%' THEN 'og'
        WHEN ipcc_sector ILIKE '%coal%' THEN 'coal'
        WHEN ipcc_sector ILIKE '%waste%' THEN 'waste'
        WHEN ipcc_sector IS NOT NULL AND ipcc_sector != '' THEN 'other'
    END AS sector,
    ST_Point(plume_longitude::DOUBLE, plume_latitude::DOUBLE)
FROM read_csv('data/plumes_cm.csv', header=true, all_varchar=true, quote='"')
WHERE plume_latitude IS NOT NULL AND plume_longitude IS NOT NULL;

-- IMEO plumes (already filtered to Permian by fetch script)
INSERT INTO raw.plumes
SELECT
    plume_id,
    'imeo' AS source,
    satellite,
    date,
    latitude,
    longitude,
    emission_rate,
    emission_uncertainty,
    CASE
        WHEN sector ILIKE '%oil%' OR sector ILIKE '%gas%' OR sector = 'og' THEN 'og'
        WHEN sector ILIKE '%coal%' THEN 'coal'
        WHEN sector ILIKE '%waste%' THEN 'waste'
        WHEN sector IS NOT NULL AND sector != '' THEN 'other'
    END,
    ST_Point(longitude, latitude)
FROM read_csv('data/plumes_imeo.csv', header=true, auto_detect=true)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Spatial indexes
CREATE INDEX IF NOT EXISTS idx_raw_wells_geom ON raw.wells USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_raw_vnf_geom ON raw.vnf USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_raw_flare_loc_geom ON raw.flare_locations USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_raw_plumes_geom ON raw.plumes USING RTREE (geom);
