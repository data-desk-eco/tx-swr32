-- load.sql: Load all data into the database

-- 1. Load SWR 32 permits
INSERT INTO permits
SELECT
    *,
    -- Parse property field: "Oil Lease-08-43066" → type, district, number
    CASE WHEN property LIKE '%-%-%'
         THEN split_part(property, '-', 1)
         ELSE NULL END AS property_type,
    CASE WHEN property LIKE '%-%-%'
         THEN split_part(property, '-', 2)
         ELSE NULL END AS lease_district,
    CASE WHEN property LIKE '%-%-%'
         THEN split_part(property, '-', 3)
         ELSE NULL END AS lease_number
FROM read_csv('data/filings.csv', delim='\t', header=true, all_varchar=true,
              columns={
                  'excep_seq': 'INTEGER',
                  'submittal_dt': 'VARCHAR',
                  'filing_no': 'INTEGER',
                  'status': 'VARCHAR',
                  'filing_type': 'VARCHAR',
                  'operator_no': 'INTEGER',
                  'operator_name': 'VARCHAR',
                  'property': 'VARCHAR',
                  'effective_dt': 'VARCHAR',
                  'expiration_dt': 'VARCHAR',
                  'fv_district': 'VARCHAR'
              });

-- 2. Load wells
INSERT INTO wells
SELECT
    api, oil_gas_code, lease_district, lease_number, well_number,
    latitude, longitude,
    CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL
              AND latitude != 0 AND longitude != 0
         THEN ST_Point(longitude, latitude)
         ELSE NULL END AS geom
FROM read_csv('data/wells.csv', header=true, auto_detect=true);

-- 3. Load operators
INSERT INTO operators
SELECT * FROM read_csv('data/operators.csv', header=true, auto_detect=true);

-- 4. Load VNF from profiles → aggregate to daily
CREATE TEMP TABLE vnf_passes AS
SELECT
    CAST(regexp_extract(filename, 'site_(\d+)', 1) AS INTEGER) AS flare_id,
    CAST("Date_Mscan" AS DATE) AS date,
    CAST("Lat_GMTCO" AS DOUBLE) AS lat,
    CAST("Lon_GMTCO" AS DOUBLE) AS lon,
    CAST("Cloud_Mask" AS INTEGER) AS cloud_mask,
    CAST("Temp_BB" AS DOUBLE) AS temp_bb,
    CAST("RH" AS DOUBLE) AS rh
FROM read_csv('data/vnf_profiles/site_*.csv',
              filename=true, union_by_name=true,
              ignore_errors=true, auto_detect=true)
WHERE CAST("Sunlit" AS INTEGER) = 0;  -- nighttime only

INSERT INTO vnf
SELECT
    flare_id,
    AVG(lat) AS lat,
    AVG(lon) AS lon,
    date,
    BOOL_OR(cloud_mask = 0) AS clear,
    BOOL_OR(cloud_mask = 0 AND temp_bb != 999999) AS detected,
    AVG(CASE WHEN cloud_mask = 0 AND temp_bb != 999999 THEN rh END) AS rh_mw,
    AVG(CASE WHEN cloud_mask = 0 AND temp_bb != 999999 THEN temp_bb END) AS temp_k,
    COUNT(*) AS n_passes
FROM vnf_passes
GROUP BY flare_id, date;

DROP TABLE vnf_passes;

-- Update VNF geometry
UPDATE vnf SET geom = ST_Point(lon, lat) WHERE lat IS NOT NULL AND lon IS NOT NULL;

-- Spatial indexes
CREATE INDEX IF NOT EXISTS idx_wells_geom ON wells USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_vnf_geom ON vnf USING RTREE (geom);
