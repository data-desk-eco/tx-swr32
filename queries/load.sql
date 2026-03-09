LOAD spatial;

-- SWR 32 permits (parse "Oil Lease-08-43066" -> type, district, number)
INSERT INTO permits
SELECT *,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 1) END,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 2) END,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 3) END
FROM read_csv('data/filings.csv', delim='\t', header=true, all_varchar=true);

-- Wells with geometry
INSERT INTO wells
SELECT *, CASE WHEN latitude != 0 AND longitude != 0
               THEN ST_Point(longitude, latitude) END
FROM read_csv('data/wells.csv', header=true, auto_detect=true);

-- Operators
INSERT INTO operators
SELECT * FROM read_csv('data/operators.csv', header=true, auto_detect=true);

-- VNF: read profiles, filter to nighttime, aggregate to daily per flare
INSERT INTO vnf
SELECT flare_id, AVG(lat), AVG(lon), date,
    BOOL_OR(cloud = 0) AS clear,
    BOOL_OR(cloud = 0 AND temp != 999999) AS detected,
    AVG(CASE WHEN cloud = 0 AND temp != 999999 THEN rh END),
    AVG(CASE WHEN cloud = 0 AND temp != 999999 THEN temp END),
    COUNT(*), NULL
FROM (
    SELECT CAST(regexp_extract(filename, 'site_(\d+)', 1) AS INTEGER) AS flare_id,
           CAST("Date_Mscan" AS DATE) AS date,
           CAST("Lat_GMTCO" AS DOUBLE) AS lat,
           CAST("Lon_GMTCO" AS DOUBLE) AS lon,
           CAST("Cloud_Mask" AS INTEGER) AS cloud,
           CAST("Temp_BB" AS DOUBLE) AS temp,
           CAST("RH" AS DOUBLE) AS rh
    FROM read_csv('data/vnf_profiles/site_*.csv',
                  filename=true, union_by_name=true, ignore_errors=true, auto_detect=true)
    WHERE CAST("Sunlit" AS INTEGER) = 0
)
GROUP BY flare_id, date;

-- Geometry columns and indexes
UPDATE vnf SET geom = ST_Point(lon, lat) WHERE lat IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_wells_geom ON wells USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_vnf_geom ON vnf USING RTREE (geom);
