-- schema.sql: Dark flaring analysis database
INSTALL spatial; LOAD spatial;

-- SWR 32 exception permits
CREATE TABLE IF NOT EXISTS permits (
    excep_seq       VARCHAR,
    submittal_dt    VARCHAR,
    filing_no       VARCHAR,
    status          VARCHAR,
    filing_type     VARCHAR,
    operator_no     VARCHAR,
    operator_name   VARCHAR,
    property        VARCHAR,
    effective_dt    VARCHAR,
    expiration_dt   VARCHAR,
    fv_district     VARCHAR,
    -- parsed from property field
    property_type   VARCHAR,
    lease_district  VARCHAR,
    lease_number    VARCHAR
);

-- RRC wells (Permian only, with locations)
CREATE TABLE IF NOT EXISTS wells (
    api             VARCHAR,
    oil_gas_code    VARCHAR,
    lease_district  VARCHAR,
    lease_number    VARCHAR,
    well_number     VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    geom            GEOMETRY
);

-- RRC operators
CREATE TABLE IF NOT EXISTS operators (
    operator_number VARCHAR,
    operator_name   VARCHAR,
    status          VARCHAR
);

-- VNF daily flare detections (Permian)
CREATE TABLE IF NOT EXISTS vnf (
    flare_id    INTEGER,
    lat         DOUBLE,
    lon         DOUBLE,
    date        DATE,
    clear       BOOLEAN,
    detected    BOOLEAN,
    rh_mw       DOUBLE,
    temp_k      DOUBLE,
    n_passes    INTEGER,
    geom        GEOMETRY
);
