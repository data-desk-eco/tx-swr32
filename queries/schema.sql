INSTALL spatial; LOAD spatial;

CREATE SCHEMA IF NOT EXISTS raw;

-- ============================================================
-- Raw tables (loaded from CSVs, minimal transformation)
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.permits (
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
    property_type   VARCHAR,
    lease_district  VARCHAR,
    lease_number    VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.wells (
    api             VARCHAR,
    oil_gas_code    VARCHAR,
    lease_district  VARCHAR,
    lease_number    VARCHAR,
    well_number     VARCHAR,
    operator_no     VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    geom            GEOMETRY
);

CREATE TABLE IF NOT EXISTS raw.operators (
    operator_number VARCHAR,
    operator_name   VARCHAR,
    status          VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.flare_locations (
    filing_no       VARCHAR,
    name            VARCHAR,
    county          VARCHAR,
    district        VARCHAR,
    release_type    VARCHAR,
    release_height_ft VARCHAR,
    gps_datum       VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    h2s_area        VARCHAR,
    h2s_concentration_ppm VARCHAR,
    h2s_distance_ft VARCHAR,
    h2s_public_area_type VARCHAR,
    geom            GEOMETRY
);

CREATE TABLE IF NOT EXISTS raw.vnf (
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

CREATE TABLE IF NOT EXISTS raw.excluded_facilities (
    facility_id     VARCHAR,
    facility_name   VARCHAR,
    sector          VARCHAR,
    subsectors      VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    geom            GEOMETRY
);

CREATE TABLE IF NOT EXISTS raw.plumes (
    plume_id        VARCHAR,
    source          VARCHAR,
    satellite       VARCHAR,
    date            DATE,
    latitude        DOUBLE,
    longitude       DOUBLE,
    emission_rate   DOUBLE,
    emission_uncertainty DOUBLE,
    sector          VARCHAR,
    geom            GEOMETRY
);
