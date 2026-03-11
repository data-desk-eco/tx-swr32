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

CREATE TABLE IF NOT EXISTS raw.permit_details (
    filing_no       VARCHAR,
    exception_number VARCHAR,
    sequence_number VARCHAR,
    exception_status VARCHAR,
    operator        VARCHAR,
    submitted_date  VARCHAR,
    filing_type     VARCHAR,
    prior_exception_no VARCHAR,
    cumulative_days_authorized VARCHAR,
    site_name       VARCHAR,
    hearing_requested VARCHAR,
    is_h8_shutdown  VARCHAR,
    permanent_exception_requested VARCHAR,
    requested_effective_date VARCHAR,
    requested_expiration_date VARCHAR,
    number_of_days  VARCHAR,
    every_day_of_month VARCHAR,
    days_per_month  VARCHAR,
    connected_to_gathering_system VARCHAR,
    distance_to_nearest_pipeline VARCHAR,
    exception_reasons VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.permit_properties (
    filing_no       VARCHAR,
    property_type   VARCHAR,
    district        VARCHAR,
    property_id     VARCHAR,
    lease_name      VARCHAR,
    requested_release_rate_mcf_day VARCHAR,
    gas_measurement_method VARCHAR
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
    other_public_area VARCHAR,
    facility_type   VARCHAR,
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

-- PDQ: lease-level gas disposition (flared/vented volumes from production reports)
CREATE TABLE IF NOT EXISTS raw.gas_disposition (
    oil_gas_code    VARCHAR,
    district_no     VARCHAR,
    lease_no        VARCHAR,
    cycle_year      VARCHAR,
    cycle_month     VARCHAR,
    operator_no     VARCHAR,
    field_no        VARCHAR,
    lease_gas_dispcd04_vol  DOUBLE,   -- gas vented or flared (MCF)
    lease_csgd_dispcde04_vol DOUBLE,  -- casinghead gas vented or flared (MCF)
    lease_gas_total_vol     DOUBLE,   -- total gas disposed, all codes (MCF)
    lease_csgd_total_vol    DOUBLE,   -- total casinghead gas disposed, all codes (MCF)
    district_name   VARCHAR,
    lease_name      VARCHAR,
    operator_name   VARCHAR,
    field_name      VARCHAR
);

-- PDQ: lease master data (lease identifying info)
CREATE TABLE IF NOT EXISTS raw.pdq_leases (
    oil_gas_code    VARCHAR,
    district_no     VARCHAR,
    lease_no        VARCHAR,
    operator_no     VARCHAR,
    field_no        VARCHAR,
    district_name   VARCHAR,
    lease_name      VARCHAR,
    operator_name   VARCHAR,
    field_name      VARCHAR,
    cycle_year_month_min VARCHAR,
    cycle_year_month_max VARCHAR
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
