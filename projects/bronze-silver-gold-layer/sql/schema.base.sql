-- ============================================================================
-- schema.base.sql  ·  GOLD layer DWH schema
-- PostgreSQL 15 (local) and Redshift (production) compatible
-- Auto-applied on first postgres-dwh container startup
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS gold.earthquake_events (
    event_id               VARCHAR(64)      NOT NULL,
    lat                    DOUBLE PRECISION,
    lon                    DOUBLE PRECISION,
    magnitude              DOUBLE PRECISION,
    depth                  DOUBLE PRECISION,
    event_time             TIMESTAMPTZ,
    region                 VARCHAR(255),
    enriched_at            TIMESTAMPTZ,
    processing_date        DATE,
    scored_at              TIMESTAMPTZ,
    tsunami_risk           DOUBLE PRECISION CHECK (tsunami_risk BETWEEN 0 AND 10),
    building_vulnerability DOUBLE PRECISION CHECK (building_vulnerability BETWEEN 0 AND 10),
    population_exposure    DOUBLE PRECISION CHECK (population_exposure BETWEEN 0 AND 10),
    infrastructure_risk    DOUBLE PRECISION CHECK (infrastructure_risk BETWEEN 0 AND 10),
    composite_score        DOUBLE PRECISION CHECK (composite_score BETWEEN 0 AND 10),
    mmi_estimate           DOUBLE PRECISION,
    population_50km        DOUBLE PRECISION,
    usgs_nearby_count      DOUBLE PRECISION,
    usgs_max_magnitude     DOUBLE PRECISION,
    ge_validation_passed   BOOLEAN,
    PRIMARY KEY (event_id, processing_date)
);

CREATE TABLE IF NOT EXISTS gold.regional_stats (
    region                   VARCHAR(255)    NOT NULL,
    processing_date          DATE            NOT NULL,
    aggregated_at            TIMESTAMPTZ,
    event_count              BIGINT,
    unique_events            BIGINT,
    avg_magnitude            DOUBLE PRECISION,
    max_magnitude            DOUBLE PRECISION,
    avg_composite_score      DOUBLE PRECISION,
    max_composite_score      DOUBLE PRECISION,
    avg_tsunami_risk         DOUBLE PRECISION,
    max_tsunami_risk         DOUBLE PRECISION,
    avg_population_exposure  DOUBLE PRECISION,
    total_population_exposed DOUBLE PRECISION,
    PRIMARY KEY (region, processing_date)
);

CREATE TABLE IF NOT EXISTS gold.daily_summary (
    processing_date                 DATE         NOT NULL PRIMARY KEY,
    aggregated_at                   TIMESTAMPTZ,
    region                          VARCHAR(32)  DEFAULT 'GLOBAL',
    total_events                    BIGINT,
    max_magnitude_global            DOUBLE PRECISION,
    max_composite_score_global      DOUBLE PRECISION,
    regions_affected                BIGINT,
    total_population_exposed_global DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS gold.quality_profiles (
    layer           VARCHAR(16)  NOT NULL,
    date            DATE         NOT NULL,
    profiled_at     TIMESTAMPTZ,
    row_count       BIGINT,
    distinct_ids    BIGINT,
    duplicate_count BIGINT,
    PRIMARY KEY (layer, date)
);

CREATE TABLE IF NOT EXISTS monitoring.events (
    event_id         VARCHAR(64)  NOT NULL,
    source           VARCHAR(32)  NOT NULL,
    polled_at        TIMESTAMPTZ  NOT NULL,
    alert_level      VARCHAR(16),
    aftershock_count INT,
    data_json        TEXT,
    PRIMARY KEY (event_id, source, polled_at)
);

CREATE INDEX IF NOT EXISTS idx_events_region    ON gold.earthquake_events (region);
CREATE INDEX IF NOT EXISTS idx_events_magnitude ON gold.earthquake_events (magnitude);
CREATE INDEX IF NOT EXISTS idx_events_time      ON gold.earthquake_events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_composite ON gold.earthquake_events (composite_score);
CREATE INDEX IF NOT EXISTS idx_regional_date    ON gold.regional_stats (processing_date);
CREATE INDEX IF NOT EXISTS idx_monitoring_event ON monitoring.events (event_id);
