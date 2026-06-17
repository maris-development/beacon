-- Schema for the benchmark observations table (subject-under-test: PostgreSQL).
-- The harness COPY-loads the generated Parquet into this table and times it
-- (that load is Postgres's "ingestion cost"). Columns mirror data-gen/generate.py.

CREATE TABLE IF NOT EXISTS obs (
    time         BIGINT,          -- epoch seconds (see data-gen/generate.py)
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    depth        REAL,
    platform     TEXT,
    platform_id  INTEGER,
    temperature  REAL,
    salinity     REAL,
    oxygen       REAL,
    pressure     REAL,
    chlorophyll  REAL,
    nitrate      REAL,
    ph           REAL
);

-- No indexes by default: the benchmark measures full-scan analytical queries,
-- matching how the columnar engines read the same data. Add indexes here only
-- if you want to measure an index-tuned Postgres variant separately.
