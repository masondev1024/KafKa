-- Run these statements after selecting the Glue database printed by:
--   terraform output -raw glue_database_name
-- The ingestion-date partition predicates in the validation query are intentional: they keep
-- Athena scan cost bounded for a portfolio-sized, date-partitioned lake.

SHOW CREATE TABLE sensor_events;

-- Replace the three values with a date that has already landed in S3.
SELECT
    ingest_year,
    ingest_month,
    ingest_day,
    COUNT(*) AS landed_rows,
    COUNT(DISTINCT event_id) AS unique_event_ids,
    COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_rows,
    SUM(CASE WHEN temperature BETWEEN -40 AND 150 THEN 0 ELSE 1 END) AS invalid_temperature_rows,
    SUM(CASE WHEN humidity BETWEEN 0 AND 100 THEN 0 ELSE 1 END) AS invalid_humidity_rows,
    SUM(CASE WHEN status IN ('RUNNING', 'IDLE', 'STOPPED', 'ERROR') THEN 0 ELSE 1 END) AS invalid_status_rows
FROM sensor_events
WHERE ingest_year = 2026
  AND ingest_month = 9
  AND ingest_day = 3
GROUP BY ingest_year, ingest_month, ingest_day;

-- A replay-safe analytical view. Firehose/Vector delivery is at-least-once,
-- so event_id is the business key used to collapse duplicates at query time.
CREATE OR REPLACE VIEW sensor_events_deduplicated AS
WITH ranked AS (
    SELECT
        schema_version,
        event_id,
        event_time,
        ingested_at,
        sensor_id,
        temperature,
        humidity,
        status,
        source,
        vector_ingest_at,
        pipeline,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at DESC, source."offset" DESC
        ) AS row_num
    FROM sensor_events
)
SELECT
    schema_version,
    event_id,
    event_time,
    ingested_at,
    sensor_id,
    temperature,
    humidity,
    status,
    source,
    vector_ingest_at,
    pipeline
FROM ranked
WHERE row_num = 1;
