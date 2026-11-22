-- ==============================================================================
-- SkyWatch ETL: hourly_rollup.pig
-- Produce regularized hourly rollups feeding downstream Spark trend analysis
-- and timeseries correlation jobs.
-- Parameters:
--   $INPUT    : Path to clean air quality dataset directory
--   $OUTPUT   : Path to target hourly rollup directory
--   $RUN_DATE : Execution date partition (optional default 'all')
-- ==============================================================================

%default RUN_DATE 'all';

-- 1. Load clean records matching canonical clean_air_quality schema
clean_records = LOAD '$INPUT' USING PigStorage(',') AS (
    record_key:chararray,
    station_id:chararray,
    station_name:chararray,
    latitude:double,
    longitude:double,
    sensor_id:chararray,
    parameter:chararray,
    value:double,
    unit:chararray,
    measurement_time_utc:chararray,
    ingestion_time_utc:chararray,
    source:chararray,
    is_valid:boolean,
    reject_reason:chararray
);

-- 2. Regularize timestamps into hourly bucket start UTC (YYYY-MM-DDTHH:00:00Z)
extracted_hours = FOREACH clean_records GENERATE
    station_id,
    parameter,
    CONCAT(SUBSTRING(measurement_time_utc, 0, 13), ':00:00Z') AS bucket_start_utc:chararray,
    value;

-- 3. Group by composite key: station, parameter, and hourly bucket
grouped_hours = GROUP extracted_hours BY (station_id, parameter, bucket_start_utc);

-- 4. Calculate hourly aggregations per bucket
hourly_stats = FOREACH grouped_hours GENERATE
    group.station_id AS station_id:chararray,
    group.parameter AS parameter:chararray,
    group.bucket_start_utc AS bucket_start_utc:chararray,
    COUNT(extracted_hours.value) AS reading_count:long,
    ROUND_TO(AVG(extracted_hours.value), 4) AS average_value:double,
    MIN(extracted_hours.value) AS minimum_value:double,
    MAX(extracted_hours.value) AS maximum_value:double;

-- 5. Store headerless hourly rollups
STORE hourly_stats INTO '$OUTPUT' USING PigStorage(',');
