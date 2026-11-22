-- ==============================================================================
-- SkyWatch ETL: station_stats.pig
-- Compute daily station aggregations: reading count, average, minimum,
-- and maximum concentration per pollutant, station, and date.
-- Parameters:
--   $INPUT    : Path to clean air quality dataset directory
--   $OUTPUT   : Path to target daily station stats directory
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

-- 2. Project date substring from UTC timestamp (first 10 chars: YYYY-MM-DD)
extracted = FOREACH clean_records GENERATE
    station_id,
    parameter,
    SUBSTRING(measurement_time_utc, 0, 10) AS date_dt:chararray,
    value;

-- 3. Group by composite key: station, pollutant parameter, and date
grouped_records = GROUP extracted BY (station_id, parameter, date_dt);

-- 4. Calculate daily aggregations per group
daily_stats = FOREACH grouped_records GENERATE
    group.station_id AS station_id:chararray,
    group.parameter AS parameter:chararray,
    group.date_dt AS date:chararray,
    COUNT(extracted.value) AS reading_count:long,
    ROUND(AVG(extracted.value) * 10000.0) / 10000.0 AS average_value:double,
    MIN(extracted.value) AS minimum_value:double,
    MAX(extracted.value) AS maximum_value:double;

-- 5. Store headerless aggregated records
STORE daily_stats INTO '$OUTPUT' USING PigStorage(',');
