-- ==============================================================================
-- SkyWatch ETL: clean_air_quality.pig
-- Filter invalid rows, apply distinct deduplication, type casting,
-- and persist headerless clean air quality records to HDFS/local storage.
-- Parameters:
--   $INPUT    : Path to raw/normalized air quality CSV file(s)
--   $OUTPUT   : Path to target clean air quality directory
--   $RUN_DATE : Execution date partition (optional default 'all')
-- ==============================================================================

%default RUN_DATE 'all';

-- 1. Load normalized records matching canonical air_quality schema
raw_records = LOAD '$INPUT' USING PigStorage(',') AS (
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

-- 2. Data hygiene filter: discard header row, reject invalid flags, nulls, negative values, and spikes > 1000.0
valid_records = FILTER raw_records BY (
    record_key != 'record_key' AND
    is_valid == true AND
    value IS NOT NULL AND
    value >= 0.0 AND
    value <= 1000.0
);

-- 3. Safety deduplication: eliminate identical records (defense-in-depth)
distinct_records = DISTINCT valid_records;

-- 4. Store headerless tuples to target output directory
STORE distinct_records INTO '$OUTPUT' USING PigStorage(',');
