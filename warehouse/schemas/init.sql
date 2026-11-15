-- warehouse/schemas/init.sql
-- NetScope Analytical Warehouse DDL for ClickHouse
-- Demonstrates MergeTree engines, LowCardinality encoding, and Materialized Views.

CREATE DATABASE IF NOT EXISTS netscope;

-- 1. Raw measurements table
CREATE TABLE IF NOT EXISTS netscope.measurements (
    timestamp        DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    measurement_id   UUID,
    probe_id         LowCardinality(String),
    target_id        LowCardinality(String),
    target           String,
    dns_ms           Nullable(Float32) CODEC(ZSTD(1)),
    tcp_ms           Nullable(Float32) CODEC(ZSTD(1)),
    tls_ms           Nullable(Float32) CODEC(ZSTD(1)),
    ttfb_ms          Nullable(Float32) CODEC(ZSTD(1)),
    total_ms         Nullable(Float32) CODEC(ZSTD(1)),
    http_status      Nullable(UInt16),
    response_bytes   Nullable(UInt32) CODEC(ZSTD(1)),
    error_type       Nullable(String),
    timed_out        UInt8
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (probe_id, target_id, timestamp);

-- 2. Aggregated streaming windows table (written by Flink)
CREATE TABLE IF NOT EXISTS netscope.latency_windows (
    window_type      LowCardinality(String),
    window_start     DateTime64(3, 'UTC'),
    window_end       DateTime64(3, 'UTC'),
    probe_id         LowCardinality(String),
    target_id        LowCardinality(String),
    target           String,
    count            UInt32,
    avg_latency_ms   Float32,
    p50_ms           Float32,
    p95_ms           Float32,
    p99_ms           Float32,
    failure_count    UInt32,
    availability_pct Float32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (probe_id, target_id, window_start);

-- 3. Incidents table (written by Anomaly & Incident Engine)
CREATE TABLE IF NOT EXISTS netscope.incidents (
    incident_id      UUID,
    target_id        LowCardinality(String),
    region           LowCardinality(String),
    start_time       DateTime64(3, 'UTC'),
    end_time         Nullable(DateTime64(3, 'UTC')),
    severity         Enum8('LOW' = 1, 'MEDIUM' = 2, 'HIGH' = 3, 'CRITICAL' = 4),
    anomaly_score    Float32,
    affected_probes  Array(String),
    classification   String,
    evidence         String,
    status           Enum8('ACTIVE' = 1, 'RECOVERING' = 2, 'RESOLVED' = 3)
) ENGINE = MergeTree()
ORDER BY (start_time, target_id);

-- 4. Materialized View for Real-Time Probe Performance Aggregation
CREATE TABLE IF NOT EXISTS netscope.target_daily_stats (
    date             Date,
    target_id        LowCardinality(String),
    probe_id         LowCardinality(String),
    sample_count     UInt32,
    total_latency    Float64,
    errors           UInt32
) ENGINE = SummingMergeTree()
PRIMARY KEY (date, target_id, probe_id)
ORDER BY (date, target_id, probe_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS netscope.mv_target_daily_stats
TO netscope.target_daily_stats AS
SELECT
    toDate(timestamp) AS date,
    target_id,
    probe_id,
    count() AS sample_count,
    sum(coalesce(total_ms, 0)) AS total_latency,
    countIf(error_type IS NOT NULL OR timed_out = 1 OR http_status >= 400) AS errors
FROM netscope.measurements
GROUP BY date, target_id, probe_id;
