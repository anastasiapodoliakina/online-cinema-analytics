CREATE TABLE IF NOT EXISTS raw_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Int32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), user_id, event_id);

CREATE TABLE IF NOT EXISTS kafka_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'movie-events',
         kafka_group_name = 'clickhouse-movie-events',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081',
         kafka_num_consumers = 3,
         kafka_thread_per_consumer = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS ingest_events TO raw_events AS
SELECT event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
FROM kafka_events;

CREATE TABLE IF NOT EXISTS daily_metrics
(
    metric_date Date,
    metric_name String,
    dimension String,
    value Nullable(Float64),
    computed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, metric_name, dimension);
