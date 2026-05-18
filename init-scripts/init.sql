CREATE DATABASE airflow_db;
CREATE DATABASE metabase_db;

\c olist_db; 
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE SCHEMA IF NOT EXISTS streaming;


CREATE TABLE IF NOT EXISTS streaming.olist_events (
    event_id UUID,
    event_type VARCHAR,
    order_id VARCHAR,
    customer_id VARCHAR,
    total_value NUMERIC,
    event_timestamp TIMESTAMP,
    ingested_at TIMESTAMP,
    kafka_offset BIGINT,
    kafka_partition INT
);