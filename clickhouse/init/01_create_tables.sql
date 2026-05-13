CREATE DATABASE IF NOT EXISTS fraud_db;

CREATE TABLE IF NOT EXISTS fraud_db.fraud_transactions
(
    txn_id String,

    step UInt32,
    event_time DateTime64(3, 'Asia/Bangkok'),

    producer_send_time Nullable(DateTime64(3, 'Asia/Bangkok')),
    processed_at Nullable(DateTime64(3, 'Asia/Bangkok')),
    inserted_at DateTime64(3, 'Asia/Bangkok') DEFAULT now64(3),

    producer_id LowCardinality(String),
    kafka_topic LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset UInt64,

    type LowCardinality(String),
    amount Float64,

    nameOrig String,
    oldbalanceOrg Float64,
    newbalanceOrig Float64,

    nameDest String,
    oldbalanceDest Float64,
    newbalanceDest Float64,

    isFraud UInt8,
    isFlaggedFraud UInt8,

    pred_is_fraud UInt8,
    fraud_score Float32,
    model_version LowCardinality(String),

    orig_txn_count_1h UInt32,
    orig_txn_count_6h UInt32,
    orig_txn_count_24h UInt32,

    orig_amount_sum_1h Float64,
    orig_amount_sum_24h Float64,
    orig_unique_dest_24h UInt32,

    dest_received_count_1h UInt32,
    dest_received_count_24h UInt32,

    hour UInt8,
    day UInt16,
    is_night UInt8,
    is_weekend UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (step, event_time, pred_is_fraud, type, txn_id);


CREATE TABLE IF NOT EXISTS fraud_db.fraud_metrics_by_step
(
    step UInt32,
    event_time DateTime64(3, 'Asia/Bangkok'),
    type LowCardinality(String),

    total_txn UInt64,
    fraud_txn UInt64,
    fraud_amount Float64,
    total_amount Float64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (step, event_time, type);


CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_db.mv_fraud_metrics_by_step
TO fraud_db.fraud_metrics_by_step
AS
SELECT
    step,
    event_time,
    type,
    count() AS total_txn,
    countIf(pred_is_fraud = 1) AS fraud_txn,
    sumIf(amount, pred_is_fraud = 1) AS fraud_amount,
    sum(amount) AS total_amount
FROM fraud_db.fraud_transactions
GROUP BY
    step,
    event_time,
    type;


