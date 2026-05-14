import json
import os
import time
from datetime import datetime

import clickhouse_connect
from confluent_kafka import Consumer, KafkaException


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "txn_enrich_fraud")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "db-consumer-clickhouse")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "fraud_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "fraud_password")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "fraud_db")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "fraud_transactions_enriched")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
FLUSH_INTERVAL_SECONDS = float(os.getenv("FLUSH_INTERVAL_SECONDS", "2"))


COLUMNS = [
    "txn_id",

    "step",
    "type",
    "amount",

    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",

    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",

    "isFraud",
    "isFlaggedFraud",

    "simu_event_time",
    "producer_send_time",

    "is_debit",
    "is_transfer",
    "is_payment",
    "is_cashin",
    "is_cashout",

    "is_night",
    "day",
    "hour",

    "orig_is_customer",
    "dest_is_customer",
    "dest_is_merchant",
    "txn_direction",

    "orig_txn_count_1h",
    "orig_txn_count_6h",
    "orig_txn_count_24h",

    "orig_amount_sum_1h",
    "orig_amount_sum_6h",
    "orig_amount_sum_24h",

    "orig_unique_dests_1h",
    "orig_unique_dests_6h",
    "orig_unique_dests_24h",

    "orig_transfer_count_1h",
    "orig_transfer_count_6h",
    "orig_transfer_count_24h",

    "orig_cashout_count_1h",
    "orig_cashout_count_6h",
    "orig_cashout_count_24h",

    "dest_received_count_1h",
    "dest_received_count_6h",
    "dest_received_count_24h",

    "dest_received_amount_sum_1h",
    "dest_received_amount_sum_6h",
    "dest_received_amount_sum_24h",

    "dest_unique_origins_1h",
    "dest_unique_origins_6h",
    "dest_unique_origins_24h",

    "pred_fraud",

    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
]


def parse_datetime(value):
    if value is None:
        return datetime.now()

    text = str(value).strip()

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    return datetime.fromisoformat(text)


def as_int(data, key, default=0):
    value = data.get(key, default)
    if value is None or value == "":
        return default
    return int(value)


def as_float(data, key, default=0.0):
    value = data.get(key, default)
    if value is None or value == "":
        return default
    return float(value)


def as_str(data, key, default=""):
    value = data.get(key, default)
    if value is None:
        return default
    return str(value)


def build_row(data, msg):
    return [
        as_str(data, "txn_id"),

        as_int(data, "step"),
        as_str(data, "type"),
        as_float(data, "amount"),

        as_str(data, "nameOrig"),
        as_float(data, "oldbalanceOrg"),
        as_float(data, "newbalanceOrig"),

        as_str(data, "nameDest"),
        as_float(data, "oldbalanceDest"),
        as_float(data, "newbalanceDest"),

        as_int(data, "isFraud"),
        as_int(data, "isFlaggedFraud"),

        parse_datetime(data.get("simu_event_time")),
        parse_datetime(data.get("producer_send_time")),

        as_int(data, "is_debit"),
        as_int(data, "is_transfer"),
        as_int(data, "is_payment"),
        as_int(data, "is_cashin"),
        as_int(data, "is_cashout"),

        as_int(data, "is_night"),
        as_int(data, "day"),
        as_int(data, "hour"),

        as_int(data, "orig_is_customer"),
        as_int(data, "dest_is_customer"),
        as_int(data, "dest_is_merchant"),
        as_str(data, "txn_direction"),

        as_int(data, "orig_txn_count_1h"),
        as_int(data, "orig_txn_count_6h"),
        as_int(data, "orig_txn_count_24h"),

        as_float(data, "orig_amount_sum_1h"),
        as_float(data, "orig_amount_sum_6h"),
        as_float(data, "orig_amount_sum_24h"),

        as_int(data, "orig_unique_dests_1h"),
        as_int(data, "orig_unique_dests_6h"),
        as_int(data, "orig_unique_dests_24h"),

        as_int(data, "orig_transfer_count_1h"),
        as_int(data, "orig_transfer_count_6h"),
        as_int(data, "orig_transfer_count_24h"),

        as_int(data, "orig_cashout_count_1h"),
        as_int(data, "orig_cashout_count_6h"),
        as_int(data, "orig_cashout_count_24h"),

        as_int(data, "dest_received_count_1h"),
        as_int(data, "dest_received_count_6h"),
        as_int(data, "dest_received_count_24h"),

        as_float(data, "dest_received_amount_sum_1h"),
        as_float(data, "dest_received_amount_sum_6h"),
        as_float(data, "dest_received_amount_sum_24h"),

        as_int(data, "dest_unique_origins_1h"),
        as_int(data, "dest_unique_origins_6h"),
        as_int(data, "dest_unique_origins_24h"),

        as_int(data, "pred_fraud"),

        msg.topic(),
        msg.partition(),
        msg.offset(),
    ]


def create_consumer():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def create_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def flush_batch(client, consumer, batch):
    if not batch:
        return

    client.insert(
        CLICKHOUSE_TABLE,
        batch,
        column_names=COLUMNS,
    )

    consumer.commit(asynchronous=False)
    print(f"Inserted and committed {len(batch)} rows")


def main():
    consumer = create_consumer()
    client = create_clickhouse_client()

    batch = []
    last_flush_time = time.time()

    print("DB consumer started")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"ClickHouse: {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}")

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            if time.time() - last_flush_time >= FLUSH_INTERVAL_SECONDS:
                flush_batch(client, consumer, batch)
                batch.clear()
                last_flush_time = time.time()
            continue

        if msg.error():
            raise KafkaException(msg.error())

        try:
            payload = json.loads(msg.value().decode("utf-8"))
            row = build_row(payload, msg)
            batch.append(row)
        except Exception as exc:
            print(f"Bad message offset={msg.offset()}: {exc}")
            consumer.commit(message=msg, asynchronous=False)
            continue

        if len(batch) >= BATCH_SIZE or time.time() - last_flush_time >= FLUSH_INTERVAL_SECONDS:
            flush_batch(client, consumer, batch)
            batch.clear()
            last_flush_time = time.time()


if __name__ == "__main__":
    main()