import json
import os
import signal
import time
from collections import Counter
from typing import Any

import requests
from confluent_kafka import Consumer, KafkaException


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "txn_enrich_fraud")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "telegram-alert-consumer")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

ALERT_FLUSH_INTERVAL_SECONDS = float(os.getenv("ALERT_FLUSH_INTERVAL_SECONDS", "30"))
MAX_TXNS_IN_MESSAGE = int(os.getenv("MAX_TXNS_IN_MESSAGE", "5"))

running = True


def handle_stop(signum, frame):
    global running
    running = False


def as_int(data: dict, key: str, default: int = 0) -> int:
    value = data.get(key, default)
    if value is None or value == "":
        return default
    return int(value)


def as_float(data: dict, key: str, default: float = 0.0) -> float:
    value = data.get(key, default)
    if value is None or value == "":
        return default
    return float(value)


def as_str(data: dict, key: str, default: str = "") -> str:
    value = data.get(key, default)
    if value is None:
        return default
    return str(value)


def create_kafka_consumer() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def send_telegram_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("Missing TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    response = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=10,
    )
    response.raise_for_status()


def build_summary_message(alerts: list[dict]) -> str:
    total_fraud = len(alerts)
    total_amount = sum(as_float(x, "amount") for x in alerts)

    by_type = Counter(as_str(x, "type") for x in alerts)

    top_txns = sorted(
        alerts,
        key=lambda x: as_float(x, "amount"),
        reverse=True,
    )[:MAX_TXNS_IN_MESSAGE]

    lines = []
    lines.append("🚨 <b>FRAUD ALERT SUMMARY</b>")
    lines.append("")
    lines.append(f"Fraud transactions: <b>{total_fraud}</b>")
    lines.append(f"Total fraud amount: <b>{total_amount:,.2f}</b>")
    lines.append("")
    lines.append("<b>By transaction type:</b>")

    for txn_type, count in by_type.most_common():
        lines.append(f"- {txn_type}: {count}")

    lines.append("")
    lines.append(f"<b>Top {len(top_txns)} suspicious transactions:</b>")

    for i, txn in enumerate(top_txns, start=1):
        txn_id = as_str(txn, "txn_id")
        step = as_int(txn, "step")
        txn_type = as_str(txn, "type")
        amount = as_float(txn, "amount")
        name_orig = as_str(txn, "nameOrig")
        name_dest = as_str(txn, "nameDest")

        lines.append(
            f"{i}. txn_id={txn_id}, step={step}, "
            f"type={txn_type}, amount={amount:,.2f}, "
            f"{name_orig} → {name_dest}"
        )

    return "\n".join(lines)


def flush_alerts(alerts: list[dict], consumer: Consumer) -> None:
    if not alerts:
        return

    text = build_summary_message(alerts)
    send_telegram_message(text)

    consumer.commit(asynchronous=False)

    print(f"Sent Telegram summary for {len(alerts)} fraud transactions")


def main():
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    consumer = create_kafka_consumer()

    alert_buffer: list[dict] = []
    seen_txn_ids: set[str] = set()
    last_flush_time = time.time()

    print("Telegram alert consumer started")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Flush interval: {ALERT_FLUSH_INTERVAL_SECONDS}s")

    try:
        while running:
            msg = consumer.poll(1.0)

            now = time.time()

            if msg is None:
                if now - last_flush_time >= ALERT_FLUSH_INTERVAL_SECONDS:
                    flush_alerts(alert_buffer, consumer)
                    alert_buffer.clear()
                    last_flush_time = now
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                payload = json.loads(msg.value().decode("utf-8"))

                txn_id = as_str(payload, "txn_id")
                pred_fraud = as_int(payload, "pred_fraud")

                if pred_fraud == 1 and txn_id not in seen_txn_ids:
                    alert_buffer.append(payload)
                    seen_txn_ids.add(txn_id)

                # Nếu non-fraud thì vẫn commit để không đọc lại.
                if pred_fraud != 1:
                    consumer.commit(message=msg, asynchronous=False)

            except Exception as exc:
                print(
                    f"Bad message topic={msg.topic()} "
                    f"partition={msg.partition()} offset={msg.offset()}: {exc}"
                )
                consumer.commit(message=msg, asynchronous=False)
                continue

            if now - last_flush_time >= ALERT_FLUSH_INTERVAL_SECONDS:
                flush_alerts(alert_buffer, consumer)
                alert_buffer.clear()
                last_flush_time = now

    finally:
        print("Stopping Telegram alert consumer...")
        flush_alerts(alert_buffer, consumer)
        consumer.close()


if __name__ == "__main__":
    main()