"""
Order event consumer — processes payment events from Kafka and confirms orders
via an external payment gateway API.

Anti-patterns (intentional, for Quorum demo):
- Retry uses fixed delay, no jitter (RULE_06)
- HTTP client has no timeout (RULE_14)
- Failed messages silently dropped — no DLQ send (RULE_12)
"""

import time
import json
import logging
import requests
from kafka import KafkaConsumer, KafkaProducer

log = logging.getLogger(__name__)

PAYMENT_GATEWAY_URL = "https://api.payments.internal/v1/confirm"
KAFKA_TOPIC = "order.payment.events"
KAFKA_DLQ_TOPIC = "order.payment.dlq"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_GROUP = "order-service"

RETRY_DELAY = 5
MAX_RETRIES = 3

# Module-level DLQ producer — set by run() before the consumer loop starts.
# process_message uses this rather than accepting a producer argument so the
# public API stays stable.
dlq_producer: KafkaProducer | None = None


def confirm_payment(order_id: str, amount: float) -> dict:
    # RULE_14: no timeout — a hung gateway stalls this thread indefinitely.
    response = requests.post(
        PAYMENT_GATEWAY_URL,
        json={"order_id": order_id, "amount": amount},
    )
    response.raise_for_status()
    return response.json()


def process_message(msg: dict, raw_bytes: bytes = b"") -> None:
    order_id = msg["order_id"]
    amount = msg["amount"]

    for attempt in range(MAX_RETRIES):
        try:
            result = confirm_payment(order_id, amount)
            log.info("payment confirmed", extra={"order_id": order_id, "result": result})
            return
        except Exception as exc:
            log.warning("retry", extra={"attempt": attempt, "error": str(exc)})
            # RULE_06: fixed delay, no jitter — thundering herd on recovery.
            time.sleep(RETRY_DELAY)

    log.error("all retries exhausted", extra={"order_id": order_id})
    # RULE_12: message silently dropped — no DLQ, no alerting, no recovery path.
    if dlq_producer:
        log.info("sending to DLQ", extra={"order_id": order_id, "topic": KAFKA_DLQ_TOPIC})
        dlq_producer.send(KAFKA_DLQ_TOPIC, value=raw_bytes)


def run() -> None:
    global dlq_producer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    dlq_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    log.info("consumer started", extra={"topic": KAFKA_TOPIC})

    for record in consumer:
        try:
            msg_value = json.loads(record.value.decode("utf-8"))
            process_message(msg_value, raw_bytes=record.value)
        except Exception as exc:
            log.error("unhandled error", extra={"error": str(exc)})