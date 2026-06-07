"""
Order event consumer — processes payment events from Kafka and confirms orders
via an external payment gateway API.

Intentional anti-patterns present (for demo purposes):
- No dead-letter queue on failed messages
- HTTP client has no timeout
- Retry uses fixed delay (no jitter)
"""

import time
import logging
import requests
import json
from kafka import KafkaConsumer, KafkaProducer

log = logging.getLogger(__name__)

PAYMENT_GATEWAY_URL = "https://api.payments.internal/v1/confirm"
KAFKA_TOPIC = "order.payment.events"
KAFKA_DLQ_TOPIC = "order.payment.dlq"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_GROUP = "order-service"

RETRY_DELAY = 5          # seconds — fixed, no jitter
MAX_RETRIES = 3


def confirm_payment(order_id: str, amount: float) -> dict:
    # RULE_14: requests.post with no timeout — if the gateway hangs,
    # this thread blocks indefinitely, starving the consumer group.
    response = requests.post(
        PAYMENT_GATEWAY_URL,
        json={"order_id": order_id, "amount": amount},
    )
    response.raise_for_status()
    return response.json()


def process_message(producer: KafkaProducer, original_value: bytes, msg: dict) -> None:
    order_id = msg["order_id"]
    amount = msg["amount"]

    for attempt in range(MAX_RETRIES):
        try:
            result = confirm_payment(order_id, amount)
            log.info("payment confirmed", extra={"order_id": order_id, "result": result})
            return
        except Exception as exc:
            log.warning("payment attempt failed", extra={"attempt": attempt, "error": str(exc)})
            # RULE_06: fixed delay — all retrying consumers wake at the same time,
            # hammering the recovering gateway simultaneously (thundering herd).
            time.sleep(RETRY_DELAY)

    log.error("payment failed after all retries, sending to DLQ", extra={"order_id": order_id})
    # FIX: Send the failed message to a dead-letter queue (DLQ) for later inspection.
    producer.send(KAFKA_DLQ_TOPIC, value=original_value)


def run() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    log.info("consumer started", extra={"topic": KAFKA_TOPIC})

    for record in consumer:
        try:
            # Assuming JSON-encoded messages
            msg_value = json.loads(record.value.decode('utf-8'))
            process_message(producer, record.value, msg_value)
        except Exception as exc:
            # Unhandled errors (e.g., JSON parsing) are still logged,
            # but processing failures are now handled with a DLQ.
            log.error("unhandled error processing record", extra={"error": str(exc)})