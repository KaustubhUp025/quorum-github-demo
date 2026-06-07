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
from kafka import KafkaConsumer

log = logging.getLogger(__name__)

PAYMENT_GATEWAY_URL = "https://api.payments.internal/v1/confirm"
KAFKA_TOPIC = "order.payment.events"
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


def process_message(msg: dict) -> None:
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

    log.error("payment failed after all retries", extra={"order_id": order_id})
    # RULE_12: no DLQ — failed messages are silently dropped.
    # A poison-pill order will be lost with no recovery path.


def run() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    log.info("consumer started", extra={"topic": KAFKA_TOPIC})

    for record in consumer:
        try:
            process_message(record.value)
        except Exception as exc:
            # RULE_12: exception swallowed — no DLQ, no alerting.
            log.error("unhandled error processing record", extra={"error": str(exc)})
