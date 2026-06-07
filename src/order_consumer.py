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


def process_message(msg: dict, producer: KafkaProducer) -> None:
    order_id = msg["order_id"]
    amount = msg["amount"]
    last_exc = None

    for attempt in range(MAX_RETRIES):
        try:
            result = confirm_payment(order_id, amount)
            log.info("payment confirmed", extra={"order_id": order_id, "result": result})
            return
        except Exception as exc:
            last_exc = exc
            log.warning("payment attempt failed", extra={"attempt": attempt, "error": str(exc)})
            # RULE_06: fixed delay — all retrying consumers wake at the same time,
            # hammering the recovering gateway simultaneously (thundering herd).
            time.sleep(RETRY_DELAY)

    log.error("payment failed after all retries", extra={"order_id": order_id})
    # FIX: Send to DLQ
    dlq_payload = {
        "original_message": msg,
        "failure_reason": str(last_exc),
        "failed_at": time.time(),
    }
    producer.send(KAFKA_DLQ_TOPIC, value=json.dumps(dlq_payload).encode("utf-8"))
    log.info("message sent to DLQ", extra={"order_id": order_id, "topic": KAFKA_DLQ_TOPIC})


def run() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    log.info("consumer started", extra={"topic": KAFKA_TOPIC})

    for record in consumer:
        try:
            process_message(record.value, producer)
        except Exception as exc:
            # This outer block is a last resort. The DLQ is handled in process_message.
            log.error("unhandled error processing record", extra={"error": str(exc)})