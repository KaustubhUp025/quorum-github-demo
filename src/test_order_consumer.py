"""Updated tests matching the DLQ-aware process_message signature."""
import json as _json
from unittest.mock import patch, MagicMock
from src.order_consumer import process_message


def _mock_producer():
    return MagicMock()


def test_process_message_success():
    producer = _mock_producer()
    original = _json.dumps({"order_id": "ord-001", "amount": 99.99}).encode()
    with patch("src.order_consumer.confirm_payment") as mock_confirm:
        mock_confirm.return_value = {"status": "confirmed"}
        process_message(producer, original, {"order_id": "ord-001", "amount": 99.99})
    mock_confirm.assert_called_once_with("ord-001", 99.99)
    producer.send.assert_not_called()


def test_process_message_retries_on_failure_sends_to_dlq():
    producer = _mock_producer()
    original = _json.dumps({"order_id": "ord-002", "amount": 10.0}).encode()
    with patch("src.order_consumer.confirm_payment", side_effect=Exception("timeout")):
        with patch("src.order_consumer.time.sleep"):
            process_message(producer, original, {"order_id": "ord-002", "amount": 10.0})
    producer.send.assert_called_once()
    call_kwargs = producer.send.call_args
    assert call_kwargs[0][0] == "order.payment.dlq"
