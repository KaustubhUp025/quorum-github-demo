"""Smoke tests for the order payment consumer."""
import json as _json
from unittest.mock import patch, MagicMock
from src.order_consumer import process_message


def test_process_message_success():
    with patch("src.order_consumer.confirm_payment") as mock_confirm:
        mock_confirm.return_value = {"status": "confirmed"}
        process_message({"order_id": "ord-001", "amount": 99.99})
    mock_confirm.assert_called_once_with("ord-001", 99.99)


def test_process_message_retries_on_failure():
    with patch("src.order_consumer.confirm_payment", side_effect=Exception("timeout")):
        with patch("src.order_consumer.time.sleep"):
            process_message({"order_id": "ord-002", "amount": 10.0})
