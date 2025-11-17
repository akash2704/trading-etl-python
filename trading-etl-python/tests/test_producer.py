import asyncio
import pytest
from unittest.mock import AsyncMock, patch

# We import the *specific function* we want to test
from src.ingestion.producer import fetch_and_send

# We need these from settings to verify the payload
from src.settings import RAW_DATA_TOPIC


@pytest.mark.asyncio
async def test_fetch_and_send_success():
    """
    Test: Does fetch_and_send correctly process and send data?
    Why: This tests our core producer logic, not external APIs.
    """
    # 1. SETUP
    symbol = "FAKE"

    # This is the fake data we're "getting" from yfinance
    fake_yf_data = {
        "currentPrice": 123.45,
        "regularMarketTime": 1731834000,  # A fake timestamp (seconds)
    }

    # This is the "spy" producer. We only care about its .send method.
    mock_producer = AsyncMock()
    mock_producer.send = AsyncMock()  # Spy on the .send() method

    # 2. THE MOCK
    # We "patch" asyncio.to_thread *as it's seen by the producer module*.
    # It will now return our fake data instead of calling yfinance.
    with patch(
        "src.ingestion.producer.asyncio.to_thread", new_callable=AsyncMock
    ) as mock_to_thread:
        # Tell our mock to return our fake data
        mock_to_thread.return_value = fake_yf_data

        # 3. EXECUTE
        # Run the function with our mock producer
        await fetch_and_send(mock_producer, symbol)

    # 4. ASSERT

    # Did we *try* to fetch data?
    mock_to_thread.assert_called_once()

    # Did we *try* to send a message?
    mock_producer.send.assert_called_once()

    # --- This is the critical part: Did we send the *right* data? ---

    # Get the arguments that .send() was called with
    call_args = mock_producer.send.call_args
    sent_topic = call_args.args[0]
    sent_value = call_args.kwargs["value"]
    sent_key = call_args.kwargs["key"]

    # Check the payload
    assert sent_topic == RAW_DATA_TOPIC
    assert sent_key == b"FAKE"
    assert sent_value["symbol"] == "FAKE"
    assert sent_value["price"] == 123.45

    # Did we correctly convert seconds to milliseconds?
    assert sent_value["timestamp"] == 1731834000 * 1000


@pytest.mark.asyncio
async def test_fetch_and_send_yfinance_failure():
    """
    Test: If yfinance gives bad data, do we *skip* sending, not crash?
    Why: Ensures robustness. We don't want to send null data.
    """
    # 1. SETUP
    symbol = "BAD"

    # This fake data is missing 'currentPrice'
    fake_yf_data = {"regularMarketTime": 1731834000}

    # Our "spy" producer
    mock_producer = AsyncMock()
    mock_producer.send = AsyncMock()

    # 2. THE MOCK
    with patch(
        "src.ingestion.producer.asyncio.to_thread", new_callable=AsyncMock
    ) as mock_to_thread:
        mock_to_thread.return_value = fake_yf_data

        # 3. EXECUTE
        await fetch_and_send(mock_producer, symbol)

    # 4. ASSERT
    # We should *not* have called .send()
    mock_producer.send.assert_not_called()
