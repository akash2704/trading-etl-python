# src/ingestion/producer.py

import asyncio
import json
import time
import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging

# Import our configs
from src.settings import (
    KAFKA_BROKER_URL,
    RAW_DATA_TOPIC,
    STOCKS_TO_WATCH,
    POLL_INTERVAL_SECONDS,
    log,
)


def create_kafka_producer() -> KafkaProducer:
    """
    Creates the KafkaProducer.
    I'm adding retries because on startup, the producer might try
    to connect before the broker (RedPanda) is fully ready.
    """
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                # I'm serializing our messages as JSON
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # --- THE FIX ---
                # acks='1' (a string) was wrong. It needs to be an integer 1.
                acks=1,
                # --- END FIX ---
            )
            log.info("KafkaProducer connected successfully.")
            return producer
        except NoBrokersAvailable:
            log.warning(
                f"No Kafka brokers available. Retrying in 5s... ({retries} left)"
            )
            retries -= 1
            time.sleep(5)

    log.error("Failed to connect to Kafka after several retries. Exiting.")
    raise SystemExit("Could not connect to Kafka.")


async def fetch_and_send(producer: KafkaProducer, symbol: str):
    """
    Fetches stock data for a *single* symbol and sends it to Kafka.
    """
    try:
        # 1. Fetch data. yfinance is blocking, so we run it in a thread.
        def fetch_info():
            # This is the blocking call
            ticker = yf.Ticker(symbol)
            # .fast_info is unreliable. .info is slower but works.
            return ticker.info

        # Run the blocking function in a separate thread
        data = await asyncio.to_thread(fetch_info)

        # 2. We need 'currentPrice' and 'regularMarketTime'
        price = data.get("currentPrice")
        # 'regularMarketTime' is a UNIX timestamp *in seconds*
        timestamp_s = data.get("regularMarketTime")

        if price is None or timestamp_s is None:
            log.warning(
                f"No 'currentPrice' or 'regularMarketTime' for {symbol}. Skipping. (Market might be closed)"
            )
            return

        # Convert to milliseconds
        timestamp_ms = int(timestamp_s) * 1000

        # 3. Prepare the message payload
        payload = {
            "symbol": symbol,
            "price": price,
            "timestamp": timestamp_ms,  # This is a UNIX timestamp
            "fetched_at": int(time.time() * 1000),  # Our internal fetch time
        }

        # 4. Send to Kafka.
        producer.send(RAW_DATA_TOPIC, value=payload, key=symbol.encode("utf-8"))

        log.info(f"Sent {symbol} data: Price={price}")

    except Exception as e:
        # ⚠️ Don't let one failed symbol crash the whole producer
        log.error(f"Error fetching/sending data for {symbol}: {e}")


async def main_loop():
    """
    The main producer loop.
    Fetches data for all stocks in parallel and then sleeps.
    """
    log.info("Starting producer...")
    producer = create_kafka_producer()

    while True:
        start_time = time.time()
        log.info(f"--- Starting new fetch cycle for {len(STOCKS_TO_WATCH)} stocks ---")

        tasks = [fetch_and_send(producer, symbol) for symbol in STOCKS_TO_WATCH]

        # Run all fetch/send tasks in parallel
        await asyncio.gather(*tasks)

        # Flush messages from producer's buffer to Kafka
        producer.flush()

        end_time = time.time()
        duration = end_time - start_time
        log.info(f"--- Fetch cycle finished in {duration:.2f}s ---")

        # Wait for the next poll interval
        sleep_time = max(0, POLL_INTERVAL_SECONDS - duration)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


def main():
    """Entry point for pyproject.toml script"""
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        log.info("Producer shutting down...")
    except SystemExit as e:
        log.error(e)


if __name__ == "__main__":
    main()
