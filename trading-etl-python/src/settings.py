# src/settings.py

import logging

# --- Stock/Market Settings ---
# Focusing on a few liquid NSE stocks
STOCKS_TO_WATCH = [
    "RELIANCE.NS",
    "TCS.NS",
    "HDFCBANK.NS",
    "INFY.NS",
    "ICICIBANK.NS",
    "SBIN.NS",
]

# --- Kafka/RedPanda Settings ---
KAFKA_BROKER_URL = "localhost:9092"
RAW_DATA_TOPIC = "raw_stock_prices" # This is our 'firehose'

# --- Ingestion Settings ---
# Poll yfinance every 5 seconds.
# ⚠️ Don't go lower, yfinance isn't a real-time API. 
# We're simulating.
POLL_INTERVAL_SECONDS = 5

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

log = logging.getLogger(__name__)