# src/settings.py
import logging
import os
from dotenv import load_dotenv
load_dotenv()
# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# --- Kafka/RedPanda Settings ---
# Read from environment, with a default for local dev
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
RAW_DATA_TOPIC = "raw_stock_prices"

# --- Database Settings ---
# Read all DB config from the environment
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "trading_db")
DB_USER = os.getenv("DB_USER", "trader")
DB_PASS = os.getenv("DB_PASS", "secret") # Default for local only

# 💡 Pro tip: Add a warning so you don't accidentally deploy this
if DB_PASS == "secret":
    log.warning(
        "--- ⚠️ WARNING: Using default 'secret' password. "
        "Set DB_PASS in your .env file for local, or "
        "in your production environment variables. ---"
    )

# --- Stock/Market Settings ---
STOCKS_TO_WATCH = [
    "RELIANCE.NS",
    "TCS.NS",
    "HDFCBANK.NS",
    "INFY.NS",
    "ICICIBANK.NS",
    "SBIN.NS",
]

# --- Ingestion Settings ---
POLL_INTERVAL_SECONDS = 5