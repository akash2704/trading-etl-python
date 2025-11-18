# src/db/setup.py
import time
import psycopg2
from psycopg2.extensions import connection

from src.settings import (
    log,
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASS,
)

# This is the table where we'll store our final, calculated data
TABLE_NAME = "stock_indicators"


def get_db_connection() -> connection:
    """
    Establishes a connection to the TimescaleDB.
    """
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
            )
            log.info("Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            log.warning(f"DB connection failed: {e}. Retrying... ({retries} left)")
            retries -= 1
            time.sleep(5)
    log.error("Failed to connect to database after several retries.")
    raise SystemExit("Could not connect to database.")


def setup_database():
    """
    Creates the table and converts it to a hypertable.
    This is idempotent - it won't crash if run twice.
    """
    log.info(f"Setting up database table '{TABLE_NAME}'...")

    # ⚠️ We are dropping the old table to apply the new schema.
    # This is a one-time operation for this upgrade.
    drop_table_sql = f"DROP TABLE IF EXISTS {TABLE_NAME};"

    # 1. Define the NEW table schema.
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        -- Core Data --
        time        TIMESTAMPTZ       NOT NULL,
        symbol      TEXT              NOT NULL,
        "open"      DOUBLE PRECISION,
        high        DOUBLE PRECISION,
        low         DOUBLE PRECISION,
        "close"     DOUBLE PRECISION  NOT NULL,
        volume      BIGINT,

        -- Trend Indicators --
        sma_20      DOUBLE PRECISION,
        ema_10      DOUBLE PRECISION,
        ema_20      DOUBLE PRECISION,
        macd_line   DOUBLE PRECISION,  -- Renamed from macd_12_26_9
        adx_14      DOUBLE PRECISION,
        
        -- Momentum Indicators --
        rsi_14      DOUBLE PRECISION,
        stoch_k_14  DOUBLE PRECISION,
        mfi_14      DOUBLE PRECISION,

        -- Volatility Indicators --
        bb_upper    DOUBLE PRECISION,
        bb_lower    DOUBLE PRECISION,
        atr_14      DOUBLE PRECISION,
        
        -- Volume Indicators --
        obv         BIGINT,
        vwap        DOUBLE PRECISION,

        PRIMARY KEY (time, symbol)
    );
    """

    # 2. This is the *magic* TimescaleDB command.
    create_hypertable_sql = f"""
    SELECT create_hypertable(
        '{TABLE_NAME}', 
        by_range('time'),
        if_not_exists => TRUE
    );
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                log.warning(
                    f"--- DROPPING TABLE '{TABLE_NAME}' (if exists) for schema upgrade ---"
                )
                cur.execute(drop_table_sql)

                log.info("Creating new table with full indicator schema...")
                cur.execute(create_table_sql)

                log.info("Creating hypertable (if not exists)...")
                cur.execute(create_hypertable_sql)

            conn.commit()
        log.info("Database setup complete.")

    except Exception as e:
        log.error(f"Error setting up database: {e}")
        raise


if __name__ == "__main__":
    setup_database()
