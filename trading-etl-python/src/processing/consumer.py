# src/processing/consumer.py
import json
import time
from collections import defaultdict

import pandas as pd
import pandas_ta as ta
import numpy as np
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values

from src.settings import (
    KAFKA_BROKER_URL,
    RAW_DATA_TOPIC,
    log,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
)
# --- FIX: Import the table name from our setup file ---
from src.db.setup import TABLE_NAME
# --- END FIX ---

KAFKA_MAX_POLL_RECORDS = 500
KAFKA_POLL_TIMEOUT_MS = 1000
# --- FIX: We deleted the old, duplicate variable ---
# INDICATORS_TABLE_NAME = "stock_indicators" 
# --- END FIX ---
LOOKBACK_PERIOD = 60

state = defaultdict(
    lambda: pd.DataFrame(
        columns=["time", "open", "high", "low", "close", "volume"]
    ).set_index("time")
)

DB_COLUMNS = [
    'time', 'symbol', 'open', 'high', 'low', 'close', 'volume',
    'sma_20', 'ema_10', 'ema_20', 'macd_line', 'adx_14',
    'rsi_14', 'stoch_k_14', 'mfi_14',
    'bb_upper', 'bb_lower', 'atr_14',
    'obv', 'vwap'
]


def get_db_connection():
    """ Creates a new database connection """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
        )
        log.info("Consumer DB connection successful.")
        return conn
    except Exception as e:
        log.error(f"Consumer DB connection failed: {e}")
        raise


def calculate_live_indicators(df: pd.DataFrame) -> pd.Series:
    """
    Calculates all *possible* indicators on the live 5-sec data.
    Returns the *latest* row of data.
    """
    # 1. Price-based
    df['sma_20'] = ta.sma(df['close'], length=20)
    df['ema_10'] = ta.ema(df['close'], length=10)
    df['ema_20'] = ta.ema(df['close'], length=20)
    df['rsi_14'] = ta.rsi(df['close'], length=14)
    
    # 2. MACD
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    if macd is not None and not macd.empty:
        df['macd_line'] = macd.iloc[:, 0]
    else:
        df['macd_line'] = np.nan
    
    # 3. Bollinger Bands
    bbands = ta.bbands(df['close'], length=20, std=2)
    if bbands is not None and not bbands.empty:
        df['bb_upper'] = bbands.iloc[:, 2]
        df['bb_lower'] = bbands.iloc[:, 0]
    else:
        df['bb_upper'] = np.nan
        df['bb_lower'] = np.nan

    # 4. HLC-based (using our "faked" HLC data)
    adx_result = ta.adx(df['high'], df['low'], df['close'], length=14)
    if adx_result is not None and not adx_result.empty:
        df['adx_14'] = adx_result.iloc[:, 0]
    else:
        df['adx_14'] = np.nan

    stoch_result = ta.stoch(df['high'], df['low'], df['close'], k=14, d=3, smooth_k=3)
    if stoch_result is not None and not stoch_result.empty:
        df['stoch_k_14'] = stoch_result.iloc[:, 0]
    else:
        df['stoch_k_14'] = np.nan
        
    df['atr_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)

    # 5. Volume-based (IMPOSSIBLE - SET TO NULL)
    df['mfi_14'] = np.nan
    df['obv'] = np.nan
    df['vwap'] = np.nan
    
    # 6. Core HLCV data (which we faked)
    df['open'] = df['open']
    df['high'] = df['high']
    df['low'] = df['low']
    df['volume'] = df['volume']

    return df.iloc[-1]


def process_messages(messages: list, db_conn) -> int:
    """
    Core logic: update state, calculate indicators, batch-insert.
    """
    records_to_insert = []
    
    for msg in messages:
        try:
            data = json.loads(msg.value.decode('utf-8'))
            symbol = data['symbol']
            price = data['price']
            timestamp = pd.to_datetime(data['timestamp'], unit='ms', utc=True)

            df = state[symbol]
            
            new_row = {
                "open": price, "high": price, "low": price, "close": price,
                "volume": np.nan
            }
            df.loc[timestamp] = new_row
            
            if len(df) > LOOKBACK_PERIOD:
                df = df.iloc[-LOOKBACK_PERIOD:]
                
            if len(df) < 26:
                state[symbol] = df
                continue
                
            latest_data = calculate_live_indicators(df.copy())
            
            if pd.isna(latest_data['sma_20']):
                state[symbol] = df
                continue
            
            record = [
                latest_data.name, # time
                symbol,
                *latest_data[DB_COLUMNS[2:]].replace({np.nan: None}).values
            ]
            records_to_insert.append(record)
            
            state[symbol] = df

        except Exception as e:
            log.error(f"Error processing message: {data} | Error: {e}", exc_info=True)
            continue

    if records_to_insert:
        try:
            with db_conn.cursor() as cur:
                cols_str = f'("{'","'.join(DB_COLUMNS)}")'
                cols_str = cols_str.replace('"Open"', '"open"').replace('"Close"', '"close"')

                # --- FIX: Use the imported TABLE_NAME ---
                sql_stmt = f"""
                INSERT INTO {TABLE_NAME} {cols_str}
                VALUES %s
                ON CONFLICT (time, symbol) DO NOTHING;
                """
                # --- END FIX ---
                
                execute_values(
                    cur,
                    sql_stmt,
                    records_to_insert
                )
            db_conn.commit()
            return len(records_to_insert)
        except Exception as e:
            log.error(f"Database insert failed: {e}")
            db_conn.rollback()
            
    return 0


def main_loop():
    log.info("Starting consumer...")
    
    try:
        consumer = KafkaConsumer(
            RAW_DATA_TOPIC,
            bootstrap_servers=KAFKA_BROKER_URL,
            auto_offset_reset='earliest',
            group_id='indicator_calculators_v2',
            max_poll_records=KAFKA_MAX_POLL_RECORDS,
        )
    except Exception as e:
        log.error(f"Failed to connect to Kafka: {e}")
        raise SystemExit("Kafka connection failed.")

    db_conn = get_db_connection()
    log.info("Consumer started. Polling for messages...")
    
    while True:
        try:
            msg_pack = consumer.poll(timeout_ms=KAFKA_POLL_TIMEOUT_MS)
            if not msg_pack:
                continue

            all_messages = []
            for tp, messages in msg_pack.items():
                all_messages.extend(messages)
            
            if not all_messages:
                continue
                
            start_time = time.time()
            log.info(f"Received {len(all_messages)} messages.")
            
            inserted_count = process_messages(all_messages, db_conn)
            
            consumer.commit() 
            
            duration = time.time() - start_time
            log.info(f"Processed batch in {duration:.2f}s. Inserted {inserted_count} new indicator rows.")
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.error(f"Unhandled error in consumer loop: {e}", exc_info=True)
            time.sleep(5)

    log.info("Consumer shutting down...")
    db_conn.close()
    consumer.close()


def main():
    """ Entry point for pyproject.toml script """
    # We do NOT run setup_database() here anymore.
    # The backfill script is responsible for that.
    # We just connect and run.
    main_loop()

if __name__ == "__main__":
    main()