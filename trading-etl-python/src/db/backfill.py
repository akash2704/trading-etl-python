# src/db/backfill.py
import yfinance as yf
import pandas_ta as ta
import pandas as pd
import numpy as np
from psycopg2.extras import execute_values

from src.settings import log, STOCKS_TO_WATCH
from src.db.setup import get_db_connection, TABLE_NAME, setup_database

def calculate_indicators(data: pd.DataFrame) -> pd.DataFrame:
    """Helper function to calculate all indicators on a DataFrame."""
    log.info("Calculating indicators...")
    
    # Price-based
    data['sma_20'] = ta.sma(data['Close'], length=20)
    data['ema_10'] = ta.ema(data['Close'], length=10)
    data['ema_20'] = ta.ema(data['Close'], length=20)
    data['rsi_14'] = ta.rsi(data['Close'], length=14)
    
    # MACD
    macd = ta.macd(data['Close'], fast=12, slow=26, signal=9)
    if macd is not None and not macd.empty:
        data['macd_line'] = macd.iloc[:, 0]
    else:
        data['macd_line'] = np.nan
        
    # Bollinger Bands
    bbands = ta.bbands(data['Close'], length=20, std=2)
    if bbands is not None and not bbands.empty:
        data['bb_upper'] = bbands.iloc[:, 2]
        data['bb_lower'] = bbands.iloc[:, 0]
    else:
        data['bb_upper'] = np.nan
        data['bb_lower'] = np.nan

    # HLC-based
    adx_result = ta.adx(data['High'], data['Low'], data['Close'], length=14)
    if adx_result is not None and not adx_result.empty:
        data['adx_14'] = adx_result.iloc[:, 0]
    else:
        data['adx_14'] = np.nan
        log.warning(f"ADX calculation failed for {data['symbol'].iloc[0]}.")

    stoch_result = ta.stoch(data['High'], data['Low'], data['Close'], k=14, d=3, smooth_k=3)
    if stoch_result is not None and not stoch_result.empty:
        data['stoch_k_14'] = stoch_result.iloc[:, 0]
    else:
        data['stoch_k_14'] = np.nan
        log.warning(f"Stochastic calculation failed for {data['symbol'].iloc[0]}.")
        
    data['atr_14'] = ta.atr(data['High'], data['Low'], data['Close'], length=14)

    # HLCV-based
    data['mfi_14'] = ta.mfi(data['High'], data['Low'], data['Close'], data['Volume'], length=14)
    data['obv'] = ta.obv(data['Close'], data['Volume'])
    data['vwap'] = ta.vwap(data['High'], data['Low'], data['Close'], data['Volume'])

    return data


def backfill_stock_data():
    """
    Downloads 1 year of *daily* data, calculates all indicators,
    and bulk-inserts into the database.
    """
    log.info("--- Starting 1-Year Daily Backfill ---")
    
    all_records_to_insert = []
    
    # --- FIX: DB_COLUMNS are now ALL lowercase ---
    DB_COLUMNS = [
        'time', 'symbol', 'open', 'high', 'low', 'close', 'volume',
        'sma_20', 'ema_10', 'ema_20', 'macd_line', 'adx_14',
        'rsi_14', 'stoch_k_14', 'mfi_14',
        'bb_upper', 'bb_lower', 'atr_14',
        'obv', 'vwap'
    ]

    for symbol in STOCKS_TO_WATCH:
        log.info(f"Backfilling data for {symbol}...")
        try:
            data = yf.download(
                symbol, 
                period="1y", 
                interval="1d",
                progress=False
            )
            
            if data.empty:
                log.warning(f"No data found for {symbol}. Skipping.")
                continue
            
            data['symbol'] = symbol

            data = calculate_indicators(data.copy())

            # 3. Clean and Format
            data.reset_index(inplace=True)
            
            # --- THIS IS THE FIX ---
            # Rename ALL yfinance columns to match our lowercase DB schema
            data.rename(columns={
                "Date": "time",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            }, inplace=True)
            # --- END FIX ---
            
            data['time'] = pd.to_datetime(data['time']).dt.tz_localize('UTC')
            
            # Replace numpy's "Not a Number" with None for psycopg2
            data.replace({np.nan: None}, inplace=True)

            # 4. Prepare for insertion
            final_data = data[DB_COLUMNS] # This will now work
            records = final_data.values.tolist()
            all_records_to_insert.extend(records)
            
            log.info(f"Prepared {len(records)} historical records for {symbol}.")

        except Exception as e:
            log.error(f"Failed to backfill {symbol}: {e}", exc_info=True)

    if not all_records_to_insert:
        log.info("No records to backfill. Exiting.")
        return

    log.info(f"Inserting {len(all_records_to_insert)} total records...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # --- THIS IS THE FIX ---
                # The .replace() hack is gone. All columns are lowercase.
                cols_str = f'("{'","'.join(DB_COLUMNS)}")'
                # --- END FIX ---
                
                sql_stmt = f"""
                INSERT INTO {TABLE_NAME} {cols_str}
                VALUES %s
                ON CONFLICT (time, symbol) DO NOTHING;
                """
                
                execute_values(
                    cur,
                    sql_stmt,
                    all_records_to_insert
                )
            conn.commit()
        log.info("--- Backfill Complete ---")
    except Exception as e:
        log.error(f"Database insert failed during backfill: {e}")
        if 'conn' in locals():
            conn.rollback()

def main():
    """
    Entry point for the backfill script.
    Ensures database is set up before running the backfill.
    """
    log.info("--- Running Database Setup ---")
    setup_database()
    
    backfill_stock_data()

if __name__ == "__main__":
    main()