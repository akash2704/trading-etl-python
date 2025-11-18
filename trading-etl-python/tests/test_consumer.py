import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from collections import defaultdict
import json

# Function to test
from src.processing.consumer import process_messages, TABLE_NAME, DB_COLUMNS

# Helper to create a fake Kafka message
def create_fake_message(symbol, price, timestamp):
    payload = {
        "symbol": symbol,
        "price": price,
        "timestamp": timestamp, # ms
    }
    msg = MagicMock()
    msg.value = json.dumps(payload).encode('utf-8')
    return msg

@pytest.fixture
def mock_db_conn():
    """Mocks the database connection and cursor."""
    with patch('src.processing.consumer.execute_values') as mock_exec:
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        yield mock_conn, mock_cursor, mock_exec

@pytest.fixture
def mock_ta():
    """
    Mocks all pandas_ta functions to return predictable data,
    using the *index of the data it receives*.
    """
    
    # "Smart" mocks using side_effect
    def mock_sma_func(series, length):
        return pd.Series([10.0] * len(series), index=series.index)
    
    def mock_ema_func(series, length):
        return pd.Series([11.0] * len(series), index=series.index)

    def mock_rsi_func(series, length):
        return pd.Series([50.0] * len(series), index=series.index)
    
    def mock_atr_func(high, low, close, length):
        return pd.Series([1.0] * len(high), index=high.index)

    def mock_macd_func(series, fast, slow, signal):
        return pd.DataFrame({'MACD_12_26_9': [0.5] * len(series)}, index=series.index)
    
    def mock_bbands_func(series, length, std):
        # Fix: Return 3 columns so iloc[:, 2] works
        return pd.DataFrame({
            'BBL_20_2.0': [9.0] * len(series),
            'BBM_20_2.0': [10.0] * len(series),
            'BBU_20_2.0': [11.0] * len(series)
        }, index=series.index)
    
    def mock_adx_func(high, low, close, length):
        return pd.DataFrame({'ADX_14': [25.0] * len(high)}, index=high.index)
    
    def mock_stoch_func(high, low, close, k, d, smooth_k):
        return pd.DataFrame({'STOCHk_14_3_3': [80.0] * len(high)}, index=high.index)

    # --- LINTER FIX: Rename all unused 'as' variables to '_' ---
    with patch('src.processing.consumer.ta.sma', side_effect=mock_sma_func) as _, \
         patch('src.processing.consumer.ta.ema', side_effect=mock_ema_func) as _, \
         patch('src.processing.consumer.ta.rsi', side_effect=mock_rsi_func) as _, \
         patch('src.processing.consumer.ta.macd', side_effect=mock_macd_func) as _, \
         patch('src.processing.consumer.ta.bbands', side_effect=mock_bbands_func) as _, \
         patch('src.processing.consumer.ta.adx', side_effect=mock_adx_func) as _, \
         patch('src.processing.consumer.ta.stoch', side_effect=mock_stoch_func) as _, \
         patch('src.processing.consumer.ta.atr', side_effect=mock_atr_func) as _:
    # --- END LINTER FIX ---
        
        yield

@pytest.mark.asyncio
async def test_consumer_warmup(mock_db_conn):
    """
    Test: Send fewer messages than needed for calculation.
    Assert: Nothing is inserted into the DB.
    """
    mock_conn, mock_cursor, mock_exec = mock_db_conn
    
    messages = [
        create_fake_message("AAPL", 150 + i, 1700000000 + i * 5000)
        for i in range(10)
    ]
    
    test_state = defaultdict(
        lambda: pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"]).set_index("time")
    )
    
    with patch('src.processing.consumer.state', test_state):
        inserted_count = process_messages(messages, mock_conn)

    assert inserted_count == 0
    mock_exec.assert_not_called()
    assert len(test_state["AAPL"]) == 10

@pytest.mark.asyncio
async def test_consumer_calculates_and_inserts(mock_db_conn, mock_ta):
    """
    Test: Send enough messages to trigger a calculation.
    Assert: `execute_values` is called with the correct data.
    """
    mock_conn, mock_cursor, mock_exec = mock_db_conn
    
    messages = [
        create_fake_message("AAPL", 150 + i, 1700000000 + i * 5000)
        for i in range(30)
    ]
    
    test_state = defaultdict(
        lambda: pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"]).set_index("time")
    )
    
    with patch('src.processing.consumer.state', test_state):
        inserted_count = process_messages(messages, mock_conn)

    # Bug Fix: We send 30 messages. First 25 are skipped (len < 26).
    # Messages 26, 27, 28, 29, 30 (5 messages) are inserted.
    assert inserted_count == 5
    
    mock_exec.assert_called_once()
    
    call_args = mock_exec.call_args
    sql_stmt = call_args[0][1]
    records = call_args[0][2]
    
    assert f"INSERT INTO {TABLE_NAME}" in sql_stmt
    assert len(records) == 5
    
    last_record = records[-1]
    assert last_record[DB_COLUMNS.index('symbol')] == "AAPL"
    assert last_record[DB_COLUMNS.index('close')] == 179 # 150 + 29
    
    # Bug Fix: Mocks are now working and return 10.0
    assert last_record[DB_COLUMNS.index('sma_20')] == 10.0 # From mock_ta
    
    assert last_record[DB_COLUMNS.index('ema_10')] == 11.0 # From mock_ta
    assert last_record[DB_COLUMNS.index('macd_line')] == 0.5 # From mock_ta
    
    assert last_record[DB_COLUMNS.index('mfi_14')] is None
    assert last_record[DB_COLUMNS.index('obv')] is None
    assert last_record[DB_COLUMNS.index('vwap')] is None