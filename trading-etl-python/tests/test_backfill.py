import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

# Functions to test
from src.db.backfill import main as backfill_main, calculate_indicators
from src.settings import STOCKS_TO_WATCH

# This is our fake data from yfinance
FAKE_DATA = pd.DataFrame({
    'Date': pd.to_datetime(['2025-11-17', '2025-11-18']),
    'Open': [100, 102],
    'High': [110, 103],
    'Low': [98, 101],
    'Close': [105, 102],
    'Volume': [1000, 1200]
})
FAKE_DATA.set_index('Date', inplace=True)

@pytest.fixture
def mock_db_conn_backfill():
    """Mocks db connection and execute_values for backfill."""
    with patch('src.db.setup.get_db_connection') as mock_setup_conn, \
         patch('src.db.backfill.get_db_connection') as mock_backfill_conn, \
         patch('src.db.backfill.execute_values') as mock_exec:
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = MagicMock()
        mock_setup_conn.return_value = mock_conn
        mock_backfill_conn.return_value = mock_conn
        
        yield mock_conn, mock_exec

@pytest.fixture
def mock_yf():
    """Mocks yfinance.download to return our fake data."""
    with patch('src.db.backfill.yf.download', return_value=FAKE_DATA) as mock_download:
        yield mock_download

@pytest.fixture
def mock_ta_backfill():
    """Mocks pandas_ta functions for backfill."""
    # Bug Fix: Mocks must return a Series/DataFrame with the *correct index*
    mock_series = pd.Series([10.0, 11.0], index=FAKE_DATA.index)
    mock_df_1 = pd.DataFrame({'col1': [0.5, 0.6]}, index=FAKE_DATA.index)
    mock_adx_df = pd.DataFrame({'ADX_14': [20.0, 22.0]}, index=FAKE_DATA.index)
    mock_stoch_df = pd.DataFrame({'STOCHk_14_3_3': [80.0, 82.0]}, index=FAKE_DATA.index)
    
    # Bug Fix: bbands mock must return 3 columns
    mock_bb_df = pd.DataFrame({
        'BBL_20_2.0': [9.0, 9.5],
        'BBM_20_2.0': [10.0, 10.5],
        'BBU_20_2.0': [11.0, 11.5]
    }, index=FAKE_DATA.index)
    
    # --- LINTER FIX: Rename all unused 'as' variables to '_' ---
    with patch('src.db.backfill.ta.sma', return_value=mock_series) as _, \
         patch('src.db.backfill.ta.ema', return_value=mock_series) as _, \
         patch('src.db.backfill.ta.rsi', return_value=mock_series) as _, \
         patch('src.db.backfill.ta.macd', return_value=mock_df_1) as _, \
         patch('src.db.backfill.ta.bbands', return_value=mock_bb_df) as _, \
         patch('src.db.backfill.ta.adx', return_value=mock_adx_df) as _, \
         patch('src.db.backfill.ta.stoch', return_value=mock_stoch_df) as _, \
         patch('src.db.backfill.ta.atr', return_value=mock_series) as _, \
         patch('src.db.backfill.ta.mfi', return_value=mock_series) as _, \
         patch('src.db.backfill.ta.obv', return_value=mock_series) as _, \
         patch('src.db.backfill.ta.vwap', return_value=mock_series) as _:
    # --- END LINTER FIX ---
        
        yield

@pytest.mark.asyncio
async def test_calculate_indicators_flaky_adx(mock_ta_backfill):
    """
    Test: Test the helper function when an indicator fails.
    """
    # --- LINTER FIX: Rename 'mock_adx' to '_' ---
    with patch('src.db.backfill.ta.adx', return_value=None) as _:
    # --- END LINTER FIX ---
        data = FAKE_DATA.copy()
        data['symbol'] = "FAKE"
        
        result_df = calculate_indicators(data)
        
        assert 'adx_14' in result_df.columns
        assert result_df['adx_14'].isna().all()

@pytest.mark.asyncio
async def test_backfill_e2e_success(mock_db_conn_backfill, mock_yf, mock_ta_backfill):
    """
    Test: Run the main backfill function.
    Assert: It calls yf.download and inserts the correct, lowercase data.
    """
    mock_conn, mock_exec = mock_db_conn_backfill
    
    with patch('src.db.backfill.setup_database') as mock_setup:
        backfill_main()

    mock_setup.assert_called_once()
    assert mock_yf.call_count == len(STOCKS_TO_WATCH)
    mock_exec.assert_called_once()
    
    call_args = mock_exec.call_args
    records = call_args[0][2]
    
    assert len(records) == len(STOCKS_TO_WATCH) * 2
    
    first_record = records[0]
    
    assert first_record[2] == 100 # open
    assert first_record[3] == 110 # high
    assert first_record[4] == 98  # low
    assert first_record[5] == 105 # close
    
    # Bug Fix: Mocks are now working and return 10.0
    assert first_record[7] == 10.0 # sma_20 (from mock)