import pandas as pd
import pandas_ta as ta
import pytest

# This is a basic sanity check.
# Your real tests will import *your* functions,
# e.g., from src.processing.indicators import calculate_sma
# and test those, not the library itself. But you start here.


@pytest.mark.asyncio
async def test_pandas_ta_sma_calculation():
    """
    Test: Verify that the SMA calculation from the library is correct.
    Why: If this fails, our entire indicator logic is wrong.
    """
    # I'm using a simple list, but in reality, this will be a pandas Series
    # from a Kafka message.
    data = pd.Series([10, 12, 15, 14, 13, 16, 18, 17, 19, 20])

    # Calculate SMA with a period of 5
    sma = ta.sma(data, length=5)

    # --- Verification ---

    # 💡 Pro tip: pandas-ta pads with NaN for periods
    # where it doesn't have enough data.
    assert pd.isna(sma.iloc[0])
    assert pd.isna(sma.iloc[3])

    # The first *real* value is at index 4 (the 5th element)
    # Prices: [10, 12, 15, 14, 13]
    # Avg: (10 + 12 + 15 + 14 + 13) / 5 = 64 / 5 = 12.8
    assert sma.iloc[4] == 12.8

    # Check the last value
    # Prices: [16, 18, 17, 19, 20]
    # Avg: (16 + 18 + 17 + 19 + 20) / 5 = 90 / 5 = 18.0
    assert sma.iloc[-1] == 18.0


@pytest.mark.asyncio
async def test_processing_empty_data():
    """
    Test: What happens if we get empty data?
    Why: Because you *will* get bad data. Guaranteed.
    """
    data = pd.Series([], dtype=float)
    sma = ta.sma(data, length=5)

    # It should just return an empty Series, not crash.
    assert sma is None
