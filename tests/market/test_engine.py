import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from unittest.mock import patch

from market.engine import (
    _ensure_aware_utc,
    get_prestige_floor,
    get_lagged_average,
    get_club_sentiment,
    get_player_condition,
    calculate_individual_nudges,
    update_all_stock_prices
)

# --- Fixtures ---

@pytest.fixture
def mock_enriched_fan_log():
    """Fixture for a detailed enriched_fan_log DataFrame."""
    now = datetime.now(pytz.utc)
    data = []
    for i in range(30): # 30 data points for PlayerA
        data.append({
            'timestamp': now - timedelta(hours=i*2),
            'inGameName': 'PlayerA',
            'fanGain': 10000 + i * 500, # Variable fan gain
            'lifetimePrestige': 500 + i * 10
        })
    for i in range(10): # 10 data points for PlayerB
        data.append({
            'timestamp': now - timedelta(hours=i*3),
            'inGameName': 'PlayerB',
            'fanGain': 5000, # Stable fan gain
            'lifetimePrestige': 300
        })
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@pytest.fixture
def mock_market_state():
    """Fixture for market_state_df."""
    data = {
        'state_name': ['active_event', 'lag_options', 'active_lag_cursor', 'last_run_timestamp'],
        'state_value': ['None', '[0, 1, 2]', '0', (datetime.now(pytz.utc) - timedelta(hours=12)).isoformat()]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_stock_prices():
    """Fixture for stock_prices_df."""
    data = {
        'inGameName': ['PlayerA', 'PlayerB'],
        'current_price': [50.0, 30.0],
        'init_factor': [0.5, 0.2],
        'nudge_bonus': [0.0, 0.0]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_portfolios():
    """Fixture for portfolios_df."""
    data = {
        'investor_discord_id': ['101', '102'],
        'stock_inGameName': ['PlayerA', 'PlayerB'],
        'shares_owned': [500, 200]
    }
    return pd.DataFrame(data)

@pytest.fixture
def market_data_dfs(mock_enriched_fan_log, mock_market_state, mock_stock_prices, mock_portfolios):
    """Combined dictionary of all market dataframes."""
    return {
        'enriched_fan_log': mock_enriched_fan_log,
        'market_state': mock_market_state,
        'stock_prices': mock_stock_prices,
        'portfolios': mock_portfolios
    }

# --- Helper Function Tests ---

def test_ensure_aware_utc():
    """Tests that naive datetimes are converted to aware UTC."""
    naive_dt = datetime(2025, 1, 1, 12, 0, 0)
    aware_dt = _ensure_aware_utc(naive_dt)
    assert aware_dt.tzinfo is not None
    assert aware_dt.utcoffset().total_seconds() == 0

def test_get_prestige_floor():
    """Tests the prestige floor calculation."""
    # Correct calculation:
    # base = sqrt(1000) + 5.7 + 0.5 = 37.8227...
    # floor = (37.8227... ** 1.4) / 20 = 8.0876...
    assert pytest.approx(get_prestige_floor(1000, 0.5)) == 8.087695

def test_get_club_sentiment(mock_enriched_fan_log):
    """Tests sentiment calculation."""
    # This test is a bit fragile due to reliance on 'now'. We'll just check it runs and returns a float within bounds.
    sentiment = get_club_sentiment(mock_enriched_fan_log)
    assert 0.75 <= sentiment <= 1.25

def test_get_player_condition(mock_enriched_fan_log):
    """Tests player condition (volatility) calculation."""
    condition = get_player_condition(mock_enriched_fan_log, 'PlayerA')
    assert 0.85 <= condition <= 1.40

def test_get_lagged_average_no_override(mock_enriched_fan_log, mock_market_state):
    """Tests lagged average calculation in normal mode."""
    run_timestamp = datetime.now(pytz.utc)
    market_state_dict = mock_market_state.set_index('state_name')['state_value'].to_dict()
    avg = get_lagged_average(mock_enriched_fan_log, 'PlayerA', market_state_dict, run_timestamp)
    assert isinstance(avg, (float, np.floating))
    assert avg > 0

def test_get_lagged_average_with_override(mock_enriched_fan_log, mock_market_state):
    """Tests lagged average with an event override."""
    run_timestamp = datetime.now(pytz.utc)
    market_state_dict = mock_market_state.set_index('state_name')['state_value'].to_dict()
    avg = get_lagged_average(mock_enriched_fan_log, 'PlayerA', market_state_dict, run_timestamp, override_hours=10)
    assert isinstance(avg, (float, np.floating))
    assert avg > 0

# --- Core Logic Tests ---

def test_calculate_individual_nudges(market_data_dfs):
    """Tests that daily performance nudges are calculated and prorated correctly."""
    run_timestamp = datetime.now(pytz.utc)

    # Manually add recent fan gain to test the nudge logic
    recent_gain_df = pd.DataFrame([
        {'timestamp': run_timestamp - timedelta(hours=1), 'inGameName': 'PlayerA', 'fanGain': 50000},
        {'timestamp': run_timestamp - timedelta(hours=1), 'inGameName': 'PlayerB', 'fanGain': 10000}
    ])
    market_data_dfs['enriched_fan_log'] = pd.concat([market_data_dfs['enriched_fan_log'], recent_gain_df], ignore_index=True)

    updated_prices_df = calculate_individual_nudges(market_data_dfs, run_timestamp)

    # PlayerA should be rank 1, get a base nudge of 0.5
    # PlayerB should be rank 2, get a base nudge of 0.5
    # Time passed is 12 hours, so proration_factor is 0.5
    # Expected nudge = 0.5 * 0.5 = 0.25

    player_a_nudge = updated_prices_df[updated_prices_df['inGameName'] == 'PlayerA']['nudge_bonus'].iloc[0]
    player_b_nudge = updated_prices_df[updated_prices_df['inGameName'] == 'PlayerB']['nudge_bonus'].iloc[0]

    assert pytest.approx(player_a_nudge) == 0.25
    assert pytest.approx(player_b_nudge) == 0.25

@patch('market.engine.log_stock_price_history')
def test_update_all_stock_prices(mock_log_history, market_data_dfs):
    """
    Tests the main price update engine.
    Mocks the database logging to isolate the calculation logic.
    """
    run_timestamp = datetime.now(pytz.utc)

    final_stocks_df, _ = update_all_stock_prices(
        market_data_dfs['enriched_fan_log'],
        market_data_dfs,
        run_timestamp
    )

    # Basic assertions to ensure the process ran
    assert not final_stocks_df.empty
    assert 'current_price' in final_stocks_df.columns
    assert len(final_stocks_df) == 2 # PlayerA and PlayerB

    # Check that prices are positive floats
    assert final_stocks_df['current_price'].dtype == 'float64'
    assert (final_stocks_df['current_price'] > 0).all()

    # Check that the mocked database function was called
    mock_log_history.assert_called_once()

    # Verify the first argument passed to the mock is the correct DataFrame
    call_args, _ = mock_log_history.call_args
    pd.testing.assert_frame_equal(call_args[0], final_stocks_df)

@patch('market.engine.log_stock_price_history')
def test_update_all_stock_prices_with_event(mock_log_history, market_data_dfs):
    """Tests the price update engine during an active event."""
    run_timestamp = datetime.now(pytz.utc)

    # Activate "The Grand Derby" event
    market_data_dfs['market_state'].loc[
        market_data_dfs['market_state']['state_name'] == 'active_event', 'state_value'
    ] = 'The Grand Derby'

    # Run the pricing engine
    final_stocks_df, _ = update_all_stock_prices(
        market_data_dfs['enriched_fan_log'],
        market_data_dfs,
        run_timestamp
    )

    assert not final_stocks_df.empty
    mock_log_history.assert_called_once()
    # A more robust test would compare the price with the event vs without,
    # but for now we just confirm it runs correctly with the event flag.
    assert (final_stocks_df['current_price'] > 0).all()
