import pytest
import pandas as pd
from datetime import datetime
import json

from market.economy import get_upgrade_value, calculate_hype_bonus, process_cc_earnings

# --- Fixtures for Mock Data ---

@pytest.fixture
def mock_shop_upgrades():
    """Fixture for shop_upgrades_df."""
    data = {
        'discord_id': ['101', '102', '101'],
        'upgrade_name': ['Study Race Tapes', 'Perfect the Starting Gate', 'Perfect the Starting Gate'],
        'tier': [2, 1, 3]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_portfolios():
    """Fixture for portfolios_df."""
    data = {
        'investor_discord_id': ['101', '102', '103', '101', '104'],
        'stock_inGameName': ['PlayerA', 'PlayerA', 'PlayerA', 'PlayerB', 'PlayerB'],
        'shares_owned': [100, 50, 200, 300, 50]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_crew_coins():
    """Fixture for crew_coins_df."""
    data = {
        'discord_id': ['101', '102', '103', '104'],
        'inGameName': ['PlayerA', 'PlayerB', 'PlayerC', 'PlayerD'],
        'balance': [1000.0, 5000.0, 2500.0, 8000.0]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_enriched_fan_log():
    """Fixture for enriched_fan_log_df."""
    data = {
        'timestamp': [datetime(2025, 1, 1, 12, 0, 0), datetime(2025, 1, 1, 12, 0, 0), datetime(2025, 1, 1, 12, 0, 0)],
        'inGameName': ['PlayerA', 'PlayerB', 'PlayerC'],
        'performancePrestigePoints': [10.0, 5.0, 2.0],
        'tenurePrestigePoints': [1.0, 1.5, 2.5]
    }
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@pytest.fixture
def mock_market_state():
    """Fixture for market_state_df."""
    data = {
        'state_name': ['active_event'],
        'state_value': ['None']
    }
    return pd.DataFrame(data)

# --- Tests for get_upgrade_value ---

def test_get_upgrade_value_with_upgrade(mock_shop_upgrades):
    """Test that existing upgrades are correctly applied."""
    result = get_upgrade_value(mock_shop_upgrades, '101', 'Perfect the Starting Gate', 10, 5)
    # Base (10) + Tier (3) * Bonus (5) = 25
    assert result == 25

def test_get_upgrade_value_no_upgrade(mock_shop_upgrades):
    """Test that base value is returned when no matching upgrade is found."""
    result = get_upgrade_value(mock_shop_upgrades, '103', 'Study Race Tapes', 10, 5)
    assert result == 10

def test_get_upgrade_value_empty_df(mock_shop_upgrades):
    """Test that base value is returned for an empty DataFrame."""
    empty_df = pd.DataFrame(columns=mock_shop_upgrades.columns)
    result = get_upgrade_value(empty_df, '101', 'Study Race Tapes', 10, 5)
    assert result == 10

# --- Tests for calculate_hype_bonus ---

def test_calculate_hype_bonus_with_shareholders(mock_portfolios):
    """Test hype bonus calculation, excluding self-owned shares."""
    # PlayerA has 50 (from 102) + 200 (from 103) = 250 shares owned by others
    # Hype Bonus = 1 + (0.0005 * 250) = 1.125
    result = calculate_hype_bonus(mock_portfolios, 'PlayerA', '101')
    assert pytest.approx(result) == 1.125

def test_calculate_hype_bonus_no_other_shareholders(mock_portfolios):
    """Test hype bonus is 1.0 when only the player owns their shares."""
    # Add self-owned shares for PlayerC
    portfolios_df = pd.concat([
        mock_portfolios,
        pd.DataFrame([{'investor_discord_id': '103', 'stock_inGameName': 'PlayerC', 'shares_owned': 500}])
    ], ignore_index=True)
    result = calculate_hype_bonus(portfolios_df, 'PlayerC', '103')
    assert result == 1.0

def test_calculate_hype_bonus_no_shares_exist(mock_portfolios):
    """Test hype bonus is 1.0 for a player with no shares in the market."""
    result = calculate_hype_bonus(mock_portfolios, 'PlayerC', '103')
    assert result == 1.0

# --- Tests for process_cc_earnings ---

def test_process_cc_earnings_base_calculation(mock_enriched_fan_log, mock_crew_coins, mock_portfolios, mock_shop_upgrades, mock_market_state):
    """Test the basic earnings calculation including incoming dividends."""
    market_data = {
        'crew_coins': mock_crew_coins,
        'portfolios': mock_portfolios,
        'shop_upgrades': mock_shop_upgrades,
        'market_state': mock_market_state
    }
    run_timestamp = datetime.now()

    updated_balances, transactions = process_cc_earnings(mock_enriched_fan_log, market_data, run_timestamp)

    # --- PlayerA Earnings ---
    # Personal: 197.15625
    # Dividend from PlayerB: 7.1675
    # Total Gained: 197.15625 + 7.1675 = 204.32375
    # New Balance: 1000 + 204.32375 = 1204.32375
    player_a_balance = updated_balances[updated_balances['inGameName'] == 'PlayerA']['balance'].iloc[0]
    assert pytest.approx(player_a_balance) == 1204.32375

    # Check that a transaction was created for PlayerA's earnings
    player_a_txn = next(t for t in transactions if t['actor_id'] == '101' and t['transaction_type'] == 'PERIODIC_EARNINGS')
    assert player_a_txn is not None
    assert pytest.approx(player_a_txn['cc_amount']) == 197.15625

    # Check for dividend transaction received by PlayerA
    player_a_div_txn = next(t for t in transactions if t['actor_id'] == '101' and t['transaction_type'] == 'DIVIDEND')
    assert player_a_div_txn is not None
    assert pytest.approx(player_a_div_txn['cc_amount']) == 7.1675

def test_process_cc_earnings_dividend_payouts(mock_enriched_fan_log, mock_crew_coins, mock_portfolios, mock_shop_upgrades, mock_market_state):
    """Test that dividend payouts are correctly calculated and applied."""
    market_data = {
        'crew_coins': mock_crew_coins,
        'portfolios': mock_portfolios,
        'shop_upgrades': mock_shop_upgrades,
        'market_state': mock_market_state
    }
    run_timestamp = datetime.now()

    updated_balances, transactions = process_cc_earnings(mock_enriched_fan_log, market_data, run_timestamp)

    # --- PlayerA's personal earnings: 197.15625 ---

    # --- Check Tier 1 Dividend (Sponsorship) paid by PlayerA ---
    # Largest shareholder of PlayerA is PlayerC (ID 103) with 200 shares.
    # Sponsorship Dividend = 0.20 * 197.15625 = 39.43125
    player_c_initial_balance = 2500
    # Player C personal earnings: 16.0 (no hype)
    # Player C new balance = 2500 (initial) + 16.0 (personal) + 39.43125 (dividend from A) = 2555.43125
    player_c_balance = updated_balances[updated_balances['inGameName'] == 'PlayerC']['balance'].iloc[0]
    assert pytest.approx(player_c_balance) == 2555.43125

    # --- Check Tier 2 Dividend (Proportional) paid by PlayerA ---
    # Second largest shareholder is PlayerB (ID 102) with 50 shares.
    # Proportional Pool = 0.10 * 197.15625 = 19.715625
    player_b_initial_balance = 5000
    # Player B personal earnings: 35.8375
    # Player B new balance = 5000 (initial) + 35.8375 (personal) + 19.715625 (dividend from A) = 5055.553125
    player_b_balance = updated_balances[updated_balances['inGameName'] == 'PlayerB']['balance'].iloc[0]
    assert pytest.approx(player_b_balance) == 5055.553125

    # Check that dividend transactions were created
    div_t1_txn = next(t for t in transactions if t['transaction_type'] == 'DIVIDEND' and t['actor_id'] == '103')
    div_t2_txn = next(t for t in transactions if t['transaction_type'] == 'DIVIDEND' and t['actor_id'] == '102')
    assert div_t1_txn is not None
    assert div_t2_txn is not None
    assert pytest.approx(div_t1_txn['cc_amount']) == 39.43125
    assert pytest.approx(div_t2_txn['cc_amount']) == 19.715625

def test_process_cc_earnings_event_modifier(mock_enriched_fan_log, mock_crew_coins, mock_portfolios, mock_shop_upgrades):
    """Test that event modifiers correctly affect earnings."""
    market_state_event_df = pd.DataFrame({
        'state_name': ['active_event'],
        'state_value': ['The Grand Derby'] # 12.0 performance yield modifier
    })
    market_data = {
        'crew_coins': mock_crew_coins,
        'portfolios': mock_portfolios,
        'shop_upgrades': mock_shop_upgrades,
        'market_state': market_state_event_df
    }
    run_timestamp = datetime.now()

    updated_balances, _ = process_cc_earnings(mock_enriched_fan_log, market_data, run_timestamp)

    # --- PlayerA Earnings (with event) ---
    # Personal: 457.03125
    # --- PlayerB Earnings (with event) ---
    # Personal: ( (5+5) * (1.75 + 12.0) + 3.0 ) * 1.175 = (10 * 13.75 + 3.0) * 1.175 = (137.5 + 3.0) * 1.175 = 140.5 * 1.175 = 165.0875
    # --- Dividend from B to A (with event) ---
    # 0.20 * 165.0875 = 33.0175
    # --- PlayerA Total Gained ---
    # 457.03125 + 33.0175 = 490.04875
    # New Balance: 1000 + 490.04875 = 1490.04875

    player_a_balance = updated_balances[updated_balances['inGameName'] == 'PlayerA']['balance'].iloc[0]
    assert pytest.approx(player_a_balance) == 1490.04875
