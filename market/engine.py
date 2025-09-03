# market/engine.py
import pandas as pd
import numpy as np
import random
from datetime import timedelta, datetime
import pytz
import os
import ast 
from market.database import log_stock_price_history

# --- HELPER FUNCTIONS (Copied from your original file) ---

def _ensure_aware_utc(dt_object):
    """
    Ensures a datetime object is timezone-aware and converted to UTC.
    If the object is naive, it's assumed to be in US/Central.
    """
    if dt_object.tzinfo is None:
        central_tz = pytz.timezone('US/Central')
        aware_dt = central_tz.localize(dt_object)
    else:
        aware_dt = dt_object
    return aware_dt.astimezone(pytz.utc)

def get_prestige_floor(prestige, random_init_factor):
    """Calculates the baseline stock value based on prestige and a random factor."""
    base = np.sqrt(prestige) + 5.7 + random_init_factor
    floor = (base ** 1.4) / 20
    return floor

def get_lagged_average(enriched_df, member_name, market_state, run_timestamp, override_hours=None):
    """
    Calculates the rolling average fan gain from a time-lagged window.
    'override_hours' forces a specific window and bypasses the market lag.
    """
    avg_hours = override_hours if override_hours is not None else 20
    
    # --- THIS IS THE FIX ---
    # We now check if an override is active. If it is, we set the lag to zero
    # by using the current timestamp. Otherwise, we use the market state lag.
    if override_hours is not None:
        # Event is active: Use the current timestamp, which sets lag_days to 0.
        end_of_window = run_timestamp
    else:
        # Normal operation: Calculate the end of the window using the market state lag.
        lag_options_str = market_state.get('lag_options', "[0]")
        try:
            lag_options = ast.literal_eval(lag_options_str)
            if not (isinstance(lag_options, list) and all(isinstance(i, int) for i in lag_options)):
                lag_options = [0]
        except (ValueError, SyntaxError):
            lag_options = [0]
            
        active_cursor = int(market_state.get('active_lag_cursor', 0))
        if active_cursor >= len(lag_options):
            active_cursor = 0
            
        lag_days = lag_options[active_cursor]
        end_of_window = run_timestamp - timedelta(days=lag_days)
    # --- END OF FIX ---

    member_df = enriched_df[enriched_df['inGameName'] == member_name].copy()
    member_df['timestamp'] = pd.to_datetime(member_df['timestamp'])
    member_df = member_df.set_index('timestamp').sort_index()
    
    historical_data = member_df[member_df.index <= end_of_window]

    if historical_data.empty:
        return 0
        
    #--- Time-Weighted Average Implementation ---
    # 1. Resample the data into 1-hour blocks, summing any gains within each block.
    resampled_data = historical_data['fanGain'].resample('1h').sum()
            
    # 2. Apply the rolling mean to this consistent, time-based data.
    rolling_avg_series = resampled_data.rolling(window=avg_hours).mean()
                    
    return rolling_avg_series.iloc[-1] if not rolling_avg_series.empty and pd.notna(rolling_avg_series.iloc[-1]) else 0 
    
def get_club_sentiment(enriched_df):
    """Calculates the club sentiment based on recent fan gain vs. 7-day average."""
    enriched_df['timestamp'] = pd.to_datetime(enriched_df['timestamp']).dt.tz_convert('US/Central')
    now = enriched_df['timestamp'].max()

    total_gain_24h = enriched_df[enriched_df['timestamp'] > now - timedelta(days=1)]['fanGain'].sum()
    total_gain_7d = enriched_df[enriched_df['timestamp'] > now - timedelta(days=7)]['fanGain'].sum()
    avg_gain_7d = total_gain_7d / 7
    
    if avg_gain_7d == 0:
        return 1.0

    sentiment = total_gain_24h / avg_gain_7d
    return np.clip(sentiment, 0.75, 1.25)

def get_player_condition(enriched_df, member_name):
    """Calculates the volatility multiplier based on fan gain standard deviation."""
    member_data = enriched_df[enriched_df['inGameName'] == member_name].tail(150)
    if len(member_data) < 20:
        return 1.0

    std_dev = member_data['fanGain'].std()
    
    min_std, max_std = 0, 50000 
    normalized_std = np.clip((std_dev - min_std) / (max_std - min_std), 0, 1)

    min_mult, max_mult = 0.85, 1.40
    return min_mult + (normalized_std * (max_mult - min_mult))

# --- REFACTORED FUNCTIONS ---

def calculate_individual_nudges(market_data_dfs, run_timestamp):
    """
    Calculates prorated nudges and returns the updated stock_prices DataFrame.
    This function no longer reads from or writes to any CSV files.
    """
    print("Calculating individual prestige nudges...")
    enriched_df = market_data_dfs['enriched_fan_log']
    market_state_df = market_data_dfs['market_state']
    # Work on a copy to avoid modifying the original DataFrame in the dictionary
    stock_prices_df = market_data_dfs['stock_prices'].copy()
    
    market_state = market_state_df.set_index('state_name')['state_value']
    last_run_series = market_state.get('last_run_timestamp')
    
    if pd.notna(last_run_series):
        last_run_timestamp = pd.to_datetime(last_run_series).tz_convert('UTC')
    else:
        event_check_series = market_state.get('last_event_check_timestamp')
        if pd.notna(event_check_series):
            last_run_timestamp = pd.to_datetime(event_check_series).tz_convert('UTC')
        else:
            print("CRITICAL: Cannot find a valid start timestamp. Nudge calculation aborted.")
            return stock_prices_df

    current_timestamp_utc = _ensure_aware_utc(run_timestamp)
    one_day_ago = current_timestamp_utc - timedelta(days=1)
    
    perf_window_df = enriched_df[
        (enriched_df['timestamp'] > one_day_ago) &
        (enriched_df['timestamp'] <= current_timestamp_utc)
    ].copy()

    if perf_window_df.empty:
        print("No fan gain data in the last 24 hours. Skipping nudges.")
        return stock_prices_df

    daily_fan_gains = perf_window_df.groupby('inGameName')['fanGain'].sum().reset_index()
    daily_fan_gains['rank'] = daily_fan_gains['fanGain'].rank(method='first', ascending=False)
    
    def assign_nudge(rank):
        if 1 <= rank <= 3: return 0.5
        if 4 <= rank <= 6: return 0.4
        if 7 <= rank <= 9: return 0.3
        if 10 <= rank <= 12: return 0.2
        if 13 <= rank <= 15: return 0.1
        if 16 <= rank <= 18: return -0.1
        if 19 <= rank <= 21: return -0.2
        if 22 <= rank <= 24: return -0.3
        if 25 <= rank <= 27: return -0.4
        return -0.5
    daily_fan_gains['base_nudge'] = daily_fan_gains['rank'].apply(assign_nudge)

    time_passed_hrs = (current_timestamp_utc - last_run_timestamp).total_seconds() / 3600
    if time_passed_hrs <= 0: return stock_prices_df
    
    proration_factor = time_passed_hrs / 24.0
    daily_fan_gains['prorated_nudge'] = daily_fan_gains['base_nudge'] * proration_factor

    # Merge nudges into the stock_prices_df and update the nudge_bonus
    merged_df = pd.merge(stock_prices_df, daily_fan_gains[['inGameName', 'prorated_nudge']], on='inGameName', how='left')
    merged_df['prorated_nudge'] = merged_df['prorated_nudge'].fillna(0)
    merged_df['nudge_bonus'] = merged_df['nudge_bonus'] + merged_df['prorated_nudge']
    
    print("Prestige nudges calculated and applied.")
    return merged_df.drop(columns=['prorated_nudge'])


def update_all_stock_prices(enriched_df, market_data_dfs, run_timestamp):
    """
    The main pricing engine. Calculates new prices using data from the database.
    """
    # --- 1. SETUP ---
    stock_prices_df = market_data_dfs['stock_prices'].copy()
    market_state_df = market_data_dfs['market_state']
    market_state = market_state_df.set_index('state_name')['state_value']
    portfolios_df = market_data_dfs['portfolios']
    
    active_event_name = str(market_state.get('active_event', 'None'))
    
    init_factor_map = stock_prices_df.set_index('inGameName')['init_factor'].to_dict()
    nudge_bonus_map = stock_prices_df.set_index('inGameName')['nudge_bonus'].to_dict()
    
    if active_event_name not in ['None', 'nan']:
        print(f"EVENT ACTIVE: Applying '{active_event_name}' modifiers.")
    
    club_sentiment = get_club_sentiment(enriched_df)
    market_state['club_sentiment'] = club_sentiment

    updated_prices = []
    
    # --- 2. CALCULATION LOOP ---
    for _, member_latest_data in enriched_df.groupby('inGameName').tail(1).iterrows():
        name = member_latest_data['inGameName']
        
        
        random_factor = init_factor_map.get(name)
        nudge_bonus = nudge_bonus_map.get(name, 0)
        
        if random_factor is None: continue # Skip if member not in stock table

        total_shares_outstanding = portfolios_df[portfolios_df['stock_inGameName'] == name]['shares_owned'].sum()
        price_impact_multiplier = (1 + (total_shares_outstanding * 0.00002)) ** 1.2
        
        prestige = member_latest_data['lifetimePrestige']
        prestige_floor = get_prestige_floor(prestige, random_factor)
        nudged_floor = prestige_floor + nudge_bonus 
        
        if active_event_name == "The Grand Derby":
            # During the Derby, force a 14-hour rolling average with NO lag.
            lagged_avg_gain = get_lagged_average(enriched_df, name, market_state, run_timestamp, override_hours=14)
        else:
            # Normal operation
            lagged_avg_gain = get_lagged_average(enriched_df, name, market_state, run_timestamp) 
        
        stochastic_jitter = np.random.normal(1.0, 0.08)
        
        performance_value = (lagged_avg_gain / 8757) * club_sentiment * stochastic_jitter
        core_value = nudged_floor + performance_value
        
        player_condition = get_player_condition(enriched_df, name)
        
        final_price = core_value * player_condition * price_impact_multiplier
        final_price = max(final_price, 0.01)
        
        updated_prices.append({'inGameName': name, 'current_price': final_price})

    # --- 3. PREPARE & LOG ---
    new_prices_df = pd.DataFrame(updated_prices)
    
    final_stocks_df = pd.merge(stock_prices_df.drop(columns=['current_price']), new_prices_df, on='inGameName', how='left')
    final_stocks_df['current_price'] = final_stocks_df['current_price'].fillna(0.01)
    
    print("Baggins Index: Prices updated, logging to database history.")
    log_stock_price_history(final_stocks_df, run_timestamp)
    
    return final_stocks_df, market_state.to_frame(name='state_value').reset_index()