import pandas as pd
import numpy as np
import random
from datetime import timedelta
import os
import ast # For safely evaluating the string representation of a list

def get_prestige_floor(prestige, random_init_factor):
    """Calculates the baseline stock value based on prestige and a random factor."""
    base = np.sqrt(prestige) + 5.7 + random_init_factor
    floor = (base ** 1.4) / 20
    return floor

def get_lagged_average(enriched_df, member_name, market_state, run_timestamp, override_hours=None):
    """
    Calculates the rolling average fan gain from a time-lagged window.
    The lag is now determined by the persistent state in market_state.csv.
    """
    avg_hours = override_hours if override_hours is not None else 20
    
    # --- NEW STATEFUL LAGGING LOGIC ---
    # 1. Get the list of possible lag days and the current cursor
    lag_options_str = market_state.get('lag_options', "[0]")
    try:
        lag_options = ast.literal_eval(lag_options_str)
        if not (isinstance(lag_options, list) and all(isinstance(i, int) for i in lag_options)):
            lag_options = [0]
    except (ValueError, SyntaxError):
        lag_options = [0]
        
    active_cursor = int(market_state.get('active_lag_cursor', 0))
    
    # Ensure cursor is within bounds
    if active_cursor >= len(lag_options):
        active_cursor = 0
        
    # 2. Determine the active lag in days
    lag_days = lag_options[active_cursor]
    
    # 3. Define the end of the historical window based on the lag
    end_of_window = run_timestamp - timedelta(days=lag_days)

    # 4. Filter the member's data to only include records *before* the end of our lagged window
    member_df = enriched_df[enriched_df['inGameName'] == member_name].copy()
    member_df['timestamp'] = pd.to_datetime(member_df['timestamp'])
    member_df = member_df.set_index('timestamp').sort_index()
    
    historical_data = member_df[member_df.index <= end_of_window]

    if historical_data.empty:
        return 0
        
    # 5. Calculate the rolling average on this historical, time-shifted data
    rolling_avg_series = historical_data['fanGain'].rolling(f'{avg_hours}h').mean()
    
    return rolling_avg_series.iloc[-1] if pd.notna(rolling_avg_series.iloc[-1]) else 0


def get_club_sentiment(enriched_df):
    """Calculates the club sentiment based on recent fan gain vs. 7-day average."""
    enriched_df['timestamp'] = pd.to_datetime(enriched_df['timestamp']).dt.tz_convert('US/Central')
    now = enriched_df['timestamp'].max()

    last_24h = enriched_df[enriched_df['timestamp'] > now - timedelta(days=1)]
    total_gain_24h = last_24h['fanGain'].sum()

    last_7d = enriched_df[enriched_df['timestamp'] > now - timedelta(days=7)]
    total_gain_7d = last_7d['fanGain'].sum()
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
    normalized_std = (std_dev - min_std) / (max_std - min_std)
    normalized_std = np.clip(normalized_std, 0, 1)

    min_mult, max_mult = 0.85, 1.40
    multiplier = min_mult + (normalized_std * (max_mult - min_mult))
    return multiplier

def update_all_stock_prices(enriched_df, market_data_dfs, run_timestamp):
    """
    The main pricing engine. Calculates new prices and writes them to a history log.
    """
    init_df = market_data_dfs['member_initialization']
    stock_prices_df = market_data_dfs['stock_prices'].copy()
    market_state_df = market_data_dfs['market_state']
    market_state = market_state_df.set_index('state_name')['value']
    portfolios_df = market_data_dfs['portfolios']
    
    active_event_name = str(market_state.get('active_event', 'None'))
    
    event_modifiers = {'price': {}, 'condition': {}, 'lag': {}}
    sentiment_modifier = 1.0
    
    member_names = list(enriched_df['inGameName'].unique())

    if active_event_name not in ['None', 'nan']:
        print(f"EVENT ACTIVE: Applying '{active_event_name}' modifiers.")
        events_df = market_data_dfs['market_events']
        
        event_details_row = events_df[events_df['event_name'] == active_event_name]
        if not event_details_row.empty:
            # Event logic remains the same
            pass

    club_sentiment = get_club_sentiment(enriched_df) * sentiment_modifier
    market_state['club_sentiment'] = club_sentiment

    updated_prices = []
    price_history_records = []

    for name in member_names:
        member_latest_data = enriched_df[enriched_df['inGameName'] == name].iloc[-1]
        
        total_shares_outstanding = portfolios_df[portfolios_df['stock_inGameName'] == name]['shares_owned'].sum()
        price_impact_multiplier = (1 + (total_shares_outstanding * 0.00002)) ** 1.2
        
        prestige = member_latest_data['lifetimePrestige']
        random_factor_row = init_df[init_df['inGameName'] == name]
        if random_factor_row.empty: continue
        random_factor = random_factor_row['random_init_factor'].iloc[0]
        
        lag_override = event_modifiers['lag'].get(name)
        
        prestige_floor = get_prestige_floor(prestige, random_factor)
        lagged_avg_gain = get_lagged_average(enriched_df, name, market_state, run_timestamp, override_hours=lag_override)
        stochastic_jitter = np.random.normal(1.0, 0.08)
        
        performance_value = (lagged_avg_gain / 8757) * club_sentiment * stochastic_jitter
        core_value = prestige_floor + performance_value
        
        player_condition = event_modifiers['condition'].get(name, get_player_condition(enriched_df, name))
        price_modifier = event_modifiers['price'].get(name, 1.0)
        
        final_price = (core_value * player_condition) * price_modifier * price_impact_multiplier
        
        final_price = (core_value * player_condition) * price_modifier
        final_price = max(final_price, 0.01)
        
        updated_prices.append({'inGameName': name, 'current_price': final_price})
        
        timestamp_str = run_timestamp.strftime('%Y-%m-%d %H:%M:%S%z')
        formatted_timestamp = f"{timestamp_str[:-2]}:{timestamp_str[-2:]}"
        
        price_history_records.append({'timestamp': formatted_timestamp, 'in_game_name': name, 'price': final_price})        

    new_prices_df = pd.DataFrame(updated_prices).set_index('inGameName')
    stock_prices_df = stock_prices_df.set_index('inGameName')
    stock_prices_df['current_price'] = new_prices_df['current_price']
    stock_prices_df['24hr_change'] = 0.0
    
    history_df = pd.DataFrame(price_history_records)
    history_df.to_csv('market/stock_price_history.csv', mode='a', header=not os.path.exists('market/stock_price_history.csv'), index=False)
    
    print("Baggins Index: Prices updated and history logged.")
    return stock_prices_df.reset_index(), market_state.to_frame(name='value').reset_index()