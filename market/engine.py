import pandas as pd
import numpy as np
import random
from datetime import timedelta

def get_prestige_floor(prestige, random_init_factor):
    """Calculates the baseline stock value based on prestige and a random factor."""
    # Formula: ((sqrt(Prestige) + 5.7 + Random Initialization Factor)^1.4) / 20
    base = np.sqrt(prestige) + 5.7 + random_init_factor
    floor = (base ** 1.4) / 20
    return floor

def get_lagged_average(enriched_df, member_name, market_state):
    """Calculates the lagged 9-hour average fan gain."""
    # The lag is complex and stateful, managed by the scheduler.
    # For this commit, we'll implement the 9-hour average without the random lag shift.
    # The lag shift will be implemented with the events system in a later commit.
    member_df = enriched_df[enriched_df['inGameName'] == member_name].copy()
    member_df = member_df.set_index('timestamp').sort_index()
    
    # Calculate 9-hour rolling average of fanGain
    # The 'fanGain' column is from the enriched_fan_log.csv
    rolling_avg = member_df['fanGain'].rolling('9H').mean().iloc[-1]
    return rolling_avg if pd.notna(rolling_avg) else 0

def get_club_sentiment(enriched_df):
    """Calculates the club sentiment based on recent fan gain vs. 7-day average."""
    # Set the timezone to ensure correct timestamp comparisons
    enriched_df['timestamp'] = pd.to_datetime(enriched_df['timestamp']).dt.tz_convert('US/Central')
    now = enriched_df['timestamp'].max()

    # Total fan gain in the last 24 hours
    last_24h = enriched_df[enriched_df['timestamp'] > now - timedelta(days=1)]
    total_gain_24h = last_24h['fanGain'].sum()

    # Average daily fan gain over the last 7 days
    last_7d = enriched_df[enriched_df['timestamp'] > now - timedelta(days=7)]
    total_gain_7d = last_7d['fanGain'].sum()
    avg_gain_7d = total_gain_7d / 7
    
    if avg_gain_7d == 0:
        return 1.0 # Avoid division by zero if there's no activity

    # Sentiment is the ratio, capped for stability
    sentiment = total_gain_24h / avg_gain_7d
    return np.clip(sentiment, 0.75, 1.25) # Capping to prevent extreme swings

def get_player_condition(enriched_df, member_name):
    """Calculates the volatility multiplier based on fan gain standard deviation."""
    member_data = enriched_df[enriched_df['inGameName'] == member_name].tail(150)
    if len(member_data) < 20: # Require a minimum number of data points
        return 1.0

    std_dev = member_data['fanGain'].std()
    
    # Normalize the standard deviation to a 0-1 scale.
    # These normalization bounds (min_std, max_std) may need tuning based on real data.
    min_std, max_std = 0, 50000 
    normalized_std = (std_dev - min_std) / (max_std - min_std)
    normalized_std = np.clip(normalized_std, 0, 1)

    # Map the normalized value to the multiplier range [0.85, 1.40]
    # Low std dev (consistent) -> low multiplier
    # High std dev (volatile) -> high multiplier
    min_mult, max_mult = 0.85, 1.40
    multiplier = min_mult + (normalized_std * (max_mult - min_mult))
    return multiplier

def update_all_stock_prices(enriched_df, market_data_dfs):
    """
    The main pricing engine. Calculates the new price for every stock.
    """
    init_df = market_data_dfs['member_initialization']
    stock_prices_df = market_data_dfs['stock_prices'].copy()
    market_state = market_data_dfs['market_state']
    
    # --- Global Factors ---
    club_sentiment = get_club_sentiment(enriched_df)
    active_event_modifier = 1.0 # Placeholder for now, will be implemented in Phase 4
    
    # Update market_state with the new sentiment
    market_state.loc[market_state['state_name'] == 'club_sentiment', 'value'] = club_sentiment

    updated_prices = []
    member_names = enriched_df['inGameName'].unique()

    for name in member_names:
        member_latest_data = enriched_df[enriched_df['inGameName'] == name].iloc[-1]
        
        # --- Core Value Components ---
        prestige = member_latest_data['cumulativePrestige']
        random_factor_row = init_df[init_df['in_game_name'] == name]
        if random_factor_row.empty:
            continue
        random_factor = random_factor_row['random_init_factor'].iloc[0]
        
        prestige_floor = get_prestige_floor(prestige, random_factor)
        
        lagged_avg_gain = get_lagged_average(enriched_df, name, market_state)
        stochastic_jitter = np.random.normal(1.0, 0.08)
        
        performance_value = (lagged_avg_gain / 8757) * club_sentiment * stochastic_jitter
        core_value = prestige_floor + performance_value
        
        # --- Multipliers ---
        player_condition = get_player_condition(enriched_df, name)
        
        # --- Final Price Formula ---
        final_price = core_value * player_condition * active_event_modifier
        final_price = max(final_price, 0.01) # Ensure price doesn't go to zero or negative
        
        updated_prices.append({'in_game_name': name, 'current_price': final_price})

    # Create a DataFrame with the new prices
    new_prices_df = pd.DataFrame(updated_prices)
    new_prices_df = new_prices_df.set_index('in_game_name')

    # Update the main stock_prices_df
    stock_prices_df = stock_prices_df.set_index('in_game_name')
    stock_prices_df['current_price'] = new_prices_df['current_price']
    
    # For now, 24hr_change is a placeholder. A more robust implementation would
    # require storing historical price data.
    stock_prices_df['24hr_change'] = 0.0
    
    print("Baggins Index: All stock prices have been updated.")
    return stock_prices_df.reset_index(), market_state