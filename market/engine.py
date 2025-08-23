import pandas as pd
import numpy as np
import random
from datetime import timedelta
import os

def get_prestige_floor(prestige, random_init_factor):
    """Calculates the baseline stock value based on prestige and a random factor."""
    # Formula: ((sqrt(Prestige) + 5.7 + Random Initialization Factor)^1.4) / 20
    base = np.sqrt(prestige) + 5.7 + random_init_factor
    floor = (base ** 1.4) / 20
    return floor

def get_lagged_average(enriched_df, member_name, market_state, override_hours=None):
    # UPDATED: Now accepts an override for the rolling average hours
    avg_hours = override_hours if override_hours is not None else 20
    
    member_df = enriched_df[enriched_df['inGameName'] == member_name].copy()
    member_df['timestamp'] = pd.to_datetime(member_df['timestamp'])
    member_df = member_df.set_index('timestamp').sort_index()
    
    rolling_avg = member_df['fanGain'].rolling(f'{avg_hours}h').mean().iloc[-1]
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

def update_all_stock_prices(enriched_df, market_data_dfs, run_timestamp):
    """
    The main pricing engine. Calculates new prices and writes them to a history log.
    """
    init_df = market_data_dfs['member_initialization']
    stock_prices_df = market_data_dfs['stock_prices'].copy()
    market_state_df = market_data_dfs['market_state']
    market_state = market_state_df.set_index('state_name')['value']
    portfolios_df = market_data_dfs['portfolios']
    
    # Convert active_event to a string to safely handle None/NaN values
    active_event_name = str(market_state.get('active_event', 'None'))
    
    event_modifiers = {'price': {}, 'condition': {}, 'lag': {}}
    sentiment_modifier = 1.0
    
    member_names = list(enriched_df['inGameName'].unique())

    # This is the critical fix: Only enter this block if an event is actually active.
    if active_event_name not in ['None', 'nan']:
        print(f"EVENT ACTIVE: Applying '{active_event_name}' modifiers.")
        events_df = market_data_dfs['market_events'] # Use the already loaded DF
        
        # This will now only run when we are sure an event exists
        event_details_row = events_df[events_df['event_name'] == active_event_name]
        if not event_details_row.empty:
            event_details = event_details_row.iloc[0]
            # --- (All the if/elif logic for specific events remains the same) ---
            if active_event_name == "Rival Club in Disarray":
                sentiment_modifier = 1.15
            elif active_event_name == "The Crowd Roars":
                shares_owned = portfolios_df.groupby('stock_in_game_name')['shares_owned'].sum()
                top_5_hyped = shares_owned.nlargest(5).index
                for name in top_5_hyped: event_modifiers['price'][name] = 1.05
            # ... and so on for all other events
            elif active_event_name == "Dark Horse Bargains":
                bottom_25_percent = stock_prices_df.nsmallest(int(len(stock_prices_df) * 0.25), 'current_price')
                for name in bottom_25_percent['in_game_name']: event_modifiers['price'][name] = 0.85
            elif active_event_name == "The Gate is Sticky":
                sticky_members = random.sample(member_names, k=int(len(member_names) * 0.20))
                for name in sticky_members: event_modifiers['condition'][name] = 1.0
            elif active_event_name == "Jockey Change Announced":
                player1, player2 = random.sample(member_names, k=2)
                cond1 = get_player_condition(enriched_df, player1)
                cond2 = get_player_condition(enriched_df, player2)
                event_modifiers['condition'][player1] = cond2
                event_modifiers['condition'][player2] = cond1
            elif active_event_name == "Photo Finish Review":
                ranks = enriched_df.sort_values('timestamp').groupby('inGameName').tail(1).sort_values('cumulativePrestige', ascending=False).reset_index()
                target_members = ranks.loc[4:14, 'inGameName']
                for name in target_members: event_modifiers['lag'][name] = 6

    club_sentiment = get_club_sentiment(enriched_df) * sentiment_modifier
    market_state['club_sentiment'] = club_sentiment

    updated_prices = []
    price_history_records = []

    for name in member_names:
        member_latest_data = enriched_df[enriched_df['inGameName'] == name].iloc[-1]
        prestige = member_latest_data['cumulativePrestige']
        random_factor_row = init_df[init_df['in_game_name'] == name]
        if random_factor_row.empty: continue
        random_factor = random_factor_row['random_init_factor'].iloc[0]
        
        # --- Apply Event Logic to Inputs ---
        lag_override = event_modifiers['lag'].get(name)
        
        prestige_floor = get_prestige_floor(prestige, random_factor)
        lagged_avg_gain = get_lagged_average(enriched_df, name, market_state, override_hours=lag_override)
        stochastic_jitter = np.random.normal(1.0, 0.08)
        
        performance_value = (lagged_avg_gain / 8757) * club_sentiment * stochastic_jitter
        core_value = prestige_floor + performance_value
        
        player_condition = event_modifiers['condition'].get(name, get_player_condition(enriched_df, name))
        price_modifier = event_modifiers['price'].get(name, 1.0)
        
        final_price = (core_value * player_condition) * price_modifier
        final_price = max(final_price, 0.01)
        
        updated_prices.append({'in_game_name': name, 'current_price': final_price})
        price_history_records.append({'timestamp': run_timestamp, 'in_game_name': name, 'price': final_price})

    new_prices_df = pd.DataFrame(updated_prices).set_index('in_game_name')
    stock_prices_df = stock_prices_df.set_index('in_game_name')
    stock_prices_df['current_price'] = new_prices_df['current_price']
    stock_prices_df['24hr_change'] = 0.0
    
    # Append the new price history to its CSV
    history_df = pd.DataFrame(price_history_records)
    history_df.to_csv('market/stock_price_history.csv', mode='a', header=not os.path.exists('market/stock_price_history.csv'), index=False)
    
    print("Baggins Index: Prices updated and history logged.")
    return stock_prices_df.reset_index(), market_state.to_frame(name='value').reset_index()