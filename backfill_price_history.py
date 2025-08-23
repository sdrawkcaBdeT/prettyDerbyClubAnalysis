import pandas as pd
import numpy as np
import os
# Import all necessary functions from your existing engine
from market.engine import get_prestige_floor, get_lagged_average, get_club_sentiment, get_player_condition

def backfill_price_history():
    """
    Calculates and populates the stock_price_history.csv with authentic historical
    prices by running the full pricing engine at each historical timestamp.
    This is a one-time script.
    """
    print("Starting historical price backfill with full engine logic...")

    # --- 1. Load Prerequisite Files ---
    try:
        enriched_df = pd.read_csv('enriched_fan_log.csv')
        init_df = pd.read_csv('market/member_initialization.csv')
    except FileNotFoundError as e:
        print(f"ERROR: Prerequisite file not found: {e.filename}")
        print("Please ensure enriched_fan_log.csv exists and you have run initialize_market.py once.")
        return

    # --- 2. Prepare Data ---
    enriched_df['timestamp'] = pd.to_datetime(enriched_df['timestamp'])
    
    init_factor_map = init_df.set_index('inGameName')['random_init_factor'].to_dict()
    member_names = init_df['inGameName'].unique()

    # --- 3. Get Unique Timestamps ---
    unique_timestamps = sorted(enriched_df['timestamp'].unique())[-96:]
    print(f"Found {len(unique_timestamps)} unique timestamps to process.")

    price_history_records = []

    # --- 4. Loop Through History and Calculate Prices ---
    for i, ts in enumerate(unique_timestamps):
        # Create a snapshot of all data available UP TO the current historical timestamp
        historical_snapshot_df = enriched_df[enriched_df['timestamp'] <= ts].copy()
        
        # Calculate historical sentiment based on the data available at that time
        # Note: get_club_sentiment is designed to work on the full df and finds the max time itself
        historical_sentiment = get_club_sentiment(historical_snapshot_df)

        for name in member_names:
            # Get the member's most recent stats *within this historical snapshot*
            member_latest_data_series = historical_snapshot_df[historical_snapshot_df['inGameName'] == name]
            if member_latest_data_series.empty:
                continue
            member_latest_data = member_latest_data_series.iloc[-1]
            
            random_factor = init_factor_map.get(name)
            if random_factor is None:
                continue

            prestige = member_latest_data['lifetimePrestige']
            
            # --- FULL ENGINE CALCULATION ---
            prestige_floor = get_prestige_floor(prestige, random_factor)
            
            # Pass the historical snapshot to the engine functions
            lagged_avg_gain = get_lagged_average(historical_snapshot_df, name, None) # market_state not needed here
            stochastic_jitter = np.random.normal(1.0, 0.08) # New jitter for each step

            performance_value = (lagged_avg_gain / 8757) * historical_sentiment * stochastic_jitter
            core_value = prestige_floor + performance_value
            
            player_condition = get_player_condition(historical_snapshot_df, name)
            
            final_price = core_value * player_condition
            final_price = max(final_price, 0.01)

            price_history_records.append({
                'timestamp': ts,
                'inGameName': name,
                'price': final_price
            })
        
        if (i + 1) % 100 == 0 or (i + 1) == len(unique_timestamps):
            print(f"Processed {i + 1}/{len(unique_timestamps)} timestamps...")

    # --- 5. Save the New History File ---
    if not price_history_records:
        print("No price history records were generated.")
        return

    history_df = pd.DataFrame(price_history_records)
    history_filepath = 'market/stock_price_history.csv'
    history_df.to_csv(history_filepath, index=False)
    
    print("\n--- Historical Price Backfill Complete ---")
    print(f"Successfully generated and saved {len(history_df)} records to {history_filepath}")

if __name__ == '__main__':
    backfill_price_history()