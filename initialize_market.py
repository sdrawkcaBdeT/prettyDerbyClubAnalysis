import os
import pandas as pd
import numpy as np
import random

def initialize_market():
    """
    Creates and populates the initial market data files for the Fan Exchange.
    This script is designed to be run only once.
    """
    MARKET_DIR = 'market'
    FILES_TO_CREATE = {
        # --- State Files (Current Status) ---
        'market_state.csv': ['state_name', 'value'],
        'crew_coins.csv': ['discord_id', 'in_game_name', 'balance'],
        'stock_prices.csv': ['in_game_name', 'current_price', '24hr_change'],
        'portfolios.csv': ['investor_discord_id', 'stock_in_game_name', 'shares_owned'],
        'shop_upgrades.csv': ['discord_id', 'upgrade_name', 'tier'],
        'market_events.csv': ['event_name', 'description', 'duration_hours', 'effect_type', 'effect_value'],
        'member_initialization.csv': ['in_game_name', 'random_init_factor', 'status', 'ticker'],
        # --- History & Log Files ---
        'stock_price_history.csv': ['timestamp', 'in_game_name', 'price'],
        'balance_history.csv': [
            'timestamp', 'in_game_name', 'discord_id', 'performance_yield', 'tenure_yield',
            'hype_bonus_yield', 'sponsorship_dividend_received', 'total_period_earnings', 'new_balance'
        ],
        'universal_transaction_log.csv': [
            'timestamp', 'actor_id', 'transaction_type', 'target_id', 'item_name',
            'item_quantity', 'cc_amount', 'fee_paid'
        ],
        'market_event_log.csv': ['timestamp', 'event_name', 'event_type', 'details']
    }
    
    
    random.seed(57)
    
    if not os.path.exists(MARKET_DIR):
        os.makedirs(MARKET_DIR)
        print(f"Created directory: {MARKET_DIR}")

    for filename in FILES_TO_CREATE.keys():
        filepath = os.path.join(MARKET_DIR, filename)
        if os.path.exists(filepath):
            print(f"ERROR: Market file '{filepath}' already exists. Halting to prevent overwriting data.")
            return

    print("No existing market files found. Proceeding with initialization.")

    for filename, headers in FILES_TO_CREATE.items():
        filepath = os.path.join(MARKET_DIR, filename)
        pd.DataFrame(columns=headers).to_csv(filepath, index=False)
    print("Created 7 new market CSV files with correct headers.")

    market_state_data = {
    'state_name': ['lag_index', 'active_event', 'event_end_time', 'club_sentiment', 'last_event_check_timestamp'],
    'value': [1, 'None', 'None', 1.0, None]
    }
    market_state_df = pd.DataFrame(market_state_data)
    market_state_df.to_csv(os.path.join(MARKET_DIR, 'market_state.csv'), index=False)
    print("Populated market_state.csv with initial values.")

    try:
        fan_log_df = pd.read_csv('enriched_fan_log.csv')
        registrations_df = pd.read_csv('user_registrations.csv')
    except FileNotFoundError as e:
        print(f"ERROR: Could not find required source file: {e.filename}")
        return

    # --- FIX APPLIED HERE ---
    # Rename columns from the CSV to match what the script expects.
    fan_log_df.rename(columns={
        'inGameName': 'in_game_name',
        'cumulativePrestige': 'total_prestige'
    }, inplace=True)
    # --- END FIX ---

    latest_entries = fan_log_df.sort_values('timestamp').groupby('in_game_name').tail(1)
    
    # Ensure in_game_name in registrations_df is string type to avoid merge errors
    registrations_df['in_game_name'] = registrations_df['in_game_name'].astype(str)
    
    member_data = pd.merge(latest_entries, registrations_df, on='in_game_name', how='left')

    crew_coins_records = []
    member_init_records = []

    for _, row in member_data.iterrows():
        in_game_name = row['in_game_name']
        discord_id = row['discord_id']
        total_prestige = row['total_prestige']

        starting_cc = (total_prestige ** 1.03) * 1.57 + 1557
        crew_coins_records.append({
            'discord_id': discord_id,
            'in_game_name': in_game_name,
            'balance': int(round(starting_cc))
        })

        random_init_factor = random.randint(15, 45)
        member_init_records.append({
            'in_game_name': in_game_name,
            'random_init_factor': random_init_factor,
            'status': 'active',
            'ticker': None
        })

    crew_coins_df = pd.DataFrame(crew_coins_records)
    crew_coins_df['discord_id'] = crew_coins_df['discord_id'].apply(lambda x: str(int(x)) if pd.notna(x) else '')
    crew_coins_df.to_csv(os.path.join(MARKET_DIR, 'crew_coins.csv'), index=False)
    print("Populated crew_coins.csv with initial balances for all members.")

    member_init_df = pd.DataFrame(member_init_records)
    member_init_df.to_csv(os.path.join(MARKET_DIR, 'member_initialization.csv'), index=False)
    print("Populated member_initialization.csv with permanent random factors.")

    print("\n--- Market Initialization Complete ---")
    print("The script should now run without errors.")

if __name__ == '__main__':
    initialize_market()