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
        'market_state.csv': ['state_name', 'value', 'last_event_check_timestamp'],
        'crew_coins.csv': ['discord_id', 'inGameName', 'balance'],
        'stock_prices.csv': ['inGameName', 'current_price', '24hr_change'],
        'portfolios.csv': ['investor_discord_id', 'stock_inGameName', 'shares_owned'],
        'shop_upgrades.csv': ['discord_id', 'upgrade_name', 'tier'],
        'market_events.csv': ['event_name', 'description', 'duration_hours', 'effect_type', 'effect_value'],
        'member_initialization.csv': ['inGameName', 'random_init_factor', 'status', 'ticker'],
        # --- History & Log Files ---
        'stock_price_history.csv': ['timestamp', 'inGameName', 'price'],
        'balance_history.csv': [
            'timestamp', 'inGameName', 'discord_id', 'performance_yield', 'tenure_yield',
            'hype_bonus_yield', 'sponsorship_dividend_received', 'total_period_earnings', 'new_balance'
        ],
        'universal_transaction_log.csv': [
            'timestamp', 'actor_id', 'transaction_type', 'target_id', 'item_name',
            'item_quantity', 'cc_amount', 'fee_paid'
        ],
        'market_event_log.csv': ['timestamp', 'event_name', 'event_type', 'details']
    }
    
    
    random.seed(5857)
    
    if not os.path.exists(MARKET_DIR):
        os.makedirs(MARKET_DIR)
        print(f"Created directory: {MARKET_DIR}")

    for filename, headers in FILES_TO_CREATE.items():
        filepath = os.path.join(MARKET_DIR, filename)
        if not os.path.exists(filepath):
            pd.DataFrame(columns=headers).to_csv(filepath, index=False)
            print(f"Created missing file: {filepath}")

    # --- Populate market_state.csv ---
    market_state_df = pd.read_csv(os.path.join(MARKET_DIR, 'market_state.csv'))
    if market_state_df.empty:
        market_state_data = {
            'state_name': ['lag_index', 'active_event', 'event_end_time', 'club_sentiment', 'last_event_check_timestamp'],
            'value': [1, 'None', 'None', 1.0, None]
        }
        pd.DataFrame(market_state_data).to_csv(os.path.join(MARKET_DIR, 'market_state.csv'), index=False)
        print("Populated market_state.csv.")

    try:
        fan_log_df = pd.read_csv('enriched_fan_log.csv')
        registrations_df = pd.read_csv('user_registrations.csv')
    except FileNotFoundError as e:
        print(f"ERROR: Could not find required source file: {e.filename}")
        return

    # --- Populate other initial files ---
    fan_log_df = pd.read_csv('enriched_fan_log.csv')
    fan_log_df.rename(columns={'inGameName': 'inGameName', 'lifetimePrestige': 'total_prestige'}, inplace=True)
    registrations_df = pd.read_csv('user_registrations.csv')
    
    latest_entries = fan_log_df.sort_values('timestamp').groupby('inGameName').tail(1)
    member_data = pd.merge(latest_entries, registrations_df, on='inGameName', how='left')

    crew_coins_records = []
    member_init_records = []

    for _, row in member_data.iterrows():
        inGameName = row['inGameName']
        discord_id = row['discord_id']
        total_prestige = row['total_prestige']

        starting_cc = (total_prestige ** 1.03) * 1.57 + 7571
        crew_coins_records.append({
            'discord_id': discord_id,
            'inGameName': inGameName,
            'balance': int(round(starting_cc))
        })

        random_init_factor = random.randint(28, 42)
        member_init_records.append({
            'inGameName': inGameName,
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

if __name__ == '__main__':
    initialize_market()