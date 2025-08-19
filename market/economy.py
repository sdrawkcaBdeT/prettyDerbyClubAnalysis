import pandas as pd
import os

def load_market_data(market_dir='market'):
    """Loads all necessary market CSVs into a dictionary of DataFrames."""
    try:
        # Specify the dtype for 'discord_id' as string to prevent it from
        # being converted to a float and saved in scientific notation.
        return {
            'crew_coins': pd.read_csv(f'{market_dir}/crew_coins.csv', dtype={'discord_id': str}),
            'portfolios': pd.read_csv(f'{market_dir}/portfolios.csv'),
            'shop_upgrades': pd.read_csv(f'{market_dir}/shop_upgrades.csv', dtype={'discord_id': str})
        }
    except FileNotFoundError as e:
        print(f"ERROR: Could not find required market file: {e.filename}")
        return None

def get_upgrade_value(upgrades_df, discord_id, upgrade_name, base_value, bonus_per_tier):
    """Calculates the value of a stat after applying tiered upgrades."""
    if upgrades_df.empty or pd.isna(discord_id):
        return base_value
        
    # Ensure discord_id in upgrades_df is the same type for comparison
    upgrades_df['discord_id'] = upgrades_df['discord_id'].astype(str)
    discord_id_str = str(int(discord_id)) # Convert float discord_id to int then string
    
    member_upgrade = upgrades_df[
        (upgrades_df['discord_id'] == discord_id_str) &
        (upgrades_df['upgrade_name'] == upgrade_name)
    ]
    
    if not member_upgrade.empty:
        tier = member_upgrade['tier'].iloc[0]
        return base_value + (tier * bonus_per_tier)
    return base_value

def calculate_hype_bonus(portfolios_df, member_name):
    """Calculates the Hype Bonus based on shares owned by others."""
    # Sum shares where the stock name matches, but exclude the owner investing in themselves
    # Assuming 'investor_discord_id' corresponds to a user who owns 'stock_in_game_name'
    # This part requires a mapping from in_game_name to discord_id, which is in crew_coins
    
    # For now, we'll just sum all shares owned by others. This can be refined if needed.
    shares_owned_by_others = portfolios_df[
        portfolios_df['stock_in_game_name'] == member_name
    ]['shares_owned'].sum() # Simple sum for now
    
    # Formula: Hype Bonus = 1 + (0.0005 * Total Shares Owned by Others)
    hype_bonus = 1 + (0.0005 * shares_owned_by_others)
    return hype_bonus

def process_cc_earnings(enriched_df, market_data_dfs, run_timestamp):
    """
    Calculates and applies continuous CC earnings and writes a detailed history log.
    Balances are handled as floats during calculation and rounded to integers for storage.
    """
    crew_coins_df = market_data_dfs['crew_coins'].copy()
    portfolios_df = market_data_dfs['portfolios']
    shop_upgrades_df = market_data_dfs['shop_upgrades']

    # --- Data Type Correction ---
    # Convert balance to a numeric type to allow float additions.
    # We will round it back to an integer before saving.
    crew_coins_df['balance'] = pd.to_numeric(crew_coins_df['balance'], errors='coerce').fillna(0)

    # Set in_game_name as index for efficient and reliable lookups
    crew_coins_df.set_index('in_game_name', inplace=True)

    latest_data = enriched_df.sort_values('timestamp').groupby('inGameName').tail(1)
    
    balance_history_records = []
    dividend_payouts = {}

    # --- First Pass: Calculate earnings ---
    for _, member in latest_data.iterrows():
        in_game_name = member['inGameName']
        if in_game_name not in crew_coins_df.index:
            continue
        
        discord_id = crew_coins_df.loc[in_game_name, 'discord_id']

        # ... (all the yield and multiplier calculations remain the same) ...
        perf_prestige = member.get('performancePrestigePoints', 0)
        tenure_prestige = member.get('tenurePrestigePoints', 0)
        perf_multiplier = get_upgrade_value(shop_upgrades_df, discord_id, "Study Race Tapes", 1.25, 0.05)
        perf_flat_bonus = get_upgrade_value(shop_upgrades_df, discord_id, "Perfect the Starting Gate", 0, 4)
        tenure_multiplier = get_upgrade_value(shop_upgrades_df, discord_id, "Build Club Morale", 1.50, 0.1)
        performance_yield = (perf_prestige + perf_flat_bonus) * perf_multiplier
        tenure_yield = tenure_prestige * tenure_multiplier
        base_cc_earned = performance_yield + tenure_yield
        hype_bonus_multiplier = calculate_hype_bonus(portfolios_df, in_game_name)
        hype_bonus_yield = base_cc_earned * (hype_bonus_multiplier - 1)
        total_personal_cc_earned = base_cc_earned + hype_bonus_yield
        
        # --- Apply Rounded Earnings ---
        # Round the final calculated earnings to the nearest whole number
        rounded_earnings = round(total_personal_cc_earned)
        crew_coins_df.loc[in_game_name, 'balance'] += rounded_earnings

        # Queue dividends (these can remain floats until the final payout)
        shareholders = portfolios_df[portfolios_df['stock_in_game_name'] == in_game_name]
        if not shareholders.empty:
            largest_shareholder = shareholders.loc[shareholders['shares_owned'].idxmax()]
            sponsor_discord_id = str(largest_shareholder['investor_discord_id']) # ensure string
            sponsorship_dividend = 0.10 * total_personal_cc_earned
            dividend_payouts[sponsor_discord_id] = dividend_payouts.get(sponsor_discord_id, 0) + sponsorship_dividend
            
        balance_history_records.append({
            'timestamp': run_timestamp, 'in_game_name': in_game_name, 'discord_id': discord_id,
            'performance_yield': performance_yield, 'tenure_yield': tenure_yield,
            'hype_bonus_yield': hype_bonus_yield, 'sponsorship_dividend_received': 0,
            'total_period_earnings': total_personal_cc_earned, 'new_balance': 0
        })

    # --- Second Pass: Apply dividends and finalize ---
    # Temporarily create a discord_id -> in_game_name map for applying dividends
    id_to_name_map = crew_coins_df['discord_id'].to_dict()
    # Invert the map for our use case: {discord_id: in_game_name}
    id_to_name_map = {v: k for k, v in id_to_name_map.items() if pd.notna(v)}

    for sponsor_id, dividend_amount in dividend_payouts.items():
        if sponsor_id in id_to_name_map:
            target_name = id_to_name_map[sponsor_id]
            # Round the dividend before adding it to the balance
            rounded_dividend = round(dividend_amount)
            crew_coins_df.loc[target_name, 'balance'] += rounded_dividend

    # Finalize history records with the true new balance
    for record in balance_history_records:
        in_game_name = record['in_game_name']
        final_balance = crew_coins_df.loc[in_game_name, 'balance']
        
        # Add any dividend they might have received to their history record
        dividend_received = dividend_payouts.get(record['discord_id'], 0)
        record['sponsorship_dividend_received'] = dividend_received
        record['total_period_earnings'] += dividend_received
        record['new_balance'] = final_balance

    print("CC earnings processed and history records created.")
    
    history_df = pd.DataFrame(balance_history_records)
    history_df.to_csv('market/balance_history.csv', mode='a', header=not os.path.exists('market/balance_history.csv'), index=False, float_format='%.2f')
    
    # Return the updated DataFrame, resetting the index for saving
    return crew_coins_df.reset_index()