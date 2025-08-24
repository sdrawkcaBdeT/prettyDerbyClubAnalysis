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
            'shop_upgrades': pd.read_csv(f'{market_dir}/shop_upgrades.csv', dtype={'discord_id': str}),
            'market_state': pd.read_csv(f'{market_dir}/market_state.csv'),
            'member_initialization': pd.read_csv(f'{market_dir}/member_initialization.csv'),
            'stock_prices': pd.read_csv(f'{market_dir}/stock_prices.csv'),
            'shop_upgrades': pd.read_csv(f'{market_dir}/shop_upgrades.csv', dtype={'discord_id': str}),
            'market_events': pd.read_csv(f'{market_dir}/market_events.csv')
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
    # Assuming 'investor_discord_id' corresponds to a user who owns 'stock_inGameName'
    # This part requires a mapping from inGameName to discord_id, which is in crew_coins
    
    # For now, we'll just sum all shares owned by others. This can be refined if needed.
    shares_owned_by_others = portfolios_df[
        portfolios_df['stock_inGameName'] == member_name
    ]['shares_owned'].sum() # Simple sum for now
    
    # Formula: Hype Bonus = 1 + (0.0005 * Total Shares Owned by Others)
    hype_bonus = 1 + (0.0005 * shares_owned_by_others)
    return hype_bonus

def process_cc_earnings(enriched_df, market_data_dfs, run_timestamp):
    crew_coins_df = market_data_dfs['crew_coins'].copy()
    portfolios_df = market_data_dfs['portfolios']
    shop_upgrades_df = market_data_dfs['shop_upgrades']
    market_state = market_data_dfs['market_state'].set_index('state_name')['value']

    # --- NEW: Event Handling for Earnings ---
    active_event_name = str(market_state.get('active_event', 'None'))
    performance_yield_modifier = 1.0
    if active_event_name == "Headwind on the Back Stretch":
        print("EVENT ACTIVE: Applying 'Headwind on the Back Stretch' modifier.")
        performance_yield_modifier = 0.5 # Halve performance earnings

    latest_data = enriched_df.sort_values('timestamp').groupby('inGameName').tail(1)
    balance_history_records = []
    dividend_payouts = {}
    
    crew_coins_df.set_index('inGameName', inplace=True)
    
    for _, member in latest_data.iterrows():
        inGameName = member['inGameName']
        if inGameName not in crew_coins_df.index: continue
        discord_id = crew_coins_df.loc[inGameName, 'discord_id']

        perf_prestige = member.get('performancePrestigePoints', 0)
        tenure_prestige = member.get('tenurePrestigePoints', 0)
        
        # Increased performance multiplier from 1.25 to 1.75
        perf_multiplier = get_upgrade_value(shop_upgrades_df, discord_id, "Study Race Tapes", 1.75, 0.15)
        
        # Increased flat bonus from 0 to 2
        perf_flat_bonus = get_upgrade_value(shop_upgrades_df, discord_id, "Perfect the Starting Gate", 2, 3)
        
        # Increased tenure multiplier from 1.50 to 2.0
        tenure_multiplier = get_upgrade_value(shop_upgrades_df, discord_id, "Build Club Morale", 2.0, 0.2)
        
        performance_yield = (perf_prestige + perf_flat_bonus) * perf_multiplier
        # Apply the event modifier
        performance_yield *= performance_yield_modifier
        
        tenure_yield = tenure_prestige * tenure_multiplier
        base_cc_earned = performance_yield + tenure_yield
        
        # --- FIX: Prevent negative base earnings from generating negative dividends ---
        base_cc_earned = max(0, base_cc_earned)
        
        hype_bonus_multiplier = calculate_hype_bonus(portfolios_df, inGameName)
        hype_bonus_yield = base_cc_earned * (hype_bonus_multiplier - 1)
        total_personal_cc_earned = base_cc_earned + hype_bonus_yield
        
        crew_coins_df.loc[inGameName, 'balance'] += round(total_personal_cc_earned)

        shareholders = portfolios_df[portfolios_df['stock_inGameName'] == inGameName]
        if not shareholders.empty:
            largest_shareholder = shareholders.loc[shareholders['shares_owned'].idxmax()]
            sponsor_discord_id = str(largest_shareholder['investor_discord_id'])
            # Increased the sponsorship dividend from 10% to 25%
            sponsorship_dividend = 0.25 * total_personal_cc_earned
            dividend_payouts[sponsor_discord_id] = dividend_payouts.get(sponsor_discord_id, 0) + sponsorship_dividend
            
        # Format the timestamp to a clean string without microseconds
        timestamp_str = run_timestamp.strftime('%Y-%m-%d %H:%M:%S%z')
        # Manually insert the colon for full consistency
        formatted_timestamp = f"{timestamp_str[:-2]}:{timestamp_str[-2:]}"

        balance_history_records.append({
            'timestamp': formatted_timestamp, 'inGameName': inGameName, 'discord_id': discord_id,
            'performance_yield': performance_yield, 'tenure_yield': tenure_yield,
            'hype_bonus_yield': hype_bonus_yield, 'sponsorship_dividend_received': 0,
            'total_period_earnings': total_personal_cc_earned, 'new_balance': 0
        })

    id_to_name_map = {v: k for k, v in crew_coins_df['discord_id'].to_dict().items() if pd.notna(v)}

    for sponsor_id, dividend_amount in dividend_payouts.items():
        if sponsor_id in id_to_name_map:
            target_name = id_to_name_map[sponsor_id]
            crew_coins_df.loc[target_name, 'balance'] += round(dividend_amount)

    for record in balance_history_records:
        inGameName = record['inGameName']
        final_balance = crew_coins_df.loc[inGameName, 'balance']
        dividend_received = dividend_payouts.get(str(record['discord_id']), 0)
        record['sponsorship_dividend_received'] = dividend_received
        record['total_period_earnings'] += dividend_received
        record['new_balance'] = final_balance

    print("CC earnings processed and history records created.")
    history_df = pd.DataFrame(balance_history_records)
    history_df.to_csv('market/balance_history.csv', mode='a', header=not os.path.exists('market/balance_history.csv'), index=False, float_format='%.2f')
    
    return crew_coins_df.reset_index()