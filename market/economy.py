import pandas as pd

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

def process_cc_earnings(enriched_df, market_data_dfs):
    """
    Calculates and applies continuous CC earnings for all members.
    This includes Performance, Tenure, Hype Bonus, and Sponsorship Deals.
    """
    crew_coins_df = market_data_dfs['crew_coins'].copy()
    portfolios_df = market_data_dfs['portfolios']
    shop_upgrades_df = market_data_dfs['shop_upgrades']
    
    # Get the most recent data point for each member from the latest analysis run
    latest_data = enriched_df.sort_values('timestamp').groupby('inGameName').tail(1)
    
    total_earnings = []

    for _, member in latest_data.iterrows():
        in_game_name = member['inGameName']
        
        # Find corresponding entry in crew_coins
        coin_row = crew_coins_df[crew_coins_df['in_game_name'] == in_game_name]
        if coin_row.empty:
            continue
        
        discord_id = coin_row['discord_id'].iloc[0]

        # --- Base Yield Calculations ---
        perf_prestige = member.get('performancePrestigePoints', 0)
        tenure_prestige = member.get('tenurePrestigePoints', 0)
        
        # --- Apply Upgrades ---
        perf_multiplier = get_upgrade_value(shop_upgrades_df, discord_id, "Study Race Tapes", 1.25, 0.05)
        perf_flat_bonus = get_upgrade_value(shop_upgrades_df, discord_id, "Perfect the Starting Gate", 0, 4)
        tenure_multiplier = get_upgrade_value(shop_upgrades_df, discord_id, "Build Club Morale", 1.50, 0.1)
        # Note: "Garner Owner's Favor" is a daily bonus and should be handled separately, perhaps in a daily script.
        
        performance_yield = (perf_prestige + perf_flat_bonus) * perf_multiplier
        tenure_yield = tenure_prestige * tenure_multiplier
        
        # --- Social Mechanics ---
        hype_bonus = calculate_hype_bonus(portfolios_df, in_game_name)
        
        # Total CC earned for the period before social bonuses
        base_cc_earned = performance_yield + tenure_yield
        
        # Apply Hype Bonus to personal earnings
        total_personal_cc_earned = base_cc_earned * hype_bonus
        
        # --- Sponsorship Dividend ---
        # Find the largest shareholder for the current member's stock
        shareholders = portfolios_df[portfolios_df['stock_in_game_name'] == in_game_name]
        if not shareholders.empty:
            largest_shareholder = shareholders.loc[shareholders['shares_owned'].idxmax()]
            sponsor_discord_id = largest_shareholder['investor_discord_id']
            
            # Formula: Sponsorship Dividend = 0.10 * Target Member's Total CC Earned
            sponsorship_dividend = 0.10 * total_personal_cc_earned
            
            # Add the dividend directly to the sponsor's balance
            crew_coins_df.loc[crew_coins_df['discord_id'] == sponsor_discord_id, 'balance'] += sponsorship_dividend

        # --- Update Balances ---
        crew_coins_df.loc[crew_coins_df['in_game_name'] == in_game_name, 'balance'] += total_personal_cc_earned
        total_earnings.append({'in_game_name': in_game_name, 'earnings': total_personal_cc_earned})

    print("CC earnings processed for all members.")
    
    # Return the updated DataFrame to be saved
    return crew_coins_df, pd.DataFrame(total_earnings)