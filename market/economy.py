import pandas as pd
import os
import json

def get_upgrade_value(upgrades_df, discord_id, upgrade_name, base_value, bonus_per_tier):
    """Calculates the value of a stat after applying tiered upgrades."""
    if upgrades_df.empty or pd.isna(discord_id):
        return base_value
        
    # Ensure discord_id in upgrades_df is the same type for comparison
    upgrades_df['discord_id'] = upgrades_df['discord_id'].astype(str)
    discord_id_str = str(discord_id)
    
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
    shares_owned_by_others = portfolios_df[
        portfolios_df['stock_inGameName'] == member_name
    ]['shares_owned'].sum()
    
    hype_bonus = 1 + (0.0005 * shares_owned_by_others)
    return hype_bonus

def process_cc_earnings(enriched_df, market_data_dfs, run_timestamp):
    """
    Calculates all periodic earnings and returns the updated balances DataFrame
    and a list of new transaction records to be logged.
    """
    # Use .copy() to avoid SettingWithCopyWarning
    crew_coins_df = market_data_dfs['crew_coins'].copy()
    portfolios_df = market_data_dfs['portfolios']
    shop_upgrades_df = market_data_dfs['shop_upgrades']
    market_state = market_data_dfs['market_state'].set_index('state_name')['state_value']

    # --- Event Handling for Earnings ---
    active_event_name = str(market_state.get('active_event', 'None'))
    performance_yield_modifier = 1.0
    if active_event_name == "Headwind on the Back Stretch":
        print("EVENT ACTIVE: Applying 'Headwind on the Back Stretch' modifier.")
        performance_yield_modifier = 0.5

    latest_data = enriched_df.sort_values('timestamp').groupby('inGameName').tail(1)
    new_transaction_records = []
    dividend_payouts = {}
    
    # Use a dictionary for faster lookups
    balance_map = crew_coins_df.set_index('inGameName')['balance'].to_dict()
    id_map = crew_coins_df.set_index('inGameName')['discord_id'].to_dict()

    for _, member in latest_data.iterrows():
        inGameName = member['inGameName']
        if inGameName not in balance_map: continue
        
        discord_id = id_map.get(inGameName)

        perf_prestige = member.get('performancePrestigePoints', 0)
        tenure_prestige = member.get('tenurePrestigePoints', 0)
        
        perf_multiplier = get_upgrade_value(shop_upgrades_df, discord_id, "Study Race Tapes", 1.75, 2.75)
        perf_flat_bonus = get_upgrade_value(shop_upgrades_df, discord_id, "Perfect the Starting Gate", 2, 3)
        tenure_multiplier = get_upgrade_value(shop_upgrades_df, discord_id, "Build Club Morale", 2.0, 0.2)
        
        performance_yield = (perf_prestige + perf_flat_bonus) * perf_multiplier * performance_yield_modifier
        tenure_yield = tenure_prestige * tenure_multiplier
        base_cc_earned = max(0, performance_yield + tenure_yield)
        
        hype_bonus_multiplier = calculate_hype_bonus(portfolios_df, inGameName)
        hype_bonus_yield = base_cc_earned * (hype_bonus_multiplier - 1)
        total_personal_cc_earned = base_cc_earned + hype_bonus_yield
        
        balance_map[inGameName] += total_personal_cc_earned
        
        shareholders = portfolios_df[portfolios_df['stock_inGameName'] == inGameName]
        external_shareholders = shareholders[shareholders['investor_discord_id'] != discord_id].copy()

        if not external_shareholders.empty:
            largest_shareholder = external_shareholders.loc[external_shareholders['shares_owned'].idxmax()]
            sponsor_discord_id = str(largest_shareholder['investor_discord_id'])
            
            sponsorship_dividend = 0.20 * total_personal_cc_earned
            dividend_payouts[sponsor_discord_id] = dividend_payouts.get(sponsor_discord_id, 0) + sponsorship_dividend
            
            proportional_dividend_pool = 0.10 * total_personal_cc_earned
            other_shareholders = external_shareholders[external_shareholders['investor_discord_id'] != sponsor_discord_id]
            
            if not other_shareholders.empty:
                total_other_shares = other_shareholders['shares_owned'].sum()
                if total_other_shares > 0:
                    for _, shareholder in other_shareholders.iterrows():
                        investor_id = str(shareholder['investor_discord_id'])
                        proportion = shareholder['shares_owned'] / total_other_shares
                        proportional_payout = proportional_dividend_pool * proportion
                        dividend_payouts[investor_id] = dividend_payouts.get(investor_id, 0) + proportional_payout

        # This dictionary will be converted to the details JSONB field
        details_json = json.dumps({
            'performance_yield': performance_yield,
            'tenure_yield': tenure_yield,
            'hype_bonus_yield': hype_bonus_yield
        })

        # We create a record for the user's personal earnings
        new_transaction_records.append((
            run_timestamp,
            discord_id,
            'SYSTEM',
            'PERIODIC_EARNINGS',
            'Personal Earnings',
            total_personal_cc_earned,
            0, # fee_paid
            details_json,
            None # balance_after will be calculated later
        ))

    # Apply dividend payouts to the balance map
    discord_id_to_name_map = {v: k for k, v in id_map.items()}
    for sponsor_id, dividend_amount in dividend_payouts.items():
        if sponsor_id in discord_id_to_name_map:
            target_name = discord_id_to_name_map[sponsor_id]
            balance_map[target_name] += dividend_amount

            # Log a separate transaction for the dividend
            new_transaction_records.append((
                run_timestamp,
                sponsor_id,
                'SYSTEM',
                'DIVIDEND',
                'Dividend Payout',
                dividend_amount,
                0, # fee_paid
                None, # details
                None # balance_after
            ))

    # Convert the updated map back to a DataFrame
    updated_balances_df = pd.DataFrame(balance_map.items(), columns=['inGameName', 'balance'])
    # Merge to get the discord_id back for saving
    updated_balances_df = pd.merge(updated_balances_df, crew_coins_df[['inGameName', 'discord_id']], on='inGameName', how='left')

    # Now, calculate the final balance_after for each transaction
    final_balance_map = updated_balances_df.set_index('discord_id')['balance'].to_dict()
    
    final_transactions = []
    for record in new_transaction_records:
        actor_id = record[1]
        balance_after = final_balance_map.get(actor_id)
        # Create a new tuple with the balance_after value
        final_transactions.append(record[:-1] + (balance_after,))

    print("CC earnings processed and transaction records created.")
    return updated_balances_df, final_transactions