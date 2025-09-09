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

def calculate_hype_bonus(portfolios_df, member_name, member_discord_id):
    """
    Calculates the Hype Bonus based on shares owned by others.
    Excludes shares owned by the member themselves if member_discord_id is provided.
    """
    shares_owned_by_others = portfolios_df[
        (portfolios_df['stock_inGameName'] == member_name) &
        (portfolios_df['investor_discord_id'] != member_discord_id)
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
    elif active_event_name == "The Grand Derby":
        print("EVENT ACTIVE: Applying 'The Grand Derby' earnings boost!")
        performance_yield_modifier = 12.0

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
        
        performance_yield = (perf_prestige + perf_flat_bonus) * (perf_multiplier + performance_yield_modifier)
        tenure_yield = tenure_prestige * tenure_multiplier
        base_cc_earned = max(0, performance_yield + tenure_yield)
        
        hype_bonus_multiplier = calculate_hype_bonus(portfolios_df, inGameName, discord_id)
        hype_bonus_yield = base_cc_earned * (hype_bonus_multiplier - 1)
        total_personal_cc_earned = base_cc_earned + hype_bonus_yield
        
        balance_map[inGameName] += total_personal_cc_earned
        
        # --- NEW: Create detailed JSON for the earnings transaction ---
        details_json = json.dumps({
            'performance_yield': round(performance_yield, 2),
            'tenure_yield': round(tenure_yield, 2),
            'hype_bonus_yield': round(hype_bonus_yield, 2),
            'base_cc_earned': round(base_cc_earned, 2),
            'hype_multiplier': round(hype_bonus_multiplier, 4)
        })

        new_transaction_records.append({
            'timestamp': run_timestamp,
            'actor_id': discord_id,
            'target_id': 'SYSTEM',
            'transaction_type': 'PERIODIC_EARNINGS',
            'item_name': 'Personal Earnings',
            'item_quantity': None,
            'cc_amount': total_personal_cc_earned,
            'fee_paid': 0,
            'details': details_json,
            'balance_after': None # To be filled later
        })
        
        # --- Dividend Logic ---
        # 1. Get all shareholders for the current earner
        all_shareholders = portfolios_df[portfolios_df['stock_inGameName'] == inGameName]

        #    All subsequent logic will operate on this clean list of external investors.
        external_shareholders = all_shareholders[all_shareholders['investor_discord_id'] != discord_id]

        if not external_shareholders.empty:
            # 3. Find all shareholders tied for the maximum number of shares
            max_shares = external_shareholders['shares_owned'].max()
            top_shareholders = external_shareholders[external_shareholders['shares_owned'] == max_shares]

            # 4. Pay the Tier 1 (Sponsorship) Dividend, splitting it among the top shareholders
            sponsorship_dividend_pool = 0.20 * total_personal_cc_earned
            if sponsorship_dividend_pool > 0 and not top_shareholders.empty:
                dividend_per_sponsor = sponsorship_dividend_pool / len(top_shareholders)
                for _, sponsor in top_shareholders.iterrows():
                    sponsor_discord_id = str(sponsor['investor_discord_id'])
                    payout_info = (dividend_per_sponsor, inGameName, 'Tier 1 Div')
                    dividend_payouts[sponsor_discord_id] = dividend_payouts.get(sponsor_discord_id, []) + [payout_info]

            # 5. Identify all OTHER external shareholders for Tier 2 dividends
            top_sponsor_ids = top_shareholders['investor_discord_id'].astype(str).tolist()
            tier_2_recipients = external_shareholders[~external_shareholders['investor_discord_id'].astype(str).isin(top_sponsor_ids)]
            
            if not tier_2_recipients.empty:
                proportional_dividend_pool = 0.10 * total_personal_cc_earned
                total_tier_2_shares = tier_2_recipients['shares_owned'].sum()

                if total_tier_2_shares > 0 and proportional_dividend_pool > 0:
                    for _, shareholder in tier_2_recipients.iterrows():
                        investor_id = str(shareholder['investor_discord_id'])
                        proportion = shareholder['shares_owned'] / total_tier_2_shares
                        proportional_payout = proportional_dividend_pool * proportion
                        
                        payout_info = (proportional_payout, inGameName, 'Tier 2 Div')
                        dividend_payouts[investor_id] = dividend_payouts.get(investor_id, []) + [payout_info]

    # --- Apply dividend payouts and create detailed transaction records ---
    discord_id_to_name_map = {v: k for k, v in id_map.items()}
    for sponsor_id, payouts in dividend_payouts.items():
        # The payout tuple now contains amount, source, and type
        for amount, source_name, dividend_type in payouts:
            if sponsor_id in discord_id_to_name_map:
                target_name = discord_id_to_name_map[sponsor_id]
                balance_map[target_name] += amount

                dividend_details = json.dumps({
                    'source_player': source_name,
                    'type': dividend_type # Use the type from our tuple
                })

                new_transaction_records.append({
                    'timestamp': run_timestamp,
                    'actor_id': sponsor_id,
                    'target_id': id_map.get(source_name), # The player who generated the dividend
                    'transaction_type': 'DIVIDEND',
                    'item_name': f"Dividend from {source_name}",
                    'item_quantity': None,
                    'cc_amount': amount,
                    'fee_paid': 0,
                    'details': dividend_details,
                    'balance_after': None # To be filled later
                })

    # --- Final processing to prepare for database insertion ---
    updated_balances_df = pd.DataFrame(balance_map.items(), columns=['inGameName', 'balance'])
    updated_balances_df = pd.merge(updated_balances_df, crew_coins_df[['inGameName', 'discord_id']], on='inGameName', how='left')


    final_balance_map = updated_balances_df.set_index('discord_id')['balance'].to_dict()
    
    for record in new_transaction_records:
        actor_id = record.get('actor_id')
        if actor_id in final_balance_map:
            record['balance_after'] = final_balance_map[actor_id]

    print(f"CC earnings processed. {len(new_transaction_records)} detailed transaction records created.")

    return updated_balances_df, new_transaction_records