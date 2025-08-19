import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
from datetime import datetime
import numpy as np
import os
import pytz
import matplotlib.patheffects as pe
import matplotlib.font_manager as fm
from market.economy import load_market_data, process_cc_earnings
from market.engine import update_all_stock_prices

# --- Configuration ---
MEMBERS_CSV = 'members.csv'
FANLOG_CSV = 'fan_log.csv'
RANKS_CSV = 'ranks.csv'
OUTPUT_DIR = 'Club_Report_Output'

def get_club_month_window(run_time_ct):
    """Calculates the start and end of the current in-game ranking period."""
    start_date = run_time_ct.replace(day=1, hour=10, minute=0, second=0, microsecond=0)
    
    if run_time_ct.month == 12:
        end_date = start_date.replace(year=start_date.year + 1, month=1, hour=4, minute=59, second=59)
    else:
        end_date = start_date.replace(month=start_date.month + 1, hour=4, minute=59, second=59)

    if run_time_ct < start_date:
        end_date = start_date.replace(hour=4, minute=59, second=59)
        if start_date.month == 1:
            start_date = start_date.replace(year=start_date.year - 1, month=12, hour=10, minute=0, second=0)
        else:
            start_date = start_date.replace(month=start_date.month - 1, hour=10, minute=0, second=0)

    first_month_start = pytz.timezone('US/Central').localize(datetime(2025, 8, 8, 23, 45, 0))
    if start_date.month == 8 and start_date.year == 2025:
        start_date = first_month_start

    return start_date, end_date


def format_time_diff(minutes):
    """Formats a duration in minutes into a clean, readable string (e.g., '3d 4h', '1h 15m')."""
    if pd.isna(minutes) or minutes <= 0:
        return '-'
    days = int(minutes // 1440)
    hours = int((minutes % 1440) // 60)
    mins = int(minutes % 60)

    if days > 0:
        return f"{days}d {hours}h"
    elif hours > 0:
        return f"{hours}h {mins}m"
    else:
        return f"{mins}m"

def main():
    """Main function to run the entire analysis pipeline."""
    print("--- 1. Loading and Cleaning Data ---")
    try:
        members_df = pd.read_csv(MEMBERS_CSV)
        fanlog_df = pd.read_csv(FANLOG_CSV)
        ranks_df = pd.read_csv(RANKS_CSV)
        print(f"Successfully loaded {len(members_df)} members, {len(fanlog_df)} log entries, and {len(ranks_df)} ranks.")
    except FileNotFoundError as e:
        print(f"FATAL ERROR: {e}. Script cannot continue.")
        return

    fanlog_df.dropna(subset=['inGameName', 'fanCount'], inplace=True)
    fanlog_df['fanCount'] = pd.to_numeric(fanlog_df['fanCount'].astype(str).str.replace(',', '', regex=False), errors='coerce')
    fanlog_df['timestamp'] = pd.to_datetime(fanlog_df['timestamp'], errors='coerce')
    fanlog_df.dropna(subset=['fanCount', 'timestamp'], inplace=True)

    central_tz = pytz.timezone('US/Central')
    fanlog_df['timestamp'] = fanlog_df['timestamp'].dt.tz_localize(central_tz)

    print(f"Found {len(fanlog_df)} valid log entries after cleaning.")

    # --- Timestamp Generation ---
    generation_ct = datetime.now(central_tz)
    start_date, end_date = get_club_month_window(generation_ct)
    last_updated_ct = fanlog_df['timestamp'].max()
    last_updated_str = last_updated_ct.strftime('%Y-%m-%d %I:%M %p %Z')
    generated_str = generation_ct.strftime('%Y-%m-%d %I:%M %p %Z')
    print(f"  - Last data collected: {last_updated_str}")
    print(f"  - Report generated:    {generated_str}")

    print("\n--- 2. Performing Core Analysis ---")

    monthly_fan_log = fanlog_df[(fanlog_df['timestamp'] >= start_date) & (fanlog_df['timestamp'] <= end_date)].copy()
    monthly_fan_log = monthly_fan_log.sort_values(by=['inGameName', 'timestamp'])

    monthly_fan_log['previousFanCount'] = monthly_fan_log.groupby('inGameName')['fanCount'].shift(1)
    monthly_fan_log['fanGain'] = monthly_fan_log['fanCount'] - monthly_fan_log['previousFanCount']
    monthly_fan_log['fanGain'].fillna(0, inplace=True)
    
    individual_log_df = monthly_fan_log.copy() # We still need this for some calculations
    
    # --- Prestige Calculations ---
    individual_log_df['timeDiffMinutes'] = monthly_fan_log.groupby('inGameName')['timestamp'].diff().dt.total_seconds() / 60
    individual_log_df['timeDiffMinutes'].fillna(0, inplace=True)
    individual_log_df['performancePrestigePoints'] = individual_log_df['fanGain'] / 8333
    individual_log_df['tenurePrestigePoints'] = 20 * (individual_log_df['timeDiffMinutes'] / 1440)
    individual_log_df['prestigeGain'] = individual_log_df['performancePrestigePoints'] + individual_log_df['tenurePrestigePoints']
    individual_log_df['cumulativePrestige'] = individual_log_df.groupby('inGameName')['prestigeGain'].cumsum()

    # This block calculates the rank name based on prestige points
    ranks_df_sorted = ranks_df.sort_values('prestige_required', ascending=False)
    def get_rank_details(cumulative_prestige):
        for _, rank_row in ranks_df_sorted.iterrows():
            if cumulative_prestige >= rank_row['prestige_required']:
                return rank_row['rank_name']
        return "Unranked"
    individual_log_df['prestigeRank'] = individual_log_df['cumulativePrestige'].apply(get_rank_details)

    # This block calculates the points needed for the next rank
    next_rank_req = ranks_df.set_index('rank_name')['prestige_required'].shift(-1).to_dict()
    def get_points_to_next_rank(row):
        current_rank = row['prestigeRank']
        if current_rank in next_rank_req and pd.notna(next_rank_req[current_rank]):
            return next_rank_req[current_rank] - row['cumulativePrestige']
        return np.nan
    individual_log_df['pointsToNextRank'] = individual_log_df.apply(get_points_to_next_rank, axis=1)
    individual_log_df['date'] = individual_log_df['timestamp'].dt.date
    
    print("\n--- 3. Saving Enriched Fan Log ---")
    individual_log_df.to_csv('enriched_fan_log.csv', index=False)
    print("  - Successfully created enriched_fan_log.csv")
    
    # =================================================================
    # FAN EXCHANGE: ECONOMY AND PRICE ENGINE
    # =================================================================
    print("\n--- Processing Fan Exchange ---")
    
    # Use the timestamp from when the report was generated for consistent logging
    run_timestamp = generation_ct 

    # --- Load Market Data ---
    market_data = load_market_data()
    if not market_data:
        print("FATAL: Could not load market data. Halting Fan Exchange processing.")
        return # Exit if market files aren't found
        
    # --- 1. Process CC Earnings & Log History ---
    print("Running CC Earnings Engine...")
    updated_crew_coins_df = process_cc_earnings(individual_log_df, market_data, run_timestamp)
    updated_crew_coins_df['balance'] = updated_crew_coins_df['balance'].round().astype(int)
    updated_crew_coins_df.to_csv('market/crew_coins.csv', index=False)
    print("Successfully updated crew_coins.csv and logged balance_history.csv.")

    # --- 2. Update Stock Prices & Log History ---
    print("\nRunning Baggins Index Price Engine...")
    # Refresh all dataframes for the price engine
    all_market_dfs = {
        'crew_coins': updated_crew_coins_df,
        'portfolios': pd.read_csv('market/portfolios.csv'),
        'shop_upgrades': pd.read_csv('market/shop_upgrades.csv', dtype={'discord_id': str}),
        'member_initialization': pd.read_csv('market/member_initialization.csv'),
        'stock_prices': pd.read_csv('market/stock_prices.csv'),
        'market_state': pd.read_csv('market/market_state.csv')
    }
    updated_stocks_df, updated_market_state_df = update_all_stock_prices(individual_log_df, all_market_dfs, run_timestamp)
    updated_stocks_df.to_csv('market/stock_prices.csv', index=False, float_format='%.2f')
    updated_market_state_df.to_csv('market/market_state.csv', index=False)
    print("Successfully updated stock_prices.csv and logged stock_price_history.csv.")
    # =================================================================
    
if __name__ == "__main__":
    main()