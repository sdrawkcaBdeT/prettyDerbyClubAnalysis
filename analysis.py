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
import csv
import ast # Required for parsing the lag options
from market.economy import load_market_data, process_cc_earnings
from market.engine import update_all_stock_prices
from market.events import clear_and_check_events, update_lag_index

# --- Configuration ---
MEMBERS_CSV = 'members.csv'
FANLOG_CSV = 'fan_log.csv'
RANKS_CSV = 'ranks.csv'
OUTPUT_DIR = 'Club_Report_Output'

def _format_timestamp(dt_object):
    """Formats a datetime object into the consistent ecosystem format."""
    # Example: 2025-08-23 10:56:33-05:00
    base_str = dt_object.strftime('%Y-%m-%d %H:%M:%S%z')
    return f"{base_str[:-2]}:{base_str[-2:]}"

def log_market_snapshot(run_timestamp, market_state):
    """Logs the current state of the market to a historical file."""
    log_file = 'market/market_snapshot_log.csv'
    file_exists = os.path.isfile(log_file)

    # --- Determine the active lag in days ---
    try:
        lag_options_str = market_state.get('lag_options', "[0]")
        lag_options = ast.literal_eval(lag_options_str)
        active_cursor = int(market_state.get('active_lag_cursor', 0))
        if active_cursor >= len(lag_options): active_cursor = 0
        active_lag_days = lag_options[active_cursor]
    except Exception:
        active_lag_days = -1 # Log -1 on error

    snapshot_data = {
        'timestamp': _format_timestamp(run_timestamp),
        'active_event': market_state.get('active_event', 'None'),
        'club_sentiment': market_state.get('club_sentiment', 1.0),
        'active_lag_days': active_lag_days
    }

    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=snapshot_data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(snapshot_data)
    print("--- Market snapshot logged. ---")

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

    # Sort all data by timestamp before doing any calculations
    fanlog_df = fanlog_df.sort_values(by=['inGameName', 'timestamp'])

    # --- Prestige Calculations (Task 1.1) ---
    fanlog_df['previousFanCount'] = fanlog_df.groupby('inGameName')['fanCount'].shift(1)
    fanlog_df['fanGain'] = fanlog_df['fanCount'] - fanlog_df['previousFanCount']
    fanlog_df['fanGain'].fillna(0, inplace=True)
    
    fanlog_df['timeDiffMinutes'] = fanlog_df.groupby('inGameName')['timestamp'].diff().dt.total_seconds() / 60
    fanlog_df['timeDiffMinutes'].fillna(0, inplace=True)
    fanlog_df['performancePrestigePoints'] = fanlog_df['fanGain'] / 8333
    fanlog_df['tenurePrestigePoints'] = 20 * (fanlog_df['timeDiffMinutes'] / 1440)
    fanlog_df['prestigeGain'] = fanlog_df['performancePrestigePoints'] + fanlog_df['tenurePrestigePoints']

    # --- NEW: Dual-Track Prestige Calculation (Task 1.2 & 1.3) ---
    # Lifetime Prestige: Cumulative sum over all time for each member
    fanlog_df['lifetimePrestige'] = fanlog_df.groupby('inGameName')['prestigeGain'].cumsum()

    # Monthly Prestige: Cumulative sum within the current month window
    monthly_fan_log = fanlog_df[(fanlog_df['timestamp'] >= start_date) & (fanlog_df['timestamp'] <= end_date)].copy()
    monthly_fan_log['monthlyPrestige'] = monthly_fan_log.groupby('inGameName')['prestigeGain'].cumsum()
    
    # Merge monthly prestige back into the main dataframe
    fanlog_df = pd.merge(fanlog_df, monthly_fan_log[['inGameName', 'timestamp', 'monthlyPrestige']], on=['inGameName', 'timestamp'], how='left')
    fanlog_df['monthlyPrestige'].fillna(0, inplace=True) # Fill prestige for entries outside the current month

    # This block calculates the rank name based on monthly prestige points (Task 1.4)
    ranks_df_sorted = ranks_df.sort_values('prestige_required', ascending=False)
    def get_rank_details(monthly_prestige):
        for _, rank_row in ranks_df_sorted.iterrows():
            if monthly_prestige >= rank_row['prestige_required']:
                return rank_row['rank_name']
        return "Unranked"
    fanlog_df['prestigeRank'] = fanlog_df['monthlyPrestige'].apply(get_rank_details)

    # This block calculates the points needed for the next rank
    next_rank_req = ranks_df.set_index('rank_name')['prestige_required'].shift(-1).to_dict()
    def get_points_to_next_rank(row):
        current_rank = row['prestigeRank']
        if current_rank in next_rank_req and pd.notna(next_rank_req[current_rank]):
            return next_rank_req[current_rank] - row['monthlyPrestige']
        return np.nan
    fanlog_df['pointsToNextRank'] = fanlog_df.apply(get_points_to_next_rank, axis=1)
    fanlog_df['date'] = fanlog_df['timestamp'].dt.date
    
    print("\n--- 3. Saving Enriched Fan Log (Task 1.5) ---")
    # Note: The old 'cumulativePrestige' column is no longer here
    fanlog_df.to_csv('enriched_fan_log.csv', index=False)
    print("  - Successfully created enriched_fan_log.csv")
    
    # =================================================================
    # FAN EXCHANGE: ECONOMY AND PRICE ENGINE
    # =================================================================
    print("\n--- Processing Fan Exchange ---")
    
    run_timestamp = generation_ct 

    initial_market_data = load_market_data()
    if not initial_market_data:
        print("FATAL: Could not load market data. Halting Fan Exchange processing.")
        return
    
    # Convert the market_state DataFrame to a Series
    market_state_series = initial_market_data['market_state'].set_index('state_name')['value']

    # Log the state of the market *before* any calculations are run.
    log_market_snapshot(run_timestamp, market_state_series)
 
    print("Running CC Earnings Engine...")
    updated_crew_coins_df = process_cc_earnings(fanlog_df, initial_market_data, run_timestamp)
    updated_crew_coins_df['balance'] = updated_crew_coins_df['balance'].round().astype(int)
    updated_crew_coins_df.to_csv('market/crew_coins.csv', index=False)
    print("Successfully updated crew_coins.csv and logged balance_history.csv.")

    print("\nRunning Baggins Index Price Engine...")
    all_market_dfs = {**initial_market_data, 'crew_coins': updated_crew_coins_df}
    updated_stocks_df, updated_market_state_df = update_all_stock_prices(fanlog_df, all_market_dfs, run_timestamp)
    updated_stocks_df.to_csv('market/stock_prices.csv', index=False, float_format='%.2f')
    updated_market_state_df.to_csv('market/market_state.csv', index=False)
    print("Successfully updated stock_prices.csv and logged stock_price_history.csv.")

    print("\n--- Checking for Market Events ---")
    # Check for a lag shift and capture the announcement
    lag_announcement = update_lag_index(run_timestamp)
    if lag_announcement:
        print(f"Queueing announcement: {lag_announcement}")
        with open("announcements.txt", "a") as f:
            f.write(lag_announcement + "\n")

    # Check for a new market event and capture the announcement
    event_announcement = clear_and_check_events(run_timestamp)
    if event_announcement:
        full_event_message = f"🎉 **Market Event!** {event_announcement}"
        print(f"Queueing announcement: {full_event_message}")
        with open("announcements.txt", "a") as f:
            f.write(full_event_message + "\n")
    
if __name__ == "__main__":
    main()