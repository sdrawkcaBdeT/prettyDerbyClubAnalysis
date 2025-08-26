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
from market.economy import process_cc_earnings
from market.engine import update_all_stock_prices, calculate_individual_nudges
from market.events import clear_and_check_events, update_lag_index
from market.database import get_market_data_from_db, save_all_market_data_to_db, save_market_state_to_db

# --- Configuration ---
MEMBERS_CSV = 'members.csv'
FANLOG_CSV = 'fan_log.csv'
RANKS_CSV = 'ranks.csv'
OUTPUT_DIR = 'Club_Report_Output'

def _format_timestamp(dt_object):
    """Formats a datetime object into the consistent ecosystem format."""
    base_str = dt_object.strftime('%Y-%m-%d %H:%M:%S%z')
    return f"{base_str[:-2]}:{base_str[-2:]}"

def log_market_snapshot(run_timestamp, market_state):
    """Logs the current state of the market to a historical file."""
    log_file = 'market/market_snapshot_log.csv'
    file_exists = os.path.isfile(log_file)

    try:
        lag_options_str = market_state.get('lag_options', "[0]")
        lag_options = ast.literal_eval(lag_options_str)
        active_cursor = int(market_state.get('active_lag_cursor', 0))
        if active_cursor >= len(lag_options): active_cursor = 0
        active_lag_days = lag_options[active_cursor]
    except Exception:
        active_lag_days = -1

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

    fanlog_df = fanlog_df.sort_values(by=['inGameName', 'timestamp'])

    fanlog_df['previousFanCount'] = fanlog_df.groupby('inGameName')['fanCount'].shift(1)
    fanlog_df['fanGain'] = fanlog_df['fanCount'] - fanlog_df['previousFanCount']
    fanlog_df['fanGain'].fillna(0, inplace=True)
    
    fanlog_df['timeDiffMinutes'] = fanlog_df.groupby('inGameName')['timestamp'].diff().dt.total_seconds() / 60
    fanlog_df['timeDiffMinutes'].fillna(0, inplace=True)
    fanlog_df['performancePrestigePoints'] = fanlog_df['fanGain'] / 8333
    fanlog_df['tenurePrestigePoints'] = 20 * (fanlog_df['timeDiffMinutes'] / 1440)
    fanlog_df['prestigeGain'] = fanlog_df['performancePrestigePoints'] + fanlog_df['tenurePrestigePoints']

    fanlog_df['lifetimePrestige'] = fanlog_df.groupby('inGameName')['prestigeGain'].cumsum()

    monthly_fan_log = fanlog_df[(fanlog_df['timestamp'] >= start_date) & (fanlog_df['timestamp'] <= end_date)].copy()
    monthly_fan_log['monthlyPrestige'] = monthly_fan_log.groupby('inGameName')['prestigeGain'].cumsum()
    
    fanlog_df = pd.merge(fanlog_df, monthly_fan_log[['inGameName', 'timestamp', 'monthlyPrestige']], on=['inGameName', 'timestamp'], how='left')
    fanlog_df['monthlyPrestige'].fillna(0, inplace=True)

    ranks_df_sorted = ranks_df.sort_values('prestige_required', ascending=False)
    def get_rank_details(monthly_prestige):
        for _, rank_row in ranks_df_sorted.iterrows():
            if monthly_prestige >= rank_row['prestige_required']:
                return rank_row['rank_name']
        return "Unranked"
    fanlog_df['prestigeRank'] = fanlog_df['monthlyPrestige'].apply(get_rank_details)

    next_rank_req = ranks_df.set_index('rank_name')['prestige_required'].shift(-1).to_dict()
    def get_points_to_next_rank(row):
        current_rank = row['prestigeRank']
        if current_rank in next_rank_req and pd.notna(next_rank_req[current_rank]):
            return next_rank_req[current_rank] - row['monthlyPrestige']
        return np.nan
    fanlog_df['pointsToNextRank'] = fanlog_df.apply(get_points_to_next_rank, axis=1)
    fanlog_df['date'] = fanlog_df['timestamp'].dt.date
    
    print("\n--- 3. Saving Enriched Fan Log ---")
    fanlog_df.to_csv('enriched_fan_log.csv', index=False)
    print("  - Successfully created enriched_fan_log.csv")
    
    # =================================================================
    # FAN EXCHANGE: ECONOMY AND PRICE ENGINE
    # =================================================================
    print("\n--- Processing Fan Exchange ---")
    
    run_timestamp = generation_ct 

    # --- 1. READ: Load the state of the market AS IT IS RIGHT NOW ---
    market_data = get_market_data_from_db()
    if not market_data:
        print("FATAL: Could not load market data from database. Halting.")
        return
    
    # Log a snapshot of the state we are using for this run's calculations
    log_market_snapshot(run_timestamp, market_data['market_state'].set_index('state_name')['state_value'])
 
    # --- 2. CALCULATE: Perform all calculations using the state we just loaded ---
    print("\nCalculating Individual Performance Nudges...")
    market_data['enriched_fan_log'] = fanlog_df # Add fanlog for this run
    updated_stock_prices_df = calculate_individual_nudges(market_data, run_timestamp)
    market_data['stock_prices'] = updated_stock_prices_df # Update for next step

    print("\nRunning CC Earnings Engine...")
    updated_balances_df, new_transactions = process_cc_earnings(fanlog_df, market_data, run_timestamp)
    
    print("\nRunning Baggins Index Price Engine...")
    final_stock_prices_df, updated_market_state_df_from_engine = update_all_stock_prices(fanlog_df, market_data, run_timestamp)
    
    # --- 3. UPDATE STATE: After all calculations, determine the state for the NEXT run ---
    print("\n--- Checking for Market Lag Shifts for the next cycle ---")
    current_market_state_df = updated_market_state_df_from_engine
    
    # The event check is now disabled, so we just call the lag update
    final_next_market_state_df, lag_announcement = update_lag_index(current_market_state_df, run_timestamp)
    # The event announcement will always be None now
    _, event_announcement = clear_and_check_events(final_next_market_state_df, run_timestamp)

    # --- 4. SAVE: Commit all results to the database ---
    print("\nSaving all market data and the new state to the database...")
    
    save_all_market_data_to_db(updated_balances_df, final_stock_prices_df, new_transactions)
    
    final_next_market_state_df.loc[final_next_market_state_df['state_name'] == 'last_run_timestamp', 'state_value'] = run_timestamp.isoformat()
    save_market_state_to_db(final_next_market_state_df)
    
    # --- 5. QUEUE ANNOUNCEMENTS ---
    if lag_announcement:
        print(f"Queueing announcement: {lag_announcement}")
        with open("announcements.txt", "a") as f:
            f.write(lag_announcement + "\n")
    
if __name__ == "__main__":
    main()