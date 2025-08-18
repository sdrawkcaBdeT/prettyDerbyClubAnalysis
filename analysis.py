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
import generate_visuals

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
    
        # --- NEW: Daily Summary Aggregation Logic ---
    print("  - Aggregating data into daily summaries...")
    daily_summary_list = []
    
    # Create a date column for grouping
    individual_log_df['date'] = individual_log_df['timestamp'].dt.date

    # 1. Calculate ACCURATE daily sums for all metrics first.
    daily_summary_df = individual_log_df.groupby(['inGameName', 'date']).agg(
        dailyFanGain=('fanGain', 'sum'),
        dailyPrestigeGain=('prestigeGain', 'sum'),
        timestamp=('timestamp', 'last')
    ).reset_index()

    # 2. Merge the latest prestige info for each day from the main log.
    prestige_info = individual_log_df.loc[individual_log_df.groupby(['inGameName', 'date'])['timestamp'].idxmax()][
        ['inGameName', 'date', 'cumulativePrestige', 'prestigeRank', 'pointsToNextRank']
    ]
    daily_summary_df = pd.merge(daily_summary_df, prestige_info, on=['inGameName', 'date'])

    # 3. Calculate ACCURATE cumulative monthly fan gain from the daily sums.
    daily_summary_df = daily_summary_df.sort_values(by=['inGameName', 'date'])
    daily_summary_df['monthlyFanGain'] = daily_summary_df.groupby('inGameName')['dailyFanGain'].cumsum()

    # 4. Now, calculate ranks based on the correct monthly fan gain.
    daily_summary_df['rank'] = daily_summary_df.groupby('date')['monthlyFanGain'].rank(method='dense', ascending=False)

    # 5. Calculate Rank Delta (change from the previous day).
    daily_summary_df['previous_rank'] = daily_summary_df.groupby('inGameName')['rank'].shift(1)
    daily_summary_df['rank_delta'] = daily_summary_df['previous_rank'] - daily_summary_df['rank']

    # 6. Calculate Fans to Next Rank.
    def get_fans_to_next(df):
        df = df.sort_values('rank')
        df['next_rank_fans'] = df['monthlyFanGain'].shift(1)
        df['fansToNextRank'] = df['next_rank_fans'] - df['monthlyFanGain'] + 1
        return df
    daily_summary_df = daily_summary_df.groupby('date', group_keys=False).apply(get_fans_to_next)

    # 7. Calculate Month Pacing.
    time_elapsed_hrs = (daily_summary_df['timestamp'] - start_date).dt.total_seconds() / 3600
    time_remaining_hrs = (end_date - daily_summary_df['timestamp']).dt.total_seconds() / 3600
    fans_per_hour = (daily_summary_df['monthlyFanGain'] / time_elapsed_hrs).replace([np.inf, -np.inf], 0).fillna(0)
    daily_summary_df['monthPacing'] = daily_summary_df['monthlyFanGain'] + (fans_per_hour * time_remaining_hrs)

    print("  - Daily summary aggregation complete.")
    
    # --- START: Replace the old club log logic with this ---
    print("  - Aggregating club data into daily summary...")
    daily_club_summary_list = []
    club_daily_groups = individual_log_df.groupby('date')

    for date, group in club_daily_groups:
        latest_entry = group.loc[group['timestamp'].idxmax()]
        
        # Sum stats for the entire club for that day
        daily_fan_gain = group['fanGain'].sum()
        daily_prestige_gain = group['prestigeGain'].sum()
        
        # Calculate month-to-date total for the club
        club_month_to_date = individual_log_df[individual_log_df['date'] <= date]
        monthly_fan_gain = club_month_to_date['fanGain'].sum()
        
        # Calculate Month Pacing for the club
        time_elapsed_hrs = (latest_entry['timestamp'] - start_date).total_seconds() / 3600
        time_remaining_hrs = (end_date - latest_entry['timestamp']).total_seconds() / 3600
        fans_per_hour = monthly_fan_gain / time_elapsed_hrs if time_elapsed_hrs > 0 else 0
        month_pacing = monthly_fan_gain + (fans_per_hour * time_remaining_hrs)
        
        daily_club_summary_list.append({
            'timestamp': latest_entry['timestamp'],
            'inGameName': 'Club Total', # Added for consistency
            'dailyFanGain': daily_fan_gain,
            'monthlyFanGain': monthly_fan_gain,
            'rank': '-',          # Placeholder
            'rank_delta': '-',    # Placeholder
            'fansToNextRank': '-',# Placeholder for "Fans to Rank 100"
            'monthPacing': month_pacing,
            'dailyPrestigeGain': daily_prestige_gain
        })

    daily_club_summary_df = pd.DataFrame(daily_club_summary_list)
    print("  - Club daily summary aggregation complete.")

    # --- Calculations for Member Summary Table (uses latest from individual_log_df)---
    member_summary_df = daily_summary_df.loc[daily_summary_df.groupby('inGameName')['timestamp'].idxmax()].copy()
    member_summary_df.rename(columns={'monthlyFanGain': 'totalMonthlyGain'}, inplace=True)
    
    # --- Calculations for Fan Contribution Chart ---
    summary_with_ranks = member_summary_df.sort_values('totalMonthlyGain', ascending=False).copy()
    summary_with_ranks['rank'] = range(1, len(summary_with_ranks) + 1)
    
    bins = [0, 6, 12, 18, 24, 30]
    labels = ['Ranks 1-6', 'Ranks 7-12', 'Ranks 13-18', 'Ranks 19-24', 'Ranks 25-30']
    summary_with_ranks['Rank Group'] = pd.cut(summary_with_ranks['rank'], bins=bins, labels=labels, right=True)

    contribution_df = summary_with_ranks.groupby('Rank Group', observed=True)['totalMonthlyGain'].sum().reset_index()
    total_club_gain = contribution_df['totalMonthlyGain'].sum()
    if total_club_gain > 0:
        contribution_df['percentage'] = (contribution_df['totalMonthlyGain'] / total_club_gain) * 100
    else:
        contribution_df['percentage'] = 0
    contribution_df = contribution_df.set_index('Rank Group')
    print("  - Fan contribution analysis complete.")

    # --- Calculations for Historical Table ---
    historical_df = monthly_fan_log.copy()
    historical_df['time_group'] = historical_df['timestamp'].dt.floor('8h')
    print("  - Historical data prepared.")

    # --- Generate all outputs ---
    print("\n--- 4. Calling Visualization Script ---")
    generate_visuals.create_all_visuals(
        # Add the missing dataframe and use consistent names
        members_df=members_df,
        summary_df=member_summary_df,
        individual_log_df=individual_log_df,
        club_log_df=daily_club_summary_df,
        contribution_df=contribution_df,
        historical_df=historical_df,
        last_updated_str=last_updated_str,
        generated_str=generated_str,
        start_date=start_date,
        end_date=end_date,
        daily_summary_df=daily_summary_df
    )

    

    print(f"\n--- Analysis Complete! ---")
    print(f"All reports have been saved to the '{OUTPUT_DIR}' folder.")

if __name__ == "__main__":
    main()