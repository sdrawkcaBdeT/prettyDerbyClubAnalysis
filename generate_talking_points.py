# generate_talking_points.py
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

# --- NEW: Import the function from your main script ---
from analysis import get_club_month_window

def generate_stats():
    """
    Calculates and prints the key stats for the Discord announcement.
    """
    try:
        members_df = pd.read_csv("Umamusume Pretty Derby_ Cash Crew Fans - members.csv")
        fanlog_df = pd.read_csv("Umamusume Pretty Derby_ Cash Crew Fans - fanLog.csv")
    except FileNotFoundError:
        print("Error: Make sure 'members.csv' and 'fanLog.csv' are in the same directory.")
        return

    # --- Data Cleaning and Timezone Handling ---
    fanlog_df['fanCount'] = pd.to_numeric(fanlog_df['fanCount'].astype(str).str.replace(',', '', regex=False), errors='coerce')
    fanlog_df['timestamp'] = pd.to_datetime(fanlog_df['timestamp'], errors='coerce')
    fanlog_df.dropna(subset=['fanCount', 'timestamp'], inplace=True)
    
    central_tz = pytz.timezone('US/Central')
    fanlog_df['timestamp'] = fanlog_df['timestamp'].dt.tz_localize(central_tz)

    # --- Calculation Logic ---
    generation_ct = datetime.now(central_tz)
    start_date, end_date = get_club_month_window(generation_ct)
    
    monthly_fan_log = fanlog_df[(fanlog_df['timestamp'] >= start_date) & (fanlog_df['timestamp'] <= end_date)].copy()
    
    active_members = members_df[members_df['status'] == 'Active']
    num_active_members = len(active_members)

    time_elapsed_so_far = (monthly_fan_log['timestamp'].max() - start_date).total_seconds() / (24 * 3600)
    total_time_in_period = (end_date - start_date).total_seconds() / (24 * 3600)

    projections = []
    for _, member in active_members.iterrows():
        member_logs = monthly_fan_log[monthly_fan_log['inGameName'] == member['inGameName']]
        if not member_logs.empty:
            first_fan_count = member_logs.iloc[0]['fanCount']
            last_fan_count = member_logs.iloc[-1]['fanCount']
            fan_gain_so_far = last_fan_count - first_fan_count
            
            projected_gain = (fan_gain_so_far / time_elapsed_so_far) * total_time_in_period
            projections.append(projected_gain)

    if not projections:
        print("No active members with fan data found for the current period.")
        return

    members_on_pace_20m = sum(1 for p in projections if p > 20_000_000)
    total_club_projection = sum(projections)
    average_member_projection = np.mean(projections)

    # --- Print Output ---
    print("--- Cash Crew Talking Points ---")
    print(f"Generated on: {generation_ct.strftime('%Y-%m-%d %I:%M %p %Z')}\n")
    
    print(f"[*] Active Members: {num_active_members}")
    print(f"[*] On Pace for >20M Fans: {members_on_pace_20m} out of {num_active_members}")
    print(f"[*] Projected Club Total: ~{total_club_projection / 1_000_000:.0f} Million Fans")
    print(f"[*] Average Member Projection: ~{average_member_projection / 1_000_000:.1f} Million Fans")

if __name__ == "__main__":
    generate_stats()