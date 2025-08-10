import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
from datetime import datetime
import os

# --- Configuration ---
MEMBERS_CSV = 'Umamusume Pretty Derby_ Cash Crew Fans - members.csv'
FANLOG_CSV = 'Umamusume Pretty Derby_ Cash Crew Fans - fanLog.csv'
OUTPUT_DIR = 'Club_Report_Output'

# --- Chart Styling ---
plt.style.use('seaborn-v0_8-whitegrid')
try:
    font_path = fm.findfont('DejaVu Sans')
    font_prop = fm.FontProperties(fname=font_path)
    plt.rcParams['font.family'] = font_prop.get_name()
except:
    print("Clean font not found, using default. Charts will still be generated.")
plt.rcParams['figure.dpi'] = 150

def generate_visualizations(summary_df, analysis_df):
    """Creates and saves all the requested charts and logs."""
    print("\n--- 3. Generating Visualizations ---")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    # Monthly Leaderboard
    if not summary_df.empty:
        plt.figure(figsize=(10, 8))
        top_10 = summary_df.nlargest(10, 'totalMonthlyGain')
        # Updated barplot call to be compatible with future Seaborn versions
        sns.barplot(x='totalMonthlyGain', y='inGameName', data=top_10, palette='viridis', hue='inGameName', dodge=False)
        plt.legend([],[], frameon=False) # Hide the legend created by 'hue'
        plt.title('Top 10 Members by Monthly Fan Gain', fontsize=16, weight='bold')
        plt.xlabel('Total Fans Gained This Month', fontsize=12)
        plt.ylabel('Member', fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'monthly_leaderboard.png'))
        plt.close()
        print("  - Saved monthly_leaderboard.png")

    # Club Update Log
    if not analysis_df.empty:
        generate_cml_log(analysis_df, 'Club Update Log', 'club_update_log.png', limit=25)

        # Individual Member Logs
        all_members = summary_df['inGameName'].unique()
        for member_name in all_members:
            member_data = analysis_df[analysis_df['inGameName'] == member_name]
            if not member_data.empty:
                # Sanitize filename by removing invalid characters
                safe_member_name = member_name.replace(' ', '_').replace('/', '').replace('\\', '')
                filename = f"log_{safe_member_name}.png"
                generate_cml_log(member_data, f"Update Log: {member_name}", filename, limit=25)
        print(f"  - Saved individual update logs for {len(all_members)} members.")

def generate_cml_log(data, title, filename, limit=25):
    """Generates and saves a text-based log as an image."""
    log_data = data.sort_values('timestamp', ascending=False).head(limit)
    if log_data.empty: return

    fig, ax = plt.subplots(figsize=(6, 8))
    fig.patch.set_facecolor('#2E2E2E')
    ax.set_facecolor('#2E2E2E')
    ax.set_title(title, color='white', fontsize=16, weight='bold', loc='left', pad=20)
    
    y_pos = 0.95
    for _, row in log_data.iterrows():
        # DEFINITIVE FIX: Universal timestamp formatting that works on all systems.
        # We get each part of the date and remove leading zeros manually.
        hour = row['timestamp'].strftime('%I').lstrip('0') or '12'
        minute = row['timestamp'].strftime('%M')
        ampm = row['timestamp'].strftime('%p')
        month = str(row['timestamp'].month)
        day = str(row['timestamp'].day)
        year = str(row['timestamp'].year)
        timestamp_str = f"{hour}:{minute} {ampm} {month}/{day}/{year}"

        gain_str = f"+{int(row['fanGain']):,}" if row['fanGain'] > 0 else str(int(row['fanGain']))
        gain_color = '#4CAF50' if row['fanGain'] > 0 else '#BDBDBD'
        
        ax.text(0.01, y_pos, timestamp_str, color='#E0E0E0', fontsize=12, transform=ax.transAxes, va='top')
        ax.text(0.99, y_pos, gain_str, color=gain_color, fontsize=12, weight='bold', transform=ax.transAxes, ha='right', va='top')
        y_pos -= (1 / (limit + 2))

    ax.axis('off')
    plt.savefig(os.path.join(OUTPUT_DIR, filename), bbox_inches='tight', pad_inches=0.3, facecolor=fig.get_facecolor())
    plt.close()

def main():
    """Main function to run the entire analysis pipeline."""
    print("--- 1. Loading and Cleaning Data ---")
    try:
        members_df = pd.read_csv(MEMBERS_CSV)
        fanlog_df = pd.read_csv(FANLOG_CSV)
        print(f"Successfully loaded {len(members_df)} members and {len(fanlog_df)} log entries.")
    except FileNotFoundError as e:
        print(f"FATAL ERROR: {e}. Script cannot continue.")
        return

    fanlog_df.dropna(subset=['inGameName', 'fanCount'], inplace=True)
    fanlog_df['fanCount'] = pd.to_numeric(fanlog_df['fanCount'].astype(str).str.replace(',', '', regex=False), errors='coerce')
    fanlog_df['timestamp'] = pd.to_datetime(fanlog_df['timestamp'], errors='coerce')
    fanlog_df.dropna(subset=['fanCount', 'timestamp'], inplace=True)
    print(f"Found {len(fanlog_df)} valid log entries after cleaning.")

    print("\n--- 2. Performing Core Analysis (New Robust Method) ---")
    
    # Sort the entire dataframe correctly one time
    fanlog_df = fanlog_df.sort_values(by=['inGameName', 'timestamp'])

    # Get previous values for the entire dataframe at once using .shift()
    fanlog_df['previousFanCount'] = fanlog_df['fanCount'].shift(1)
    fanlog_df['previousTimestamp'] = fanlog_df['timestamp'].shift(1)
    fanlog_df['previousName'] = fanlog_df['inGameName'].shift(1)

    # A boolean mask to identify the first entry for each member
    is_first_entry = (fanlog_df['inGameName'] != fanlog_df['previousName'])

    # Calculate fanGain for all rows at once
    fanlog_df['fanGain'] = fanlog_df['fanCount'] - fanlog_df['previousFanCount']
    fanlog_df.loc[is_first_entry, 'fanGain'] = fanlog_df['fanCount']

    # Calculate timeDiffMinutes for all rows at once
    time_diff = (fanlog_df['timestamp'] - fanlog_df['previousTimestamp']).dt.total_seconds()
    fanlog_df['timeDiffMinutes'] = time_diff / 60
    fanlog_df.loc[is_first_entry, 'timeDiffMinutes'] = 0

    # Calculate fansPerMinute for all rows at once
    fanlog_df['fansPerMinute'] = (fanlog_df['fanGain'] / fanlog_df['timeDiffMinutes']).fillna(0)
    fanlog_df.loc[fanlog_df['timeDiffMinutes'] == 0, 'fansPerMinute'] = 0
    
    # Create the final analysis dataframe
    fan_gain_analysis_df = fanlog_df[['timestamp', 'memberID', 'inGameName', 'fanCount', 'fanGain', 'timeDiffMinutes', 'fansPerMinute']].copy()
    print("  - Fan gain analysis complete.")

    # --- Member Summary Calculation ---
    report_date = pd.to_datetime(datetime.now())
    active_members_df = members_df[members_df['status'].isin(['Active', 'Pending'])].copy()
    summary_list = []

    for _, member in active_members_df.iterrows():
        name = member['inGameName']
        member_logs = fan_gain_analysis_df[fan_gain_analysis_df['inGameName'] == name]
        
        if not member_logs.empty:
            latest_update = member_logs['timestamp'].max()
            days_since_update = (report_date - latest_update).days
            
            start_of_month = report_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            monthly_logs = member_logs[member_logs['timestamp'] >= start_of_month]
            total_monthly_gain = monthly_logs['fanGain'].sum()

            gain_last_7_days = member_logs[member_logs['timestamp'] > (report_date - pd.Timedelta(days=7))]['fanGain'].sum()
            gain_last_14_days = member_logs[member_logs['timestamp'] > (report_date - pd.Timedelta(days=14))]['fanGain'].sum()

            total_gain = member_logs['fanGain'].sum()
            total_time_minutes = member_logs['timeDiffMinutes'].sum()
            avg_daily_gain = (total_gain / (total_time_minutes / 1440)) if total_time_minutes > 0 else 0

            summary_list.append({
                'inGameName': name, 'memberID': member['memberID'], 'latestUpdate': latest_update,
                'daysSinceUpdate': days_since_update, 'totalMonthlyGain': total_monthly_gain,
                'avgDailyGain': avg_daily_gain, 'gainLast7Days': gain_last_7_days, 'gainLast14Days': gain_last_14_days
            })

    member_summary_df = pd.DataFrame(summary_list)
    print("  - Member summary complete.")

    # --- Generate Visuals and Save Outputs ---
    generate_visualizations(member_summary_df, fan_gain_analysis_df)

    # --- Save Data Files ---
    output_gain_file = os.path.join(OUTPUT_DIR, 'fanGainAnalysis_output.csv')
    output_summary_file = os.path.join(OUTPUT_DIR, 'memberSummary_output.csv')
    
    fan_gain_analysis_df.to_csv(output_gain_file, index=False)
    member_summary_df.to_csv(output_summary_file, index=False)
    
    print(f"\n--- ✅ Analysis Complete! ---")
    print(f"All reports have been saved to the '{OUTPUT_DIR}' folder.")

if __name__ == "__main__":
    main()
