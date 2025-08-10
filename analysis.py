import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
from datetime import datetime
import numpy as np
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

def generate_visualizations(summary_df, analysis_df):
    """Creates and saves all the requested charts and logs."""
    print("\n--- 3. Generating Visualizations ---")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    # Monthly Leaderboard
    if not summary_df.empty:
        plt.figure(figsize=(12, 8))
        top_10 = summary_df.nlargest(10, 'totalMonthlyGain').copy()
        
        ax = sns.barplot(x='totalMonthlyGain', y='inGameName', data=top_10, color='#2E7D32', hue='inGameName', dodge=False)
        plt.legend([],[], frameon=False)
        
        ax.xaxis.set_major_formatter(lambda x, pos: f'{int(x/1000):,}K')
        
        for container in ax.containers:
            labels = [f'{int(v/1000):,}K' for v in container.datavalues]
            ax.bar_label(container, labels=labels, padding=5, fontsize=10, color='black')

        plt.title('Top 10 Members by Monthly Fan Gain', fontsize=16, weight='bold')
        plt.xlabel('Total Fans Gained This Month', fontsize=12)
        plt.ylabel('Member', fontsize=12)
        ax.set_xlim(right=ax.get_xlim()[1] * 1.15)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'monthly_leaderboard.png'))
        plt.close()
        print("  - Saved monthly_leaderboard.png")

    # Club and Individual Logs
    if not analysis_df.empty:
        # For the club log, we still need historical sums, so we create them here.
        club_log_df = analysis_df.set_index('timestamp')
        club_log_df['gainLast12h'] = club_log_df.groupby('inGameName')['fanGain'].rolling('12H').sum().values
        club_log_df['gainLast24h'] = club_log_df.groupby('inGameName')['fanGain'].rolling('24H').sum().values
        club_log_df['gainLast3d'] = club_log_df.groupby('inGameName')['fanGain'].rolling('3D').sum().values
        club_log_df['gainLast7d'] = club_log_df.groupby('inGameName')['fanGain'].rolling('7D').sum().values
        club_log_df['gainLast30d'] = club_log_df.groupby('inGameName')['fanGain'].rolling('30D').sum().values
        club_log_df = club_log_df.reset_index()

        generate_log_image(club_log_df, 'Club Update Log', 'club_update_log.png', limit=25, is_club_log=True)
        print("  - Saved club_update_log.png")

        all_members = summary_df.copy().sort_values('totalMonthlyGain', ascending=False)
        all_members['rank'] = range(1, len(all_members) + 1)

        for _, member_row in all_members.iterrows():
            member_name = member_row['inGameName']
            rank = member_row['rank'] if member_row['rank'] <= 10 else None
            member_data = analysis_df[analysis_df['inGameName'] == member_name]
            if not member_data.empty:
                safe_member_name = member_name.replace(' ', '_').replace('/', '').replace('\\', '')
                filename = f"log_{safe_member_name}.png"
                generate_log_image(member_data, f"Update Log: {member_name}", filename, limit=25, is_club_log=False, rank=rank)
        print(f"  - Saved individual update logs for {len(all_members)} members.")
    
    # New Club Pacing Chart
    if not analysis_df.empty:
        generate_club_pacing_chart(analysis_df)

    # New Member Cumulative Gain Chart
    if not analysis_df.empty:
        generate_member_area_chart(analysis_df)


def generate_club_pacing_chart(analysis_df):
    """Generates the cumulative club gain chart with pacing projection."""
    print("  - Generating club pacing chart...")
    monthly_data = analysis_df[analysis_df['timestamp'].dt.month == datetime.now().month].copy()
    if monthly_data.empty:
        print("    - Skipping pacing chart: No data for the current month.")
        return

    cumulative_gain = monthly_data.groupby(monthly_data['timestamp'].dt.date)['fanGain'].sum().cumsum()
    
    first_day = cumulative_gain.index.min()
    last_day = cumulative_gain.index.max()
    days_elapsed = (last_day - first_day).days + 1
    total_gain_so_far = cumulative_gain.iloc[-1]
    daily_rate = total_gain_so_far / days_elapsed if days_elapsed > 0 else 0

    today = datetime.now().date()
    end_of_month = pd.to_datetime(today).to_period('M').end_time.date()
    days_remaining = (end_of_month - last_day).days
    
    projected_gain = total_gain_so_far + (daily_rate * days_remaining)
    
    projection_dates = pd.to_datetime([last_day, end_of_month])
    projection_values = [total_gain_so_far, projected_gain]

    plt.figure(figsize=(12, 7))
    plt.plot(cumulative_gain.index, cumulative_gain.values, marker='o', linestyle='-', label='Actual Club Gain')
    plt.plot(projection_dates, projection_values, marker='', linestyle='--', color='red', label='Projected Pace')
    
    plt.title('Club Cumulative Fan Gain and Monthly Projection', fontsize=16, weight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Cumulative Fans Gained', fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.legend()
    
    plt.text(projection_dates[-1], projection_values[-1], f' {int(projected_gain/1000):,}K', color='red', va='center')
    
    plt.gca().yaxis.set_major_formatter(lambda x, pos: f'{int(x/1000):,}K')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'club_pacing_chart.png'))
    plt.close()
    print("  - Saved club_pacing_chart.png")


def generate_member_area_chart(analysis_df):
    """Generates the new stacked area chart for cumulative gain by member."""
    print("  - Generating member cumulative gain chart...")
    
    # Pivot data to have members as columns and their cumulative gain as values
    pivot_df = analysis_df.pivot_table(index='timestamp', columns='inGameName', values='fanGain', aggfunc='sum').fillna(0)
    cumulative_df = pivot_df.cumsum()

    # Resample to daily frequency to have a smooth x-axis
    daily_cumulative_df = cumulative_df.resample('D').max().fillna(method='ffill')

    if daily_cumulative_df.empty:
        print("    - Skipping member area chart: No data to plot.")
        return

    plt.figure(figsize=(14, 8))
    ax = daily_cumulative_df.plot.area(stacked=True, colormap='viridis', linewidth=0.5)
    
    plt.title('Cumulative Fan Gain by Member Over Time', fontsize=16, weight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Cumulative Fans Gained', fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    # Format Y-axis to millions (M)
    ax.yaxis.set_major_formatter(lambda x, pos: f'{x/1000000:.1f}M')
    
    # Improve legend
    plt.legend(title='Members', bbox_to_anchor=(1.02, 1), loc='upper left')
    
    plt.tight_layout(rect=[0, 0, 0.85, 1]) # Adjust layout to make space for legend
    plt.savefig(os.path.join(OUTPUT_DIR, 'member_cumulative_gain.png'))
    plt.close()
    print("  - Saved member_cumulative_gain.png")


def generate_log_image(data, title, filename, limit=25, is_club_log=False, rank=None):
    """Generates and saves a CML-style log as an image."""
    log_data = data.sort_values('timestamp', ascending=False).head(limit)
    if is_club_log:
        log_data = data.groupby('timestamp').agg({
            'fanGain': 'sum', 'gainLast12h': 'sum', 'gainLast24h': 'sum',
            'gainLast3d': 'sum', 'gainLast7d': 'sum', 'gainLast30d': 'sum'
        }).reset_index()
        log_data = log_data.sort_values('timestamp', ascending=False).head(limit)
        log_data['timeDiffMinutes'] = -log_data['timestamp'].diff(periods=-1).dt.total_seconds() / 60
    
    if log_data.empty: return

    fig, ax = plt.subplots(figsize=(12, 8))
    fig.patch.set_facecolor('#2E2E2E')
    ax.set_facecolor('#2E2E2E')
    
    if rank:
        ax.text(0.99, 1.0, f"RANK {rank}", color='#FFD700', fontsize=14, weight='bold', transform=ax.transAxes, ha='right', va='bottom')

    ax.set_title(title, color='white', fontsize=16, weight='bold', loc='left', pad=20)
    
    headers = ['Timestamp', 'Time Since', 'Fan Gain', '12h', '24h', '3d', '7d', 'Month-End'] if not is_club_log else ['Timestamp', 'Time Since', 'Fan Gain', '12h', '24h', '3d', '7d', '30d']
    header_positions = [0.01, 0.28, 0.45, 0.58, 0.68, 0.78, 0.88, 0.98]
    for i, header in enumerate(headers):
        ax.text(header_positions[i], 0.97, header, color='#A0A0A0', fontsize=10, weight='bold', transform=ax.transAxes, va='top', ha='left' if i < 2 else 'center')

    y_pos = 0.92
    for _, row in log_data.iterrows():
        hour = row['timestamp'].strftime('%I').lstrip('0') or '12'
        timestamp_str = f"{hour}:{row['timestamp'].strftime('%M %p %m/%d/%Y')}"
        
        time_diff_str = format_time_diff(row['timeDiffMinutes'])
        time_diff_color = '#66BB6A'
        
        gain_val = row['fanGain']
        if not is_club_log and row['is_first_entry']: gain_val = 0
            
        gain_str = f"+{int(gain_val):,}" if gain_val > 0 else str(int(gain_val))
        gain_color = '#4CAF50' if gain_val > 0 else '#BDBDBD'
        
        if is_club_log:
             pacing_values = [row['gainLast12h'], row['gainLast24h'], row['gainLast3d'], row['gainLast7d'], row['gainLast30d']]
        else:
            pacing_values = [row['proj12h'], row['proj24h'], row['proj3d'], row['proj7d'], row['projMonthEnd']]

        ax.text(header_positions[0], y_pos, timestamp_str, color='#E0E0E0', fontsize=12, transform=ax.transAxes, va='top')
        ax.text(header_positions[1], y_pos, time_diff_str, color=time_diff_color, fontsize=11, transform=ax.transAxes, va='top', ha='left')
        ax.text(header_positions[2], y_pos, gain_str, color=gain_color, fontsize=12, weight='bold', transform=ax.transAxes, ha='center', va='top')
        
        for i, val in enumerate(pacing_values):
            pacing_str = f"{int(val/1000):,}K" if val >= 1000 else str(int(val))
            ax.text(header_positions[i+3], y_pos, pacing_str, color='#E0E0E0', fontsize=11, transform=ax.transAxes, ha='center', va='top')

        y_pos -= (1 / (limit + 5))

    footer_text = "PACING columns show projected gain based on long-term performance." if not is_club_log else "PACING columns (12h, 24h, 3d, 7d, 30d) show the total fan gain in the trailing time period from the timestamp."
    fig.text(0.01, 0.01, footer_text, color='white', fontsize=9, style='italic', weight='bold', va='bottom')

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

    print("\n--- 2. Performing Core Analysis ---")
    
    fanlog_df = fanlog_df.sort_values(by=['inGameName', 'timestamp'])

    # --- Standard Fan Gain Calculation (between consecutive updates) ---
    fanlog_df['previousFanCount'] = fanlog_df.groupby('inGameName')['fanCount'].shift(1)
    fanlog_df['previousTimestamp'] = fanlog_df.groupby('inGameName')['timestamp'].shift(1)
    fanlog_df['is_first_entry'] = fanlog_df['previousTimestamp'].isnull()
    fanlog_df['fanGain'] = fanlog_df['fanCount'] - fanlog_df['previousFanCount']
    fanlog_df.loc[fanlog_df['is_first_entry'], 'fanGain'] = 0
    time_diff = (fanlog_df['timestamp'] - fanlog_df['previousTimestamp']).dt.total_seconds()
    fanlog_df['timeDiffMinutes'] = (time_diff / 60).fillna(0)

    # --- New Projection-Based Pacing Calculation ---
    # Get the first record for each member
    fanlog_df['firstTimestamp'] = fanlog_df.groupby('inGameName')['timestamp'].transform('first')
    fanlog_df['firstFanCount'] = fanlog_df.groupby('inGameName')['fanCount'].transform('first')

    # Calculate total time elapsed and gain from the very first entry
    total_time_diff = (fanlog_df['timestamp'] - fanlog_df['firstTimestamp']).dt.total_seconds()
    total_time_minutes = total_time_diff / 60
    total_fan_gain = fanlog_df['fanCount'] - fanlog_df['firstFanCount']

    # Calculate the long-term fans per minute rate
    fanlog_df['longTermFansPerMinute'] = (total_fan_gain / total_time_minutes).where(total_time_minutes > 0, 0)

    # Calculate future projections based on this rate
    fanlog_df['proj12h'] = fanlog_df['longTermFansPerMinute'] * 12 * 60
    fanlog_df['proj24h'] = fanlog_df['longTermFansPerMinute'] * 24 * 60
    fanlog_df['proj3d'] = fanlog_df['longTermFansPerMinute'] * 3 * 24 * 60
    fanlog_df['proj7d'] = fanlog_df['longTermFansPerMinute'] * 7 * 24 * 60
    
    # Calculate projection to the end of the month
    end_of_month_ts = fanlog_df['timestamp'].dt.to_period('M').dt.end_time
    minutes_to_eom = (end_of_month_ts - fanlog_df['timestamp']).dt.total_seconds() / 60
    fanlog_df['projMonthEnd'] = (fanlog_df['longTermFansPerMinute'] * minutes_to_eom).where(minutes_to_eom > 0, 0)
    
    print("  - Fan gain and projection analysis complete.")

    fan_gain_analysis_df = fanlog_df.copy()

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

            non_first_entries = member_logs[~member_logs['is_first_entry']]
            total_gain = non_first_entries['fanGain'].sum()
            total_time_minutes_summary = non_first_entries['timeDiffMinutes'].sum()
            avg_daily_gain = (total_gain / (total_time_minutes_summary / 1440)) if total_time_minutes_summary > 0 else 0

            summary_list.append({
                'inGameName': name, 'memberID': member['memberID'], 'latestUpdate': latest_update,
                'daysSinceUpdate': days_since_update, 'totalMonthlyGain': total_monthly_gain,
                'avgDailyGain': avg_daily_gain, 'gainLast7Days': gain_last_7_days, 'gainLast14Days': gain_last_14_days
            })

    member_summary_df = pd.DataFrame(summary_list)
    print("  - Member summary complete.")

    generate_visualizations(member_summary_df, fan_gain_analysis_df)

    output_gain_file = os.path.join(OUTPUT_DIR, 'fanGainAnalysis_output.csv')
    output_summary_file = os.path.join(OUTPUT_DIR, 'memberSummary_output.csv')
    
    # Save a clean version of the analysis to CSV
    csv_output_cols = [
        'timestamp', 'memberID', 'inGameName', 'fanCount', 'fanGain', 
        'timeDiffMinutes', 'longTermFansPerMinute', 'proj12h', 'proj24h', 
        'proj3d', 'proj7d', 'projMonthEnd'
    ]
    csv_output_df = fan_gain_analysis_df[csv_output_cols].copy()
    for col in csv_output_df.columns[4:]: # Round numeric columns
        csv_output_df[col] = csv_output_df[col].round().astype(int)

    csv_output_df.to_csv(output_gain_file, index=False)
    member_summary_df.to_csv(output_summary_file, index=False)
    
    print(f"\n--- ✅ Analysis Complete! ---")
    print(f"All reports have been saved to the '{OUTPUT_DIR}' folder.")

if __name__ == "__main__":
    main()
