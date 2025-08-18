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

# --- Configuration ---
MEMBERS_CSV = 'members.csv'
FANLOG_CSV = 'fan_log.csv'
OUTPUT_DIR = 'Club_Report_Output'

# Create a FontProperties object that points to your font file
try:
    myfont = fm.FontProperties(fname='D:/github/prettyDerbyClubAnalysis/fonts/25318.OTF')
    rankfont = fm.FontProperties(fname='D:/github/prettyDerbyClubAnalysis/fonts/industryultra.OTF')
except Exception as e:
    myfont = None
    rankfont = None
    print(f"Warning: Custom fonts not found, using defaults. Error: {e}")

# --- Chart Styling ---
plt.style.use('seaborn-v0_8-whitegrid')
try:
    font_path = fm.findfont('DejaVu Sans')
    font_prop = fm.FontProperties(fname=font_path)
    plt.rcParams['font.family'] = font_prop.get_name()
except:
    print("Clean font not found, using default. Charts will still be generated.")
plt.rcParams['figure.dpi'] = 150

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

def add_timestamps_to_fig(fig, generated_str):
    """Adds standardized timestamp footers to a matplotlib figure."""
    fig.text(0.92, 0.01, f"GENERATED: {generated_str}", color='white', fontsize=8, va='bottom', ha='right')

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


def generate_visualizations(summary_df, individual_log_df, club_log_df, contribution_df, historical_df, last_updated_str, generated_str, start_date, end_date, daily_summary_df):
    """Creates and saves all the requested charts and logs."""
    print("\n--- 3. Generating Visualizations ---")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # --- Generate Prestige Leaderboard ---
    if not individual_log_df.empty:
        generate_prestige_leaderboard(individual_log_df, last_updated_str, generated_str)

    # Monthly Leaderboard
    if not summary_df.empty:
        fig, ax = plt.subplots(figsize=(12, 8))
        top_10 = summary_df.nlargest(10, 'totalMonthlyGain').copy()

        sns.barplot(ax=ax, x='totalMonthlyGain', y='inGameName', data=top_10, palette='dark:#2E7D32', hue='inGameName', dodge=False)
        plt.legend([],[], frameon=False)

        ax.xaxis.set_major_formatter(lambda x, pos: f'{int(x/1000):,}K')

        for container in ax.containers:
            labels = [f'{int(v/1000):,}K' for v in container.datavalues]
            ax.bar_label(container, labels=labels, padding=5, fontsize=10, color='black')

        # --- MODIFIED TITLE AND NEW ANNOTATIONS ---
        plt.title(f'Top 10 Members by Monthly Fan Gain | Updated: {last_updated_str}', fontsize=15, weight='bold', loc='left')

        time_elapsed = (datetime.now(pytz.timezone('US/Central')) - start_date).total_seconds() / 3600
        total_duration = (end_date - start_date).total_seconds() / 3600
        time_remaining = total_duration - time_elapsed

        fig.text(0.125, 0.05, f"Time Elapsed: {time_elapsed:.1f} hrs ({time_elapsed / total_duration * 100:.1f}%)", ha='left', va='center', fontsize=10, style='italic', color='gray')
        fig.text(0.875, 0.05, f"Time Remaining: {time_remaining:.1f} hrs ({time_remaining / total_duration * 100:.1f}%)", ha='right', va='center', fontsize=10, style='italic', color='gray')
        # ---

        plt.xlabel('Total Fans Gained This Month', fontsize=12)
        plt.ylabel('Member', fontsize=14)
        ax.set_xlim(right=ax.get_xlim()[1] * 1.05)
        plt.tight_layout(rect=[0, 0.03, 0.975, 0.95]) # Adjust rect to make space for annotations
        add_timestamps_to_fig(fig, generated_str)
        plt.savefig(os.path.join(OUTPUT_DIR, 'monthly_leaderboard.png'))
        plt.close(fig)
        print("  - Saved monthly_leaderboard.png")
        
    # Fan Contribution Chart
    if not contribution_df.empty:
        generate_contribution_chart(contribution_df, last_updated_str, generated_str, start_date, end_date)

    # Club and Individual Logs
    if not club_log_df.empty:
        # This now correctly calls the function with the new daily club summary data.
        generate_log_image(club_log_df, f"Club Performance Summary | Updated {last_updated_str}", 'club_update_log.png', generated_str, limit=15, is_club_log=True)
        print("  - Saved club_update_log.png")

    # --- MODIFIED: This section now generates the new daily summary logs ---
    if not daily_summary_df.empty:
        all_members = summary_df.copy().sort_values('totalMonthlyGain', ascending=False)
        
        print("\n  - Generating DAILY SUMMARY individual logs...")
        for _, member_row in all_members.iterrows():
            member_name = member_row['inGameName']
            member_data = daily_summary_df[daily_summary_df['inGameName'] == member_name]
            if not member_data.empty:
                safe_member_name = member_name.replace(' ', '_').replace('/', '').replace('\\', '')
                filename = f"log_cumulative_{safe_member_name}.png"
                generate_log_image(member_data, f"Daily Performance Summary: {member_name} | Updated: {last_updated_str}", filename, generated_str, limit=15, is_club_log=False)
        print(f"  - Saved DAILY SUMMARY logs for {len(all_members)} members.")
    
    # --- MODIFIED: Historical Tables ---
    if not historical_df.empty:
        generate_performance_heatmap(historical_df, summary_df, "fan_performance_heatmap.png", generated_str, last_updated_str, start_date, end_date)
        

def generate_prestige_leaderboard(individual_log_df, last_updated_str, generated_str):
    """Creates and saves the prestige leaderboard chart with custom styling."""
    print("  - Generating prestige_leaderboard.png")

    # --- 1. Data Preparation ---
    latest_prestige = individual_log_df.loc[individual_log_df.groupby('inGameName')['timestamp'].idxmax()]
    top_15 = latest_prestige.nlargest(30, 'cumulativePrestige').sort_values('cumulativePrestige', ascending=True)

    # --- 2. Custom Color Mapping ---
    rank_colors = {
        "Local Newcomer": "#ebd3b4", "Track Regular": "#f6bf83", "Podium Finisher": "#ec9130",
        "Stakes Contender": "#7c95e0", "Derby Winner": "#446bdf", "Grand Prix Champion": "#0833b4",
        "Grand Cup Holder": "#a7e296", "Champion Cup Holder": "#8ef172", "Triple Crown Winner": "#37e606",
        "Hall of Fame Inductee": "#e4d00a", "Racing Legend": "#9c27b0", "The Founder's Idol": "#db1616"
    }
    bar_colors = top_15['prestigeRank'].map(rank_colors).fillna('grey')

    # --- 3. Chart Styling ---
    fig, ax = plt.subplots(figsize=(12, 10))
    fig.patch.set_facecolor('#2E2E2E')
    ax.set_facecolor('#2E2E2E')

    bars = ax.barh(
        y=[f"{name} ({rank})" for name, rank in zip(top_15['inGameName'], top_15['prestigeRank'])],
        width=top_15['cumulativePrestige'],
        color=bar_colors
    )
    
    ax.bar_label(bars, labels=[f"{int(p):,}" for p in top_15['cumulativePrestige']], padding=5, color='white', fontsize=11, weight='bold')
    
    ax.set_xlabel('Total Prestige Points', fontsize=12, color='white')
    ax.set_ylabel('Member', fontsize=12, color='white')
    ax.set_title(f'Prestige Leaderboard | Updated: {last_updated_str}', fontsize=18, weight='bold', loc='left', color='white')

    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white', labelsize=11)
    
    ax.grid(axis='x', linestyle='--', alpha=0.5)
    ax.grid(axis='y', linestyle='', alpha=0)
    
    # --- 4. Next Rank Line ---
    ranks_df = pd.read_csv('ranks.csv')
    highest_rank_on_chart = top_15['prestigeRank'].iloc[-1]
    
    current_rank_index = ranks_df[ranks_df['rank_name'] == highest_rank_on_chart].index
    if not current_rank_index.empty and current_rank_index[0] + 1 < len(ranks_df):
        next_rank_info = ranks_df.iloc[current_rank_index[0] + 1]
        next_rank_name = next_rank_info['rank_name']
        next_rank_req = next_rank_info['prestige_required']
        
        ax.axvline(x=next_rank_req, color='yellow', linestyle='--', linewidth=2)
        ax.text(next_rank_req, -0.9, f"Next Rank:\n {next_rank_name} ({next_rank_req:,} Pts)", 
                color='yellow', ha='right', va='bottom', fontsize=10, weight='bold')

    plt.tight_layout(rect=[0.01, 0.01, 0.99, 0.95])
    add_timestamps_to_fig(fig, generated_str)
    plt.savefig(os.path.join(OUTPUT_DIR, 'prestige_leaderboard.png'), facecolor=fig.get_facecolor())
    plt.close(fig)
    print("  - Saved prestige_leaderboard.png")


def generate_member_summary(summary_df, individual_log_df, start_date, end_date, generated_str):
    """Generates a summary table of member performance as both a CSV and a styled image."""
    print("  - Generating member performance summary...")

    # --- 1. Data Calculation ---
    summary_list = []
    for _, member in summary_df.iterrows():
        name = member['inGameName']
        member_logs = individual_log_df[individual_log_df['inGameName'] == name]

        if not member_logs.empty:
            first_log = member_logs.iloc[0]
            latest_log = member_logs.iloc[-1]

            fan_contribution = latest_log['fanCount'] - first_log['fanCount']

            hrs_elapsed = (latest_log['timestamp'] - first_log['timestamp']).total_seconds() / 3600
            fans_per_hour = fan_contribution / hrs_elapsed if hrs_elapsed > 0 else 0

            hrs_remaining = (end_date - latest_log['timestamp']).total_seconds() / 3600

            month_end_projection = fan_contribution + (fans_per_hour * hrs_remaining)

            summary_list.append({
                'inGameName': name,
                'first_update': first_log['timestamp'],
                'last_update': latest_log['timestamp'],
                'hrs_elapsed': hrs_elapsed,
                'hrs_remaining': hrs_remaining,
                'fan_contribution': fan_contribution,
                'fans_per_hr': fans_per_hour,
                'month_end_proj': month_end_projection
            })

    summary_table = pd.DataFrame(summary_list).sort_values('month_end_proj', ascending=False).reset_index(drop=True)

    # --- 2. Save to CSV ---
    csv_path = os.path.join(OUTPUT_DIR, 'member_performance_summary.csv')
    summary_table.to_csv(csv_path, index=False)
    print(f"  - Saved {csv_path}")

    # --- 3. Generate Image ---
    # Create a formatted version for the image
    formatted_table = summary_table.copy()
    formatted_table['first_update'] = formatted_table['first_update'].dt.strftime('%m/%d %H:%M')
    formatted_table['last_update'] = formatted_table['last_update'].dt.strftime('%m/%d %H:%M')
    formatted_table['hrs_elapsed'] = formatted_table['hrs_elapsed'].map('{:,.1f}'.format)
    formatted_table['hrs_remaining'] = formatted_table['hrs_remaining'].map('{:,.1f}'.format)
    formatted_table['fan_contribution'] = formatted_table['fan_contribution'].map('{:,.0f}'.format)
    formatted_table['fans_per_hr'] = formatted_table['fans_per_hr'].map('{:,.0f}'.format)
    formatted_table['month_end_proj'] = formatted_table['month_end_proj'].map('{:,.0f}'.format)

    # Rename columns for display
    formatted_table.rename(columns={
        'inGameName': 'Member', 'first_update': 'First Update', 'last_update': 'Last Update',
        'hrs_elapsed': 'Hrs Elapsed', 'hrs_remaining': 'Hrs Remaining', 'fan_contribution': 'Fan Contribution',
        'fans_per_hr': 'Fans/Hr', 'month_end_proj': 'Month-End Proj.'
    }, inplace=True)

    # Rendering logic
    fig, ax = plt.subplots(figsize=(16, max(6, len(formatted_table) * 0.5)))
    ax.axis('off')
    fig.patch.set_facecolor('#2E2E2E')

    table = ax.table(cellText=formatted_table.values,
                     colLabels=formatted_table.columns,
                     cellLoc='center',
                     loc='center',
                     colWidths=[0.2, 0.1, 0.1, 0.1, 0.1, 0.15, 0.1, 0.15])
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)

    for (i, j), cell in table.get_celld().items():
        cell.set_text_props(color='white' if i==0 else 'black')
        cell.set_facecolor('#40466e' if i==0 else ('#f2f2f2' if (i-1) % 2 == 0 else 'white'))

    plt.title('Member Performance Summary', color='white', fontsize=18, weight='bold', y=0.95)
    add_timestamps_to_fig(fig, generated_str)

    img_path = os.path.join(OUTPUT_DIR, 'member_performance_summary.png')
    plt.savefig(img_path, bbox_inches='tight', pad_inches=0.4, facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  - Saved {img_path}")


def generate_performance_heatmap(historical_df, summary_df, filename, generated_str, last_updated_str, start_date, end_date):
    """Generates the historical performance heatmap for Club and Rank Groups."""
    print("  - Generating historical performance heatmap...")
    
    # --- Data Preparation ---
    summary_df = summary_df.sort_values('totalMonthlyGain', ascending=False).copy()
    summary_df['rank'] = range(1, len(summary_df) + 1)
    
    historical_df = pd.merge(historical_df, summary_df[['inGameName', 'rank']], on='inGameName')
    
    bins = [0, 6, 12, 18, 24, 30]
    labels = ['Ranks 1-6', 'Ranks 7-12', 'Ranks 13-18', 'Ranks 19-24', 'Ranks 25-30']
    historical_df['Rank Group'] = pd.cut(historical_df['rank'], bins=bins, labels=labels, right=True)

    # Create Club and Rank data pivot table
    club_total = historical_df.groupby('time_group')['fanGain'].sum()
    rank_groups = historical_df.groupby(['time_group', 'Rank Group'], observed=True)['fanGain'].sum().unstack()
    data_to_plot = pd.concat([pd.DataFrame({'Club': club_total}), rank_groups], axis=1).T.fillna(0)
    
    
    # --- Pacing Calculations ---
    cumulative_gain_before = club_total.cumsum().shift(1).fillna(0)
    time_since_start_before = (club_total.index - start_date).total_seconds() / 3600
    cumulative_rate_before = (cumulative_gain_before / time_since_start_before).where(time_since_start_before > 0, 0)
    
    cumulative_gain_through = club_total.cumsum()
    time_since_start_through = (club_total.index + pd.Timedelta(hours=8) - start_date).total_seconds() / 3600
    cumulative_rate_through = (cumulative_gain_through / time_since_start_through).where(time_since_start_through > 0, 0)
    
    total_month_hours = (end_date - start_date).total_seconds() / 3600

    projected_window_gain = (cumulative_rate_before * 8).rename("This Window Proj.")
    projected_eom_gain = (cumulative_rate_through * total_month_hours).rename("End-of-Month Proj.")
    
    data_to_plot = pd.concat([data_to_plot, pd.DataFrame(projected_window_gain).T, pd.DataFrame(projected_eom_gain).T])
    
    # Select only the last 13 columns (time periods) for the plot
    if len(data_to_plot.columns) > 13:
        data_to_plot = data_to_plot.iloc[:, -13:]

    
    # --- Rendering ---
    formatted_data = data_to_plot.map(lambda x: f"{x/1000:,.0f}")

    time_cols = [col for col in data_to_plot.columns if isinstance(col, pd.Timestamp)]
    
    time_labels = [f"{dt.strftime('%m/%d')}\n{dt.strftime('%H:%M')}-{(dt + pd.Timedelta(hours=7, minutes=59)).strftime('%H:%M')}" for dt in time_cols]
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(max(15, len(time_labels) * 1.2), 10), 
                                   gridspec_kw={'height_ratios': [len(data_to_plot)-2, 2], 'hspace': 0.1})

    # Top Heatmap (Club & Ranks)
    sns.heatmap(data_to_plot.iloc[:-2] / 1000, ax=ax1, annot=formatted_data.iloc[:-2], fmt="s", cmap="Greens", cbar=False, linewidths=.5, linecolor='lightgray', annot_kws={"size": 14})
    ax1.set_ylabel('')
    ax1.set_xlabel('')
    ax1.set_xticklabels(time_labels, rotation=0)
    ax1.xaxis.tick_top()
    ax1.tick_params(axis='x', length=0)
    ax1.tick_params(axis='y', rotation=0, labelsize=13)
    
    # Bottom Heatmap (Pacing)
    sns.heatmap(data_to_plot.iloc[-2:] / 1000, ax=ax2, annot=formatted_data.iloc[-2:], fmt="s", cmap="Blues", cbar=False, linewidths=.5, linecolor='lightgray', annot_kws={"size": 14})
    ax2.set_ylabel('')
    ax2.set_xlabel('')
    ax2.set_xticklabels([])
    ax2.tick_params(axis='x', length=0)
    ax2.tick_params(axis='y', rotation=0, labelsize=13)

    # Add a thick line between the two heatmaps
    ax1.axhline(y=0, color='black', linewidth=2)
    ax1.axhline(y=len(data_to_plot)-2, color='black', linewidth=2)
    
    fig.suptitle(f'Historical Fan Gains (8-Hour Intervals) | Updated: {last_updated_str}', fontsize=18, y=0.98, ha='left', x=0.05)
    fig.text(0.05, 0.945, "Cell values are actual Fan Gains in 1000s.", ha='left', va='center', fontsize=10, style='italic', color='gray')
             
    add_timestamps_to_fig(fig, generated_str)
    
    plt.savefig(os.path.join(OUTPUT_DIR, filename), bbox_inches='tight', pad_inches=0.3)
    plt.close(fig)
    print(f"  - Saved {filename}")


def generate_contribution_chart(contribution_df, last_updated_str, generated_str, start_date, end_date):
    """Generates the fan contribution by rank group stacked bar chart."""
    print("  - Generating fan contribution chart...")
    
    fig, ax = plt.subplots(figsize=(28, 8))
    
    percentages = contribution_df['percentage']
    my_hex_colors = ['#d7191c', '#fdae61', '#ffffbf', '#abd9e9', '#2c7bb6'] 
    colors = my_hex_colors[:len(percentages)]

    left = 0
    for i, (label, percentage) in enumerate(percentages.items()):
        ax.barh('Monthly Fan Gain', percentage, left=left, label=label, color=colors[i], edgecolor='white')
        
        if percentage > 2:
            ax.text(left + percentage / 2, 0, f'{percentage:.0f}%', ha='center', va='center', color='white', weight='bold', fontsize=20, path_effects=[pe.withStroke(linewidth=4, foreground='black')])
        left += percentage

    ax.set_xlim(0, 100)
    ax.set_xticks(np.arange(0, 101, 10))
    ax.set_xticklabels([f'{x}%' for x in np.arange(0, 101, 10)])
    ax.tick_params(axis='x', labelsize=24)
    ax.set_yticklabels([])
    ax.set_ylabel('')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)

    plt.title(f'Monthly Fan Contribution by Rank Group | Updated: {last_updated_str}', fontsize=24, weight='bold', loc='left', y=1.075)
    ax.legend(title='', bbox_to_anchor=(0.5, 1.075), loc='upper center', ncol=len(percentages), fontsize=16)
    
    time_elapsed = (datetime.now(pytz.timezone('US/Central')) - start_date).total_seconds() / 3600
    time_remaining = (end_date - datetime.now(pytz.timezone('US/Central'))).total_seconds() / 3600
    
    plt.tight_layout(rect=[0, 0.05, 0.95, 0.95])
    add_timestamps_to_fig(fig, generated_str)
    plt.savefig(os.path.join(OUTPUT_DIR, 'fan_contribution_by_rank.png'))
    plt.close(fig)
    print("  - Saved fan_contribution_by_rank.png")


def generate_log_image(log_data, title, filename, generated_str, limit=15, is_club_log=False):
    """Generates and saves a CML-style log as an image from pre-processed daily summary data."""
    
    log_data_limited = log_data.sort_values('timestamp', ascending=False).head(limit)
    if log_data_limited.empty: return

    fig, ax = plt.subplots(figsize=(16, 10))
    fig.patch.set_facecolor('#2E2E2E')
    ax.set_facecolor('#2E2E2E')
    ax.set_title(title, color='white', loc='left', pad=20, fontproperties=rankfont, fontsize=16)

    latest_entry = log_data_limited.iloc[0] # Get the most recent day's data for the header

    # --- START: Add Prestige Header back ---
    if not is_club_log:
        rank = latest_entry.get('rank')
        if pd.notna(rank):
            ax.text(1, 1.025, f"RANK {int(rank)}", color='#FFD700', fontsize=14, transform=ax.transAxes, ha='right', va='bottom', fontproperties=rankfont)

        prestige_rank = latest_entry['prestigeRank']
        total_prestige = latest_entry['cumulativePrestige']
        points_to_next = latest_entry['pointsToNextRank']
        
        header_y = 1.015
        ax.text(0.01, header_y, "Prestige Rank:", color='#A0A0A0', fontsize=10, weight='bold', transform=ax.transAxes, va='top')
        ax.text(0.11, header_y, prestige_rank, color='white', fontsize=12, transform=ax.transAxes, va='top')
        
        ax.text(0.36, header_y, "Total Prestige:", color='#A0A0A0', fontsize=10, weight='bold', transform=ax.transAxes, va='top')
        ax.text(0.47, header_y, f"{total_prestige:,.0f} Prestige", color='white', fontsize=12, transform=ax.transAxes, va='top')
        
        ax.text(0.71, header_y, "Next Rank:", color='#A0A0A0', fontsize=10, weight='bold', transform=ax.transAxes, va='top')
        if pd.notna(points_to_next):
             ax.text(0.80, header_y, f"{points_to_next:,.0f} Points", color='white', fontsize=12, transform=ax.transAxes, va='top')
        else:
             ax.text(0.80, header_y, "Max Rank", color='white', fontsize=12, transform=ax.transAxes, va='top')
    # --- END: Add Prestige Header back ---
    
    if is_club_log:
        headers = ['Timestamp (CT)', "Day's Fan Gain", "Month's Fans", 'Rank', 'Rank Δ', 'Fans to Rank 100', 'Month Pacing', 'Prestige Gain']
    else:
        headers = ['Timestamp (CT)', "Day's Fan Gain", "Month's Fans", 'Rank', 'Rank Δ', 'Fans to Next Rank', 'Month Pacing', 'Prestige Gain']
        
    header_positions = [0.01, 0.22, 0.36, 0.45, 0.53, 0.65, 0.78, 0.90]

    for i, header in enumerate(headers):
        ax.text(header_positions[i], 0.935, header, color='#A0A0A0', fontsize=10, weight='bold', transform=ax.transAxes, va='top', ha='left' if i < 1 else 'center')

    y_pos = 0.91
    for _, row in log_data_limited.iterrows():
        hour = row['timestamp'].strftime('%I').lstrip('0') or '12'
        timestamp_str = f"{hour}:{row['timestamp'].strftime('%M %p %m/%d/%Y')}"
        gain_val = row['dailyFanGain']
        gain_str = f"+{int(gain_val):,}" if gain_val > 0 else str(int(gain_val))
        gain_color = '#4CAF50' if gain_val > 0 else '#BDBDBD'
        month_fans_str = f"{int(row['monthlyFanGain']):,}"
        
        rank_str = f"#{int(row['rank'])}" if isinstance(row['rank'], (int, float)) and pd.notna(row['rank']) else '-'
        rank_delta = row['rank_delta']
        if pd.isna(rank_delta) or rank_delta == 0 or isinstance(rank_delta, str):
            delta_str, delta_color = '-', '#BDBDBD'
        elif rank_delta > 0:
            delta_str, delta_color = f"↑{int(rank_delta)}", '#4CAF50'
        else:
            delta_str, delta_color = f"↓{int(abs(rank_delta))}", '#F44336'

        fans_to_next = row['fansToNextRank']
        if pd.isna(fans_to_next) or isinstance(fans_to_next, str) or fans_to_next <= 0:
            fans_next_str = '-'
        else:
            fans_next_str = f"{int(fans_to_next/1000):,}K"

        pacing_str = f"{int(row['monthPacing']/1000):,}K"
        prestige_gain_str = f"+{row['dailyPrestigeGain']:.1f}"

        ax.text(header_positions[0], y_pos, timestamp_str, color='#E0E0E0', fontsize=12, transform=ax.transAxes, va='top')
        ax.text(header_positions[1], y_pos, gain_str, color=gain_color, fontsize=12, weight='bold', transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[2], y_pos, month_fans_str, color='#E0E0E0', fontsize=12, transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[3], y_pos, rank_str, color='#E0E0E0', fontsize=12, transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[4], y_pos, delta_str, color=delta_color, fontsize=11, weight='bold', transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[5], y_pos, fans_next_str, color='#E0E0E0', fontsize=11, transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[6], y_pos, pacing_str, color='#64B5F6', fontsize=12, weight='bold', transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[7], y_pos, prestige_gain_str, color='#FFD700', fontsize=11, transform=ax.transAxes, ha='center', va='top')

        y_pos -= (1 / (limit + 5))

    add_timestamps_to_fig(fig, generated_str)
    
    ax.axis('off')
    os.makedirs(os.path.join(OUTPUT_DIR, "individual_logs"), exist_ok=True)
    plt.savefig(os.path.join(OUTPUT_DIR, "individual_logs", filename), bbox_inches='tight', pad_inches=0.3, facecolor=fig.get_facecolor())
    plt.close(fig)

def main():
    """Main function to run the entire analysis pipeline."""
    print("--- 1. Loading and Cleaning Data ---")
    try:
        members_df = pd.read_csv(MEMBERS_CSV)
        fanlog_df = pd.read_csv(FANLOG_CSV)
        ranks_df = pd.read_csv('ranks.csv')
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
    generate_visualizations(
        member_summary_df,
        individual_log_df,
        daily_club_summary_df,
        contribution_df,
        historical_df,
        last_updated_str,
        generated_str,
        start_date,
        end_date,
        daily_summary_df # --- MODIFIED: Pass the new dataframe ---
    )

    if not member_summary_df.empty and not individual_log_df.empty:
        generate_member_summary(member_summary_df, individual_log_df, start_date, end_date, generated_str)

    output_gain_file = os.path.join(OUTPUT_DIR, 'fanGainAnalysis_output.csv')
    output_summary_file = os.path.join(OUTPUT_DIR, 'memberSummary_output.csv')
    
    # --- This part needs to be updated to join with ranks and other details if needed ---
    final_csv_df = pd.merge(individual_log_df, members_df[['inGameName', 'memberID']], on='inGameName', how='left')
    
    # --- Adding rank details to the final CSV output ---
    latest_ranks = daily_summary_df.loc[daily_summary_df.groupby('inGameName')['timestamp'].idxmax()][['inGameName', 'rank']]
    final_csv_df = pd.merge(final_csv_df, latest_ranks, on='inGameName', how='left')
    
    csv_output_cols = [
        'timestamp', 'memberID', 'inGameName', 'fanCount', 'fanGain',
        'timeDiffMinutes', 'performancePrestigePoints', 'tenurePrestigePoints',
        'cumulativePrestige', 'prestigeRank', 'pointsToNextRank', 'rank' 
    ]
    
    final_csv_df = final_csv_df[csv_output_cols]

    numeric_cols_to_format = ['fanGain', 'timeDiffMinutes', 'performancePrestigePoints', 'tenurePrestigePoints', 'cumulativePrestige']
    for col in numeric_cols_to_format:
        if col in final_csv_df.columns:
            final_csv_df[col] = pd.to_numeric(final_csv_df[col], errors='coerce').fillna(0)
            if col in ['fanGain', 'timeDiffMinutes']:
                 final_csv_df[col] = final_csv_df[col].astype(int)
            else:
                 final_csv_df[col] = final_csv_df[col].round(2)

    final_csv_df.to_csv(output_gain_file, index=False)
    member_summary_df.to_csv(output_summary_file, index=False)

    print(f"\n--- Analysis Complete! ---")
    print(f"All reports have been saved to the '{OUTPUT_DIR}' folder.")

if __name__ == "__main__":
    main()