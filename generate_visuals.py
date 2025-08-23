import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
from datetime import datetime
import numpy as np
import os
import pytz
import matplotlib.patheffects as pe

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

def add_timestamps_to_fig(fig, generated_str):
    """Adds standardized timestamp footers to a matplotlib figure."""
    fig.text(0.92, 0.01, f"GENERATED: {generated_str}", color='white', fontsize=8, va='bottom', ha='right')


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
    top_15 = latest_prestige.nlargest(30, 'monthlyPrestige').sort_values('monthlyPrestige', ascending=True)

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
        width=top_15['monthlyPrestige'],
        color=bar_colors
    )

    ax.bar_label(bars, labels=[f"{int(p):,}" for p in top_15['monthlyPrestige']], padding=5, color='white', fontsize=11, weight='bold')

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

    # --- Use a wider figure for individual logs with CC data ---
    fig_width = 24 if not is_club_log else 16
    fig, ax = plt.subplots(figsize=(fig_width, 10))
    fig.patch.set_facecolor('#2E2E2E')
    ax.set_facecolor('#2E2E2E')
    ax.set_title(title, color='white', loc='left', pad=20, fontproperties=rankfont, fontsize=16)

    latest_entry = log_data_limited.iloc[0]

    if not is_club_log:
        rank = latest_entry.get('rank')
        if pd.notna(rank):
            ax.text(1, 1.025, f"RANK {int(rank)}", color='#FFD700', fontsize=14, transform=ax.transAxes, ha='right', va='bottom', fontproperties=rankfont)

        prestige_rank = latest_entry['prestigeRank']
        total_prestige = latest_entry['monthlyPrestige']
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

    if is_club_log:
        headers = ['Timestamp (CT)', "Day's Fan Gain", "Month's Fans", 'Rank', 'Rank Δ', 'Fans to Rank 100', 'Month Pacing', 'Prestige Gain']
        header_positions = [0.01, 0.22, 0.36, 0.45, 0.53, 0.65, 0.78, 0.90]
    else:
        headers = ['Timestamp (CT)', "Day's Fan Gain", "Month's Fans", 'Rank', 'Rank Δ', 'Fans to Next', 'Pacing', 'Prestige',
                   'Perf. Yield', 'Tenure Yield', 'Hype Yield', 'Dividends', 'Total CC']
        header_positions = [0.01, 0.16, 0.25, 0.32, 0.37, 0.43, 0.49, 0.55,
                            0.62, 0.70, 0.78, 0.86, 0.94]


    for i, header in enumerate(headers):
        ax.text(header_positions[i], 0.935, header, color='#A0A0A0', fontsize=10, weight='bold', transform=ax.transAxes, va='top', ha='left' if i < 1 else 'center')

    y_pos = 0.91
    for _, row in log_data_limited.iterrows():
        # --- Existing Data ---
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


        # --- Existing Text Rendering ---
        ax.text(header_positions[0], y_pos, timestamp_str, color='#E0E0E0', fontsize=12, transform=ax.transAxes, va='top')
        ax.text(header_positions[1], y_pos, gain_str, color=gain_color, fontsize=12, weight='bold', transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[2], y_pos, month_fans_str, color='#E0E0E0', fontsize=12, transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[3], y_pos, rank_str, color='#E0E0E0', fontsize=12, transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[4], y_pos, delta_str, color=delta_color, fontsize=11, weight='bold', transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[5], y_pos, fans_next_str, color='#E0E0E0', fontsize=11, transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[6], y_pos, pacing_str, color='#64B5F6', fontsize=12, weight='bold', transform=ax.transAxes, ha='center', va='top')
        ax.text(header_positions[7], y_pos, prestige_gain_str, color='#FFD700', fontsize=11, transform=ax.transAxes, ha='center', va='top')

        # --- FIX: Only render CC data for individual logs ---
        if not is_club_log:
            perf_yield_str = f"{row.get('performance_yield', 0):.0f}"
            tenure_yield_str = f"{row.get('tenure_yield', 0):.0f}"
            hype_yield_str = f"{row.get('hype_bonus_yield', 0):.0f}"
            dividend_str = f"{row.get('sponsorship_dividend_received', 0):.0f}"
            total_cc_str = f"{row.get('total_period_earnings', 0):.0f}"

            ax.text(header_positions[8], y_pos, perf_yield_str, color='#AED581', fontsize=11, transform=ax.transAxes, ha='center', va='top')
            ax.text(header_positions[9], y_pos, tenure_yield_str, color='#AED581', fontsize=11, transform=ax.transAxes, ha='center', va='top')
            ax.text(header_positions[10], y_pos, hype_yield_str, color='#AED581', fontsize=11, transform=ax.transAxes, ha='center', va='top')
            ax.text(header_positions[11], y_pos, dividend_str, color='#AED581', fontsize=11, transform=ax.transAxes, ha='center', va='top')
            ax.text(header_positions[12], y_pos, total_cc_str, color='#FFFFFF', fontsize=12, weight='bold', transform=ax.transAxes, ha='center', va='top')


        y_pos -= (1 / (limit + 5))

    add_timestamps_to_fig(fig, generated_str)

    ax.axis('off')
    os.makedirs(os.path.join(OUTPUT_DIR, "individual_logs"), exist_ok=True)
    plt.savefig(os.path.join(OUTPUT_DIR, "individual_logs", filename), bbox_inches='tight', pad_inches=0.3, facecolor=fig.get_facecolor())
    plt.close(fig)

def create_all_visuals(members_df, summary_df, individual_log_df, club_log_df, contribution_df, historical_df, last_updated_str, generated_str, start_date, end_date, daily_summary_df):
    """
    Main function to create and save all visualizations.
    This function is called by analysis.py after the main data processing is complete.
    """
    generate_visualizations(
        summary_df,
        individual_log_df,
        club_log_df,
        contribution_df,
        historical_df,
        last_updated_str,
        generated_str,
        start_date,
        end_date,
        daily_summary_df
    )

    if not summary_df.empty and not individual_log_df.empty:
        generate_member_summary(summary_df, individual_log_df, start_date, end_date, generated_str)

        print("  - Generating final CSV reports...")

    output_gain_file = os.path.join(OUTPUT_DIR, 'fanGainAnalysis_output.csv')
    output_summary_file = os.path.join(OUTPUT_DIR, 'memberSummary_output.csv')

    final_csv_df = pd.merge(individual_log_df, members_df[['inGameName', 'memberID']], on='inGameName', how='left')

    latest_ranks = daily_summary_df.loc[daily_summary_df.groupby('inGameName')['timestamp'].idxmax()][['inGameName', 'rank']]
    final_csv_df = pd.merge(final_csv_df, latest_ranks, on='inGameName', how='left')

    csv_output_cols = [
        'timestamp', 'memberID', 'inGameName', 'fanCount', 'fanGain',
        'timeDiffMinutes', 'performancePrestigePoints', 'tenurePrestigePoints',
        'monthlyPrestige', 'prestigeRank', 'pointsToNextRank', 'rank'
    ]

    # Ensure all expected columns exist before trying to select them
    for col in csv_output_cols:
        if col not in final_csv_df.columns:
            final_csv_df[col] = np.nan # Add missing columns with a default value

    final_csv_df = final_csv_df[csv_output_cols]


    numeric_cols_to_format = ['fanGain', 'timeDiffMinutes', 'performancePrestigePoints', 'tenurePrestigePoints', 'monthlyPrestige']
    for col in numeric_cols_to_format:
        if col in final_csv_df.columns:
            final_csv_df[col] = pd.to_numeric(final_csv_df[col], errors='coerce').fillna(0)
            if col in ['fanGain', 'timeDiffMinutes']:
                 final_csv_df[col] = final_csv_df[col].astype(int)
            else:
                 final_csv_df[col] = final_csv_df[col].round(2)

    final_csv_df.to_csv(output_gain_file, index=False)
    summary_df.to_csv(output_summary_file, index=False)

    print(f"  - Saved {output_gain_file}")
    print(f"  - Saved {output_summary_file}")

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
def main():
    """
    Main function to load enriched data and generate all visual and CSV outputs.
    This script is now independent and reads from the 'Golden Record'.
    """
    print("--- 1. Loading Data for Visualization ---")
    try:
        members_df = pd.read_csv('members.csv')
        ranks_df = pd.read_csv('ranks.csv')
        individual_log_df = pd.read_csv('enriched_fan_log.csv')
        individual_log_df['timestamp'] = pd.to_datetime(individual_log_df['timestamp'])
        # --- FIX: Create the 'date' column for merging ---
        individual_log_df['date'] = individual_log_df['timestamp'].dt.date

        balance_history_df = pd.read_csv('market/balance_history.csv')
        balance_history_df['timestamp'] = pd.to_datetime(balance_history_df['timestamp'])
        balance_history_df['date'] = balance_history_df['timestamp'].dt.date

        print("  - Successfully loaded enriched fan log and supporting files.")
    except FileNotFoundError as e:
        print(f"FATAL ERROR: Missing data file {e}. Cannot generate visuals.")
        return

    print("--- 2. Aggregating Data for Reports ---")
    central_tz = pytz.timezone('US/Central')
    generation_ct = datetime.now(central_tz)
    start_date, end_date = get_club_month_window(generation_ct)
    last_updated_ct = individual_log_df['timestamp'].max()
    last_updated_str = last_updated_ct.strftime('%Y-%m-%d %I:%M %p %Z')
    generated_str = generation_ct.strftime('%Y-%m-%d %I:%M %p %Z')

    daily_cc_summary_df = balance_history_df.groupby(['inGameName', 'date']).agg(
        performance_yield=('performance_yield', 'sum'),
        tenure_yield=('tenure_yield', 'sum'),
        hype_bonus_yield=('hype_bonus_yield', 'sum'),
        sponsorship_dividend_received=('sponsorship_dividend_received', 'sum'),
        total_period_earnings=('total_period_earnings', 'sum')
    ).reset_index()

    daily_summary_df = individual_log_df.groupby(['inGameName', 'date']).agg(
        dailyFanGain=('fanGain', 'sum'),
        dailyPrestigeGain=('prestigeGain', 'sum'),
        timestamp=('timestamp', 'last')
    ).reset_index()
    prestige_info = individual_log_df.loc[individual_log_df.groupby(['inGameName', 'date'])['timestamp'].idxmax()][
        ['inGameName', 'date', 'monthlyPrestige', 'prestigeRank', 'pointsToNextRank']
    ]
    daily_summary_df = pd.merge(daily_summary_df, prestige_info, on=['inGameName', 'date'])

    daily_summary_df = pd.merge(daily_summary_df, daily_cc_summary_df, on=['inGameName', 'date'], how='left')
    cc_cols = ['performance_yield', 'tenure_yield', 'hype_bonus_yield', 'sponsorship_dividend_received', 'total_period_earnings']
    daily_summary_df[cc_cols] = daily_summary_df[cc_cols].fillna(0)


    daily_summary_df['timestamp'] = pd.to_datetime(daily_summary_df['timestamp']) # Ensure datetime type
    daily_summary_df = daily_summary_df.sort_values(by=['inGameName', 'date'])
    daily_summary_df['monthlyFanGain'] = daily_summary_df.groupby('inGameName')['dailyFanGain'].cumsum()
    daily_summary_df['rank'] = daily_summary_df.groupby('date')['monthlyFanGain'].rank(method='dense', ascending=False)
    daily_summary_df['previous_rank'] = daily_summary_df.groupby('inGameName')['rank'].shift(1)
    daily_summary_df['rank_delta'] = daily_summary_df['previous_rank'] - daily_summary_df['rank']
    def get_fans_to_next(df):
        df = df.sort_values('rank')
        df['next_rank_fans'] = df['monthlyFanGain'].shift(1)
        df['fansToNextRank'] = df['next_rank_fans'] - df['monthlyFanGain'] + 1
        return df
    daily_summary_df = daily_summary_df.groupby('date', group_keys=False).apply(get_fans_to_next)
    time_elapsed_hrs = (daily_summary_df['timestamp'] - start_date).dt.total_seconds() / 3600
    time_remaining_hrs = (end_date - daily_summary_df['timestamp']).dt.total_seconds() / 3600
    fans_per_hour = (daily_summary_df['monthlyFanGain'] / time_elapsed_hrs).replace([np.inf, -np.inf], 0).fillna(0)
    daily_summary_df['monthPacing'] = daily_summary_df['monthlyFanGain'] + (fans_per_hour * time_remaining_hrs)

    daily_club_summary_list = []
    club_daily_groups = individual_log_df.groupby('date')
    for date, group in club_daily_groups:
        latest_entry = group.loc[group['timestamp'].idxmax()]
        daily_fan_gain = group['fanGain'].sum()
        daily_prestige_gain = group['prestigeGain'].sum()
        club_month_to_date = individual_log_df[individual_log_df['date'] <= date]
        monthly_fan_gain = club_month_to_date['fanGain'].sum()
        time_elapsed_hrs = (latest_entry['timestamp'] - start_date).total_seconds() / 3600
        time_remaining_hrs = (end_date - latest_entry['timestamp']).total_seconds() / 3600
        fans_per_hour = monthly_fan_gain / time_elapsed_hrs if time_elapsed_hrs > 0 else 0
        month_pacing = monthly_fan_gain + (fans_per_hour * time_remaining_hrs)
        daily_club_summary_list.append({
            'timestamp': latest_entry['timestamp'], 'inGameName': 'Club Total', 'dailyFanGain': daily_fan_gain,
            'monthlyFanGain': monthly_fan_gain, 'rank': '-', 'rank_delta': '-', 'fansToNextRank': '-',
            'monthPacing': month_pacing, 'dailyPrestigeGain': daily_prestige_gain
        })
    daily_club_summary_df = pd.DataFrame(daily_club_summary_list)

    summary_df = daily_summary_df.loc[daily_summary_df.groupby('inGameName')['timestamp'].idxmax()].copy()
    summary_df.rename(columns={'monthlyFanGain': 'totalMonthlyGain'}, inplace=True)

    summary_with_ranks = summary_df.sort_values('totalMonthlyGain', ascending=False).copy()
    summary_with_ranks['rank'] = range(1, len(summary_with_ranks) + 1)
    bins = [0, 6, 12, 18, 24, 30]
    labels = ['Ranks 1-6', 'Ranks 7-12', 'Ranks 13-18', 'Ranks 19-24', 'Ranks 25-30']
    summary_with_ranks['Rank Group'] = pd.cut(summary_with_ranks['rank'], bins=bins, labels=labels, right=True)
    contribution_df = summary_with_ranks.groupby('Rank Group', observed=True)['totalMonthlyGain'].sum().reset_index()
    total_club_gain = contribution_df['totalMonthlyGain'].sum()
    contribution_df['percentage'] = (contribution_df['totalMonthlyGain'] / total_club_gain) * 100 if total_club_gain > 0 else 0
    contribution_df = contribution_df.set_index('Rank Group')

    historical_df = individual_log_df.copy() 
    historical_df['time_group'] = historical_df['timestamp'].dt.floor('8h')
    print("  - All data successfully aggregated.")

    # --- 3. Calling Visualization Function ---
    print("--- 3. Generating Visuals and Reports ---")
    create_all_visuals(
        members_df=members_df,
        summary_df=summary_df,
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
    print("\n--- Visualization Complete! ---")


if __name__ == "__main__":
    main()