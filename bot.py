import discord
from discord.ext import commands, tasks
import os
import csv
from datetime import datetime
import pytz
import pandas as pd
import re
from dotenv import load_dotenv

# --- Configuration ---
COMMAND_LOG_CSV = 'command_log.csv'
PROGRESS_LOG_CSV = 'progress_log.csv'
USER_REGISTRATIONS_CSV = 'user_registrations.csv' # New file for linking users
OUTPUT_DIR = 'Club_Report_Output'
INDIVIDUAL_LOGS_DIR = os.path.join(OUTPUT_DIR, 'individual_logs')
FAN_LOG_CSV = 'fan_log.csv'
MEMBERS_CSV = 'members.csv'
RANKS_CSV = 'ranks.csv'
ANALYSIS_OUTPUT_CSV = os.path.join(OUTPUT_DIR, 'fanGainAnalysis_output.csv')
MEMBER_SUMMARY_CSV = os.path.join(OUTPUT_DIR, 'member_performance_summary.csv')

SCOREBOARD_CHANNEL_NAME = 'the-scoreboard'
PROMOTION_CHANNEL_NAME = 'the-scoreboard' 

# --- Bot Setup ---
intents = discord.Intents.default()
intents.message_content = True
intents.members = True # Required to find members by ID
bot = commands.Bot(command_prefix='/', intents=intents, help_command=None)


# --- Helper Functions ---

def get_in_game_name(discord_id):
    """Looks up a user's in-game name from the registration file."""
    if not os.path.exists(USER_REGISTRATIONS_CSV):
        return None
    registrations_df = pd.read_csv(USER_REGISTRATIONS_CSV)
    user_entry = registrations_df[registrations_df['discord_id'] == discord_id]
    if not user_entry.empty:
        return user_entry.iloc[0]['in_game_name']
    return None

def get_last_update_timestamp():
    """Reads the fan log and returns a Discord-formatted timestamp of the last entry."""
    try:
        df = pd.read_csv(FAN_LOG_CSV)
        # When reading, pandas correctly infers the timestamp is already timezone-aware
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        last_update_time = df['timestamp'].max()
        
        # --- FIX: Convert the timezone-aware timestamp to UTC before getting the unix value ---
        last_update_time_utc = last_update_time.tz_convert('UTC')
        unix_timestamp = int(last_update_time_utc.timestamp())
        # --- END FIX ---

        return f"<t:{unix_timestamp}:F>"
    except Exception as e:
        print(f"Error getting last update timestamp: {e}")
        return "Not available"

async def send_to_scoreboard(ctx, message_content, file_path=None):
    """Finds the scoreboard channel and sends a message and optional file there."""
    scoreboard_channel = discord.utils.get(ctx.guild.channels, name=SCOREBOARD_CHANNEL_NAME)
    
    if not scoreboard_channel:
        await ctx.send(f"Error: I couldn't find the `#{SCOREBOARD_CHANNEL_NAME}` channel. Please create it.", ephemeral=True)
        return

    if file_path and not os.path.exists(file_path):
        await scoreboard_channel.send(f"Sorry {ctx.author.mention}, I couldn't find the requested file. Please check if the analysis has been run.")
        return

    try:
        file_to_send = discord.File(file_path) if file_path else None
        await scoreboard_channel.send(message_content, file=file_to_send)
        if ctx.interaction: # Check if it's a slash command interaction
             await ctx.followup.send(f"Done! I've posted the results in {scoreboard_channel.mention}.", ephemeral=True)
        else: # For prefix commands
             await ctx.send(f"Done! I've posted the results in {scoreboard_channel.mention}.")

    except Exception as e:
        print(f"Error sending to scoreboard: {e}")
        await ctx.send("Sorry, I encountered an error while trying to post the results.", ephemeral=True)

def log_command_usage(ctx):
    """Logs the details of a command execution to a CSV file."""
    file_exists = os.path.isfile(COMMAND_LOG_CSV)
    try:
        with open(COMMAND_LOG_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Timestamp', 'Username', 'Command', 'Channel'])
            
            central_tz = pytz.timezone('US/Central')
            timestamp = datetime.now(central_tz).strftime('%Y-%m-%d %H:%M:%S')
            
            writer.writerow([
                timestamp,
                ctx.author.name,
                ctx.command.name,
                ctx.channel.name
            ])
    except Exception as e:
        print(f"Error logging command usage: {e}")

def get_all_prestige_roles(guild):
    """Reads ranks.csv and returns a dictionary of rank_name: role_object."""
    try:
        ranks_df = pd.read_csv(RANKS_CSV)
        prestige_role_names = set(ranks_df['rank_name'])
        roles = {role.name: role for role in guild.roles if role.name in prestige_role_names}
        return roles
    except FileNotFoundError:
        print("Error: ranks.csv not found. Cannot manage prestige roles.")
        return {}

def format_timedelta_ddhhmm(td):
    """Formats a timedelta object into a 'dd hh mm' string."""
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{days}d {hours}h {minutes}m"

def get_club_rank(df, target_timestamp, in_game_name):
    """Calculates the club rank for a specific member at a given time."""
    snapshot_df = df[df['timestamp'] <= target_timestamp].copy()
    if snapshot_df.empty: return None

    # --- FIX: Calculate total monthly gain correctly before ranking ---
    # First, calculate the cumulative sum of fan gains for each member over time
    snapshot_df['totalMonthlyGain'] = snapshot_df.groupby('inGameName')['fanGain'].cumsum()
    
    # Then, find the single latest entry for each member, which now contains their correct total gain
    latest_entries = snapshot_df.loc[snapshot_df.groupby('inGameName')['timestamp'].idxmax()]
    
    # Now, sort by the correct total monthly gain
    ranked_df = latest_entries.sort_values('totalMonthlyGain', ascending=False).reset_index()
    # --- END FIX ---
    
    member_rank_series = ranked_df[ranked_df['inGameName'] == in_game_name].index
    
    return member_rank_series[0] + 1 if not member_rank_series.empty else None


# --- Events ---
@bot.event
async def on_ready():
    """Runs once when the bot successfully connects to Discord."""
    print(f'Success! {bot.user} is online and ready.')
    print('------')
    if not update_ranks_task.is_running():
        update_ranks_task.start()

@bot.event
async def on_command_completion(ctx):
    """Runs automatically after any command is successfully executed."""
    print(f"Command '{ctx.command.name}' was run by {ctx.author.name} in #{ctx.channel.name}")
    log_command_usage(ctx)


# --- Scheduled Task for Rank Updates ---
@tasks.loop(minutes=60)
async def update_ranks_task():
    """Periodically checks and updates member roles based on their prestige rank."""
    print("Running scheduled rank update...")
    
    if not os.path.exists(USER_REGISTRATIONS_CSV):
        print("`user_registrations.csv` not found. Skipping rank update until users have registered.")
        return

    guild = bot.guilds[0]
    if not guild:
        print("Bot is not connected to any server.")
        return

    try:
        registrations_df = pd.read_csv(USER_REGISTRATIONS_CSV)
        analysis_df = pd.read_csv(ANALYSIS_OUTPUT_CSV)
        all_prestige_roles = get_all_prestige_roles(guild)
    except FileNotFoundError as e:
        print(f"Error loading data for rank update: {e}")
        return

    latest_stats = analysis_df.loc[analysis_df.groupby('inGameName')['timestamp'].idxmax()]
    
    registrations_df.rename(columns={'in_game_name': 'inGameName'}, inplace=True)
    merged_df = pd.merge(latest_stats, registrations_df, on='inGameName')

    for _, player_data in merged_df.iterrows():
        member = guild.get_member(player_data['discord_id'])
        if not member:
            print(f"Could not find member with ID: {player_data['discord_id']}")
            continue

        # --- FIX: Check if bot has permission to manage the user's roles ---
        # The bot's highest role must be higher than the member's highest role.
        if member.top_role >= guild.me.top_role:
            print(f"Cannot manage roles for {member.name} because their role is higher than or equal to mine. Skipping.")
            continue
        # --- END FIX ---

        correct_rank_name = player_data['prestigeRank']
        correct_role = all_prestige_roles.get(correct_rank_name)
        if not correct_role: continue

        member_prestige_roles = [role for role in member.roles if role.name in all_prestige_roles]
        
        if correct_role not in member.roles:
            await member.add_roles(correct_role)
            print(f"Promoted {member.name} to {correct_role.name}")
            promo_channel = discord.utils.get(guild.channels, name=PROMOTION_CHANNEL_NAME)
            if promo_channel:
                await promo_channel.send(f"🎉 **RANK UP!** Congratulations {member.mention}, you have achieved the rank of **{correct_role.name}**! �")

        for old_role in member_prestige_roles:
            if old_role != correct_role:
                await member.remove_roles(old_role)


# --- Bot Commands ---

@bot.command()
async def help(ctx):
    """Displays a list of all available commands and their descriptions."""
    
    # Create an Embed object for a clean, formatted message
    embed = discord.Embed(
        title="TRACKMASTER BOT Help",
        description="Here are all the commands you can use to track your performance and prestige.",
        color=discord.Color.blue() # You can change the color
    )
    
    # Add a field for the one-time setup command
    embed.add_field(
        name="SETUP (Required)",
        value="`/register [your-in-game-name]` - Links your Discord account to your exact in-game name. You must do this first!",
        inline=False
    )
    
    # Add a field for personal commands
    embed.add_field(
        name="PERSONAL STATS",
        value="`/myprogress` - Get a personalized summary of your progress since you last checked.",
        inline=False
    )

    # Add a field for public commands that show charts
    embed.add_field(
        name="CLUB CHARTS & REPORTS",
        value=(
            "`/top10` - Shows the current top 10 members by monthly fan gain.\n"
            "`/prestige_leaderboard` - Displays the all-time prestige point leaderboard.\n"
            "`/performance` - Posts the historical fan gain heatmap.\n"
            "`/log [member_name]` - Gets the detailed performance log for any member."
        ),
        inline=False
    )
    
    embed.set_footer(text="Remember to use the command prefix '/' before each command.")
    
    await ctx.send(embed=embed, ephemeral=True)

@bot.command()
async def register(ctx, *, in_game_name: str):
    """Links your Discord account to your EXACT in-game name."""
    try:
        members_df = pd.read_csv(MEMBERS_CSV)
        if os.path.exists(USER_REGISTRATIONS_CSV):
            registrations_df = pd.read_csv(USER_REGISTRATIONS_CSV)
        else:
            registrations_df = pd.DataFrame(columns=['discord_id', 'in_game_name'])
    except FileNotFoundError:
        await ctx.send("I'm missing the `members.csv` file. Please tell the admin.", ephemeral=True)
        return

    if in_game_name not in members_df['inGameName'].values:
        await ctx.send(f"Sorry, I can't find a club member with the name **{in_game_name}**. "
                       "Please make sure your name is spelled **EXACTLY** as it appears in-game, including capitalization and spaces.", ephemeral=True)
        return

    if in_game_name in registrations_df['in_game_name'].values:
        existing_reg = registrations_df[registrations_df['in_game_name'] == in_game_name]
        if existing_reg.iloc[0]['discord_id'] != ctx.author.id:
            await ctx.send(f"That in-game name is already registered to another Discord user. If this is an error, please contact an admin.", ephemeral=True)
            return

    registrations_df = registrations_df[registrations_df['discord_id'] != ctx.author.id]
    new_entry = pd.DataFrame([{'discord_id': ctx.author.id, 'in_game_name': in_game_name}])
    registrations_df = pd.concat([registrations_df, new_entry], ignore_index=True)
    registrations_df.to_csv(USER_REGISTRATIONS_CSV, index=False)
    
    await ctx.send(f"✅ Success! Your Discord account has been linked to the in-game name: **{in_game_name}**. You can now use personal commands like `/myprogress`.", ephemeral=True)

@bot.command()
async def myprogress(ctx):
    """Provides a personalized progress report since the user's last request."""
    in_game_name = get_in_game_name(ctx.author.id)
    if not in_game_name:
        await ctx.send("You need to register your in-game name first! Use the command: `/register [your-exact-in-game-name]`", ephemeral=True)
        return
    
    try:
        ranks_df = pd.read_csv(RANKS_CSV)
        analysis_df = pd.read_csv(ANALYSIS_OUTPUT_CSV)
        analysis_df['timestamp'] = pd.to_datetime(analysis_df['timestamp'])
        summary_df = pd.read_csv(MEMBER_SUMMARY_CSV)
        
        if os.path.exists(PROGRESS_LOG_CSV):
            progress_df = pd.read_csv(PROGRESS_LOG_CSV)
        else:
            progress_df = pd.DataFrame(columns=['discord_id', 'last_checked_timestamp'])
    except FileNotFoundError as e:
        await ctx.send(f"Sorry, I'm missing a required data file (`{e.filename}`). Please run the analysis first.", ephemeral=True)
        return

    user_analysis_df = analysis_df[analysis_df['inGameName'] == in_game_name].sort_values('timestamp')
    if user_analysis_df.empty:
        await ctx.send("I couldn't find any analysis data for you yet.", ephemeral=True)
        return

    last_checked_entry = progress_df[progress_df['discord_id'] == ctx.author.id]
    last_checked_timestamp = pd.to_datetime(last_checked_entry.iloc[0]['last_checked_timestamp']) if not last_checked_entry.empty else user_analysis_df.iloc[0]['timestamp']

    current_time_utc = datetime.now(pytz.utc)
    last_checked_timestamp_utc = last_checked_timestamp.tz_convert('UTC')
    time_since_last_check = current_time_utc - last_checked_timestamp_utc

    progress_period_df = user_analysis_df[user_analysis_df['timestamp'] > last_checked_timestamp]
    before_stats = user_analysis_df[user_analysis_df['timestamp'] <= last_checked_timestamp].iloc[-1]
    after_stats = user_analysis_df.iloc[-1]

    fans_gained = progress_period_df['fanGain'].sum()
    prestige_gained = after_stats['cumulativePrestige'] - before_stats['cumulativePrestige']
    
    rank_before = get_club_rank(analysis_df, last_checked_timestamp, in_game_name)
    rank_after = get_club_rank(analysis_df, after_stats['timestamp'], in_game_name)
    rank_change = (rank_before - rank_after) if rank_before and rank_after else 0
    
    eom_projection = summary_df[summary_df['inGameName'] == in_game_name].iloc[0]['month_end_proj']

    time_ago_str = format_timedelta_ddhhmm(time_since_last_check)
    fans_line = f"**Fans:** You've earned **{fans_gained:,.0f}** fans. Your end-of-month projection is **{eom_projection:,.0f}**."
    
    next_rank_index = ranks_df[ranks_df['rank_name'] == after_stats['prestigeRank']].index
    next_rank_name = "Max Rank"
    if not next_rank_index.empty and next_rank_index[0] + 1 < len(ranks_df):
        next_rank_name = ranks_df.iloc[next_rank_index[0] + 1]['rank_name']
        
    prestige_line = f"**Prestige:** You've gained **{prestige_gained:,.2f}** prestige. You're **{after_stats['pointsToNextRank']:,.2f}** points from reaching **{next_rank_name}**."
    
    rank_change_str = f"moved up **{abs(rank_change)}** spots" if rank_change > 0 else (f"moved down **{abs(rank_change)}** spots" if rank_change < 0 else "held your ground")
    club_rank_line = f"**Club Rank:** You've {rank_change_str} and are now ranked **#{rank_after}** in the club for monthly fan gain."

    response_message = (
        f"{ctx.author.mention}, here's your progress report since you last checked in **{time_ago_str}** ago.\n"
        f"{fans_line}\n"
        f"{prestige_line}\n"
        f"{club_rank_line}\n"
    )
    
    await ctx.send(response_message, ephemeral=True)
    
    progress_df = progress_df[progress_df['discord_id'] != ctx.author.id]
    new_entry = pd.DataFrame([{'discord_id': ctx.author.id, 'last_checked_timestamp': after_stats['timestamp']}])
    progress_df = pd.concat([progress_df, new_entry], ignore_index=True)
    progress_df.to_csv(PROGRESS_LOG_CSV, index=False)

@bot.command()
async def prestige_leaderboard(ctx):
    """Posts the prestige_leaderboard.png chart."""
    timestamp_str = get_last_update_timestamp()
    message = f"{ctx.author.mention} here is the Prestige Leaderboard!"
    file_path = os.path.join(OUTPUT_DIR, 'prestige_leaderboard.png')
    await send_to_scoreboard(ctx, message, file_path)

@bot.command()
async def top10(ctx):
    """Posts the monthly_leaderboard.png chart."""
    timestamp_str = get_last_update_timestamp()
    message = f"{ctx.author.mention} here is the Top 10 Monthly Fan Gain chart!"
    file_path = os.path.join(OUTPUT_DIR, 'monthly_leaderboard.png')
    await send_to_scoreboard(ctx, message, file_path)

@bot.command()
async def performance(ctx):
    """Posts the fan_performance_heatmap.png chart."""
    timestamp_str = get_last_update_timestamp()
    message = f"{ctx.author.mention} here is the historical performance heatmap!"
    file_path = os.path.join(OUTPUT_DIR, 'fan_performance_heatmap.png')
    await send_to_scoreboard(ctx, message, file_path)

@bot.command()
async def log(ctx, *, name: str):
    """Finds and posts the cumulative log for a specific member."""
    timestamp_str = get_last_update_timestamp()
    sanitized_input = re.sub(r'[^a-zA-Z0-9]', '', name).lower()
    
    # --- FIX: Look in the correct subdirectory ---
    if not os.path.exists(INDIVIDUAL_LOGS_DIR):
        await send_to_scoreboard(ctx, f"Sorry {ctx.author.mention}, I can't find the individual logs folder.")
        return

    found_file = None
    for filename in os.listdir(INDIVIDUAL_LOGS_DIR):
        if filename.startswith('log_cumulative_'):
            file_base = filename.replace('log_cumulative_', '').replace('.png', '')
            sanitized_file_name = re.sub(r'[^a-zA-Z0-9]', '', file_base).lower()
            if sanitized_input == sanitized_file_name:
                found_file = os.path.join(INDIVIDUAL_LOGS_DIR, filename)
                break
    
    if found_file:
        message = f"{ctx.author.mention} here is the cumulative log for **{name}**!"
        await send_to_scoreboard(ctx, message, found_file)
    else:
        message = f"Sorry {ctx.author.mention}, I couldn't find a cumulative log for a member named **{name}**."
        await send_to_scoreboard(ctx, message)


# --- Run the Bot ---
load_dotenv() # Loads variables from .env file
TOKEN = os.getenv('DISCORD_TOKEN')

if TOKEN is None:
    print("ERROR: DISCORD_TOKEN not found in .env file.")
else:
    bot.run(TOKEN)
