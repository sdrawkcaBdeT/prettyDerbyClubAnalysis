import discord
from discord.ext import commands, tasks
import os
import csv
from datetime import datetime
import pytz
import pandas as pd
import re
from dotenv import load_dotenv
from analysis import get_club_month_window # We need this helper function
import matplotlib.pyplot as plt
import io

# --- Configuration ---
COMMAND_LOG_CSV = 'command_log.csv'
PROGRESS_LOG_CSV = 'progress_log.csv'
USER_REGISTRATIONS_CSV = 'user_registrations.csv'
OUTPUT_DIR = 'Club_Report_Output'
INDIVIDUAL_LOGS_DIR = os.path.join(OUTPUT_DIR, 'individual_logs')
FAN_LOG_CSV = 'fan_log.csv'
MEMBERS_CSV = 'members.csv'
RANKS_CSV = 'ranks.csv'
ENRICHED_FAN_LOG_CSV = 'enriched_fan_log.csv'

SCOREBOARD_CHANNEL_NAME = 'the-scoreboard'
PROMOTION_CHANNEL_NAME = 'the-scoreboard' 

# --- Shop Configuration ---
SHOP_ITEMS = {
    "PRESTIGE": { # Changed from LOBBYING
        "prestige20": {
            "name": "Lobby the Stewards (20 Prestige)",
            "description": "Permanently increases your total prestige by 20.",
            "amount": 20, # How much prestige is given
            "type": "prestige"
        },
        "prestige100": {
            "name": "Secure a Dynasty (100 Prestige)",
            "description": "Permanently increases your total prestige by 100.",
            "amount": 100,
            "type": "prestige"
        }
    },
    "PERFORMANCE": {
        "p1": {
            "name": "Study Race Tapes",
            "description": "Increases Performance Yield multiplier by +0.05 per tier.",
            "costs": [15000, 40000, 120000],
            "max_tier": 3,
            "type": "upgrade"
        },
        "p2": {
            "name": "Perfect the Starting Gate",
            "description": "Adds a flat +4 bonus to Performance Prestige before multiplication.",
            "costs": [25000, 70000, 200000],
            "max_tier": 3,
            "type": "upgrade"
        }
    },
    "TENURE": {
        "t1": {
            "name": "Build Club Morale",
            "description": "Increases Tenure Yield multiplier by +0.1 per tier.",
            "costs": [20000, 60000, 180000],
            "max_tier": 3,
            "type": "upgrade"
        },
        "t2": {
            "name": "Garner Owner's Favor",
            "description": "Adds a flat +150 CC per day. (Note: This is a passive effect handled by the backend)",
            "costs": [75000, 225000, 675000],
            "max_tier": 3,
            "type": "upgrade"
        }
    }
}


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
    snapshot_df['totalMonthlyGain'] = snapshot_df.groupby('inGameName')['fanGain'].cumsum()
    latest_entries = snapshot_df.loc[snapshot_df.groupby('inGameName')['timestamp'].idxmax()]
    ranked_df = latest_entries.sort_values('totalMonthlyGain', ascending=False).reset_index()
    member_rank_series = ranked_df[ranked_df['inGameName'] == in_game_name].index
    return member_rank_series[0] + 1 if not member_rank_series.empty else None

# --- NEW HELPER FUNCTIONS FOR MARKET ---
def load_market_file(filename, dtype=None):
    """Safely loads a CSV file from the market directory."""
    try:
        return pd.read_csv(f'market/{filename}', dtype=dtype)
    except FileNotFoundError:
        return pd.DataFrame()

def format_cc(amount):
    """Formats a number as a string with commas and 'CC'."""
    return f"{int(amount):,} CC"

def log_market_transaction(actor_id, transaction_type, target_id, item_name, item_quantity, cc_amount, fee_paid):
    """Logs a market transaction to the universal log file."""
    log_file = 'market/universal_transaction_log.csv'
    file_exists = os.path.isfile(log_file)
    central_tz = pytz.timezone('US/Central')
    timestamp = datetime.now(central_tz).isoformat()
    
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                'timestamp', 'actor_id', 'transaction_type', 'target_id', 
                'item_name', 'item_quantity', 'cc_amount', 'fee_paid'
            ])
        writer.writerow([
            timestamp, actor_id, transaction_type, target_id,
            item_name, item_quantity, cc_amount, fee_paid
        ])
        
def get_name_from_ticker_or_name(identifier: str):
    """
    Finds a member's full in_game_name from either their ticker or name.
    Returns the in_game_name on success, or None on failure.
    """
    # First, try to find a match in the tickers
    init_df = load_market_file('member_initialization.csv')
    if not init_df.empty:
        # Tickers are stored in uppercase, so we search in uppercase
        ticker_match = init_df[init_df['ticker'].str.upper() == identifier.upper()]
        if not ticker_match.empty:
            return ticker_match.iloc[0]['in_game_name']
            
    # If no ticker match, try to find a direct name match (case-insensitive)
    stock_prices_df = load_market_file('stock_prices.csv')
    if not stock_prices_df.empty:
        name_match = stock_prices_df[stock_prices_df['in_game_name'].str.lower() == identifier.lower()]
        if not name_match.empty:
            return name_match.iloc[0]['in_game_name']
            
    return None # Return None if no match is found

def calculate_prestige_bundle_cost(current_prestige, amount_to_buy):
    """Calculates the total cost of buying a bundle of prestige points."""
    total_cost = 0
    # --- FINAL RE-BALANCED FORMULA ---
    for i in range(amount_to_buy):
        # Base cost of 50, same gentle ramp.
        cost_for_this_point = 50 * (1.000015 ** (current_prestige + i))
        total_cost += cost_for_this_point
    return total_cost

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
        enriched_df = pd.read_csv(ENRICHED_FAN_LOG_CSV)
        all_prestige_roles = get_all_prestige_roles(guild)
    except FileNotFoundError as e:
        print(f"Error loading data for rank update: {e}")
        return

    # Convert timestamp to datetime for correct sorting
    enriched_df['timestamp'] = pd.to_datetime(enriched_df['timestamp'])
    # Find the latest entry for each player from the enriched log
    latest_stats = enriched_df.loc[enriched_df.groupby('inGameName')['timestamp'].idxmax()]
        
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
                await promo_channel.send(f"🎉 **RANK UP!** Congratulations {member.mention}, you have achieved the rank of **{correct_role.name}**! ")

        for old_role in member_prestige_roles:
            if old_role != correct_role:
                await member.remove_roles(old_role)


# --- Bot Commands ---

@bot.command()
async def help(ctx):
    """Displays a list of all available commands and their descriptions."""
    
    embed = discord.Embed(title="TRACKMASTER BOT Help", description="Here are all the commands you can use to track your performance and prestige.", color=discord.Color.blue())
    embed.add_field(name="SETUP (Required)", value="`/register [your-in-game-name]` - Links your Discord account to your exact in-game name. You must do this first!", inline=False)
    embed.add_field(name="PERSONAL STATS", value="`/myprogress` - Get a personalized summary of your progress since you last checked.", inline=False)
    embed.add_field(name="CLUB CHARTS & REPORTS", value=("`/top10` - Shows the current top 10 members by monthly fan gain.\n" "`/prestige_leaderboard` - Displays the all-time prestige point leaderboard.\n" "`/performance` - Posts the historical fan gain heatmap.\n" "`/log [member_name]` - Gets the detailed performance log for any member."), inline=False)
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
        enriched_df = pd.read_csv(ENRICHED_FAN_LOG_CSV)
        enriched_df['timestamp'] = pd.to_datetime(enriched_df['timestamp'])
        
        if os.path.exists(PROGRESS_LOG_CSV):
            progress_df = pd.read_csv(PROGRESS_LOG_CSV)
        else:
            progress_df = pd.DataFrame(columns=['discord_id', 'last_checked_timestamp'])
    except FileNotFoundError as e:
        await ctx.send(f"Sorry, I'm missing a required data file (`{e.filename}`). Please run the analysis first.", ephemeral=True)
        return

    user_analysis_df = enriched_df[enriched_df['inGameName'] == in_game_name].sort_values('timestamp')
    if user_analysis_df.empty:
        await ctx.send("I couldn't find any analysis data for you yet.", ephemeral=True)
        return

    last_checked_entry = progress_df[progress_df['discord_id'] == ctx.author.id]
    last_checked_timestamp = pd.to_datetime(last_checked_entry.iloc[0]['last_checked_timestamp']) if not last_checked_entry.empty else user_analysis_df.iloc[0]['timestamp']

    current_time_utc = datetime.now(pytz.utc)
    last_checked_timestamp_utc = last_checked_timestamp.tz_convert('UTC')
    time_since_last_check = current_time_utc - last_checked_timestamp_utc

    progress_period_df = user_analysis_df[user_analysis_df['timestamp'] > last_checked_timestamp]
    
    if not progress_period_df.empty:
        before_stats = user_analysis_df[user_analysis_df['timestamp'] <= last_checked_timestamp].iloc[-1]
    else:
        before_stats = user_analysis_df.iloc[-1]

    after_stats = user_analysis_df.iloc[-1]

    fans_gained = progress_period_df['fanGain'].sum()
    prestige_gained = after_stats['cumulativePrestige'] - before_stats['cumulativePrestige']
    
    rank_before = get_club_rank(enriched_df, last_checked_timestamp, in_game_name)
    rank_after = get_club_rank(enriched_df, after_stats['timestamp'], in_game_name)
    rank_change = (rank_before - rank_after) if rank_before and rank_after else 0
    
    # --- Re-implement EOM Projection Calculation ---    
    # Get the start and end dates of the current club month
    start_date, end_date = get_club_month_window(datetime.now(pytz.timezone('US/Central')))
    
    # Filter the user's logs for the current month
    user_monthly_logs = user_analysis_df[(user_analysis_df['timestamp'] >= start_date) & (user_analysis_df['timestamp'] <= end_date)]
    
    if not user_monthly_logs.empty:
        first_log = user_monthly_logs.iloc[0]
        latest_log = user_monthly_logs.iloc[-1]
        
        fan_contribution = latest_log['fanCount'] - first_log['fanCount']
        
        hrs_elapsed = (latest_log['timestamp'] - first_log['timestamp']).total_seconds() / 3600
        fans_per_hour = fan_contribution / hrs_elapsed if hrs_elapsed > 0 else 0
        
        hrs_remaining = (end_date - latest_log['timestamp']).total_seconds() / 3600
        
        eom_projection = fan_contribution + (fans_per_hour * hrs_remaining)
    else:
        eom_projection = 0

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

# --- Fan Exchange Commands ---

@bot.command(name="set_ticker")
async def set_ticker(ctx, ticker: str):
    """Sets a permanent, unique stock ticker for your name (2-5 letters)."""
    user_id = str(ctx.author.id)
    in_game_name = get_in_game_name(ctx.author.id)

    if not in_game_name:
        await ctx.send("You must be registered with `/register` to set a ticker.", ephemeral=True)
        return

    # --- Validation ---
    ticker = ticker.upper() # Tickers are always uppercase
    if not (2 <= len(ticker) <= 5):
        await ctx.send("Ticker must be between 2 and 5 letters.", ephemeral=True)
        return
    if not ticker.isalpha():
        await ctx.send("Ticker can only contain letters (A-Z).", ephemeral=True)
        return

    # --- File Locking ---
    lock_file = 'market/market.lock'
    if os.path.exists(lock_file):
        await ctx.send("The market is busy. Please try again in a moment.", ephemeral=True)
        return
    open(lock_file, 'w').close()

    try:
        init_df = load_market_file('member_initialization.csv')
        if init_df.empty:
            await ctx.send("Market initialization file not found. Please contact an admin.", ephemeral=True)
            return

        # Check if user already has a ticker
        user_row = init_df[init_df['in_game_name'] == in_game_name]
        if not user_row.empty and pd.notna(user_row.iloc[0]['ticker']):
            await ctx.send(f"You have already set your ticker to **${user_row.iloc[0]['ticker']}**. It cannot be changed.", ephemeral=True)
            return
            
        # Check if ticker is already in use
        if ticker in init_df['ticker'].values:
            await ctx.send(f"The ticker **${ticker}** is already taken. Please choose another.", ephemeral=True)
            return

        # --- Update and Save ---
        init_df.loc[init_df['in_game_name'] == in_game_name, 'ticker'] = ticker
        init_df.to_csv('market/member_initialization.csv', index=False)

        embed = discord.Embed(
            title="✅ Ticker Set Successfully!",
            description=f"Your official stock ticker is now **${ticker}**.\nOther users can now use this ticker with the `/stock`, `/invest`, and `/sell` commands.",
            color=discord.Color.purple()
        )
        await ctx.send(embed=embed)

    finally:
        os.remove(lock_file)
        
@bot.command(name="shop")
async def shop(ctx):
    """Displays the Prestige Shop with available items and your upgrade tiers."""
    user_id = str(ctx.author.id)
    crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
    upgrades_df = load_market_file('shop_upgrades.csv', dtype={'discord_id': str})
    enriched_df = load_market_file('enriched_fan_log.csv')

    user_data = crew_coins_df[crew_coins_df['discord_id'] == user_id]
    if user_data.empty:
        await ctx.send("You must be registered to use the shop.", ephemeral=True)
        return
    balance = float(user_data['balance'].iloc[0])
    in_game_name = user_data['in_game_name'].iloc[0]
    user_upgrades = upgrades_df[upgrades_df['discord_id'] == user_id].set_index('upgrade_name')['tier'].to_dict()

    embed = discord.Embed(title="Prestige Shop", description="Spend your Crew Coins to get ahead!", color=discord.Color.purple())
    embed.set_footer(text=f"Your current balance: {format_cc(balance)}")

    # --- Prestige Bundles ---
    latest_stats = enriched_df[enriched_df['inGameName'] == in_game_name].sort_values('timestamp').iloc[-1]
    current_prestige = latest_stats['cumulativePrestige']
    
    prestige_text = ""
    for item_id, item_details in SHOP_ITEMS['PRESTIGE'].items():
        bundle_cost = calculate_prestige_bundle_cost(current_prestige, item_details['amount'])
        prestige_text += f"**{item_details['name']} (ID: `{item_id}`)**\nCost: **{format_cc(bundle_cost)}**\n\n"
    embed.add_field(name="--- Prestige Purchases ---", value=prestige_text, inline=False)


    # --- Tiered Upgrades ---
    for category, items in SHOP_ITEMS.items():
        if category == "PRESTIGE": continue
        category_text = ""
        for item_id, item_details in items.items():
            current_tier = user_upgrades.get(item_details['name'], 0)
            if current_tier >= item_details['max_tier']:
                status = "**(Max Tier)**"
            else:
                cost = item_details['costs'][current_tier]
                status = f"Tier {current_tier+1} Cost: **{format_cc(cost)}**"
            
            category_text += f"**{item_details['name']} (ID: `{item_id}`)**\n{item_details['description']}\n*Your Tier: {current_tier}* | {status}\n\n"
        embed.add_field(name=f"--- {category} Upgrades ---", value=category_text, inline=False)
        
    await ctx.send(embed=embed)


@bot.command(name="buy")
async def buy(ctx, item_id: str):
    """Purchases an item or upgrade from the Prestige Shop."""
    user_id = str(ctx.author.id)
    item_id = item_id.lower()

    # --- Find the Item ---
    item_details = None
    if item_id in SHOP_ITEMS['PRESTIGE']:
        item_details = SHOP_ITEMS['PRESTIGE'][item_id]
    else:
        for category in ['PERFORMANCE', 'TENURE']:
            if item_id in SHOP_ITEMS[category]:
                item_details = SHOP_ITEMS[category][item_id]
                break
    
    if not item_details:
        await ctx.send("Invalid item ID. Please use the IDs listed in the `/shop` command.", ephemeral=True)
        return

    lock_file = 'market/market.lock'
    if os.path.exists(lock_file):
        await ctx.send("The market is busy. Please try again in a moment.", ephemeral=True)
        return
    open(lock_file, 'w').close()

    try:
        crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
        upgrades_df = load_market_file('shop_upgrades.csv', dtype={'discord_id': str})

        user_data_row = crew_coins_df[crew_coins_df['discord_id'] == user_id]
        if user_data_row.empty:
            await ctx.send("You must be registered to make purchases.", ephemeral=True)
            return
        
        balance = float(user_data_row['balance'].iloc[0])
        in_game_name = user_data_row['in_game_name'].iloc[0]
        cost = 0
        
        # --- Handle Purchase Logic ---
        if item_details['type'] == 'upgrade':
            # ... (this logic remains the same as before) ...
            upgrade_name = item_details['name']
            user_upgrade_row = upgrades_df[(upgrades_df['discord_id'] == user_id) & (upgrades_df['upgrade_name'] == upgrade_name)]
            current_tier = 0
            if not user_upgrade_row.empty:
                current_tier = user_upgrade_row['tier'].iloc[0]
            if current_tier >= item_details['max_tier']:
                await ctx.send(f"You have already reached the max tier for **{upgrade_name}**.", ephemeral=True)
                return
            cost = item_details['costs'][current_tier]
            if balance < cost:
                await ctx.send(f"You don't have enough Crew Coins. You need {format_cc(cost)}.", ephemeral=True)
                return
            if not user_upgrade_row.empty:
                upgrades_df.loc[user_upgrade_row.index, 'tier'] += 1
            else:
                new_upgrade = pd.DataFrame([{'discord_id': user_id, 'upgrade_name': upgrade_name, 'tier': 1}])
                upgrades_df = pd.concat([upgrades_df, new_upgrade], ignore_index=True)
            upgrades_df.to_csv('market/shop_upgrades.csv', index=False)

        elif item_details['type'] == 'prestige':
            enriched_df = load_market_file('enriched_fan_log.csv')
            latest_stats = enriched_df[enriched_df['inGameName'] == in_game_name].sort_values('timestamp').iloc[-1]
            current_prestige = latest_stats['cumulativePrestige']
            
            amount_to_buy = item_details['amount']
            cost = calculate_prestige_bundle_cost(current_prestige, amount_to_buy)

            if balance < cost:
                await ctx.send(f"You don't have enough Crew Coins. You need {format_cc(cost)}.", ephemeral=True)
                return

            new_prestige_row = latest_stats.copy()
            new_prestige_row['timestamp'] = datetime.now(pytz.timezone('US/Central')).isoformat()
            new_prestige_row['prestigeGain'] = float(amount_to_buy)
            new_prestige_row['cumulativePrestige'] += float(amount_to_buy)
            new_prestige_row['performancePrestigePoints'] = 0
            new_prestige_row['tenurePrestigePoints'] = 0
            
            enriched_df = pd.concat([enriched_df, new_prestige_row.to_frame().T], ignore_index=True)
            enriched_df.to_csv('enriched_fan_log.csv', index=False)

        # --- Update Balance and Log ---
        crew_coins_df.loc[crew_coins_df['discord_id'] == user_id, 'balance'] -= cost
        crew_coins_df.to_csv('market/crew_coins.csv', index=False)
        
        log_market_transaction(
            actor_id=user_id, transaction_type='PURCHASE', target_id='SYSTEM',
            item_name=item_details['name'], item_quantity=1,
            cc_amount=-cost, fee_paid=0
        )

        await ctx.send(embed=discord.Embed(
            title="✅ Purchase Successful!",
            description=f"You spent **{format_cc(cost)}** to acquire **{item_details['name']}**.",
            color=discord.Color.green()
        ))
    finally:
        os.remove(lock_file)

@bot.command(name="portfolio")
async def portfolio(ctx):
    """Displays the user's current CC balance and their stock holdings."""
    user_id = str(ctx.author.id)

    crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
    portfolios_df = load_market_file('portfolios.csv', dtype={'investor_discord_id': str})

    if crew_coins_df.empty:
        await ctx.send("Market data is currently unavailable. Please try again later.", ephemeral=True)
        return

    user_coin_data = crew_coins_df[crew_coins_df['discord_id'] == user_id]
    if user_coin_data.empty:
        await ctx.send("You do not have a Fan Exchange account yet. Make sure you are registered with `/register`.", ephemeral=True)
        return
    
    balance = user_coin_data['balance'].iloc[0]
    in_game_name = user_coin_data['in_game_name'].iloc[0]

    user_stocks = portfolios_df[portfolios_df['investor_discord_id'] == user_id]

    embed = discord.Embed(
        title=f"{ctx.author.display_name}'s Portfolio",
        description=f"**In-Game Name:** {in_game_name}",
        color=discord.Color.gold()
    )
    embed.add_field(name="💰 CC", value=format_cc(balance), inline=False)

    if not user_stocks.empty:
        stock_prices_df = load_market_file('stock_prices.csv')
        if not stock_prices_df.empty:
            stock_prices_df = stock_prices_df.set_index('in_game_name')
            portfolio_value = 0
            stock_display = ""
            for _, stock in user_stocks.iterrows():
                stock_name = stock['stock_in_game_name']
                shares = stock['shares_owned']
                try:
                    current_price = stock_prices_df.loc[stock_name, 'current_price']
                    value = shares * current_price
                    portfolio_value += value
                    stock_display += f"**{stock_name}**: {shares:,.2f} shares ({format_cc(value)})\n"
                except KeyError:
                    stock_display += f"**{stock_name}**: {shares:,.2f} shares (Price data unavailable)\n"
            
            embed.add_field(name="📈 Stock Holdings", value=stock_display, inline=False)
            embed.set_footer(text=f"Total Portfolio Value: {format_cc(float(balance) + portfolio_value)}")
    else:
        embed.add_field(name="📈 Stock Holdings", value="You do not own any shares.", inline=False)

    await ctx.send(embed=embed)


@bot.command(name="market")
async def market(ctx):
    """Displays the top 5 highest and lowest priced stocks."""
    stock_prices_df = load_market_file('stock_prices.csv')
    if stock_prices_df.empty or len(stock_prices_df) < 2:
        await ctx.send("Market is currently closed or has insufficient data.", ephemeral=True)
        return
        
    top_5 = stock_prices_df.sort_values(by='current_price', ascending=False).head(5)
    bottom_5 = stock_prices_df.sort_values(by='current_price', ascending=True).head(5)

    embed = discord.Embed(
        title="Baggins Index Market Overview",
        color=discord.Color.blue()
    )
    
    top_str = "\n".join([f"**{row.in_game_name}**: {format_cc(row.current_price)}" for _, row in top_5.iterrows()])
    bottom_str = "\n".join([f"**{row.in_game_name}**: {format_cc(row.current_price)}" for _, row in bottom_5.iterrows()])

    embed.add_field(name="🔼 Top 5 Stocks by Price", value=top_str, inline=False)
    embed.add_field(name="🔽 Bottom 5 Stocks by Price", value=bottom_str, inline=False)
    
    await ctx.send(embed=embed)


@bot.command(name="stock")
async def stock(ctx, *, member: str):
    """Displays detailed information and a price chart for a given stock."""
    stock_prices_df = load_market_file('stock_prices.csv')
    history_df = load_market_file('stock_price_history.csv')
    
    target_name = get_name_from_ticker_or_name(member)
    if not target_name:
        await ctx.send(f"Could not find a stock for a member or ticker named '{member}'.", ephemeral=True)
        return
    stock_info = stock_prices_df[stock_prices_df['in_game_name'] == target_name]
    
    if stock_info.empty:
        await ctx.send(f"Could not find a stock for a member named '{member}'. Please check the spelling.", ephemeral=True)
        return
        
    stock_name = stock_info['in_game_name'].iloc[0]
    current_price = stock_info['current_price'].iloc[0]

    embed = discord.Embed(
        title=f"Stock Ticker: ${stock_name.upper()}",
        description=f"Viewing market data for **{stock_name}**.",
        color=discord.Color.green()
    )
    embed.add_field(name="Current Price", value=format_cc(current_price), inline=True)

    stock_history = history_df[history_df['in_game_name'] == stock_name]
    if len(stock_history) > 1:
        stock_history = stock_history.copy() # Avoid SettingWithCopyWarning
        stock_history['timestamp'] = pd.to_datetime(stock_history['timestamp'])
        
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(stock_history['timestamp'], stock_history['price'], color='#00FF00', linewidth=2)
        
        ax.set_title(f'{stock_name} Price History', color='white')
        ax.set_ylabel('Price (CC)', color='white')
        ax.tick_params(axis='x', colors='white')
        ax.tick_params(axis='y', colors='white')
        ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', pad_inches=0.1, transparent=True)
        buf.seek(0)
        plt.close(fig)
        
        file = discord.File(buf, filename="price_chart.png")
        embed.set_image(url="attachment://price_chart.png")
        await ctx.send(embed=embed, file=file)
    else:
        await ctx.send(embed=embed)

@bot.command(name="invest")
async def invest(ctx, member: str, amount: str):
    """Invest a specific amount of CC into a member's stock."""
    # --- Input Validation ---
    try:
        # Handle "all" keyword
        if amount.lower() == 'all':
            # We'll determine the 'all' amount after loading the user's balance
            cc_amount = -1 
        else:
            cc_amount = int(amount)
        if cc_amount <= 0 and amount.lower() != 'all':
            await ctx.send("Please enter a positive whole number for the amount to invest.", ephemeral=True)
            return
    except ValueError:
        await ctx.send("Invalid amount. Please enter a whole number (e.g., 1000) or 'all'.", ephemeral=True)
        return

    investor_id = str(ctx.author.id)

    # --- File Locking to prevent race conditions ---
    lock_file = 'market/market.lock'
    if os.path.exists(lock_file):
        await ctx.send("The market is currently busy with another transaction. Please try again in a moment.", ephemeral=True)
        return
    open(lock_file, 'w').close() # Create lock

    try:
        # --- Load Data ---
        crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
        stock_prices_df = load_market_file('stock_prices.csv')
        portfolios_df = load_market_file('portfolios.csv', dtype={'investor_discord_id': str})

        # --- Get Investor Info ---
        investor_data = crew_coins_df[crew_coins_df['discord_id'] == investor_id]
        if investor_data.empty:
            await ctx.send("You do not have a Fan Exchange account yet.", ephemeral=True)
            return

        investor_balance = float(investor_data['balance'].iloc[0])
        
        # Resolve "all" amount
        if cc_amount == -1:
            cc_amount = int(investor_balance)
            if cc_amount <= 0:
                await ctx.send("You have no CC to invest.", ephemeral=True)
                return

        if investor_balance < cc_amount:
            await ctx.send(f"You don't have enough CC. Your balance is {format_cc(investor_balance)}.", ephemeral=True)
            return

        # --- Get Target Stock Info ---
        target_name = get_name_from_ticker_or_name(member)
        if not target_name:
            await ctx.send(f"Could not find a stock for a member or ticker named '{member}'.", ephemeral=True)
            return
        target_stock = stock_prices_df[stock_prices_df['in_game_name'] == target_name]
        if target_stock.empty:
            await ctx.send(f"Could not find a stock for '{member}'.", ephemeral=True)
            return
            
        target_name = target_stock['in_game_name'].iloc[0]
        current_price = float(target_stock['current_price'].iloc[0])
        if current_price <= 0:
            await ctx.send("This stock has no value and cannot be invested in at this time.", ephemeral=True)
            return

        # --- Transaction Calculation ---
        broker_fee = cc_amount * 0.03
        net_investment = cc_amount - broker_fee
        shares_purchased = net_investment / current_price
        
        # --- Update DataFrames ---
        # 1. Update investor's balance
        crew_coins_df.loc[crew_coins_df['discord_id'] == investor_id, 'balance'] -= cc_amount

        # 2. Update investor's portfolio
        existing_holding = portfolios_df[
            (portfolios_df['investor_discord_id'] == investor_id) &
            (portfolios_df['stock_in_game_name'] == target_name)
        ]
        if not existing_holding.empty:
            portfolios_df.loc[existing_holding.index, 'shares_owned'] += shares_purchased
        else:
            new_row = pd.DataFrame([{
                'investor_discord_id': investor_id,
                'stock_in_game_name': target_name,
                'shares_owned': shares_purchased
            }])
            portfolios_df = pd.concat([portfolios_df, new_row], ignore_index=True)

        # --- Save Updated Data ---
        crew_coins_df.to_csv('market/crew_coins.csv', index=False)
        portfolios_df.to_csv('market/portfolios.csv', index=False)
        
        # --- Log the Transaction ---
        target_id = crew_coins_df[crew_coins_df['in_game_name'] == target_name]['discord_id'].iloc[0]
        log_market_transaction(
            actor_id=investor_id, transaction_type='INVEST', target_id=target_id,
            item_name=f"{target_name}'s Stock", item_quantity=shares_purchased,
            cc_amount=-cc_amount, fee_paid=broker_fee
        )

        # --- Confirmation Message ---
        embed = discord.Embed(title="✅ Investment Successful", color=discord.Color.green())
        embed.description = (
            f"You invested **{format_cc(cc_amount)}** into **{target_name}**.\n"
            f"After a 3% Broker's Fee ({format_cc(broker_fee)}), you purchased **{shares_purchased:,.2f} shares**."
        )
        await ctx.send(embed=embed)

    finally:
        os.remove(lock_file) # Release lock


@bot.command(name="sell")
async def sell(ctx, member: str, shares_to_sell_str: str):
    """Sell a specific number of shares you own."""
    # --- Input Validation ---
    try:
        if shares_to_sell_str.lower() == 'all':
            shares_to_sell = -1 # Sentinel for 'all'
        else:
            shares_to_sell = float(shares_to_sell_str)
        if shares_to_sell <= 0 and shares_to_sell_str.lower() != 'all':
            await ctx.send("Please enter a positive number of shares to sell.", ephemeral=True)
            return
    except ValueError:
        await ctx.send("Invalid amount. Please enter a number (e.g., 50.5) or 'all'.", ephemeral=True)
        return

    seller_id = str(ctx.author.id)

    # --- File Locking ---
    lock_file = 'market/market.lock'
    if os.path.exists(lock_file):
        await ctx.send("The market is currently busy with another transaction. Please try again in a moment.", ephemeral=True)
        return
    open(lock_file, 'w').close()

    try:
        # --- Load Data ---
        crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
        stock_prices_df = load_market_file('stock_prices.csv')
        portfolios_df = load_market_file('portfolios.csv', dtype={'investor_discord_id': str})
        
        # --- Get Target Stock Info ---
        target_name = get_name_from_ticker_or_name(member)
        if not target_name:
            await ctx.send(f"Could not find a stock for a member or ticker named '{member}'.", ephemeral=True)
            return
        target_stock = stock_prices_df[stock_prices_df['in_game_name'] == target_name]
        if target_stock.empty:
            await ctx.send(f"Could not find a stock for '{member}'.", ephemeral=True)
            return
        target_name = target_stock['in_game_name'].iloc[0]
        current_price = float(target_stock['current_price'].iloc[0])

        # --- Get Seller's Holdings ---
        holding_index = portfolios_df[
            (portfolios_df['investor_discord_id'] == seller_id) &
            (portfolios_df['stock_in_game_name'] == target_name)
        ].index
        
        if holding_index.empty:
            await ctx.send(f"You do not own any shares of **{target_name}**.", ephemeral=True)
            return

        shares_owned = float(portfolios_df.loc[holding_index, 'shares_owned'].iloc[0])
        
        # Resolve 'all' amount
        if shares_to_sell == -1:
            shares_to_sell = shares_owned

        if shares_owned < shares_to_sell:
            await ctx.send(f"You don't have enough shares. You only own **{shares_owned:,.2f}** of **{target_name}**.", ephemeral=True)
            return

        # --- Transaction Calculation ---
        gross_value = shares_to_sell * current_price
        broker_fee = gross_value * 0.03
        net_proceeds = gross_value - broker_fee
        
        # --- Update DataFrames ---
        # 1. Update seller's balance
        crew_coins_df.loc[crew_coins_df['discord_id'] == seller_id, 'balance'] += net_proceeds

        # 2. Update seller's portfolio
        portfolios_df.loc[holding_index, 'shares_owned'] -= shares_to_sell
        # Remove the row if shares are zero to keep the file clean
        if portfolios_df.loc[holding_index, 'shares_owned'].iloc[0] < 0.001:
            portfolios_df.drop(holding_index, inplace=True)

        # --- Save Updated Data ---
        crew_coins_df.to_csv('market/crew_coins.csv', index=False)
        portfolios_df.to_csv('market/portfolios.csv', index=False)

        # --- Log the Transaction ---
        target_id = crew_coins_df[crew_coins_df['in_game_name'] == target_name]['discord_id'].iloc[0]
        log_market_transaction(
            actor_id=seller_id, transaction_type='SELL', target_id=target_id,
            item_name=f"{target_name}'s Stock", item_quantity=-shares_to_sell,
            cc_amount=net_proceeds, fee_paid=broker_fee
        )

        # --- Confirmation Message ---
        embed = discord.Embed(title="✅ Sale Successful", color=discord.Color.red())
        embed.description = (
            f"You sold **{shares_to_sell:,.2f} shares** of **{target_name}** for a gross value of {format_cc(gross_value)}.\n"
            f"After a 3% Broker's Fee ({format_cc(broker_fee)}), you received **{format_cc(net_proceeds)}**."
        )
        await ctx.send(embed=embed)

    finally:
        os.remove(lock_file) # Release lock

# --- Run the Bot ---
load_dotenv() # Loads variables from .env file
TOKEN = os.getenv('DISCORD_TOKEN')

if TOKEN is None:
    print("ERROR: DISCORD_TOKEN not found in .env file.")
else:
    bot.run(TOKEN)