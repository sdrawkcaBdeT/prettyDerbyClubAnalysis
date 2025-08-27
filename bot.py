import discord
from discord.ext import commands, tasks
import os
import csv
from datetime import datetime, timedelta
import pytz
import pandas as pd
import re
from dotenv import load_dotenv
from analysis import get_club_month_window 
import matplotlib.pyplot as plt
import io
import asyncio
import subprocess
import json
import random
from race_logic import Race, Horse
import skills # We don't use it directly, but race_logic needs it
from race_logic_v2 import Horse, Race, Bookie
from race_bots import BotManager
from market import database
import numpy as np
import math
from generate_visuals import generate_portfolio_image, format_pl_part

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
FAN_EXCHANGE_CHANNEL_NAME = 'fan-exchange'
WINNERS_CIRCLE_CHANNEL_NAME = 'winners-circle-racing'

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
            "description": "Increases Performance Yield multiplier by +2.75 per tier. Multiplier without upgrades (base) is 1.75.",
            "costs": [15000, 40000, 120000],
            "max_tier": 3,
            "type": "upgrade"
        },
        "p2": {
            "name": "Perfect the Starting Gate",
            "description": "Adds a flat +3 bonus to Performance Prestige before multiplication. No upgrade (base) value is 2.",
            "costs": [25000, 70000, 200000],
            "max_tier": 3,
            "type": "upgrade"
        }
    },
    "TENURE": {
        "t1": {
            "name": "Build Club Morale",
            "description": "Increases Tenure Yield multiplier by +0.2 per tier. No upgrade (base) value is 2.0.",
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

# --- NEW: Event Flavor Text ---
EVENT_FLAVOR_TEXT = {
    "Dark Horse Bargains": "A respected scout has been spotted in the stands, taking a keen interest in some overlooked talent! A few underdog stocks are now available at a surprising discount for the next 24 hours. Could this be the next big thing?",
    "Stewards' Tax Holiday": "By order of the board, a Tax Holiday is now in effect! For the next 72 hours, all broker's fees have been drastically reduced to encourage trading. Happy investing!",
    "Bumper Crowds": "The stands are packed! Concession and ticket sales are through the roof, and the owners are sharing the profits with all club members!",
    "Sponsor's Showcase": "It's a Sponsor's Showcase! For the next 48 hours, corporate partners are doubling the dividend payouts to the top shareholders of every racer. It pays to be a patron!",
    "Rival Club in Disarray": "Whispers are coming from the paddock... a rival club is in turmoil after a disastrous race day. Their misfortune has put our club in a favorable light, and market sentiment is soaring!",
    "The Crowd Roars": "The Crowd Roars! The top 5 most popular racers are getting a surge of support, boosting their market value!",
    "Jockey Change Announced": "A last-minute Jockey Change has been announced! This could shake up the field, making some racers more predictable and others a wild card.",
    "Headwind on the Back Stretch": "A strong Headwind on the Back Stretch is making it tough for racers to pull ahead based on pure performance. Experience and tenure will be key for the next couple of days.",
    "The Gate is Sticky": "There's a slight delay at the start... The Gate is Sticky! Several racers are off to a slow but steady start, neutralizing some of the usual chaos at the opening bell.",
    "False Start Declared": "A False Start has been declared by the stewards! They're reviewing the tapes, which may change the timing of how market data is processed. Stand by for the official results.",
    "Photo Finish Review": "It's a Photo Finish for the middle of the pack! The stewards are taking a close look at the recent performance of racers ranked 5th through 15th, making their stock prices extra sensitive."
}

# --- Bot Setup ---
intents = discord.Intents.default()
intents.message_content = True
intents.members = True # Required to find members by ID
bot = commands.Bot(command_prefix='/', intents=intents, help_command=None)


# --- Helper Functions ---

def get_inGameName(discord_id):
    """Looks up a user's in-game name from the registration file."""
    if not os.path.exists(USER_REGISTRATIONS_CSV):
        return None
    registrations_df = pd.read_csv(USER_REGISTRATIONS_CSV)
    user_entry = registrations_df[registrations_df['discord_id'] == discord_id]
    if not user_entry.empty:
        return user_entry.iloc[0]['inGameName']
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

async def send_to_fan_exchange(guild, message_content, file=None):
    """Finds the fan-exchange channel and sends a message there."""
    channel = discord.utils.get(guild.channels, name=FAN_EXCHANGE_CHANNEL_NAME)
    if channel:
        await channel.send(message_content, file=file)
    else:
        print(f"ERROR: Could not find the #{FAN_EXCHANGE_CHANNEL_NAME} channel.")

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

def get_club_rank(df, target_timestamp, inGameName):
    """Calculates the club rank for a specific member at a given time."""
    snapshot_df = df[df['timestamp'] <= target_timestamp].copy()
    if snapshot_df.empty: return None
    snapshot_df['totalMonthlyGain'] = snapshot_df.groupby('inGameName')['fanGain'].cumsum()
    latest_entries = snapshot_df.loc[snapshot_df.groupby('inGameName')['timestamp'].idxmax()]
    ranked_df = latest_entries.sort_values('totalMonthlyGain', ascending=False).reset_index()
    member_rank_series = ranked_df[ranked_df['inGameName'] == inGameName].index
    return member_rank_series[0] + 1 if not member_rank_series.empty else None

# --- HELPER FUNCTIONS FOR MARKET ---
def is_admin(ctx):
    """A check function to see if the user is an admin."""
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        admin_ids = config.get("ADMIN_DISCORD_IDS", [])
        return str(ctx.author.id) in admin_ids
    except FileNotFoundError:
        return False

@bot.command(name="award_cc")
@commands.check(is_admin)
async def award_cc(ctx, member: str, amount: int):
    """(Admin Only) Awards a specified amount of CC to a member."""
    admin_id = str(ctx.author.id)

    # --- 1. Input Validation ---
    if amount <= 0:
        return await ctx.send("Please enter a positive whole number for the amount.", ephemeral=True)

    target_name = get_name_from_ticker_or_name(member)
    if not target_name:
        return await ctx.send(f"Could not find a member or ticker named '{member}'.", ephemeral=True)

    # --- 2. File Locking for Safe Modification ---
    lock_file = 'market/market.lock'
    if os.path.exists(lock_file):
        return await ctx.send("The market is currently busy. Please try again in a moment.", ephemeral=True)
    open(lock_file, 'w').close()

    try:
        # --- 3. Core Logic: Modification ---
        crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
        target_user_row = crew_coins_df[crew_coins_df['inGameName'] == target_name]

        if target_user_row.empty:
            return await ctx.send(f"Could not find '{target_name}' in the crew_coins ledger.", ephemeral=True)

        # Add the CC to the user's balance
        crew_coins_df.loc[target_user_row.index, 'balance'] += amount
        crew_coins_df.to_csv('market/crew_coins.csv', index=False)

        # --- 4. Auditing ---
        target_id = target_user_row['discord_id'].iloc[0]
        log_market_transaction(
            actor_id=admin_id,
            transaction_type='ADMIN_AWARD',
            target_id=target_id,
            item_name="Admin Discretionary Award",
            item_quantity=1, # Not relevant here, but the function requires it
            cc_amount=amount,
            fee_paid=0
        )

        # --- 5. Confirmation ---
        new_balance = crew_coins_df.loc[target_user_row.index, 'balance'].iloc[0]
        embed = discord.Embed(
            title="✅ CC Awarded Successfully",
            description=f"You have awarded **{format_cc(amount)}** to **{target_name}**.",
            color=discord.Color.gold()
        )
        embed.set_footer(text=f"Their new balance is {format_cc(new_balance)}.")
        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"An unexpected error occurred: {e}", ephemeral=True)
    finally:
        # Always remove the lock file, even if an error occurs
        os.remove(lock_file)

@award_cc.error
async def award_cc_error(ctx, error):
    if isinstance(error, commands.CheckFailure):
        await ctx.send("You do not have permission to use this command.", ephemeral=True)


def load_market_file(filename, dtype=None):
    """Safely loads a CSV file from the market directory."""
    try:
        return pd.read_csv(f'market/{filename}', dtype=dtype)
    except FileNotFoundError:
        return pd.DataFrame()

def format_cc(amount):
    """Formats a number as a string with commas and 'CC'."""
    if pd.isna(amount):
        amount = 0
    return f"{float(amount):,.0f} CC"

def log_market_transaction(actor_id, transaction_type, target_id, item_name, item_quantity, cc_amount, fee_paid):
    """Logs a market transaction to the universal log file."""
    log_file = 'market/universal_transaction_log.csv'
    file_exists = os.path.isfile(log_file)
    central_tz = pytz.timezone('US/Central')
    now = datetime.now(central_tz)
    timestamp_str = now.strftime('%Y-%m-%d %H:%M:%S%z')
    timestamp = f"{timestamp_str[:-2]}:{timestamp_str[-2:]}"
    
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

def log_exchange_tax(actor_id, transaction_type, target_name, fee_paid):
    """Logs the broker fee from a transaction to the tax stash."""
    log_file = 'market/exchange_tax_stash.csv'
    file_exists = os.path.isfile(log_file)
    central_tz = pytz.timezone('US/Central')
    now = datetime.now(central_tz)
    timestamp_str = now.strftime('%Y-%m-%d %H:%M:%S%z')
    timestamp = f"{timestamp_str[:-2]}:{timestamp_str[-2:]}"

    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                'timestamp', 'actor_id', 'transaction_type', 'target_name', 'fee_paid'
            ])
        writer.writerow([
            timestamp, actor_id, transaction_type, target_name, fee_paid
        ])
        
def get_name_from_ticker_or_name(identifier: str):
    """
    Finds a member's full inGameName from either their ticker or name.
    Returns the inGameName on success, or None on failure.
    """
    # First, try to find a match in the tickers
    init_df = load_market_file('member_initialization.csv')
    if not init_df.empty:        
        # Explicitly treat the 'ticker' column as a string to handle empty values.
        init_df['ticker'] = init_df['ticker'].astype(str)
        # Tickers are stored in uppercase, so we search in uppercase
        ticker_match = init_df[init_df['ticker'].str.upper() == identifier.upper()]
        if not ticker_match.empty:
            return ticker_match.iloc[0]['inGameName']
            
    # If no ticker match, try to find a direct name match (case-insensitive)
    stock_prices_df = load_market_file('stock_prices.csv')
    if not stock_prices_df.empty:
        name_match = stock_prices_df[stock_prices_df['inGameName'].str.lower() == identifier.lower()]
        if not name_match.empty:
            return name_match.iloc[0]['inGameName']
            
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

async def announce_event(event_name):
    """Finds the fan-exchange channel and announces a new market event."""
    flavor_text = EVENT_FLAVOR_TEXT.get(event_name, f"A new market event has begun: **{event_name}**!")
    
    guild = bot.guilds[0]
    if not guild:
        print("Bot is not in any guild, cannot announce event.")
        return
        
    await send_to_fan_exchange(guild, f"**📢 MARKET UPDATE!**\n{flavor_text}")

def get_cost_basis(user_id: str, stock_name: str, transactions_df: pd.DataFrame):
    """Calculates the average cost basis for a user's holdings of a specific stock."""
    user_transactions = transactions_df[
        (transactions_df['actor_id'] == user_id) & 
        (transactions_df['item_name'] == f"{stock_name}'s Stock") &
        (transactions_df['transaction_type'] == 'INVEST')
    ]
    if user_transactions.empty:
        return 0
    total_cost = -user_transactions['cc_amount'].sum() # cc_amount is negative for INVEST
    total_shares = user_transactions['item_quantity'].sum()
    return total_cost / total_shares if total_shares > 0 else 0

def get_24hr_change(stock_name: str, history_df: pd.DataFrame, current_price: float):
    """Calculates the price change over the last 24 hours."""
    now = datetime.now(pytz.utc)
    one_day_ago = now - timedelta(days=1)
    
    stock_history = history_df[history_df['inGameName'] == stock_name].copy()
    if stock_history.empty:
        return 0, 0

    stock_history['timestamp'] = pd.to_datetime(stock_history['timestamp']).dt.tz_convert('UTC')
    
    # Find the most recent price from *before* 24 hours ago
    past_prices = stock_history[stock_history['timestamp'] <= one_day_ago]
    if past_prices.empty:
        # If no data from >24h ago, use the earliest known price
        past_price = stock_history['price'].iloc[0]
    else:
        past_price = past_prices.sort_values('timestamp', ascending=False)['price'].iloc[0]

    price_change = current_price - past_price
    percent_change = (price_change / past_price) * 100 if past_price != 0 else 0
    return price_change, percent_change

# --- Events ---
@bot.event
async def on_ready():
    """Runs once when the bot successfully connects to Discord."""
    print(f'Success! {bot.user} is online and ready.')
    print('------')
    # --- FIX: Start both tasks ---
    if not update_ranks_task.is_running():
        print("Starting scheduled rank update task...")
        update_ranks_task.start()
    if not check_for_announcements.is_running():
        print("Starting announcement checking task...")
        check_for_announcements.start()

@bot.event
async def on_command_completion(ctx):
    """Runs automatically after any command is successfully executed."""
    print(f"Command '{ctx.command.name}' was run by {ctx.author.name} in #{ctx.channel.name}")
    log_command_usage(ctx)


# --- Scheduled Task for Rank Updates ---
@tasks.loop(minutes=60)
async def update_ranks_task():
    """Periodically checks and updates member roles based on their prestige rank."""
    await bot.wait_until_ready()
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
        
    registrations_df.rename(columns={'inGameName': 'inGameName'}, inplace=True)
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

# --- NEW BACKGROUND TASK FOR ANNOUNCEMENTS ---
@tasks.loop(minutes=20) # Checks every 20 minutes
async def check_for_announcements():
    # This line ensures the task doesn't run until the bot is fully ready
    await bot.wait_until_ready()
    
    announcement_file = "announcements.txt"
    # Check if the file exists and is not empty
    if os.path.exists(announcement_file) and os.path.getsize(announcement_file) > 0:
        print("Found announcements to send...") # Added for logging
        # We found messages to send
        with open(announcement_file, "r+") as f:
            messages = f.readlines()
            f.truncate(0) # Clear the file immediately after reading

        # Get the guild (server) object
        if not bot.guilds:
            print("ERROR: Bot is not connected to any guilds. Cannot send announcement.")
            return
        guild = bot.guilds[0]

        for message in messages:
            message = message.strip() # Remove any leading/trailing whitespace
            if message: # Ensure we don't send empty lines
                print(f"Sending announcement from file: {message}")
                await send_to_fan_exchange(guild, message)

# --- Bot Commands ---

@bot.command()
async def help(ctx):
    """Displays a list of all available commands and their descriptions."""
    
    embed = discord.Embed(title="TRACKMASTER BOT Help", description="Here are all the commands you can use to track your performance and prestige.", color=discord.Color.blue())
    embed.add_field(name="SETUP (Required)", value="`/register [your-in-game-name]` - Links your Discord account to your exact in-game name. You must do this first!", inline=False)
    embed.add_field(name="PERSONAL STATS", value="`/myprogress` - Get a personalized summary of your progress since you last checked.", inline=False)
    embed.add_field(name="CLUB CHARTS & REPORTS", value=("`/top10` - Shows the current top 10 members by monthly fan gain.\n" "`/prestige_leaderboard` - Displays the all-time prestige point leaderboard.\n" "`/performance` - Posts the historical fan gain heatmap.\n" "`/log [member_name]` - Gets the detailed performance log for any member."), inline=False)
    embed.add_field(name="FAN EXCHANGE (Stock Market)", value=("`/exchange_help` - Provides a concise explanation and startup guide for the Fan Exchange system.\n" "`/market` - Displays the all stocks and some info.\n" "`/portfolio` - View your current stock holdings and their performance.\n" "`/stock [name/ticker]` - Shows the price history and stats for a specific racer.\n" "`/invest [name/ticker] [amount]` - Buy shares in a racer's stock, specifying CC to invest.\n" "`/sell [name/ticker] [amount]` - Sell shares of a racer's stock, specifying shares to sell.\n" "`/shop` - See what you can buy with your CC! Earnings upgrades and prestige!" "`/buy [shop_id]` - Purchase something from the shop!" "`/set_ticker [2-5 letter ticker]` - Set your unique stock ticker symbol."), inline=False)
    embed.add_field(name="FINANCIAL REPORTING", value=("`/financial_summary` - Get a high-level overview of your net worth, P/L, and ROI.\n" "`/earnings [7 or 30]` - View a detailed list of your income from earnings and dividends.\n" "`/ledger` - See a complete, paginated history of all your transactions."), inline=False)
    embed.set_footer(text="Remember to use the command prefix '/' before each command.")
    await ctx.send(embed=embed, ephemeral=True)

@bot.command()
async def register(ctx, *, inGameName: str):
    """Links your Discord account to your EXACT in-game name."""
    try:
        members_df = pd.read_csv(MEMBERS_CSV)
        if os.path.exists(USER_REGISTRATIONS_CSV):
            registrations_df = pd.read_csv(USER_REGISTRATIONS_CSV)
        else:
            registrations_df = pd.DataFrame(columns=['discord_id', 'inGameName'])
    except FileNotFoundError:
        await ctx.send("I'm missing the `members.csv` file. Please tell the admin.", ephemeral=True)
        return

    if inGameName not in members_df['inGameName'].values:
        await ctx.send(f"Sorry, I can't find a club member with the name **{inGameName}**. "
                       "Please make sure your name is spelled **EXACTLY** as it appears in-game, including capitalization and spaces.", ephemeral=True)
        return

    if inGameName in registrations_df['inGameName'].values:
        existing_reg = registrations_df[registrations_df['inGameName'] == inGameName]
        if existing_reg.iloc[0]['discord_id'] != ctx.author.id:
            await ctx.send(f"That in-game name is already registered to another Discord user. If this is an error, please contact an admin.", ephemeral=True)
            return

    registrations_df = registrations_df[registrations_df['discord_id'] != ctx.author.id]
    new_entry = pd.DataFrame([{'discord_id': ctx.author.id, 'inGameName': inGameName}])
    registrations_df = pd.concat([registrations_df, new_entry], ignore_index=True)
    registrations_df.to_csv(USER_REGISTRATIONS_CSV, index=False)
    
    await ctx.send(f"✅ Success! Your Discord account has been linked to the in-game name: **{inGameName}**. You can now use personal commands like `/myprogress`.", ephemeral=True)

# This defines a global 15-minute cooldown for the /refresh command
cooldown = commands.CooldownMapping.from_cooldown(1, 900, commands.BucketType.guild)

@bot.command(name="refresh")
async def refresh(ctx):
    """Triggers a manual refresh of all club and market data."""
    bucket = cooldown.get_bucket(ctx.message)
    retry_after = bucket.update_rate_limit()
    if retry_after:
        return await ctx.send(f"This command is on cooldown. Please try again in {round(retry_after)} seconds.", ephemeral=True)

    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        return await ctx.send("`config.json` not found. Manual refresh is disabled.", ephemeral=True)

    if not config.get("ALLOW_MANUAL_REFRESH", False):
        owner = await bot.fetch_user(config.get("OWNER_DISCORD_ID"))
        if owner:
            await owner.send(f"Hey! {ctx.author.name} tried to run a manual refresh, but it's disabled in the config.")
        return await ctx.send("Manual refresh is currently disabled by the admin.", ephemeral=True)

    await ctx.send("🔄 Manual data refresh initiated... This should take about a minute.")

    try:
        # --- CORRECTED LOGIC ---
        # Use Popen to run the script in the background without blocking the bot.
        # We run the 'full_run' sequence once and then exit.
        process = subprocess.Popen(['python', 'race_day_scheduler.py', 'full_run_once'])
        # Give the script time to run. You may need to adjust this value.
        await asyncio.sleep(120) 

        # Now we can send the completion message.
        await ctx.send("✅ Data refresh complete! The market has been updated.")
        
        # Check for the flag file to see if a new event was triggered
        event_flag_file = 'market/new_event.txt'
        if os.path.exists(event_flag_file):
            with open(event_flag_file, 'r') as f:
                new_event_name = f.read().strip()
            if new_event_name:
                await announce_event(new_event_name)
            os.remove(event_flag_file)

    except Exception as e:
        await ctx.send("❌ An error occurred during the data refresh. The admin has been notified.")
        owner = await bot.fetch_user(config.get("OWNER_DISCORD_ID"))
        if owner:
            error_message = f"**CRITICAL ERROR in `/refresh` triggered by {ctx.author.name}:**\n```\n{e}\n```"
            await owner.send(error_message[:1990])


@bot.command(name="exchange_help")
async def exchange_help(ctx):
    """Provides a concise explanation and startup guide for the Fan Exchange system."""
    embed = discord.Embed(
        title="Welcome to the Fan Exchange!",
        description="A stock market where the stocks are **us**.",
        color=discord.Color.blue()
    )
    embed.add_field(
        name="🚀 Getting Started: Your First 5 Steps",
        value="1️⃣ **Register** if you haven't! Use `/register [in-game name]`.\n"
              "2️⃣ **Set Your Ticker**! Use `/set_ticker [2-5 letter ticker]` to create your unique market ID.\n"
              "3️⃣ **Explore**! Use `/market` to see top players and `/stock [name/ticker]` to view their history.\n"
              "4️⃣ **Strategize**! Check the `/shop` for permanent CC upgrades or `/portfolio` to see your starting Crew Coins.\n"
              "5️⃣ **Participate**! `/invest` in members you believe in, or `/buy` upgrades to boost your own earnings! \n"
              "6️⃣ **Check-in**! `/portfolio` to check how you're doing, and `/sell` to capture growth!",
        inline=False
    )
    embed.add_field(
        name="💰 What are Crew Coins (CC)?",
        value="**Crew Coins (CC)** are the official currency. You earn them automatically from the **Prestige** you earn from tenure and in-game Fan gains. The more active you are, the more you earn.",
        inline=False
    )
    embed.add_field(
        name="📈 Advanced Mechanics",
        value="**Hype Bonus**: The more members who own your stock, the bigger the bonus to your personal CC earnings!\n"
              "**Sponsorship Deal**: Become the single largest shareholder of a stock to earn a **10% dividend** on that member's total CC earnings!\n"
              "**Shop Upgrades**: Use the `/shop` to spend CC on permanent upgrades that boost your Performance (active) and Tenure (passive) Yields, increasing your income.",
        inline=False
    )
    embed.add_field(
        name="🤖 Core Commands",
        value="`/portfolio` - See your CC and stock holdings.\n"
              "`/financial_summary` - View your overall Net Worth and P/L.\n"
              "`/earnings` - Check your recent income history.\n"
              "`/ledger` - Review every transaction you've ever made.\n"
              "`/market` - View the top market movers.\n"
              "`/stock [name/ticker]` - Get info on a specific stock.\n"
              "`/invest` & `/sell` - Buy and sell shares.\n"
              "`/shop` & `/buy` - Spend your CC!",
        inline=False
    )
    await ctx.send(embed=embed, ephemeral=True)

@bot.command()
async def myprogress(ctx):
    """Provides a personalized progress report since the user's last request."""
    inGameName = get_inGameName(ctx.author.id)
    if not inGameName:
        await ctx.send("You need to register first! Use `/register [your-exact-in-game-name]`", ephemeral=True)
        return
    
    try:
        ranks_df = pd.read_csv(RANKS_CSV)
        enriched_df = pd.read_csv(ENRICHED_FAN_LOG_CSV)
        enriched_df['timestamp'] = pd.to_datetime(enriched_df['timestamp'])
        progress_df = pd.read_csv(PROGRESS_LOG_CSV) if os.path.exists(PROGRESS_LOG_CSV) else pd.DataFrame(columns=['discord_id', 'last_checked_timestamp'])
    except FileNotFoundError as e:
        await ctx.send(f"Missing a data file (`{e.filename}`). Please run the analysis.", ephemeral=True)
        return

    user_analysis_df = enriched_df[enriched_df['inGameName'] == inGameName].sort_values('timestamp')
    if user_analysis_df.empty:
        await ctx.send("I couldn't find any analysis data for you yet.", ephemeral=True)
        return

    last_checked_entry = progress_df[progress_df['discord_id'] == ctx.author.id]
    last_checked_timestamp = pd.to_datetime(last_checked_entry.iloc[0]['last_checked_timestamp']) if not last_checked_entry.empty else user_analysis_df.iloc[0]['timestamp']
    
    time_since_last_check = datetime.now(pytz.utc) - last_checked_timestamp.tz_convert('UTC')
    progress_period_df = user_analysis_df[user_analysis_df['timestamp'] > last_checked_timestamp]
    
    before_stats = user_analysis_df[user_analysis_df['timestamp'] <= last_checked_timestamp].iloc[-1] if not progress_period_df.empty else user_analysis_df.iloc[-1]
    after_stats = user_analysis_df.iloc[-1]

    fans_gained = progress_period_df['fanGain'].sum()
    lifetime_prestige_gained = after_stats['lifetimePrestige'] - before_stats['lifetimePrestige']
    
    rank_before = get_club_rank(enriched_df, last_checked_timestamp, inGameName)
    rank_after = get_club_rank(enriched_df, after_stats['timestamp'], inGameName)
    rank_change = (rank_before - rank_after) if rank_before and rank_after else 0
    
    start_date, end_date = get_club_month_window(datetime.now(pytz.timezone('US/Central')))
    user_monthly_logs = user_analysis_df[(user_analysis_df['timestamp'] >= start_date) & (user_analysis_df['timestamp'] <= end_date)]
    
    eom_projection = 0
    if not user_monthly_logs.empty:
        first_log, latest_log = user_monthly_logs.iloc[0], user_monthly_logs.iloc[-1]
        fan_contribution = latest_log['fanCount'] - first_log['fanCount']
        hrs_elapsed = (latest_log['timestamp'] - first_log['timestamp']).total_seconds() / 3600
        fans_per_hour = fan_contribution / hrs_elapsed if hrs_elapsed > 0 else 0
        hrs_remaining = (end_date - latest_log['timestamp']).total_seconds() / 3600
        eom_projection = fan_contribution + (fans_per_hour * hrs_remaining)

    time_ago_str = format_timedelta_ddhhmm(time_since_last_check)
    fans_line = f"**Fans:** You've gained **{fans_gained:,.0f}** fans. Your EOM projection is **{eom_projection:,.0f}**."
    
    next_rank_index = ranks_df[ranks_df['rank_name'] == after_stats['prestigeRank']].index
    next_rank_name = "Max Rank"
    if not next_rank_index.empty and next_rank_index[0] + 1 < len(ranks_df):
        next_rank_name = ranks_df.iloc[next_rank_index[0] + 1]['rank_name']
        
    # --- UPDATED (Task 3.1) ---
    monthly_prestige_line = f"**Monthly Prestige:** You have **{after_stats['monthlyPrestige']:,.2f}** this month. You need **{after_stats['pointsToNextRank']:,.2f}** more for **{next_rank_name}**."
    lifetime_prestige_line = f"**Lifetime Prestige:** You gained **{lifetime_prestige_gained:,.2f}**, bringing your total to **{after_stats['lifetimePrestige']:,.2f}**."
    
    rank_change_str = f"moved up **{abs(rank_change)}** spots" if rank_change > 0 else (f"moved down **{abs(rank_change)}** spots" if rank_change < 0 else "held your ground")
    club_rank_line = f"**Club Rank:** You've {rank_change_str} and are now **#{rank_after}** in monthly fan gain."

    response_message = (f"{ctx.author.mention}, here's your progress from the last **{time_ago_str}**.\n"
                        f"{fans_line}\n"
                        f"{monthly_prestige_line}\n"
                        f"{lifetime_prestige_line}\n"
                        f"{club_rank_line}")
    
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
    inGameName = get_inGameName(ctx.author.id)
    if not inGameName:
        await ctx.send("You must be registered with `/register` to set a ticker.", ephemeral=True)
        return

    # --- Validation ---
    ticker = ticker.upper()
    if not (2 <= len(ticker) <= 5 and ticker.isalpha()):
        await ctx.send("Ticker must be 2-5 letters (A-Z).", ephemeral=True)
        return

    # --- REFACTORED: Use database function ---
    # The file lock is no longer needed.
    success = database.update_user_ticker(inGameName, ticker)

    if success:
        embed = discord.Embed(
            title="✅ Ticker Set Successfully!",
            description=f"Your official stock ticker is now **{ticker}**.",
            color=discord.Color.purple()
        )
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"The ticker **{ticker}** might already be taken, or an error occurred. Please try another.", ephemeral=True)
        
@bot.command(name="shop")
async def shop(ctx):
    """Displays the Prestige Shop with available items and your upgrade tiers."""
    user_id = str(ctx.author.id)

    # --- REFACTORED: Get all shop data in one go ---
    shop_data = database.get_shop_data(user_id)
    if not shop_data:
        return await ctx.send("Could not retrieve your account data. Are you registered?", ephemeral=True)

    balance = shop_data['balance']
    user_upgrades = shop_data['upgrades']
    current_prestige = shop_data['prestige']

    embed = discord.Embed(title="Prestige Shop", description="Spend your Crew Coins to get ahead!", color=discord.Color.purple())
    embed.set_footer(text=f"Your current balance: {format_cc(balance)}")

    prestige_text = ""
    for item_id, item_details in SHOP_ITEMS['PRESTIGE'].items():
        # Using your original helper function for dynamic cost
        bundle_cost = calculate_prestige_bundle_cost(current_prestige, item_details['amount'])
        prestige_text += f"**{item_details['name']} (ID: `{item_id}`)**\nCost: **{format_cc(bundle_cost)}**\n\n"
    embed.add_field(name="--- Prestige Purchases ---", value=prestige_text, inline=False)

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

    item_details = None
    for category in SHOP_ITEMS.values():
        if item_id in category:
            item_details = category[item_id]
            break
    
    if not item_details:
        return await ctx.send("Invalid item ID. Use `/shop` to see available items.", ephemeral=True)
    
    # --- REFACTORED: Get necessary data from DB ---
    shop_data = database.get_shop_data(user_id)
    if not shop_data:
        return await ctx.send("Could not retrieve your account data.", ephemeral=True)

    balance = shop_data['balance']
    user_upgrades = shop_data['upgrades']
    current_prestige = shop_data['prestige']
    
    cost = 0
    new_tier = None
    
    if item_details['type'] == 'upgrade':
        upgrade_name = item_details['name']
        current_tier = user_upgrades.get(upgrade_name, 0)
        
        if current_tier >= item_details['max_tier']:
            return await ctx.send(f"You have reached the max tier for **{upgrade_name}**.", ephemeral=True)
        
        cost = item_details['costs'][current_tier]
        new_tier = current_tier + 1

    elif item_details['type'] == 'prestige':
        cost = calculate_prestige_bundle_cost(current_prestige, item_details['amount'])

    if balance < cost:
        return await ctx.send(f"You need {format_cc(cost)} but only have {format_cc(balance)}.", ephemeral=True)
    
    # --- REFACTORED: Execute purchase as a single transaction ---
    # The file lock is no longer needed.
    new_balance = database.execute_purchase_transaction(
        actor_id=user_id, 
        item_name=item_details['name'], 
        cost=cost, 
        upgrade_tier=new_tier # This will be None for prestige items
    )

    if new_balance is not None:
        if item_details['type'] == 'prestige':
            # The purchase is logged, but we still need to manually update the CSV for now
            # This is a temporary measure until prestige is fully in the DB
            try:
                inGameName = database.get_inGameName_by_discord_id(user_id) # Assumes this function exists
                enriched_df = pd.read_csv('enriched_fan_log.csv')
                latest_stats = enriched_df[enriched_df['inGameName'] == inGameName].sort_values('timestamp').iloc[-1]
                new_prestige_row = latest_stats.copy()
                new_prestige_row['timestamp'] = datetime.now(pytz.timezone('US/Central')).isoformat()
                new_prestige_row['prestigeGain'] = float(item_details['amount'])
                new_prestige_row['lifetimePrestige'] += float(item_details['amount'])
                enriched_df = pd.concat([enriched_df, new_prestige_row.to_frame().T], ignore_index=True)
                enriched_df.to_csv('enriched_fan_log.csv', index=False)
            except Exception as e:
                print(f"CRITICAL: DB purchase succeeded but CSV update for prestige failed: {e}")

        embed = discord.Embed(title="✅ Purchase Successful!", description=f"You spent **{format_cc(cost)}** on **{item_details['name']}**.", color=discord.Color.green())
        embed.set_footer(text=f"Your new balance is {format_cc(new_balance)}")
        await ctx.send(embed=embed)
    else:
        await ctx.send("❌ Purchase failed due to insufficient funds or a database error.", ephemeral=True)

@bot.command(name="portfolio")
async def portfolio(ctx):
    """Displays account summary and a paginated image of stock holdings."""
    user_id = str(ctx.author.id)
    inGameName = get_inGameName(user_id)

    # --- 1. Data Fetching and Calculations (Unchanged) ---
    balance = database.get_user_balance_by_discord_id(user_id)
    if balance is None:
        return await ctx.send("You do not have a Fan Exchange account yet.", ephemeral=True)

    portfolio_df = database.get_portfolio_details(user_id)
    market_snapshot, _ = database.get_market_snapshot()
    sponsorships_list = database.get_sponsorships(user_id)

    total_stock_value = 0
    total_day_change = 0

    if not portfolio_df.empty and market_snapshot is not None:
        portfolio_df = pd.merge(portfolio_df, market_snapshot[['ingamename', 'price_24h_ago']], left_on='stock_ingamename', right_on='ingamename', how='left')
        portfolio_df['value'] = portfolio_df['shares_owned'] * portfolio_df['current_price']
        portfolio_df['pl'] = (portfolio_df['current_price'] - portfolio_df['cost_basis']) * portfolio_df['shares_owned']
        portfolio_df['pl_percent'] = portfolio_df.apply(
            lambda row: (row['pl'] / (row['cost_basis'] * row['shares_owned'])) * 100 if row['cost_basis'] > 0 and row['shares_owned'] > 0 else 0,
            axis=1
        )
        day_price_change = portfolio_df['current_price'] - portfolio_df['price_24h_ago']
        portfolio_df['day_change_value'] = day_price_change * portfolio_df['shares_owned']
        total_stock_value = portfolio_df['value'].sum()
        total_day_change = portfolio_df['day_change_value'].sum()

    total_portfolio_value = float(balance) + total_stock_value
    total_day_change_percent = (total_day_change / (total_portfolio_value - total_day_change)) * 100 if (total_portfolio_value - total_day_change) != 0 else 0
    
    portfolio_df.sort_values('value', ascending=False, inplace=True)

    sponsorships = []
    for sponsor_info in sponsorships_list:
        lead = float(sponsor_info['lead_amount'])
        sponsorship_text = f"(by {lead:.2f} sh)" if lead > 0 else "(Sole Owner)"
        sponsorship_name = f"${sponsor_info['ticker']}" if pd.notna(sponsor_info['ticker']) else sponsor_info['stock_ingamename']
        sponsorships.append(f"{sponsorship_name} {sponsorship_text}")

    # --- 2. Pagination and Embed Generation ---
    stocks_per_page = 10
    pages = [portfolio_df.iloc[i:i + stocks_per_page] for i in range(0, len(portfolio_df), stocks_per_page)]
    if not pages: # Ensure there's at least one (empty) page
        pages.append(pd.DataFrame())
    total_pages = len(pages)
    current_page = 0

    # This function now builds the complete embed, including the summary and footer.
    async def get_page_content(page_num):
        page_data = pages[page_num]
        
        # Generate the image for the current page's holdings
        image_file = generate_portfolio_image(page_data)
        
        embed = discord.Embed(
            title=f"{ctx.author.display_name}'s Portfolio",
            description=f"*In-Game Name: {inGameName}*",
            color=discord.Color.gold()
        )
        
        # --- SUMMARY AND FOOTER ARE NOW PART OF THE EMBED ---
        summary_text = (
            f"```\n"
            f"CC Balance:     {format_cc(balance)}\n"
            f"Stock Value:    {format_cc(total_stock_value)}\n"
            f"Net Worth:      {format_cc(total_portfolio_value)}\n"
            f"24 Hour Δ:      {'+' if total_day_change >= 0 else ''}{format_cc(total_day_change)}, {'+' if total_day_change_percent >= 0 else ''}{total_day_change_percent:.2f}%\n"
            f"```"
        )
        embed.add_field(name="💰 Account Summary", value=summary_text, inline=False)
        
        # The title of this field now acts as the holdings header
        embed.add_field(
            name=f"📈 Stock Holdings (Page {page_num + 1}/{total_pages})",
            value="See image below.",
            inline=False
        )

        if sponsorships:
            embed.set_footer(text=f"🏆 Sponsorships: {', '.join(sponsorships)}")
        
        embed.set_image(url=f"attachment://{image_file.filename}")
        return embed, image_file

    # --- 3. Initial Display and View Setup ---
    initial_embed, initial_file = await get_page_content(current_page)
    
    view = discord.ui.View()
    prev_button = discord.ui.Button(label="◀️ Previous", style=discord.ButtonStyle.secondary, disabled=True)
    next_button = discord.ui.Button(label="Next ▶️", style=discord.ButtonStyle.secondary, disabled=total_pages <= 1)

    async def prev_callback(interaction: discord.Interaction):
        nonlocal current_page
        current_page -= 1
        embed, file = await get_page_content(current_page)
        prev_button.disabled = current_page == 0
        next_button.disabled = False
        await interaction.response.edit_message(embed=embed, attachments=[file], view=view)

    async def next_callback(interaction: discord.Interaction):
        nonlocal current_page
        current_page += 1
        embed, file = await get_page_content(current_page)
        next_button.disabled = current_page >= total_pages - 1
        prev_button.disabled = False
        await interaction.response.edit_message(embed=embed, attachments=[file], view=view)

    prev_button.callback = prev_callback
    next_button.callback = next_callback
    view.add_item(prev_button)
    view.add_item(next_button)

    await ctx.send(embed=initial_embed, file=initial_file, view=view)

@bot.command(name="market")
async def market(ctx):
    """Displays a comprehensive overview of the stock market with pagination."""
    market_df, volume_24h = database.get_market_snapshot()
    if market_df is None or market_df.empty:
        return await ctx.send("Market is currently closed or has insufficient data.", ephemeral=True)

    # --- Calculations (Your original logic, now on DB data) ---
    market_df['price_change'] = market_df['current_price'] - market_df['price_24h_ago']
    # Avoid division by zero if the price 24h ago was 0
    market_df['percent_change'] = (market_df['price_change'] / market_df['price_24h_ago'].replace(0, np.nan)) * 100
    
    total_market_cap = market_df['market_cap'].sum()
    stocks_up = len(market_df[market_df['price_change'] > 0])
    market_sentiment = "Bullish" if stocks_up > len(market_df) / 2 else "Bearish" if stocks_up < len(market_df) / 2 else "Neutral"

    sorted_by_change = market_df.sort_values('current_price', ascending=False, na_position='last')
    
    # --- PAGINATION LOGIC (Your original logic) ---
    stocks_per_page = 15
    # Sort by price for the paginated display
    sorted_by_price = market_df.sort_values('current_price', ascending=False)
    pages = [sorted_by_price.iloc[i:i + stocks_per_page] for i in range(0, len(sorted_by_price), stocks_per_page)]
    if not pages: pages.append(pd.DataFrame())
    current_page = 0

    async def generate_embed(page_num):
        embed = discord.Embed(title="Baggins Index Market Overview", description="*A snapshot of all market activity.*", color=discord.Color.blue())
        stats_text = (
            f"```\n"
            f"Market Sentiment:   {market_sentiment} ({stocks_up} up, {len(market_df) - stocks_up} down)\n"
            f"Total Market Cap:   {format_cc(total_market_cap)}\n"
            f"24h Volume:         {format_cc(volume_24h)}\n"
            f"```"
        )
        embed.add_field(name="📈 Market-Wide Statistics", value=stats_text, inline=False)
        
        if not sorted_by_change.empty:
            top_gainer = sorted_by_change.iloc[0]
            biggest_drop = sorted_by_change.iloc[-1]
            gainer_ticker = f"{top_gainer['ticker']}" if pd.notna(top_gainer['ticker']) else top_gainer['ingamename']
            drop_ticker = f"{biggest_drop['ticker']}" if pd.notna(biggest_drop['ticker']) else biggest_drop['ingamename']
            movers_text = f"**Biggest Gainer:** {gainer_ticker} ({'+' if top_gainer['percent_change'] >= 0 else ''}{top_gainer['percent_change']:.1f}%)\n"
            movers_text += f"**Biggest Drop:** {drop_ticker} ({'+' if biggest_drop['percent_change'] >= 0 else ''}{biggest_drop['percent_change']:.1f}%)"
            embed.add_field(name="🔥 Top Movers (Last 24h)", value=movers_text, inline=False)
        
        page_data = pages[page_num]
        list_text = "```\n"
        list_text += "{:<16} | {:<7} | {:<8} | {}\n".format("Ticker", "Price", "24h Δ", "Largest Holder")
        list_text += "-"*56 + "\n"
        for _, stock in page_data.iterrows():
            display_name = f"{stock['ticker']}" if pd.notna(stock['ticker']) else stock['ingamename']
            change_str = f"{'+' if stock['percent_change'] >= 0 else ''}{stock['percent_change']:.1f}%"
            holder_info = f"{float(stock['largest_holder_shares']):.1f} sh" if pd.notna(stock['largest_holder']) else "N/A"

            list_text += "{:<16} | {:<7.2f} | {:<8} | {}\n".format(
                display_name[:15], float(stock['current_price']), change_str, holder_info
            )
        list_text += "```"
        embed.add_field(name=f"📊 Full Stock List (Page {page_num + 1}/{len(pages)})", value=list_text, inline=False)
        return embed

    view = discord.ui.View()
    prev_button = discord.ui.Button(label="◀️ Previous", style=discord.ButtonStyle.secondary, disabled=True)
    next_button = discord.ui.Button(label="Next ▶️", style=discord.ButtonStyle.secondary, disabled=len(pages) <= 1)

    async def prev_callback(interaction):
        nonlocal current_page
        current_page -= 1
        prev_button.disabled = current_page == 0
        next_button.disabled = False
        await interaction.response.edit_message(embed=await generate_embed(current_page), view=view)

    async def next_callback(interaction):
        nonlocal current_page
        current_page += 1
        next_button.disabled = current_page == len(pages) - 1
        prev_button.disabled = False
        await interaction.response.edit_message(embed=await generate_embed(current_page), view=view)

    prev_button.callback = prev_callback
    next_button.callback = next_callback
    view.add_item(prev_button)
    view.add_item(next_button)

    await ctx.send(embed=await generate_embed(current_page), view=view)

@bot.command(name="stock")
async def stock(ctx, *, identifier: str):
    """Displays detailed information and a price chart for a given stock."""
    stock_info, history_df, top_holders_df = database.get_stock_details(identifier)
    
    if not stock_info:
        return await ctx.send(f"Could not find a stock for '{identifier}'.", ephemeral=True)

    ingamename = stock_info['ingamename']
    current_price = float(stock_info['current_price'])
    
    # --- Calculations ---
    price_24h_ago = current_price
    all_time_high = current_price
    all_time_low = current_price

    if not history_df.empty:
        # This conversion is now correct and necessary
        history_df['timestamp'] = pd.to_datetime(history_df['timestamp'])
        
        past_prices = history_df[history_df['timestamp'] < (datetime.now(pytz.utc) - timedelta(days=1))]
        if not past_prices.empty:
            price_24h_ago = float(past_prices['price'].iloc[-1])
        
        all_time_high = history_df['price'].max()
        all_time_low = history_df['price'].min()

    price_change = current_price - price_24h_ago
    percent_change = (price_change / price_24h_ago) * 100 if price_24h_ago > 0 else 0
    
    market_snapshot, _ = database.get_market_snapshot()
    market_cap = 0
    if market_snapshot is not None:
        stock_market_info = market_snapshot[market_snapshot['ingamename'] == ingamename]
        if not stock_market_info.empty:
            market_cap = stock_market_info['market_cap'].iloc[0]

    display_ticker = f" ({stock_info['ticker']})" if stock_info['ticker'] else ""
    embed = discord.Embed(title=f"Stock Info: {ingamename}{display_ticker}", color=discord.Color.green())
    
    stats_text = (
        f"```\n"
        f"Current Price:    {current_price:,.2f} CC\n"
        f"Today's Change:   {'+' if price_change >= 0 else ''}{price_change:,.2f} CC ({'+' if percent_change >= 0 else ''}{percent_change:.2f}%)\n"
        f"All-Time High:    {all_time_high:,.2f} CC\n"
        f"All-Time Low:     {all_time_low:,.2f} CC\n"
        f"Market Cap:       {format_cc(market_cap)}\n"
        f"```"
    )
    embed.add_field(name="📊 Key Statistics", value=stats_text, inline=False)
    
    holders_text = "```\n"
    if not top_holders_df.empty:
        # --- THIS IS THE FIX ---
        # The loop is now structured correctly. 'i' gets the index, and 'holder'
        # gets the entire row as a pandas Series object.
        for i, holder in top_holders_df.iterrows():
            holders_text += f"{i+1}. {holder['ingamename']:<15} {float(holder['shares_owned']):.2f} shares\n"
    else:
        holders_text += "No public shareholders.\n"
    holders_text += "```"
    embed.add_field(name="🏆 Top 5 Shareholders", value=holders_text, inline=False)
    
    user_portfolio = database.get_portfolio_details(str(ctx.author.id))
    user_holding = user_portfolio[user_portfolio['stock_ingamename'] == ingamename]
    if not user_holding.empty:
        shares_owned = float(user_holding['shares_owned'].iloc[0])
        cost_basis = float(user_holding['cost_basis'].iloc[0])
        pl = (current_price - cost_basis) * shares_owned if cost_basis > 0 else 0
        pl_percent = (pl / (cost_basis * shares_owned)) * 100 if cost_basis > 0 and shares_owned > 0 else 0
        footer_text = f"Your Position: You own {shares_owned:.2f} shares with a P/L of {format_cc(pl)} ({'+' if pl_percent >= 0 else ''}{pl_percent:.1f}%)."
        embed.set_footer(text=footer_text)

    if not history_df.empty and len(history_df) > 1 and pd.api.types.is_datetime64_any_dtype(history_df['timestamp']):
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(history_df['timestamp'], history_df['price'], color='#00FF00', linewidth=2)
        
        ax.set_title(f'{ingamename} Price History', color='white')
        ax.set_ylabel('Price (CC)', color='white')
        ax.tick_params(axis='x', colors='white', rotation=15)
        ax.tick_params(axis='y', colors='white')
        ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
        fig.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', transparent=True)
        buf.seek(0)
        plt.close(fig)
        
        file = discord.File(buf, filename="price_chart.png")
        embed.set_image(url="attachment://price_chart.png")
        await ctx.send(embed=embed, file=file)
    else:
        await ctx.send(embed=embed)

@bot.command(name="financial_summary")
async def financial_summary(ctx):
    """Displays a user's high-level financial summary, including Net Worth, P/L, and ROI."""
    user_id = str(ctx.author.id)

    # --- FIX: Registration check is now performed against the database ---
    # This aligns the command's behavior with /portfolio and other core
    # market functions, using the database as the single source of truth.
    user_details = database.get_user_details(user_id)
    if not user_details:
        await ctx.send("You do not have a Fan Exchange account. Please use `/register` first.", ephemeral=True)
        return

    in_game_name = user_details['ingamename']

    # This command relies on a single, efficient database call that performs all
    # complex calculations (P/L, ROI, etc.) at the database level. This keeps the
    # bot's logic clean, simple, and fast.
    summary_data = database.get_financial_summary(user_id)

    # The embed is designed for clarity, using formatted fields to present
    # the key performance indicators (KPIs) in an easily digestible way.
    embed = discord.Embed(
        title=f"{ctx.author.display_name}'s Financial Summary",
        description=f"*In-Game Name: {in_game_name}*",
        color=discord.Color.blue()
    )

    # Determine color and sign for P/L and ROI for better visual feedback.
    p_l = summary_data['p_l']
    roi = summary_data['roi_percent']
    p_l_sign = "+" if p_l >= 0 else ""
    roi_sign = "+" if roi >= 0 else ""
    embed_color = discord.Color.green() if p_l >= 0 else discord.Color.red()
    embed.color = embed_color

    embed.add_field(
        name="💰 Net Worth",
        value=f"**{format_cc(summary_data['net_worth'])}**\n(CC Balance + Stock Value)",
        inline=False
    )
    embed.add_field(
        name="📈 Portfolio Value",
        value=f"{format_cc(summary_data['total_portfolio_value'])}",
        inline=True
    )
    embed.add_field(
        name="💸 Profit / Loss (P/L)",
        value=f"{p_l_sign}{format_cc(p_l)}",
        inline=True
    )
    embed.add_field(
        name="📊 Return on Investment (ROI)",
        value=f"{roi_sign}{roi:.2f}%",
        inline=True
    )
    embed.set_footer(text="All data reflects your lifetime market activity.")

    await ctx.send(embed=embed)

# bot.py

# (Replace the existing /earnings command with this new version)

@bot.command(name="earnings")
async def earnings(ctx, days: int = 7):
    """
    Displays your earnings history (periodic income and dividends) over the
    last 7 or 30 days in a paginated embed.
    """
    user_id = str(ctx.author.id)

    # --- 1. Input Validation & Data Fetching ---
    if days not in [7, 30]:
        await ctx.send("Please choose a valid period: 7 or 30 days.", ephemeral=True)
        return

    user_details = database.get_user_details(user_id)
    if not user_details:
        await ctx.send("You do not have a Fan Exchange account.", ephemeral=True)
        return

    earnings_df = database.get_earnings_history(user_id, days)

    if earnings_df.empty:
        await ctx.send(f"You have no earnings recorded in the last {days} days.", ephemeral=True)
        return

    # --- 2. Pagination Setup ---
    items_per_page = 10
    total_pages = math.ceil(len(earnings_df) / items_per_page)

    async def get_page_embed(page_num: int):
        start_index = page_num * items_per_page
        end_index = start_index + items_per_page
        page_df = earnings_df.iloc[start_index:end_index]

        embed = discord.Embed(
            title=f"Earnings History (Last {days} Days)",
            color=discord.Color.gold()
        )
        
        # --- SIMPLIFIED LAYOUT ---
        # This version removes the "Type" column for a cleaner look,
        # focusing only on the timestamp, amount, and the item itself.
        
        body = "```\n"
        # Adjusted header to be simpler.
        body += "{:<16} | {:>14} | {}\n".format("Timestamp (CT)", "Amount", "Item")
        body += "-" * 56 + "\n"

        central_tz = pytz.timezone('US/Central')

        for _, row in page_df.iterrows():
            timestamp = row['timestamp'].astimezone(central_tz)
            ts_str = timestamp.strftime('%Y/%m/%d %H:%M')
            
            # The item name is now the primary descriptor.
            transaction_type = row['transaction_type'] if pd.notna(row['transaction_type']) else 'OTHER'
            if len(transaction_type) > 25:
                transaction_type = transaction_type[:22] + "..."

            amount_str = f"{row['cc_amount']:,.2f} CC"

            # The format string is updated to reflect the new three-column layout.
            body += "{:<16} | {:>14} | {}\n".format(
                ts_str,
                amount_str,
                transaction_type
            )
        
        body += "```"
        embed.description = body
        embed.set_footer(text=f"Page {page_num + 1} of {total_pages}")
        return embed

    # --- 3. Initial Display ---
    initial_embed = await get_page_embed(0)
    view = PaginationView(ctx.interaction, get_page_embed, total_pages)
    await ctx.send(embed=initial_embed, view=view)

@bot.command(name="ledger")
async def ledger(ctx):
    """
    Displays a full, paginated ledger of all your transactions with a running balance.
    Provides interactive buttons to view detailed transaction metadata.
    """
    user_id = str(ctx.author.id)

    user_details = database.get_user_details(user_id)
    if not user_details:
        await ctx.send("You do not have a Fan Exchange account.", ephemeral=True)
        return

    ledger_df = database.get_transaction_ledger(user_id)

    if ledger_df.empty:
        await ctx.send("You have no transactions recorded.", ephemeral=True)
        return

    # --- THIS IS THE FIX for the context error ---
    # We now pass the standard `ctx` object to the view.
    view = LedgerPaginationView(ctx, ledger_df)
    initial_embed = view.get_current_page_embed()
    await ctx.send(embed=initial_embed, view=view)
    
class PaginationView(discord.ui.View):
    """
    A generic, reusable view for creating paginated embeds.
    This class handles the button logic and page state management.
    It's designed to be subclassed or used directly by commands that need pagination.
    """
    def __init__(self, interaction: discord.Interaction, get_page_embed_func, total_pages: int):
        super().__init__(timeout=180)
        self.interaction = interaction
        self.get_page_embed_func = get_page_embed_func
        self.total_pages = total_pages
        self.current_page = 0
        self.update_buttons()

    def update_buttons(self):
        """Disables/enables navigation buttons based on the current page."""
        # The `children` attribute is a list of all UI components in the view.
        # We can safely assume the first is 'prev' and the second is 'next'.
        self.children[0].disabled = self.current_page == 0
        self.children[1].disabled = self.current_page == self.total_pages - 1

    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # This callback decrements the page counter, updates the buttons, and edits
        # the original message with the new page's embed.
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            embed = await self.get_page_embed_func(self.current_page)
            await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # This callback increments the page counter, updates the buttons, and edits
        # the original message with the new page's embed.
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self.update_buttons()
            embed = await self.get_page_embed_func(self.current_page)
            await interaction.response.edit_message(embed=embed, view=self)



def format_transaction_details(details: dict, item_name: str, transaction_type: str) -> str:
    """
    A helper utility that intelligently formats the details of a transaction
    based on its type, providing a clear and readable breakdown.
    """
    if not details:
        return "No further details available for this transaction."

    formatted_string = ""
    
    if transaction_type in ('INVEST', 'SELL'):
        # Handles trades with correct signs and formatting
        formatted_string += f"**Stock**: {item_name}\n"
        shares = details.get('shares_transacted', 0)
        price = details.get('price_per_share', 0)
        subtotal = details.get('subtotal', 0)
        fee = details.get('fee_paid', 0)
        
        shares_sign = "+" if shares >= 0 else ""
        if transaction_type == "INVEST":
            formatted_string += f"**Shares PURCHASED**: {shares_sign}{shares:,.2f}\n" # Not currency
        
        if transaction_type == "SELL":
            formatted_string += f"**Shares SOLD**: {shares_sign}{shares:,.2f}\n" # Not currency
        
        formatted_string += f"**Price / Share**: {price:.2f} CC\n"
        formatted_string += f"**Subtotal**: {subtotal:.2f} CC\n"
        
        formatted_string += f"**Fee Paid**: {fee:.2f} CC\n"
        
        formatted_string += "---------------------------------\n"
        if transaction_type == "INVEST":            
            formatted_string += f"**TOTAL COST**: {subtotal+fee:.2f} CC\n"
        if transaction_type == "SELL":
            formatted_string += f"**NET PROCEEDS**: {subtotal-fee:.2f} CC\n"

    elif transaction_type == 'PERIODIC_EARNINGS':
        # Handles earnings with logical order and correct formatting
        key_order = [
            ('tenure_yield', 'Tenure Yield'),
            ('performance_yield', 'Performance Yield'),
            ('hype_multiplier', 'Hype Multiplier'),
            ('hype_bonus_yield', 'Hype Bonus Yield'),
            ('base_cc_earned', 'Personal CC Earned')
        ]
        
        for key, title in key_order:
            if key in details:
                value = details[key]
                if key == 'base_cc_earned':
                    formatted_string += "---------------------------------\n"
                if key == 'hype_multiplier':
                    formatted_string += f"**{title}**: {value:.2f}x\n" # Not currency
                else:
                    formatted_string += f"**{title}**: {value:.2f} CC\n"

    elif transaction_type == 'DIVIDEND':
        # Handles dividends with a clean, simple format
        source = details.get('source_player', 'Unknown')
        div_type = details.get('type', 'Dividend')
        formatted_string += f"**Source Player**: {source}\n"
        formatted_string += f"**Type**: {div_type}\n"

    else:
        # A fallback for any other type with details
        for key, value in details.items():
            title = key.replace('_', ' ').title()
            formatted_string += f"**{title}**: {value}\n"
            
    return formatted_string

class LedgerPaginationView(discord.ui.View):
    """
    A specialized pagination view for the transaction ledger with improved UI
    for linking transactions to their detail buttons.
    """
    def __init__(self, ctx, ledger_df: pd.DataFrame):
        super().__init__(timeout=180)
        self.ctx = ctx
        self.ledger_df = ledger_df
        self.items_per_page = 8
        self.total_pages = math.ceil(len(self.ledger_df) / self.items_per_page)
        self.current_page = 0
        self.details_cache = {}
        self.rebuild_view()

    def rebuild_view(self):
        """Clears and rebuilds the UI components for the current page."""
        self.clear_items()
        self.details_cache.clear()
        
        start_index = self.current_page * self.items_per_page
        end_index = start_index + self.items_per_page
        page_df = self.ledger_df.iloc[start_index:end_index]

        # Navigation buttons (row 0)
        prev_button = discord.ui.Button(label="◀️ Previous", style=discord.ButtonStyle.secondary, disabled=self.current_page == 0, row=0)
        next_button = discord.ui.Button(label="Next ▶️", style=discord.ButtonStyle.secondary, disabled=self.current_page >= self.total_pages - 1, row=0)
        prev_button.callback = self.prev_page
        next_button.callback = self.next_page
        self.add_item(prev_button)
        self.add_item(next_button)

        details_counter = 1
        for _, row in page_df.iterrows():
            if row['details'] is not None and pd.notna(row['details']):
                transaction_id = row['transaction_id']
                self.details_cache[transaction_id] = {
                    "details": row['details'],
                    "item_name": row['item_name'],
                    "transaction_type": row['transaction_type']
                }
                
                button_row = 1 + ((details_counter - 1) // 5)
                
                details_button = discord.ui.Button(
                    label=f"[{details_counter}] Details",
                    style=discord.ButtonStyle.primary,
                    custom_id=f"ledger_details:{transaction_id}",
                    row=button_row
                )
                details_button.callback = self.show_details
                self.add_item(details_button)
                details_counter += 1
    
    def get_current_page_embed(self) -> discord.Embed:
        """Generates the Discord embed for the current page."""
        start_index = self.current_page * self.items_per_page
        end_index = start_index + self.items_per_page
        page_df = self.ledger_df.iloc[start_index:end_index]

        embed = discord.Embed(title="Transaction Ledger", color=discord.Color.purple())
        
        body = ""
        details_counter = 1
        for _, row in page_df.iterrows():
            unix_ts = int(row['timestamp'].timestamp())
            cc_amount = row['cc_amount']
            sign = "+" if cc_amount >= 0 else ""
            
            item_display_name = row['item_name']
            if row['transaction_type'] == 'DIVIDEND' and row['details']:
                div_type = row['details'].get('type', 'Dividend')
                source = row['details'].get('source_player', 'Unknown')
                item_display_name = f"{div_type} ({source})"

            details_marker = ""
            if row['details'] is not None and pd.notna(row['details']):
                details_marker = f" `[{details_counter}]`"
                details_counter += 1
            
            body += (f"**<t:{unix_ts}:R>**: **{row['transaction_type']}**\n"
                     f"> Item: `{item_display_name}`\n"
                     f"> Amount: `{sign}{cc_amount:.2f} CC` | Balance: `{format_cc(row['running_balance'])}`{details_marker}\n")
        
        embed.description = body
        embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")
        return embed

    # --- THESE METHODS WERE MISSING ---
    async def prev_page(self, interaction: discord.Interaction):
        if self.current_page > 0:
            self.current_page -= 1
            self.rebuild_view()
            embed = self.get_current_page_embed()
            await interaction.response.edit_message(embed=embed, view=self)

    async def next_page(self, interaction: discord.Interaction):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self.rebuild_view()
            embed = self.get_current_page_embed()
            await interaction.response.edit_message(embed=embed, view=self)

    async def show_details(self, interaction: discord.Interaction):
        transaction_id = int(interaction.data['custom_id'].split(':')[1])
        cached_data = self.details_cache.get(transaction_id)
        
        formatted_details = format_transaction_details(
            cached_data['details'],
            cached_data['item_name'],
            cached_data['transaction_type']
        )
        
        await interaction.response.send_message(
            f"### Details for Transaction ID: `{transaction_id}`\n{formatted_details}",
            ephemeral=True
        )

# --- NEW: UI Class for Trade Confirmations ---
class TradeConfirmationView(discord.ui.View):
    def __init__(self, *, timeout=30, trade_details: dict):
        super().__init__(timeout=timeout)
        self.trade_details = trade_details
        self.confirmed = None

    @discord.ui.button(label='Confirm Trade', style=discord.ButtonStyle.green)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        # Disable buttons to prevent double-clicking
        for item in self.children:
            item.disabled = True
        # Update the original message to show the action was taken
        await interaction.response.edit_message(content="Processing your trade...", view=self)
        self.stop()

    @discord.ui.button(label='Cancel', style=discord.ButtonStyle.red)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = False
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Trade cancelled.", view=self)
        self.stop()

@bot.command(name="invest")
async def invest(ctx, identifier: str, shares_str: str):
    """Invest in a stock by purchasing a specific number of shares."""
    try:
        shares_to_buy = float(shares_str)
        if shares_to_buy <= 0:
            return await ctx.send("Please enter a positive number of shares to buy.", ephemeral=True)
    except ValueError:
        return await ctx.send("Invalid number of shares. Please enter a number (e.g., 10.5).", ephemeral=True)

    user_id = str(ctx.author.id)
    
    # 1. Get Data
    stock = database.get_stock_by_ticker_or_name(identifier)
    if not stock:
        return await ctx.send(f"Could not find a stock for '{identifier}'.", ephemeral=True)

    balance = database.get_user_balance_by_discord_id(user_id)
    if balance is None:
        return await ctx.send("You do not have a Fan Exchange account.", ephemeral=True)

    # 2. Calculate Trade Details
    current_price = float(stock['current_price'])
    subtotal = shares_to_buy * current_price
    broker_fee = subtotal * 0.03
    total_cost = subtotal + broker_fee

    if float(balance) < total_cost:
        return await ctx.send(f"Insufficient funds. You need {format_cc(total_cost)} but only have {format_cc(balance)}.", ephemeral=True)

    # 3. Confirmation UI
    new_balance_preview = float(balance) - total_cost
    embed = discord.Embed(title="Trade Confirmation: BUY", color=discord.Color.green())
    details = (
        f"**Stock**: {stock['ingamename']} (${stock['ticker']})\n"
        f"**Shares**: {shares_to_buy:,.2f}\n"
        f"**Price/Share**: {current_price:,.2f} CC\n"
        f"--------------------------\n"
        f"**Subtotal**: {subtotal:,.2f} CC\n"
        f"**Broker's Fee (3%)**: {broker_fee:,.2f} CC\n"
        f"**Total Cost**: **{total_cost:,.2f} CC**\n\n"
        f"*Your new balance will be: {new_balance_preview:,.2f} CC*"
    )
    embed.description = details
    # Get the discord_id of the person whose stock is being bought
    target_id = database.get_discord_id_by_name(stock['ingamename'])

    trade_details = {
        'actor_id': user_id,
        'target_id': target_id, # Add target_id
        'stock_name': stock['ingamename'],
        'shares': shares_to_buy,
        'price_per_share': current_price, # Add price_per_share
        'total_cost': -total_cost,
        'fee': broker_fee,
        'transaction_type': 'INVEST'
    }
    view = TradeConfirmationView(trade_details=trade_details)
    
    await ctx.send(embed=embed, view=view, ephemeral=True)
    await view.wait() # Wait for the user to click a button

    # 4. Execute Trade if Confirmed
    if view.confirmed:
        new_balance = database.execute_trade_transaction(**view.trade_details)
        if new_balance is not None:
            await ctx.send(f"✅ **Trade Executed!** You purchased {shares_to_buy:,.2f} shares of **{stock['ingamename']}**. Your new balance is {format_cc(new_balance)}.", ephemeral=True)
        else:
            # Changed from ctx.followup.send to ctx.send for prefix command compatibility.
            await ctx.send("❌ **Trade Failed!** This could be due to a price change or insufficient funds. Please try again.", ephemeral=True)

@bot.command(name="sell")
async def sell(ctx, identifier: str, shares_str: str):
    """Sell a specific number of shares you own."""
    try:
        shares_to_sell = float(shares_str)
        if shares_to_sell <= 0:
            return await ctx.send("Please enter a positive number of shares to sell.", ephemeral=True)
    except ValueError:
        return await ctx.send("Invalid number of shares. Please enter a number (e.g., 10.5).", ephemeral=True)

    user_id = str(ctx.author.id)

    # 1. Get Data
    stock = database.get_stock_by_ticker_or_name(identifier)
    if not stock:
        return await ctx.send(f"Could not find a stock for '{identifier}'.", ephemeral=True)

    portfolio = database.get_portfolio_details(user_id)
    user_holding = portfolio[portfolio['stock_ingamename'] == stock['ingamename']]
    
    if user_holding.empty or float(user_holding['shares_owned'].iloc[0]) < shares_to_sell:
        shares_owned = 0 if user_holding.empty else float(user_holding['shares_owned'].iloc[0])
        return await ctx.send(f"Insufficient shares. You are trying to sell {shares_to_sell:,.2f} but you only own {shares_owned:,.2f} of **{stock['ingamename']}**.", ephemeral=True)

    balance = database.get_user_balance_by_discord_id(user_id)

    # 2. Calculate Trade Details
    current_price = float(stock['current_price'])
    subtotal = shares_to_sell * current_price
    broker_fee = subtotal * 0.03
    total_proceeds = subtotal - broker_fee

    # 3. Confirmation UI
    new_balance_preview = float(balance) + total_proceeds
    embed = discord.Embed(title="Trade Confirmation: SELL", color=discord.Color.red())
    details = (
        f"**Stock**: {stock['ingamename']} (${stock['ticker']})\n"
        f"**Shares**: {shares_to_sell:,.2f}\n"
        f"**Price/Share**: {current_price:,.2f} CC\n"
        f"--------------------------\n"
        f"**Gross Proceeds**: {subtotal:,.2f} CC\n"
        f"**Broker's Fee (3%)**: {broker_fee:,.2f} CC\n"
        f"**Total Proceeds**: **{total_proceeds:,.2f} CC**\n\n"
        f"*Your new balance will be: {new_balance_preview:,.2f} CC*"
    )
    embed.description = details
    
    target_id = database.get_discord_id_by_name(stock['ingamename'])

    trade_details = {
        'actor_id': user_id,
        'target_id': target_id, # Add target_id
        'stock_name': stock['ingamename'],
        'shares': -shares_to_sell, # Selling removes shares
        'price_per_share': current_price, # Add price_per_share
        'total_cost': total_proceeds, # Proceeds are positive
        'fee': broker_fee,
        'transaction_type': 'SELL'
    }
    view = TradeConfirmationView(trade_details=trade_details)

    await ctx.send(embed=embed, view=view, ephemeral=True)
    await view.wait()

    # 4. Execute Trade if Confirmed
    if view.confirmed:
        new_balance = database.execute_trade_transaction(**view.trade_details)
        if new_balance is not None:
            await ctx.send(f"✅ **Trade Executed!** You sold {shares_to_sell:,.2f} shares of **{stock['ingamename']}**. Your new balance is {format_cc(new_balance)}.", ephemeral=True)
        else:
            # Changed from ctx.followup.send to ctx.send for prefix command compatibility.
            await ctx.send("❌ **Trade Failed!** This could be due to a price change or insufficient funds. Please try again.", ephemeral=True)
        
BOT_PERSONALITIES = {
    "StartingGateSally": {"type": "starter"},
    "PaddockPete": {"type": "starter"},
    "FirstTurnFrank": {"type": "starter"},
    "BackstretchBarry": {"type": "starter"},
    "HomestretchHarry": {"type": "starter"},
    "GrandstandGus": {"type": "starter"},
    "ClubhouseClara": {"type": "starter"},
    "BagginsTheBookie": {"type": "human_trigger"}, # The original bot
    "SniperSam": {"type": "sniper"},
    "DailyDoubleDoug": {"type": "sniper"},
    "PhotoFinishPhil": {"type": "sniper"},
}
        
active_races = {}  # In-memory dictionary to hold live Race objects

DEFINED_HORSES = ["Sakura Bakushin Oh", "Marzensty", "El", "Oguri Hat", "Gold Trip", "Earth Rut"]

HORSE_NAME_PARTS = {
    "adjectives": ["Galloping", "Midnight", "Dusty", "Iron", "Star", "Thunder", "Shadow", "Golden", "Baggins", "Cheating", "BOT", "Nice", "Mean", "Godly", "Inspector", "Dishonest", "Old School", "Broken", "Neon", "Lucky", "Turbo", "Sticky", "Captain", "Major", "Wild", "Furious", "Stoic", "Noble", "Cash"],
    "nouns": ["Bullet", "Fury", "Runner", "Chaser", "Stallion", "Dreamer", "Comet", "Baggins", "Cheater", "God", "Nature", "Gadget", "Wave", "Insomnia", "Twice", "Epidemic", "King", "Queen", "Jester", "Engine", "Glitch", "Rocket", "Mirror", "Major", "Pilgrim", "Mountain", "Crew"]
}
HORSE_STRATEGIES = ["Front Runner", "Pace Chaser", "Late Surger", "End Closer"]
GENERIC_SKILLS = ["Straightaway Adept", "Homestretch Haste", "Slipstream", "Late Start"]
UNIQUE_HORSES = {
    "Sakura Bakushin Oh": {"strategy": "Front Runner", "skills": ["Huge Lead"]},
    "Marzensty": {"strategy": "Front Runner", "skills": ["Early Lead"]},
    "El": {"strategy": "Pace Chaser", "skills": ["Victoria por plata"]},
    "Oguri Hat": {"strategy": "Pace Chaser", "skills": ["Trumpet Blast"]},
    "Gold Trip": {"strategy": "End Closer", "skills": ["Uma Stan"]},
    "Earth Rut": {"strategy": "Pace Chaser", "skills": ["Fiery Satisfaction"]},
}

def generate_race_field(num_horses_needed):
    """
    Generates a list of horse names for a race, prioritizing defined horses
    and filling the rest with unique, randomly generated names.
    """
    # 1. Start with the list of defined horses.
    available_horses = DEFINED_HORSES[:]
    
    # 2. Generate additional random horses if needed.
    while len(available_horses) < num_horses_needed:
        adj = random.choice(HORSE_NAME_PARTS["adjectives"])
        noun = random.choice(HORSE_NAME_PARTS["nouns"])
        new_name = f"{adj} {noun}"
        
        # 3. Ensure the new name is unique before adding it.
        if new_name not in available_horses:
            available_horses.append(new_name)
            
    # 4. Shuffle the list and return the exact number needed for the race.
    random.shuffle(available_horses)
    return available_horses[:num_horses_needed]

# --- Helper Functions for Racing ---
def get_or_create_csv(filepath, headers):
    """Checks if a CSV exists, creates it with headers if not."""
    if not os.path.exists(filepath):
        pd.DataFrame(columns=headers).to_csv(filepath, index=False)
        print(f"Created missing file: {filepath}")

def initialize_race_files():
    """Ensure all necessary CSV files for the racing game exist."""
    os.makedirs('market', exist_ok=True)
    # Define headers for all our files
    races_headers = ['race_id', 'message_id', 'channel_id', 'track_length', 'num_horses', 'status', 'winner', 'start_time']
    horses_headers = ['race_id', 'horse_number', 'horse_name', 'strategy', 'skills']
    bets_headers = ['race_id', 'bettor_id', 'horse_number', 'bet_amount', 'odds_at_bet', 'time_left_in_window', 'winnings']
    results_headers = ['race_id', 'horse_name', 'horse_number', 'strategy', 'skills', 'final_position', 'final_place']
    events_headers = ['race_id', 'round', 'horse_name', 'rolls_str', 'modifier', 'skill_bonus', 'total_movement', 'skill_roll', 'skill_chance', 'skills_activated', 'position_after']
    jackpot_headers = ['current_jackpot']

    # Create files if they don't exist
    get_or_create_csv('market/races.csv', races_headers)
    get_or_create_csv('market/race_horses.csv', horses_headers)
    get_or_create_csv('market/race_bets.csv', bets_headers)
    get_or_create_csv('market/race_results.csv', results_headers)
    get_or_create_csv('market/race_events.csv', events_headers)
    
    if not os.path.exists('market/jackpot_ledger.csv'):
        pd.DataFrame([{'current_jackpot': 0}]).to_csv('market/jackpot_ledger.csv', index=False)
    
    bot_ledger_file = 'market/bot_ledgers.csv'
    if not os.path.exists(bot_ledger_file):
        bot_data = [{'bot_name': name, 'bankroll': 10000, 'total_bets': 0, 'total_winnings': 0} for name in BOT_PERSONALITIES]
        pd.DataFrame(bot_data).to_csv(bot_ledger_file, index=False)

# Call initialization on startup
initialize_race_files()

# In bot.py

# In bot.py

async def _record_bet(race, bettor_id, horse_number, bet_amount):
    """Calculates live odds and time, then records the bet to the CSV."""
    loop = asyncio.get_running_loop()
    
    # 1. Calculate time remaining
    # FIX IS HERE: We wrap the command in a lambda to correctly pass the keyword argument.
    races_df = await loop.run_in_executor(
        None,
        lambda: pd.read_csv('market/races.csv', parse_dates=['start_time'])
    )
    
    race_info = races_df[races_df['race_id'] == race.race_id].iloc[0]
    start_time = race_info['start_time']
    
    if start_time.tzinfo is None:
        start_time = start_time.tz_localize('UTC')

    duration = timedelta(seconds=120)
    time_elapsed = datetime.now(pytz.utc) - start_time
    time_left = (duration - time_elapsed).total_seconds()

    # 2. Calculate odds at the moment of the bet
    bets_df = await loop.run_in_executor(None, pd.read_csv, 'market/race_bets.csv')
    race_bets = bets_df[bets_df['race_id'] == race.race_id]
    total_pot = race_bets['bet_amount'].sum()
    bets_on_horse = race_bets[race_bets['horse_number'] == horse_number]['bet_amount'].sum()
    odds = (total_pot - bets_on_horse) / bets_on_horse if bets_on_horse > 0 else total_pot

    # 3. Create and append the new bet data
    new_bet = pd.DataFrame([{
        'race_id': race.race_id,
        'bettor_id': bettor_id,
        'horse_number': horse_number,
        'bet_amount': bet_amount,
        'odds_at_bet': round(odds, 2),
        'time_left_in_window': round(time_left, 2),
        'winnings': 0
    }])
    await loop.run_in_executor(None, lambda: new_bet.to_csv('market/race_bets.csv', mode='a', header=False, index=False))

async def place_starting_bot_bets(race, channel):
    """Gets starter bots to bet on different horses."""
    print(f"Placing initial bot bets for Race ID: {race.race_id}")
    starter_bots = [name for name, props in BOT_PERSONALITIES.items() if props['type'] == 'starter']
    
    if len(race.horses) < len(starter_bots):
        return

    loop = asyncio.get_running_loop()
    bot_ledgers = await loop.run_in_executor(None, pd.read_csv, 'market/bot_ledgers.csv')
    horses_to_bet_on = random.sample(race.horses, len(starter_bots))
    
    for i, bot_name in enumerate(starter_bots):
        horse = horses_to_bet_on[i]
        bankroll = bot_ledgers.loc[bot_ledgers['bot_name'] == bot_name, 'bankroll'].iloc[0]
        bet_amount = random.randint(int(bankroll * 0.01), int(bankroll * 0.05))
        bet_amount = min(bet_amount, bankroll)

        if bet_amount > 0:
            bot_ledgers.loc[bot_ledgers['bot_name'] == bot_name, 'bankroll'] -= bet_amount
            bot_ledgers.loc[bot_ledgers['bot_name'] == bot_name, 'total_bets'] += bet_amount
            await _record_bet(race, bot_name, horse.number, bet_amount)
    
    await loop.run_in_executor(None, lambda: bot_ledgers.to_csv('market/bot_ledgers.csv', index=False))
    await channel.send("A flurry of early bets have come in from the regular crowd!")

# --- Racing Commands (Prefix-based) ---

async def run_complete_race(bot, channel, message, race):
    """
    A single, robust function that handles the entire race lifecycle.
    This replaces all the previous background tasks.
    """
    try:
        # --- PHASE 1: BETTING COUNTDOWN ---
        print(f"Starting countdown for Race ID: {race.race_id}")
        start_time = datetime.now(pytz.utc)
        duration = timedelta(seconds=120) # Change to seconds=120 for testing
        
        # Get a list of all sniper bots from your definitions
        sniper_bots = [name for name, props in BOT_PERSONALITIES.items() if props['type'] == 'sniper']
        snipers_who_have_bet = [] # Track which snipers have bet in this race

        while True:
            time_elapsed = datetime.now(pytz.utc) - start_time
            time_remaining = duration - time_elapsed
            if time_remaining.total_seconds() <= 0:
                break # Exit the countdown loop

            # --- Update Odds and Message ---           
            try:
                loop = asyncio.get_running_loop()
                bets_df = await loop.run_in_executor(None, pd.read_csv, 'market/race_bets.csv')
                bets_df = bets_df.query(f"race_id == {race.race_id}")
                total_pot = bets_df['bet_amount'].sum()

                embed = message.embeds[0]
                field_text = ""
                for horse in race.horses:
                    bets_on_horse = bets_df[bets_df['horse_number'] == horse.number]['bet_amount'].sum()
                    odds = (total_pot - bets_on_horse) / bets_on_horse if bets_on_horse > 0 else 0
                    odds_str = f"{odds:.1f}:1" if bets_on_horse > 0 else "--:--"
                    field_text += f"`[{horse.number}]` **{horse.name}** ({horse.strategy}) - Odds: {odds_str}\n"

                embed.set_field_at(0, name="THE FIELD", value=field_text, inline=False)
                minutes, seconds = divmod(int(time_remaining.total_seconds()), 60)
                embed.set_field_at(1, name="Betting Window", value=f"Closing in {minutes}m {seconds}s")
                embed.description = f"The total pot is now **{format_cc(total_pot)}**!"
                await message.edit(embed=embed)
            except Exception as e:
                print(f"Error updating odds (expected during testing with no bets): {e}")
            
            # Sniper Logic
            if time_remaining.total_seconds() <= 10:
                # Loop through each sniper and give them a chance to bet
                for sniper_name in sniper_bots:
                    # Check if this sniper has already placed their bet for this race
                    if sniper_name not in snipers_who_have_bet:
                        # Each sniper gets their own 1-in-50 chance
                        if random.randint(1, 50) == 1:
                            snipers_who_have_bet.append(sniper_name) # Prevent them from betting again
                            print(f"{sniper_name} is betting on Race ID: {race.race_id}!")
                            
                            loop = asyncio.get_running_loop()
                            bot_ledgers = await loop.run_in_executor(None, pd.read_csv, 'market/bot_ledgers.csv')
                            sniper_row = bot_ledgers[bot_ledgers['bot_name'] == sniper_name]
                            
                            if not sniper_row.empty:
                                bankroll = sniper_row['bankroll'].iloc[0]
                                bet_amount = random.randint(int(bankroll * 0.15), int(bankroll * 0.30))
                                bet_amount = min(bet_amount, bankroll)
                                
                                if bet_amount > 0:
                                    chosen_horse = random.choice(race.horses)
                                    bot_ledgers.loc[bot_ledgers['bot_name'] == sniper_name, 'bankroll'] -= bet_amount
                                    bot_ledgers.loc[bot_ledgers['bot_name'] == sniper_name, 'total_bets'] += bet_amount
                                    
                                    bet_df = pd.DataFrame([{'race_id': race.race_id, 'bettor_id': sniper_name, 'horse_number': chosen_horse.number, 'bet_amount': bet_amount}])
                                    bet_df.to_csv('market/race_bets.csv', mode='a', header=False, index=False)
                                    await loop.run_in_executor(None, lambda: bot_ledgers.to_csv('market/bot_ledgers.csv', index=False))
                                    
                                    # Announce with the correct name
                                    await channel.send(f"A huge last-minute bet has just come in! **{sniper_name}** places **{format_cc(bet_amount)}** on **{chosen_horse.name}**!")

            await asyncio.sleep(5) # Wait 5 seconds before the next update

        # --- PHASE 2: PRE-RACE CHECK ---
        print(f"Countdown finished for Race ID: {race.race_id}. Checking for bets...")
        loop = asyncio.get_running_loop()
        bets_df = await loop.run_in_executor(None, pd.read_csv, 'market/race_bets.csv')
        race_bets = bets_df[bets_df['race_id'] == race.race_id]

        if race_bets.empty:
            print(f"No bets placed for Race ID: {race.race_id}. Cancelling.")
            races_df = await loop.run_in_executor(None, pd.read_csv, 'market/races.csv')
            races_df.loc[races_df['race_id'] == race.race_id, 'status'] = 'cancelled'
            await loop.run_in_executor(None, lambda: races_df.to_csv('market/races.csv', index=False))
            
            cancel_embed = discord.Embed(title="🏇 Race Cancelled 🏇", description="This race has been cancelled due to a lack of bets.", color=discord.Color.red())
            await message.edit(embed=cancel_embed)
            if channel.id in active_races: del active_races[channel.id]
            return # End the function here

        # --- PHASE 3: LIVE RACE SIMULATION ---
        print(f"Bets found! Starting race simulation for Race ID: {race.race_id}")
        races_df = await loop.run_in_executor(None, pd.read_csv, 'market/races.csv')
        races_df.loc[races_df['race_id'] == race.race_id, 'status'] = 'running'
        await loop.run_in_executor(None, lambda: races_df.to_csv('market/races.csv', index=False))

        start_embed = discord.Embed(title="🏁 THE PADDOCK DASH IS UNDERWAY! 🏁", description="The betting window has closed. And they're off!", color=discord.Color.blue())
        start_embed.add_field(name="THE FIELD", value=message.embeds[0].fields[0].value, inline=False)
        await message.edit(embed=start_embed)
        
        live_race_embed = (await channel.fetch_message(message.id)).embeds[0]
        
        while not race.is_finished():
            race.run_round()
            
            # --- Static Track Display Logic ---
            # 1. Find the longest name for perfect alignment
            max_name_len = max(len(h.name) for h in race.horses)
            track_display = ""
            track_visual_len = 22 # How many characters wide the track is

            # 2. Build the display line by line for each horse
            for horse in sorted(race.horses, key=lambda h: h.number):
                # Create a padded string for the horse's name and number
                # e.g., "#1 Sakura Bakushin Oh" padded to fit the longest name
                name_str = f"#{horse.number} {horse.name}".ljust(max_name_len + 4)

                # Calculate the horse's progress on the visual track
                progress = int((horse.position / race.track_length) * track_visual_len)
                progress = min(track_visual_len, progress) # Ensure it doesn't go past the end

                # Create the track with the horse emoji at its current progress
                track = '─' * progress + '🏇' + '─' * (track_visual_len - progress)
                
                track_display += f"`{name_str} |{track}| {horse.position}/{race.track_length}`\n"
            
            # 3. Update the embed with the new display
            live_race_embed.set_field_at(0, name=f"LIVE RACE - Round {race.round_number}", value=track_display, inline=False)
            live_race_embed.description = "\n".join(race.log)
            await message.edit(embed=live_race_embed)
            await asyncio.sleep(6)

        # --- PHASE 4: PAYOUTS & CLEANUP ---
        print(f"Race finished for Race ID: {race.race_id}. Processing results...")
        finishers = [h for h in race.horses if h.position >= race.track_length]
        
        # If no one finished (which shouldn't happen), end gracefully.
        if not finishers:
            print(f"Race {race.race_id} ended with no finishers.")
            if channel.id in active_races: del active_races[channel.id]
            return

        top_position = max(h.position for h in finishers)
        tied_winners = [h for h in finishers if h.position == top_position]

        if len(tied_winners) > 1:
            # A true tie has occurred, perform a roll-off.
            tiebreaker_text = "📸 **IT'S A PHOTO FINISH!**\nA tiebreaker roll will determine the winner:\n"
            tiebreaker_results = []
            for horse in tied_winners:
                roll = random.randint(1, 100)
                tiebreaker_results.append((roll, horse))
                tiebreaker_text += f"**{horse.name}** rolls a **{roll}**!\n"
            
            await channel.send(tiebreaker_text)
            await asyncio.sleep(2) # Dramatic pause

            # Find the winner from the tiebreaker results
            final_winner = max(tiebreaker_results, key=lambda item: item[0])[1]

        else:
            # Only one horse was in the lead, no tiebreaker needed.
            final_winner = tied_winners[0]

        # --- A: Load all data ---
        loop = asyncio.get_running_loop()
        races_df = await loop.run_in_executor(None, pd.read_csv, 'market/races.csv')
        bets_df = await loop.run_in_executor(None, pd.read_csv, 'market/race_bets.csv')
        crew_coins_df = await loop.run_in_executor(None, lambda: pd.read_csv('market/crew_coins.csv', dtype={'discord_id': str}))
        bot_ledgers = await loop.run_in_executor(None, pd.read_csv, 'market/bot_ledgers.csv')
        jackpot_ledger = await loop.run_in_executor(None, pd.read_csv, 'market/jackpot_ledger.csv')
        
        # --- B: Calculate Pot and Fund the Jackpot ---
        race_bets = bets_df[bets_df['race_id'] == race.race_id]
        total_pot = race_bets['bet_amount'].sum()
        track_fee = total_pot * 0.0789
        winnings_pool = total_pot - track_fee        
        jackpot_ledger.loc[0, 'current_jackpot'] += track_fee
        
        # --- C: Handle Jackpot Win Condition ---
        jackpot_hit = False
        if random.randint(1, 200) == 1 and jackpot_ledger['current_jackpot'].iloc[0] > 0:
            jackpot_hit = True
            current_jackpot = jackpot_ledger['current_jackpot'].iloc[0]
            winnings_pool += current_jackpot
            jackpot_ledger.loc[0, 'current_jackpot'] = 0 # Reset jackpot
            await channel.send(f"🎉 **THE WINNER'S PURSE HAS BEEN HIT!** An extra **{format_cc(current_jackpot)}** has been added to the prize pool!")
        
        # --- D: Calculate and Distribute Payouts ---
        payout_summary = "No one bet on the winner."
        winner_horse_obj = next((h for h in race.horses if h.name == final_winner.name), None)
        if winner_horse_obj:
            winning_bets = race_bets[race_bets['horse_number'] == winner_horse_obj.number]
            total_winning_bets_amount = winning_bets['bet_amount'].sum()

            if total_winning_bets_amount > 0:
                payout_summary = ""
                for index, bet in winning_bets.iterrows():
                    bettor_id = str(bet['bettor_id'])
                    winnings = winnings_pool * (bet['bet_amount'] / total_winning_bets_amount)
                    bets_df.loc[index, 'winnings'] = winnings # Log the winnings for this specific bet

                    if bettor_id in BOT_PERSONALITIES:
                        bot_ledgers.loc[bot_ledgers['bot_name'] == bettor_id, 'bankroll'] += winnings
                        bot_ledgers.loc[bot_ledgers['bot_name'] == bettor_id, 'total_winnings'] += winnings
                        payout_summary += f"💰 **{bettor_id}** won **{format_cc(winnings)}**!\n"
                    else:
                        if bettor_id in crew_coins_df['discord_id'].values:
                            crew_coins_df.loc[crew_coins_df['discord_id'] == bettor_id, 'balance'] += winnings
                            payout_summary += f"👑 <@{bettor_id}> won **{format_cc(winnings)}**!\n"
        
        # --- E: Create the Podium Finish Embed ---
        podium_embed = discord.Embed(title=f"🏆 Winner's Circle: Race #{race.race_id} Results 🏆", color=discord.Color.gold())
        sorted_finishers = sorted(race.horses, key=lambda h: h.position, reverse=True)
        podium_text = ""
        if len(sorted_finishers) > 0: podium_text += f"🥇 **1st Place:** {sorted_finishers[0].name}\n"
        if len(sorted_finishers) > 1: podium_text += f"🥈 **2nd Place:** {sorted_finishers[1].name}\n"
        if len(sorted_finishers) > 2: podium_text += f"🥉 **3rd Place:** {sorted_finishers[2].name}\n"
        podium_embed.description = podium_text
        podium_embed.add_field(name="Payouts", value=payout_summary, inline=False)
        podium_embed.set_footer(text=f"Total Pot: {format_cc(total_pot)} | Winnings Pool: {format_cc(winnings_pool)}")
        if jackpot_hit:
            podium_embed.set_author(name="JACKPOT WIN!")
        
        # --- F: Generate and Save Final Logs ---
        # 1. Save Race Results
        results_data = []
        for i, horse in enumerate(sorted_finishers):
            results_data.append({
                'race_id': race.race_id, 'horse_name': horse.name, 'horse_number': horse.number,
                'strategy': horse.strategy, 'skills': ",".join(horse.skills),
                'final_position': horse.position, 'final_place': i + 1
            })
        pd.DataFrame(results_data).to_csv('market/race_results.csv', mode='a', header=False, index=False)

        # 2. Append to Master Event Log
        log_df = pd.DataFrame(race.structured_log)
        if not log_df.empty:
            log_df['race_id'] = race.race_id
            log_df.to_csv('market/race_events.csv', mode='a', header=False, index=False)

        # --- G: Save all updated DataFrames ---
        await loop.run_in_executor(None, lambda: bets_df.to_csv('market/race_bets.csv', index=False))
        await loop.run_in_executor(None, lambda: crew_coins_df.to_csv('market/crew_coins.csv', index=False))
        await loop.run_in_executor(None, lambda: bot_ledgers.to_csv('market/bot_ledgers.csv', index=False))
        await loop.run_in_executor(None, lambda: jackpot_ledger.to_csv('market/jackpot_ledger.csv', index=False))
        
        # --- H: Send Final Message and Cleanup ---
        await channel.send(embed=podium_embed)
        if channel.id in active_races: del active_races[channel.id]
        print(f"Cleanup complete for Race ID: {race.race_id}") 

    except Exception as e:
        print(f"A critical error occurred in the main race loop for Race ID {race.race_id}: {e}")
        if channel.id in active_races: del active_races[channel.id]


@bot.group(invoke_without_command=True)
async def race(ctx):
    """Parent command for Winner's Circle Racing."""
    await ctx.send("Invalid race command. Use `/race create` or `/race bet`.", ephemeral=True)

@commands.cooldown(1, 300, commands.BucketType.user)
@race.command(name="create")
async def race_create(ctx, num_horses: int = 10, track_length: int = 60):
    """Creates a new horse race and starts the game loop. Defaults to 10 horses."""
    
    VALID_HORSE_COUNTS = [8, 9, 10, 16, 18]
    if num_horses not in VALID_HORSE_COUNTS:
        await ctx.send(f"Invalid number of horses. Please choose from: `{', '.join(map(str, VALID_HORSE_COUNTS))}`", ephemeral=True)
        return

    if ctx.channel.id in active_races:
        return await ctx.send("A race is already in progress in this channel!", ephemeral=True)

    # 1. Create the Race object and post the initial message
    race_id = int(datetime.now().timestamp())
    new_race = Race(race_id=race_id, track_length=track_length)
    
    horse_names_for_this_race = generate_race_field(num_horses)

    for i, horse_name in enumerate(horse_names_for_this_race, 1):
        if horse_name in UNIQUE_HORSES:
            horse_data = UNIQUE_HORSES[horse_name]
            h = Horse(number=i, name=horse_name, strategy=horse_data['strategy'], skills=horse_data['skills'][:])
        else:
            skills_list = random.sample(GENERIC_SKILLS, k=random.randint(0, 2))
            h = Horse(number=i, name=horse_name, strategy=random.choice(HORSE_STRATEGIES), skills=skills_list)
        new_race.add_horse(h)

    # 2. Post the initial message
    embed = discord.Embed(title=f"🏇 Race #{race_id} - Morning Line", description="Calculating odds... Betting is now open!", color=discord.Color.green())
    field_text = ""
    for horse in new_race.horses:
        field_text += f"`[{horse.number}]` **{horse.name}** ({horse.strategy}) - Odds: --:--\n"
    embed.add_field(name="THE FIELD", value=field_text, inline=False)
    embed.add_field(name="Betting Window", value="Closing in 2m 0s") # You can change this for testing
    race_message = await ctx.send(embed=embed)

    # 3. Save initial state to CSVs for persistence
    start_time = datetime.now(pytz.utc)
    races_df = pd.DataFrame([{'race_id': race_id, 'message_id': race_message.id, 'channel_id': race_message.channel.id, 'track_length': track_length, 'status': 'betting', 'winner': None, 'start_time': start_time.isoformat()}])
    races_df.to_csv('market/races.csv', mode='a', header=False, index=False)
    horses_data = [{'race_id': race_id, 'horse_number': h.number, 'horse_name': h.name, 'position': 0, 'strategy': h.strategy, 'skills': ",".join(h.skills)} for h in new_race.horses]
    pd.DataFrame(horses_data).to_csv('market/race_horses.csv', mode='a', header=False, index=False)
    
    # 4. Store the live race object and START THE GAME LOOP
    active_races[ctx.channel.id] = new_race
    bot.loop.create_task(run_complete_race(bot, ctx.channel, race_message, new_race))
    await place_starting_bot_bets(new_race, ctx.channel)
    await ctx.send("Race created!", ephemeral=True)


@race.command(name="bet")
async def race_bet(ctx, horse_number: int, amount: int):
    """Places a bet on a horse in the current race."""
    if ctx.channel.id not in active_races:
        return await ctx.send("There is no race currently accepting bets in this channel.", ephemeral=True)
    
    race = active_races[ctx.channel.id]
    bettor_id = str(ctx.author.id)

    if race.round_number > 0:
        return await ctx.send("The betting window for this race has closed.", ephemeral=True)
    if amount <= 0:
        return await ctx.send("You must bet a positive amount.", ephemeral=True)

    # --- File Locking to prevent simultaneous bets ---
    lock_file = 'market/market.lock'
    if os.path.exists(lock_file):
        return await ctx.send("The betting windows are busy, please try again in a moment.", ephemeral=True)
    open(lock_file, 'w').close()

    try:
        loop = asyncio.get_running_loop()

        # --- 1. Player Bet Validation ---
        crew_coins_df = await loop.run_in_executor(None, lambda: pd.read_csv('market/crew_coins.csv', dtype={'discord_id': str}))
        user_row = crew_coins_df[crew_coins_df['discord_id'] == bettor_id]

        if user_row.empty:
            return await ctx.send("You do not have a Fan Exchange account to bet with.", ephemeral=True)
        
        user_balance = user_row['balance'].iloc[0]
        if user_balance < amount:
            return await ctx.send(f"You don't have enough CC. Your balance is {format_cc(user_balance)}.", ephemeral=True)

        # --- 2. Record Player's Bet ---
        crew_coins_df.loc[crew_coins_df['discord_id'] == bettor_id, 'balance'] -= amount
        await _record_bet(race, bettor_id, horse_number, amount)
        await ctx.send(f"✅ Your bet of **{format_cc(amount)}** on horse **#{horse_number}** has been placed!", ephemeral=True)
        
        # --- 3. BagginsTheBookie Logic ---
        all_bets_df = await loop.run_in_executor(None, pd.read_csv, 'market/race_bets.csv')
        race_bets = all_bets_df[all_bets_df['race_id'] == race.race_id]
        human_bets = race_bets[~race_bets['bettor_id'].isin(BOT_PERSONALITIES.keys())]

        if len(human_bets) == 1:
            print("First human bet detected, triggering BagginsTheBookie...")
            bot_ledgers = await loop.run_in_executor(None, pd.read_csv, 'market/bot_ledgers.csv')
            bookie_row = bot_ledgers[bot_ledgers['bot_name'] == 'BagginsTheBookie']
            
            if not bookie_row.empty:
                # Properly define the bot's bankroll
                baggins_bankroll = bookie_row['bankroll'].iloc[0]
                baggins_bet_amount = random.randint(int(amount * 0.6), int(amount * 1.2))
                baggins_bet_amount = min(baggins_bet_amount, baggins_bankroll)

                possible_horses = [h.number for h in race.horses if h.number != horse_number]
                if possible_horses and baggins_bet_amount > 0:
                    baggins_choice = random.choice(possible_horses)
                    baggins_horse_name = next((h.name for h in race.horses if h.number == baggins_choice), "Unknown Horse")
                                        
                    # Use the helper function to calculate odds/time and log the bet
                    await _record_bet(race, 'BagginsTheBookie', baggins_choice, baggins_bet_amount)
                    
                    await ctx.channel.send(f"**BagginsTheBookie** has entered the fray, placing a bet of **{format_cc(baggins_bet_amount)}** on **#{baggins_choice} {baggins_horse_name}**!")
                    # Save the correct ledger file
                    await loop.run_in_executor(None, lambda: bot_ledgers.to_csv('market/bot_ledgers.csv', index=False))

        # --- 4. Save all changes to disk ---
        await loop.run_in_executor(None, lambda: all_bets_df.to_csv('market/race_bets.csv', index=False))
        await loop.run_in_executor(None, lambda: crew_coins_df.to_csv('market/crew_coins.csv', index=False))

    except Exception as e:
        print(f"An error occurred in /race bet: {e}")
        await ctx.send("An error occurred while placing your bet. Please contact an admin.", ephemeral=True)
    finally:
        os.remove(lock_file)

@bot.command(name="refund_race")
@commands.has_permissions(administrator=True)
async def refund_race(ctx, race_id: int):
    """(Admin Only) Refunds all bets for a given race ID."""
    lock_file = 'market/market.lock'
    if os.path.exists(lock_file):
        await ctx.send("The market is busy. Please try again in a moment.", ephemeral=True)
        return
    open(lock_file, 'w').close()

    try:
        races_df = load_market_file('races.csv')
        bets_df = load_market_file('race_bets.csv')
        crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
        bot_ledgers_df = load_market_file('bot_ledgers.csv')

        target_race = races_df[races_df['race_id'] == race_id]
        if target_race.empty:
            await ctx.send(f"Could not find a race with ID `{race_id}`.", ephemeral=True)
            return

        if target_race['status'].iloc[0] in ['refunded', 'cancelled']:
            await ctx.send(f"Race `{race_id}` has already been cancelled or refunded.", ephemeral=True)
            return
            
        bets_to_refund = bets_df[bets_df['race_id'] == race_id]
        if bets_to_refund.empty:
            await ctx.send(f"No bets were placed on race `{race_id}` to refund.", ephemeral=True)
            return
            
        refund_summary = ""
        total_refunded = 0

        for _, bet in bets_to_refund.iterrows():
            bettor_id = str(bet['bettor_id'])
            amount = bet['bet_amount']
            total_refunded += amount
            
            # Check if the bettor is a bot or a human user
            if bettor_id in bot_ledgers_df['bot_name'].values:
                bot_ledgers_df.loc[bot_ledgers_df['bot_name'] == bettor_id, 'bankroll'] += amount
                refund_summary += f"🤖 Refunded {format_cc(amount)} to bot **{bettor_id}**.\n"
            else:
                user_index = crew_coins_df[crew_coins_df['discord_id'] == bettor_id].index
                if not user_index.empty:
                    crew_coins_df.loc[user_index, 'balance'] += amount
                    refund_summary += f"👤 Refunded {format_cc(amount)} to <@{bettor_id}>.\n"

        # Update the race status to 'refunded'
        races_df.loc[races_df['race_id'] == race_id, 'status'] = 'refunded'

        # Save all changes
        races_df.to_csv('market/races.csv', index=False)
        crew_coins_df.to_csv('market/crew_coins.csv', index=False)
        bot_ledgers_df.to_csv('market/bot_ledgers.csv', index=False)
        
        # Announce the refund
        embed = discord.Embed(
            title="🏁 Race Refund Announcement 🏁",
            description=f"All bets for **Race #{race_id}** have been refunded by an administrator.",
            color=discord.Color.orange()
        )
        embed.add_field(name="Total CC Refunded", value=format_cc(total_refunded))
        
        winners_circle_channel = discord.utils.get(ctx.guild.channels, name=WINNERS_CIRCLE_CHANNEL_NAME)
        if winners_circle_channel:
            await winners_circle_channel.send(embed=embed)
        
        await ctx.send(f"✅ Successfully refunded all bets for race `{race_id}`.", embed=embed, ephemeral=True)

    finally:
        os.remove(lock_file)

@refund_race.error
async def refund_race_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("You do not have permission to use this command.", ephemeral=True)


##########################        
##### RACE VERSION 2 #####
##########################

# --- NEW HELPER FUNCTION FOR HORSE GENERATION ---

def generate_random_horse_field(num_horses: int) -> list:
    """
    Reads the attributes config and generates a list of unique, random horses.
    """
    try:
        with open('market/configs/horse_attributes.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print("ERROR: horse_attributes.json not found. Cannot generate horses.")
        return []

    adjectives = list(config['adjectives'].keys())
    nouns = list(config['nouns'].keys())
    strategies = list(config['strategies'].keys())
    
    generated_horses = []
    used_names = set()

    while len(generated_horses) < num_horses:
        adj = random.choice(adjectives)
        noun = random.choice(nouns)
        name = f"{adj} {noun}"

        if name not in used_names:
            used_names.add(name)
            strategy = random.choice(strategies)
            generated_horses.append(Horse(name, strategy))
            
    return generated_horses


# --- NEW V2 RACING COMMANDS ---

active_races_v2 = {} 

@bot.group(name="race_v2", invoke_without_command=True)
async def race_v2(ctx):
    """Parent command for the new V2 Winner's Circle Racing."""
    await ctx.send("Invalid command. Use `/race_v2 create` or `/race_v2 bet`.", ephemeral=True)

@race_v2.command(name="create")
@commands.has_permissions(administrator=True)
async def race_v2_create(ctx, distance: int = 2400, num_horses: int = 10):
    """(V2) Creates a new, statistically-driven horse race."""
    
    if ctx.channel.id in active_races_v2:
        return await ctx.send("A V2 race is already in progress in this channel.", ephemeral=True)

    await ctx.send("`🏁 V2 Race creation initiated...`\n`Generating horses and running Monte Carlo simulation (this may take a moment)...`")

    # --- 1. Generate the Field (NOW CORRECTED) ---
    horse_list = generate_random_horse_field(num_horses)
    if not horse_list:
        return await ctx.send("Failed to generate horses. Check configuration.", ephemeral=True)

    # --- 2. Initialize the Core Objects ---
    race_obj = Race(horses=horse_list, distance=distance)
    bookie_obj = Bookie(race_obj)
    
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, bookie_obj.run_monte_carlo, 10000)

    # --- 3. Store Race in Database ---
    race_id = int(datetime.now().timestamp())
    database.create_race(race_id, distance, horse_list)

    # --- 4. Initialize the Bot Manager and Start Betting Cycle ---
    bot_manager = BotManager(race_id, bookie_obj)
    bot.loop.create_task(bot_manager.run_betting_cycle(duration_seconds=120))

    # --- 5. Post the Initial Race Card ---
    embed = discord.Embed(title=f"🏇 Race #{race_id} - Morning Line Odds", color=discord.Color.green())
    odds_text = "```\n"
    for name, data in bookie_obj.morning_line_odds.items():
        odds_text += f"{name:<20} | {data['odds']:.2f} to 1\n"
    odds_text += "```"
    embed.add_field(name="Betting is now OPEN for 2 minutes!", value=odds_text)
    
    race_message = await ctx.send(embed=embed)

    active_races_v2[ctx.channel.id] = {
        "race": race_obj, "bookie": bookie_obj, "bot_manager": bot_manager, "message": race_message
    }
    
    await ctx.send("V2 Race created successfully!", ephemeral=True)




##########################
##########################


# --- Run the Bot ---
load_dotenv() # Loads variables from .env file
TOKEN = os.getenv('DISCORD_TOKEN')

if TOKEN is None:
    print("ERROR: DISCORD_TOKEN not found in .env file.")
else:
    bot.run(TOKEN)