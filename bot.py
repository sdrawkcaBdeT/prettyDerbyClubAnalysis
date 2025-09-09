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
            "description": "Adds a flat +150 CC per day. (Note: This is a passive effect handled by the backend [NOT IMPLEMENTED YET]).",
            "costs": [75000, 225000, 675000],
            "max_tier": 3,
            "type": "upgrade"
        }
    },
    "GAMBLING": {
    "g1": {
        "name": "High Roller License",
        "description": "Permanently increases your personal maximum bet limit for all gambling games.",
        # Tiers:  1        2        3         4         5         6
        "costs": [75000, 200000, 500000, 1250000, 2500000, 5000000],
        "max_tier": 6,
        "type": "upgrade"
        }
    }
}

CARD_SUITS = {"Spades": "♠️", "Hearts": "♥️", "Clubs": "♣️", "Diamonds": "♦️"}
CARD_RANKS = {"2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9, "10": 10, "J": 11, "Q": 12, "K": 13, "A": 14}
# The payout multiplier for a winning bet in higher or lower 1.75x creates a house edge)
PAYOUT_MULTIPLIER = 1.45
BET_LIMITS = {
    0: 2000000,      # Base limit (no upgrade)
    1: 39999,
    2: 99999,
    3: 249999,
    4: 499999,
    5: 1000000,
    6: 1000000    # Max tier
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

async def send_to_channel_by_name(channel_name, message_content, file_path=None):
    """Finds a channel by name and sends a message and optional file there."""
    if not bot.guilds:
        print("Bot is not in any guild.")
        return

    guild = bot.guilds[0]
    channel = discord.utils.get(guild.channels, name=channel_name)

    if not channel:
        print(f"Error: I couldn't find the `#{channel_name}` channel.")
        return

    if file_path and not os.path.exists(file_path):
        print(f"Error: I couldn't find the file at {file_path}.")
        return

    try:
        file_to_send = discord.File(file_path) if file_path else None
        await channel.send(message_content, file=file_to_send)
    except Exception as e:
        print(f"Error sending to channel {channel_name}: {e}")

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
    """(Admin Only) Awards a specified amount of CC to a member via the database."""
    admin_id = str(ctx.author.id)

    # --- 1. Input Validation ---
    if amount <= 0:
        return await ctx.send("Please enter a positive whole number for the amount.", ephemeral=True)

    # --- 2. Find User in the Database ---
    # This finds the user by their in-game name or ticker.
    target_user = database.get_user_details_by_identifier(member)
    if not target_user:
        return await ctx.send(f"Could not find a member or ticker named '{member}'.", ephemeral=True)
    
    target_id = target_user['discord_id']
    target_name = target_user['ingamename']

    # --- 3. Execute the Database Transaction ---
    # This single function updates the balance AND logs the transaction safely.
    new_balance = database.execute_admin_award(
        admin_id=admin_id,
        target_id=target_id,
        amount=amount
    )

    if new_balance is None:
        return await ctx.send("The transaction failed. Please check the bot logs.", ephemeral=True)
        
    # --- 4. Confirmation ---
    embed = discord.Embed(
        title="✅ CC Awarded Successfully",
        description=f"You have awarded **{format_cc(amount)}** to **{target_name}**.",
        color=discord.Color.gold()
    )
    embed.set_footer(text=f"Their new balance is {format_cc(new_balance)}.")
    await ctx.send(embed=embed)

        
@award_cc.error
async def award_cc_error(ctx, error):
    # This block handles when a non-admin tries to use the command
    if isinstance(error, commands.CheckFailure):
        member_who_failed = ctx.author

        # --- 1. Create the embed and load the GIF files ---
        file1 = discord.File("gifs/notalking.gif", filename="notalking.gif")
        file2 = discord.File("gifs/banned.gif", filename="banned.gif")
        files_to_send = [file1, file2]

        embed = discord.Embed(
            title="🚨 Permission Denied 🚨",
            # Mention the user directly in the public message
            description=f"**Action Taken:**{member_who_failed.mention} has been put in TIMEOUT for 60 minutes for attempting to use an admin command. This is a dictatorship and I'm having fun.",
            color=discord.Color.red()
        )
        embed.set_image(url="attachment://banned.gif")
        embed.set_thumbnail(url="attachment://notalking.gif")
        
        # --- 2. Take Action (Timeout) BEFORE sending the message ---
        duration = timedelta(minutes=5)
        reason = "Unauthorized use of an admin command."
        
        try:
            await member_who_failed.timeout(duration, reason=reason)
            # Add a field to the embed confirming the action
            embed.add_field(name="Action Taken", value=f"User has been timed out for {duration.seconds // 60} minutes.")

        except discord.Forbidden:
            # If the bot can't time them out, add a field explaining that instead
            embed.add_field(name="Action Failed", value=f"I don't have permission to take action against this user.")
        
        # --- 3. Send the final, public message to the channel ---
        await ctx.send(embed=embed, files=files_to_send)
        
        return # Stop the function here

    # This is a fallback for other potential errors with the command
    await ctx.send(f"An unexpected error occurred: {error}")

@bot.command(name="remove_cc")
@commands.check(is_admin)
async def remove_cc(ctx, member: str, amount: int):
    """(Admin Only) Removes a specified amount of CC from a member."""
    admin_id = str(ctx.author.id)

    # --- 1. Input Validation ---
    if amount <= 0:
        return await ctx.send("Please enter a positive whole number for the amount to remove.", ephemeral=True)

    # --- 2. Find User in the Database ---
    target_user = database.get_user_details_by_identifier(member)
    if not target_user:
        return await ctx.send(f"Could not find a member or ticker named '{member}'.", ephemeral=True)
    
    target_id = target_user['discord_id']
    target_name = target_user['ingamename']

    # --- 3. Execute the Database Transaction ---
    # This new function safely handles the removal and logging.
    new_balance = database.execute_admin_removal(
        admin_id=admin_id,
        target_id=target_id,
        amount=amount
    )

    if new_balance is None:
        return await ctx.send("The transaction failed. The user may not have enough CC, or a database error occurred. Please check the logs.", ephemeral=True)
        
    # --- 4. Confirmation ---
    embed = discord.Embed(
        title="✅ CC Removed Successfully",
        description=f"You have removed **{format_cc(amount)}** from **{target_name}**.",
        color=discord.Color.orange() # A different color to distinguish from awards
    )
    embed.set_footer(text=f"Their new balance is {format_cc(new_balance)}.")
    await ctx.send(embed=embed)

@remove_cc.error
async def remove_cc_error(ctx, error):
    # This uses the same error handling as your award_cc command for non-admins.
    if isinstance(error, commands.CheckFailure):
        member_who_failed = ctx.author
        
        file1 = discord.File("gifs/notalking.gif", filename="notalking.gif")
        file2 = discord.File("gifs/banned.gif", filename="banned.gif")
        files_to_send = [file1, file2]

        embed = discord.Embed(
            title="🚨 Permission Denied 🚨",
            description=f"{member_who_failed.mention} has been put in TIMEOUT for trying to use an admin command. This is a dictatorship.",
            color=discord.Color.red()
        )
        embed.set_image(url="attachment://banned.gif")
        embed.set_thumbnail(url="attachment://notalking.gif")
        
        duration = timedelta(minutes=5)
        reason = "Unauthorized use of an admin command."
        
        try:
            await member_who_failed.timeout(duration, reason=reason)
            embed.add_field(name="Action Taken", value=f"User has been timed out for {duration.seconds // 60} minutes.")
        except discord.Forbidden:
            embed.add_field(name="Action Failed", value="I don't have permission to take action against this user.")
        
        await ctx.send(embed=embed, files=files_to_send)
        return

    await ctx.send(f"An unexpected error occurred: {error}")

@bot.command(name="remove_upgrade")
@commands.check(is_admin)
async def remove_upgrade(ctx, member: discord.Member, upgrade_id: str):
    """(Admin Only) Removes a specific shop upgrade from a member."""
    target_id = str(member.id)
    upgrade_id = upgrade_id.lower()

    # Find the full upgrade name from the SHOP_ITEMS dictionary
    upgrade_name = None
    for category in SHOP_ITEMS.values():
        if upgrade_id in category:
            upgrade_name = category[upgrade_id]['name']
            break
    
    if not upgrade_name:
        return await ctx.send(f"Invalid upgrade ID: `{upgrade_id}`.", ephemeral=True)

    # Call the new database function
    success = database.remove_shop_upgrade(target_id, upgrade_name)

    if success:
        embed = discord.Embed(
            title="✅ Upgrade Removed",
            description=f"Successfully removed the **{upgrade_name}** upgrade from **{member.display_name}**.",
            color=discord.Color.orange()
        )
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"Could not remove the upgrade. It's possible **{member.display_name}** doesn't own that upgrade.", ephemeral=True)

@remove_upgrade.error
async def remove_upgrade_error(ctx, error):
    if isinstance(error, commands.CheckFailure):
        await ctx.send("You do not have permission to use this command.", ephemeral=True)
    else:
        await ctx.send(f"An error occurred: {error}", ephemeral=True)


def get_inGameName(discord_id):
    """Looks up a user's in-game name from the registration file."""
    if not os.path.exists(USER_REGISTRATIONS_CSV):
        return None
    registrations_df = pd.read_csv(USER_REGISTRATIONS_CSV)
    user_entry = registrations_df[registrations_df['discord_id'] == discord_id]
    if not user_entry.empty:
        return user_entry.iloc[0]['inGameName']
    return None

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

# --- Fan Update Task ---
last_update_announced_timestamp = None

@tasks.loop(minutes=1)
async def post_fan_update():
    """Checks for new fan data and posts an update to the scoreboard."""
    global last_update_announced_timestamp
    await bot.wait_until_ready()

    try:
        if not os.path.exists(ENRICHED_FAN_LOG_CSV):
            return

        df = pd.read_csv(ENRICHED_FAN_LOG_CSV)
        if df.empty:
            return

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        latest_timestamp = df['timestamp'].max()

        if last_update_announced_timestamp is None:
            # On first run, just set the timestamp and do nothing.
            last_update_announced_timestamp = latest_timestamp
            return

        if latest_timestamp > last_update_announced_timestamp:
            update_df = df[df['timestamp'] == latest_timestamp]
            total_fan_gain = update_df['fanGain'].sum()

            if total_fan_gain > 0:
                message = (f"📢 **Club Update!**\n"
                           f"The data has been updated! The club gained a total of **{total_fan_gain:,.0f}** fans!")
                await send_to_channel_by_name(SCOREBOARD_CHANNEL_NAME, message)

            last_update_announced_timestamp = latest_timestamp

    except Exception as e:
        print(f"Error in post_fan_update task: {e}")


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
    if not post_fan_update.is_running():
        print("Starting fan update checking task...")
        post_fan_update.start()

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
    embed.add_field(name="CLUB CHARTS & REPORTS", value=("`/top10` - Shows the current top 10 members by monthly fan gain.\n" "`/alltime_top10` - Shows the current top 10 members by all-time fan gain.\n" "`/prestige_leaderboard` - Displays the all-time prestige point leaderboard.\n" "`/performance` - Posts the historical fan gain heatmap.\n" "`/log [member_name]` - Gets the detailed performance log for any member.\n" "`/livegains` - Shows a log of all fan gains across the club in the last 24 hours."), inline=False)
    embed.add_field(name="FAN EXCHANGE (Stock Market)", value=("`/exchange_help` - Provides a concise explanation and startup guide for the Fan Exchange system.\n" "`/market` - Displays the all stocks and some info.\n" "`/portfolio` - View your current stock holdings and their performance.\n" "`/stock [name/ticker]` - Shows the price history and stats for a specific racer.\n" "`/invest [name/ticker] [amount]` - Buy shares in a racer's stock, specifying SHARES to invest.\n" "`/sell [name/ticker] [amount]` - Sell shares of a racer's stock, specifying shares to sell.\n" "`/shop` - See what you can buy with your CC! Earnings upgrades and prestige!" "`/buy [shop_id]` - Purchase something from the shop!" "`/set_ticker [2-5 letter ticker]` - Set your unique stock ticker symbol."), inline=False)
    embed.add_field(name="FINANCIAL REPORTING", value=("`/financial_summary` - Get a high-level overview of your net worth, P/L, and ROI.\n" "`/earnings [7 or 30]` - View a detailed list of your income from earnings and dividends.\n" "`/ledger` - See a complete, paginated history of all your transactions."), inline=False)
    embed.add_field(name="UTILITY & SOCIAL", value="`/compare [member1] [member2]` - Puts two members' key stats side-by-side for easy comparison.\n`/whos_hot` - Shows the top 5 trending stocks over the last 3 days.\n`/gift_cc [member] [amount]` - Give another member some of your CC (5% fee).", inline=False)
    embed.add_field(name="GAMBA", value=("`/higherlower [betamount]` - - Play a game of Higher or Lower! Bet your CC and guess if the next card will be higher or lower than the one shown. Aces are high and ties are a loss."), inline=False)
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

# This defines a global 30-minute cooldown for the /refresh command
cooldown = commands.CooldownMapping.from_cooldown(1, 1800, commands.BucketType.guild)

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
              "**Sponsorship Deal**: Become the single largest shareholder of a stock to earn a **20% dividend** on that member's total Personal CC Earnings! A Tier 2 Dividend pays out 10% proportionally to shareholders who own the stock buy are not the largest shareholder, so be sure to invest!\n"
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
async def alltime_top10(ctx):
    """Posts the alltime_leaderboard.png chart."""
    timestamp_str = get_last_update_timestamp()
    message = f"{ctx.author.mention} here is the Top 10 All-Time Fan Gain chart!"
    file_path = os.path.join(OUTPUT_DIR, 'alltime_leaderboard.png')
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


@bot.command()
async def livegains(ctx):
    """Posts the 24-hour fan gain log."""
    timestamp_str = get_last_update_timestamp()
    message = f"{ctx.author.mention} here is the live fan gain log for the last 24 hours!"
    file_path = os.path.join(OUTPUT_DIR, 'update_log_24hr.png')
    await send_to_scoreboard(ctx, message, file_path)

@bot.command(name="compare")
async def compare(ctx, member1: str, member2: str):
    """Provides a side-by-side comparison of two members."""

    async def get_member_stats(identifier: str):
        """Helper function to fetch all stats for a single member."""
        # 1. Get basic user details
        user_details = database.get_user_details_by_identifier(identifier)
        if not user_details:
            return None, f"Could not find a member or ticker for '{identifier}'."

        ingamename = user_details['ingamename']

        # 2. Get stock and market details
        stock_info, _, _ = database.get_stock_details(ingamename)
        if not stock_info:
            return None, f"Could not retrieve stock details for {ingamename}."

        market_snapshot, _ = database.get_market_snapshot()
        if market_snapshot is None:
            return None, "Market snapshot is currently unavailable."

        stock_market_info = market_snapshot[market_snapshot['ingamename'] == ingamename]
        market_cap = stock_market_info['market_cap'].iloc[0] if not stock_market_info.empty else 0
        price_24h_ago = stock_market_info['price_24h_ago'].iloc[0] if not stock_market_info.empty else stock_info['current_price']

        price_change = stock_info['current_price'] - price_24h_ago
        percent_change = (price_change / price_24h_ago) * 100 if price_24h_ago > 0 else 0

        # 3. Get prestige details from CSV
        try:
            enriched_df = pd.read_csv(ENRICHED_FAN_LOG_CSV)
            enriched_df['timestamp'] = pd.to_datetime(enriched_df['timestamp'])
            latest_stats = enriched_df.loc[enriched_df[enriched_df['inGameName'] == ingamename]['timestamp'].idxmax()]
            monthly_prestige = latest_stats['monthlyPrestige']
            lifetime_prestige = latest_stats['lifetimePrestige']
        except (FileNotFoundError, KeyError, ValueError):
            monthly_prestige = 'N/A'
            lifetime_prestige = 'N/A'

        # 4. Assemble stats
        stats = {
            "name": f"{ingamename} (${stock_info.get('ticker', '')})",
            "Price": f"{stock_info['current_price']:,.2f} CC",
            "24h Change": f"{'+' if percent_change >= 0 else ''}{percent_change:.2f}%",
            "Market Cap": format_cc(market_cap),
            "Monthly Prestige": f"{monthly_prestige:,.2f}" if isinstance(monthly_prestige, (int, float)) else "N/A",
            "Lifetime Prestige": f"{lifetime_prestige:,.2f}" if isinstance(lifetime_prestige, (int, float)) else "N/A"
        }
        return stats, None

    # Fetch stats for both members
    stats1, error1 = await get_member_stats(member1)
    if error1:
        await ctx.send(error1, ephemeral=True)
        return

    stats2, error2 = await get_member_stats(member2)
    if error2:
        await ctx.send(error2, ephemeral=True)
        return

    # Build the embed
    embed = discord.Embed(title=f"Comparison: {stats1['name']} vs. {stats2['name']}", color=discord.Color.purple())

    # Member 1 Field
    field1_value = ""
    for key, value in stats1.items():
        if key != 'name':
            field1_value += f"**{key}**: {value}\n"
    embed.add_field(name=stats1['name'], value=field1_value, inline=True)

    # Member 2 Field
    field2_value = ""
    for key, value in stats2.items():
        if key != 'name':
            field2_value += f"**{key}**: {value}\n"
    embed.add_field(name=stats2['name'], value=field2_value, inline=True)

    await ctx.send(embed=embed)

@bot.command(name="whos_hot")
async def whos_hot(ctx, days: int = 3):
    """Displays the top 5 trending stocks over a given number of days."""
    if days <= 0:
        return await ctx.send("Please enter a positive number of days.", ephemeral=True)

    trending_stocks_df = database.get_trending_stocks(days=days)

    if trending_stocks_df.empty:
        return await ctx.send(f"Could not retrieve trending stock data for the last {days} days.", ephemeral=True)

    top_5 = trending_stocks_df.head(5)

    embed = discord.Embed(
        title=f"🔥 Who's Hot? Top 5 Movers (Last {days} Days) 🔥",
        description="These stocks have the highest percentage price increase over the period.",
        color=discord.Color.orange()
    )

    if top_5.empty:
        embed.description = "No significant stock movement in the last {days} days."
    else:
        for index, row in top_5.iterrows():
            ticker = f"(${row['ticker']})" if pd.notna(row['ticker']) else ""
            name = f"**{row['ingamename']}** {ticker}"

            change_str = f"{'+' if row['percent_change'] >= 0 else ''}{row['percent_change']:.2f}%"
            value = (f"**Change**: {change_str}\n"
                     f"**Current Price**: {format_cc(row['current_price'])}")

            embed.add_field(name=name, value=value, inline=False)

    await ctx.send(embed=embed)

@bot.command(name="gift_cc")
async def gift_cc(ctx, member: str, amount: int):
    """Gifts a specified amount of CC to another member."""
    sender_id = str(ctx.author.id)

    # 1. Input Validation
    if amount <= 0:
        return await ctx.send("You must gift a positive amount of CC.", ephemeral=True)

    sender_details = database.get_user_details(sender_id)
    if not sender_details:
         return await ctx.send("Could not find your user account. Are you registered?", ephemeral=True)
    sender_name = sender_details['ingamename']

    receiver_details = database.get_user_details_by_identifier(member)
    if not receiver_details:
        return await ctx.send(f"Could not find a member or ticker for '{member}'.", ephemeral=True)

    receiver_id = receiver_details['discord_id']
    receiver_name = receiver_details['ingamename']

    if sender_id == receiver_id:
        return await ctx.send("You cannot gift CC to yourself.", ephemeral=True)

    # 2. Preliminary Balance Check
    sender_balance = sender_details['balance']
    if sender_balance is None or sender_balance < amount:
        return await ctx.send(
            f"Insufficient funds. You need **{format_cc(amount)}** to send this gift, but your balance is only {format_cc(sender_balance)}.",
            ephemeral=True
        )

    # 3. Confirmation View
    class GiftConfirmationView(discord.ui.View):
        def __init__(self, *, timeout=30):
            super().__init__(timeout=timeout)
            self.confirmed = None

        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("This is not your gift to confirm.", ephemeral=True)
                return False
            return True

        @discord.ui.button(label='Confirm Gift', style=discord.ButtonStyle.green)
        async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
            self.confirmed = True
            for item in self.children:
                item.disabled = True
            await interaction.response.edit_message(content="💸 Sending your gift...", view=self)
            self.stop()

        @discord.ui.button(label='Cancel', style=discord.ButtonStyle.red)
        async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
            self.confirmed = False
            for item in self.children:
                item.disabled = True
            await interaction.response.edit_message(content="Gift cancelled.", view=self)
            self.stop()

    embed = discord.Embed(
        title="🎁 Gift Confirmation",
        description=f"You are about to send **{format_cc(amount)}** to **{receiver_name}**.",
        color=discord.Color.blue()
    )
    embed.add_field(name="Total Cost to You", value=format_cc(amount), inline=False)
    embed.set_footer(text="Please confirm you want to proceed.")

    view = GiftConfirmationView()
    await ctx.send(embed=embed, view=view, ephemeral=True)
    await view.wait()

    # 4. Execute Transaction
    if view.confirmed:
        new_balance = database.execute_gift_transaction(
            sender_id=sender_id,
            sender_name=sender_name,
            receiver_id=receiver_id,
            receiver_name=receiver_name,
            amount=amount
        )

        if new_balance is not None:
            await ctx.send(f"✅ Gift sent! You gave **{format_cc(amount)}** to **{receiver_name}**. Your new balance is {format_cc(new_balance)}.", ephemeral=True)
            # Optionally, send a DM to the receiver
            try:
                receiver_user = await bot.fetch_user(int(receiver_id))
                await receiver_user.send(f"🎁 You received a gift of **{format_cc(amount)}** from **{ctx.author.display_name}**!")
            except (discord.Forbidden, discord.NotFound):
                pass # Can't send DM, but the transaction succeeded.
        else:
            await ctx.send("❌ Gift failed. This could be due to a change in your balance. Please try again.", ephemeral=True)


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

    shop_data = database.get_shop_data(user_id)
    if not shop_data:
        return await ctx.send("Could not retrieve your account data. Are you registered?", ephemeral=True)

    balance = shop_data['balance']
    user_upgrades = shop_data['upgrades']
    current_prestige = shop_data['prestige']

    embed = discord.Embed(title="Prestige Shop", description="Spend your Crew Coins to get ahead!", color=discord.Color.purple())
    embed.set_footer(text=f"Your current balance: {format_cc(balance)}")

    # --- Prestige Purchases (No changes here) ---
    prestige_text = ""
    for item_id, item_details in SHOP_ITEMS['PRESTIGE'].items():
        bundle_cost = calculate_prestige_bundle_cost(current_prestige, item_details['amount'])
        prestige_text += f"**{item_details['name']} (ID: `{item_id}`)**\nCost: **{format_cc(bundle_cost)}**\n\n"
    embed.add_field(name="--- Prestige Purchases ---", value=prestige_text, inline=False)

    # --- Upgrades Loop (Contains the fix) ---
    for category, items in SHOP_ITEMS.items():
        if category in ["PRESTIGE"]: continue # Skip prestige, already handled
        
        category_text = ""
        for item_id, item_details in items.items():
            current_tier = user_upgrades.get(item_details['name'], 0)
            description = item_details['description'] # Start with the base description

            # --- START OF NEW LOGIC FOR DYNAMIC DESCRIPTION ---
            if item_details['name'] == "High Roller License":
                current_limit = BET_LIMITS.get(current_tier, 9999)
                if current_tier < item_details['max_tier']:
                    next_limit = BET_LIMITS.get(current_tier + 1, 1000000)
                    # Append the dynamic info
                    description += f"\n*Current Limit: {format_cc(current_limit)} → Next Tier: {format_cc(next_limit)}*"
                else:
                    description += f"\n*Current Limit: {format_cc(current_limit)} (Max Tier)*"
            # --- END OF NEW LOGIC ---

            if current_tier >= item_details['max_tier']:
                status = "**(Max Tier)**"
            else:
                cost = item_details['costs'][current_tier]
                status = f"Tier {current_tier+1} Cost: **{format_cc(cost)}**"
            
            category_text += f"**{item_details['name']} (ID: `{item_id}`)**\n{description}\n*Your Tier: {current_tier}* | {status}\n\n"
        
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
    
    # --- Database Transaction for CC Deduction ---
    new_balance = database.execute_purchase_transaction(
        actor_id=user_id, 
        item_name=item_details['name'], 
        cost=cost, 
        upgrade_tier=new_tier
    )

    if new_balance is not None:
        # --- LOGIC FOR PRESTIGE ---
        if item_details['type'] == 'prestige':
            # Instead of updating the CSV, we now log the purchase to our new ledger table.
            log_success = database.log_prestige_purchase(user_id, item_details['amount'])
            if not log_success:
                # This is a critical error state. The user paid but didn't get credit.
                # Needs manual admin intervention.
                await ctx.send("CRITICAL ERROR: Your CC was spent, but the prestige purchase could not be logged. Please contact an admin immediately!")
                return
        
        # This success message now applies to both upgrades and prestige
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
    """Displays detailed information and a price chart for a given stock with 24h, 7d, and all-time views."""
    stock_info, history_df, top_holders_df = database.get_stock_details(identifier)
    
    if not stock_info:
        return await ctx.send(f"Could not find a stock for '{identifier}'.", ephemeral=True)

    ingamename = stock_info['ingamename']
    current_price = float(stock_info['current_price'])
    
    # --- Prepare historical data ---
    if not history_df.empty:
        history_df['price'] = pd.to_numeric(history_df['price'], errors='coerce')
        history_df['timestamp'] = pd.to_datetime(history_df['timestamp'])
    
    now = datetime.now(pytz.utc)
    history_24h = history_df[history_df['timestamp'] >= now - timedelta(days=1)]
    history_7d = history_df[history_df['timestamp'] >= now - timedelta(days=7)]
    history_3d = history_df[history_df['timestamp'] >= now - timedelta(days=3)]
    history_all = history_df

    def timeframe_stats(df, default_price):
        if df.empty:
            return default_price, default_price, default_price
        return df['price'].iloc[0], df['price'].max(), df['price'].min()

    price_24h, high_24h, low_24h = timeframe_stats(history_24h, current_price)
    price_7d, high_7d, low_7d = timeframe_stats(history_7d, current_price)
    all_time_high = history_all['price'].max() if not history_all.empty else current_price
    all_time_low = history_all['price'].min() if not history_all.empty else current_price

    price_change_24h = current_price - price_24h
    percent_change_24h = (price_change_24h / price_24h) * 100 if price_24h > 0 else 0

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
        f"Current Price:   {current_price:,.2f} CC\n"
        f"24h Change:      {'+' if price_change_24h >= 0 else ''}{price_change_24h:,.2f} CC ({'+' if percent_change_24h >= 0 else ''}{percent_change_24h:.2f}%)\n"
        f"All-Time High:   {all_time_high:,.2f} CC\n"
        f"All-Time Low:    {all_time_low:,.2f} CC\n"
        f"Market Cap:      {format_cc(market_cap)}\n"
        f"```"
    )
    embed.add_field(name="📊 Key Statistics", value=stats_text, inline=False)
    
    holders_text = "```\n"
    if not top_holders_df.empty:
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

    # --- Plotting function ---
    def plot_price(df, title):
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(df['timestamp'], df['price'], color='#00FF00', linewidth=2)
        ax.set_title(title, color='white')
        ax.set_ylabel('Price (CC)', color='white')
        ax.tick_params(axis='x', colors='white', rotation=15)
        ax.tick_params(axis='y', colors='white')
        ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
        fig.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', transparent=True)
        buf.seek(0)
        plt.close(fig)
        return buf

    # --- Send initial embed with all-time chart ---
    buf = plot_price(history_all, f"{ingamename} Price (All-Time)")
    file = discord.File(buf, filename="price_chart.png")
    embed.set_image(url="attachment://price_chart.png")
    message = await ctx.send(embed=embed, file=file)

    # --- Buttons for timeframe selection ---
    from discord.ui import View, Button

    class TimeframeView(View):
        def __init__(self, embed, df_24h, df_3d, df_7d, df_all, ingamename):
            super().__init__(timeout=120)
            self.embed = embed
            self.df_24h = df_24h
            self.df_3d = df_3d
            self.df_7d = df_7d
            self.df_all = df_all
            self.ingamename = ingamename

        async def update_chart(self, interaction, df, title):
            buf = plot_price(df, title)
            file = discord.File(buf, filename="price_chart.png")
            self.embed.set_image(url="attachment://price_chart.png")
            await interaction.response.send_message(embed=self.embed, file=file, ephemeral=True)

        @discord.ui.button(label="24h", style=discord.ButtonStyle.blurple)
        async def show_24h(self, interaction, button):
            await self.update_chart(interaction, self.df_24h, f"{self.ingamename} Price (Last 24h)")
        
        @discord.ui.button(label="3d", style=discord.ButtonStyle.blurple)
        async def show_3d(self, interaction, button):
            await self.update_chart(interaction, self.df_3d, f"{self.ingamename} Price (Last 3d)")

        @discord.ui.button(label="7d", style=discord.ButtonStyle.blurple)
        async def show_7d(self, interaction, button):
            await self.update_chart(interaction, self.df_7d, f"{self.ingamename} Price (Last 7d)")

        @discord.ui.button(label="All-Time", style=discord.ButtonStyle.green)
        async def show_all(self, interaction, button):
            await self.update_chart(interaction, self.df_all, f"{self.ingamename} Price (All-Time)")

    view = TimeframeView(embed, history_24h, history_3d, history_7d, history_all, ingamename)
    await message.edit(view=view)

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


@bot.command(name="wealth")
async def wealth(ctx):
    """Displays the top 10 wealthiest players by net worth."""
    wealth_df = database.get_wealth_leaderboard()
    if wealth_df.empty:
        return await ctx.send("Could not retrieve the wealth leaderboard.", ephemeral=True)

    top_10_df = wealth_df.head(10).copy()
    top_10_df['net_worth'] = top_10_df['net_worth'].map('{:,.0f}'.format)
    top_10_df['cc_balance'] = top_10_df['cc_balance'].map('{:,.0f}'.format)
    top_10_df['share_value'] = top_10_df['share_value'].map('{:,.0f}'.format)

    top_10_df.rename(columns={'ingamename': 'Name', 'net_worth': 'Net Worth', 'cc_balance': 'CC Balance', 'share_value': 'Share Value'}, inplace=True)

    headers = ['Name', 'Net Worth', 'CC Balance', 'Share Value']
    image_file = generate_cml_image(top_10_df[headers], headers, "Wealth Leaderboard")
    await ctx.send(file=image_file)


async def _send_flows_visual(ctx, days: int = None):
    """Helper function to generate and send the flows visual."""
    wealth_df = database.get_wealth_leaderboard()
    if wealth_df.empty:
        return await ctx.send("Could not retrieve wealth leaderboard to determine top players.", ephemeral=True)

    top_10_df = wealth_df.head(10)
    top_10_ids = top_10_df['discord_id'].tolist()

    flows_df = database.get_financial_flows_for_users(top_10_ids, days=days)
    if flows_df.empty:
        return await ctx.send("No financial flows found for the top players in this period.", ephemeral=True)

    flows_df = pd.merge(top_10_df[['ingamename']], flows_df, on='ingamename', how='left').fillna(0)

    for col in flows_df.columns:
        if col != 'ingamename':
            flows_df[col] = pd.to_numeric(flows_df[col], errors='coerce').fillna(0).map('{:,.0f}'.format)

    title = f"Financial Flows (All-Time)" if days is None else f"Financial Flows (Last {days} Days)"
    flows_df.rename(columns={'ingamename': 'Name'}, inplace=True)

    image_file = generate_cml_image(flows_df, flows_df.columns.tolist(), title)
    await ctx.send(file=image_file)

@bot.command(name="alltime_flows")
async def alltime_flows(ctx):
    """Displays a summary of all-time financial flows for the top 10 wealthiest players."""
    await _send_flows_visual(ctx, None)

@bot.command(name="flows")
async def flows(ctx, days: int):
    """Displays a summary of financial flows for the top 10 wealthiest players over a number of days."""
    if days <= 0:
        return await ctx.send("Please provide a positive number of days.", ephemeral=True)
    await _send_flows_visual(ctx, days)


@bot.command(name="hype")
async def hype(ctx):
    """Displays a leaderboard for CC generation assistance."""
    hype_df = database.get_hype_data_for_all_users()
    if hype_df.empty:
        return await ctx.send("Could not retrieve hype data at this time.", ephemeral=True)

    hype_df['Hype Multiplier Granted'] = (hype_df['shares_held'] * 0.0005).map('{:.4f}x'.format)
    hype_df['Shares Held (in others)'] = hype_df['shares_held'].map('{:,.2f}'.format)
    hype_df['Gifts Given (CC)'] = hype_df['gifts_given'].map('{:,.0f}'.format)
    hype_df['Dividends Generated (CC)'] = hype_df['dividends_generated'].map('{:,.0f}'.format)

    hype_df.rename(columns={'ingamename': 'Name'}, inplace=True)

    headers = ['Name', 'Shares Held (in others)', 'Hype Multiplier Granted', 'Gifts Given (CC)', 'Dividends Generated (CC)']

    image_file = generate_cml_image(hype_df[headers], headers, "Hype & Generosity Leaderboard")
    await ctx.send(file=image_file)


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
            ('base_cc_earned', 'Base CC Earned'),
            ('hype_multiplier', 'Hype Multiplier'),
            ('hype_bonus_yield', 'Hype Bonus Yield')
        ]
        
        # 2. Calculate the true total, which isn't stored in the details
        total_earned = details.get('base_cc_earned', 0) + details.get('hype_bonus_yield', 0)
        
        # 3. Build the formatted string
        for key, title in key_order:
            if key in details:
                value = details[key]
                
                # Show the subtotal before the hype bonus
                if key == 'base_cc_earned':
                    formatted_string += "---------------------------------\n"
                    formatted_string += f"**{title}**: {value:.2f} CC\n"
                    
                elif key == 'hype_multiplier':
                    formatted_string += f"**{title}**: {value:.2f}x\n"
                
                else: # Tenure and Performance Yields
                    formatted_string += f"**{title}**: {value:.2f} CC\n"

        # 4. Add the final, clear total at the end
        formatted_string += "---------------------------------\n"
        formatted_string += f"**TOTAL PERSONAL EARNINGS**: {total_earned:.2f} CC\n"

    elif transaction_type == 'DIVIDEND':
        # Handles dividends with a clean, simple format
        source = details.get('source_player', 'Unknown')
        div_type = details.get('type', 'Dividend')
        formatted_string += f"**Source Player**: {source}\n"
        formatted_string += f"**Type**: {div_type}\n"
    
    elif transaction_type == 'GAMBLE':
        formatted_string = f"**Game**: {item_name}\n"
        formatted_string += f"**Bet**: {details.get('bet', 0):,.2f} CC\n"
        formatted_string += "---------------------------------\n"
        formatted_string += f"**Starting Card**: {details.get('starting_card', 'N/A')}\n"
        formatted_string += f"**Your Choice**: {details.get('choice', 'N/A')}\n"
        formatted_string += "---------------------------------\n"
        formatted_string += f"**Next Card**: {details.get('next_card', 'N/A')}\n"
        formatted_string += f"**Outcome**: {details.get('outcome', 'N/A')}\n"
        formatted_string += "---------------------------------\n"
        net_cc = details.get('net_cc', 0)
        sign = "+" if net_cc >= 0 else ""
        formatted_string += f"**Net CC**: {sign}{net_cc:,.2f} CC\n"
        return formatted_string

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

    # --- NEW: SECURITY CHECK ADDED HERE ---
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Check if the user clicking is the one who ran the /ledger command
        if interaction.user != self.ctx.author:
            # 1. Load your local GIF file
            file = discord.File("gifs/thatsridiculous.gif", filename="thatsridiculous.gif")

            # 2. Create the embed
            embed = discord.Embed(
                title="Hey! What do you think you're doing?",
                description="This transaction ledger doesn't belong to you. FOR PRIVATE EYES ONLY!",
                color=discord.Color.red()
            )
            
            # 3. Tell the embed to use the attached file as its image
            embed.set_image(url="attachment://thatsridiculous.gif")

            # 4. Send the private message and block the button press
            await interaction.response.send_message(embed=embed, file=file, ephemeral=True)
            return False
        # If the check passes, allow the button press
        return True

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
    # Add 'author' to the init method
    def __init__(self, *, timeout=30, trade_details: dict, author: discord.User):
        super().__init__(timeout=timeout)
        self.trade_details = trade_details
        self.confirmed = None
        self.author = author # Store the user who started the command

    # Add an interaction check to make sure the right user is clicking
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user != self.author:
            file = discord.File("gifs/Youcantdothat.gif", filename="Youcantdothat.gif")
            
            embed = discord.Embed(
                title="Access Denied",
                description="This isn't for you! Good try tho.",
                color=discord.Color.red()
            )
            # Use the direct image link you just copied
            embed.set_image(url="attachment://Youcantdothat.gif")
            await interaction.response.send_message(embed=embed, file=file, ephemeral=True)
            return False
        return True
    
    

    @discord.ui.button(label='Confirm Trade', style=discord.ButtonStyle.green)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # The interaction_check will automatically run before this callback
        self.confirmed = True
        # Disable buttons to prevent double-clicking
        for item in self.children:
            item.disabled = True
        # Update the original message to show the action was taken
        await interaction.response.edit_message(content="Processing your trade...", view=self)
        self.stop()

    @discord.ui.button(label='Cancel', style=discord.ButtonStyle.red)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # The interaction_check will automatically run before this callback
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
    market_state_df = pd.DataFrame(database.get_market_data_from_db()['market_state'])
    active_event = ""
    if not market_state_df.empty:
        market_state = market_state_df.set_index('state_name')['state_value']
        active_event = str(market_state.get('active_event', 'None'))
                                
    invest_tax_rate = 0.005 if active_event == "The Grand Derby" else 0.03
    current_price = float(stock['current_price'])
    subtotal = shares_to_buy * current_price
    broker_fee = subtotal * invest_tax_rate
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
        f"**Broker's Fee ({invest_tax_rate*100:.2f}%)**: {broker_fee:,.2f} CC\n"
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
    view = TradeConfirmationView(trade_details=trade_details, author=ctx.author)
    
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
    """Sell a specific number of shares you own, or use 'all' to sell everything."""
    user_id = str(ctx.author.id)

    # --- 1. Get Stock and Portfolio Data ---
    stock = database.get_stock_by_ticker_or_name(identifier)
    if not stock:
        return await ctx.send(f"Could not find a stock for '{identifier}'.", ephemeral=True)

    portfolio = database.get_portfolio_details(user_id)
    user_holding = portfolio[portfolio['stock_ingamename'] == stock['ingamename']]
    
    if user_holding.empty:
        return await ctx.send(f"You do not own any shares of **{stock['ingamename']}**.", ephemeral=True)

    shares_owned = float(user_holding['shares_owned'].iloc[0])

    # --- 2. Determine the amount to sell ---
    shares_to_sell = 0.0
    if shares_str.lower() == 'all':
        shares_to_sell = shares_owned
    else:
        try:
            shares_to_sell = float(shares_str)
            if shares_to_sell <= 0:
                return await ctx.send("Please enter a positive number of shares to sell.", ephemeral=True)
        except ValueError:
            return await ctx.send("Invalid amount. Please enter a number (e.g., 10.5) or the word 'all'.", ephemeral=True)

    # --- 3. Validate the Amount ---
    if shares_to_sell > shares_owned + 1e-9: # Add a tiny tolerance for floating point comparisons
        return await ctx.send(f"Insufficient shares. You are trying to sell {shares_to_sell:,.4f} but you only own {shares_owned:,.4f} of **{stock['ingamename']}**.", ephemeral=True)

    # --- 4. Calculate Trade Details ---
    market_state_df = pd.DataFrame(database.get_market_data_from_db()['market_state'])
    active_event = ""
    if not market_state_df.empty:
        market_state = market_state_df.set_index('state_name')['state_value']
        active_event = str(market_state.get('active_event', 'None'))
    
    balance = database.get_user_balance_by_discord_id(user_id)
    current_price = float(stock['current_price'])
    subtotal = shares_to_sell * current_price
    sell_tax_rate = 0.50 if active_event == "The Grand Derby" else 0.03
    broker_fee = subtotal * sell_tax_rate    
    total_proceeds = subtotal - broker_fee

    # --- 5. Confirmation UI ---
    new_balance_preview = float(balance) + total_proceeds
    embed = discord.Embed(title="Trade Confirmation: SELL", color=discord.Color.red())
    details = (
        f"**Stock**: {stock['ingamename']} (${stock['ticker']})\n"
        f"**Shares**: {shares_to_sell:,.4f}\n"
        f"**Price/Share**: {current_price:,.2f} CC\n"
        f"--------------------------\n"
        f"**Gross Proceeds**: {subtotal:,.2f} CC\n"
        f"**Broker's Fee ({sell_tax_rate*100:.1f}%)**: {broker_fee:,.2f} CC\n"
        f"**Total Proceeds**: **{total_proceeds:,.2f} CC**\n\n"
        f"*Your new balance will be: {new_balance_preview:,.2f} CC*"
    )
    embed.description = details
    
    target_id = database.get_discord_id_by_name(stock['ingamename'])

    trade_details = {
        'actor_id': user_id,
        'target_id': target_id,
        'stock_name': stock['ingamename'],
        'shares': -shares_to_sell,
        'price_per_share': current_price,
        'total_cost': total_proceeds,
        'fee': broker_fee,
        'transaction_type': 'SELL'
    }
    view = TradeConfirmationView(trade_details=trade_details, author=ctx.author)

    await ctx.send(embed=embed, view=view, ephemeral=True)
    await view.wait()

    # --- 6. Execute Trade if Confirmed ---
    if view.confirmed:
        # This calls the robust, fixed function in database.py
        new_balance = database.execute_trade_transaction(**view.trade_details)
        if new_balance is not None:
            await ctx.send(f"✅ **Trade Executed!** You sold {shares_to_sell:,.4f} shares of **{stock['ingamename']}**. Your new balance is {format_cc(new_balance)}.", ephemeral=True)
        else:
            await ctx.send("❌ **Trade Failed!** This could be due to insufficient shares or a database error. Please try again.", ephemeral=True)


@bot.command(name="derbyleaderboard")
async def derby_leaderboard(ctx):
    """Displays the live leaderboard for the current Grand Derby event."""
    
    # Check if an event is actually active
    market_state_df = pd.DataFrame(database.get_market_data_from_db()['market_state'])
    if not market_state_df.empty:
        market_state = market_state_df.set_index('state_name')['state_value']
        active_event = str(market_state.get('active_event', 'None'))
        if active_event != "The Grand Derby":
            return await ctx.send("The Grand Derby is not currently active.", ephemeral=True)
    else:
        return await ctx.send("Could not retrieve market state.", ephemeral=True)

    await ctx.send("`Fetching the latest Grand Derby leaderboard...`", ephemeral=True)

    leaderboard_df = database.get_event_leaderboard_data()

    if leaderboard_df.empty:
        return await ctx.send("Could not generate the leaderboard. No data found or an error occurred.", ephemeral=True)

    # --- Pagination and Embed Generation ---
    items_per_page = 15
    pages = [leaderboard_df.iloc[i:i + items_per_page] for i in range(0, len(leaderboard_df), items_per_page)]
    if not pages: pages.append(pd.DataFrame())
    total_pages = len(pages)
    current_page = 0

    async def generate_embed(page_num):
        page_data = pages[page_num]
        embed = discord.Embed(title="🏆 Grand Derby Live Leaderboard 🏆", color=discord.Color.gold())
        
        leaderboard_text = "```\n"
        leaderboard_text += "{:<16} | {:<10} | {:<8} | {:<8} | {}\n".format("Name", "CC Gained", "Fans Gained", "Perf. Yield", "Stock Profit")
        leaderboard_text += "-" * 56 + "\n"

        for i, row in page_data.iterrows():
            name = row['ingamename'][:15]
            cc_gained = f"{row['cc_gained']:,.0f}"
            fans_gained = f"{row['fans_gained']:,.0f}"
            perf_yield = f"{row['performance_yield']:,.0f}"
            stock_profit = f"{row['stock_profit']:,.0f}"
            leaderboard_text += "{:<16} | {:<12} | {:<10} | {:<12} | {}\n".format(name, cc_gained, fans_gained, perf_yield, stock_profit)

        leaderboard_text += "```"
        embed.description = leaderboard_text
        embed.set_footer(text=f"Page {page_num + 1} of {total_pages} | Sorted by Total CC Gained")
        return embed

    # Using your existing PaginationView class
    initial_embed = await generate_embed(0)
    view = PaginationView(ctx.interaction, generate_embed, total_pages)
    await ctx.send(embed=initial_embed, view=view)

# --- NEW: Manual Event Control Commands ---

@bot.group(name="event", invoke_without_command=True)
@commands.check(is_admin)
async def event(ctx):
    """Parent command for manually controlling market events."""
    await ctx.send("Invalid event command. Use `/event start [hours]` or `/event stop`.")

@event.command(name="start")
@commands.check(is_admin)
async def event_start(ctx, hours: int):
    """(Admin Only) Manually starts the Grand Derby for a set number of hours."""
    if hours <= 0:
        await ctx.send("Duration must be a positive number of hours.")
        return

    # --- THIS IS THE FIX ---
    # Send a simple, initial message to acknowledge the command is running.
    # We remove defer() and thinking=True.
    await ctx.send(f"Processing `/event start {hours}`...")

    # Calculate the event's end time based on the current time and duration
    run_timestamp = datetime.now(pytz.timezone('US/Central'))
    end_time = run_timestamp + timedelta(hours=hours)

    # Format the end time into a string that the analysis script can read
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S%z')
    formatted_end_time = f"{end_time_str[:-2]}:{end_time_str[-2:]}"

    # --- Take Snapshot FIRST ---
    snapshot_success = database.create_event_snapshot()
    if not snapshot_success:
        await ctx.send("`ERROR: Failed to create the event leaderboard snapshot. The contest cannot be tracked. Aborting event start.`")
        return
        
    # --- Set Event State in Database ---
    database.update_market_state_value('active_event', 'The Grand Derby')
    success = database.update_market_state_value('event_end_timestamp', formatted_end_time)

    if success:
        discord_timestamp = f"<t:{int(end_time.timestamp())}:R>"

        # --- THIS IS THE PUBLIC ANNOUNCEMENT ---
        fan_exchange_channel = discord.utils.get(ctx.guild.channels, name=FAN_EXCHANGE_CHANNEL_NAME)
        
        announcement_text = (
            f"**THE GRAND DERBY IS LIVE!**\n\n"
            f"For the next **{hours} hours**, the market is in overdrive! The event will conclude {discord_timestamp}.\n\n"
            f"**Amplified Earnings**: The Performance Yield multiplier is now **x5.0**!\n"
            f"**Accelerated Prices**: The Baggins Index is now hyper-responsive, reacting to performance over the last **3 hours**!\n\n"
            f"Let's rally some fans! Use `/derbyleaderboard` to track the contest!"
        )

        if fan_exchange_channel:
            await fan_exchange_channel.send(announcement_text)
            await ctx.send(f"✅ Successfully started the Grand Derby in {fan_exchange_channel.mention}.")
        else:
            await ctx.send(f"Could not find the `#{FAN_EXCHANGE_CHANNEL_NAME}` channel to post the announcement.")
    else:
        # Use ctx.send instead of ctx.followup.send
        await ctx.send("Failed to start the event. Could not update the market state.")

@event.command(name="stop")
@commands.check(is_admin)
async def event_stop(ctx):
    """(Admin Only) Stops the Grand Derby immediately."""
    database.update_market_state_value('active_event', 'None')
    success = database.update_market_state_value('event_end_timestamp', 'None')

    if success:
        await ctx.send("Successfully stopped the active event. The market will return to normal on the next analysis cycle.")
    else:
        await ctx.send("Failed to stop the event in the database.")

@event.error
@event_start.error
@event_stop.error
async def event_command_error(ctx, error):
    """Handles errors for the event command group, specifically for non-admins."""
    if isinstance(error, commands.CheckFailure):
        await ctx.send("You do not have permission to use this command.")
    else:
        # Send the actual error message to the channel to help debug.
        await ctx.send(f"An unexpected error occurred: {error}")

class HigherLowerView(discord.ui.View):
    """An interactive view with buttons for the Higher or Lower game."""
    def __init__(self, author: discord.User, bet_amount: int, first_card: tuple):
        super().__init__(timeout=60)
        self.author = author
        self.bet_amount = bet_amount
        self.first_card = first_card
        self.deck = self._create_deck()
        self.deck.remove(first_card)
        self.choice = None

    def _create_deck(self):
        return [(rank, suit) for rank in CARD_RANKS for suit in CARD_SUITS]

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("This is not your game! Start one with `/higherlower`.", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        """
        Called when the view's 60-second timer expires.
        This now treats the timeout as a forfeit and resolves the game as a loss.
        """
        # Disable all the buttons to show the game is over
        for item in self.children:
            item.disabled = True

        # --- START OF NEW LOGIC ---

        # The outcome is a loss, so winnings are 0.
        winnings = 0
        net_change = winnings - self.bet_amount # This will be a negative number

        details = {
            "bet": self.bet_amount,
            "starting_card": f"{self.first_card[0]} {CARD_SUITS[self.first_card[1]]}",
            "choice": "TIMEOUT (FORFEIT)",
            "next_card": "N/A",
            "outcome": "FORFEIT",
            "net_cc": net_change,
        }

        # Execute the database transaction to deduct the bet
        new_balance = database.execute_gambling_transaction(
            str(self.author.id), "Higher or Lower", self.bet_amount, winnings, details
        )
        
        # Create a new, clear message indicating the timeout
        timeout_embed = discord.Embed(
            title="Game Timed Out: Forfeit!",
            description=f"{self.author.display_name} did not make a choice in time. The bet of **{format_cc(self.bet_amount)}** has been forfeited.",
            color=discord.Color.dark_grey()
        )
        if new_balance is not None:
            timeout_embed.set_footer(text=f"Your new balance is {format_cc(new_balance)}")
        
        # Edit the original game message to show the timeout status
        await self.message.edit(embed=timeout_embed, view=self)
    
    async def resolve_game(self, interaction: discord.Interaction):
        self.stop()
        for item in self.children:
            item.disabled = True

        second_card = random.choice(self.deck)
        first_val = CARD_RANKS[self.first_card[0]]
        second_val = CARD_RANKS[second_card[0]]

        outcome = "LOSS" # Default to loss (covers ties)
        if (self.choice == "HIGHER" and second_val > first_val) or \
           (self.choice == "LOWER" and second_val < first_val):
            outcome = "WIN"

        winnings = self.bet_amount * PAYOUT_MULTIPLIER if outcome == "WIN" else 0
        net_change = winnings - self.bet_amount
        
        details = {
            "bet": self.bet_amount,
            "starting_card": f"{self.first_card[0]} {CARD_SUITS[self.first_card[1]]}",
            "choice": self.choice,
            "next_card": f"{second_card[0]} {CARD_SUITS[second_card[1]]}",
            "outcome": outcome,
            "net_cc": net_change,
        }

        new_balance = database.execute_gambling_transaction(
            str(self.author.id), "Higher or Lower", self.bet_amount, winnings, details
        )
        
        color = discord.Color.green() if outcome == "WIN" else discord.Color.red()
        result_text = f"You win! Net gain: **{format_cc(net_change)}**" if outcome == "WIN" else f"You lose. Net loss: **{format_cc(abs(net_change))}**"
        
        embed = discord.Embed(title=f"Higher or Lower: {outcome}!", description=result_text, color=color)
        embed.add_field(name="Starting Card", value=f"**{details['starting_card']}**", inline=True)
        embed.add_field(name="Next Card", value=f"**{details['next_card']}**", inline=True)
        if new_balance is not None:
            embed.set_footer(text=f"Your new balance is {format_cc(new_balance)}")
        
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Higher", style=discord.ButtonStyle.success)
    async def higher_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "HIGHER"
        await self.resolve_game(interaction)

    @discord.ui.button(label="Lower", style=discord.ButtonStyle.danger)
    async def lower_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "LOWER"
        await self.resolve_game(interaction)

@bot.command(name="higherlower")
@commands.cooldown(1, 5, commands.BucketType.user)
async def higherlower(ctx, bet: int):
    """Play a game of Higher or Lower for CC!"""
    user_id = str(ctx.author.id)

    if bet <= 100:
        return await ctx.send("The minimum bet is 101 CC.", ephemeral=True)

    # --- START OF NEW BET LIMIT LOGIC ---

    # 1. Get the two potential maximums
    house_balance = database.get_house_balance()
    house_max_bet = max(1000, int(house_balance * 0.35))
    player_personal_limit = database.get_player_betting_limit(user_id)

    # 2. The true max bet is the LOWER of the two limits
    max_bet = min(house_max_bet, player_personal_limit)

    if bet > max_bet:
        await ctx.send(
            f"Your bet of **{format_cc(bet)}** exceeds your current maximum bet limit of **{format_cc(max_bet)}**.\n"
            f"Purchase a 'High Roller License' in the `/shop` to increase your personal limit.",
            ephemeral=True
        )
        higherlower.reset_cooldown(ctx)
        return

    # 2. Perform a PRELIMINARY balance check for good user experience.
    # The final, secure check happens in the database transaction.
    balance = database.get_user_balance_by_discord_id(user_id)
    if balance is None or balance < bet:
        await ctx.send(f"You don't have enough CC to make that bet. Your balance is {format_cc(balance)}.", ephemeral=True)
        higherlower.reset_cooldown(ctx) # Reset cooldown on a failed check
        return

    # 3. Create the game if all checks pass
    deck = [(rank, suit) for rank in CARD_RANKS for suit in CARD_SUITS]
    random.shuffle(deck)
    first_card = deck.pop()
    
    embed = discord.Embed(
        title=f"{ctx.author.display_name} bets {format_cc(bet)}!",
        description="Will the next card be higher or lower?",
        color=discord.Color.blue()
    )
    card_str = f"{first_card[0]} {CARD_SUITS[first_card[1]]}"
    embed.add_field(name="Your Card", value=f"**{card_str}**")
    embed.set_footer(text="Aces are high. A tie is a loss. A decision must be made within 60 seconds or the bet is forfeit.")

    view = HigherLowerView(author=ctx.author, bet_amount=bet, first_card=first_card)
    
    await ctx.send(embed=embed, view=view)

@bot.command(name="add_house_funds")
@commands.check(is_admin)
async def add_house_funds(ctx, amount: int):
    """(Admin Only) Adds a specified amount of CC to the house wallet."""
    admin_id = str(ctx.author.id)

    if amount <= 0:
        return await ctx.send("Please enter a positive amount to add.", ephemeral=True)

    # Call the new, safe database function
    new_balance = database.add_funds_to_house_wallet(float(amount), admin_id)

    if new_balance is not None:
        embed = discord.Embed(
            title="✅ House Wallet Funded",
            description=f"You have successfully added **{format_cc(amount)}** to the house wallet.",
            color=discord.Color.green()
        )
        embed.set_footer(text=f"The new house balance is {format_cc(new_balance)}")
        await ctx.send(embed=embed)
    else:
        await ctx.send("❌ An error occurred while trying to add funds to the house wallet. Check the logs.", ephemeral=True)

@add_house_funds.error
async def add_house_funds_error(ctx, error):
    if isinstance(error, commands.CheckFailure):
        await ctx.send("You do not have permission to use this command.", ephemeral=True)
    else:
        await ctx.send(f"An error occurred: {error}", ephemeral=True)

# --- Run the Bot ---
load_dotenv() # Loads variables from .env file
TOKEN = os.getenv('DISCORD_TOKEN')

if TOKEN is None:
    print("ERROR: DISCORD_TOKEN not found in .env file.")
else:
    bot.run(TOKEN)