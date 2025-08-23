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
        
    # UPDATED: This now correctly sends to the dedicated channel
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


# --- Bot Commands ---

@bot.command()
async def help(ctx):
    """Displays a list of all available commands and their descriptions."""
    
    embed = discord.Embed(title="TRACKMASTER BOT Help", description="Here are all the commands you can use to track your performance and prestige.", color=discord.Color.blue())
    embed.add_field(name="SETUP (Required)", value="`/register [your-in-game-name]` - Links your Discord account to your exact in-game name. You must do this first!", inline=False)
    embed.add_field(name="PERSONAL STATS", value="`/myprogress` - Get a personalized summary of your progress since you last checked.", inline=False)
    embed.add_field(name="CLUB CHARTS & REPORTS", value=("`/top10` - Shows the current top 10 members by monthly fan gain.\n" "`/prestige_leaderboard` - Displays the all-time prestige point leaderboard.\n" "`/performance` - Posts the historical fan gain heatmap.\n" "`/log [member_name]` - Gets the detailed performance log for any member."), inline=False)
    embed.add_field(name="FAN EXCHANGE (Stock Market)", value=("`/exchange_help` - Provides a concise explanation and startup guide for the Fan Exchange system.\n" "`/market` - Displays the all stocks and some info.\n" "`/portfolio` - View your current stock holdings and their performance.\n" "`/stock [name/ticker]` - Shows the price history and stats for a specific racer.\n" "`/invest [name/ticker] [amount]` - Buy shares in a racer's stock, specifying CC to invest.\n" "`/sell [name/ticker] [amount]` - Sell shares of a racer's stock, specifying shares to sell.\n" "`/shop` - See what you can buy with your CC! Earnings upgrades and prestige!" "`/buy [shop_id]` - Purchase something from the shop!" "`/set_ticker [2-5 letter ticker]` - Set your unique stock ticker symbol."), inline=False)
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
              "`/market` - View the top market movers.\n"
              "`/stock [name/ticker]` - Get info on a specific stock.\n"
              "`/invest [name/ticker] [amount]` - Buy shares.\n"
              "`/sell [name/ticker] [shares]` - Sell shares.\n"
              "`/shop` & `/buy [item_id]` - Spend your CC!",
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
    user_id = str(ctx.author.id)
    inGameName = get_inGameName(ctx.author.id)

    if not inGameName:
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
        user_row = init_df[init_df['inGameName'] == inGameName]
        if not user_row.empty and pd.notna(user_row.iloc[0]['ticker']):
            await ctx.send(f"You have already set your ticker to **${user_row.iloc[0]['ticker']}**. It cannot be changed.", ephemeral=True)
            return
            
        # Check if ticker is already in use
        if ticker in init_df['ticker'].values:
            await ctx.send(f"The ticker **${ticker}** is already taken. Please choose another.", ephemeral=True)
            return

        # --- Update and Save ---
        init_df.loc[init_df['inGameName'] == inGameName, 'ticker'] = ticker
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

    # --- Load Data ---
    try:
        crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
        upgrades_df = load_market_file('shop_upgrades.csv', dtype={'discord_id': str})
        enriched_df = pd.read_csv(ENRICHED_FAN_LOG_CSV)
    except FileNotFoundError as e:
        await ctx.send(f"**Admin Alert:** A required market file is missing: `{e.filename}`. The shop cannot function.", ephemeral=True)
        return

    # --- START OF DEBUGGING BLOCK ---
    # We print the columns right after loading to see what pandas is reading.
    print(f"DEBUG: Columns in enriched_fan_log.csv are: {enriched_df.columns.tolist()}")
    # --- END OF DEBUGGING BLOCK ---

    # --- Robust User Data Lookup ---
    user_data = crew_coins_df[crew_coins_df['discord_id'] == user_id]
    if user_data.empty:
        return await ctx.send("It looks like you don't have a Fan Exchange account yet. Make sure you are registered with `/register`.", ephemeral=True)

    balance = float(user_data['balance'].iloc[0])
    inGameName = user_data['inGameName'].iloc[0]

    try:
        # This is the line that has been causing the error.
        user_stats = enriched_df[enriched_df['inGameName'] == inGameName]
    except KeyError:
        # If we get a KeyError, this block will run, giving us a definitive answer.
        error_message = (
            "**CRITICAL ERROR in `/shop`:**\n"
            f"I tried to find the column `inGameName` in `enriched_fan_log.csv`, but it doesn't exist.\n"
            f"The columns I actually found are: `{enriched_df.columns.tolist()}`\n\n"
            "Please check `analysis.py` to ensure it's writing the correct column headers, and that the bot is reading the correct, most recent file."
        )
        print(error_message) # Also print to console for the bot owner
        await ctx.send("Sorry, a critical data error occurred. The `inGameName` column could not be found. Please notify the admin.", ephemeral=True)
        return # Stop the command here

    if user_stats.empty:
        return await ctx.send("I couldn't find any analysis data for you in the logs. A data refresh may be needed to purchase prestige.", ephemeral=True)

    latest_stats = user_stats.sort_values('timestamp').iloc[-1]
    current_prestige = latest_stats['lifetimePrestige']
    user_upgrades = upgrades_df[upgrades_df['discord_id'] == user_id].set_index('upgrade_name')['tier'].to_dict()

    # --- Build and Send Embed (This part remains the same) ---
    embed = discord.Embed(title="Prestige Shop", description="Spend your Crew Coins to get ahead!", color=discord.Color.purple())
    embed.set_footer(text=f"Your current balance: {format_cc(balance)}")

    prestige_text = ""
    for item_id, item_details in SHOP_ITEMS['PRESTIGE'].items():
        bundle_cost = calculate_prestige_bundle_cost(current_prestige, item_details['amount'])
        prestige_text += f"**{item_details['name']} (ID: `{item_id}`)**\nCost: **{format_cc(bundle_cost)}**\n\n"
    embed.add_field(name="--- Prestige Purchases ---", value=prestige_text, inline=False)

    for category, items in SHOP_ITEMS.items():
        if category == "PRESTIGE": continue
        category_text = ""
        for item_id, item_details in items.items():
            current_tier = user_upgrades.get(item_details['name'], 0)
            status = "**(Max Tier)**" if current_tier >= item_details['max_tier'] else f"Tier {current_tier+1} Cost: **{format_cc(item_details['costs'][current_tier])}**"
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
        await ctx.send("Invalid item ID. Use `/shop` to see available items.", ephemeral=True)
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
        inGameName = user_data_row['inGameName'].iloc[0]
        
        if item_details['type'] == 'upgrade':
            upgrade_name = item_details['name']
            user_upgrade_row = upgrades_df[(upgrades_df['discord_id'] == user_id) & (upgrades_df['upgrade_name'] == upgrade_name)]
            current_tier = user_upgrade_row['tier'].iloc[0] if not user_upgrade_row.empty else 0
            if current_tier >= item_details['max_tier']:
                await ctx.send(f"You have reached the max tier for **{upgrade_name}**.", ephemeral=True)
                return
            cost = item_details['costs'][current_tier]
            if balance < cost:
                await ctx.send(f"You need {format_cc(cost)}.", ephemeral=True)
                return
            if not user_upgrade_row.empty:
                upgrades_df.loc[user_upgrade_row.index, 'tier'] += 1
            else:
                new_upgrade = pd.DataFrame([{'discord_id': user_id, 'upgrade_name': upgrade_name, 'tier': 1}])
                upgrades_df = pd.concat([upgrades_df, new_upgrade], ignore_index=True)
            upgrades_df.to_csv('market/shop_upgrades.csv', index=False)

        elif item_details['type'] == 'prestige':
            enriched_df = pd.read_csv(ENRICHED_FAN_LOG_CSV)
            latest_stats = enriched_df[enriched_df['inGameName'] == inGameName].sort_values('timestamp').iloc[-1]
            # --- UPDATED (Task 3.2) ---
            current_lifetime_prestige = latest_stats['lifetimePrestige']
            amount_to_buy = item_details['amount']
            cost = calculate_prestige_bundle_cost(current_lifetime_prestige, amount_to_buy)

            if balance < cost:
                await ctx.send(f"You need {format_cc(cost)}.", ephemeral=True)
                return

            new_prestige_row = latest_stats.copy()
            now = datetime.now(pytz.timezone('US/Central'))
            timestamp_str = now.strftime('%Y-%m-%d %H:%M:%S%z')
            new_prestige_row['timestamp'] = f"{timestamp_str[:-2]}:{timestamp_str[-2:]}"
            new_prestige_row['prestigeGain'] = float(amount_to_buy)
            new_prestige_row['lifetimePrestige'] += float(amount_to_buy)
            new_prestige_row['monthlyPrestige'] = latest_stats['monthlyPrestige'] # Monthly is unaffected
            new_prestige_row['performancePrestigePoints'] = 0
            new_prestige_row['tenurePrestigePoints'] = 0
            enriched_df = pd.concat([enriched_df, new_prestige_row.to_frame().T], ignore_index=True)
            enriched_df.to_csv('enriched_fan_log.csv', index=False)

        crew_coins_df.loc[crew_coins_df['discord_id'] == user_id, 'balance'] -= cost
        crew_coins_df.to_csv('market/crew_coins.csv', index=False)
        log_market_transaction(user_id, 'PURCHASE', 'SYSTEM', item_details['name'], 1, -cost, 0)
        await ctx.send(embed=discord.Embed(title="✅ Purchase Successful!", description=f"You spent **{format_cc(cost)}** on **{item_details['name']}**.", color=discord.Color.green()))
    finally:
        os.remove(lock_file)

@bot.command(name="portfolio")
async def portfolio(ctx):
    """Displays the user's current CC balance, stock holdings, and P/L with pagination."""
    user_id = str(ctx.author.id)

    # --- Load Data ---
    crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
    portfolios_df = load_market_file('portfolios.csv', dtype={'investor_discord_id': str})
    stock_prices_df = load_market_file('stock_prices.csv')
    history_df = load_market_file('stock_price_history.csv')
    transactions_df = load_market_file('universal_transaction_log.csv', dtype={'actor_id': str})
    init_df = load_market_file('member_initialization.csv')
    ticker_map = pd.Series(init_df.ticker.values, index=init_df.inGameName).to_dict()

    user_coin_data = crew_coins_df[crew_coins_df['discord_id'] == user_id]
    if user_coin_data.empty:
        return await ctx.send("You do not have a Fan Exchange account yet.", ephemeral=True)
    
    balance = user_coin_data['balance'].iloc[0]
    inGameName = user_coin_data['inGameName'].iloc[0]
    user_stocks = portfolios_df[portfolios_df['investor_discord_id'] == user_id].copy()

    # --- Calculations ---
    total_stock_value = 0
    total_day_change = 0
    sponsorships = []

    if not user_stocks.empty and not stock_prices_df.empty:
        stock_prices_map = stock_prices_df.set_index('inGameName')['current_price'].to_dict()
        
        for index, stock in user_stocks.iterrows():
            stock_name = stock['stock_inGameName']
            current_price = stock_prices_map.get(stock_name, 0)
            value = stock['shares_owned'] * current_price
            user_stocks.loc[index, 'value'] = value
            total_stock_value += value
            
            cost_basis = get_cost_basis(user_id, stock_name, transactions_df)
            pl = (current_price - cost_basis) * stock['shares_owned'] if cost_basis > 0 else 0
            pl_percent = (pl / (cost_basis * stock['shares_owned'])) * 100 if cost_basis > 0 and stock['shares_owned'] > 0 else 0
            user_stocks.loc[index, 'pl'] = pl
            user_stocks.loc[index, 'pl_percent'] = pl_percent
            
            day_change, _ = get_24hr_change(stock_name, history_df, current_price)
            day_change_value = day_change * stock['shares_owned']
            user_stocks.loc[index, 'day_change_value'] = day_change_value
            total_day_change += day_change_value

            all_holders = portfolios_df[portfolios_df['stock_inGameName'] == stock_name].sort_values('shares_owned', ascending=False)
            if not all_holders.empty and all_holders.iloc[0]['investor_discord_id'] == user_id:
                sponsorship_text = ""
                if len(all_holders) > 1:
                    lead = all_holders.iloc[0]['shares_owned'] - all_holders.iloc[1]['shares_owned']
                    sponsorship_text = f"(by {lead:.2f} sh)"
                else:
                    sponsorship_text = "(Sole Owner)"
                
                ticker = ticker_map.get(stock_name)
                sponsorship_name = f"${ticker}" if pd.notna(ticker) and isinstance(ticker, str) else stock_name
                sponsorships.append(f"{sponsorship_name} {sponsorship_text}")

        user_stocks.sort_values('value', ascending=False, inplace=True)

    total_portfolio_value = balance + total_stock_value
    total_day_change_percent = (total_day_change / (total_portfolio_value - total_day_change)) * 100 if (total_portfolio_value - total_day_change) != 0 else 0

    # --- PAGINATION LOGIC ---
    stocks_per_page = 10
    pages = [user_stocks.iloc[i:i + stocks_per_page] for i in range(0, len(user_stocks), stocks_per_page)]
    if not pages: pages.append(pd.DataFrame())
    current_page = 0

    async def generate_embed(page_num):
        embed = discord.Embed(
            title=f"{ctx.author.display_name}'s Portfolio",
            description=f"*In-Game Name: {inGameName}*",
            color=discord.Color.gold()
        )
        summary_text = (
            f"```\n"
            f"CC Balance:      {format_cc(balance)}\n"
            f"Stock CC:       {format_cc(total_stock_value)}\n"
            f"Total CC:       {format_cc(total_portfolio_value)}\n"
            f"Today's Δ:    {'+' if total_day_change >= 0 else ''}{format_cc(total_day_change)} ({'+' if total_day_change_percent >= 0 else ''}{total_day_change_percent:.2f}%)\n"
            f"```"
        )
        embed.add_field(name="💰 Account Summary", value=summary_text, inline=False)
        
        page_data = pages[page_num]
        holdings_text = "```\n"
        holdings_text += "{:<7} | {:<7} | {:<8} | {:<10} | {}\n".format("Ticker", "Shares", "CC", "Today's Δ", "P/L")
        holdings_text += "-"*56 + "\n"
        if not page_data.empty:
            for _, stock in page_data.iterrows():
                stock_name = stock['stock_inGameName']
                ticker = ticker_map.get(stock_name)
                if pd.notna(ticker) and isinstance(ticker, str):
                    display_name = f"${ticker[:5]}"
                else:
                    display_name = stock_name[:6]
                
                pl_str = f"{'+' if stock['pl'] >= 0 else ''}{stock['pl']:,.0f}, {'+' if stock['pl_percent'] >= 0 else ''}{stock['pl_percent']:.1f}%"
                day_change_str = f"{'+' if stock['day_change_value'] >= 0 else ''}{stock['day_change_value']:,.0f}"
                value_str = f"{stock['value']:,.0f}"

                holdings_text += "{:<7} | {:<7.2f} | {:<8} | {:<10} | {}\n".format(
                    display_name, stock['shares_owned'], value_str, day_change_str, pl_str
                )
        else:
            holdings_text += "You do not own any stocks.\n"
        holdings_text += "```"
        embed.add_field(name=f"📈 Stock Holdings (Page {page_num + 1}/{len(pages)})", value=holdings_text, inline=False)

        if sponsorships:
            embed.set_footer(text=f"🏆 Sponsorships: {', '.join(sponsorships)}")
        return embed

    # --- INTERACTIVE MESSAGE ---
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


@bot.command(name="market")
async def market(ctx):
    """Displays a comprehensive overview of the stock market with pagination."""
    # --- Load Data ---
    stock_prices_df = load_market_file('stock_prices.csv')
    history_df = load_market_file('stock_price_history.csv')
    portfolios_df = load_market_file('portfolios.csv')
    transactions_df = load_market_file('universal_transaction_log.csv')
    init_df = load_market_file('member_initialization.csv')
    ticker_map = pd.Series(init_df.ticker.values, index=init_df.inGameName).to_dict()

    if stock_prices_df.empty:
        return await ctx.send("Market is currently closed or has insufficient data.", ephemeral=True)

    # --- Calculations ---
    market_data = []
    total_market_cap = 0
    stocks_up = 0
    
    for _, stock in stock_prices_df.iterrows():
        stock_name = stock['inGameName']
        current_price = stock['current_price']
        
        price_change, percent_change = get_24hr_change(stock_name, history_df, current_price)
        if price_change > 0: stocks_up += 1

        total_shares = portfolios_df[portfolios_df['stock_inGameName'] == stock_name]['shares_owned'].sum()
        market_cap = total_shares * current_price
        total_market_cap += market_cap

        largest_holder_row = portfolios_df[portfolios_df['stock_inGameName'] == stock_name].nlargest(1, 'shares_owned')
        if not largest_holder_row.empty:
            holder_id = largest_holder_row['investor_discord_id'].iloc[0]
            holder_name = get_inGameName(int(holder_id)) or "N/A"
            holder_shares = largest_holder_row['shares_owned'].iloc[0]
            largest_holder = f"{holder_shares:.1f} sh" # Trimmed name
        else:
            largest_holder = "N/A"
            
        market_data.append({
            'name': stock_name,
            'ticker': ticker_map.get(stock_name),
            'price': current_price,
            'price_change': price_change,
            'percent_change': percent_change,
            'largest_holder': largest_holder
        })

    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='mixed').dt.tz_convert('UTC')
    one_day_ago = datetime.now(pytz.utc) - timedelta(days=1)
    recent_trades = transactions_df[transactions_df['timestamp'] >= one_day_ago]
    volume = recent_trades['cc_amount'].abs().sum()
    
    market_sentiment = "Bullish" if stocks_up > len(stock_prices_df) / 2 else "Bearish" if stocks_up < len(stock_prices_df) / 2 else "Neutral"
    sorted_by_change = sorted(market_data, key=lambda x: x['percent_change'], reverse=True)
    
    # --- PAGINATION LOGIC ---
    stocks_per_page = 15
    pages = [market_data[i:i + stocks_per_page] for i in range(0, len(market_data), stocks_per_page)]
    current_page = 0

    async def generate_embed(page_num):
        embed = discord.Embed(title="Baggins Index Market Overview", description="*A snapshot of all market activity.*", color=discord.Color.blue())
        stats_text = (
            f"```\n"
            f"Market Sentiment:   {market_sentiment} ({stocks_up} up, {len(stock_prices_df) - stocks_up} down)\n"
            f"Total Market Cap:   {format_cc(total_market_cap)}\n"
            f"24h Volume:         {format_cc(volume)}\n"
            f"```"
        )
        embed.add_field(name="📈 Market-Wide Statistics", value=stats_text, inline=False)
        
        if sorted_by_change:
            top_gainer = sorted_by_change[0]
            biggest_drop = sorted_by_change[-1]
            gainer_ticker = top_gainer['ticker'] if pd.notna(top_gainer['ticker']) and isinstance(top_gainer['ticker'], str) else top_gainer['name']
            drop_ticker = biggest_drop['ticker'] if pd.notna(biggest_drop['ticker']) and isinstance(biggest_drop['ticker'], str) else biggest_drop['name']
            movers_text = f"**Biggest Gainer:** ${gainer_ticker} ({'+' if top_gainer['percent_change'] >= 0 else ''}{top_gainer['percent_change']:.1f}%)\n"
            movers_text += f"**Biggest Drop:** ${drop_ticker} ({'+' if biggest_drop['percent_change'] >= 0 else ''}{biggest_drop['percent_change']:.1f}%)"
            embed.add_field(name="🔥 Top Movers (Last 24h)", value=movers_text, inline=False)
        
        page_data = pages[page_num]
        list_text = "```\n"
        list_text += "{:<16} | {:<7} | {:<8} | {}\n".format("Ticker", "Price", "24h Δ", "Largest Holder")
        list_text += "-"*56 + "\n"
        for stock in sorted(page_data, key=lambda x: x['price'], reverse=True):
            ticker = stock['ticker']
            if pd.notna(ticker) and isinstance(ticker, str):
                display_name = f"${ticker[:5]}"
            else:
                display_name = stock['name']

            change_str = f"{'+' if stock['percent_change'] >= 0 else ''}{stock['percent_change']:.1f}%"
            list_text += "{:<16} | {:<7.2f} | {:<8} | {}\n".format(
                display_name, stock['price'], change_str, stock['largest_holder']
            )
        list_text += "```"
        embed.add_field(name=f"📊 Full Stock List (Page {page_num + 1}/{len(pages)})", value=list_text, inline=False)
        return embed

    # --- INTERACTIVE MESSAGE ---
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
async def stock(ctx, *, member: str):
    """Displays detailed information and a price chart for a given stock."""
    # --- Load Data ---
    stock_prices_df = load_market_file('stock_prices.csv')
    history_df = load_market_file('stock_price_history.csv')
    portfolios_df = load_market_file('portfolios.csv', dtype={'investor_discord_id': str})
    transactions_df = load_market_file('universal_transaction_log.csv', dtype={'actor_id': str})
    init_df = load_market_file('member_initialization.csv')
    ticker_map = pd.Series(init_df.ticker.values, index=init_df.inGameName).to_dict()

    target_name = get_name_from_ticker_or_name(member)
    if not target_name:
        return await ctx.send(f"Could not find a stock for a member or ticker named '{member}'.", ephemeral=True)
    
    stock_info = stock_prices_df[stock_prices_df['inGameName'] == target_name]
    if stock_info.empty:
        return await ctx.send(f"Could not find price data for '{target_name}'. Please run a data refresh.", ephemeral=True)
        
    # --- Calculations ---
    current_price = stock_info['current_price'].iloc[0]
    stock_history = history_df[history_df['inGameName'] == target_name]
    
    price_change, percent_change = get_24hr_change(target_name, history_df, current_price)
    all_time_high = stock_history['price'].max() if not stock_history.empty else current_price
    all_time_low = stock_history['price'].min() if not stock_history.empty else current_price
    
    total_shares = portfolios_df[portfolios_df['stock_inGameName'] == target_name]['shares_owned'].sum()
    market_cap = total_shares * current_price
    
    top_holders = portfolios_df[portfolios_df['stock_inGameName'] == target_name].nlargest(5, 'shares_owned')

    # --- Formatting ---
    display_ticker = ticker_map.get(target_name)
    title = f"Stock Info: {target_name}" + (f" (${display_ticker.upper()})" if pd.notna(display_ticker) and display_ticker != 'None' else "")
    
    embed = discord.Embed(title=title, description=f"*Viewing market data for **{target_name}**.*", color=discord.Color.green())
    
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
    for i, (_, holder) in enumerate(top_holders.iterrows()):
        holder_name = get_inGameName(int(holder['investor_discord_id'])) or "Unknown"
        holders_text += f"{i+1}. {holder_name:<15} ({holder['shares_owned']:.2f} sh)\n"
    holders_text += "```"
    embed.add_field(name="🏆 Top 5 Shareholders", value=holders_text, inline=False)
    
    # Personalized Footer
    user_holding = portfolios_df[(portfolios_df['investor_discord_id'] == str(ctx.author.id)) & (portfolios_df['stock_inGameName'] == target_name)]
    if not user_holding.empty:
        shares_owned = user_holding['shares_owned'].iloc[0]
        cost_basis = get_cost_basis(str(ctx.author.id), target_name, transactions_df)
        pl = (current_price - cost_basis) * shares_owned if cost_basis > 0 else 0
        pl_percent = (pl / (cost_basis * shares_owned)) * 100 if cost_basis > 0 and shares_owned > 0 else 0
        footer_text = f"Your Position: You own {shares_owned:.2f} shares with a P/L of {format_cc(pl)} ({'+' if pl_percent >= 0 else ''}{pl_percent:.1f}%)."
        embed.set_footer(text=footer_text)

    # --- Chart ---
    if len(stock_history) > 1:
        stock_history = stock_history.copy()
        stock_history['timestamp'] = pd.to_datetime(stock_history['timestamp'])
        
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(stock_history['timestamp'], stock_history['price'], color='#00FF00', linewidth=2)
        
        ax.set_title(f'{target_name} Price History', color='white')
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
async def invest(ctx, *, member_and_amount: str):
    """Invest a specific amount of CC into a member's stock."""
    # --- Input Parsing to handle spaces in names ---
    parts = member_and_amount.rsplit(' ', 1)
    if len(parts) != 2:
        await ctx.send("Invalid format. Please use `/invest [member name] [amount]`. Example: `/invest Dill Dough 1000`", ephemeral=True)
        return
    
    member, amount_str = parts

    # --- Input Validation ---
    try:
        if amount_str.lower() == 'all':
            cc_amount = -1 
        else:
            cc_amount = int(amount_str)
        if cc_amount <= 0 and amount_str.lower() != 'all':
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
        crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
        stock_prices_df = load_market_file('stock_prices.csv')
        portfolios_df = load_market_file('portfolios.csv', dtype={'investor_discord_id': str})
        market_state = load_market_file('market_state.csv').set_index('state_name')['value']

        investor_data = crew_coins_df[crew_coins_df['discord_id'] == investor_id]
        if investor_data.empty: return await ctx.send("You do not have a Fan Exchange account yet.", ephemeral=True)

        investor_balance = float(investor_data['balance'].iloc[0])
        if cc_amount == -1: cc_amount = int(investor_balance)
        if cc_amount <= 0: return await ctx.send("You have no Crew Coins to invest.", ephemeral=True)
        if investor_balance < cc_amount: return await ctx.send(f"You don't have enough. Your balance is {format_cc(investor_balance)}.", ephemeral=True)

        target_name = get_name_from_ticker_or_name(member)
        if not target_name: return await ctx.send(f"Could not find a stock for '{member}'.", ephemeral=True)
        
        target_stock = stock_prices_df[stock_prices_df['inGameName'] == target_name]
        current_price = float(target_stock['current_price'].iloc[0])

        broker_fee_rate = 0.03
        if str(market_state.get('active_event')) == "Stewards' Tax Holiday":
            broker_fee_rate = 0.005
        
        broker_fee = cc_amount * broker_fee_rate
        shares_purchased = (cc_amount - broker_fee) / current_price
        
        # Calculate the new balance before updating the DataFrame
        new_balance = investor_balance - cc_amount
        crew_coins_df.loc[crew_coins_df['discord_id'] == investor_id, 'balance'] = new_balance

        existing_holding = portfolios_df[(portfolios_df['investor_discord_id'] == investor_id) & (portfolios_df['stock_inGameName'] == target_name)]
        if not existing_holding.empty:
            portfolios_df.loc[existing_holding.index, 'shares_owned'] += shares_purchased
        else:
            new_row = pd.DataFrame([{'investor_discord_id': investor_id, 'stock_inGameName': target_name, 'shares_owned': shares_purchased}])
            portfolios_df = pd.concat([portfolios_df, new_row], ignore_index=True)

        crew_coins_df.to_csv('market/crew_coins.csv', index=False)
        portfolios_df.to_csv('market/portfolios.csv', index=False)
        
        target_id_row = crew_coins_df[crew_coins_df['inGameName'] == target_name]
        target_id = target_id_row['discord_id'].iloc[0] if not target_id_row.empty else 'N/A'
        log_market_transaction(actor_id=investor_id, transaction_type='INVEST', target_id=target_id, item_name=f"{target_name}'s Stock", item_quantity=shares_purchased, cc_amount=-cc_amount, fee_paid=broker_fee)

        embed = discord.Embed(title="✅ Investment Successful", color=discord.Color.green())
        embed.description = (
            f"You invested **{format_cc(cc_amount)}** into **{target_name}**.\n"
            f"After a {broker_fee_rate:.1%} Broker's Fee ({format_cc(broker_fee)}), you purchased **{shares_purchased:,.2f} shares**."
        )
        # Add the new balance to the footer
        embed.set_footer(text=f"Your new balance is {format_cc(new_balance)}")
        await ctx.send(embed=embed)

    finally:
        os.remove(lock_file)


@bot.command(name="sell")
async def sell(ctx, *, member_and_shares: str):
    """Sell a specific number of shares you own."""
    # --- Input Parsing to handle spaces in names ---
    parts = member_and_shares.rsplit(' ', 1)
    if len(parts) != 2:
        await ctx.send("Invalid format. Please use `/sell [member name] [shares]`. Example: `/sell Dill Dough 50`", ephemeral=True)
        return
    
    member, shares_to_sell_str = parts
    
    # --- Input Validation ---
    try:
        if shares_to_sell_str.lower() == 'all':
            shares_to_sell = -1 
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
        crew_coins_df = load_market_file('crew_coins.csv', dtype={'discord_id': str})
        stock_prices_df = load_market_file('stock_prices.csv')
        portfolios_df = load_market_file('portfolios.csv', dtype={'investor_discord_id': str})
        market_state = load_market_file('market_state.csv').set_index('state_name')['value']

        target_name = get_name_from_ticker_or_name(member)
        if not target_name: return await ctx.send(f"Could not find a stock for '{member}'.", ephemeral=True)
        
        target_stock = stock_prices_df[stock_prices_df['inGameName'] == target_name]
        current_price = float(target_stock['current_price'].iloc[0])

        holding_index = portfolios_df[(portfolios_df['investor_discord_id'] == seller_id) & (portfolios_df['stock_inGameName'] == target_name)].index
        if holding_index.empty: return await ctx.send(f"You do not own any shares of **{target_name}**.", ephemeral=True)
        
        shares_owned = float(portfolios_df.loc[holding_index, 'shares_owned'].iloc[0])
        if shares_to_sell == -1: shares_to_sell = shares_owned
        if shares_owned < shares_to_sell:
            return await ctx.send(f"You don't have enough shares. You only own **{shares_owned:,.2f}**.", ephemeral=True)

        broker_fee_rate = 0.03
        if str(market_state.get('active_event')) == "Stewards' Tax Holiday":
            broker_fee_rate = 0.005

        gross_value = shares_to_sell * current_price
        broker_fee = gross_value * broker_fee_rate
        net_proceeds = gross_value - broker_fee
        
        crew_coins_df.loc[crew_coins_df['discord_id'] == seller_id, 'balance'] += net_proceeds
        portfolios_df.loc[holding_index, 'shares_owned'] -= shares_to_sell
        if portfolios_df.loc[holding_index, 'shares_owned'].iloc[0] < 0.001:
            portfolios_df.drop(holding_index, inplace=True)
            
        crew_coins_df.to_csv('market/crew_coins.csv', index=False)
        portfolios_df.to_csv('market/portfolios.csv', index=False)
        
        target_id_row = crew_coins_df[crew_coins_df['inGameName'] == target_name]
        target_id = target_id_row['discord_id'].iloc[0] if not target_id_row.empty else 'N/A'
        
        log_market_transaction(actor_id=seller_id, transaction_type='SELL', target_id=target_id, item_name=f"{target_name}'s Stock", item_quantity=-shares_to_sell, cc_amount=net_proceeds, fee_paid=broker_fee)
        
        embed = discord.Embed(title="✅ Sale Successful", color=discord.Color.red())
        embed.description = f"You sold **{shares_to_sell:,.2f} shares** of **{target_name}** for a gross value of {format_cc(gross_value)}.\nAfter a {broker_fee_rate:.1%} Broker's Fee ({format_cc(broker_fee)}), you received **{format_cc(net_proceeds)}**."
        await ctx.send(embed=embed)
    finally:
        os.remove(lock_file)

# --- Run the Bot ---
load_dotenv() # Loads variables from .env file
TOKEN = os.getenv('DISCORD_TOKEN')

if TOKEN is None:
    print("ERROR: DISCORD_TOKEN not found in .env file.")
else:
    bot.run(TOKEN)