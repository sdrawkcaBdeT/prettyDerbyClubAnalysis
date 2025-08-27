# market/database.py
import psycopg2
from psycopg2 import extras
import logging
import os
from dotenv import load_dotenv
import json
import pandas as pd

# Load credentials from .env file for security
load_dotenv()
PG_HOST = os.getenv("DB_HOST")
PG_PORT = os.getenv("DB_PORT", 5432)
PG_USER = os.getenv("DB_USER")
PG_PASSWORD = os.getenv("DB_PASSWORD")
PG_DATABASE = os.getenv("DB_NAME")

logging.basicConfig(level=logging.INFO)

def get_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None

def initialize_database():
    """Creates all necessary tables if they don't already exist."""
    conn = get_connection()
    if not conn: return

    # Use a single transaction to create all tables
    with conn.cursor() as cursor:
        try:
            # Note: We use DECIMAL for currency to avoid floating point errors.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crew_coin_wallets (
                    discord_id TEXT PRIMARY KEY,
                    balance DECIMAL(12, 2) NOT NULL DEFAULT 10000.00
                );
                CREATE TABLE IF NOT EXISTS races (
                    race_id BIGINT PRIMARY KEY,
                    distance INT NOT NULL,
                    status TEXT NOT NULL,
                    winner_name TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
                );
                CREATE TABLE IF NOT EXISTS race_horses (
                    horse_id SERIAL PRIMARY KEY,
                    race_id BIGINT REFERENCES races(race_id),
                    horse_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    stats_json JSONB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bets (
                    bet_id SERIAL PRIMARY KEY,
                    race_id BIGINT REFERENCES races(race_id),
                    bettor_id TEXT NOT NULL,
                    horse_name TEXT NOT NULL,
                    amount DECIMAL(10, 2) NOT NULL,
                    locked_in_odds DECIMAL(10, 2) NOT NULL,
                    placed_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
                );
            """)
            conn.commit()
            logging.info("Database tables created or verified successfully.")
        except psycopg2.Error as e:
            logging.error(f"Error initializing tables: {e}")
            conn.rollback()
    conn.close()

def create_market_tables():
    """Creates and alters all necessary tables for the Fan Exchange market."""
    commands = (
        # (Keep existing CREATE TABLE commands for balances, portfolios, etc.)
        """
        CREATE TABLE IF NOT EXISTS balances (
            discord_id VARCHAR(255) PRIMARY KEY,
            ingamename VARCHAR(255) NOT NULL,
            balance NUMERIC(15, 2) NOT NULL DEFAULT 10000.00
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS stock_prices (
            ingamename VARCHAR(255) PRIMARY KEY,
            current_price NUMERIC(10, 2) NOT NULL DEFAULT 10.00
        );
        """,
        """
        ALTER TABLE stock_prices
        ADD COLUMN IF NOT EXISTS ticker VARCHAR(5) UNIQUE,
        ADD COLUMN IF NOT EXISTS init_factor NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'active',
        ADD COLUMN IF NOT EXISTS nudge_bonus NUMERIC(10, 4) DEFAULT 0.0;
        """,
        # (Keep existing CREATE TABLE commands for portfolios, etc.)
        """
        CREATE TABLE IF NOT EXISTS portfolios (
            portfolio_id SERIAL PRIMARY KEY,
            investor_discord_id VARCHAR(255) NOT NULL,
            stock_ingamename VARCHAR(255) NOT NULL,
            shares_owned NUMERIC(15, 6) NOT NULL,
            CONSTRAINT fk_investor
                FOREIGN KEY(investor_discord_id) 
                REFERENCES balances(discord_id),
            CONSTRAINT fk_stock
                FOREIGN KEY(stock_ingamename) 
                REFERENCES stock_prices(ingamename)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS shop_upgrades (
            upgrade_id SERIAL PRIMARY KEY,
            discord_id VARCHAR(255) NOT NULL,
            upgrade_name VARCHAR(255) NOT NULL,
            tier INTEGER NOT NULL,
            CONSTRAINT fk_user
                FOREIGN KEY(discord_id) 
                REFERENCES balances(discord_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            actor_id VARCHAR(255),
            target_id VARCHAR(255),
            transaction_type VARCHAR(50) NOT NULL,
            item_name VARCHAR(255),
            item_quantity NUMERIC(15, 6),
            cc_amount NUMERIC(15, 2),
            fee_paid NUMERIC(15, 2),
            details JSONB,
            balance_after NUMERIC(15, 2)
        );
        """,
        """
        ALTER TABLE transactions
        ADD COLUMN IF NOT EXISTS item_quantity NUMERIC(15, 6);
        """,
        """
        CREATE TABLE IF NOT EXISTS stock_price_history (
            history_id SERIAL PRIMARY KEY,
            ingamename VARCHAR(255) NOT NULL,
            price NUMERIC(10, 2) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT fk_stock_history_stock
                FOREIGN KEY(ingamename) 
                REFERENCES stock_prices(ingamename)
        );
        """,
        # --- NEW: Create the market_state table ---
        """
        CREATE TABLE IF NOT EXISTS market_state (
            state_name VARCHAR(255) PRIMARY KEY,
            state_value TEXT
        );
        """
    )
    conn = None
    try:
        conn = get_connection()
        if conn:
            cur = conn.cursor()
            for command in commands:
                cur.execute(command)
            cur.close()
            conn.commit()
            print("Market tables created or altered successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error during table creation: {error}")
    finally:
        if conn is not None:
            conn.close()

def get_market_data_from_db():
    """
    Fetches all core market tables from the database and returns them as a
    dictionary of pandas DataFrames.
    """
    conn = get_connection()
    if not conn:
        logging.error("Cannot fetch market data, no database connection.")
        return None
    
    try:
        with conn:
            balances_df = pd.read_sql("SELECT * FROM balances", conn)
            stock_prices_df = pd.read_sql("SELECT * FROM stock_prices", conn)
            portfolios_df = pd.read_sql("SELECT * FROM portfolios", conn)
            shop_upgrades_df = pd.read_sql("SELECT * FROM shop_upgrades", conn)
            market_state_df = pd.read_sql("SELECT * FROM market_state", conn)
            
            # Clean up potential NULL values from the database that pandas reads as NaN
            market_state_df['state_value'] = market_state_df['state_value'].replace("NaN", "Test")

            # Rename columns to match expected DataFrame format
            balances_df.rename(columns={'ingamename': 'inGameName'}, inplace=True)
            stock_prices_df.rename(columns={'ingamename': 'inGameName'}, inplace=True)
            portfolios_df.rename(columns={'stock_ingamename': 'stock_inGameName'}, inplace=True)
            
        logging.info("Successfully fetched all market data from the database.")
        
        return {
            'crew_coins': balances_df,
            'stock_prices': stock_prices_df,
            'portfolios': portfolios_df,
            'shop_upgrades': shop_upgrades_df,
            'market_state': market_state_df,
        }
        
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error fetching market data from DB: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()

def save_market_state_to_db(market_state_df):
    """Saves the market_state DataFrame to the database."""
    conn = get_connection()
    if not conn:
        logging.error("Cannot save market state, no database connection.")
        return False
        
    with conn.cursor() as cursor:
        try:
            state_tuples = [tuple(x) for x in market_state_df.to_numpy()]
            extras.execute_values(
                cursor,
                """
                INSERT INTO market_state (state_name, state_value) VALUES %s
                ON CONFLICT (state_name) DO UPDATE SET state_value = EXCLUDED.state_value;
                """,
                state_tuples
            )
            conn.commit()
            logging.info("Successfully saved market state to the database.")
            return True
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error saving market state to DB: {error}")
            conn.rollback()
            return False
        finally:
            if conn is not None:
                conn.close()

def save_all_market_data_to_db(balances_df, stock_prices_df, new_transactions):
    """
    Saves all updated market data to the database in a single transaction.
    - Updates balances
    - Updates stock prices (including nudge_bonus)
    - Inserts new, detailed periodic earnings and dividend transactions
    """
    conn = get_connection()
    if not conn:
        logging.error("Cannot save market data, no database connection.")
        return False
        
    with conn.cursor() as cursor:
        try:
            # 1. Update Balances
            cursor.execute("CREATE TEMP TABLE temp_balances (discord_id VARCHAR(255) PRIMARY KEY, balance NUMERIC(15, 2));")
            balances_tuples = list(balances_df[['discord_id', 'balance']].itertuples(index=False, name=None))
            extras.execute_values(cursor, "INSERT INTO temp_balances (discord_id, balance) VALUES %s", balances_tuples)
            cursor.execute("""
                UPDATE balances
                SET balance = temp_balances.balance
                FROM temp_balances
                WHERE balances.discord_id = temp_balances.discord_id;
            """)
            logging.info(f"Updated {len(balances_df)} rows in 'balances' table.")

            # 2. Update Stock Prices
            cursor.execute("CREATE TEMP TABLE temp_stock_prices (ingamename VARCHAR(255) PRIMARY KEY, current_price NUMERIC(10, 2), nudge_bonus NUMERIC(10, 4));")
            prices_tuples = list(stock_prices_df[['inGameName', 'current_price', 'nudge_bonus']].itertuples(index=False, name=None))
            extras.execute_values(cursor, "INSERT INTO temp_stock_prices (ingamename, current_price, nudge_bonus) VALUES %s", prices_tuples)
            cursor.execute("""
                UPDATE stock_prices
                SET current_price = temp_stock_prices.current_price,
                    nudge_bonus = temp_stock_prices.nudge_bonus
                FROM temp_stock_prices
                WHERE stock_prices.ingamename = temp_stock_prices.ingamename;
            """)
            logging.info(f"Updated {len(stock_prices_df)} rows in 'stock_prices' table.")

            # --- Convert list of dictionaries to list of tuples ---
            # 3. Insert new transactions
            if new_transactions:
                # The new_transactions object is now a list of dictionaries.
                # We need to convert it to a list of tuples in the correct order.
                transaction_tuples = [
                    (
                        record['timestamp'], record['actor_id'], record['target_id'],
                        record['transaction_type'], record['item_name'], record['item_quantity'],
                        record['cc_amount'], record['fee_paid'], record['details'], record['balance_after']
                    )
                    for record in new_transactions
                ]
                
                extras.execute_values(
                    cursor,
                    """INSERT INTO transactions 
                       (timestamp, actor_id, target_id, transaction_type, item_name, item_quantity, cc_amount, fee_paid, details, balance_after) 
                       VALUES %s""",
                    transaction_tuples
                )
                logging.info(f"Inserted {len(new_transactions)} new transactions.")

            conn.commit()
            logging.info("Successfully saved all market data to the database.")
            return True

        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error saving market data to DB: {error}")
            conn.rollback()
            return False
        finally:
            if conn is not None:
                conn.close()
                
# --- Function to log the price history ---
def log_stock_price_history(stock_prices_df, run_timestamp):
    """Inserts the current stock prices into the history table with a specific timestamp."""
    conn = get_connection()
    if not conn:
        logging.error("Cannot log price history, no database connection.")
        return False
    
    with conn.cursor() as cursor:
        try:
            # --- FIX: Include the run_timestamp in the data to be inserted ---
            history_data = [
                (row['inGameName'], row['current_price'], run_timestamp)
                for _, row in stock_prices_df.iterrows()
            ]
            extras.execute_values(
                cursor,
                "INSERT INTO stock_price_history (ingamename, price, timestamp) VALUES %s",
                history_data
            )
            conn.commit()
            logging.info(f"Successfully logged {len(history_data)} price points to history table.")
            return True
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error logging stock price history: {error}")
            conn.rollback()
            return False
        finally:
            if conn is not None:
                conn.close()

### BOT SPECIFIC DATA ACCESS FUNCTIONS ###
def get_user_balance_by_discord_id(discord_id: str):
    """Fetches a single user's balance from the database."""
    conn = get_connection()
    if not conn: return None
    with conn.cursor() as cursor:
        cursor.execute("SELECT balance FROM balances WHERE discord_id = %s;", (discord_id,))
        result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def get_stock_by_ticker_or_name(identifier: str):
    """
    Fetches all data for a single stock by its ticker or in-game name.
    Returns a dictionary of the stock's data or None if not found.
    """
    conn = get_connection()
    if not conn: return None
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        # Check by ticker first (case-insensitive)
        cursor.execute("SELECT * FROM stock_prices WHERE ticker ILIKE %s;", (identifier,))
        result = cursor.fetchone()
        if not result:
            # If not found, check by in-game name (case-insensitive)
            cursor.execute("SELECT * FROM stock_prices WHERE ingamename ILIKE %s;", (identifier,))
            result = cursor.fetchone()
    conn.close()
    return dict(result) if result else None

def get_portfolio_details(discord_id: str):
    """
    Fetches a user's complete portfolio with calculated cost basis and joins
    with current stock prices to get all data needed for the /portfolio command.
    """
    conn = get_connection()
    if not conn: return pd.DataFrame()
    query = """
    WITH CostBasis AS (
        -- Calculate the total cost and shares purchased for each stock a user has bought
        SELECT
            actor_id,
            item_name,
            SUM(item_quantity) AS total_shares_bought,
            SUM(ABS(cc_amount) - fee_paid) AS total_cost
        FROM transactions
        WHERE transaction_type = 'INVEST' AND actor_id = %(user_id)s
        GROUP BY actor_id, item_name
    )
    -- Main query to join portfolio with prices and calculated cost basis
    SELECT 
        p.stock_ingamename,
        p.shares_owned,
        s.current_price,
        s.ticker,
        -- Calculate average cost per share, avoiding division by zero
        CASE 
            WHEN cb.total_shares_bought > 0 THEN cb.total_cost / cb.total_shares_bought
            ELSE 0 
        END AS cost_basis
    FROM portfolios p
    JOIN stock_prices s ON p.stock_ingamename = s.ingamename
    LEFT JOIN CostBasis cb ON p.investor_discord_id = cb.actor_id AND s.ingamename || '''s Stock' = cb.item_name
    WHERE p.investor_discord_id = %(user_id)s;
    """
    df = pd.read_sql(query, conn, params={'user_id': discord_id})
    conn.close()
    return df

def get_market_snapshot():
    """
    Fetches a comprehensive snapshot of the entire market, including 24h price
    changes, market cap, top holders, and 24h volume.
    """
    conn = get_connection()
    if not conn: return None, None
    
    query = """
    WITH PriceHistory24h AS (
        -- For each stock, find the most recent price from more than 24 hours ago
        SELECT 
            ingamename,
            FIRST_VALUE(price) OVER (PARTITION BY ingamename ORDER BY timestamp DESC) as price_24h_ago
        FROM stock_price_history
        WHERE timestamp < NOW() - INTERVAL '24 hours'
    ),
    LatestPriceHistory AS (
        -- Get only the single most recent 24h price for each stock
        SELECT DISTINCT ingamename, price_24h_ago FROM PriceHistory24h
    ),
    RankedPortfolios AS (
        -- Rank all holders for every stock by shares_owned
        SELECT
            p.stock_ingamename,
            b.ingamename AS holder_name,
            p.shares_owned,
            ROW_NUMBER() OVER(PARTITION BY p.stock_ingamename ORDER BY p.shares_owned DESC) as rn
        FROM portfolios p
        JOIN balances b ON p.investor_discord_id = b.discord_id
    ),
    MarketCaps AS (
        -- Calculate market cap for each stock
        SELECT
            stock_ingamename,
            SUM(shares_owned) as total_shares
        FROM portfolios
        GROUP BY stock_ingamename
    )
    -- Final join to bring it all together
    SELECT
        s.ingamename,
        s.current_price,
        s.ticker,
        COALESCE(lph.price_24h_ago, s.current_price) AS price_24h_ago,
        mc.total_shares * s.current_price AS market_cap,
        rp.holder_name as largest_holder,
        rp.shares_owned as largest_holder_shares
    FROM stock_prices s
    LEFT JOIN LatestPriceHistory lph ON s.ingamename = lph.ingamename
    LEFT JOIN MarketCaps mc ON s.ingamename = mc.stock_ingamename
    LEFT JOIN RankedPortfolios rp ON s.ingamename = rp.stock_ingamename AND rp.rn = 1;
    """
    market_df = pd.read_sql(query, conn)

    # A separate query for 24h volume
    volume_query = """
        SELECT SUM(ABS(cc_amount)) 
        FROM transactions
        WHERE transaction_type IN ('INVEST', 'SELL') 
        AND timestamp >= NOW() - INTERVAL '24 hours';
    """
    with conn.cursor() as cursor:
        cursor.execute(volume_query)
        volume_24h = cursor.fetchone()[0]

    conn.close()
    return market_df, volume_24h or 0


def get_stock_details(ingamename: str):
    """Fetches detailed info for one stock, including history and top 5 holders."""
    conn = get_connection()
    if not conn: return None, pd.DataFrame(), pd.DataFrame()

    # Query 1: Stock Info
    stock_info = get_stock_by_ticker_or_name(ingamename) # Re-use existing function
    
    # Query 2: Price History
    history_df = get_stock_price_history(ingamename, days=30) # Re-use existing function

    # Query 3: Top 5 Holders
    holders_query = """
        SELECT
            b.ingamename,
            p.shares_owned
        FROM portfolios p
        JOIN balances b ON p.investor_discord_id = b.discord_id
        WHERE p.stock_ingamename = %s
        ORDER BY p.shares_owned DESC
        LIMIT 5;
    """
    top_holders_df = pd.read_sql(holders_query, conn, params=(ingamename,))
    
    conn.close()
    return stock_info, history_df, top_holders_df

def get_user_portfolio(discord_id: str):
    """
    Fetches a user's complete stock portfolio, joining with stock_prices
    to get current values. Returns a pandas DataFrame.
    """
    conn = get_connection()
    if not conn: return pd.DataFrame()
    query = """
        SELECT 
            p.stock_ingamename,
            p.shares_owned,
            s.current_price,
            s.ticker
        FROM portfolios p
        JOIN stock_prices s ON p.stock_ingamename = s.ingamename
        WHERE p.investor_discord_id = %s;
    """
    df = pd.read_sql(query, conn, params=(discord_id,))
    conn.close()
    return df

def get_full_market_data():
    """Fetches a joined DataFrame of all stocks and their top holder for the /market command."""
    conn = get_connection()
    if not conn: return pd.DataFrame()
    # This is a more complex query that finds the top holder for each stock
    query = """
        WITH RankedPortfolios AS (
            SELECT
                p.stock_ingamename,
                b.ingamename AS holder_name,
                p.shares_owned,
                ROW_NUMBER() OVER(PARTITION BY p.stock_ingamename ORDER BY p.shares_owned DESC) as rn
            FROM portfolios p
            JOIN balances b ON p.investor_discord_id = b.discord_id
        )
        SELECT 
            s.ingamename,
            s.current_price,
            s.ticker,
            rp.holder_name,
            rp.shares_owned AS holder_shares
        FROM stock_prices s
        LEFT JOIN RankedPortfolios rp ON s.ingamename = rp.stock_ingamename AND rp.rn = 1;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_stock_price_history(ingamename: str, days: int = 30):
    """Fetches the price history for a specific stock for a given number of days."""
    conn = get_connection()
    if not conn: return pd.DataFrame()
    query = """
        SELECT timestamp, price FROM stock_price_history
        WHERE ingamename = %s AND timestamp >= NOW() - INTERVAL '%s days'
        ORDER BY timestamp ASC;
    """
    df = pd.read_sql(query, conn, params=(ingamename, days))
    conn.close()
    return df

def get_sponsorships(discord_id: str):
    """
    Finds all stocks for which the given user is the #1 shareholder.
    Uses a window function to efficiently rank holders and calculate the lead.
    Returns a list of dictionaries with sponsorship details.
    """
    conn = get_connection()
    if not conn: return []
    query = """
    WITH RankedHolders AS (
        -- Rank all holders for every stock by shares_owned
        SELECT
            stock_ingamename,
            investor_discord_id,
            shares_owned,
            ROW_NUMBER() OVER(PARTITION BY stock_ingamename ORDER BY shares_owned DESC) as rn,
            LEAD(shares_owned, 1, 0.0) OVER(PARTITION BY stock_ingamename ORDER BY shares_owned DESC) as second_place_shares
        FROM portfolios
    )
    -- Select only the #1 ranked holders where the investor is our user
    SELECT 
        rh.stock_ingamename,
        s.ticker,
        (rh.shares_owned - rh.second_place_shares) as lead_amount
    FROM RankedHolders rh
    JOIN stock_prices s ON rh.stock_ingamename = s.ingamename
    WHERE rh.investor_discord_id = %s AND rh.rn = 1;
    """
    df = pd.read_sql(query, conn, params=(discord_id,))
    conn.close()
    # Convert the DataFrame to a list of dictionaries for easy use in the bot
    return df.to_dict('records')

def update_user_ticker(ingamename: str, ticker: str):
    """
    Sets or updates a user's stock ticker.
    Returns True on success, False on failure (e.g., ticker already taken).
    """
    conn = get_connection()
    if not conn: return False
    with conn.cursor() as cursor:
        try:
            cursor.execute("UPDATE stock_prices SET ticker = %s WHERE ingamename = %s;", (ticker, ingamename))
            conn.commit()
            success = cursor.rowcount > 0
        except psycopg2.IntegrityError: # This will catch the UNIQUE constraint violation
            conn.rollback()
            success = False
    conn.close()
    return success


def execute_trade_transaction(actor_id: str, stock_name: str, shares: float, total_cost: float, fee: float, transaction_type: str):
    """
    Executes a buy or sell order as a single, atomic transaction.
    Handles balance updates, portfolio changes, and transaction logging.
    - For a BUY: cost is negative (money leaving wallet)
    - For a SELL: cost is positive (money entering wallet)
    Returns the new balance on success, None on failure.
    """
    conn = get_connection()
    if not conn: return None

    with conn.cursor() as cursor:
        try:
            # 1. Lock the user's balance row and update it
            cursor.execute("SELECT balance FROM balances WHERE discord_id = %s FOR UPDATE;", (actor_id,))
            wallet = cursor.fetchone()
            if not wallet or wallet[0] < -total_cost: # Check for sufficient funds on buy
                conn.rollback()
                return None # Insufficient funds
            
            cursor.execute("UPDATE balances SET balance = balance + %s WHERE discord_id = %s RETURNING balance;", (total_cost, actor_id))
            new_balance = cursor.fetchone()[0]

            # 2. Update the portfolio (UPSERT logic)
            # This will insert a new portfolio row or update an existing one
            cursor.execute("""
                INSERT INTO portfolios (investor_discord_id, stock_ingamename, shares_owned)
                VALUES (%s, %s, %s)
                ON CONFLICT (investor_discord_id, stock_ingamename) DO UPDATE
                SET shares_owned = portfolios.shares_owned + EXCLUDED.shares_owned;
            """, (actor_id, stock_name, shares))

            # 3. Log the transaction
            cursor.execute("""
                INSERT INTO transactions (actor_id, transaction_type, item_name, cc_amount, fee_paid, balance_after)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (actor_id, transaction_type, f"{stock_name}'s Stock", total_cost, fee, new_balance))

            conn.commit()
            return new_balance
        except Exception as e:
            logging.error(f"Trade transaction failed: {e}")
            conn.rollback()
            return None
    conn.close()

def get_shop_data(discord_id: str):
    """
    Fetches all data needed for the /shop command for a specific user:
    - Current balance
    - A dictionary of their current upgrade tiers
    - Their most recent lifetime prestige from the enriched_fan_log.csv
    """
    conn = get_connection()
    if not conn: return None, {}, 0

    shop_data = {}
    inGameName = None
    
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        # 1. Get balance and inGameName
        cursor.execute("SELECT balance, ingamename FROM balances WHERE discord_id = %s;", (discord_id,))
        balance_result = cursor.fetchone()
        if balance_result:
            shop_data['balance'] = float(balance_result['balance'])
            inGameName = balance_result['ingamename']
        else:
            shop_data['balance'] = 0

        # 2. Get upgrades
        cursor.execute("SELECT upgrade_name, tier FROM shop_upgrades WHERE discord_id = %s;", (discord_id,))
        upgrades = {row['upgrade_name']: row['tier'] for row in cursor.fetchall()}
        shop_data['upgrades'] = upgrades

    conn.close()

    # --- FIX: Read the CSV to get the real lifetime prestige ---
    prestige = 0
    if inGameName:
        try:
            enriched_df = pd.read_csv('enriched_fan_log.csv')
            user_stats = enriched_df[enriched_df['inGameName'] == inGameName]
            if not user_stats.empty:
                # Find the entry with the most recent timestamp
                latest_stats = user_stats.loc[user_stats['timestamp'].idxmax()]
                prestige = float(latest_stats['lifetimePrestige'])
        except FileNotFoundError:
            logging.error("enriched_fan_log.csv not found. Cannot calculate prestige for shop.")
        except Exception as e:
            logging.error(f"Error reading prestige from CSV: {e}")
            
    shop_data['prestige'] = prestige

    return shop_data

def execute_purchase_transaction(actor_id: str, item_name: str, cost: float, upgrade_tier: int = None):
    """
    Executes a shop purchase as a single, atomic transaction.
    Returns the new balance on success, None on failure.
    """
    conn = get_connection()
    if not conn: return None

    with conn.cursor() as cursor:
        try:
            # 1. Lock and debit balance
            cursor.execute("SELECT balance FROM balances WHERE discord_id = %s FOR UPDATE;", (actor_id,))
            wallet = cursor.fetchone()
            if not wallet or wallet[0] < cost:
                conn.rollback()
                return None # Insufficient funds
            
            cursor.execute("UPDATE balances SET balance = balance - %s WHERE discord_id = %s RETURNING balance;", (cost, actor_id))
            new_balance = cursor.fetchone()[0]

            # 2. If it's an upgrade, update or insert the tier
            if upgrade_tier is not None:
                cursor.execute("""
                    INSERT INTO shop_upgrades (discord_id, upgrade_name, tier)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (discord_id, upgrade_name) DO UPDATE
                    SET tier = EXCLUDED.tier;
                """, (actor_id, item_name, upgrade_tier))

            # 3. Log the transaction
            cursor.execute("""
                INSERT INTO transactions (actor_id, transaction_type, item_name, cc_amount, balance_after)
                VALUES (%s, 'PURCHASE', %s, %s, %s);
            """, (actor_id, item_name, -cost, new_balance))

            conn.commit()
            return new_balance
        except Exception as e:
            logging.error(f"Purchase transaction failed: {e}")
            conn.rollback()
            return None
    conn.close()
    
def get_financial_summary(discord_id: str) -> dict:
    """
    Calculates a user's complete financial summary using a single, efficient query.
    This function uses Common Table Expressions (CTEs) to avoid multiple database
    round-trips, ensuring optimal performance.

    Returns:
        A dictionary containing the calculated KPIs, or a default dictionary
        of zeros if the user has no financial data.
    """
    conn = get_connection()
    if not conn:
        return {
            'net_worth': 0,
            'total_portfolio_value': 0,
            'p_l': 0,
            'roi_percent': 0
        }

    # This query is designed for high efficiency.
    # - CTEs break down the complex calculation into logical, readable steps.
    # - It fetches all necessary data in one go, preventing multiple network latencies.
    # - COALESCE is used extensively to handle cases where a user might have a balance
    #   but no stocks, or vice-versa, preventing NULL-related errors in calculations.
    query = """
    WITH PortfolioValue AS (
        -- First, calculate the current total value of the user's stock holdings.
        SELECT
            p.investor_discord_id,
            COALESCE(SUM(p.shares_owned * s.current_price), 0) AS total_stock_value
        FROM portfolios p
        JOIN stock_prices s ON p.stock_ingamename = s.ingamename
        WHERE p.investor_discord_id = %(user_id)s
        GROUP BY p.investor_discord_id
    ),
    TradeHistory AS (
        -- Next, aggregate the user's buy and sell history from the transactions table.
        SELECT
            actor_id,
            -- Sum of negative cc_amount for 'INVEST' transactions represents total cost.
            -- We use ABS() to get a positive value for total invested.
            COALESCE(SUM(CASE WHEN transaction_type = 'INVEST' THEN ABS(cc_amount) - fee_paid ELSE 0 END), 0) AS total_invested,
            -- Sum of positive cc_amount for 'SELL' transactions represents total returns from sales.
            COALESCE(SUM(CASE WHEN transaction_type = 'SELL' THEN cc_amount - fee_paid ELSE 0 END), 0) AS total_returns_from_sales
        FROM transactions
        WHERE actor_id = %(user_id)s AND transaction_type IN ('INVEST', 'SELL')
        GROUP BY actor_id
    )
    -- Finally, join everything together to calculate the final KPIs.
    SELECT
        b.balance AS cc_balance,
        COALESCE(pv.total_stock_value, 0) AS total_portfolio_value,
        (b.balance + COALESCE(pv.total_stock_value, 0)) AS net_worth,
        COALESCE(th.total_invested, 0) AS total_invested,
        -- Profit/Loss = (Current Value + What I've Sold) - What I've Bought
        (COALESCE(pv.total_stock_value, 0) + COALESCE(th.total_returns_from_sales, 0) - COALESCE(th.total_invested, 0)) AS p_l
    FROM balances b
    LEFT JOIN PortfolioValue pv ON b.discord_id = pv.investor_discord_id
    LEFT JOIN TradeHistory th ON b.discord_id = th.actor_id
    WHERE b.discord_id = %(user_id)s;
    """
    summary = {
        'net_worth': 0,
        'total_portfolio_value': 0,
        'p_l': 0,
        'roi_percent': 0
    }
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, {'user_id': discord_id})
            result = cursor.fetchone()
            if result:
                # The result object is a DictRow, which can be accessed by key.
                # We calculate ROI here in Python to avoid a division-by-zero error in SQL
                # if the user has never invested.
                total_invested = result['total_invested']
                p_l = result['p_l']
                
                roi_percent = (p_l / total_invested) * 100 if total_invested > 0 else 0

                summary['net_worth'] = float(result['net_worth'])
                summary['total_portfolio_value'] = float(result['total_portfolio_value'])
                summary['p_l'] = float(p_l)
                summary['roi_percent'] = roi_percent

    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error fetching financial summary for {discord_id}: {error}")
    finally:
        if conn is not None:
            conn.close()
            
    return summary


### Racing Gambling Game Functions ###

def create_race(race_id, distance, horses):
    """
    Logs a new race and its participating horses to the database.
    """
    conn = get_connection()
    if not conn: return
    with conn.cursor() as cursor:
        try:
            # Insert the main race record
            cursor.execute(
                "INSERT INTO races (race_id, distance, status) VALUES (%s, %s, %s);",
                (race_id, distance, 'betting')
            )
            # Insert all the horse records
            horse_data = [
                (race_id, h.name, h.strategy_name, json.dumps(h.stats)) for h in horses
            ]
            extras.execute_values(
                cursor,
                "INSERT INTO race_horses (race_id, horse_name, strategy, stats_json) VALUES %s;",
                horse_data
            )
            conn.commit()
            logging.info(f"Successfully created race #{race_id} in the database.")
        except psycopg2.Error as e:
            logging.error(f"Error creating race: {e}")
            conn.rollback()
    conn.close()

def place_bet_transaction(race_id, bettor_id, horse_name, amount, odds) -> bool:
    """
    Executes a bet as a single, atomic transaction.
    1. Checks and debits the user's balance.
    2. Inserts the bet record.
    Returns True on success, False on failure.
    """
    conn = get_connection()
    if not conn: return False

    with conn.cursor() as cursor:
        try:
            # 1. Check and debit wallet balance
            # The FOR UPDATE locks the row to prevent race conditions.
            cursor.execute(
                "SELECT balance FROM crew_coin_wallets WHERE discord_id = %s FOR UPDATE;",
                (bettor_id,)
            )
            wallet = cursor.fetchone()
            if not wallet or wallet[0] < amount:
                logging.warning(f"Bet failed: Insufficient funds for {bettor_id}.")
                conn.rollback()
                return False
            
            cursor.execute(
                "UPDATE crew_coin_wallets SET balance = balance - %s WHERE discord_id = %s;",
                (amount, bettor_id)
            )

            # 2. Insert the bet record
            cursor.execute(
                """INSERT INTO bets (race_id, bettor_id, horse_name, amount, locked_in_odds)
                   VALUES (%s, %s, %s, %s, %s);""",
                (race_id, bettor_id, horse_name, amount, odds)
            )
            
            # If all steps succeed, commit the transaction
            conn.commit()
            logging.info(f"Bet successfully placed for {bettor_id}.")
            return True
        except psycopg2.Error as e:
            logging.error(f"Transaction failed: {e}")
            conn.rollback()
            return False
    conn.close()

# We can add more functions here later as needed (e.g., for payouts, creating races, etc.)

if __name__ == "__main__":
    """
    This block will only run when the script is executed directly.
    It's used here for one-time database setup.
    """
    print("Attempting to initialize the database...")
    initialize_database()
    create_market_tables()
    print("Initialization complete.")