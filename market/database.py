# market/database.py
import psycopg2
from psycopg2 import extras
import logging
import os
from dotenv import load_dotenv
import json
import pandas as pd
from datetime import datetime

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
        """
        CREATE TABLE IF NOT EXISTS central_bank (
            bank_id SERIAL PRIMARY KEY,
            balance NUMERIC(20, 2) NOT NULL DEFAULT 0.00
        );
        """,        
        # --- NEW: A simple ledger just for fees ---
        """
        CREATE TABLE IF NOT EXISTS fee_ledger (
            fee_id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            transaction_type VARCHAR(50),
            fee_amount NUMERIC(15, 2) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS event_leaderboard_snapshots (
            discord_id VARCHAR(255) PRIMARY KEY,
            ingamename VARCHAR(255),
            start_balance NUMERIC(15, 2),
            start_fan_count BIGINT,
            start_stock_value NUMERIC(15, 2)
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

def get_discord_id_by_name(ingamename: str) -> str:
    """Fetches a user's discord_id by their in-game name."""
    conn = get_connection()
    if not conn: return None
    with conn.cursor() as cursor:
        cursor.execute("SELECT discord_id FROM balances WHERE ingamename = %s;", (ingamename,))
        result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def get_discord_id_to_ingamename_map() -> dict:
    """
    Fetches all users from the balances table and returns a dictionary
    mapping their discord_id to their ingamename.
    """
    conn = get_connection()
    if not conn:
        return {}
    
    id_map = {}
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        try:
            cursor.execute("SELECT discord_id, ingamename FROM balances;")
            results = cursor.fetchall()
            # The dictionary comprehension is a clean way to build the map
            id_map = {str(row['discord_id']): row['ingamename'] for row in results}
        except Exception as e:
            logging.error(f"Error fetching discord_id to ingamename map: {e}")
    
    conn.close()
    return id_map

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

# market/database.py

# (Add this function at the end of the file, after all other functions)

def execute_backfill(final_balances_df, historical_transactions):
    """
    Executes the data backfill as a single, atomic transaction.
    This is a special function just for the backfill script. It updates
    final balances and inserts all historical earnings records.
    """
    conn = get_connection()
    if not conn:
        logging.error("BACKFILL FAILED: Cannot connect to the database.")
        return False
        
    with conn.cursor() as cursor:
        try:
            # 1. Update Balances to their final, correct state using a temp table
            cursor.execute("CREATE TEMP TABLE temp_balances (discord_id VARCHAR(255) PRIMARY KEY, balance NUMERIC(15, 2));")
            balances_tuples = list(final_balances_df[['discord_id', 'balance']].itertuples(index=False, name=None))
            extras.execute_values(cursor, "INSERT INTO temp_balances (discord_id, balance) VALUES %s", balances_tuples)
            cursor.execute("""
                UPDATE balances
                SET balance = temp_balances.balance
                FROM temp_balances
                WHERE balances.discord_id = temp_balances.discord_id;
            """)
            logging.info(f"BACKFILL: Updated {len(balances_tuples)} user balances.")

            # 2. Insert all the historical, missed transaction records
            if historical_transactions:
                # The backfill script provides a list of dictionaries, which we convert to tuples
                transaction_tuples = [
                    (
                        record['timestamp'], record['actor_id'], record['target_id'],
                        record['transaction_type'], record['item_name'], record['item_quantity'],
                        record['cc_amount'], record['fee_paid'], record['details'], record['balance_after']
                    )
                    for record in historical_transactions
                ]
                
                extras.execute_values(
                    cursor,
                    """INSERT INTO transactions 
                       (timestamp, actor_id, target_id, transaction_type, item_name, item_quantity, cc_amount, fee_paid, details, balance_after) 
                       VALUES %s""",
                    transaction_tuples
                )
                logging.info(f"BACKFILL: Inserted {len(historical_transactions)} historical transactions.")

            # If both operations succeed, commit the changes.
            conn.commit()
            logging.info("BACKFILL: Successfully saved all historical data to the database.")
            return True

        except (Exception, psycopg2.Error) as error:
            # If anything fails, roll back all changes.
            logging.error(f"BACKFILL FAILED: Error saving data to DB: {error}")
            conn.rollback()
            return False
        finally:
            if conn is not None:
                conn.close()

### BOT SPECIFIC DATA ACCESS FUNCTIONS ###
def get_user_details(discord_id: str) -> psycopg2.extras.DictRow:
    """
    Fetches a user's core details (balance, in-game name) from the balances table.
    This provides a single, consistent source for verifying if a user has an account.

    Returns:
        A DictRow object containing user details, or None if the user is not found.
    """
    conn = get_connection()
    if not conn: return None
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        cursor.execute("SELECT ingamename, balance FROM balances WHERE discord_id = %s;", (discord_id,))
        result = cursor.fetchone()
    conn.close()
    return result

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


def get_stock_details(identifier: str):
    """
    Fetches all detailed information for a single stock in one efficient query.
    This includes current price, 30-day price history, and top 5 holders.
    """
    conn = get_connection()
    if not conn:
        return None, pd.DataFrame(), pd.DataFrame()

    query = """
    WITH SelectedStock AS (
        SELECT * FROM stock_prices
        WHERE ticker ILIKE %(identifier)s OR ingamename ILIKE %(identifier)s
        LIMIT 1
    ),
    PriceHistory AS (
        -- --- THIS IS THE FIX ---
        -- The to_char function formats the timestamp into a standard ISO 8601 string.
        -- This removes the microsecond ambiguity and ensures pandas can parse it reliably.
        SELECT to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as timestamp, price
        FROM stock_price_history
        WHERE ingamename = (SELECT ingamename FROM SelectedStock)
          AND timestamp >= NOW() - INTERVAL '30 days'
        ORDER BY timestamp ASC
    ),
    TopHolders AS (
        SELECT
            b.ingamename,
            p.shares_owned
        FROM portfolios p
        JOIN balances b ON p.investor_discord_id = b.discord_id
        WHERE p.stock_ingamename = (SELECT ingamename FROM SelectedStock)
        ORDER BY p.shares_owned DESC
        LIMIT 5
    )
    SELECT
        (SELECT row_to_json(ss) FROM SelectedStock ss) as stock_info,
        (SELECT json_agg(ph) FROM PriceHistory ph) as history,
        (SELECT json_agg(th) FROM TopHolders th) as top_holders;
    """
    
    stock_info = None
    history_df = pd.DataFrame()
    top_holders_df = pd.DataFrame()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, {'identifier': identifier})
            result = cursor.fetchone()
            
            if result and result['stock_info']:
                stock_info = dict(result['stock_info'])
                
                if result['history']:
                    history_df = pd.DataFrame(result['history'])
                    
                    # --- THIS IS THE FIX ---
                    # We ensure the conversion is applied directly and correctly.
                    # The `utc=True` argument makes the resulting datetime objects
                    # timezone-aware, which is what's needed for the comparison.
                    history_df['timestamp'] = pd.to_datetime(history_df['timestamp'], utc=True)

                if result['top_holders']:
                    top_holders_df = pd.DataFrame(result['top_holders'])

    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error fetching stock details for {identifier}: {error}")
    finally:
        if conn is not None:
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

def update_market_state_value(state_name: str, state_value: str):
    """
    Updates a single key-value pair in the market_state table.
    This is more efficient than reading and writing the entire table.
    """
    conn = get_connection()
    if not conn:
        logging.error(f"Cannot update market state for {state_name}, no database connection.")
        return False
    
    with conn.cursor() as cursor:
        try:
            cursor.execute(
                """
                INSERT INTO market_state (state_name, state_value) VALUES (%s, %s)
                ON CONFLICT (state_name) DO UPDATE SET state_value = EXCLUDED.state_value;
                """,
                (state_name, state_value)
            )
            conn.commit()
            logging.info(f"Successfully updated market state: {state_name} = {state_value}")
            return True
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error updating single market state value: {error}")
            conn.rollback()
            return False
        finally:
            if conn is not None:
                conn.close()

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


def execute_trade_transaction(
    actor_id: str,
    target_id: str,
    stock_name: str,
    shares: float,
    price_per_share: float,
    total_cost: float,
    fee: float,
    transaction_type: str
):
    """
    Executes a buy or sell order as a single, atomic transaction.
    This version includes a check to ensure a user's share balance cannot go negative.
    """
    conn = get_connection()
    if not conn: return None

    with conn.cursor() as cursor:
        try:
            # 1. Lock and update CC balance
            cursor.execute("SELECT balance FROM balances WHERE discord_id = %s FOR UPDATE;", (actor_id,))
            wallet = cursor.fetchone()
            if transaction_type == 'INVEST' and (not wallet or wallet[0] < -total_cost):
                conn.rollback()
                return None
            
            cursor.execute("UPDATE balances SET balance = balance + %s WHERE discord_id = %s RETURNING balance;", (total_cost, actor_id))
            new_balance = cursor.fetchone()[0]

            # 2. Update portfolio
            update_portfolio_query = """
                UPDATE portfolios
                SET shares_owned = portfolios.shares_owned + %(shares)s
                WHERE investor_discord_id = %(actor_id)s
                  AND stock_ingamename = %(stock_name)s
                  AND portfolios.shares_owned + %(shares)s >= 0;
            """
            cursor.execute(update_portfolio_query, {
                'shares': shares,
                'actor_id': actor_id,
                'stock_name': stock_name
            })
            
            if cursor.rowcount == 0:
                if shares > 0:
                    cursor.execute("""
                        INSERT INTO portfolios (investor_discord_id, stock_ingamename, shares_owned)
                        VALUES (%s, %s, %s)
                    """, (actor_id, stock_name, shares))
                else:
                    logging.error(f"Trade failed: Insufficient shares for {actor_id} to sell {stock_name}.")
                    conn.rollback()
                    return None

            # 3. Log the user-facing transaction
            details = json.dumps({
                "price_per_share": round(price_per_share, 2),
                "shares_transacted": round(shares, 4),
                "subtotal": round(abs(total_cost) - fee, 2),
                "fee_paid": round(fee, 2)
            })
            cursor.execute("""
                INSERT INTO transactions (actor_id, target_id, transaction_type, item_name, item_quantity, cc_amount, fee_paid, details, balance_after)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (actor_id, target_id, transaction_type, f"{stock_name}'s Stock", shares, total_cost, fee, details, new_balance))

            if fee > 0:
                # Add the fee to the house_wallet's balance
                cursor.execute("UPDATE house_wallet SET balance = balance + %s WHERE id = 1;", (fee,))

                # Log the fee collection in the new house_ledger
                cursor.execute(
                    "INSERT INTO house_ledger (transaction_type, net_change, player_id) VALUES (%s, %s, %s);",
                    (transaction_type, fee, actor_id)
                )

            conn.commit()
            return new_balance
        except Exception as e:
            logging.error(f"Trade transaction failed: {e}")
            conn.rollback()
            return None
    conn.close()

def get_trades_without_details():
    """
    Fetches all INVEST and SELL transactions that are missing the details JSON.
    This is used by the backfill script to identify which records to process.
    """
    conn = get_connection()
    if not conn: return pd.DataFrame()
    query = """
        SELECT transaction_id, timestamp, item_name, item_quantity, cc_amount, fee_paid
        FROM transactions
        WHERE transaction_type IN ('INVEST', 'SELL') AND details IS NULL
        ORDER BY timestamp ASC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_historical_price(ingamename: str, timestamp: datetime):
    """
    Finds the price of a stock at a specific point in history.
    It looks for the most recent price record at or before the given timestamp.
    """
    conn = get_connection()
    if not conn: return None
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT price FROM stock_price_history
            WHERE ingamename = %s AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1;
            """,
            (ingamename, timestamp)
        )
        result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def execute_trade_details_backfill(updates: list):
    """
    Executes the backfill for trade details.
    Takes a list of tuples (details_json, target_id, transaction_id)
    and updates the corresponding rows in the transactions table.
    """
    conn = get_connection()
    if not conn:
        logging.error("BACKFILL FAILED: Cannot connect to the database.")
        return False
    
    with conn.cursor() as cursor:
        try:
            # extras.execute_batch is highly efficient for running many UPDATEs.
            extras.execute_batch(
                cursor,
                """
                UPDATE transactions
                SET details = %s, target_id = %s
                WHERE transaction_id = %s;
                """,
                updates
            )
            conn.commit()
            logging.info(f"BACKFILL: Successfully updated details for {len(updates)} trade transactions.")
            return True
        except (Exception, psycopg2.Error) as error:
            logging.error(f"BACKFILL FAILED: Error updating trade details: {error}")
            conn.rollback()
            return False
        finally:
            if conn is not None:
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

def remove_shop_upgrade(discord_id: str, upgrade_name: str) -> bool:
    """
    Removes a specific shop upgrade from a user.
    Returns True on success, False on failure.
    """
    conn = get_connection()
    if not conn: return False

    success = False
    with conn.cursor() as cursor:
        try:
            cursor.execute(
                "DELETE FROM shop_upgrades WHERE discord_id = %s AND upgrade_name = %s;",
                (discord_id, upgrade_name)
            )
            # cursor.rowcount will be > 0 if a row was successfully deleted
            success = cursor.rowcount > 0
            conn.commit()
            if success:
                logging.info(f"Successfully removed upgrade '{upgrade_name}' for user {discord_id}.")
            else:
                logging.warning(f"Attempted to remove upgrade '{upgrade_name}' for user {discord_id}, but no such upgrade was found.")
        except Exception as e:
            logging.error(f"Error removing shop upgrade: {e}")
            conn.rollback()
    conn.close()
    return success

def get_inGameName_by_discord_id(discord_id: str) -> str | None:
    """Fetches a user's in-game name using their Discord ID."""
    conn = get_connection()
    if not conn:
        return None
    
    in_game_name = None
    with conn.cursor() as cursor:
        try:
            cursor.execute("SELECT ingamename FROM balances WHERE discord_id = %s;", (discord_id,))
            result = cursor.fetchone()
            if result:
                in_game_name = result[0]
        except Exception as e:
            logging.error(f"Error fetching in-game name for discord_id {discord_id}: {e}")
    
    conn.close()
    return in_game_name

def get_user_details_by_identifier(identifier: str) -> dict | None:
    """Finds a user's details by their in-game name or ticker."""
    conn = get_connection()
    if not conn: return None

    # Use a dictionary cursor to get column names
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # First, try to find by in-game name in the 'balances' table
    cursor.execute("SELECT discord_id, ingamename FROM balances WHERE ingamename = %s", (identifier,))
    user = cursor.fetchone()
    
    # If not found, try to find by ticker in the 'stock_prices' table
    if not user:
        cursor.execute("SELECT ingamename FROM stock_prices WHERE ticker ILIKE %s", (identifier,))
        stock = cursor.fetchone()
        # If we found a stock, use its name to find the user in the 'balances' table
        if stock:
            cursor.execute("SELECT discord_id, ingamename FROM balances WHERE ingamename = %s", (stock['ingamename'],))
            user = cursor.fetchone()

    cursor.close()
    conn.close()
    
    # Return the user data as a dictionary if found
    return dict(user) if user else None


def execute_admin_award(admin_id: str, target_id: str, amount: int) -> float | None:
    """
    Atomically awards CC to a user and logs the transaction.
    Returns the new balance on success, None on failure.
    """
    conn = get_connection()
    cursor = conn.cursor()
    new_balance = None
    
    try:
        print("--- Starting Admin Award Transaction ---")
        
        # 1. Update the user's balance in the CORRECT table
        update_query = "UPDATE balances SET balance = balance + %s WHERE discord_id = %s"
        cursor.execute(update_query, (amount, target_id))
        print(f"UPDATE balances query affected {cursor.rowcount} row(s).")

        # 2. Log this action in the universal transaction log
        log_query = """
            INSERT INTO transactions 
            (actor_id, transaction_type, target_id, item_name, cc_amount)
            VALUES (%s, %s, %s, %s, %s)
        """
        log_values = (admin_id, 'ADMIN_AWARD', target_id, 'Admin Discretionary Award', amount)
        cursor.execute(log_query, log_values)
        print(f"INSERT transactions query affected {cursor.rowcount} row(s).")

        # 3. Get the new balance from the CORRECT table
        cursor.execute("SELECT balance FROM balances WHERE discord_id = %s", (target_id,))
        result = cursor.fetchone()
        if result:
            new_balance = result[0]
            print(f"Fetched new balance: {new_balance}")

        # 4. Commit changes
        print("Attempting to commit transaction...")
        conn.commit()
        print("✅ Transaction committed successfully.")

    except Exception as err:
        print(f"❌ DATABASE ERROR: {err}")
        print("Rolling back transaction...")
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()
        print("--- Admin Award Transaction Finished ---")

    return new_balance

def execute_admin_removal(admin_id: str, target_id: str, amount: int) -> float | None:
    """
    Atomically removes CC from a user and logs the transaction.
    Returns the new balance on success, None on failure.
    """
    conn = get_connection()
    if not conn: return None
    
    with conn.cursor() as cursor:
        new_balance = None
        try:
            # 1. Lock the row and check if the user has enough balance
            cursor.execute("SELECT balance FROM balances WHERE discord_id = %s FOR UPDATE;", (target_id,))
            wallet = cursor.fetchone()
            if not wallet or wallet[0] < amount:
                # Optional: You might want to allow balances to go negative for corrections.
                # If so, you can remove this check.
                print(f"Admin removal failed: {target_id} has insufficient funds.")
                conn.rollback()
                return None

            # 2. Update the user's balance by subtracting the amount
            cursor.execute("UPDATE balances SET balance = balance - %s WHERE discord_id = %s RETURNING balance;", (amount, target_id))
            new_balance = cursor.fetchone()[0]

            # 3. Log this action in the universal transaction log
            log_query = """
                INSERT INTO transactions 
                (actor_id, transaction_type, target_id, item_name, cc_amount, balance_after)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            # Note: cc_amount is stored as a negative value to reflect the removal
            log_values = (admin_id, 'ADMIN_REMOVAL', target_id, 'Admin Discretionary Removal', -amount, new_balance)
            cursor.execute(log_query, log_values)

            # 4. Commit changes
            conn.commit()
            print("✅ Admin removal transaction committed successfully.")

        except Exception as err:
            print(f"❌ DATABASE ERROR during admin removal: {err}")
            conn.rollback()
            return None
        finally:
            if conn is not None:
                conn.close()

    return new_balance

    
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


def get_earnings_history(discord_id: str, days: int) -> pd.DataFrame:
    """
    Fetches a user's earnings history for a specified number of days.
    This includes standard earnings, dividends, and special cases like admin awards.

    Args:
        discord_id: The user's Discord ID.
        days: The number of past days to retrieve earnings for (e.g., 7 or 30).

    Returns:
        A pandas DataFrame containing the user's earnings history, sorted by
        most recent first. Returns an empty DataFrame if no history is found.
    """
    conn = get_connection()
    if not conn:
        return pd.DataFrame()

    # --- REVISED QUERY ---
    # The WHERE clause is now more complex to handle two different conditions.
    # 1. It selects transactions where the user is the ACTOR for standard income types.
    # 2. It ORs that with a condition where the user is the TARGET for ADMIN_AWARDs.
    # This correctly captures all forms of income for the user.
    query = """
        SELECT
            timestamp,
            transaction_type,
            item_name,
            cc_amount
        FROM transactions
        WHERE (
            -- Condition 1: User is the one performing the action
            (actor_id = %(user_id)s AND transaction_type IN ('PERIODIC_EARNINGS', 'DIVIDEND', 'SELL'))
            OR
            -- Condition 2: User is the one receiving an admin award
            (target_id = %(user_id)s AND transaction_type = 'ADMIN_AWARD')
        )
        AND timestamp >= NOW() - INTERVAL '%(days)s days'
        ORDER BY timestamp DESC;
    """
    df = pd.DataFrame()
    try:
        # We now pass a dictionary of parameters to handle the named placeholders.
        df = pd.read_sql(query, conn, params={'user_id': discord_id, 'days': days})
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error fetching earnings history for {discord_id}: {error}")
    finally:
        if conn is not None:
            conn.close()
    return df

def get_transaction_ledger(discord_id: str) -> pd.DataFrame:
    """
    Retrieves a user's complete transaction history, calculating a running balance.
    This version correctly includes transactions where the user is the target (e.g., admin awards).
    """
    conn = get_connection()
    if not conn:
        return pd.DataFrame()

    # This corrected query finds all transactions relevant to the user and calculates
    # the running balance from their perspective.
    query = """
    WITH RelevantTransactions AS (
        -- Step 1: Gather all transactions where the user is either the actor or the target.
        SELECT
            *,
            -- Step 2: Determine the net effect of each transaction on THIS user's balance.
            CASE
                -- NEW RULE: If this is a dividend I GENERATED for someone else (I am the target),
                -- the effect on my personal balance is zero.
                WHEN transaction_type = 'DIVIDEND' AND target_id = %(user_id)s THEN 0
                
                -- Standard logic for all other cases (buys are negative, sells/earnings are positive)
                WHEN actor_id = %(user_id)s THEN cc_amount 
                
                -- Logic for cases where I am the target (e.g., ADMIN_AWARD)
                WHEN target_id = %(user_id)s THEN cc_amount 
                
                ELSE 0
            END AS balance_effect
        FROM transactions
        WHERE actor_id = %(user_id)s OR target_id = %(user_id)s
    ),
    RunningBalance AS (
        -- Step 3: Calculate the running total of the balance effects over time.
        SELECT
            *,
            SUM(balance_effect) OVER (ORDER BY timestamp ASC, transaction_id ASC) as running_sum
        FROM RelevantTransactions
    )
    -- Step 4: Use the running total and the user's current balance to calculate the historical balance for each transaction.
    SELECT
        rb.transaction_id, rb.timestamp, rb.transaction_type, rb.item_name, rb.item_quantity,
        rb.cc_amount, rb.fee_paid, rb.details,
        (SELECT balance FROM balances WHERE discord_id = %(user_id)s) -- Current Balance
        -
        (SELECT SUM(balance_effect) FROM RunningBalance) -- Total Net Change Ever
        +
        rb.running_sum AS running_balance
    FROM RunningBalance rb
    ORDER BY rb.timestamp DESC;
    """
    df = pd.DataFrame()
    try:
        df = pd.read_sql(query, conn, params={'user_id': discord_id})
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error fetching transaction ledger for {discord_id}: {error}")
    finally:
        if conn is not None:
            conn.close()
    return df


def create_event_snapshot():
    """
    Takes a snapshot of all users' current financial and fan data.
    This function should be called at the start of an event.
    """
    conn = get_connection()
    if not conn:
        logging.error("SNAPSHOT FAILED: Cannot connect to DB.")
        return False
    
    # This query calculates each user's current stock value and joins it
    # with their balance and latest fan count to create a complete snapshot.
    query = """
    WITH CurrentStockValues AS (
        SELECT
            p.investor_discord_id,
            SUM(p.shares_owned * s.current_price) AS current_stock_value
        FROM portfolios p
        JOIN stock_prices s ON p.stock_ingamename = s.ingamename
        GROUP BY p.investor_discord_id
    )
    SELECT
        b.discord_id,
        b.ingamename,
        b.balance,
        COALESCE(csv.current_stock_value, 0) AS stock_value
    FROM balances b
    LEFT JOIN CurrentStockValues csv ON b.discord_id = csv.investor_discord_id;
    """
    
    try:
        with conn.cursor() as cursor:
            # 1. Clear any old snapshot data
            cursor.execute("TRUNCATE TABLE event_leaderboard_snapshots;")
            logging.info("Cleared previous event leaderboard snapshots.")

            # 2. Fetch the combined snapshot data
            snapshot_df = pd.read_sql(query, conn)
            
            # 3. Get the latest fan count for each user from the CSV
            # (This assumes fan data is not yet in the database)
            fan_log_df = pd.read_csv('enriched_fan_log.csv')
            fan_log_df['timestamp'] = pd.to_datetime(fan_log_df['timestamp'])
            latest_fans = fan_log_df.loc[fan_log_df.groupby('inGameName')['timestamp'].idxmax()]
            latest_fans = latest_fans[['inGameName', 'fanCount']]

            # 4. Merge fan data into the main snapshot
            snapshot_df = pd.merge(snapshot_df, latest_fans, left_on='ingamename', right_on='inGameName', how='left').fillna(0)

            # 5. Insert the new snapshot data into the database
            snapshot_tuples = [
                (row['discord_id'], row['ingamename'], row['balance'], row['fanCount'], row['stock_value'])
                for _, row in snapshot_df.iterrows()
            ]
            
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO event_leaderboard_snapshots 
                (discord_id, ingamename, start_balance, start_fan_count, start_stock_value) 
                VALUES %s;
                """,
                snapshot_tuples
            )
            
            conn.commit()
            logging.info(f"Successfully created event snapshot for {len(snapshot_tuples)} users.")
            return True

    except (Exception, psycopg2.Error) as error:
        logging.error(f"DATABASE ERROR during snapshot creation: {error}")
        conn.rollback()
        return False
    finally:
        if conn is not None:
            conn.close()

def get_event_leaderboard_data():
    """
    Calculates the live event leaderboard by comparing current stats
    to the stored snapshot. Returns a pandas DataFrame.
    """
    conn = get_connection()
    if not conn: return pd.DataFrame()

    # This comprehensive query joins current data with the snapshot and calculates the deltas.
    query = """
    WITH CurrentStockValues AS (
        SELECT
            p.investor_discord_id,
            SUM(p.shares_owned * s.current_price) AS current_stock_value
        FROM portfolios p
        JOIN stock_prices s ON p.stock_ingamename = s.ingamename
        GROUP BY p.investor_discord_id
    ),
    CurrentData AS (
        SELECT
            b.discord_id,
            b.balance AS current_balance,
            COALESCE(csv.current_stock_value, 0) AS current_stock_value
        FROM balances b
        LEFT JOIN CurrentStockValues csv ON b.discord_id = csv.investor_discord_id
    )
    SELECT
        s.ingamename,
        -- CC Gained (Earnings + Stock Profit)
        (c.current_balance - s.start_balance) + (c.current_stock_value - s.start_stock_value) AS cc_gained,
        -- Stock Profit Only
        (c.current_stock_value - s.start_stock_value) AS stock_profit,
        s.start_fan_count -- We will calculate fan gain and performance yield in Python
    FROM event_leaderboard_snapshots s
    JOIN CurrentData c ON s.discord_id = c.discord_id;
    """
    try:
        leaderboard_df = pd.read_sql(query, conn)
        
        # --- Python-side Calculations for CSV-based data ---
        fan_log_df = pd.read_csv('enriched_fan_log.csv')
        fan_log_df['timestamp'] = pd.to_datetime(fan_log_df['timestamp'])
        latest_fans = fan_log_df.loc[fan_log_df.groupby('inGameName')['timestamp'].idxmax()]
        
        # Merge to get current fan count and calculate gain
        leaderboard_df = pd.merge(leaderboard_df, latest_fans[['inGameName', 'fanCount']], left_on='ingamename', right_on='inGameName', how='left')
        leaderboard_df['fans_gained'] = leaderboard_df['fanCount'] - leaderboard_df['start_fan_count']

        # Calculate performance yield during the event
        # NOTE: This is an estimation. For exact values, you would need to sum transaction details.
        leaderboard_df['performance_yield'] = (leaderboard_df['fans_gained'] / 8333) * 1.75 * 5.0 # Base formula with x5 multiplier
        
        # Final cleanup and sorting
        leaderboard_df = leaderboard_df[['ingamename', 'cc_gained', 'fans_gained', 'performance_yield', 'stock_profit']]
        leaderboard_df.sort_values('cc_gained', ascending=False, inplace=True)

    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error fetching event leaderboard data: {error}")
        return pd.DataFrame()
    finally:
        if conn is not None:
            conn.close()
            
    return leaderboard_df

# --- Functions for Prestige Purchase Ledger ---

def log_prestige_purchase(actor_id: str, amount: float):
    """Logs a new prestige purchase to the ledger table."""
    conn = get_connection()
    if not conn: return False
    with conn.cursor() as cursor:
        try:
            cursor.execute(
                """INSERT INTO purchased_prestige_ledger (discord_id, prestige_amount)
                   VALUES (%s, %s);""",
                (actor_id, amount)
            )
            conn.commit()
            logging.info(f"Successfully logged prestige purchase for {actor_id} of {amount}.")
            return True
        except Exception as e:
            logging.error(f"Failed to log prestige purchase: {e}")
            conn.rollback()
            return False
    conn.close()

def get_unapplied_prestige_purchases() -> pd.DataFrame:
    """Fetches all prestige purchases that have not yet been applied."""
    conn = get_connection()
    if not conn: return pd.DataFrame()
    query = "SELECT purchase_id, discord_id, prestige_amount FROM purchased_prestige_ledger WHERE is_applied = FALSE;"
    try:
        df = pd.read_sql(query, conn)
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error fetching unapplied prestige: {error}")
        df = pd.DataFrame()
    finally:
        if conn is not None:
            conn.close()
    return df

def flag_prestige_purchases_as_applied(purchase_ids: list):
    """Marks a list of prestige purchase IDs as applied."""
    if not purchase_ids:
        return True # Nothing to do
    conn = get_connection()
    if not conn: return False
    with conn.cursor() as cursor:
        try:
            # Use tuple to make it compatible with SQL IN clause
            extras.execute_values(
                cursor,
                "UPDATE purchased_prestige_ledger SET is_applied = TRUE WHERE purchase_id IN %s;",
                [(pid,) for pid in purchase_ids]
            )
            conn.commit()
            logging.info(f"Flagged {len(purchase_ids)} prestige purchases as applied.")
            return True
        except Exception as e:
            logging.error(f"Failed to flag prestige purchases: {e}")
            conn.rollback()
            return False
    conn.close()

def get_house_balance() -> float:
    """Fetches the current balance of the house wallet."""
    conn = get_connection()
    if not conn:
        return 0.0
    
    balance = 0.0
    with conn.cursor() as cursor:
        try:
            cursor.execute("SELECT balance FROM house_wallet WHERE id = 1;")
            result = cursor.fetchone()
            if result:
                balance = float(result[0])
        except Exception as e:
            logging.error(f"Error fetching house balance: {e}")
    
    conn.close()
    return balance

def execute_gambling_transaction(
    actor_id: str,
    game_name: str,
    bet_amount: float,
    winnings: float,
    details: dict
) -> float | None:
    """
    Executes a gambling win or loss as a single, atomic transaction against the house_wallet.
    Returns the new balance on success, None on failure.
    """
    conn = get_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cursor:
            # 1. Lock the house and player wallets to prevent race conditions.
            cursor.execute("SELECT balance FROM house_wallet WHERE id = 1 FOR UPDATE;")
            house_wallet = cursor.fetchone()
            cursor.execute("SELECT balance FROM balances WHERE discord_id = %s FOR UPDATE;", (actor_id,))
            player_wallet = cursor.fetchone()

            # 2. Final verification checks
            if not player_wallet or player_wallet[0] < bet_amount:
                logging.warning(f"Gambling failed for {actor_id}: insufficient funds at time of execution.")
                conn.rollback()
                return None
            # Check if the house can afford the *net payout*, not the gross winnings.
            net_payout = winnings - bet_amount
            if not house_wallet or house_wallet[0] < net_payout:
                logging.warning(f"Gambling failed for {actor_id}: House has insufficient funds to pay out {winnings}.")
                conn.rollback()
                return None

            # 3. Calculate net changes and update balances
            player_net_change = winnings - bet_amount
            house_net_change = bet_amount - winnings

            cursor.execute(
                "UPDATE balances SET balance = balance + %s WHERE discord_id = %s RETURNING balance;",
                (player_net_change, actor_id)
            )
            new_balance = cursor.fetchone()[0]
            
            cursor.execute(
                "UPDATE house_wallet SET balance = balance + %s WHERE id = 1;",
                (house_net_change,)
            )

            # 4. Log the transaction in the main player ledger
            cursor.execute(
                """
                INSERT INTO transactions (actor_id, transaction_type, item_name, cc_amount, details, balance_after)
                VALUES (%s, 'GAMBLE', %s, %s, %s, %s);
                """,
                (actor_id, game_name, player_net_change, json.dumps(details), new_balance)
            )
            
            # 5. Log the transaction in the house ledger for auditing
            cursor.execute(
                """
                INSERT INTO house_ledger (transaction_type, game_name, player_id, player_bet, player_winnings, net_change)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                ('GAMBLE', game_name, actor_id, bet_amount, winnings, house_net_change)
            )

            conn.commit()
            return new_balance

    except Exception as e:
        logging.error(f"Gambling transaction failed: {e}")
        conn.rollback()
        return None
    finally:
        # Corrected syntax for the finally block
        if conn is not None:
            conn.close()


def get_house_balance() -> float:
    """Fetches the current balance of the house wallet."""
    conn = get_connection()
    if not conn:
        return 0.0
    
    balance = 0.0
    with conn.cursor() as cursor:
        try:
            cursor.execute("SELECT balance FROM house_wallet WHERE id = 1;")
            result = cursor.fetchone()
            if result:
                balance = float(result[0])
        except Exception as e:
            logging.error(f"Error fetching house balance: {e}")
    
    conn.close()
    return balance

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