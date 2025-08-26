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
    """Creates the necessary tables for the Fan Exchange market if they don't exist."""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS balances (
            discord_id VARCHAR(255) PRIMARY KEY,
            inGameName VARCHAR(255) NOT NULL,
            balance NUMERIC(15, 2) NOT NULL DEFAULT 10000.00
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS stock_prices (
            inGameName VARCHAR(255) PRIMARY KEY,
            current_price NUMERIC(10, 2) NOT NULL DEFAULT 10.00
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS portfolios (
            portfolio_id SERIAL PRIMARY KEY,
            investor_discord_id VARCHAR(255) NOT NULL,
            stock_inGameName VARCHAR(255) NOT NULL,
            shares_owned NUMERIC(15, 6) NOT NULL,
            CONSTRAINT fk_investor
                FOREIGN KEY(investor_discord_id) 
                REFERENCES balances(discord_id),
            CONSTRAINT fk_stock
                FOREIGN KEY(stock_inGameName) 
                REFERENCES stock_prices(inGameName)
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
            cc_amount NUMERIC(15, 2),
            fee_paid NUMERIC(15, 2),
            details JSONB,
            balance_after NUMERIC(15, 2)
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
            print("Market tables created or already exist.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

### Marketplace (Fan Exchange) Functions ###
def get_market_data_from_db():
    """
    Fetches all core market tables from the database and returns them as a
    dictionary of pandas DataFrames, mimicking the old CSV loading logic.
    Also fetches non-migrated CSV files for hybrid operation.
    """
    conn = get_connection()
    if not conn:
        logging.error("Cannot fetch market data, no database connection.")
        return None
    
    try:
        with conn: # Using 'with' ensures the connection is closed
            balances_df = pd.read_sql("SELECT * FROM balances", conn)
            stock_prices_df = pd.read_sql("SELECT * FROM stock_prices", conn)
            portfolios_df = pd.read_sql("SELECT * FROM portfolios", conn)
            shop_upgrades_df = pd.read_sql("SELECT * FROM shop_upgrades", conn)
            
            # --- Rename columns to match expected DataFrame format ---
            balances_df.rename(columns={'ingamename': 'inGameName'}, inplace=True)
            stock_prices_df.rename(columns={'ingamename': 'inGameName'}, inplace=True)
            portfolios_df.rename(columns={'stock_ingamename': 'stock_inGameName'}, inplace=True)
            
            # For hybrid mode, we still load these from CSV
            market_state_df = pd.read_csv('market/market_state.csv')
            member_initialization_df = pd.read_csv('market/member_initialization.csv')
            market_events_df = pd.read_csv('market/market_events.csv')

        logging.info("Successfully fetched all market data from the database.")
        
        return {
            'crew_coins': balances_df, # Use the old key for compatibility
            'stock_prices': stock_prices_df,
            'portfolios': portfolios_df,
            'shop_upgrades': shop_upgrades_df,
            'market_state': market_state_df,
            'member_initialization': member_initialization_df,
            'market_events': market_events_df
        }
        
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error fetching market data from DB: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()

def save_all_market_data_to_db(balances_df, stock_prices_df, new_transactions):
    """
    Saves all updated market data to the database in a single transaction.
    - Updates balances
    - Updates stock prices
    - Inserts new periodic earnings transactions
    """
    conn = get_connection()
    if not conn:
        logging.error("Cannot save market data, no database connection.")
        return False
        
    with conn.cursor() as cursor:
        try:
            # 1. Update Balances
            # Create a temporary table, insert the new data, then update the main table
            cursor.execute("CREATE TEMP TABLE temp_balances (discord_id VARCHAR(255) PRIMARY KEY, balance NUMERIC(15, 2));")
            balances_tuples = [tuple(x) for x in balances_df[['discord_id', 'balance']].to_numpy()]
            extras.execute_values(cursor, "INSERT INTO temp_balances (discord_id, balance) VALUES %s", balances_tuples)
            cursor.execute("""
                UPDATE balances
                SET balance = temp_balances.balance
                FROM temp_balances
                WHERE balances.discord_id = temp_balances.discord_id;
            """)
            logging.info(f"Updated {len(balances_df)} rows in 'balances' table.")

            # 2. Update Stock Prices
            cursor.execute("CREATE TEMP TABLE temp_stock_prices (inGameName VARCHAR(255) PRIMARY KEY, current_price NUMERIC(10, 2));")
            prices_tuples = [tuple(x) for x in stock_prices_df[['inGameName', 'current_price']].to_numpy()]
            extras.execute_values(cursor, "INSERT INTO temp_stock_prices (inGameName, current_price) VALUES %s", prices_tuples)
            cursor.execute("""
                UPDATE stock_prices
                SET current_price = temp_stock_prices.current_price
                FROM temp_stock_prices
                WHERE stock_prices.inGameName = temp_stock_prices.inGameName;
            """)
            logging.info(f"Updated {len(stock_prices_df)} rows in 'stock_prices' table.")

            # 3. Insert new transactions
            if new_transactions:
                extras.execute_values(
                    cursor,
                    """INSERT INTO transactions 
                       (timestamp, actor_id, target_id, transaction_type, item_name, cc_amount, fee_paid, details, balance_after) 
                       VALUES %s""",
                    new_transactions
                )
                logging.info(f"Inserted {len(new_transactions)} new PERIODIC_EARNINGS transactions.")

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
    print("Initialization complete.")