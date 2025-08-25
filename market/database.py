# market/database.py
import psycopg2
from psycopg2 import extras
import logging
import os
from dotenv import load_dotenv
import json

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