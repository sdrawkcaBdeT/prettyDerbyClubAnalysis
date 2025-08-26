# reset_and_migrate.py
import psycopg2
import logging
import pandas as pd
from psycopg2 import extras
import json
from market.database import get_connection, create_market_tables

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Part 1: Clear Tables Logic ---

TABLES_TO_CLEAR = [
    'portfolios',
    'shop_upgrades',
    'transactions',
    'stock_price_history',
    'balances',
    'stock_prices',
    'market_state'
]

def clear_tables(conn):
    """Truncates the specified tables within an existing connection."""
    logging.info(f"Preparing to clear the following tables: {', '.join(TABLES_TO_CLEAR)}")
    with conn.cursor() as cursor:
        sql_command = f"TRUNCATE TABLE {', '.join(TABLES_TO_CLEAR)} RESTART IDENTITY CASCADE;"
        cursor.execute(sql_command)
    logging.info("✅ All specified tables have been successfully cleared.")

# --- Part 2: Migration Logic ---

def migrate_all_data(conn):
    """Migrates all data from CSVs into the freshly cleared tables."""
    with conn.cursor() as cursor:
        # Balances
        df = pd.read_csv('market/crew_coins.csv', dtype={'discord_id': str})
        data = [tuple(x) for x in df[['discord_id', 'inGameName', 'balance']].to_numpy()]
        extras.execute_values(cursor, "INSERT INTO balances (discord_id, ingamename, balance) VALUES %s ON CONFLICT (discord_id) DO NOTHING", data)
        logging.info(f"Migrated {len(df)} rows to 'balances'.")

        # Stock Prices (initial)
        df = pd.read_csv('market/stock_prices.csv')
        data = [tuple(x) for x in df[['inGameName', 'current_price']].to_numpy()]
        extras.execute_values(cursor, "INSERT INTO stock_prices (ingamename, current_price) VALUES %s ON CONFLICT (ingamename) DO NOTHING", data)
        logging.info(f"Migrated {len(df)} rows to 'stock_prices'.")
        
        # Portfolios
        df = pd.read_csv('market/portfolios.csv', dtype={'investor_discord_id': str})
        data = list(df.itertuples(index=False, name=None))
        extras.execute_values(cursor, "INSERT INTO portfolios (investor_discord_id, stock_ingamename, shares_owned) VALUES %s", data)
        logging.info(f"Migrated {len(df)} rows to 'portfolios'.")

        # Shop Upgrades
        df = pd.read_csv('market/shop_upgrades.csv', dtype={'discord_id': str})
        data = list(df.itertuples(index=False, name=None))
        extras.execute_values(cursor, "INSERT INTO shop_upgrades (discord_id, upgrade_name, tier) VALUES %s", data)
        logging.info(f"Migrated {len(df)} rows to 'shop_upgrades'.")

        # Transactions (Universal Log)
        df = pd.read_csv('market/universal_transaction_log.csv', dtype={'actor_id': str, 'target_id': str})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.fillna({'item_quantity': 0, 'cc_amount': 0, 'fee_paid': 0})
        df['details'] = None
        df['balance_after'] = None
        data = list(df[['timestamp', 'actor_id', 'target_id', 'transaction_type', 'item_name', 'item_quantity', 'cc_amount', 'fee_paid', 'details', 'balance_after']].itertuples(index=False, name=None))
        extras.execute_values(
            cursor, 
            """INSERT INTO transactions (timestamp, actor_id, target_id, transaction_type, item_name, item_quantity, cc_amount, fee_paid, details, balance_after) 
               VALUES %s""", 
            data
        )
        logging.info(f"Migrated {len(df)} rows to 'transactions' from universal log.")

        # Transactions (Earnings History)
        history_df = pd.read_csv('market/balance_history.csv', dtype={'discord_id': str})
        history_df['timestamp'] = pd.to_datetime(history_df['timestamp'])
        data = []
        for _, row in history_df.iterrows():
            details = json.dumps({'performance_yield': row.get('performance_yield'), 'tenure_yield': row.get('tenure_yield'), 'hype_bonus_yield': row.get('hype_bonus_yield'), 'sponsorship_dividend_received': row.get('sponsorship_dividend_received')})
            data.append((row['timestamp'], row['discord_id'], 'SYSTEM', 'PERIODIC_EARNINGS', 'Periodic Earnings', 0, row.get('total_period_earnings'), 0, details, row.get('new_balance')))
        extras.execute_values(cursor, "INSERT INTO transactions (timestamp, actor_id, target_id, transaction_type, item_name, item_quantity, cc_amount, fee_paid, details, balance_after) VALUES %s", data)
        logging.info(f"Back-filled {len(data)} earnings transactions.")

        # Stock Price History
        df = pd.read_csv('market/stock_price_history.csv')
        df.rename(columns={'inGameName': 'ingamename'}, inplace=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        data = list(df[['ingamename', 'price', 'timestamp']].itertuples(index=False, name=None))
        extras.execute_values(cursor, "INSERT INTO stock_price_history (ingamename, price, timestamp) VALUES %s", data)
        logging.info(f"Migrated {len(df)} rows to 'stock_price_history'.")

        # Stock Metadata
        init_df = pd.read_csv('market/member_initialization.csv').replace({float('nan'): None})
        for _, row in init_df.iterrows():
            cursor.execute("UPDATE stock_prices SET ticker = COALESCE(%s, ticker), init_factor = %s, status = %s WHERE ingamename = %s;", (row.get('ticker'), row['random_init_factor'], row['status'], row['inGameName']))
        logging.info(f"Migrated metadata for {len(init_df)} stocks.")

        # Nudge Data
        nudges_df = pd.read_csv('market/prestige_nudges.csv')
        for _, row in nudges_df.iterrows():
            cursor.execute("UPDATE stock_prices SET nudge_bonus = %s WHERE ingamename = %s;", (row['nudge_bonus'], row['inGameName']))
        logging.info(f"Migrated nudge data for {len(nudges_df)} stocks.")

        # Market State
        state_df = pd.read_csv('market/market_state.csv')
        data = [tuple(x) for x in state_df.to_numpy()]
        extras.execute_values(cursor, "INSERT INTO market_state (state_name, state_value) VALUES %s ON CONFLICT (state_name) DO UPDATE SET state_value = EXCLUDED.state_value;", data)
        logging.info(f"Migrated {len(data)} market state rows.")

        # Add missing lag cursor
        cursor.execute("INSERT INTO market_state (state_name, state_value) VALUES ('active_lag_cursor', '0') ON CONFLICT (state_name) DO NOTHING;")
        logging.info("Ensured 'active_lag_cursor' exists.")

# --- Part 3: Main Orchestration ---

def main():
    """Orchestrates the entire reset and migration process."""
    create_market_tables()

    conn = get_connection()
    if not conn:
        logging.fatal("Could not establish database connection. Aborting.")
        return

    try:
        logging.info("--- Starting Database Reset and Migration ---")
        clear_tables(conn)
        migrate_all_data(conn)
        
        conn.commit()
        logging.info("--- ✅ Database Reset and Migration Completed Successfully! ---")
    except (Exception, psycopg2.Error) as error:
        logging.error(f"A fatal error occurred: {error}")
        conn.rollback()
        logging.error("--- ❌ Process failed. Transaction has been rolled back. ---")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()