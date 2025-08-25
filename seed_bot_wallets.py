import json
import logging
from market import database # Import our existing database module

logging.basicConfig(level=logging.INFO)

def seed_wallets():
    """
    Reads the bot personality config and creates/updates their wallets
    in the database. This is safe to run multiple times.
    """
    try:
        with open('market/configs/bot_personalities.json', 'r') as f:
            bots = json.load(f)
            logging.info(f"Loaded {len(bots)} bot profiles from config.")
    except FileNotFoundError:
        logging.error("FATAL: bot_personalities.json not found. Cannot seed wallets.")
        return

    conn = database.get_connection()
    if not conn:
        logging.error("Could not connect to the database. Aborting.")
        return

    with conn.cursor() as cursor:
        try:
            for bot in bots:
                bot_name = bot['name']
                bankroll = bot['bankroll']
                
                # UPSERT logic:
                # - If a bot with this name exists, UPDATE its balance.
                # - If it doesn't exist, INSERT a new row.
                # This makes the script safe to re-run without creating duplicates.
                cursor.execute(
                    """
                    INSERT INTO crew_coin_wallets (discord_id, balance)
                    VALUES (%s, %s)
                    ON CONFLICT (discord_id) 
                    DO UPDATE SET balance = EXCLUDED.balance;
                    """,
                    (bot_name, bankroll)
                )
            
            conn.commit()
            logging.info(f"Successfully seeded/updated wallets for {len(bots)} bots.")

        except Exception as e:
            logging.error(f"An error occurred during seeding: {e}")
            conn.rollback()
    
    conn.close()

if __name__ == "__main__":
    seed_wallets()