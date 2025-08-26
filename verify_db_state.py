# verify_db_state.py
import pandas as pd
import logging
from market.database import get_connection

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def verify_state():
    """Connects to the database and runs several checks to verify data."""
    conn = get_connection()
    if not conn:
        logging.fatal("Could not establish database connection. Aborting.")
        return

    try:
        print("\n--- Verifying Database State ---")

        # 1. Check a specific user's balance
        print("\n[CHECK 1] Balance for user 'CashBaggins':")
        # Using pd.read_sql is a convenient way to get query results into a table
        balance_df = pd.read_sql("""
            SELECT *
            FROM balances 
            WHERE ingamename = 'CashBaggins';
        """, conn)
        print(balance_df.to_string(index=False))
        
        # 2. Check the top 5 stock prices
        print("\n[CHECK 2] Top 5 stock prices by value:")
        prices_df = pd.read_sql("""
            SELECT *
            FROM stock_prices 
            ORDER BY current_price DESC 
            LIMIT 5;
        """, conn)
        print(prices_df.to_string(index=False))

        # 3. Check for new periodic earnings transactions
        print("\n[CHECK 3] 10 most recent 'PERIODIC_EARNINGS' transactions:")
        
        # timestamp, actor_id, transaction_type, cc_amount, balance_after
        # WHERE transaction_type = 'PERIODIC_EARNINGS' 
        transactions_df = pd.read_sql("""
            SELECT *
            FROM transactions 
            
            ORDER BY timestamp DESC 
            LIMIT 50;
        """, conn)
        # Format for readability
        if not transactions_df.empty:
            transactions_df['timestamp'] = transactions_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        print(transactions_df.to_string(index=False))
        
        # 3. Check for new periodic earnings transactions
        print("\n[CHECK 4] marketstate")
        
        # timestamp, actor_id, transaction_type, cc_amount, balance_after
        transactions_df = pd.read_sql("""
            SELECT *
            FROM market_state 
            ;
        """, conn)
        # Format for readability
        
        print(transactions_df.to_string(index=False))

        print("\n[CHECK 5] stockpricehistory")
        
        # timestamp, actor_id, transaction_type, cc_amount, balance_after
        transactions_df = pd.read_sql("""
            SELECT *
            FROM stock_price_history 
            ;
        """, conn)
        # Format for readability
        
        print(transactions_df.to_string(index=False))


        print("\n--- Verification Complete ---")

    except Exception as e:
        logging.error(f"An error occurred during verification: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    verify_state()