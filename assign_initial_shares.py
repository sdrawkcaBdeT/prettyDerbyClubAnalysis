import pandas as pd
import os

def assign_initial_shares():
    """
    Assigns 1 share of each registered member's own stock to them.
    This is a one-time script to be run after market initialization.
    """
    print("Assigning initial shares to all registered members...")

    MARKET_DIR = 'market'
    REGISTRATIONS_FILE = 'user_registrations.csv'
    PORTFOLIOS_FILE = os.path.join(MARKET_DIR, 'portfolios.csv')

    # --- 1. Load Prerequisite Files ---
    try:
        registrations_df = pd.read_csv(REGISTRATIONS_FILE)
        portfolios_df = pd.read_csv(PORTFOLIOS_FILE)
    except FileNotFoundError as e:
        print(f"ERROR: Prerequisite file not found: {e.filename}")
        print("Please ensure you have run initialize_market.py first.")
        return

    # --- 2. Generate New Share Records ---
    new_holdings = []
    for _, row in registrations_df.iterrows():
        discord_id = str(int(row['discord_id']))
        ingame_name = row['inGameName']

        # Check if the user already has a holding of their own stock
        owns_own_stock = not portfolios_df[
            (portfolios_df['investor_discord_id'] == discord_id) &
            (portfolios_df['stock_inGameName'] == ingame_name)
        ].empty

        if not owns_own_stock:
            new_holdings.append({
                'investor_discord_id': discord_id,
                'stock_inGameName': ingame_name,
                'shares_owned': 1.0
            })
            print(f"Assigning 1 share of {ingame_name} to the owner.")
        else:
            print(f"{ingame_name} already owns their own stock. Skipping.")


    # --- 3. Save the Updated Portfolios File ---
    if not new_holdings:
        print("No new shares were assigned.")
        return

    new_holdings_df = pd.DataFrame(new_holdings)
    updated_portfolios_df = pd.concat([portfolios_df, new_holdings_df], ignore_index=True)
    
    updated_portfolios_df.to_csv(PORTFOLIOS_FILE, index=False)
    
    print("\n--- Initial Share Assignment Complete ---")
    print(f"Successfully assigned {len(new_holdings)} new share holdings.")

if __name__ == '__main__':
    assign_initial_shares()