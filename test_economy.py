import unittest
import pandas as pd
from datetime import datetime
import pytz
import json
from market.economy import process_cc_earnings

class TestEconomy(unittest.TestCase):

    def setUp(self):
        """Set up common data for tests."""
        self.run_timestamp = pytz.timezone('US/Central').localize(datetime(2025, 9, 8, 12, 0, 0))

        self.enriched_fan_log_df = pd.DataFrame({
            'inGameName': ['PlayerA'],
            'timestamp': [self.run_timestamp],
            'performancePrestigePoints': [100.0],
            'tenurePrestigePoints': [10.0],
            'prestigePurchased': [0],
            'fanGain': [83330],
            'timeDiffMinutes': [720]
        })

        self.portfolios_df = pd.DataFrame({
            'investor_discord_id': ['101', '102', '103', '100'],
            'stock_inGameName': ['PlayerA', 'PlayerA', 'PlayerA', 'PlayerA'],
            'shares_owned': [500, 500, 100, 200]
        })

        self.crew_coins_df = pd.DataFrame({
            'discord_id': ['100', '101', '102', '103'],
            'inGameName': ['PlayerA', 'Investor1', 'Investor2', 'Investor3'],
            'balance': [1000.0, 5000.0, 5000.0, 5000.0]
        })

        self.shop_upgrades_df = pd.DataFrame(columns=['discord_id', 'upgrade_name', 'tier'])

        self.market_state_df = pd.DataFrame({
            'state_name': ['active_event', 'club_sentiment', 'active_lag_cursor', 'lag_options'],
            'state_value': ['None', '1.0', '0', '[0]']
        })

        self.market_data = {
            'crew_coins': self.crew_coins_df,
            'portfolios': self.portfolios_df,
            'shop_upgrades': self.shop_upgrades_df,
            'market_state': self.market_state_df
        }

    def test_dividend_tie_logic(self):
        """
        Tests that when two investors are tied for top shareholder, the Tier 1
        dividend is split equally between them.
        """
        updated_balances, new_transactions = process_cc_earnings(
            self.enriched_fan_log_df, self.market_data, self.run_timestamp
        )

        # PlayerA's earnings calculation as per the code's logic:
        # The default performance_yield_modifier is 1.0 and it's ADDED to the multiplier.
        # Performance Yield: (100 perf_prestige + 2 perf_flat_bonus) * (1.75 perf_multiplier + 1.0 modifier) = 102 * 2.75 = 280.5
        # Tenure Yield: 10 tenure_prestige * 2.0 tenure_multiplier = 20.0
        # Base CC Earned (before hype): 280.5 + 20.0 = 300.5
        # Hype bonus multiplier: 1 + (0.0005 * 1100 other shares) = 1.55
        # Total Personal Earnings: 300.5 * 1.55 = 465.775

        # Dividend logic is based on TOTAL personal earnings.
        # Tier 1 dividend pool (20%): 465.775 * 0.20 = 93.155
        # Tier 2 dividend pool (10%): 465.775 * 0.10 = 46.5775

        dividend_txs = [tx for tx in new_transactions if tx['transaction_type'] == 'DIVIDEND']

        investor1_dividend = sum(tx['cc_amount'] for tx in dividend_txs if tx['actor_id'] == '101')
        investor2_dividend = sum(tx['cc_amount'] for tx in dividend_txs if tx['actor_id'] == '102')
        investor3_dividend = sum(tx['cc_amount'] for tx in dividend_txs if tx['actor_id'] == '103')

        # Correct Behavior: Tier 1 pool is split between the two top investors.
        # 93.155 / 2 = 46.5775 each
        self.assertAlmostEqual(investor1_dividend, 46.5775, places=4, msg="Investor1's Tier 1 dividend is incorrect.")
        self.assertAlmostEqual(investor2_dividend, 46.5775, places=4, msg="Investor2's Tier 1 dividend is incorrect.")

        # Investor3 is the only one in Tier 2, so they get the whole pool.
        self.assertAlmostEqual(investor3_dividend, 46.5775, places=4, msg="Investor3's Tier 2 dividend is incorrect.")

if __name__ == '__main__':
    unittest.main()
