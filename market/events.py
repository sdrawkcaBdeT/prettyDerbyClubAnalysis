import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import os
import json
import ast

def _format_timestamp(dt_object):
    """Formats a datetime object into the consistent ecosystem format."""
    # Example: 2025-08-23 10:56:33-05:00
    base_str = dt_object.strftime('%Y-%m-%d %H:%M:%S%z')
    return f"{base_str[:-2]}:{base_str[-2:]}"

def get_market_state():
    return pd.read_csv('market/market_state.csv', index_col='state_name')['state_value']

def save_market_state(state_series):
    state_series.to_frame(name='state_value').to_csv('market/market_state.csv')

def log_market_event(event_name, event_type, details="{}"):
    log_file = 'market/market_event_log.csv'
    file_exists = os.path.isfile(log_file)
    timestamp = datetime.now(pytz.timezone('US/Central')).isoformat()
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = pd.io.common.get_csv_writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'event_name', 'event_type', 'details'])
        writer.writerow([timestamp, event_name, event_type, details])

def update_lag_index(market_state_df, run_timestamp):
    """
    Checks if the market's data lag should shift.
    Accepts the market_state DataFrame as input.
    Returns the updated market_state DataFrame and an announcement string.
    """
    market_state = market_state_df.set_index('state_name')['state_value']
    
    last_check_str = str(market_state.get('last_lag_check_timestamp', run_timestamp.isoformat()))
    last_check_time = pd.to_datetime(last_check_str).tz_convert('US/Central')
    hours_elapsed = (run_timestamp - last_check_time).total_seconds() / 3600
    
    prob_of_no_change = 0.0001 ** (hours_elapsed / 24)
    announcement = None
    
    if np.random.rand() > prob_of_no_change:
        shift = 2 if np.random.rand() > 0.5 else 1
        
        lag_options_str = market_state.get('lag_options', "[0]")
        try:
            lag_options = ast.literal_eval(lag_options_str)
            if not isinstance(lag_options, list): lag_options = [0]
        except (ValueError, SyntaxError):
            lag_options = [0]
            
        current_cursor = int(market_state.get('active_lag_cursor', 0))
        current_lag_days = lag_options[current_cursor % len(lag_options)]
        
        new_cursor = (current_cursor + shift) % len(lag_options)
        market_state['active_lag_cursor'] = new_cursor
        new_lag_days = lag_options[new_cursor]
        
        if new_lag_days < current_lag_days:
            announcement = f"📈 **Market Intel Update:** Analysts are reacting to more recent data, shortening their analytical window."
        elif new_lag_days > current_lag_days:
            announcement = f"📉 **Market Intel Update:** Analysts are taking a long-term view, extending their analytical window to focus on historical trends."
        else:
            announcement = f"📊 **Market Intel Update:** Analysts have reaffirmed their focus. The Baggins Index continues to operate with the same analytical window."

        print(f"LAG INDEX SHIFT: New cursor is {new_cursor} ({new_lag_days} days).")
        # log_market_event("Lag Index Shift", "SHIFT", details=json.dumps({'new_lag_days': new_lag_days, 'old_lag_days': current_lag_days}))

    market_state['last_lag_check_timestamp'] = _format_timestamp(run_timestamp)
    
    # Return the updated state and the announcement
    return market_state.reset_index().rename(columns={'index': 'state_name', 'state_value': 'state_value'}), announcement


def clear_and_check_events(market_state_df, run_timestamp):
    """
    This function is now disabled and acts as a pass-through, 
    returning the state it was given without modification.
    """
    print("Market event checking is currently disabled.")
    return market_state_df, None # Return the original state and no announcement
