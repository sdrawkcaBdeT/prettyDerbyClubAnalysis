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
    return pd.read_csv('market/market_state.csv', index_col='state_name')['value']

def save_market_state(state_series):
    state_series.to_frame(name='value').to_csv('market/market_state.csv')

def log_market_event(event_name, event_type, details="{}"):
    log_file = 'market/market_event_log.csv'
    file_exists = os.path.isfile(log_file)
    timestamp = datetime.now(pytz.timezone('US/Central')).isoformat()
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = pd.io.common.get_csv_writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'event_name', 'event_type', 'details'])
        writer.writerow([timestamp, event_name, event_type, details])

def update_lag_index(run_timestamp):
    """
    Checks if the market's data lag should shift.
    97% chance to remain the same.
    1.5% chance to shift forward 1 position.
    1.5% chance to shift forward 2 positions.
    Returns an announcement string if a change occurs.
    """
    market_state = get_market_state()
    
    # Check how long it's been since the last check to scale the probability
    last_check_str = str(market_state.get('last_lag_check_timestamp', run_timestamp.isoformat()))
    last_check_time = pd.to_datetime(last_check_str).tz_convert('US/Central')
    hours_elapsed = (run_timestamp - last_check_time).total_seconds() / 3600
    
    # Base 3% chance of change per 24 hours. Scale it to the time elapsed.
    prob_of_no_change = 0.97 ** (hours_elapsed / 24)
    
    announcement = None
    
    if np.random.rand() > prob_of_no_change:
        # A change has occurred. Determine if it's +1 or +2.
        shift = 2 if np.random.rand() > 0.5 else 1
        
        lag_options_str = market_state.get('lag_options', "[0]")
        try:
            lag_options = ast.literal_eval(lag_options_str)
            if not isinstance(lag_options, list): lag_options = [0]
        except (ValueError, SyntaxError):
            lag_options = [0]
            
        current_cursor = int(market_state.get('active_lag_cursor', 0))
        current_lag_days = lag_options[current_cursor]
        
        # Calculate the new cursor position with wrap-around
        new_cursor = (current_cursor + shift) % len(lag_options)
        
        market_state['active_lag_cursor'] = new_cursor
        
        new_lag_days = lag_options[new_cursor]
        
        # --- NEW THEMATIC ANNOUNCEMENT LOGIC ---
        if new_lag_days < current_lag_days:
            # The lag is shorter (e.g., went from 5 days to 3 days)
            announcement = f"📈 **Market Intel Update:** Analysts are reacting to more recent data, shortening their analytical window."
        elif new_lag_days > current_lag_days:
            # The lag is longer (e.g., went from 3 days to 5 days)
            announcement = f"📉 **Market Intel Update:** Analysts are taking a long-term view, extending their analytical window to focus on historical trends."
        else:
            # This case would only happen if all lag options are the same, but it's good to have a fallback.
            announcement = f"📊 **Market Intel Update:** Analysts have reaffirmed their focus. The Baggins Index continues to operate with the same analytical window."

        print(f"LAG INDEX SHIFT: New cursor is {new_cursor} ({new_lag_days} days).")
        log_market_event("Lag Index Shift", "SHIFT", details=json.dumps({'new_lag_days': new_lag_days, 'old_lag_days': current_lag_days}))

    market_state['last_lag_check_timestamp'] = _format_timestamp(run_timestamp)
    save_market_state(market_state)
    return announcement


def clear_and_check_events(run_timestamp):
    """
    Clears expired events and then checks if a new one should trigger.
    Returns the name of any new event that was just triggered.
    """
    market_state = get_market_state()
    new_event_triggered = None

    # --- 1. Clear Expired Events ---
    active_event = str(market_state.get('active_event', 'None'))
    if active_event != 'None':
        event_end_time_str = str(market_state.get('event_end_time', 'None'))
        if event_end_time_str != 'None':
            event_end_time = pd.to_datetime(event_end_time_str).tz_convert('US/Central')
            if run_timestamp > event_end_time:
                print(f"EVENT EXPIRED: {active_event}")
                log_market_event(active_event, 'END')
                market_state['active_event'] = 'None'
                market_state['event_end_time'] = 'None'
                active_event = 'None' # Update for the next check

    # --- 2. Check for New Event Trigger ---
    if active_event == 'None':
        last_check_str = str(market_state.get('last_event_check_timestamp', 'NaT'))
        if pd.isna(last_check_str) or last_check_str in ['None', 'NaT']:
            hours_elapsed = 1 # On first run, default to 1 hour
        else:
            last_check_time = pd.to_datetime(last_check_str).tz_convert('US/Central')
            hours_elapsed = (run_timestamp - last_check_time).total_seconds() / 3600
        
        trigger_chance = 1 - (0.995 ** hours_elapsed)
        if np.random.rand() < trigger_chance:
            market_events_df = pd.read_csv('market/market_events.csv')
            if not market_events_df.empty:
                chosen_event = market_events_df.sample(1).iloc[0]
                event_name = chosen_event['event_name']
                duration_hours = int(chosen_event['duration_hours'])
                end_time = run_timestamp + timedelta(hours=duration_hours)
                
                market_state['active_event'] = event_name
                market_state['event_end_time'] = _format_timestamp(end_time)
                new_event_triggered = event_name # Set the flag for the return value
                print(f"EVENT TRIGGERED: {event_name}")
                log_market_event(event_name, 'START', details=json.dumps(chosen_event.to_dict()))

    market_state['last_event_check_timestamp'] = _format_timestamp(run_timestamp)
    save_market_state(market_state)
    return new_event_triggered
