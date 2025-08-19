import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import os
import json

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
                market_state['event_end_time'] = end_time.isoformat()
                new_event_triggered = event_name # Set the flag for the return value
                print(f"EVENT TRIGGERED: {event_name}")
                log_market_event(event_name, 'START', details=json.dumps(chosen_event.to_dict()))

    market_state['last_event_check_timestamp'] = run_timestamp.isoformat()
    save_market_state(market_state)
    return new_event_triggered