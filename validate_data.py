import pandas as pd
import os
import pytz
import shutil
from datetime import datetime
import csv

# --- Configuration ---
FANLOG_CSV = 'fan_log.csv'
CROPPED_SCREENSHOTS_FOLDER = os.path.join("dataGet", "data", "croppedFanCounts")
ERROR_LOG_CSV = 'data_validation_errors.csv'
ERROR_IMAGE_FOLDER = os.path.join("dataGet", "data", "error_images") # Folder for problematic images

def find_source_image_filename(timestamp, member_name):
    """
    Finds the corresponding cropped image file for a given log entry.
    This works by matching the member name and the timestamp format used in the filenames.
    """
    try:
        # The filename format for the timestamp part is '%Y%m%d%H%M'
        filename_timestamp_str = timestamp.strftime("%Y%m%d%H%M")
        
        # Search the directory for a file that contains both the timestamp string and member name
        for filename in os.listdir(CROPPED_SCREENSHOTS_FOLDER):
            if filename_timestamp_str in filename and member_name in filename:
                return filename
    except Exception as e:
        # This might happen if the directory doesn't exist or there are other issues
        print(f"  - Could not search for image file for {member_name}: {e}")
    return "File Not Found"

def move_error_image(image_filename):
    """Moves the problematic screenshot to an error folder for review."""
    if not os.path.exists(ERROR_IMAGE_FOLDER):
        os.makedirs(ERROR_IMAGE_FOLDER)
    
    source_path = os.path.join(CROPPED_SCREENSHOTS_FOLDER, image_filename)
    destination_path = os.path.join(ERROR_IMAGE_FOLDER, image_filename)
    
    if os.path.exists(source_path):
        try:
            shutil.move(source_path, destination_path)
            print(f"  - Quarantined image: {destination_path}")
            return destination_path # Return the new path
        except Exception as e:
            print(f"  - ❌ ERROR: Could not move {source_path}. Reason: {e}")
    else:
        print(f"  - WARNING: Source image not found at {source_path}")
    return source_path # Return original path if move fails

def log_anomalies_to_csv(anomalies_to_log):
    """
    Appends detected anomalies to a CSV log file for later review.
    """
    file_exists = os.path.isfile(ERROR_LOG_CSV)
    
    try:
        with open(ERROR_LOG_CSV, 'a', newline='') as f:
            fieldnames = ['timestamp', 'inGameName', 'previousFanCount', 'incorrectFanCount', 'negativeFanGain', 'sourceImageFile']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(anomalies_to_log)
        print(f"--- ✅ Successfully logged {len(anomalies_to_log)} error(s) to {ERROR_LOG_CSV} for manual review. ---")
    except Exception as e:
        print(f"--- ❌ ERROR: Could not write to {ERROR_LOG_CSV}: {e} ---")


def validate_fan_gains():
    """
    Loads the fan log, checks ONLY THE LATEST ENTRIES for negative fan gains, 
    quarantines the source image, and logs the error without stopping the scheduler.
    """
    print("--- 2a. Running Data Validation ---")
    try:
        df = pd.read_csv(FANLOG_CSV)
    except FileNotFoundError:
        print(f"  - ERROR: {FANLOG_CSV} not found. Cannot perform validation.")
        return

    # --- Data Cleaning ---
    df.dropna(subset=['inGameName', 'fanCount'], inplace=True)
    df['fanCount'] = pd.to_numeric(df['fanCount'].astype(str).str.replace(',', '', regex=False), errors='coerce')
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.dropna(subset=['fanCount', 'timestamp'], inplace=True)
    
    # Localize timestamps to ensure correct sorting and comparison
    central_tz = pytz.timezone('US/Central')
    df['timestamp'] = df['timestamp'].dt.tz_localize(central_tz, ambiguous='infer')
    
    # --- Anomaly Detection on MOST RECENT entries only ---
    
    # Find the timestamp of the most recent data collection run
    latest_timestamp = df['timestamp'].max()
    
    # Get all entries from the latest run
    latest_entries = df[df['timestamp'] == latest_timestamp]
    
    anomalies_found = []

    print(f"--- Validating {len(latest_entries)} entries from run at {latest_timestamp.strftime('%Y-%m-%d %H:%M')} ---")

    for _, current_row in latest_entries.iterrows():
        member_name = current_row['inGameName']
        
        # Get all historical data for this specific member and sort it
        member_history = df[df['inGameName'] == member_name].sort_values('timestamp')
        
        # Ensure there are at least two data points to compare
        if len(member_history) < 2:
            continue # Skip members with only one entry (e.g., new members)
            
        # The current entry is the last one in their history
        # The previous entry is the second to last one
        previous_row = member_history.iloc[-2]
        
        previous_fan_count = previous_row['fanCount']
        current_fan_count = current_row['fanCount']
        
        fan_gain = current_fan_count - previous_fan_count
        
        if fan_gain < 0:
            # Anomaly detected!
            print(f"\n  - ⚠️ ANOMALY DETECTED for {member_name}!")
            
            image_filename = find_source_image_filename(current_row['timestamp'], member_name)
            
            # Move the image and get its new path
            quarantined_image_path = move_error_image(image_filename)

            # Prepare the record for logging
            anomaly_record = {
                'timestamp': current_row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'inGameName': member_name,
                'previousFanCount': int(previous_fan_count),
                'incorrectFanCount': int(current_fan_count),
                'negativeFanGain': int(fan_gain),
                'sourceImageFile': quarantined_image_path
            }
            anomalies_found.append(anomaly_record)
            
            print(f"    - Previous Count: {int(previous_fan_count):,}")
            print(f"    - Incorrect Count: {int(current_fan_count):,}")

    if anomalies_found:
        print("\n--- ⚠️ DATA VALIDATION ALERT SUMMARY ---")
        log_anomalies_to_csv(anomalies_found)
        print("--- Scheduler will continue. Please correct the data in fan_log.csv later. ---")
    else:
        print("--- ✅ Data validation passed. No negative fan gains found in the latest run. ---")

if __name__ == "__main__":
    validate_fan_gains()