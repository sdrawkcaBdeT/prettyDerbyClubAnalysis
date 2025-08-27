import subprocess
import sys
import time
from datetime import datetime, timedelta

# Define your scripts with a clear name-to-file mapping
scripts = {
    "collect": ["dataGet.py"],
    "analyze": ["analysis.py"],
    "visualize": ["generate_visuals.py"],
    
    "talking_points": ["generate_talking_points.py"],
    
    # --- Sequences ---
    # Runs the full data collection and analysis pipeline
    "full_run": ["dataGet.py","validate_data.py", "analysis.py","generate_visuals.py"], # 
    "full_run_once": ["dataGet.py","validate_data.py", "analysis.py","generate_visuals.py"],
}

def wait_for_scheduled_start():
    """
    Calculates the time until the next 5-minute mark before the hour
    (e.g., 6:55, 7:55) and waits until that time.
    """
    now = datetime.now()
    # Calculate the next hour
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    
    # Set the start time to 5 minutes before the next hour
    start_time = next_hour - timedelta(minutes=5)
    
    # If the calculated start time is in the past (i.e., it's between xx:55 and xx:59), 
    # we need to aim for the *next* hour's 55-minute mark.
    if start_time < now:
        start_time += timedelta(hours=1)

    wait_seconds = (start_time - now).total_seconds()
    
    if wait_seconds > 0:
        print(f"--- Waiting for {wait_seconds:.0f} seconds to start at {start_time.strftime('%H:%M:%S')} ---")
        time.sleep(wait_seconds)

def run_script(script_paths, sleep_time=0):
    """
    Executes a list of scripts in sequence.
    """
    for script_path in script_paths:
        if script_path:
            print(f"--- Running {script_path}... ---")
            try:
                # Using check=True to raise an error if the script fails
                subprocess.run(['python', script_path], check=True)
                print(f"--- Finished {script_path} ---")
            except subprocess.CalledProcessError as e:
                print(f"--- ERROR: {script_path} failed with exit code {e.returncode} ---")
                # Stop the sequence if a script fails
                return False
            except FileNotFoundError:
                print(f"--- ERROR: Script not found at '{script_path}' ---")
                return False
        else:
            print(f"--- ERROR: Invalid script path provided. ---")
            return False
            
    if sleep_time > 0:
        print(f"--- Sleeping for {sleep_time} seconds... ---")
        time.sleep(sleep_time)
        
    # Return True if all scripts in the sequence ran successfully
    return True


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == 'full_run_once':
            print("--- Starting single 'full_run_once' sequence... ---")
            success = run_script(scripts['full_run_once'])
            if not success:
                sys.exit(1)
            print("--- 'full_run_once' finished successfully. ---")
            sys.exit(0)
        try:
            # --- 1. IMMEDIATE INITIAL RUN ---
            print("--- Starting initial run immediately... ---")
            for arg in sys.argv[1:]:
                if arg in scripts:
                    success = run_script(scripts[arg])
                    if not success:
                        print(f"--- Halting scheduler due to error in initial run of '{arg}' sequence. ---")
                        sys.exit(1) # Exit if the initial run fails
                else:
                    print(f"--- Keyword '{arg}' not recognized. Skipping for initial run. ---")
            
            print("--- Initial run finished. Starting continuous scheduled runs. Press Ctrl+C to exit. ---")
            
            # --- 2. CONTINUOUS SCHEDULED LOOP ---
            while True: # Loop indefinitely
                # Wait for the scheduled time to start the cycle
                wait_for_scheduled_start()
                
                print(f"--- Starting scheduled run at {datetime.now().strftime('%H:%M:%S')} ---")
                
                # Run the scripts specified by the user
                for arg in sys.argv[1:]:
                    if arg in scripts:
                        # Pass the list of script files to the run_script function
                        success = run_script(scripts[arg])
                        if not success:
                            print(f"--- Halting execution for this cycle due to an error in the '{arg}' sequence. ---")
                            break # Stop processing further arguments if a sequence fails
                    else:
                        print(f"--- Keyword '{arg}' not recognized. Skipping. ---")
                
                print(f"--- Current run cycle finished. Waiting for next scheduled time. ---")

        except KeyboardInterrupt:
            print("\n--- Scheduler stopped by user. Goodbye! ---")
            sys.exit(0)
    else:
        # Default message if no arguments are provided
        print("--- Race Day Scheduler ---")
        print("Please specify which script or sequence you would like to run.")
        print("Available keywords:")
        for keyword in scripts.keys():
            print(f"  - {keyword}")
        print("\nExample for continuous run: python race_day_scheduler.py full_run")