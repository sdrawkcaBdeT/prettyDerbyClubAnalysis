import pyautogui
import pytesseract
import time
import pandas as pd
from PIL import Image, ImageEnhance, ImageFilter
import os
import cv2
import csv
import pytz
from datetime import datetime
from guiNavigationFunctions import clickClub, clickClubMenu, clickClubInfo, clickClubMemberArea, scrollDown, clickHome, clickClubMenuClose, clickClubInfoClose, harmlessClick

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# --- Configuration ---
screenshots_folder = os.path.join("dataGet","data","rawScreenShots")
cropped_screenshots_folder = os.path.join("dataGet","data","croppedFanCounts")
templates_dir = "dataGet/imgToFind/clubMembers"


# list of members in order they appear in the club info page, used to load club member images and find their coordinates
clubMembers = [
    "CashBaggins",
    # "Top",
    "WaveofGratitude",
    "GhostlyInsomnia",
    "Kino",
    "Claimses",
    "d",
    "Yuto",
    "Dill Dough",
    "Pygums",
    "Eunoia",
    "Ray",
    "Epidemic",
    "Gearras",
    "SmillBisser",
    # "StarLight",
    # "cinciol",
    "Iyy",
    "Walzy",
    "Keyevin",
    "gumgum",
    "Twice",
    "Ermereas",
    "Maslow",
    "Trainer#3487",
    "Wither",
    "AO",
    "s",
    "Kurumi",
    "Kei",
    "Nice",
    "g",
]
clubMemberTemplateImageFileNames = [os.path.join(templates_dir, f"clubMember{name}.png") for name in clubMembers]

# sequence of scrolls; we get two club members per screenshot, so we need 15 screenshots to get all 30 members.
# each number in the list is how many scroll ticks to do before taking the next screenshot.
scrollsBeforePic = [0,6,5,6,5,
                    6,6,5,6,6,
                    5,6,6,5,5]

# the relevant png of the top left of the club member's stat box is at dataGet/imgToFind/clubMembers/ and is named f"clubMember{member}.png"
# the following fan coordinates are RELATIVE to top left of club members stats box we'll find.
fanCoordinates = (341, 71, 545, 114) #left,top,right,bottom

def save_scan_id_to_csv(scan_id):
    with open("dataGet/data/scan_id.csv", mode="w", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["scan_id"])  # Write the header
        writer.writerow([scan_id])  # Write the current scan ID

def load_scan_id_from_csv():
    try:
        with open("dataGet/data/scan_id.csv", mode="r") as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header
            return int(next(reader)[0])  # Read the first row and return the scan_id
    except (FileNotFoundError, StopIteration):
        return 0  # Return a default value if the file does not exist or is empty

def preprocess_image(image):
    """Prepares a cropped image for better OCR accuracy."""
    image = image.convert('L')
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2)
    image = image.point(lambda p: p > 128 and 255)
    image = image.resize([2 * s for s in image.size], Image.Resampling.LANCZOS)
    return image

def extract_data_from_png(image_path):
    """Uses OCR to extract numeric text from a preprocessed image."""
    try:
        image = Image.open(image_path)
        preprocessed_image = preprocess_image(image)
        config = '--psm 6 outputbase digits'
        text = pytesseract.image_to_string(preprocessed_image, config=config).strip()
        fan_count = ''.join(filter(str.isdigit, text))
        return fan_count
    except Exception as e:
        print(f"  - Error during OCR for {os.path.basename(image_path)}: {e}")
        return None

# --- Main Script Workflow ---

# 1. Navigate to the Club Member Screen
print("--- 1. Navigating to Club Member Screen ---")
# In case we're in a weird state (day restart), do a 12x harmless click first
harmlessClick()
harmlessClick()
harmlessClick()
harmlessClick()
harmlessClick()
harmlessClick()
harmlessClick()
harmlessClick()
harmlessClick()
harmlessClick()
harmlessClick()
harmlessClick()

clickClub()
clickClubMenu()
clickClubInfo()
clickClubMemberArea()

# 2. Capture Screenshots
print("\n--- 2. Capturing Screenshots ---")
scan_id = load_scan_id_from_csv() + 1
central = pytz.timezone("America/Chicago")
now_central = datetime.now(central)
filename_timestamp = now_central.strftime("%Y%m%d%H%M")
formatted_display_timestamp = now_central.strftime("%Y/%m/%d %H:%M")

print(f"Scan Time: {formatted_display_timestamp} | Scan ID: {scan_id}")

for i, scrolls in enumerate(scrollsBeforePic):
    scrollDown(scrolls)
    screenshot_filename = os.path.join(screenshots_folder, f"{filename_timestamp}_{scan_id:04d}_{i+1:02d}.png")
    pyautogui.screenshot().save(screenshot_filename)
    print(f"  - Saved {screenshot_filename}")

save_scan_id_to_csv(scan_id)

# Navigate back to the home page
clickClubMemberArea()
clickClubInfoClose()
clickClubMenuClose()
clickHome()

# 3. Locate Members and Crop Fan Counts
print("\n--- 3. Locating Members and Cropping Fan Counts ---")
raw_screenshot_filenames = sorted([
    entry.path for entry in os.scandir(screenshots_folder)
    if entry.is_file() and entry.name.startswith(f"{filename_timestamp}_{scan_id:04d}")
])

for i, screenshot_filename in enumerate(raw_screenshot_filenames):
    print(f"Processing screenshot {os.path.basename(screenshot_filename)}...")
    screenshot = cv2.imread(screenshot_filename)
    screenshot = cv2.cvtColor(screenshot, cv2.COLOR_BGRA2BGR) if screenshot.shape[2] == 4 else screenshot
    
    # Each screenshot can contain two members
    member_index1 = i * 2
    member_index2 = i * 2 + 1

    # Process first member
    if member_index1 < len(clubMembers):
        template_image = cv2.imread(clubMemberTemplateImageFileNames[member_index1], cv2.IMREAD_COLOR)
        template_image = cv2.cvtColor(template_image, cv2.COLOR_BGRA2BGR) if template_image.shape[2] == 4 else template_image

        result = cv2.matchTemplate(screenshot, template_image, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, max_loc = cv2.minMaxLoc(result)

        if max_val >= 0.8:
            top_left = max_loc
            fan_box = (top_left[0] + fanCoordinates[0], top_left[1] + fanCoordinates[1], 
                       top_left[0] + fanCoordinates[2], top_left[1] + fanCoordinates[3])
            cropped = screenshot[fan_box[1]:fan_box[3], fan_box[0]:fan_box[2]]
            cropped_pil = Image.fromarray(cv2.cvtColor(cropped, cv2.COLOR_BGR2RGB))
            cropped_filename = os.path.join(cropped_screenshots_folder, f"{scan_id}_{filename_timestamp}_{clubMembers[member_index1]}_fans.png")
            cropped_pil.save(cropped_filename)
            print(f"  - Cropped fan count for {clubMembers[member_index1]}")

    # Process second member
    if member_index2 < len(clubMembers):
        template_image = cv2.imread(clubMemberTemplateImageFileNames[member_index2], cv2.IMREAD_COLOR)
        template_image = cv2.cvtColor(template_image, cv2.COLOR_BGRA2BGR) if template_image.shape[2] == 4 else template_image

        result = cv2.matchTemplate(screenshot, template_image, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, max_loc = cv2.minMaxLoc(result)

        if max_val >= 0.8:
            top_left = max_loc
            fan_box = (top_left[0] + fanCoordinates[0], top_left[1] + fanCoordinates[1], 
                       top_left[0] + fanCoordinates[2], top_left[1] + fanCoordinates[3])
            cropped = screenshot[fan_box[1]:fan_box[3], fan_box[0]:fan_box[2]]
            cropped_pil = Image.fromarray(cv2.cvtColor(cropped, cv2.COLOR_BGR2RGB))
            cropped_filename = os.path.join(cropped_screenshots_folder, f"{scan_id}_{filename_timestamp}_{clubMembers[member_index2]}_fans.png")
            cropped_pil.save(cropped_filename)
            print(f"  - Cropped fan count for {clubMembers[member_index2]}")

# 4. Extract Data and Append to fan_log.csv
print("\n--- 4. Extracting Data and Appending to Log ---")
cropped_files = [
    entry.path for entry in os.scandir(cropped_screenshots_folder)
    if entry.is_file() and entry.name.startswith(f"{scan_id}_{filename_timestamp}")
]

new_log_entries = []
for file_path in cropped_files:
    filename = os.path.basename(file_path)
    try:
        member_name = filename.split('_')[2]
        fan_count = extract_data_from_png(file_path)
        if fan_count:
            new_log_entries.append({
                "timestamp": formatted_display_timestamp,
                "inGameName": member_name,
                "fanCount": fan_count
            })
            print(f"  - Extracted {fan_count} for {member_name}")
        else:
            print(f"  - WARNING: Failed to extract fan count for {member_name}")
    except IndexError:
        print(f"  - WARNING: Could not parse member name from filename: {filename}")

if new_log_entries:
    output_filename = 'fan_log.csv'
    fieldnames = ["timestamp", "inGameName", "fanCount"]
    file_exists = os.path.isfile(output_filename)

    try:
        with open(output_filename, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_log_entries)
        print(f"\n--- SUCCESS added {len(new_log_entries)} new entries to {output_filename}! ---")
    except Exception as e:
        print(f"\n--- ERROR writing to {output_filename}: {e} ---")
else:
    print("\n--- No new fan counts to log. ---")
