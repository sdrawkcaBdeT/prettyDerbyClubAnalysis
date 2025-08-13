import pyautogui
import time

# CONFIGURATION
pyautogui.PAUSE = 0.1 # a tenth of a second pause between autogui calls

### Key functions ###


# "Home" page actions

def clickClub():
    pyautogui.moveTo(x=400, y=1240)
    pyautogui.click()
    time.sleep(5) # wait for page to load

# "Club" page actions

def clickClubMenu():
    pyautogui.moveTo(x=1200, y=75)
    pyautogui.click()
    time.sleep(1) # wait for page to load

def clickHome():
    pyautogui.moveTo(x=740, y=1350)
    pyautogui.click()
    time.sleep(1) # wait for page to load
    
# "Club Menu" page actions

def clickClubInfo():
    pyautogui.moveTo(x=540, y=600)
    pyautogui.click()
    time.sleep(3) # wait for page to load.

def clickClubMenuClose():
    pyautogui.moveTo(x=735, y=937)
    pyautogui.click()
    time.sleep(1) # wait for page to load

# "Club Info" page actions
def clickClubMemberArea():
    """
    Clicks inside the area where club members are displayed, so we can scroll down.
    """
    pyautogui.press("space") 
    pyautogui.moveTo(x=1000, y=800)  
    pyautogui.click()
    time.sleep(0.5)  # Wait for any potential animations or loading
    
def clickClubInfoClose():
    pyautogui.moveTo(x=734, y=1333)  
    pyautogui.click()
    time.sleep(0.5)  # Wait for any potential animations or loading
    
# Generic functions (scrolling)

def scrollDown(ticks=1):
    """
    Scrolls down the page by a specified number of ticks.
    """
    for _ in range(ticks):
        pyautogui.scroll(-1)  # Scroll down
    time.sleep(0.3)