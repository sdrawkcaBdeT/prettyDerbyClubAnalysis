# skills.py
import random

# --- Helper Condition Functions ---
# These functions check if a skill's conditions are met. They are kept separate for clarity.

def _get_rank(horse, all_horses):
    sorted_horses = sorted(all_horses, key=lambda h: h.position, reverse=True)
    return sorted_horses.index(horse) + 1

def _get_nearby_horses_count(horse, all_horses, space_range):
    count = 0
    for other_horse in all_horses:
        if horse == other_horse:
            continue
        if abs(horse.position - other_horse.position) <= space_range:
            count += 1
    return count

# --- Skill Definitions ---

SKILL_DEFINITIONS = {
    # --- Unique Skills ---
    "Huge Lead": {
        "description": "Increase ability to maintain the lead when leading by a large margin mid-race.",
        "trigger_phase": "pre-roll",
        "condition": lambda horse, race: (
            race.get_leg(horse.position) == "Middle" and
            _get_rank(horse, race.horses) == 1 and
            horse.position >= (sorted(race.horses, key=lambda h: h.position, reverse=True)[1].position + 8)
        ),
        "chance": 50,
        "effect": lambda horse, race: {"negate_final_penalty": True}
    },
    "Early Lead": {
        "description": "Increase ability to go to the front early race.",
        "trigger_phase": "passive", # Passive skills are handled directly in the roll calculation
        "effect_bonus": 2
    },
    "Victoria por plata": {
        "description": "Hang onto advantage when positioned towards the front on the final straight.",
        "trigger_phase": "pre-roll",
        "condition": lambda horse, race: (
            race.get_leg(horse.position) == "Final" and
            _get_rank(horse, race.horses) <= 2
        ),
        "chance": 40,
        "effect": lambda horse, race: {"roll_bonus": 3}
    },
    "Trumpet Blast": {
        "description": "Increase ability to break out of the pack in the final meters.",
        "trigger_phase": "pre-roll",
        "condition": lambda horse, race: race.is_final_round, # A flag set by the race runner
        "chance": 60,
        "effect": lambda horse, race: {"roll_bonus": 5}
    },
    "Uma Stan": {
        "description": "Increase velocity when close to many other runners.",
        "trigger_phase": "pre-roll",
        "condition": lambda horse, race: (
            race.get_leg(horse.position) in ["Middle", "Final"] and
            _get_nearby_horses_count(horse, race.horses, 2) > 0
        ),
        "chance": lambda horse, race: min(100, 10 * _get_nearby_horses_count(horse, race.horses, 2)),
        "effect": lambda horse, race: {"roll_bonus": _get_nearby_horses_count(horse, race.horses, 2)}
    },
    "Fiery Satisfaction": {
        "description": "Increase velocity when passing another runner towards the back on the final corner.",
        "trigger_phase": "post-roll",
        "condition": lambda horse, race, context: (
            race.get_leg(horse.position) == "Final" and
            context.get("horses_passed", 0) > 0
        ),
        "chance": 50,
        "effect": lambda horse, race, context: {"extra_movement": context.get("horses_passed", 0)}
    },

    # --- Generic Skills ---
    "Straightaway Adept": {
        "description": "In the Middle Leg, 30% chance to gain +2 on a roll.",
        "trigger_phase": "pre-roll",
        "condition": lambda horse, race: race.get_leg(horse.position) == "Middle",
        "chance": 30,
        "effect": lambda horse, race: {"roll_bonus": 2}
    },
    "Homestretch Haste": {
        "description": "In the Final Leg, 35% chance to gain +3 on a roll.",
        "trigger_phase": "pre-roll",
        "condition": lambda horse, race: race.get_leg(horse.position) == "Final",
        "chance": 35,
        "effect": lambda horse, race: {"roll_bonus": 3}
    },
    "Slipstream": {
        "description": "If 1-2 spaces behind another horse, 40% chance to gain +3 on a roll.",
        "trigger_phase": "pre-roll",
        "condition": lambda horse, race: any(
            (other.position - horse.position) in [1, 2]
            for other in race.horses if other != horse
        ),
        "chance": 40,
        "effect": lambda horse, race: {"roll_bonus": 3}
    },
    "Late Start": {
        "description": "A negative trait. 20% chance to get -1 on the first roll of the race.",
        "trigger_phase": "pre-roll",
        "condition": lambda horse, race: race.round_number == 1,
        "chance": 20,
        "effect": lambda horse, race: {"roll_bonus": -1}
    }
}