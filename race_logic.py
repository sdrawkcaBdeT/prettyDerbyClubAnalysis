# race_logic.py
import random
from skills import SKILL_DEFINITIONS

# A simple dice roller
def roll_dice(num_dice, sides=6):
    return sum(random.randint(1, sides) for _ in range(num_dice))

class Horse:
    """Represents a single horse in a race."""
    def __init__(self, number, name, strategy, skills):
        self.number = number
        self.name = name
        self.strategy = strategy
        self.skills = skills
        self.position = 0
        self.final_leg_penalty_negated = False # For Sakura's skill

    def __repr__(self):
        return f"Horse(#{self.number} {self.name}, Pos: {self.position})"

class Race:
    """Manages the state and simulation of a single horse race."""
    def __init__(self, race_id, track_length):
        self.race_id = race_id
        self.track_length = track_length
        self.horses = []
        self.round_number = 0
        self.is_final_round = False
        self.log = []
        self.full_race_log = []
        self.structured_log = []

    def add_horse(self, horse):
        self.horses.append(horse)

    def get_leg(self, position):
        percent_complete = (position / self.track_length) * 100
        if percent_complete <= 30: return "Opening"
        if percent_complete <= 75: return "Middle"
        return "Final"

    def is_finished(self):
        return any(h.position >= self.track_length for h in self.horses)
    
    def _roll_dice(self, num_dice, sides=6):
        """Rolls dice and returns the list of rolls."""
        return [random.randint(1, sides) for _ in range(num_dice)]

    def _calculate_base_roll(self, horse, leg):
        """Determines the base dice rolls and modifiers based on strategy."""
        strategy = horse.strategy
        
        # 1. Start with a default roll of 2d6
        num_dice = 2
        modifier = 0

        # 2. Adjust dice and modifiers based on strategy and leg
        if strategy == "Front Runner":
            if leg == "Opening":
                num_dice = 3
            elif leg == "Final" and not horse.final_leg_penalty_negated:
                modifier = -2
        
        elif strategy == "Pace Chaser":
            if leg == "Middle":
                modifier = 1
            elif leg == "Final":
                modifier = 2
        
        elif strategy == "Late Surger":
            if leg == "Opening":
                modifier = -1
            elif leg == "Final":
                num_dice = 3
                modifier = 2
        
        elif strategy == "End Closer":
            if leg == "Opening":
                num_dice = 1
            elif leg == "Middle":
                modifier = -2
            elif leg == "Final":
                num_dice = 4

        # 3. Now, perform the roll with the determined number of dice
        rolls = self._roll_dice(num_dice)

        # 4. Apply any passive skill modifiers
        if "Early Lead" in horse.skills and leg == "Opening":
            modifier += 2

        return rolls, modifier

    def run_round(self):
        """Simulates one full round, now capturing individual rolls."""
        self.round_number += 1
        self.log.clear()
        self.full_race_log.append(f"\n--- Round {self.round_number} ---")

        if not self.is_final_round and any((h.position + 18) >= self.track_length for h in self.horses):
             self.is_final_round = True
             self.log.append("🔔 The final bell rings! This is the last round!")

        for horse in sorted(self.horses, key=lambda h: h.number):
            current_leg = self.get_leg(horse.position)
            skill_bonus, extra_movement = 0, 0
            skill_activations = []
            
            # Data for logging
            skill_roll, skill_chance = None, None
            
            # --- PRE-ROLL SKILL CHECKS ---
            for skill_name in horse.skills:
                skill = SKILL_DEFINITIONS.get(skill_name)
                if skill and skill["trigger_phase"] == "pre-roll" and skill["condition"](horse, self):
                    skill_chance = skill["chance"]
                    if callable(skill_chance): skill_chance = skill_chance(horse, self)
                    
                    skill_roll = random.randint(1, 100)
                    if skill_roll <= skill_chance:
                        skill_activations.append(skill_name)
                        effect = skill["effect"](horse, self)
                        skill_bonus += effect.get("roll_bonus", 0)
                        if effect.get("negate_final_penalty"):
                            horse.final_leg_penalty_negated = True

            # --- MOVEMENT PHASE ---
            base_rolls, modifier = self._calculate_base_roll(horse, current_leg)
            base_roll_total = sum(base_rolls) + modifier
            total_movement = max(0, base_roll_total + skill_bonus)
            pos_before = horse.position
            horse.position += total_movement

            # --- POST-ROLL SKILL CHECKS ---
            horses_passed = len([h for h in self.horses if h != horse and pos_before < h.position < horse.position])
            extra_movement = 0
            context = {"horses_passed": horses_passed}

            for skill_name in horse.skills:
                skill = SKILL_DEFINITIONS[skill_name]
                if skill["trigger_phase"] == "post-roll" and skill["condition"](horse, self, context):
                    if random.randint(1, 100) <= skill["chance"]:
                        skill_activations.append(f"🔥 {skill_name}")
                        effect = skill["effect"](horse, self, context)
                        extra_movement += effect.get("extra_movement", 0)
            
            horse.position += extra_movement

            # --- LOGGING ---
            rolls_str = ' + '.join(map(str, base_rolls))
            modifier_str = f" + {modifier}" if modifier > 0 else f" - {abs(modifier)}" if modifier < 0 else ""
            bonus_str = f" + {skill_bonus} bonus" if skill_bonus != 0 else ""
            log_entry = f"**{horse.name}**: {rolls_str}{modifier_str}{bonus_str} = **{total_movement}**."
            if extra_movement > 0: log_entry += f" Surged **+{extra_movement}**!"
            if skill_activations: log_entry += f" _Skills: {', '.join(skill_activations)}_"
            self.log.append(log_entry)
            self.full_race_log.append(log_entry)
            # Capture structured data for the master event log
            turn_data = {
                'round': self.round_number, 'horse_name': horse.name,
                'rolls_str': rolls_str, 'modifier': modifier, 'skill_bonus': skill_bonus,
                'total_movement': total_movement, 'skill_roll': skill_roll, 'skill_chance': skill_chance,
                'skills_activated': ", ".join(skill_activations) if skill_activations else "—",
                'position_after': horse.position
            }
            self.structured_log.append(turn_data)

        # Sort horses by position for display
        self.horses.sort(key=lambda h: h.position, reverse=True)