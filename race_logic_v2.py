import json
import random
import numpy as np
from copy import deepcopy

class Horse:
    """
    Represents a procedurally generated horse with a unique statistical profile
    derived from its name and a central configuration file.
    """
    
    def __init__(self, name: str, strategy: str):
        """
        Initializes a Horse object.

        Args:
            name (str): The full name of the horse (e.g., "Iron Fury").
            strategy (str): The racing strategy (e.g., "Front Runner").
        """
        self.name = name
        self.strategy_name = strategy
        self.stats = {} # Will hold the final calculated stats

        # Load the configuration file that defines all attributes
        with open('market/configs/horse_attributes.json', 'r') as f:
            self.attributes_config = json.load(f)
            
        self._calculate_stats()

    def _calculate_stats(self):
        """
        Parses the horse's name and the attributes configuration to calculate
        the final mean (μ) and sigma (σ) for each phase of the race.
        """
        try:
            adjective, noun = self.name.split(' ', 1)
        except ValueError:
            print(f"Warning: Could not parse name '{self.name}'. Using default modifiers.")
            adjective, noun = None, None

        # 1. Get base stats from the horse's strategy
        strategy_stats = self.attributes_config['strategies'][self.strategy_name]

        # 2. Get modifiers from the adjective and noun
        adj_mod = self.attributes_config['adjectives'].get(adjective, {"sigma_modifier": 0})
        noun_mod = self.attributes_config['nouns'].get(noun, {"mean_modifier": 0, "preferred_distance": 2400})

        # 3. Calculate final stats for each race phase
        for phase in ['early_race', 'mid_race', 'late_race']:
            base_mean = strategy_stats[phase]['mean']
            base_sigma = strategy_stats[phase]['sigma']
            
            final_mean = base_mean + noun_mod['mean_modifier']
            final_sigma = base_sigma + adj_mod['sigma_modifier']
            
            self.stats[phase] = {
                "mean": final_mean,
                "sigma": max(1, final_sigma) # Ensure sigma is at least 1 to avoid statistical errors
            }
        
        # 4. Store preferred distance
        self.preferred_distance = noun_mod['preferred_distance']

    def display_stats(self):
        """A helper method for debugging and verification."""
        print(f"--- Stats for: {self.name} ({self.strategy_name}) ---")
        print(f"Preferred Distance: {self.preferred_distance}m")
        for phase, values in self.stats.items():
            print(f"  {phase.title()}: μ = {values['mean']}, σ = {values['sigma']}")
        print("-" * 20)
        
# # --- Test Code ---
# horse1 = Horse("Iron Fury", "Pace Chaser")
# horse1.display_stats()

# horse2 = Horse("Broken Bullet", "Front Runner")
# horse2.display_stats()

class Race:
    """
    Manages a single race simulation, including the track, the horses,
    and the round-by-round progression.
    """
    
    def __init__(self, horses: list, distance: int = 2400):
        """
        Initializes a Race object.

        Args:
            horses (list): A list of Horse objects participating in the race.
            distance (int): The total distance of the race in meters.
        """
        self.horses = horses
        self.distance = distance
        self.track_segments = 24
        self.segment_length = self.distance / self.track_segments
        
        self.round_number = 0
        self.positions = {horse.name: 0 for horse in self.horses}
        self.log = [] # To store a text log of the race events

    def _get_current_phase(self) -> str:
        """Determines the current phase of the race based on segments covered."""
        # Note: Segments are 1-based for the design doc, 0-based in code is easier.
        # A 24-segment track has segments 0-23.
        current_segment = self.round_number
        if current_segment <= 3: # Segments 1-4
            return 'early_race'
        elif current_segment <= 15: # Segments 5-16
            return 'mid_race'
        else: # Segments 17-24
            return 'late_race'

    def _calculate_distance_penalty(self, horse: Horse) -> float:
        """
        Calculates the performance penalty based on the difference between
        the race distance and the horse's preferred distance.
        
        Penalty: -5% to final mean for every 400m of difference.
        """
        distance_diff = abs(self.distance - horse.preferred_distance)
        penalty_intervals = distance_diff // 400
        penalty_percentage = penalty_intervals * 0.05
        return 1.0 - penalty_percentage # Returns a multiplier, e.g., 0.95

    def run_round(self):
        """Simulates a single round of the race for all horses."""
        self.round_number += 1
        current_phase = self._get_current_phase()
        self.log.append(f"--- Round {self.round_number} ({current_phase.replace('_', ' ').title()}) ---")

        for horse in self.horses:
            stats = horse.stats[current_phase]
            base_mean = stats['mean']
            sigma = stats['sigma']
            
            # Apply the distance suitability penalty to the mean
            distance_multiplier = self._calculate_distance_penalty(horse)
            adjusted_mean = base_mean * distance_multiplier
            
            # Draw movement from the Normal Distribution
            movement = np.random.normal(loc=adjusted_mean, scale=sigma)
            movement = max(0, movement) # A horse cannot move backwards
            
            self.positions[horse.name] += movement
            self.log.append(f"{horse.name} moves {movement:.0f}m.")

    def is_finished(self) -> bool:
        """
        Checks if the race is complete. The race is considered finished
        once at least 5 horses have crossed the finish line.
        """
        finishers = [h for h in self.horses if self.positions[h.name] >= self.distance]
        return len(finishers) >= 5

    def get_results(self) -> list:
        """Returns a sorted list of finishers."""
        # Sort horses by their final position, descending
        sorted_results = sorted(self.horses, key=lambda h: self.positions[h.name], reverse=True)
        return sorted_results
    
# --- Race Test Code ---
# (Assume the Horse class and some test horses are defined)
# test_horses = [Horse("Iron Fury", "Pace Chaser"), Horse("Broken Bullet", "Front Runner"), Horse("Nice Nature", "Late Surger"), Horse("Broken Cheater", "End Closer"), Horse("Inspector Gadget", "Pace Chaser"), Horse("Thunder Wave", "End Closer")]
# race = Race(test_horses, distance=1600)

# while not race.is_finished():
#     race.run_round()

# print("\n--- RACE FINISHED ---")
# results = race.get_results()
# for i, horse in enumerate(results):
#     print(f"{i+1}. {horse.name} - Final Position: {race.positions[horse.name]:.0f}m")

# # Optional: Print the full log to see the round-by-round action
# print("\n--- Full Race Log ---")
# for entry in race.log:
#     print(entry)

import json
import random
import numpy as np
from copy import deepcopy # Needed for Monte Carlo simulation

# (The Horse and Race classes from the previous epic remain here)

class Bookie:
    """
    Manages the odds and betting for a given race, acting as the house.
    """
    
    def __init__(self, race_to_manage, house_vig=0.08):
        """
        Initializes the Bookie.

        Args:
            race_to_manage (Race): The Race object this bookie is managing.
            house_vig (float): The house edge or "vig" (e.g., 0.08 for 8%).
        """
        self.race = race_to_manage
        self.house_vig = house_vig
        self.morning_line_odds = {}
        self.bets = [] # Using an in-memory list as discussed
        self.total_liability = {horse.name: 0 for horse in self.race.horses}

    def _calculate_odds_from_win_rate(self, win_rate: float) -> float:
        """
        Converts a win probability into fractional betting odds,
        including the house vig.
        """
        if win_rate == 0:
            return 10001 # A horse that never wins has infinite odds
        
        # Fair odds are 1 / probability
        fair_odds = (1 / win_rate) - 1
        
        # Add the house edge
        final_odds = fair_odds * (1 - self.house_vig)
        
        return max(0.1, final_odds) # Ensure odds are never zero or negative

    def run_monte_carlo(self, simulations: int = 10000):
        """
        Runs a Monte Carlo simulation to determine the win probabilities
        for each horse and sets the morning line odds.
        """
        print(f"Running Monte Carlo simulation with {simulations} iterations...")
        win_counts = {horse.name: 0 for horse in self.race.horses}

        for i in range(simulations):
            # Create a deep copy of the race to run an independent simulation
            sim_race = deepcopy(self.race)
            
            while not sim_race.is_finished():
                sim_race.run_round()
            
            # The winner is the first horse in the sorted results list
            winner = sim_race.get_results()[0]
            win_counts[winner.name] += 1
            
            if (i + 1) % 1000 == 0:
                print(f"  ...completed {i + 1}/{simulations} simulations.")

        # Calculate win rates and convert to odds
        for horse_name, wins in win_counts.items():
            win_rate = wins / simulations
            odds = self._calculate_odds_from_win_rate(win_rate)
            self.morning_line_odds[horse_name] = {
                "win_rate": win_rate,
                "odds": odds
            }
        
        print("Monte Carlo simulation complete. Morning line odds are set.")
        
    def place_bet(self, bettor_id: str, horse_name: str, amount: int):
        if horse_name not in self.morning_line_odds:
            print(f"Error: No odds found for horse '{horse_name}'.")
            return None
        locked_in_odds = self.morning_line_odds[horse_name]['odds']
        bet_details = {
            "bettor_id": bettor_id,
            "horse_name": horse_name,
            "amount": amount,
            "locked_in_odds": locked_in_odds
        }
        self.bets.append(bet_details)
        potential_winnings = amount * locked_in_odds
        self.total_liability[horse_name] += potential_winnings
        print(f"Bet placed: {bettor_id} bets {amount} CC on {horse_name} at {locked_in_odds:.2f} to 1.")
        return bet_details

    def calculate_payouts(self, winning_horse_name: str) -> list:
        payouts = []
        print(f"\n--- Calculating Payouts for Winner: {winning_horse_name} ---")
        winning_bets = [b for b in self.bets if b['horse_name'] == winning_horse_name]
        if not winning_bets:
            print("No winning bets were placed.")
            return payouts
        for bet in winning_bets:
            winnings = bet['amount'] * bet['locked_in_odds']
            total_return = bet['amount'] + winnings
            payouts.append({ "bettor_id": bet['bettor_id'], "winnings": total_return })
            print(f"  - {bet['bettor_id']} wins {total_return:,.2f} CC (initial {bet['amount']} CC bet).")
        return payouts
        
# # --- Simulation Test Code ---
# test_horses = [Horse("Iron Fury", "Pace Chaser"), Horse("BOT Bullet", "Front Runner"), Horse("Nice Nature", "Late Surger"), Horse("Broken Cheater", "End Closer"), Horse("Inspector Gadget", "Pace Chaser"), Horse("Thunder Wave", "End Closer")]
# for horse in test_horses:
#     horse.display_stats()
# race = Race(test_horses, distance=1600)

# bookie = Bookie(race)
# bookie.run_monte_carlo(simulations=10000) # Use a lower number for faster testing if needed

# print("\n--- Morning Line Odds ---")
# for name, data in bookie.morning_line_odds.items():
#     # Fractional odds are often displayed as "X to 1"
#     print(f"{name}: {data['odds']:.2f} to 1 (Win Rate: {data['win_rate']:.2%})")