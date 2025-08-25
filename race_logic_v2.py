import json
import random

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