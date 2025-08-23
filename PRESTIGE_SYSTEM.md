# The "Road to Legend" Club Prestige System: A Dual-Track Approach

## 1. System Overview

The "Road to Legend" is a permanent progression system designed for the "Cash Crew Fans" club. It has been updated to a **dual-track system** to better reward both short-term, competitive performance and long-term member loyalty. This system consists of two distinct components: **Monthly Ranks** and **Lifetime Prestige**.

### Monthly Ranks
- **Function:** A competitive system that measures a member's performance and tenure within a specific "in-game month." This is your **monthly score**.
- **Reset Cadence:** This score resets to zero for all members at the beginning of each new in-game month, allowing everyone a fresh start to compete on the leaderboards.
- **Purpose:** To drive monthly engagement, create regular competition, and recognize the top performers for the current period. Your visible Discord role and rank (e.g., "Derby Winner") are determined by this score.

### Lifetime Prestige
- **Function:** A permanent, cumulative score that reflects a member's total contributions over their entire history with the club. This is your **career score**.
- **Reset Cadence:** This score **never resets**. It is a running total of all prestige you have ever earned.
- **Purpose:** To recognize and reward long-term loyalty and create a sense of permanent progression. This score is used to determine the cost of purchasing prestige bundles from the shop, making it a core part of your economic standing in the club.

The system is designed to be:

- **Engaging:** With both a resetting monthly competition and a rewarding long-term progression path.
- **Clear:** Using a simple "100-point" daily benchmark for solid performance that contributes to both your monthly and lifetime scores.
- **Thematic:** Based on the real-world progression of horse racing awards, from medals to legendary trophies.

## 2. The Prestige Formula: "Core Contributor"

The daily prestige calculation is anchored to a benchmark where a member on pace for 20 million fans per month earns approximately 100 points per day. This `prestigeGain` is added to both your `monthlyPrestige` and `lifetimePrestige` scores.

- **Performance Points:** 1 Prestige Point for every 8,333 fans gained.
- **Tenure Points:** A flat 20 Prestige Points awarded to every active member, for each day of tenure.

**Example Calculation:**
A member who gains 667,000 fans in a day earns:
- `667,000 / 8,333` = **80.0 Performance Points**
- **20 Tenure Points**
- **Total `prestigeGain`**: 100 Prestige Points

This 100-point gain would be added to both their `monthlyPrestige` for the current month's ranking and their permanent `lifetimePrestige`.

## 3. Rank Structure & Requirements

The system is divided into four distinct "Eras" of achievement, each with its own visual theme. **Your rank is determined by your `monthlyPrestige` score and will reset at the end of each in-game month.**

| Era      | Rank Tier (Name)        | Prestige Points Required (for the month) |
|----------|-------------------------|------------------------------------------|
| Medals   | 1. Local Newcomer       | 900                                      |
|          | 2. Track Regular        | 1,560                                    |
|          | 3. Podium Finisher      | 2,280                                    |
| Ribbons  | 4. Stakes Contender     | 3,000                                    |
|          | 5. Derby Winner         | 3,960                                   |
|          | 6. Grand Prix Champion  | 4,800                                   |
| Trophies | 7. Grand Cup Holder     | 5,640                                   |
|          | 8. Champion Cup Holder  | 6,600                                   |
|          | 9. Triple Crown Winner  | 7,740                                  |
| Pantheon | 10. Hall of Fame Inductee| 8,400                                  |
|          | 11. Racing Legend       | 8,400                                  |
|          | 12. The Founder's Idol  | 12,600                                  |

## 4. Data & Automation Flow

The system operates on a simple, daily data pipeline:

- **Data Collection:** The existing club data script runs, collecting the latest fan counts and creating an up-to-date `fan_log.csv`.
- **Prestige Calculation:** The `analysis.py` script is run daily. It reads `fan_log.csv`, calculates the `prestigeGain` for every member, and updates both the `monthlyPrestige` and `lifetimePrestige` columns in the `enriched_fan_log.csv`.
- **Discord Bot Automation:** The `bot.py` reads the updated `enriched_fan_log.csv`.
  - **Commands:** When a user runs commands like `/myprogress` or `/prestige_leaderboard`, the bot reads the CSV to provide real-time information.
  - **Daily Role Update:** Once per day, a scheduled task in the bot iterates through all server members, checks their `monthlyPrestige` from the CSV, and automatically assigns the correct Discord role, removing any old ones.

## 5. Bot Commands

- **/myprogress**: Checks your progress, showing both your monthly and lifetime prestige scores.
- **/prestige_leaderboard**: Displays the top members by monthly prestige points.
- **/charts**: Posts the latest performance charts generated by the main analysis script.