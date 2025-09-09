# Pretty Derby Club Analysis & Fan Exchange

This project is a comprehensive economic and performance analysis simulation built for a Discord community. It features a dynamic "Fan Exchange" stock market, where the value of members' stocks is tied to their in-game performance metrics.

## Core Features

-   **Fan Exchange Stock Market**: A fully-featured stock market where users can buy and sell shares of fellow club members.
-   **Dynamic Stock Pricing**: Stock prices are not arbitrary; they are calculated based on a proprietary algorithm that factors in member performance (fan gain), tenure, and overall market sentiment.
-   **Performance-Based Earnings**: Users earn in-game currency (`CC`) based on their performance, which they can use to invest in the market.
-   **Automated Data Pipeline**: The system automatically collects data, runs analysis, updates the market, and generates reports on a regular schedule.
-   **Discord Bot Integration**: A robust Discord bot serves as the primary user interface for all market interactions, including trading, portfolio management, and viewing financial reports.
-   **Data Visualization**: The project generates a suite of visual reports, including leaderboards, performance summaries, and historical data charts, which are posted directly to Discord.

## Architecture Overview

The project is built with a clear, multi-layered architecture to separate concerns:

1.  **Data Collection (`dataGet.py`)**: A Python script using screen scraping (PyAutoGUI) and OCR (Tesseract) to capture raw performance data from a GUI application, saving it to `fan_log.csv`.
2.  **Orchestration & Analysis (`analysis.py`)**: The central engine that reads the raw data, enriches it with calculated metrics (like prestige), and orchestrates the entire market update cycle.
3.  **Core Logic (`market/`)**:
    -   `engine.py`: Contains the core algorithms for stock price calculation.
    -   `economy.py`: Calculates user earnings based on their performance data.
4.  **Data Persistence (`market/database.py`)**: The sole Data Access Layer (DAL) that manages all interactions with the PostgreSQL database, where all user wallets, portfolios, and market states are stored.
5.  **User Interface (`bot.py`)**: The Discord bot that handles all user commands and presents data and reports to the community.

## Getting Started

To get the project running locally, follow these steps:

### Prerequisites

-   Python 3.x
-   PostgreSQL Server
-   Tesseract OCR

### Installation & Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd prettyDerbyClubAnalysis
    ```

2.  **Install dependencies:**
    It is recommended to create a virtual environment.
    ```bash
    pip install -r requirements.txt
    ```
    *(Note: A `requirements.txt` file may need to be generated if it's not present.)*

3.  **Set up environment variables:**
    Create a `.env` file in the root directory and add your PostgreSQL and Discord bot credentials:
    ```
    DB_HOST=your_host
    DB_PORT=your_port
    DB_USER=your_user
    DB_PASSWORD=your_password
    DB_NAME=your_database
    DISCORD_TOKEN=your_discord_bot_token
    ```

4.  **Initialize the database:**
    Run the database script to create all the necessary tables.
    ```bash
    python market/database.py
    ```

## Usage

The entire data pipeline is managed by the `race_day_scheduler.py` script.

-   **To run a single, complete cycle** (data collection, analysis, and visualization):
    ```bash
    python race_day_scheduler.py full_run_once
    ```

-   **To run the scheduler continuously** as it would run in production:
    ```bash
    python race_day_scheduler.py full_run
    ```

-   **To start the Discord bot**:
    ```bash
    python bot.py
    ```
    