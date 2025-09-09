# Copilot Instructions for prettyDerbyClubAnalysis

This document provides guidance for AI agents working in the `prettyDerbyClubAnalysis` codebase.

## Project Overview & Architecture

This project is a complex economic simulation, likely for a Discord community. The core of the project is a "Fan Exchange" stock market where members' stocks are traded, driven by performance metrics scraped from an external source.

The architecture is composed of four main parts:

1.  **Data Collection (`dataGet.py`)**: This script is the entry point for all data. It uses screen scraping (PyAutoGUI) and Optical Character Recognition (Tesseract OCR) to read member fan counts from a GUI application. It writes this raw data to `fan_log.csv`.

2.  **Orchestration & Enrichment (`analysis.py`)**: This is the heartbeat of the project. It runs on a schedule to:
    a. Read the raw `fan_log.csv`.
    b. Enrich the data with prestige calculations and other metrics, saving the result to `enriched_fan_log.csv`.
    c. Trigger the core market logic to update prices and process earnings.
    d. Save the new state of the market to the database.

3.  **Core Logic Layer**:
    *   **Market (`market/engine.py`, `market/economy.py`)**: This is the heart of the stock market simulation. `market/engine.py` contains the algorithms for calculating stock prices based on member prestige and fan gain. `market/economy.py` calculates user earnings from their performance. Both operate on pandas DataFrames and are decoupled from the database.
    *   **Reporting (`generate_visuals.py`)**: This script takes the enriched data and generates performance reports, leaderboards, and other visualizations as images. These images are a critical output used by the Discord bot.

4.  **Data & Application Layer**:
    *   **Database (`market/database.py`)**: A PostgreSQL database is the primary data store for all market transactions, user wallets, and stock prices. The `market/database.py` script is the sole Data Access Layer (DAL). All database interactions **must** go through this module.
    *   **Discord Bot (`bot.py`)**: This is the main user-facing application. It handles user commands for trading and viewing portfolios. It uses the DAL in `market/database.py` to execute transactions and serves the images created by `generate_visuals.py` to display performance reports.

## Key Data Flow

The primary data flow follows a consistent pattern orchestrated by `race_day_scheduler.py`, which runs `analysis.py`:

1.  **Collect**: `dataGet.py` scrapes screen data and populates `fan_log.csv`.
2.  **Enrich**: `analysis.py` reads `fan_log.csv`, calculates prestige and other metrics, and creates `enriched_fan_log.csv`.
3.  **Fetch DB State**: `analysis.py` loads the current market state (wallets, prices) from the PostgreSQL database via `market/database.py`.
4.  **Process Market**: The enriched data and DB state are passed to the `market/engine.py` and `market/economy.py` modules to calculate new stock prices and user earnings.
5.  **Save Market State**: `analysis.py` persists the updated DataFrames (new prices, new balances) back to the database using functions from `market/database.py`.
6.  **Generate Visuals**: After the analysis is complete, `generate_visuals.py` is run to create updated charts and tables for the bot to use.

This pattern enforces a clean separation of concerns. When making changes, respect this flow.

## Developer Workflow & Conventions

*   **Environment Setup**: Before running any script that accesses the database, you must create a `.env` file in the root directory with the following PostgreSQL connection details:
    ```
    DB_HOST=your_host
    DB_PORT=your_port
    DB_USER=your_user
    DB_PASSWORD=your_password
    DB_NAME=your_database
    ```

*   **Database Initialization**: To set up the database schema for the first time, run the `database.py` script directly:
    ```bash
    python market/database.py
    ```

*   **Running the Full Pipeline**: The entire data and analysis pipeline is managed by `race_day_scheduler.py`. To run a single, complete cycle of data collection, analysis, and visualization, use:
    ```bash
    python race_day_scheduler.py full_run_once
    ```
    To run the scheduler continuously as it would in production:
    ```bash
    python race_day_scheduler.py full_run
    ```

*   **Atomic Transactions**: For operations that require multiple database steps (like a trade), use the specific transaction functions in `market/database.py` (e.g., `execute_trade_transaction`). These functions ensure data integrity.

## Key Files & Directories

*   `race_day_scheduler.py`: The main scheduler that automates the entire project pipeline.
*   `dataGet.py`: The starting point for all data collection.
*   `analysis.py`: The central orchestrator for data enrichment and market simulation.
*   `generate_visuals.py`: Generates all performance reports and charts for the bot.
*   `market/database.py`: The **only** place for database interaction. Contains the schema and all CRUD functions.
*   `market/engine.py`: The core stock price calculation logic.
*   `market/economy.py`: The core user earnings calculation logic.
*   `bot.py`: The entry point and command handler for the Discord bot.
*   `fan_log.csv`: Raw data output from `dataGet.py`.
*   `enriched_fan_log.csv`: Processed data from `analysis.py`, used as the primary input for the market engine and visual generation.
*   `initialize_market.py`, `reset_and_migrate.py`: Administrative scripts for managing the market state.
