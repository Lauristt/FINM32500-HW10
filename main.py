try:
    from data_loader import DataLoader
    from sqlite_storage import SQLiteManager
    from parquet_storage import ParquetManager
except ImportError as e:
    print(f"Fatal! Source Broken. please confirm if all source code is installed. aborting... error:{e}")

def main():
    #1.Load Data
    print("1. Data Loading")
    loader = DataLoader()  # Defaults to 'market_data_multi.csv' and 'tickers.csv'
    try:
        tickers_df = loader.load_tickers()
        prices_df = loader.load_market_data()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure 'market_data_multi.csv' and 'tickers.csv' are in the folder.")
        return

    #2. SQLite Operations
    print("\n2. SQLite Tasks")
    sql_mgr = SQLiteManager()

    # Initialize and Insert (Only needed once, but safe to run if handled correctly)
    # Note: schema.sql has DROP TABLE IF EXISTS, so this resets the DB every run
    sql_mgr.initialize_db()
    sql_mgr.insert_data(tickers_df, prices_df)

    #Task 1: TSLA Data
    print("\n[SQL Task 1] TSLA Data (2025-11-17 to 2025-11-18):")
    tsla_data = sql_mgr.query_1_tsla_data('2025-11-17', '2025-11-18')
    print(tsla_data.head())

    #Task 2: Avg Daily Volume
    print("\n[SQL Task 2] Average Daily Volume:")
    print(sql_mgr.query_2_avg_daily_volume())

    #Task 3: Top 3 Tickers by Return
    print("\n[SQL Task 3] Top 3 Tickers by Return:")
    print(sql_mgr.query_3_top_tickers_return())

    # Task 4: First/Last Trade Per Day
    print("\n[SQL Task 4] First and Last Trade per Day (First 5 rows):")
    print(sql_mgr.query_4_first_last_daily().head())

    #parquet operations
    print("\n3. Parquet Tasks")
    pq_mgr = ParquetManager()
    pq_mgr.save_to_parquet(prices_df)

    #Task 1: AAPL 5-min Rolling
    print("\n[Parquet Task 1] AAPL 5-min Rolling Average (First 5 rows):")
    print(pq_mgr.task_1_aapl_rolling())

    #Task 2: 5-Day Rolling Volatility
    print("\n[Parquet Task 2] 5-Day Rolling Volatility (First 5 rows):")
    print(pq_mgr.task_2_rolling_volatility())

    #Task 3: Comparison
    pq_mgr.task_3_compare_performance(sql_mgr)


if __name__ == "__main__":
    main()