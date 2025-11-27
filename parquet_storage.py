import pandas as pd
import numpy as np
import shutil
import os
import time


class ParquetManager:
    def __init__(self, output_dir="market_data_parquet"):
        self.output_dir = output_dir

    def save_to_parquet(self, df):
        """
        Converts the dataframe to Parquet, partitioned by ticker.
        """
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        print("Saving to Parquet (partitioned by ticker)...")
        df.to_parquet(
            self.output_dir,
            engine='pyarrow',
            partition_cols=['ticker'],
            compression='snappy'
        )
        print("Parquet save complete.")

    def task_1_aapl_rolling(self):
        """
        Load AAPL data and compute 5-minute rolling average of close price.
        """
        # Load only AAPL partition
        try:
            df = pd.read_parquet(self.output_dir, filters=[('ticker', '=', 'AAPL')])
        except Exception:
            # Fallback if filters fail or older pyarrow version
            df = pd.read_parquet(self.output_dir)
            df = df[df['ticker'] == 'AAPL']

        df = df.sort_values('timestamp')

        # 5-minute rolling average
        # Since data is 1-min frequency (based on your screenshot), window=5 covers 5 minutes
        df['rolling_5m_close'] = df['close'].rolling(window=5).mean()

        return df[['timestamp', 'close', 'rolling_5m_close']].dropna().head(10)

    def task_2_rolling_volatility(self):
        """
        Compute 5-day rolling volatility (std dev) of returns for each ticker.
        Strategy: Resample to Daily -> Calculate Daily Returns -> Rolling(5) Std Dev
        """
        df = pd.read_parquet(self.output_dir)

        # 1. Resample to Daily Close per Ticker
        daily_df = df.set_index('timestamp').groupby('ticker')['close'].resample('D').last().reset_index()

        # 2. Calculate Daily Returns
        daily_df['daily_return'] = daily_df.groupby('ticker')['close'].pct_change()

        # 3. Calculate 5-Day Rolling Volatility
        daily_df['rolling_5d_vol'] = daily_df.groupby('ticker')['daily_return'].transform(
            lambda x: x.rolling(window=5).std()
        )

        return daily_df.dropna().head(10)

    def task_3_compare_performance(self, sqlite_mgr):
        """
        Compare query time for Task 1 (Get AAPL data) between SQLite and Parquet.
        """
        print("\n--- Performance Comparison (Load AAPL Data) ---")

        # Measure SQLite
        start_time = time.time()
        # Retrieve AAPL data via SQL (filtering manually constructed in SQL manager usually,
        # but here we reuse the logic or just run a raw query for fairness)
        conn = sqlite_mgr._get_conn()
        _ = pd.read_sql_query("SELECT * FROM prices p JOIN tickers t ON p.ticker_id=t.ticker_id WHERE t.symbol='AAPL'",
                              conn)
        conn.close()
        sqlite_duration = time.time() - start_time

        # Measure Parquet
        start_time = time.time()
        _ = pd.read_parquet(self.output_dir, filters=[('ticker', '=', 'AAPL')])
        parquet_duration = time.time() - start_time

        print(f"SQLite Time:  {sqlite_duration:.5f} seconds")
        print(f"Parquet Time: {parquet_duration:.5f} seconds")

        # Compare File Sizes
        sqlite_size = os.path.getsize(sqlite_mgr.db_name) / (1024 * 1024)

        parquet_size = 0
        for root, dirs, files in os.walk(self.output_dir):
            for f in files:
                parquet_size += os.path.getsize(os.path.join(root, f))
        parquet_size = parquet_size / (1024 * 1024)

        print(f"SQLite Size:  {sqlite_size:.2f} MB")
        print(f"Parquet Size: {parquet_size:.2f} MB")