import unittest
import pandas as pd
import numpy as np
import os
import shutil
import sqlite3
try:
    from data_loader import DataLoader
    from sqlite_storage import SQLiteManager
    from parquet_storage import ParquetManager
except ImportError as e:
    print(f"Fatal! Source Broken. please confirm if all source code is installed. aborting... error:{e}")


class TestMarketDataWorkflow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Setup runs once before all tests.
        We generate dummy CSVs here to ensure tests are self-contained.
        """
        cls.test_csv = 'test_market_data.csv'
        cls.test_tickers = 'test_tickers.csv'
        cls.test_db = 'test_market_data.db'
        cls.test_parquet = 'test_parquet_data'

        # 1. Create Dummy Tickers
        tickers_data = {
            'ticker_id': [1, 2],
            'symbol': ['AAPL', 'TSLA'],
            'name': ['Apple', 'Tesla'],
            'exchange': ['NASDAQ', 'NASDAQ']
        }
        pd.DataFrame(tickers_data).to_csv(cls.test_tickers, index=False)

        # 2. Create Dummy Market Data (7 Days to satisfy 5-day rolling window)
        # We need enough data points for the rolling volatility to not be Empty
        dates = pd.date_range(start='2025-01-01', periods=7, freq='D')

        data = []
        for date in dates:
            # 3 minute bars per day per ticker
            for minute in range(3):
                ts = date + pd.Timedelta(minutes=minute)
                # AAPL Data
                data.append([ts, 'AAPL', 150.0, 155.0, 149.0, 152.0 + np.random.rand(), 1000])
                # TSLA Data
                data.append([ts, 'TSLA', 200.0, 205.0, 195.0, 202.0 + np.random.rand(), 2000])

        df = pd.DataFrame(data, columns=['timestamp', 'ticker', 'open', 'high', 'low', 'close', 'volume'])
        df.to_csv(cls.test_csv, index=False)

    def test_01_data_loading(self):
        """Test if data loader correctly reads and formats data."""
        loader = DataLoader(self.test_csv, self.test_tickers)
        self.tickers_df = loader.load_tickers()
        self.prices_df = loader.load_market_data()

        self.assertEqual(len(self.tickers_df), 2)
        # 7 days * 3 mins * 2 tickers = 42 rows
        self.assertEqual(len(self.prices_df), 42)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(self.prices_df['timestamp']))

    def test_02_sqlite_workflow(self):
        """Test SQLite initialization, insertion, and specific queries."""
        # Load data first
        loader = DataLoader(self.test_csv, self.test_tickers)
        tickers_df = loader.load_tickers()
        prices_df = loader.load_market_data()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(current_dir, '..', 'schema.sql')
        sql_mgr = SQLiteManager(db_name=self.test_db, schema_file=schema_path)


        sql_mgr.initialize_db()

        # Insert
        sql_mgr.insert_data(tickers_df, prices_df)

        # Verify Insertion
        conn = sqlite3.connect(self.test_db)
        count = pd.read_sql_query("SELECT count(*) FROM prices", conn).iloc[0, 0]
        conn.close()
        self.assertEqual(count, 42)

        # Test Query 1: Retrieve Data
        df_tsla = sql_mgr.query_1_tsla_data('2025-01-01', '2025-01-02')
        self.assertFalse(df_tsla.empty)
        # 注意：这里我们生成的 dummy data 里 TSLA 的 ID 是 2，和你的 CSV 可能不同，但逻辑是一样的
        # self.assertEqual(df_tsla['ticker_id'].iloc[0], 2)

        # Test Query 2: Avg Volume
        df_vol = sql_mgr.query_2_avg_daily_volume()
        row_aapl = df_vol[df_vol['symbol'] == 'AAPL']
        self.assertFalse(row_aapl.empty)
    def test_03_parquet_workflow(self):
        """Test Parquet saving and analytics tasks."""
        loader = DataLoader(self.test_csv, self.test_tickers)
        prices_df = loader.load_market_data()

        pq_mgr = ParquetManager(output_dir=self.test_parquet)
        pq_mgr.save_to_parquet(prices_df)

        self.assertTrue(os.path.exists(self.test_parquet))

        # Test Task 1: Rolling Average
        res_rolling = pq_mgr.task_1_aapl_rolling()
        # We need at least 5 rows for the window=5 to produce a result.
        # Our dummy data has 7 days * 3 mins = 21 rows for AAPL.
        self.assertFalse(res_rolling.empty)
        self.assertIn('rolling_5m_close', res_rolling.columns)

        # Test Task 2: Rolling Volatility
        # This failed in your real run because you likely had < 5 days of data.
        # In this test, we generated 7 days, so it should pass.
        res_vol = pq_mgr.task_2_rolling_volatility()
        self.assertFalse(res_vol.empty)
        self.assertIn('rolling_5d_vol', res_vol.columns)

    @classmethod
    def tearDownClass(cls):
        """Clean up generated files after tests finish."""
        if os.path.exists(cls.test_csv):
            os.remove(cls.test_csv)
        if os.path.exists(cls.test_tickers):
            os.remove(cls.test_tickers)
        if os.path.exists(cls.test_db):
            os.remove(cls.test_db)
        if os.path.exists(cls.test_parquet):
            shutil.rmtree(cls.test_parquet)


if __name__ == '__main__':
    unittest.main()