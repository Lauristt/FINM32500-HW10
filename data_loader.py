import pandas as pd
import os


class DataLoader:
    def __init__(self, data_path='market_data_multi.csv', tickers_path='tickers.csv'):
        self.data_path = data_path
        self.tickers_path = tickers_path

    def load_tickers(self):
        """
        Loads the tickers.csv file.
        Expected columns: ticker_id, symbol, name, exchange
        """
        if not os.path.exists(self.tickers_path):
            raise FileNotFoundError(f"{self.tickers_path} not found.")

        df = pd.read_csv(self.tickers_path)
        print(f"Loaded {len(df)} tickers.")
        return df

    def load_market_data(self):
        """
        Loads the market_data_multi.csv file.
        Expected columns: timestamp, ticker, open, high, low, close, volume
        """
        if not os.path.exists(self.data_path):
            raise FileNotFoundError(f"{self.data_path} not found.")

        df = pd.read_csv(self.data_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        #override original names
        df.columns = [c.strip() for c in df.columns]
        print(f"Loaded {len(df)} market data rows.")
        return df
