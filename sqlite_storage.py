import sqlite3
import pandas as pd
import os


class SQLiteManager:
    def __init__(self, db_name = 'market_data.db', schema_file = 'schema.sql'):
        self.db_name=db_name
        self.schema_file = schema_file
    def _get_conn(self):
        return sqlite3.connect(self.db_name) # return type check: sqlite3.Connection

    def initialize_db(self):
        # create tables, sql file here should be able to create the table
        if not os.path.exists(self.schema_file):
            raise FileNotFoundError("schema.sql not found in folder. aborting..")
        with open(self.schema_file, 'r') as f:
            schema_sql=f.read()
        conn = self._get_conn()
        conn.executescript(schema_sql)
        conn.close()
        print("Database initialized from schema.sql.")

    def insert_data(self,tickers_df, prices_df):
        conn = self._get_conn()
        # insert ticker data into the table
        tickers_df[['ticker_id','symbol','name','exchange']].to_sql(
            'tickers',conn,if_exists='append',index=False
        )
        print("Tickers inserted.")

        # prep price df
        ## create a dictionary map:{'aapl':1,",sft": 2}
        symbol_to_id = dict(zip(tickers_df['symbol'], tickers_df['ticker_id']))
        df_insert = prices_df.copy()
        df_insert['ticker_id'] = df_insert['ticker'].map(symbol_to_id)

        # select columns matching schema.sql
        cols_to_insert = ['timestamp', 'ticker_id', 'open', 'high', 'low', 'close', 'volume']
        df_insert = df_insert[cols_to_insert]

        # Convert timestamp to string for SQLite storage
        df_insert['timestamp'] = df_insert['timestamp'].astype(str)

        df_insert.to_sql('prices', conn, if_exists='append', index=False)
        print(f"Inserted {len(df_insert)} price rows.")
        conn.close()

    def query_1_tsla_data(self,start_date, end_date):
        """Retrieve all data for TSLA between given dates"""
        query = """
                    SELECT p.* FROM prices p
                    JOIN tickers t ON p.ticker_id = t.ticker_id
                    WHERE t.symbol = 'TSLA' 
                    AND p.timestamp BETWEEN ? AND ?
                """
        conn = self._get_conn()# connect to the database
        start_str = f"{start_date} 00:00:00"
        end_str = f"{end_date} 23:59:59"
        df=pd.read_sql_query(query, conn, params = (start_str, end_str))
        conn.close()
        return df

    def query_2_avg_daily_volume(self):
        query = """
                    WITH daily_volume AS (
                        SELECT 
                            t.symbol,
                            DATE(p.timestamp) as trade_date,
                            SUM(p.volume) as total_vol
                        FROM prices p
                        JOIN tickers t ON p.ticker_id = t.ticker_id
                        GROUP BY t.symbol, trade_date
                    )
                    SELECT symbol, AVG(total_vol) as avg_daily_volume
                    FROM daily_volume
                    GROUP BY symbol
                """
        conn = self._get_conn()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def query_3_top_tickers_return(self):
        """
        Identify top 3 tickers by return over the full period.
        Return = (Last Close - First Open) / First Open
        """
        query = """
            WITH ticker_stats AS (
                SELECT 
                    ticker_id,
                    MIN(timestamp) as first_time,
                    MAX(timestamp) as last_time
                FROM prices
                GROUP BY ticker_id
            ),
            first_prices AS (
                SELECT p.ticker_id, p.open as first_open
                FROM prices p
                JOIN ticker_stats ts ON p.ticker_id = ts.ticker_id AND p.timestamp = ts.first_time
            ),
            last_prices AS (
                SELECT p.ticker_id, p.close as last_close
                FROM prices p
                JOIN ticker_stats ts ON p.ticker_id = ts.ticker_id AND p.timestamp = ts.last_time
            )
            SELECT 
                t.symbol,
                ((lp.last_close - fp.first_open) / fp.first_open) * 100 as return_pct
            FROM tickers t
            JOIN first_prices fp ON t.ticker_id = fp.ticker_id
            JOIN last_prices lp ON t.ticker_id = lp.ticker_id
            ORDER BY return_pct DESC
            LIMIT 3
        """
        conn = self._get_conn()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def query_4_first_last_daily(self):
        """Find first and last trade price for each ticker per day."""
        query = """
            WITH priced AS (
                SELECT 
                    p.ticker_id,
                    t.symbol,
                    DATE(p.timestamp) AS trade_date,
                    p.timestamp,
                    p.open,
                    p.close
                FROM prices p
                JOIN tickers t ON p.ticker_id = t.ticker_id
            )
            SELECT 
                symbol,
                trade_date,
                (SELECT open 
                 FROM priced p2
                 WHERE p2.ticker_id = p1.ticker_id
                   AND p2.trade_date = p1.trade_date
                 ORDER BY p2.timestamp ASC
                 LIMIT 1) AS day_open,
                 
                (SELECT close 
                 FROM priced p3
                 WHERE p3.ticker_id = p1.ticker_id
                   AND p3.trade_date = p1.trade_date
                 ORDER BY p3.timestamp DESC
                 LIMIT 1) AS day_close
            FROM priced p1
            GROUP BY symbol, trade_date;           
        """
        conn = self._get_conn()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df






