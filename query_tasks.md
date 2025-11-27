# Query Tasks

## SQLite3

1. Retrieve all data for TSLA between 2025-11-17 and 2025-11-18.
2. Calculate average daily volume per ticker.
3. Identify top 3 tickers by return over the full period.
4. Find first and last trade price for each ticker per day.

## Parquet

1. Load all data for AAPL and compute 5-minute rolling average of close price.
2. Compute 5-day rolling volatility (std dev) of returns for each ticker.
3. Compare query time and file size with SQLite3 for Task 1.