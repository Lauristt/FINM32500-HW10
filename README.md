# Assignment 10: Market Data Storage and Querying with SQLite3 and Parquet
*FINM 32500 – University of Chicago*

![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![Manager](https://img.shields.io/badge/dependency-uv-purple)
![Status](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

A comprehensive data engineering project comparing **Relational (Row-based)** and **Columnar** storage formats for high-frequency financial data.

This project implements an End-to-End ETL pipeline that ingests raw market data, normalizes it into a **SQLite3** database for transactional querying, and partitions it into **Parquet** files for analytical workflows. It concludes with a performance benchmark comparing storage efficiency and query latency.

---

## 🚀 Features

### **1. Data Ingestion & Validation (`data_loader.py`)**
- **ETL Pipeline:** Loads raw CSV data (`market_data_multi.csv`) and reference data (`tickers.csv`).
- **Validation:** Ensures data integrity, timestamp formatting, and schema consistency before storage.

### **2. Relational Storage (`sqlite_storage.py`)**
- **Schema Design:** Implements a normalized schema (3NF) with `tickers` and `prices` tables using `schema.sql`.
- **SQL Analytics:** Executes complex SQL queries including:
  - Average Daily Volume calculations.
  - Cross-sectional returns analysis.
  - First/Last trade identification per day.

### **3. Columnar Storage (`parquet_storage.py`)**
- **Partitioning:** Efficiently stores data using PyArrow, partitioned by `Ticker`.
- **Vectorized Analytics:** Computes rolling statistics (e.g., 5-day Rolling Volatility) using Pandas/Numpy.

### **4. Performance Benchmarking**
- **Comparison:** Automatically benchmarks SQLite vs. Parquet on:
  - **Read Speed:** Query latency for specific ticker retrieval.
  - **Storage Size:** Disk space usage comparison.


### **5. Architecture Diagram**

This diagram illustrates the end-to-end ETL and storage workflow used in the project.

    CSV (Raw Market Data)
            |
            v
┌──────────────────────────┐
│   Data Loader (ETL)      │
└──────────────────────────┘
        |                |
        v                v
 SQLite3 DB        Parquet Partitioned Files
(Row-based OLTP)   (Columnar OLAP Storage)
        |                |
        └----- Benchmark Runner -----┘


---

## 📂 Project Structure

```text
├── data_loader.py       # Handles CSV loading and data cleaning
├── sqlite_storage.py    # Manages SQLite connections, insertions, and queries
├── parquet_storage.py   # Handles Parquet conversion and partitioned reads
├── main.py              # Main entry point to run the full workflow
├── schema.sql           # SQL definition for table creation
├── market_data_multi.csv # Source data (Prices)
├── tickers.csv          # Source data (Metadata)
├── comparison.md        # Generated report on performance metrics
├── pyproject.toml       # Project configuration and dependencies (uv-managed)
├── uv.lock              # Locked dependencies for reproducibility
├── tests/               # Unit tests ensuring workflow integrity
│   └── test_workflow.py
└── .github/
    └── workflows/       # CI/CD configuration for automated testing

---

## 🛠️ Installation & Setup

This project uses **uv** for modern & reproducible dependency management.

### **1. Install `uv` (if not installed)**

**macOS / Linux**
```bash
curl -LsSf [https://astral.sh/uv/install.sh](https://astral.sh/uv/install.sh) | sh
````

**Windows**

```powershell
powershell -c "irm [https://astral.sh/uv/install.ps1](https://astral.sh/uv/install.ps1) | iex"
```

-----

### **2. Sync Dependencies**

Creates virtual environment + installs all dependencies (Pandas, Numpy, PyArrow) from `uv.lock`:

```bash
uv sync
```

-----

## 🖥️ Usage

### **1. Run the Full Pipeline**

This command performs ingestion, database creation, Parquet conversion, and prints the performance comparison.

```bash
uv run main.py
```

**Expected Output:**

  - `market_data.db` (SQLite Database) created.
  - `market_data_parquet/` (Parquet Folder) created.
  - Console output showing SQL query results and timing benchmarks.

### **2. Run Unit Tests**

Validate the ETL logic and storage mechanisms using the built-in test suite (generates dummy data automatically).

```bash
uv run -m unittest tests/test_workflow.py
```

-----

## 📊 Benchmarking Results (Sample)

The system automatically compares the two formats. Typical results observe:

| Metric | SQLite3 | Parquet | Winner |
| :--- | :--- | :--- | :--- |
| **Storage Size** | \~0.66 MB | \~0.33 MB | **Parquet (2x smaller)** |
| **Read Speed (Single Ticker)** | \~5.2ms | \~1.5ms | **Parquet (\~3.5x faster)** |

*Note: Parquet performance gains are due to column-wise compression (Snappy) and partition pruning.*

-----

## 📝 License

This project is licensed under the **MIT License**.

© 2025 Yuting Li
