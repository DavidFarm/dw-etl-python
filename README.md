# dw-etl-python

![Python](https://img.shields.io/badge/Python-3.11-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)
![License](https://img.shields.io/badge/License-MIT-green)

A Python-based ETL pipeline that extracts data from AdventureWorksDW, transforms 
and enriches it using pandas and a live currency API, then produces aggregated 
business intelligence outputs using PySpark.

Built to demonstrate production-grade Python data engineering practices including 
modular pipeline architecture, structured logging, unit testing, and multi-format output.

---

## Sample Output

Sales performance by product category and year, with live USD→SEK conversion:

| Category | Year | Orders | Sales (USD) | Sales (SEK) | Avg Margin % |
|---|---|---|---|---|---|
| Accessories | 2013 | 34,409 | $668,242 | 6,316,030 kr | 62.6% |
| Bikes | 2013 | 9,706 | $15,359,502 | 145,175,137 kr | 39.9% |
| Bikes | 2012 | 3,269 | $5,839,695 | 55,195,710 kr | 41.1% |
| Clothing | 2013 | 8,686 | $323,806 | 3,060,545 kr | 38.5% |

*Exchange rates fetched live from ExchangeRate-API on each pipeline run.*

---

## Architecture
```
SQL Server (AdventureWorksDW)
        │
        ▼
   Extract (pyodbc / SQLAlchemy)
        │
        ▼
   Transform (pandas)
   - Type casting and column standardisation
   - Derived metrics (margin, margin %)
   - Currency enrichment (USD → SEK via live API)
        │
        ▼
   Load (CSV + Parquet)
        │
        ▼
   Spark Aggregation (PySpark)
   - Sales summary by category and year
   - Output: CSV
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Data manipulation | pandas |
| Database connectivity | SQLAlchemy + pyodbc |
| Distributed processing | PySpark 3.5 |
| API integration | requests |
| Output formats | CSV, Parquet |
| Logging | Python logging module |
| Testing | pytest |
| Source control | Git / GitHub |

---

## Project Structure
```
dw-etl-python/
├── src/
│   ├── extract/
│   │   ├── extractor.py          # SQL extraction via SQLAlchemy
│   │   ├── queries.py            # SQL query definitions
│   │   └── api_client.py         # Live exchange rate API client
│   ├── transform/
│   │   ├── transformer.py        # pandas transformations and enrichment
│   │   └── spark_transformer.py  # PySpark aggregation stage
│   ├── load/
│   │   └── writer.py             # CSV and Parquet writers
│   ├── utils/
│   │   ├── config.py             # Environment variable loader
│   │   ├── db.py                 # Database connection factory
│   │   └── logger.py             # Structured logging setup
│   └── pipeline.py               # Pipeline orchestrator
├── tests/
│   └── test_transformer.py       # Unit tests for transformation logic
├── main.py                       # Pipeline entrypoint
├── conftest.py                   # pytest path configuration
├── .env.example                  # Environment variable template
└── requirements.txt              # Python dependencies
```

---

## Setup

### Prerequisites
- Python 3.11
- SQL Server with AdventureWorksDW database
- Java 21 (required for PySpark)

### Installation
```bash
git clone https://github.com/DavidFarm/dw-etl-python.git
cd dw-etl-python
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### Configuration

Copy `.env.example` to `.env` and fill in your local SQL Server details:
```
DB_SERVER=your_server_name
DB_NAME=AdventureWorksDW2019
DB_DRIVER=ODBC Driver 17 for SQL Server
```

### Run
```bash
python main.py
```

---

## Pipeline Stages

**Extract** — Connects to AdventureWorksDW via Windows Authentication and extracts
customer and sales data using SQLAlchemy. Column naming is handled at the SQL layer
using AS aliases, producing clean snake_case column names directly from the query.

**Transform** — Applies type fixes, derives business metrics (margin, margin %), and
enriches sales data with live USD/SEK exchange rates fetched from ExchangeRate-API.

**Load** — Writes transformed datasets to both CSV and Parquet format.

**Spark Aggregation** — Loads the enriched sales DataFrame into a local PySpark session
and produces a grouped summary by product category and order year, including total
orders, total sales in USD and SEK, and average margin percentage.

---

## Output Files

| File | Description |
|---|---|
| `customers.csv / .parquet` | Cleaned customer data with geography |
| `sales.csv / .parquet` | Enriched sales data with margin and SEK amounts |
| `sales_summary_spark/summary.csv` | PySpark aggregation: sales by category and year |

---

## Testing
```bash
pytest tests/
```

Three unit tests cover customer transformation, sales transformation, and currency
enrichment logic independently of the database.

---

## Notes

- Database credentials are managed via `.env` and never committed to the repository
- The PySpark stage uses `createDataFrame()` from pandas to avoid Hadoop filesystem
  dependencies on Windows. In a cloud or Databricks environment this would write
  natively via Spark
- Exchange rates are fetched live on each pipeline run from [ExchangeRate-API](https://open.er-api.com)