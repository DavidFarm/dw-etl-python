# dw-etl-python

A Python-based ETL pipeline that extracts data from AdventureWorksDW, applies 
transformation logic using pandas, enriches with live currency data via a public 
API, and produces aggregated outputs using PySpark. Designed to demonstrate 
production-grade Python data engineering practices on a local SQL Server source.

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
   - Output: CSV via pandas writer
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
│   │   ├── extractor.py       # SQL extraction via SQLAlchemy
│   │   ├── queries.py         # SQL query definitions
│   │   └── api_client.py      # Live exchange rate API client
│   ├── transform/
│   │   ├── transformer.py     # pandas transformations and enrichment
│   │   └── spark_transformer.py  # PySpark aggregation stage
│   ├── load/
│   │   └── writer.py          # CSV and Parquet writers
│   ├── utils/
│   │   ├── config.py          # Environment variable loader
│   │   ├── db.py              # Database connection factory
│   │   └── logger.py          # Structured logging setup
│   └── pipeline.py            # Pipeline orchestrator
├── data/output/               # Pipeline outputs (gitignored)
├── tests/
│   └── test_transformer.py    # Unit tests for transformation logic
├── docs/                      # Architecture notes
├── logs/                      # Runtime logs (gitignored)
├── main.py                    # Pipeline entrypoint
├── conftest.py                # pytest path configuration
├── .env.example               # Environment variable template
└── requirements.txt           # Python dependencies
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

**Load** — Writes transformed datasets to both CSV and Parquet format in `data/output/`.

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