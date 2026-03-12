# dw-etl-python

A Python-based ETL pipeline that extracts data from AdventureWorksDW,
applies transformation logic using pandas, and outputs clean datasets
in CSV and Parquet format. Designed for extensibility toward PySpark
and Databricks workflows.

## Tech Stack
- Python 3.12
- pandas
- SQLAlchemy + pyodbc
- PySpark (later stages)
- SQL Server (AdventureWorksDW2019)

## Project Structure
```
dw-etl-python/
├── src/
│   ├── extract/       # DB connection and data extraction
│   ├── transform/     # Transformation logic
│   ├── load/          # Output writers (CSV, Parquet)
│   └── utils/         # Logging, config helpers
├── data/output/       # Pipeline outputs (gitignored)
├── tests/             # Unit tests
├── docs/              # Architecture notes
├── .env.example       # Environment variable template
└── main.py            # Pipeline entrypoint
```

## Status
In active development.