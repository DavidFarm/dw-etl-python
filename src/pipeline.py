from src.extract.extractor import extract_table
from src.extract.queries import CUSTOMERS, SALES
from src.extract.api_client import get_usd_exchange_rates
from src.transform.transformer import (
    transform_customers,
    transform_sales,
    enrich_sales_with_currency
)
from src.load.writer import write_all
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_pipeline() -> None:
    """
    Execute the full ETL pipeline:
    - Extract customer and sales data from AdventureWorksDW
    - Fetch live exchange rates from ExchangeRate-API
    - Apply transformation, cleaning, and currency enrichment
    - Write outputs to CSV and Parquet
    """
    logger.info("Pipeline started")

    try:
        # Extract
        df_customers_raw = extract_table(CUSTOMERS, label="customers")
        df_sales_raw = extract_table(SALES, label="sales")

        # Fetch exchange rates
        rates = get_usd_exchange_rates()

        # Transform
        df_customers = transform_customers(df_customers_raw)
        df_sales = transform_sales(df_sales_raw)

        # Enrich
        df_sales = enrich_sales_with_currency(df_sales, rates, "SEK")

        # Load
        write_all(df_customers, "customers")
        write_all(df_sales, "sales")

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.critical(f"Pipeline failed: {e}")
        raise