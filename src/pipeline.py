from src.extract.extractor import extract_table
from src.extract.queries import CUSTOMERS, SALES
from src.extract.api_client import get_usd_exchange_rates
from src.transform.transformer import (
    transform_customers,
    transform_sales,
    enrich_sales_with_currency
)
from src.transform.spark_transformer import (
    get_spark_session,
    transform_sales_spark,
    write_spark_output
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
    - Run Spark aggregations on sales data
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

        # Load pandas outputs
        write_all(df_customers, "customers")
        write_all(df_sales, "sales")

        # Spark aggregation stage
        spark = get_spark_session()
        summary = transform_sales_spark(spark, df_sales)
        write_spark_output(summary, "data/output/sales_summary_spark")
        spark.stop()

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.critical(f"Pipeline failed: {e}")
        raise