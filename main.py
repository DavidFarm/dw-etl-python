from src.extract.extractor import extract_table
from src.extract.queries import CUSTOMERS, SALES
from src.transform.transformer import transform_customers, transform_sales
from src.load.writer import write_all
from src.utils.logger import get_logger

logger = get_logger("pipeline")


if __name__ == "__main__":
    logger.info("Pipeline started")

    try:
        df_customers_raw = extract_table(CUSTOMERS, label="customers")
        df_sales_raw = extract_table(SALES, label="sales")

        df_customers = transform_customers(df_customers_raw)
        df_sales = transform_sales(df_sales_raw)

        write_all(df_customers, "customers")
        write_all(df_sales, "sales")

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.critical(f"Pipeline failed: {e}")
        raise

# Data types and null counts
# print(df_customers.dtypes)
# print(df_customers.isnull().sum())

# print(df_sales.dtypes)
# print(df_sales.isnull().sum())

# Verify types
# print(df_customers.dtypes)
# print(df_sales.dtypes)

# Verify margin logic on first row
# row = df_sales.iloc[0]
# print(f"SalesAmount: {row.sales_amount}, Cost: {row.total_product_cost}, Margin: {row.margin}, Margin%: {row.margin_pct}")
