import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the customer DataFrame.
    - Standardise column names to lowercase
    - Convert yearly_income to integer
    - Add full_name derived column
    """
    logger.info("Transforming customers...")
    try:
        df = df.copy()
        df.columns = [col.lower() for col in df.columns]
        df["yearly_income"] = df["yearly_income"].astype(int)
        df["full_name"] = df["first_name"] + " " + df["last_name"]
        df = df[[
            "customer_key", "full_name", "first_name", "last_name",
            "email_address", "gender", "education", "occupation",
            "yearly_income", "total_children", "country", "state", "city"
        ]]
        logger.info(f"Customers transformed: {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Failed to transform customers: {e}")
        raise


def transform_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the sales DataFrame.
    - Standardise column names to lowercase
    - Convert order_date to datetime
    - Add derived margin columns
    """
    logger.info("Transforming sales...")
    try:
        df = df.copy()
        df.columns = [col.lower() for col in df.columns]
        df["order_date"] = pd.to_datetime(df["order_date"])
        df["margin"] = df["sales_amount"] - df["total_product_cost"]
        df["margin_pct"] = (df["margin"] / df["sales_amount"] * 100).round(2)
        df = df[[
            "sales_order_number", "order_date", "customer_key", "product_key",
            "product_name", "category", "order_quantity", "unit_price",
            "total_product_cost", "sales_amount", "margin", "margin_pct"
        ]]
        logger.info(f"Sales transformed: {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Failed to transform sales: {e}")
        raise