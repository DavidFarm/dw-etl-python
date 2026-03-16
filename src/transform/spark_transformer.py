import os
import sys

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_spark_session(app_name: str = "dw-etl-python") -> SparkSession:
    """
    Create and return a local SparkSession.
    PYSPARK_PYTHON is set explicitly to ensure Spark workers use
    the same Python interpreter as the driver (required on Windows).
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def transform_sales_spark(spark: SparkSession, df_pandas: pd.DataFrame) -> DataFrame:
    """
    Convert pandas sales DataFrame to Spark and apply aggregations
    to produce a sales summary by category and year.
    """
    logger.info("Converting pandas DataFrame to Spark...")
    df = spark.createDataFrame(df_pandas)
    logger.info(f"Spark loaded {df.count()} rows")

    logger.info("Applying Spark aggregations...")
    summary = (
        df.withColumn("order_year", F.year(F.col("order_date")))
        .groupBy("category", "order_year")
        .agg(
            F.count("sales_order_number").alias("total_orders"),
            F.round(F.sum("sales_amount"), 2).alias("total_sales_usd"),
            F.round(F.sum("sales_amount_sek"), 2).alias("total_sales_sek"),
            F.round(F.avg("margin_pct"), 2).alias("avg_margin_pct")
        )
        .orderBy("category", "order_year")
    )
    return summary


def write_spark_output(summary: DataFrame, output_path: str) -> None:
    """
    Write Spark summary DataFrame to CSV via pandas.
    Uses pandas writer locally to avoid Hadoop filesystem dependency
    on Windows. In production this would write natively via Spark
    to cloud storage.
    """
    logger.info(f"Writing Spark output to {output_path}...")
    pandas_df = summary.toPandas()
    os.makedirs(output_path, exist_ok=True)
    pandas_df.to_csv(f"{output_path}/summary.csv", index=False)
    logger.info("Spark output written successfully")