import pandas as pd
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)

OUTPUT_DIR = Path("data/output")


def write_csv(df: pd.DataFrame, filename: str) -> None:
    """Write DataFrame to CSV in the output directory."""
    path = OUTPUT_DIR / f"{filename}.csv"
    df.to_csv(path, index=False)
    logger.info(f"Written CSV: {path} ({len(df)} rows)")


def write_parquet(df: pd.DataFrame, filename: str) -> None:
    """Write DataFrame to Parquet in the output directory."""
    path = OUTPUT_DIR / f"{filename}.parquet"
    df.to_parquet(path, index=False)
    logger.info(f"Written Parquet: {path} ({len(df)} rows)")


def write_all(df: pd.DataFrame, filename: str) -> None:
    """Write DataFrame to both CSV and Parquet."""
    write_csv(df, filename)
    write_parquet(df, filename)