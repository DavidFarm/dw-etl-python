import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("data/output")


def write_csv(df: pd.DataFrame, filename: str) -> None:
    """Write DataFrame to CSV in the output directory."""
    path = OUTPUT_DIR / f"{filename}.csv"
    df.to_csv(path, index=False)
    print(f"  Written: {path} ({len(df)} rows)")


def write_parquet(df: pd.DataFrame, filename: str) -> None:
    """Write DataFrame to Parquet in the output directory."""
    path = OUTPUT_DIR / f"{filename}.parquet"
    df.to_parquet(path, index=False)
    print(f"  Written: {path} ({len(df)} rows)")


def write_all(df: pd.DataFrame, filename: str) -> None:
    """Write DataFrame to both CSV and Parquet."""
    write_csv(df, filename)
    write_parquet(df, filename)