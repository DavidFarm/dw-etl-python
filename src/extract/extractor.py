import pandas as pd
from src.utils.db import get_engine
from src.utils.logger import get_logger

logger = get_logger(__name__)


def extract_table(query: str, label: str = "table") -> pd.DataFrame:
    """Execute a SQL query and return results as a pandas DataFrame."""
    logger.info(f"Extracting {label}...")
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        logger.info(f"Extracted {len(df)} rows from {label}")
        return df
    except Exception as e:
        logger.error(f"Failed to extract {label}: {e}")
        raise