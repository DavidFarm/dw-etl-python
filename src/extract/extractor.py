import pandas as pd
from src.utils.db import get_engine

def extract_table(query: str) -> pd.DataFrame:
    """Execute a SQL query and return results as a pandas DataFrame."""
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df