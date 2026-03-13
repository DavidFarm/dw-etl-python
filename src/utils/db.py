from sqlalchemy import create_engine, text
from src.utils.config import DB_SERVER, DB_NAME, DB_DRIVER
import urllib

def get_engine():
    """Create and return a SQLAlchemy engine using Windows Authentication."""
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"Trusted_Connection=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine

def test_connection():
    """Test the database connection and print SQL Server version."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT @@VERSION"))
        row = result.fetchone()
        print("Connection successful:")
        print(row[0])