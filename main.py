from src.extract.extractor import extract_table
from src.extract.queries import CUSTOMERS, SALES

if __name__ == "__main__":

    print("Extracting customers...")
    df_customers = extract_table(CUSTOMERS)
    print(f"  Rows: {len(df_customers)}")
    print(f"  Columns: {list(df_customers.columns)}")
    print(df_customers.head(3))

    print("\nExtracting sales...")
    df_sales = extract_table(SALES)
    print(f"  Rows: {len(df_sales)}")
    print(f"  Columns: {list(df_sales.columns)}")
    print(df_sales.head(3))

    # Data types and null counts
# print(df_customers.dtypes)
# print(df_customers.isnull().sum())

# print(df_sales.dtypes)
# print(df_sales.isnull().sum())