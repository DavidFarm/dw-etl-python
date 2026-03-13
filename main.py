from src.extract.extractor import extract_table
from src.extract.queries import CUSTOMERS, SALES
from src.transform.transformer import transform_customers, transform_sales
from src.load.writer import write_all

if __name__ == "__main__":

    print("Extracting customers...")
    df_customers_raw = extract_table(CUSTOMERS)
    print(f"  Rows extracted: {len(df_customers_raw)}")

    print("\nExtracting sales...")
    df_sales_raw = extract_table(SALES)
    print(f"  Rows extracted: {len(df_sales_raw)}")

    print("\nTransforming customers...")
    df_customers = transform_customers(df_customers_raw)

    print("\nTransforming sales...")
    df_sales = transform_sales(df_sales_raw)

    print("\nWriting output files...")
    write_all(df_customers, "customers")
    write_all(df_sales, "sales")

    print("\nPipeline complete.")


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
