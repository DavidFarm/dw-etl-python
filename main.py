from src.pipeline import run_pipeline

if __name__ == "__main__":
    run_pipeline()

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
