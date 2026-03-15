import pandas as pd
from src.transform.transformer import transform_customers, transform_sales
from src.transform.transformer import enrich_sales_with_currency

def test_transform_customers_columns():
    """Check that transform_customers returns expected columns."""
    raw = pd.DataFrame([{
        "customer_key": 1,
        "first_name": "David",
        "last_name": "Farm",
        "email_address": "david@test.com",
        "gender": "M",
        "yearly_income": 75000.0,
        "total_children": 2,
        "education": "Bachelors",
        "occupation": "Professional",
        "country": "Sweden",
        "state": "Stockholm",
        "city": "Nacka"
    }])
    result = transform_customers(raw)
    assert "full_name" in result.columns
    assert result["yearly_income"].dtype == "int64"
    assert result["full_name"].iloc[0] == "David Farm"


def test_transform_sales_margin():
    """Check that margin and margin_pct are correctly derived."""
    raw = pd.DataFrame([{
        "sales_order_number": "SO001",
        "order_date": "2024-01-01",
        "customer_key": 1,
        "product_key": 1,
        "product_name": "Test Product",
        "category": "Bikes",
        "order_quantity": 1,
        "unit_price": 100.0,
        "total_product_cost": 60.0,
        "sales_amount": 100.0
    }])
    result = transform_sales(raw)
    assert result["margin"].iloc[0] == 40.0
    assert result["margin_pct"].iloc[0] == 40.0

def test_enrich_sales_with_currency():
    """Check that currency enrichment adds correct converted column."""
    df = pd.DataFrame([{
        "sales_order_number": "SO001",
        "order_date": pd.Timestamp("2024-01-01"),
        "customer_key": 1,
        "product_key": 1,
        "product_name": "Test Product",
        "category": "Bikes",
        "order_quantity": 1,
        "unit_price": 100.0,
        "total_product_cost": 60.0,
        "sales_amount": 100.0,
        "margin": 40.0,
        "margin_pct": 40.0
    }])
    rates = {"SEK": 10.5}
    result = enrich_sales_with_currency(df, rates, "SEK")
    assert "sales_amount_sek" in result.columns
    assert result["sales_amount_sek"].iloc[0] == 1050.0