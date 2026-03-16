CUSTOMERS = """
    SELECT
        c.CustomerKey           AS customer_key,
        c.FirstName             AS first_name,
        c.LastName              AS last_name,
        c.EmailAddress          AS email_address,
        c.Gender                AS gender,
        c.YearlyIncome          AS yearly_income,
        c.TotalChildren         AS total_children,
        c.EnglishEducation      AS education,
        c.EnglishOccupation     AS occupation,
        g.EnglishCountryRegionName AS country,
        g.StateProvinceName     AS state,
        g.City                  AS city
    FROM
        DimCustomer c
        LEFT JOIN DimGeography g ON c.GeographyKey = g.GeographyKey
"""

SALES = """
    SELECT
        f.SalesOrderNumber      AS sales_order_number,
        f.CustomerKey           AS customer_key,
        f.ProductKey            AS product_key,
        f.OrderQuantity         AS order_quantity,
        f.UnitPrice             AS unit_price,
        f.TotalProductCost      AS total_product_cost,
        f.SalesAmount           AS sales_amount,
        d.FullDateAlternateKey  AS order_date,
        p.EnglishProductName    AS product_name,
        p.EnglishProductCategoryName AS category
    FROM
        FactInternetSales f
        LEFT JOIN DimDate d      ON f.OrderDateKey = d.DateKey
        LEFT JOIN (
            SELECT
                p.ProductKey,
                p.EnglishProductName,
                pc.EnglishProductCategoryName
            FROM DimProduct p
            LEFT JOIN DimProductSubcategory ps 
                ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
            LEFT JOIN DimProductCategory pc 
                ON ps.ProductCategoryKey = pc.ProductCategoryKey
        ) p ON f.ProductKey = p.ProductKey
"""