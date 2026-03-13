CUSTOMERS = """
    SELECT
        c.CustomerKey,
        c.FirstName,
        c.LastName,
        c.EmailAddress,
        c.Gender,
        c.YearlyIncome,
        c.TotalChildren,
        c.EnglishEducation        AS Education,
        c.EnglishOccupation       AS Occupation,
        g.EnglishCountryRegionName AS Country,
        g.StateProvinceName        AS State,
        g.City
    FROM
        DimCustomer c
        LEFT JOIN DimGeography g ON c.GeographyKey = g.GeographyKey
"""

SALES = """
    SELECT
        f.SalesOrderNumber,
        f.OrderDateKey,
        f.CustomerKey,
        f.ProductKey,
        f.OrderQuantity,
        f.UnitPrice,
        f.TotalProductCost,
        f.SalesAmount,
        d.FullDateAlternateKey    AS OrderDate,
        p.EnglishProductName      AS ProductName,
        p.EnglishProductCategoryName AS Category
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