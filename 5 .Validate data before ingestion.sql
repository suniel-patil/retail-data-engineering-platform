SELECT 'Customers' AS table_name, COUNT(*) AS row_count
FROM dbo.Customers

UNION ALL

SELECT 'Products', COUNT(*)
FROM dbo.Products

UNION ALL

SELECT 'Orders', COUNT(*)
FROM dbo.Orders

UNION ALL

SELECT 'OrderItems', COUNT(*)
FROM dbo.OrderItems

UNION ALL

SELECT 'Payments', COUNT(*)
FROM dbo.Payments;