SELECT
    c.customer_id,
    c.customer_name,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM dbo.Customers c
INNER JOIN dbo.Orders o
    ON c.customer_id = o.customer_id
INNER JOIN dbo.OrderItems oi
    ON o.order_id = oi.order_id
WHERE o.order_status = 'SUCCESS'
GROUP BY
    c.customer_id,
    c.customer_name
ORDER BY
    total_revenue DESC;