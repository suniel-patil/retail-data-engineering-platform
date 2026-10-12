USE RetailLakehouseDB;
GO

-- Customers
INSERT INTO dbo.Customers
(customer_id, customer_name, email, city, created_at, updated_at)
VALUES
(1, 'John Smith', 'john@example.com', 'Dallas', '2026-01-01', '2026-01-01'),
(2, 'Sarah Johnson', 'sarah@example.com', 'Frisco', '2026-01-02', '2026-01-02'),
(3, 'Michael Brown', 'michael@example.com', 'Plano', '2026-01-03', '2026-01-03'),
(4, 'Priya Sharma', 'priya@example.com', 'Irving', '2026-01-04', '2026-01-04'),
(5, 'David Lee', 'david@example.com', 'McKinney', '2026-01-05', '2026-01-05');

-- Products
INSERT INTO dbo.Products
(product_id, product_name, category, unit_price)
VALUES
(10, 'Keyboard', 'Electronics', 100.00),
(20, 'Mouse', 'Electronics', 50.00),
(30, 'Desk', 'Furniture', 300.00),
(40, 'Chair', 'Furniture', 150.00),
(50, 'Monitor', 'Electronics', 400.00);

-- Orders
INSERT INTO dbo.Orders
(order_id, customer_id, order_date, order_status, updated_at)
VALUES
(101, 1, '2026-01-10', 'SUCCESS', '2026-01-10'),
(102, 1, '2026-01-11', 'SUCCESS', '2026-01-11'),
(103, 2, '2026-01-12', 'FAILED',  '2026-01-12'),
(104, 2, '2026-01-13', 'SUCCESS', '2026-01-13'),
(105, 3, '2026-01-14', 'SUCCESS', '2026-01-14'),
(106, 4, '2026-01-15', 'SUCCESS', '2026-01-15');

-- Order items
INSERT INTO dbo.OrderItems
(order_item_id, order_id, product_id, quantity, unit_price)
VALUES
(1001, 101, 10, 2, 100.00),
(1002, 101, 20, 1, 50.00),
(1003, 102, 30, 1, 300.00),
(1004, 103, 20, 1, 50.00),
(1005, 104, 40, 2, 150.00),
(1006, 105, 50, 1, 400.00),
(1007, 106, 10, 1, 100.00);

-- Payments
INSERT INTO dbo.Payments
(payment_id, order_id, payment_amount, payment_status, payment_date)
VALUES
(5001, 101, 250.00, 'SUCCESS', '2026-01-10'),
(5002, 102, 300.00, 'SUCCESS', '2026-01-11'),
(5003, 103, 50.00,  'FAILED',  '2026-01-12'),
(5004, 104, 300.00, 'SUCCESS', '2026-01-13'),
(5005, 105, 400.00, 'SUCCESS', '2026-01-14'),
(5006, 106, 100.00, 'SUCCESS', '2026-01-15');