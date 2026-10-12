IF DB_ID('RetailLakehouseDB') IS NULL
BEGIN
    CREATE DATABASE RetailLakehouseDB;
END;
GO

USE RetailLakehouseDB;
GO

SELECT DB_NAME() AS current_database;


USE RetailLakehouseDB;
GO

-- 1. Customers
CREATE TABLE dbo.Customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(150),
    city VARCHAR(100),
    created_at DATETIME2(0) NOT NULL,
    updated_at DATETIME2(0) NOT NULL
);

-- 2. Products
CREATE TABLE dbo.Products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    unit_price DECIMAL(12,2) NOT NULL
);

-- 3. Orders
CREATE TABLE dbo.Orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    updated_at DATETIME2(0) NOT NULL,

    CONSTRAINT FK_Orders_Customers
        FOREIGN KEY (customer_id)
        REFERENCES dbo.Customers(customer_id)
);

-- 4. Order Items
CREATE TABLE dbo.OrderItems (
    order_item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(12,2) NOT NULL,

    CONSTRAINT FK_OrderItems_Orders
        FOREIGN KEY (order_id)
        REFERENCES dbo.Orders(order_id),

    CONSTRAINT FK_OrderItems_Products
        FOREIGN KEY (product_id)
        REFERENCES dbo.Products(product_id)
);

-- 5. Payments
CREATE TABLE dbo.Payments (
    payment_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    payment_amount DECIMAL(12,2) NOT NULL,
    payment_status VARCHAR(20) NOT NULL,
    payment_date DATETIME2(0),

    CONSTRAINT FK_Payments_Orders
        FOREIGN KEY (order_id)
        REFERENCES dbo.Orders(order_id)
);
GO