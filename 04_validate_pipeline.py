# Databricks notebook source
from pyspark.sql import functions as F

catalog = "workspace"
schema = "retail_dbt"

models = [
    "mart_daily_revenue",
    "mart_customer_sales",
    "mart_product_sales"
]

totals = {}

for model in models:
    table = f"{catalog}.{schema}.{model}"

    df = spark.table(table)

    revenue = df.agg(
        F.sum("total_revenue")
    ).first()[0]

    assert revenue is not None, f"No revenue found: {table}"

    totals[model] = revenue

    print(f"{model}: {revenue}")

# Check that all mart totals match
values = list(totals.values())

assert max(values) - min(values) <= 0.01, (
    f"Revenue mismatch: {totals}"
)

# Check Silver Orders have no duplicate order IDs
orders = spark.table(
    f"{catalog}.retail_silver.silver_orders"
)

duplicates = (
    orders.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert duplicates == 0, "Duplicate order IDs detected"

print("PASS: Revenue reconciled and no duplicate orders")