# Databricks notebook source
from pyspark.sql import functions as F

catalog = "workspace"

tables = [
    "customers",
    "orders",
    "products",
    "order_items",
    "payments"
]

for name in tables:
    table = f"{catalog}.retail_silver.silver_{name}"

    df = spark.table(table)

    print(f"{name}: {df.count()} records")

print("Silver table validation complete")

# COMMAND ----------

orders = spark.table(
    f"{catalog}.retail_silver.silver_orders"
)

duplicates = (
    orders.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert duplicates == 0, (
    f"Found {duplicates} duplicate order IDs"
)

print("PASS: No duplicate order IDs")

# COMMAND ----------

table_name = f"{catalog}.retail_bronze.bronze_orders"

if not spark.catalog.tableExists(table_name):
    (
        bronze_orders.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(table_name)
    )
    print("Created:", table_name)
else:
    print("Existing Bronze table preserved:", table_name)