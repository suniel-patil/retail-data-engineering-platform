# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timedelta

catalog = "workspace"

orders_source = f"{catalog}.retail_silver.silver_orders"
items_source = f"{catalog}.retail_silver.silver_order_items"

orders = spark.table(orders_source)
items = spark.table(items_source)

print("ORDERS SCHEMA")
orders.printSchema()

print("ORDER ITEMS SCHEMA")
items.printSchema()

display(orders)
display(items)

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.retail_demo
""")

demo_orders = f"{catalog}.retail_demo.orders_merge_demo"
demo_items = f"{catalog}.retail_demo.items_merge_demo"

if not spark.catalog.tableExists(demo_orders):
    orders.write.format("delta").mode("ignore").saveAsTable(demo_orders)

if not spark.catalog.tableExists(demo_items):
    items.write.format("delta").mode("ignore").saveAsTable(demo_items)

print("Demo Orders:", spark.table(demo_orders).count())
print("Demo Items:", spark.table(demo_items).count())

# COMMAND ----------

from datetime import datetime, timedelta

# Read the original Silver data to maintain stable test IDs
source_orders = spark.table(orders_source)

existing_row = (
    source_orders.orderBy("order_id")
    .first()
    .asDict()
)

# Stable ID based on the original source
max_id = source_orders.agg(
    F.max("order_id")
).first()[0]

new_order_id = max_id + 1000

# Choose a timestamp later than the current demo records
latest_timestamp = spark.table(demo_orders).agg(
    F.max("updated_at")
).first()[0]

new_timestamp = max(
    datetime.utcnow(),
    latest_timestamp or datetime.utcnow()
) + timedelta(days=1)

# Existing order update
updated_row = existing_row.copy()
updated_row["updated_at"] = new_timestamp

# New order insertion
new_row = existing_row.copy()
new_row["order_id"] = new_order_id
new_row["updated_at"] = new_timestamp

incremental_orders = spark.createDataFrame(
    [updated_row, new_row],
    schema=source_orders.schema
)

display(incremental_orders)

# COMMAND ----------

target = DeltaTable.forName(
    spark,
    demo_orders
)

(
    target.alias("t")
    .merge(
        incremental_orders.alias("s"),
        "t.order_id = s.order_id"
    )
    .whenMatchedUpdateAll(
        condition="s.updated_at > t.updated_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("MERGE completed successfully")

# COMMAND ----------

result = spark.table(demo_orders)

print("Total orders:", result.count())

display(
    result.filter(
        F.col("order_id").isin(
            existing_row["order_id"],
            new_order_id
        )
    )
)

duplicate_count = (
    result.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Duplicate IDs:", duplicate_count)

assert duplicate_count == 0

assert result.count() == source_orders.count() + 1

print("PASS: One new order, no duplicates")