# Databricks notebook source
print("Databricks is ready")

print("Spark version:", spark.version)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.retail_ingestion;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.retail_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS
# MAGIC workspace.retail_ingestion.source_files;

# COMMAND ----------

catalog = "workspace"  # Replace if your catalog is different

volume_path = f"/Volumes/{catalog}/retail_ingestion/source_files"

df_orders = spark.read.parquet(
    f"{volume_path}/orders.parquet"
)

df_orders.printSchema()
df_orders.show()

# COMMAND ----------

print("Order count:", df_orders.count())

# COMMAND ----------

from pyspark.sql import functions as F

bronze_orders = (
    df_orders
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_system", F.lit("SQLServer"))
    .withColumn("source_table", F.lit("Orders"))
)

bronze_orders.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.retail_bronze.bronze_orders
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS bronze_count,
# MAGIC     COUNT(DISTINCT order_id) AS unique_orders,
# MAGIC     SUM(CASE WHEN order_status = 'SUCCESS' THEN 1 ELSE 0 END)
# MAGIC         AS successful_orders
# MAGIC FROM workspace.retail_bronze.bronze_orders;

# COMMAND ----------

print("Spark version:", spark.version)

# COMMAND ----------

catalog = "workspace"

volume_path = f"/Volumes/{catalog}/retail_ingestion/source_files"

print(volume_path)

# COMMAND ----------

files = dbutils.fs.ls(volume_path)

for file in files:
    print(file.name)

# COMMAND ----------

df_orders = spark.read.parquet(
    f"{volume_path}/orders.parquet"
)

df_orders.show()

# COMMAND ----------

df_orders.printSchema()

# COMMAND ----------

print("Total orders:", df_orders.count())

# COMMAND ----------

from pyspark.sql import functions as F

df_orders.select(
    F.countDistinct("order_id").alias("unique_orders")
).show()

# COMMAND ----------

df_orders.groupBy("order_status") \
         .count() \
         .show()

# COMMAND ----------

from pyspark.sql import functions as F

bronze_orders = (
    df_orders
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_system",
        F.lit("SQLServer")
    )
    .withColumn(
        "source_table",
        F.lit("Orders")
    )
)

# COMMAND ----------

bronze_orders.show()

# COMMAND ----------

table_name = f"{catalog}.retail_bronze.bronze_orders"

(
    bronze_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

print("Bronze Orders table created:", table_name)

# COMMAND ----------

table_name = f"{catalog}.retail_bronze.bronze_orders"

if not spark.catalog.tableExists(table_name):

    (
        bronze_orders.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(table_name)
    )

    print("Table created:", table_name)

else:
    print("Table already exists:", table_name)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

spark.sql(f"""
SELECT COUNT(*) AS total_records
FROM {table_name}
""").show()

# COMMAND ----------

table_name = f"{catalog}.retail_bronze.bronze_orders"

df = spark.table(table_name)

print("Bronze Orders Row Count:", df.count())

df.printSchema()

display(df.limit(10))

# COMMAND ----------

# Replace with your actual volume folder
volume_path = "/Volumes/workspace/retail_ingestion/source_files"

display(dbutils.fs.ls(volume_path))

# COMMAND ----------

from pyspark.sql import functions as F

def create_bronze_table(source_path, table_name):

    df = (
        spark.read
        .format("parquet")
        .load(source_path)
    )

    df = df.withColumn(
        "bronze_ingestion_timestamp",
        F.current_timestamp()
    )

    if not spark.catalog.tableExists(table_name):

        (
            df.write
            .format("delta")
            .mode("errorifexists")
            .saveAsTable(table_name)
        )

        print("Created:", table_name)

    else:
        print("Already exists, skipped:", table_name)

# COMMAND ----------

sources = {
    "customers": f"{volume_path}/customers.parquet",
    "products": f"{volume_path}/products.parquet",
    "order_items": f"{volume_path}/order_items.parquet",
    "payments": f"{volume_path}/payments.parquet"
}

for source_name, source_path in sources.items():

    bronze_table = (
        f"{catalog}.retail_bronze.bronze_{source_name}"
    )

    create_bronze_table(source_path, bronze_table)

# COMMAND ----------

from pyspark.sql import functions as F

def create_bronze_table(source_path, bronze_table):

    df = spark.read.parquet(source_path)

    bronze_df = (
        df.withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "source_file",
            F.col("_metadata.file_path")
        )
    )

    (
        bronze_df.write
        .format("delta")
        .mode("ignore")
        .saveAsTable(bronze_table)
    )

    print(f"Processed: {bronze_table}")

# COMMAND ----------

sources = {
    "customers": f"{volume_path}/customers.parquet",
    "products": f"{volume_path}/products.parquet",
    "order_items": f"{volume_path}/order_items.parquet",
    "payments": f"{volume_path}/payments.parquet"
}

for source_name, source_path in sources.items():

    bronze_table = (
        f"{catalog}.retail_bronze.bronze_{source_name}"
    )

    create_bronze_table(source_path, bronze_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.retail_bronze.bronze_orders;

# COMMAND ----------

tables = [
    "bronze_customers",
    "bronze_orders",
    "bronze_products",
    "bronze_order_items",
    "bronze_payments"
]

for table in tables:

    full_name = f"{catalog}.retail_bronze.{table}"

    df = spark.table(full_name)

    print(f"{table}: {df.count()} rows")

# COMMAND ----------

tables = [
    "bronze_customers",
    "bronze_orders",
    "bronze_products",
    "bronze_order_items",
    "bronze_payments"
]

for table in tables:

    full_name = f"{catalog}.retail_bronze.{table}"

    df = spark.table(full_name)

    print(f"{table}: {df.count()} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN retail_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN retail_bronze;

# COMMAND ----------

silver_schema = f"{catalog}.retail_silver"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {silver_schema}
""")

print("Silver schema ready:", silver_schema)

# COMMAND ----------

bronze_orders = spark.table(
    f"{catalog}.retail_bronze.bronze_orders"
)

print("Bronze order count:", bronze_orders.count())

bronze_orders.printSchema()

display(bronze_orders)

# COMMAND ----------

from pyspark.sql import functions as F

null_counts = bronze_orders.select([
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in bronze_orders.columns
])

display(null_counts)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

clean_orders = (
    bronze_orders
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
)

display(clean_orders)

# COMMAND ----------

from pyspark.sql import functions as F

null_counts = bronze_orders.select([
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in bronze_orders.columns
])

display(null_counts)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

clean_orders = (
    bronze_orders
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
)

display(clean_orders)

# COMMAND ----------

valid_orders = clean_orders.filter(
    F.col("order_id").isNotNull() &
    F.col("customer_id").isNotNull() &
    F.col("order_date").isNotNull() &
    F.col("updated_at").isNotNull()
)

print("Valid records:", valid_orders.count())

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("updated_at").desc())
)

silver_orders = (
    valid_orders
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

display(silver_orders)

# COMMAND ----------

silver_schema = f"{catalog}.retail_silver"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {silver_schema}
""")

print("Silver schema ready:", silver_schema)

# COMMAND ----------

bronze_orders = spark.table(
    f"{catalog}.retail_bronze.bronze_orders"
)

print("Bronze order count:", bronze_orders.count())

bronze_orders.printSchema()

display(bronze_orders)

# COMMAND ----------

from pyspark.sql import functions as F

null_counts = bronze_orders.select([
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in bronze_orders.columns
])

display(null_counts)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

clean_orders = (
    bronze_orders
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    )


display(clean_orders)

# COMMAND ----------

bronze_orders = spark.table(
    f"{catalog}.retail_bronze.bronze_orders"
)

print("Bronze order count:", bronze_orders.count())

bronze_orders.printSchema()

display(bronze_orders)

# COMMAND ----------

from pyspark.sql import functions as F

null_counts = bronze_orders.select([
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in bronze_orders.columns
])

display(null_counts)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("updated_at").desc())
)

silver_orders = (
    valid_orders
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

display(silver_orders)

# COMMAND ----------

silver_orders = bronze_orders

# COMMAND ----------

bronze_orders = spark.table(
    f"{catalog}.retail_bronze.bronze_orders"
)

bronze_orders.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read Bronze Orders
bronze_orders = spark.table(
    f"{catalog}.retail_bronze.bronze_orders"
)

# Window for latest record per order
window_spec = (
    Window.partitionBy("order_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("ingestion_timestamp").desc_nulls_last()
    )
)

# Silver transformation
silver_orders = (
    bronze_orders
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("order_status").isin("SUCCESS", "FAILED", "PENDING"))
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

silver_orders.show()

# COMMAND ----------

silver_table = f"{catalog}.retail_silver.silver_orders"

(
    silver_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

print("Silver Orders table created:", silver_table)

# COMMAND ----------

silver_orders_check = spark.table(silver_table)

silver_orders_check.select(
    F.count("*").alias("total_orders"),
    F.countDistinct("order_id").alias("unique_orders")
).show()

# COMMAND ----------

silver_orders_check.groupBy("order_status").count().show()

# COMMAND ----------

bronze_items = spark.table(
    f"{catalog}.retail_bronze.bronze_order_items"
)

order_amounts = (
    bronze_items
    .withColumn(
        "line_amount",
        F.col("quantity") * F.col("unit_price")
    )
    .groupBy("order_id")
    .agg(
        F.sum("line_amount").alias("order_amount")
    )
)

order_amounts.show()

# COMMAND ----------

silver_table = f"{catalog}.retail_silver.silver_orders"

(
    silver_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

print("Silver Orders table created:", silver_table)

# COMMAND ----------

silver_check = spark.table(silver_table)

print("Silver row count:", silver_check.count())

display(silver_check)

# COMMAND ----------

duplicates = (
    silver_check
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate order IDs:", duplicates.count())

# COMMAND ----------

tables = [
    "customers",
    "products",
    "order_items",
    "payments"
]

for table in tables:
    df = spark.table(
        f"{catalog}.retail_bronze.bronze_{table}"
    )

    print(f"\n===== {table.upper()} =====")
    df.printSchema()
    display(df.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_silver_table(source, primary_key):

    bronze_table = (
        f"{catalog}.retail_bronze.bronze_{source}"
    )

    silver_table = (
        f"{catalog}.retail_silver.silver_{source}"
    )

    df = spark.table(bronze_table)

    # Remove records missing a primary key
    df = df.filter(
        F.col(primary_key).isNotNull()
    )

    # Remove exact duplicate records
    df = df.dropDuplicates()

    # Save cleaned data
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )

    print(
        f"{silver_table}: "
        f"{spark.table(silver_table).count()} records"
    )

# COMMAND ----------

create_silver_table("customers", "customer_id")

create_silver_table("products", "product_id")

create_silver_table("order_items", "order_item_id")

create_silver_table("payments", "payment_id")

# COMMAND ----------

tables = [
    "customers",
    "orders",
    "products",
    "order_items",
    "payments"
]

for table in tables:

    full_name = (
        f"{catalog}.retail_silver.silver_{table}"
    )

    df = spark.table(full_name)

    print(
        f"silver_{table}: {df.count()} records"
    )

# COMMAND ----------

from pyspark.sql import functions as F

silver_table = f"{catalog}.retail_silver.silver_orders"

orders_df = spark.table(silver_table)

orders_df.printSchema()
display(orders_df.orderBy("order_id"))

# COMMAND ----------

from datetime import datetime, timedelta
from decimal import Decimal
from pyspark.sql import functions as F

orders_df = spark.table(silver_table)

# Select one existing record
existing_row = orders_df.orderBy("order_id").first().asDict()

# Record the current count
before_count = orders_df.count()

# Create an updated version
updated_row = existing_row.copy()
updated_row["updated_at"] = (
    datetime.now() + timedelta(days=1)
)

# Change amount if that field exists
if "amount" in updated_row:
    updated_row["amount"] = (
        updated_row["amount"] + Decimal("50.00")
    )

# Create a new order from the same sample structure
new_row = existing_row.copy()

new_order_id = orders_df.agg(
    F.max("order_id")
).first()[0] + 1

new_row["order_id"] = new_order_id
new_row["updated_at"] = (
    datetime.now() + timedelta(days=1)
)

# Match the existing Silver schema
incremental_df = spark.createDataFrame(
    [updated_row, new_row],
    schema=orders_df.schema
)

display(incremental_df)

# COMMAND ----------

from delta.tables import DeltaTable

silver_delta = DeltaTable.forName(
    spark,
    silver_table
)

(
    silver_delta.alias("target")
    .merge(
        incremental_df.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdateAll(
        condition="source.updated_at > target.updated_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Incremental MERGE completed")

# COMMAND ----------

after_df = spark.table(silver_table)

print("Before MERGE:", before_count)
print("After MERGE:", after_df.count())

display(
    after_df
    .filter(
        F.col("order_id").isin(
            existing_row["order_id"],
            new_order_id
        )
    )
)

# COMMAND ----------

duplicate_count = (
    after_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Duplicate order IDs:", duplicate_count)

# COMMAND ----------

from pyspark.sql import functions as F

silver_orders = spark.table(
    f"{catalog}.retail_silver.silver_orders"
)

print("Silver Orders Count:", silver_orders.count())
silver_orders.printSchema()

display(silver_orders.orderBy("order_id"))

# COMMAND ----------

for name in ["orders", "order_items", "payments"]:
    df = spark.table(
        f"{catalog}.retail_silver.silver_{name}"
    )
    print(name, ":", df.columns)

# COMMAND ----------

gold_schema = f"{catalog}.retail_gold"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {gold_schema}
""")

print("Gold schema ready:", gold_schema)

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table(
    f"{catalog}.retail_silver.silver_orders"
)

items = spark.table(
    f"{catalog}.retail_silver.silver_order_items"
)

# Confirm required columns exist
required = {"order_id", "quantity", "unit_price"}

missing = required - set(items.columns)

if missing:
    raise ValueError(
        f"Missing order-item columns: {missing}. "
        f"Available columns: {items.columns}"
    )

# Calculate sales amount for each order item
items_with_amount = items.withColumn(
    "line_amount",
    F.col("quantity") * F.col("unit_price")
)

# Join items with successful orders
successful_orders = orders.filter(
    F.upper(F.trim(F.col("order_status"))) == "SUCCESS"
)

sales = (
    successful_orders
    .select("order_id", "customer_id", "order_date")
    .join(
        items_with_amount,
        on="order_id",
        how="inner"
    )
)

# Aggregate to daily sales
daily_revenue = (
    sales
    .groupBy(F.to_date("order_date").alias("order_day"))
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("line_amount").alias("total_revenue")
    )
    .orderBy("order_day")
)

display(daily_revenue)

# COMMAND ----------

gold_table = f"{catalog}.retail_gold.gold_daily_revenue"

(
    daily_revenue.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

print("Gold table created:", gold_table)

display(spark.table(gold_table))

# COMMAND ----------

detail_revenue = sales.agg(
    F.sum("line_amount")
).first()[0]

gold_revenue = spark.table(gold_table).agg(
    F.sum("total_revenue")
).first()[0]

print("Detail revenue:", detail_revenue)
print("Gold revenue:", gold_revenue)
print("Revenue matches:", detail_revenue == gold_revenue)

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table(
    f"{catalog}.retail_silver.silver_orders"
)

customers = spark.table(
    f"{catalog}.retail_silver.silver_customers"
)

items = spark.table(
    f"{catalog}.retail_silver.silver_order_items"
)

products = spark.table(
    f"{catalog}.retail_silver.silver_products"
)

for name, df in [
    ("orders", orders),
    ("customers", customers),
    ("order_items", items),
    ("products", products)
]:
    print(name, df.columns)

# COMMAND ----------

# Validate the required columns
requirements = {
    "orders": (
        orders,
        ["order_id", "customer_id", "order_date", "order_status"]
    ),
    "order_items": (
        items,
        ["order_id", "product_id", "quantity", "unit_price"]
    )
}

for name, (df, required_columns) in requirements.items():
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"{name} missing columns: {missing}")

successful_orders = orders.filter(
    F.upper(F.trim(F.col("order_status"))) == "SUCCESS"
)

valid_items = (
    items
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") >= 0)
    .withColumn(
        "line_amount",
        F.col("quantity") * F.col("unit_price")
    )
)

sales = (
    successful_orders
    .select("order_id", "customer_id", "order_date")
    .join(valid_items, on="order_id", how="inner")
)

display(sales)

# COMMAND ----------

customer_sales = (
    sales
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("line_amount").alias("total_revenue")
    )
    .withColumn(
        "avg_order_value",
        F.round(
            F.col("total_revenue") / F.col("total_orders"),
            2
        )
    )
)

display(customer_sales)

# COMMAND ----------

# Ensure customer IDs are unique before joining
customer_duplicates = (
    customers.groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if customer_duplicates:
    raise ValueError("Duplicate customer IDs found in Silver")

gold_customer_sales = (
    customers
    .join(customer_sales, on="customer_id", how="left")
    .fillna({
        "total_orders": 0,
        "total_revenue": 0,
        "avg_order_value": 0
    })
)

display(gold_customer_sales)

# COMMAND ----------

gold_schema = f"{catalog}.retail_gold"

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {gold_schema}"
)

(
    gold_customer_sales.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{gold_schema}.gold_customer_sales"
    )
)

# COMMAND ----------

product_sales = (
    sales
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("line_amount").alias("total_revenue")
    )
)

display(product_sales)

# COMMAND ----------

product_duplicates = (
    products.groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if product_duplicates:
    raise ValueError("Duplicate product IDs found in Silver")

gold_product_sales = (
    products
    .join(product_sales, on="product_id", how="left")
    .fillna({
        "total_quantity": 0,
        "total_orders": 0,
        "total_revenue": 0
    })
)

display(gold_product_sales)

# COMMAND ----------

(
    gold_product_sales.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{gold_schema}.gold_product_sales"
    )
)

print("Gold Product Sales created")

# COMMAND ----------

daily = spark.table(
    f"{gold_schema}.gold_daily_revenue"
)

customer = spark.table(
    f"{gold_schema}.gold_customer_sales"
)

product = spark.table(
    f"{gold_schema}.gold_product_sales"
)

totals = {
    "daily": daily.agg(F.sum("total_revenue")).first()[0],
    "customer": customer.agg(F.sum("total_revenue")).first()[0],
    "product": product.agg(F.sum("total_revenue")).first()[0]
}

print(totals)

if len(set(totals.values())) == 1:
    print("PASS: Revenue reconciled!")
else:
    print("FAIL: Revenue mismatch — investigate.")

# COMMAND ----------

gold_tables = [
    "gold_daily_revenue",
    "gold_customer_sales",
    "gold_product_sales"
]

for table in gold_tables:
    full_name = f"{catalog}.retail_gold.{table}"

    if spark.catalog.tableExists(full_name):
        df = spark.table(full_name)
        print(f"{table}: {df.count()} records")
    else:
        print(f"{table}: NOT CREATED")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Daily Revenue' AS model,
# MAGIC        SUM(total_revenue) AS revenue
# MAGIC FROM workspace.retail_dbt.mart_daily_revenue
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Customer Sales',
# MAGIC        SUM(total_revenue)
# MAGIC FROM workspace.retail_dbt.mart_customer_sales
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Product Sales',
# MAGIC        SUM(total_revenue)
# MAGIC FROM workspace.retail_dbt.mart_product_sales;

# COMMAND ----------

from pyspark.sql import functions as F

for name in [
    "daily_revenue",
    "customer_sales",
    "product_sales"
]:
    pyspark_total = spark.table(
        f"{catalog}.retail_gold.gold_{name}"
    ).agg(F.sum("total_revenue")).first()[0]

    dbt_total = spark.table(
        f"{catalog}.retail_dbt.mart_{name}"
    ).agg(F.sum("total_revenue")).first()[0]

    print(
        name,
        "PySpark:", pyspark_total,
        "dbt:", dbt_total,
        "PASS:", abs(pyspark_total - dbt_total) <= 0.01
    )

# COMMAND ----------

