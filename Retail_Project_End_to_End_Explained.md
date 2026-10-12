# Retail Data Engineering Platform — End-to-End Learning Notebook

**SQL Server → Python/Parquet → Databricks Bronze → PySpark Silver → dbt Gold → Lakeflow Jobs**

**Purpose:** Understand the business flow, technical flow, code, why each component exists, and how to explain the project to an interviewer.

> **Important:** This is a guided reconstruction based on our project conversation, **not a byte-for-byte export of your actual notebooks**. Some complete original scripts and source schemas weren't supplied. Code labeled *template* needs paths/columns adjusted before execution. Do not run this whole notebook top-to-bottom against your live project: several cells are demonstrations, not a new ingestion job.

**Evidence confirmed in our work:** Five Bronze tables (5 customers, 6 orders, 5 products, 7 order items, 6 payments), five Silver tables with those same reported counts, three PySpark Gold tables (5 rows each), and working dbt marts where daily/customer/product revenue each summed to **$1,350**. You later reported all three Lakeflow tasks ran. **Not yet independently confirmed here:** record-level PySpark↔dbt comparison, repeatable changed-file ingestion, fully wired incremental MERGE, and a successful fourth validation task.

---

## 1. Data flow diagram

![Retail project pipeline](docs/data_flow.png)

### Nontechnical explanation
A retail company has sales, customers, products, items, and payments in SQL Server. We copy those records into Databricks, store the original copies, clean them, and turn them into useful answers: *How much revenue did we make each day? Which customers spent the most? Which products sold best?* The workflow makes the stages run in order, and tests check the results.

### Technical explanation
- **SQL Server** is the transactional source, not the analytical engine.
- **Python export to Parquet** creates columnar exchange files. This local export was performed separately from the Job.
- **Bronze Delta** persists the extracted source with minimum transformations for traceability.
- **Silver Delta** casts, validates, and deduplicates by business key; a production-grade incremental update should use `MERGE`.
- **dbt staging** declares upstream Silver sources through `source()` and produces reusable views.
- **dbt intermediate** joins Orders to Order Items at **order-item grain** and calculates `line_amount = quantity * unit_price`.
- **dbt marts** aggregate that shared detail into daily, customer, and product outputs using `ref()` dependencies.
- **dbt tests / reconciliation** check keys and consistent totals. **Lakeflow Jobs** orchestrates independent notebook/dbt tasks.

---

## 2. Source data and business grain

| Source | What a row represents | Key / typical columns | Business use |
|---|---|---|---|
| Customers | One customer | `customer_id` | Customer dimension |
| Orders | One order (or later version of it) | `order_id`, `customer_id`, `order_date`, `order_status`, `updated_at` | Order status and date |
| Products | One product | `product_id` | Product dimension |
| Order Items | One line of an order | line key *or* correctly defined composite key, `order_id`, `product_id`, `quantity`, `unit_price` | **Merchandise revenue** |
| Payments | One payment record | `payment_id`, `order_id`, status/amount fields per your schema | Payment audit / future reconciliation |

**Grain is critical.** An order with three items creates three joined rows. Use `COUNT(DISTINCT order_id)` for number of orders; do not `COUNT(*)` and call it order count. Avoid joining payments to order items in the revenue model without accounting for multiple payments per order: this can multiply amounts.

**Project-specific correction:** The Orders table did **not** contain a column called `amount`. Use Order Items to calculate merchandise revenue.

---

## 3. Step A — Python extracts SQL Server data to Parquet

**Why:** Separate the transactional database from downstream analytics. Parquet is compact, typed, columnar, and read efficiently by Spark.

**Where:** VS Code / Windows Python (outside your current three-task Lakeflow Job).

**Example/template only:** Replace connection details; never commit credentials. This shows the *logic*, not your exact original export script.

---

```python
# TEMPLATE: run locally after configuring a secure SQL Server connection.
# Requires: pandas, sqlalchemy, pyarrow and a suitable SQL Server DB driver.
# Do not put passwords in your GitHub repository.

# import pandas as pd
# from sqlalchemy import create_engine
# from pathlib import Path
# engine = create_engine(<get-connection-from-environment-or-secret-store>)
# output = Path('exports'); output.mkdir(exist_ok=True)
# for name in ['customers', 'orders', 'products', 'order_items', 'payments']:
#     df = pd.read_sql(f'SELECT * FROM dbo.{name}', engine)
#     df.to_parquet(output / f'{name}.parquet', index=False)
#     print(name, len(df))
```

---

## 4. Step B — Bronze ingestion (Databricks notebook / Job Task 1)

**Why:** Keep a durable copy of extracted source records, separate from cleansing. Raw landing makes troubleshooting, replays, and audit comparisons easier.

**Where:** Databricks notebook; files were placed in a Unity Catalog Volume.

**Static first-load sample:** `errorifexists` creates a table once and fails if it exists. The `tableExists` guard makes a *static initial-load notebook* safe to rerun, but it does **not** ingest new files after initial creation.

---

```python
# Template, adapted to your actual Volume location
from pyspark.sql import functions as F
catalog = "workspace"
volume_path = "/Volumes/workspace/YOUR_SCHEMA/YOUR_VOLUME"  # replace

for name in ["customers", "orders", "products", "order_items", "payments"]:
    path = f"{volume_path}/{name}.parquet"
    table_name = f"{catalog}.retail_bronze.bronze_{name}"
    if not spark.catalog.tableExists(table_name):
        bronze_df = (spark.read.format("parquet").load(path)
                     .withColumn("bronze_ingestion_timestamp", F.current_timestamp()))
        bronze_df.write.format("delta").mode("errorifexists").saveAsTable(table_name)
    else:
        print("Static source already loaded; skipped:", table_name)
```

---

**Key syntax:** `spark.read.parquet` reads columnar data; `withColumn` adds audit metadata; `format("delta")` creates a transactional Delta table; `saveAsTable` registers it in Unity Catalog.

**In your project:** Reported Bronze row counts were Customers 5, Orders 6, Products 5, Order Items 7, Payments 6.

---

## 5. Step C — Silver cleaning and deduplication (Job Task 2)

**Why:** Downstream reports should use typed, valid, deduplicated business records rather than the untouched raw source.

**Where:** Databricks PySpark notebook. **Important:** Your real Job Task 2 may still be validation-only or contain overwrite code; the precise code needs review before describing it as a fully incremental production transformation.

**Correct Orders schema:** `order_id`, `customer_id`, `order_date`, `order_status`, `updated_at` — **no `amount`**.

---

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders_raw = spark.table("workspace.retail_bronze.bronze_orders")
clean_orders = (orders_raw
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
    .filter(F.col("order_id").isNotNull() &
            F.col("customer_id").isNotNull() &
            F.col("order_date").isNotNull() &
            F.col("updated_at").isNotNull()))

w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
latest_orders = (clean_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn"))

# Training-stage full refresh only; DON'T routinely overwrite an incremental target:
# latest_orders.write.format("delta").mode("overwrite").saveAsTable(
#    "workspace.retail_silver.silver_orders")
```

---

**How `ROW_NUMBER` works:** Within each `order_id`, order versions newest-first; keep row 1. Add a deterministic second sort field when `updated_at` ties. Validate business keys per table; `dropDuplicates()` on entire rows **does not guarantee** one row per key. Order Items may require `order_item_id` or a verified composite key.

---

## 6. Delta `MERGE` — change data without full refresh

**Business story:** Order 101 is corrected, and order 1007 arrives. The target should update 101 and insert 1007 **once**; repeated processing of the identical batch must not add rows.

**Why:** Rewriting every Silver row is expensive and can undo earlier incremental changes. `MERGE` compares incoming records to a Delta target by stable business key.

**Safe demo pattern:** Use separate `retail_demo` tables, a fixed batch timestamp, and do not modify live `retail_silver`. The sample assumes the known Orders columns and includes a check before mutation.

---

```python
# DEMONSTRATION TEMPLATE: run in a new notebook; this creates/updates DEMO table only.
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.retail_demo")
src_name = "workspace.retail_silver.silver_orders"
demo_name = "workspace.retail_demo.orders_merge_demo"

if not spark.catalog.tableExists(demo_name):
    spark.table(src_name).write.format("delta").mode("errorifexists").saveAsTable(demo_name)

original = spark.table(src_name)
base = original.orderBy("order_id").first().asDict()
new_id = original.agg(F.max("order_id")).first()[0] + 1000
batch_ts = datetime(2027, 1, 1, 0, 0, 0) # fixed for repeatability; advance if source has later timestamp
updated = {**base, "updated_at": batch_ts}
inserted = {**base, "order_id": new_id, "updated_at": batch_ts}
batch = spark.createDataFrame([updated, inserted], schema=original.schema)

(DeltaTable.forName(spark, demo_name).alias("t")
 .merge(batch.alias("s"), "t.order_id = s.order_id")
 .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
 .whenNotMatchedInsertAll()
 .execute())

check = spark.table(demo_name)
assert check.groupBy("order_id").count().filter("count > 1").count() == 0
assert check.count() == original.count() + 1
```

---

**Caution:** This Orders-only demo illustrates idempotent row-upsert behavior for a fixed batch *if the source timestamps are earlier*. It does **not** demonstrate a revenue correction: revenue is in Order Items. For a true business end-to-end incremental demo, add a new item for the inserted order and modify the quantity or price on an existing item, using the verified item key, then rebuild the dbt marts and inspect changed totals. The exact `silver_order_items` schema/key must be confirmed before providing that mutation.

---

## 7. Why use dbt when we already have PySpark?

**PySpark strengths:** distributed ingestion/cleaning, typed conversions, file-oriented data processing, Delta Lake operations.

**dbt strengths:** SQL transformations organized as reusable models, dependency graph, materializations, declarative tests, lineage, documentation, environment configuration.

In your final architecture, **dbt owns the reporting marts**. Your existing PySpark Gold tables can be retained as a comparison exercise; don't independently maintain two production versions of the same KPI without a reason.

---

## 8. dbt project structure (VS Code; copied to Databricks for Job Task 3)

```text
retail_analytics/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_products.sql
│   │   └── stg_payments.sql
│   ├── intermediate/int_sales.sql
│   └── marts/
│       ├── mart_daily_revenue.sql
│       ├── mart_customer_sales.sql
│       ├── mart_product_sales.sql
│       └── schema.yml
└── tests/reconcile_mart_revenue.sql
```

**`source()`** declares a table that already exists outside dbt (Silver). **`ref()`** points to another dbt model and establishes build order. The previous issue where the reconciliation SQL was under `models/tests/` caused dbt to treat it as a *model*, not a test; keep custom tests under project-root `tests/`.

---

### `models/staging/sources.yml` — declare Silver sources

```yaml
version: 2
sources:
  - name: retail_silver
    database: workspace
    schema: retail_silver
    tables:
      - name: silver_orders
      - name: silver_customers
      - name: silver_order_items
      - name: silver_products
      - name: silver_payments
```

### `models/staging/stg_orders.sql` — dbt view over Orders

```sql
{{ config(materialized='view') }}
SELECT order_id, customer_id, CAST(order_date AS DATE) AS order_date,
       UPPER(TRIM(order_status)) AS order_status,
       CAST(updated_at AS TIMESTAMP) AS updated_at
FROM {{ source('retail_silver', 'silver_orders') }}
```

Other staging models follow the same pattern for Customers, Order Items, Products, and Payments. **Why views?** No second persisted physical copy is necessary at the simple staging layer; results are built on querying the Silver source. For expensive transforms or performance needs, materializations may differ.

---

### `models/intermediate/int_sales.sql` — row grain and revenue logic

```sql
{{ config(materialized='view') }}
SELECT o.order_id, o.customer_id, o.order_date,
       i.product_id, i.quantity, i.unit_price,
       i.quantity * i.unit_price AS line_amount
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} i
  ON o.order_id = i.order_id
WHERE UPPER(TRIM(o.order_status)) = 'SUCCESS'
  AND i.quantity > 0 AND i.unit_price >= 0
```

**Why a shared intermediate?** All reporting marts use the same line-revenue definition. If Orders uses `COMPLETED` instead of `SUCCESS`, update the filter consistently throughout the project. This model assumes no duplicate order-item business keys and no fan-out on Orders.

---

### Three dbt Gold marts — complete SQL examples

**Daily revenue (`models/marts/mart_daily_revenue.sql`)**
```sql
{{ config(materialized='table') }}
SELECT CAST(order_date AS DATE) AS order_day,
       COUNT(DISTINCT order_id) AS total_orders,
       SUM(line_amount) AS total_revenue
FROM {{ ref('int_sales') }}
GROUP BY CAST(order_date AS DATE)
```

**Customer sales (`models/marts/mart_customer_sales.sql`)**
```sql
{{ config(materialized='table') }}
WITH r AS (
  SELECT customer_id, COUNT(DISTINCT order_id) AS total_orders,
         SUM(line_amount) AS total_revenue
  FROM {{ ref('int_sales') }}
  GROUP BY customer_id
)
SELECT c.customer_id,
       COALESCE(r.total_orders,0) AS total_orders,
       COALESCE(r.total_revenue,0) AS total_revenue,
       CASE WHEN COALESCE(r.total_orders,0) > 0
            THEN ROUND(r.total_revenue / r.total_orders,2)
            ELSE 0 END AS avg_order_value
FROM {{ ref('stg_customers') }} c LEFT JOIN r ON c.customer_id=r.customer_id
```

**Product sales (`models/marts/mart_product_sales.sql`)**
```sql
{{ config(materialized='table') }}
WITH r AS (
  SELECT product_id, SUM(quantity) AS total_quantity,
         COUNT(DISTINCT order_id) AS total_orders,
         SUM(line_amount) AS total_revenue
  FROM {{ ref('int_sales') }} GROUP BY product_id
)
SELECT p.product_id, COALESCE(r.total_quantity,0) AS total_quantity,
       COALESCE(r.total_orders,0) AS total_orders,
       COALESCE(r.total_revenue,0) AS total_revenue
FROM {{ ref('stg_products') }} p LEFT JOIN r ON p.product_id=r.product_id
```

**Why a `LEFT JOIN` from Customers/Products?** Those dimension members should still appear even when they have no successful sales. The `stg_customers`/`stg_products` IDs must be unique or totals could be multiplied.

---

## 9. Tests and reconciliation

**Generic dbt tests** in `schema.yml` check `unique`, `not_null`, `relationships`. In your earlier run, the 13 tests associated with the three marts passed. Newer dbt syntax nests `relationships` properties under `arguments:`.

Example:
```yaml
version: 2
models:
  - name: mart_customer_sales
    columns:
      - name: customer_id
        data_tests:
          - unique
          - not_null
          - relationships:
              arguments:
                to: ref('stg_customers')
                field: customer_id
```

**Custom singular test — place in `tests/reconcile_mart_revenue.sql`:** This query **must return zero rows** to pass.
```sql
WITH totals AS (
  SELECT 'detail' AS model_name, COALESCE(SUM(line_amount),0) AS revenue
  FROM {{ ref('int_sales') }}
  UNION ALL
  SELECT 'daily', COALESCE(SUM(total_revenue),0) FROM {{ ref('mart_daily_revenue') }}
  UNION ALL
  SELECT 'customer', COALESCE(SUM(total_revenue),0) FROM {{ ref('mart_customer_sales') }}
  UNION ALL
  SELECT 'product', COALESCE(SUM(total_revenue),0) FROM {{ ref('mart_product_sales') }}
), reference AS (
  SELECT revenue AS expected_revenue FROM totals WHERE model_name='detail'
)
SELECT t.model_name, t.revenue, r.expected_revenue
FROM totals t CROSS JOIN reference r
WHERE ABS(t.revenue-r.expected_revenue)>0.01
```

**Last verified business totals:** Daily $1,350; Customer $1,350; Product $1,350. This shows consistent totals, but not necessarily record-level reconciliation against SQL Server.

---

## 10. Key dbt commands and why they exist

```powershell
dbt debug                              # Check project/profile and Databricks connection
dbt ls                                 # Show recognized models/tests
dbt run --select stg_orders            # Build one model
dbt build --select +mart_daily_revenue # Build upstream models + mart + tests
dbt test --select reconcile_mart_revenue  # Run custom singular test
dbt build                              # Build all models, then execute tests
dbt docs generate                      # Generate catalog/manifest documentation
dbt docs serve                         # Serve local lineage/docs in browser
```

Your earlier full build initially failed due to dbt's *default starter example model*, not the retail marts. The custom test was initially stored under `models/tests` and therefore did not run as a test. Those are useful troubleshooting stories.

**Job configuration issue you resolved:** A dbt Job was attempting Hive Metastore and needed the explicit Unity Catalog (`workspace`) and destination schema (`retail_dbt`). Avoid unnecessary `dbt seed` when there are no CSV seeds.

---

## 11. Lakeflow Jobs — automation and dependencies

**Job:** `retail_end_to_end_pipeline`

**Reported working three-task order**:
1. `bronze_ingestion` — notebook; check/read file input and Bronze tables
2. `silver_transformation` — notebook; current contents must be reviewed for overwrite vs validation-only behavior
3. `dbt_gold` — dbt project; depends on Silver; builds models and tests

**Optional fourth task (not yet confirmed):** `validate_pipeline` notebook — checks table existence, duplicate keys, and mart totals; depends on `dbt_gold`.

**Why dependencies matter:** dbt cannot create reporting marts from yesterday's Silver if the new Silver update hasn't completed. Jobs give ordered execution, task-level logs, retries, and observability. **A successful Job is proof the configured code executed**, not proof new files are being ingested if Task 1 only skips existing tables.

**Job image for GitHub:** Capture the actual Job graph with succeeded tasks, removing any credentials/identifiers you don't want public.

---

## 12. Project validation notebook — safe read-only code

This can run in a separate Notebook or become a fourth Job task. It does **not** modify live tables.

---

```python
from pyspark.sql import functions as F
catalog = "workspace"
models = ["mart_daily_revenue", "mart_customer_sales", "mart_product_sales"]
totals = {}
for model in models:
    name = f"{catalog}.retail_dbt.{model}"
    total = spark.table(name).agg(F.sum("total_revenue")).first()[0]
    assert total is not None, f"No total in {name}"
    totals[model] = total
    print(model, total)
assert max(totals.values()) - min(totals.values()) <= 0.01

orders = spark.table(f"{catalog}.retail_silver.silver_orders")
dups = orders.groupBy("order_id").count().filter("count > 1").count()
assert dups == 0, f"Found {dups} duplicate order IDs"
print("PASS: marts reconcile and Silver Orders IDs are unique")
```

---

## 13. Debugging stories from this project

| Error or surprise | Root cause | Correct lesson |
|---|---|---|
| `UNRESOLVED_COLUMN amount` | `bronze_orders` doesn't contain `amount` | Use `quantity * unit_price` after joining order items |
| `errorifexists` fails | Target table already exists | Make initial-load logic explicit; don't overwrite by accident |
| `dbt_project.yml not found` | CLI run from wrong folder | Run in dbt project root |
| `Nothing to do` for reconciliation test | File placed under `models/tests` and treated as model | Place custom SQL test in root `tests/` |
| Starter example `not_null` test fails | Demo/example model still included | Remove unused starter models |
| Hive Metastore disabled | Job catalog not configured | Use Unity Catalog (`workspace`) and desired schema |
| `dbt seed` no-op | No CSV seed data | This is fine; use `dbt build` for project models/tests |

**Production improvements still open:** Stable batch identifiers, incremental file discovery/Auto Loader, quarantine records, deterministic duplicate tie-breaks, merge matched/inserted metrics, alerting, and comparison to transactional source totals.

---

## 14. Five-minute interview talk track

**00:00–00:45: Problem.** “I built a retail analytics platform to report daily revenue, customer spending, and product performance from a SQL Server source.”

**00:45–01:30: Ingestion.** “A local Python step exported five relational tables to Parquet. Databricks read those files and persisted them as Bronze Delta tables for traceability.”

**01:30–02:15: Silver.** “I used PySpark for type enforcement, null filtering, standardized statuses, and latest-record deduplication using window functions. Delta MERGE is the pattern for incremental upserts; I built a separate demo and am validating full job integration.”

**02:15–03:30: dbt.** “I exposed Silver through dbt `source()` models, used `ref()` dependencies to join valid orders and order items, and defined line revenue at item grain. Three marts aggregate that metric by date, customer, and product.”

**03:30–04:15: Quality and orchestration.** “dbt tests validate unique/non-null keys and relationships; a revenue reconciliation query showed $1,350 in all three marts. Lakeflow executes the Bronze, Silver and dbt tasks in dependency order.”

**04:15–05:00: Tradeoffs.** “Source extraction is not yet part of Lakeflow; the first Bronze task is a static load. My planned next improvement is automatic file ingestion, a fully idempotent item-level MERGE, and failure alerts.”

**Do not describe an enhancement as deployed until you've executed and validated it.**

---

## 15. Interview self-check

Try explaining these without reading the answer: (1) Why Parquet? (2) Why Delta rather than plain Parquet? (3) What is Bronze vs Silver? (4) How does ROW_NUMBER deduplicate? (5) Why isn't `amount` in Orders? (6) What happens when an order has two items? (7) Why dbt `source()` versus `ref()`? (8) Why use `LEFT JOIN` in customer and product marts? (9) Why can matching grand totals still hide record-level mismatches? (10) Why doesn't the current static Bronze task support ongoing incremental ingestion?

### What to show an interviewer in GitHub
- `databricks/` exported *actual, cleaned* notebooks
- `dbt/` your actual SQL/YAML/tests; no `.venv`, tokens, profiles, `target`, or logs
- `docs/data_flow.png`, redacted successful Job graph, dbt test result, revenue comparison
- A README stating what was run, what remains a demo, and how to reproduce the project

**Next action:** Compare this reconstruction to your actual exported notebook files. Replace snippets where your exact code differs. Include this learning notebook as documentation, not as an executable pipeline notebook.
