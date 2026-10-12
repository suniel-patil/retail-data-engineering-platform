# retail-data-engineering-platform
End-to-end retail analytics pipeline using SQL Server, Python, Databricks, Delta Lake, dbt and Lakeflow Jobs

# Retail Data Engineering Platform

## Project Overview
Built an end-to-end retail analytics pipeline using SQL Server, Python, Databricks, Delta Lake, dbt, and Lakeflow Jobs.

## Architecture
SQL Server → Python → Parquet → Databricks Bronze → PySpark Silver → dbt Staging → Intermediate Sales → Gold Analytics.

## Technologies
- SQL Server
- Python and PySpark
- Databricks and Delta Lake
- dbt Core with dbt-databricks
- Unity Catalog
- Lakeflow Jobs
- GitHub

## Data Pipeline
**Bronze:** Ingests five source datasets into Delta tables.

**Silver:** Applies data type conversions, validation, filtering, and deduplication.

**Gold:** Builds daily revenue, customer sales, and product sales marts using dbt.

**Data Quality:** Uses dbt uniqueness, non-null, relationship, and revenue reconciliation checks.

**Orchestration:** Runs Databricks notebook tasks and dbt transformations in dependency order.

## Validation Results
The three dbt reporting marts reconciled to a total revenue of $1,350 on the sample dataset.

## Project Scope
SQL Server extraction was performed separately. Databricks Lakeflow Jobs orchestrates the notebook and dbt processing stages. Incremental MERGE demonstrations and enhancements are documented separately.

## Screenshots
Add images of the Lakeflow Job task graph, successful dbt execution, and revenue reconciliation.

## Future Enhancements
- Automated file arrival and incremental ingestion
- Advanced monitoring and alerting
- Snowflake Snowpipe, Streams, and Tasks implementation
