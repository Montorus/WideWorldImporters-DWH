# ETL Pipeline Flow

## 1. Source Systems

| Source | Technology | Current Extracted Data |
|---|---|---|
| WideWorldImporters | MSSQL | Customers |
| AdventureWorks | PostgreSQL | Sales orders, sales details, products, employees |

## 2. Airflow Orchestration

Airflow is responsible for scheduled extraction and loading into the PostgreSQL data warehouse.

| DAG | Responsibility | Target RAW Tables |
|---|---|---|
| `etl_mssql_customers` | Extract customers from MSSQL WideWorldImporters | `raw_customers` |
| `etl_postgresql_adventureworks` | Extract AdventureWorks PostgreSQL tables | `raw_sales_orders`, `raw_sales_details`, `raw_products`, `raw_employees` |

Current schedule target is every 15 minutes with retry handling configured in the DAG defaults.

### Airflow Connection IDs

| Connection ID | Docker Environment Variable | Target Service | Used By |
|---|---|---|---|
| `mssql_source` | `AIRFLOW_CONN_MSSQL_SOURCE` | `mssql_source:1433/WideWorldImporters` | `etl_mssql_customers` |
| `postgres_source` | `AIRFLOW_CONN_POSTGRES_SOURCE` | `postgres_source:5432/${POSTGRES_SOURCE_DB}` | `etl_postgresql_adventureworks` |
| `postgres_dwh` | `AIRFLOW_CONN_POSTGRES_DWH` | `postgres_dwh:5432/${POSTGRES_DWH_DB}` | Both ETL DAGs |

## 3. RAW Layer

The RAW layer stores source-shaped data with minimal transformation. Airflow truncates and reloads the current raw tables.

| RAW Table | Source Table |
|---|---|
| `raw_customers` | `Sales.Customers` |
| `raw_sales_orders` | `sales.salesorderheader` |
| `raw_sales_details` | `sales.salesorderdetail` |
| `raw_products` | `production.product` |
| `raw_employees` | `humanresources.employee` |

Pipeline execution metadata is stored in `pipeline_runs`. The metadata table and all RAW tables are created by `sql/queries/init_warehouse.sql`.

## 4. dbt Staging Layer

Staging models are materialized as views. They standardize column names, apply basic null filtering, and expose cleaner inputs for the mart layer.

| Staging Model | Input |
|---|---|
| `stg_customers` | `raw_customers` |
| `stg_sales_orders` | `raw_sales_orders` |
| `stg_sales_details` | `raw_sales_details` |
| `stg_products` | `raw_products` |
| `stg_employees` | `raw_employees` |

## 5. dbt Mart Layer

Mart models are materialized as tables for reporting performance.

| Mart Model | Type | Purpose |
|---|---|---|
| `dim_customers` | Dimension | Customer attributes |
| `dim_products` | Dimension | Product attributes |
| `dim_employees` | Dimension | Employee attributes |
| `dim_date` | Dimension | Date spine from 2010 to 2030 |
| `fact_sales` | Fact | Sales line transactions with measures |

## 6. BI Consumption

Power BI should connect to the mart layer, using `fact_sales` as the central fact table and joining to the dimension tables by their keys.

## Known Follow-Up Items

1. Decide whether customer and sales data should come from the same business source or require a documented mapping.
2. Either implement true SCD Type 2 handling or describe dimensions as current-state dimensions.
3. Add stronger dbt relationship tests for fact-to-dimension integrity.
