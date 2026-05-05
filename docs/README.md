# Project Documentation

This directory contains supporting documentation and reporting artifacts for the WideWorldImporters Data Warehouse project.

## Contents

| Path | Purpose |
|---|---|
| `etl/pipeline_flow.md` | End-to-end ETL flow from source systems to Power BI marts |
| `WideWorldImporters-PowerBI.pbix` | Power BI report connected to the warehouse mart layer |

## Main Technical Areas

1. Source extraction is orchestrated by Airflow DAGs in `airflow/dags/`.
2. Raw data is loaded into PostgreSQL DWH tables in the `public` schema.
3. dbt transforms raw data into staging views and mart tables.
4. Power BI consumes the mart layer, primarily `fact_sales` and related dimensions.

## Operational Notes

- Warehouse initialization SQL is stored in `sql/queries/init_warehouse.sql`.
- Docker runtime configuration is stored under `docker/`.
- Airflow connection IDs are provided through `AIRFLOW_CONN_*` environment variables in `docker/docker-compose.yml`.
- Airflow DAG import tests are stored in `tests/test_airflow_dags.py`.
- Generated runtime logs should not be treated as documentation or source code.
