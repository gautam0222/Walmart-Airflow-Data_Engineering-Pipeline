# Walmart Data Engineering Project

This repository contains an end-to-end Walmart analytics pipeline built with dbt, Databricks, and Apache Airflow.

The project ingests source data from Databricks bronze tables, transforms it through dbt staging and business layers, and publishes final gold outputs such as snapshots and a fact table. Airflow is used to orchestrate the workflow and dbt is used for the transformation logic.

## Overview

The pipeline follows a medallion-style flow:

1. Bronze data already exists in Databricks under the `walmart_db_gautam.bronze` schema.
2. dbt `silver_technical` models pull from the source tables and standardize them with incremental processing.
3. dbt `silver_business` builds an operational business table (OBT) by joining the technical models.
4. dbt `gold` models create ephemeral helper datasets, snapshots for dimension history, and a final sales fact table.
5. Airflow orchestrates ingestion, freshness checks, dbt runs, dbt tests, and snapshots.

## Tech Stack

- Python 3.12
- Apache Airflow 3.3
- dbt Core 1.12
- dbt Databricks 1.12
- Databricks SQL Warehouse / Databricks SDK

## Repository Structure

```text
.
├── main.py
├── pyproject.toml
├── requirements.txt
├── airflow/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   ├── config/
│   │   └── airflow.cfg
│   └── dags/
│       ├── orchestrate.py
│       └── test.py
├── logs/
├── walmart_dataset/
│   ├── data/
│   │   ├── customers.csv
│   │   ├── employees.csv
│   │   ├── order_items.csv
│   │   ├── orders.csv
│   │   ├── products.csv
│   │   └── stores.csv
│   └── ddl/
│       └── walmart_schema.sql
└── walmart_project/
	├── dbt_project.yml
	├── profiles.yml
	├── README.md
	├── analyses/
	├── macros/
	│   └── custom_schema.sql
	├── models/
	│   ├── source/
	│   │   └── sources.yml
	│   ├── silver_technical/
	│   │   ├── customers_t.sql
	│   │   ├── employees_t.sql
	│   │   ├── order_items_t.sql
	│   │   ├── orders_t.sql
	│   │   ├── products_t.sql
	│   │   ├── stores_t.sql
	│   │   └── properties.yml
	│   ├── silver_business/
	│   │   └── obt_b.sql
	│   └── gold/
	│       ├── ephemeral/
	│       │   ├── eph_customers.sql
	│       │   ├── eph_employees.sql
	│       │   ├── eph_orders.sql
	│       │   ├── eph_products.sql
	│       │   └── eph_stores.sql
	│       └── fact/
	│           └── fact_orders.sql
	├── snapshots/
	│   ├── dim_customers.yml
	│   ├── dim_employees.yml
	│   ├── dim_orders.yml
	│   ├── dim_products.yml
	│   └── dim_stores.yml
	└── tests/
		└── test_obt.sql
```

Generated artifacts such as `target/`, `dbt_packages/`, `airflow/logs/`, and run outputs are created during execution and are not the source of truth for the project logic.

## Data Flow

### 1. Source Layer

The dbt source definition points to Databricks tables in the `bronze` schema:

- `orders`
- `customers`
- `products`
- `order_items`
- `stores`
- `employees`

### 2. Silver Technical Layer

The `silver_technical` models are the first dbt transformation step. They:

- read directly from the source tables,
- add processing metadata such as `processed_at`,
- apply incremental logic where needed,
- standardize raw data for downstream joins and tests.

Example models:

- `orders_t`
- `customers_t`
- `employees_t`
- `order_items_t`
- `products_t`
- `stores_t`

### 3. Silver Business Layer

The `obt_b` model creates a wide business table by joining the technical models into a single OBT-style dataset. This gives downstream analytics one table with order, customer, product, store, and employee context.

### 4. Gold Layer

The gold layer contains three parts:

- Ephemeral helper models (`eph_*`) that select and deduplicate fields from the OBT.
- Snapshots (`dim_*.yml`) that preserve slowly changing dimension history.
- A final fact model (`fact_orders`) that exposes the main order grain for reporting.

## Airflow Orchestration

The main DAG is defined in [airflow/dags/orchestrate.py](airflow/dags/orchestrate.py).

Task flow:

1. `ingest_cdc` triggers a Databricks job using the Databricks SDK and waits for completion.
2. `source_freshness` runs `dbt source freshness`.
3. `silver_technical` runs the silver technical models.
4. `silver_technical_tests` runs tests for the silver technical layer.
5. `silver_business` builds the OBT.
6. `silver_business_tests` runs tests for the business layer.
7. `gold_ephemeral` builds the gold ephemeral models.
8. `gold_dimensions` runs `dbt snapshot` for the dimension history tables.
9. `gold_fact` builds the final fact model.

The DAG is scheduled daily at midnight UTC and has `catchup=False`.

## dbt Configuration

Key dbt settings live in [walmart_project/dbt_project.yml](walmart_project/dbt_project.yml) and [walmart_project/profiles.yml](walmart_project/profiles.yml).

Important configuration details:

- dbt profile name: `walmart_project`
- target schema for local runs: `dbt_schema`
- Databricks catalog: `walmart_db_gautam`
- warehouse path: configured in `profiles.yml`
- Databricks token: loaded from the `DATABRICKS_TOKEN` environment variable

Model materialization rules:

- `silver_technical`: table
- `silver_business`: table
- `gold`: table
- `gold.ephemeral`: ephemeral

## Local Setup

### Prerequisites

- Python 3.12
- Databricks workspace access
- A Databricks SQL warehouse
- Docker and Docker Compose for Airflow

### Install Dependencies

From the repository root:

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

If you want the Airflow environment specifically, use the Airflow requirements file inside the `airflow/` folder as well.

### Environment Variables

dbt expects a Databricks token in `DATABRICKS_TOKEN`.

Airflow uses values from `.env` or shell environment variables for the Docker Compose stack, including:

- `FERNET_KEY`
- `_AIRFLOW_WWW_USER_USERNAME`
- `_AIRFLOW_WWW_USER_PASSWORD`
- `AIRFLOW__API_AUTH__JWT_SECRET`

## Running dbt

The dbt project is located in [walmart_project](walmart_project).

Common commands:

```bash
cd walmart_project
dbt debug
dbt deps
dbt source freshness
dbt run
dbt test
dbt snapshot
```

To run a specific layer:

```bash
dbt run --select silver_technical
dbt run --select silver_business
dbt run --select gold_fact
```

## Running Airflow Locally

From the [airflow](airflow) directory:

```bash
docker compose up --build
```

After the containers start, open the Airflow UI at `http://localhost:8080`.

The DAG should appear as `orchestrate`.

## Data Assets

The repository includes a small Walmart dataset and schema reference under [walmart_dataset](walmart_dataset):

- CSV files for customers, employees, order items, orders, products, and stores
- DDL file for the warehouse schema

## Notes Before Publishing to GitHub

- Replace any hardcoded Databricks credentials in the code with environment variables before making the repository public.
- Do not commit real secrets in `.env` or other config files.
- Treat `target/`, `logs/`, and other generated artifacts as build outputs, not source files.
- The `main.py` file is currently a placeholder entry point and is not part of the pipeline.

## Summary

This project demonstrates a complete analytics engineering workflow:

- Databricks for storage and execution
- dbt for transformation, testing, and snapshots
- Airflow for orchestration and scheduling
- a layered structure that moves from bronze source tables to silver models and then gold outputs

It is ready to be documented, reviewed, and published as a portfolio-style data engineering project.
