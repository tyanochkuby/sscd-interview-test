# Candidate Task: Domain Data Engineer

This repository contains a practical data engineering task.

Your goal is to complete a small local pipeline that:

- ingests raw order events
- transforms them with PySpark
- writes a curated analytics-friendly dataset
- validates the final result locally

The project is intentionally incomplete. You are expected to implement the missing business logic and make reasonable design decisions.

## Scenario

The business domain produces raw order events. In production, these events would arrive as JSON files in S3.

For this task, the same data is provided locally under `data/input/`.

You need to build or complete a pipeline that:

- loads raw order events
- derives the latest state for each order
- produces a curated dataset suitable for analytics and future upserts
- supports a downstream query such as: top 5 customers by shipped order amount

## What You Should Do

### 1. Orchestration and ingest

Create or complete a Mage pipeline with three blocks:

- Data Loader
- Transformer
- Data Exporter

The input data includes fields such as:

- `order_id`
- `customer_id`
- `event_timestamp`
- `status`
- `total_amount`

### 2. Transformation and domain logic

Using PySpark, implement the transformation logic to:

- filter invalid transactions where `total_amount <= 0`
- keep only the latest status per order using `event_timestamp`
- optionally join a small customer dictionary

### 3. Save the curated result

Assume the curated output should eventually live in a lakehouse table.

Choose and be ready to explain either `Iceberg` or `Hudi` for a production version of this pipeline.

Key points to think through:

- how you would support upserts when order status changes
- how you would retain historical query capability
- why your chosen table format fits those needs

This starter writes Parquet locally by default so the task can run without cloud setup. If your local environment supports it, you can replace or extend that with Iceberg or Hudi, but it is also acceptable to keep the local Parquet output and explain how you would evolve it in production.

Expected target shape:

- business key: `order_id`
- partition concept: `order_date`

### 4. Analytics and consumption

Assume the curated table is later registered in AWS Glue and queried from Athena.

You should be able to produce or explain a query such as:

- top 5 customers by shipped order amount

You should also be ready to discuss:

- partitioning strategy
- Parquet and compression choices
- how to reduce scanned data volume in Athena
- how those choices affect BI tools such as Sisense

## What Is Already Provided

- sample raw input data
- sample customer dimension data
- a minimal Mage-like project structure
- a reusable PySpark transformation module with `TODO` markers
- a local runner for the pipeline
- a validator for the expected business outcome

## What Is Intentionally Missing

- final deduplication logic
- final join logic
- final curated output model
- final export to Iceberg or Hudi
- Athena DDL and production AWS wiring

## Project Structure

```text
.
├── data/
│   ├── input/
│   │   ├── customers.json
│   │   └── orders_events.jsonl
│   └── output/
├── mage_project/
│   ├── metadata.yaml
│   └── order_events_pipeline/
│       ├── data_exporter.py
│       ├── data_loader.py
│       └── transformer.py
├── scripts/
│   ├── run_local_pipeline.py
│   └── validate_results.py
├── src/
│   └── interview_task/
│       ├── config.py
│       ├── schemas.py
│       └── transform.py
├── pyproject.toml
└── README.md
```

## Where To Work

Primary implementation points:

- `src/interview_task/transform.py`
- optionally `mage_project/order_events_pipeline/*.py`
- optionally `scripts/run_local_pipeline.py`

## Quick Start

### Prerequisites

- `uv`
- Python `3.14`
- Java `21` for local PySpark runs

Spark `4.1.1` supports Java `17` and `21`, with Python `3.10+`. Use JDK `21` rather than a newer unsupported Java release.

### 1. Install dependencies

```bash
uv sync
```

This creates a local `.venv` from `pyproject.toml`.

### 2. Run the local pipeline

```bash
uv run python -m scripts.run_local_pipeline
```

This reads `data/input/orders_events.jsonl` and writes output to `data/output/curated_orders`.

Initially, the output will likely be incomplete because part of the task is to finish the implementation in `src/interview_task/transform.py`.

### 3. Validate the result

```bash
uv run python -m scripts.validate_results
```

The validator checks whether your curated dataset satisfies the core business expectations.

## Helpful Commands

```bash
uv sync
uv run python -m scripts.run_local_pipeline
uv run python -m scripts.validate_results
uv run pytest
```

## Suggested Approach

1. Read the task and inspect the input data.
2. Complete the PySpark transformation logic.
3. Run the local pipeline.
4. Validate the output.
5. Be ready to explain how you would move from local Parquet to Iceberg or Hudi on S3.
6. Be ready to write or discuss the Athena query for the analytical use case.

## Example Athena Query

Something like the following should work for the downstream use case:

```sql
SELECT
  customer_id,
  SUM(total_amount) AS shipped_revenue
FROM curated_orders
WHERE status = 'SHIPPED'
GROUP BY customer_id
ORDER BY shipped_revenue DESC
LIMIT 5;
```

## What Good Output Looks Like

By the end, you should have:

- a runnable local pipeline
- correct latest-state modeling per `order_id`
- invalid transactions filtered out
- a curated output dataset that passes validation
- a clear explanation of how you would support production upserts and Athena querying
