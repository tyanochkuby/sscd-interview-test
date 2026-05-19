# Domain Data Engineer Interview Task

This repository is a starter kit for a practical interview task around domain data engineering.

It gives the candidate a ready-to-run structure for:

- orchestration in Mage
- transformation in PySpark
- writing curated data in a lakehouse-friendly table format
- validating the final analytical result locally

It is intentionally **not a finished solution**. The candidate should complete the business logic and make design decisions during the interview.

## Scenario

The business domain produces raw order events. These events land as JSON files in S3.

The task is to build a simple pipeline in Mage that:

- loads raw order events
- transforms them with PySpark
- writes the curated result in a table format suitable for analytics and updates
- exposes data that could later be queried from Athena and used in Sisense

For local interview work, this repository uses files under `data/input/` instead of real S3.

## What the candidate should do

### Step A: Orchestration and ingest

Create or complete a Mage pipeline with three blocks:

- Data Loader
- Transformer
- Data Exporter

Input data contains fields such as:

- `order_id`
- `customer_id`
- `event_timestamp`
- `status`
- `total_amount`

### Step B: Transformation and domain logic

Using PySpark, implement the transformation logic:

- filter invalid transactions where `total_amount <= 0`
- keep only the latest status per order using `event_timestamp`
- optionally join a small customer dictionary

### Step C: Save to lakehouse table

Discuss and choose **Iceberg** or **Hudi**.

The intended discussion point is:

- how to support upserts for changing order status
- how to retain historical query capabilities
- why the chosen table format fits those needs

In this starter, the export step writes Parquet locally by default so the exercise is runnable without cloud setup. The candidate can replace or extend it with Iceberg/Hudi if the local environment supports it.

Expected target shape:

- primary business key: `order_id`
- partitioning concept: `order_date`

### Step D: Consumption and analytics

Assume the curated table is later registered in AWS Glue and queried from Athena.

The candidate should be able to produce a query such as:

- top 5 customers by shipped order amount

Discussion topic:

- how to optimize Athena/S3-side performance for BI tools like Sisense

Expected intuition:

- proper partitioning
- Parquet + compression
- avoiding `SELECT *`
- controlling scanned data volume

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

## What is already provided

- sample raw input data
- sample customer dimension data
- a minimal Mage-like project structure
- a reusable PySpark transformation module with `TODO` markers
- a local runner for the pipeline
- a validator for the expected business outcome

## What is intentionally missing

- final deduplication logic
- final join logic
- final curated output model
- final export to Iceberg/Hudi
- Athena DDL and production AWS wiring

## Quick Start

### Prerequisites

- `uv` installed
- Python `3.14`
- Java `21` for local PySpark runs

Note: Spark `4.1.1` documents support for Java `17` and `21`, with Python `3.10+`. Use JDK `21` rather than newer unsupported feature releases.

### 1. Install dependencies with uv

```bash
uv sync
```

This creates a local `.venv` based on `pyproject.toml`.

The project expects Python `3.14`, which is pinned in `.python-version`.

### 2. Run the local pipeline

```bash
uv run python -m scripts.run_local_pipeline
```

This reads `data/input/orders_events.jsonl` and writes output to `data/output/curated_orders`.

At the start, the output will likely be incomplete because the candidate is expected to finish the implementation in `src/interview_task/transform.py`.

### 3. Validate the result

```bash
uv run python -m scripts.validate_results
```

The validator checks whether the curated dataset satisfies the core business expectations.

## Where the candidate should work

Primary implementation points:

- `src/interview_task/transform.py`
- optionally `mage_project/order_events_pipeline/*.py`
- optionally `scripts/run_local_pipeline.py`

## Helpful uv commands

```bash
uv sync
uv run python -m scripts.run_local_pipeline
uv run python -m scripts.validate_results
uv run pytest
```

## Suggested interview flow

1. Read the task and inspect input data.
2. Complete the PySpark transformation.
3. Run the local pipeline.
4. Validate the result.
5. Explain how the local Parquet export would become Iceberg or Hudi on S3.
6. Write or discuss the Athena query for the analytical use case.

## Example Athena analytical query

The candidate can discuss or write something equivalent to:

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

## Evaluation Areas

- PySpark fluency
- reasoning about domain events and latest-state modeling
- understanding of upserts and historical tables
- clarity around partitioning and file formats
- ability to connect storage design with Athena/Sisense query performance

## Notes for interviewer

- The repository is runnable locally without AWS.
- If desired, the interviewer can ask the candidate to explain the changes needed for real S3, Glue, Athena, and Iceberg/Hudi registration.
- The candidate does not need to spend time scaffolding the project from zero.
