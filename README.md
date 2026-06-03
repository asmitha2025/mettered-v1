# Subscription Intelligence Pipeline

Production-style data engineering project for SaaS billing analytics.

This project simulates how a subscription platform turns raw billing events into
business metrics such as MRR, ARR, LTV, churn, cohort retention, and revenue
projection scenarios.

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![PySpark 3.5](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org)
[![PostgreSQL 15](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://postgresql.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-dashboard-009688.svg)](https://fastapi.tiangolo.com)

## Problem

Subscription businesses need reliable answers to questions like:

- How much recurring revenue did we generate this month?
- Which customers are growing, shrinking, or at risk?
- Which plans have the highest churn?
- Are newer customer cohorts retaining better than older cohorts?
- What happens if churn, ARPU, growth, or customer count changes?
- How should analytics pipelines be designed for multi-tenant SaaS data?

This repository answers those questions with an end-to-end analytics pipeline:

```text
Raw CSV events
  -> validation and enrichment
  -> partitioned Parquet
  -> MRR / ARR / LTV transforms
  -> churn and cohort retention transforms
  -> FastAPI dashboard and PostgreSQL analytics schema
  -> interactive revenue simulator
```

## Features

- Generates deterministic synthetic SaaS billing data across 500 tenants.
- Ingests raw billing CSV files, validates required fields, and writes clean
  partitioned Parquet outputs.
- Computes MRR, ARR, MRR growth, rolling MRR, ARPU, and tenant LTV.
- Computes churn by plan and monthly cohort retention.
- Serves results through a FastAPI dashboard API and responsive web dashboard.
- Includes an interactive revenue what-if simulator for growth, churn, ARPU,
  tenant count, and projection window inputs.
- Includes a PostgreSQL schema with tenant-aware tables, indexes, materialized
  view design, and row-level security policies.
- Includes a Kafka producer for real-time billing event simulation.
- Benchmarks Pandas and PySpark tradeoffs.
- Includes pytest coverage, lint configuration, Docker support, and CI workflow.

## Architecture

```text
                         +---------------------------+
                         | Synthetic Billing Events  |
                         | tenants.csv, events.csv   |
                         +-------------+-------------+
                                       |
                                       v
                         +---------------------------+
                         | Job 1: Ingest + Validate  |
                         | schema checks, bad rows   |
                         | clean partitioned Parquet |
                         +-------------+-------------+
                                       |
              +------------------------+------------------------+
              |                                                 |
              v                                                 v
 +-----------------------------+                  +-----------------------------+
 | Job 2: Revenue Metrics      |                  | Job 3: Retention Metrics    |
 | MRR, ARR, ARPU, LTV         |                  | churn, cohort retention     |
 | rolling and growth windows  |                  | 12-month cohort tracking    |
 +---------------+-------------+                  +---------------+-------------+
                 |                                                |
                 +------------------------+-----------------------+
                                          |
                                          v
                         +-----------------------------+
                         | Analytics Serving Layer     |
                         | FastAPI dashboard API       |
                         | PostgreSQL schema design    |
                         | revenue simulator endpoint  |
                         +-----------------------------+

 Kafka producer simulates real-time billing events for the streaming layer.
```

## Project Structure

```text
subscription-intelligence-pipeline/
|-- src/
|   |-- ingestion/
|   |   |-- generate_data.py      # synthetic billing events
|   |   |-- etl_ingest.py         # raw CSV -> validated Parquet
|   |   `-- kafka_producer.py     # streaming event simulation
|   |-- transforms/
|   |   |-- mrr_transform.py      # MRR, ARR, LTV metrics
|   |   `-- churn_cohort.py       # churn and cohort retention
|   |-- analytics/
|   |   `-- benchmark.py          # Pandas vs PySpark benchmark
|   |-- utils/
|   |   |-- filesystem.py         # local output cleanup helpers
|   |   `-- spark_session.py      # Spark session and Pandas fallback
|   `-- web/
|       |-- app.py                # FastAPI dashboard API
|       `-- templates/index.html  # dashboard UI
|-- sql/schema.sql                # PostgreSQL schema, indexes, RLS
|-- tests/
|   |-- test_generation.py
|   |-- test_simulator.py
|   |-- test_spark_session.py
|   |-- test_transforms.py
|   `-- test_web_churn.py
|-- docs/
|   `-- benchmark_results.json
|-- Dockerfile
|-- docker-compose.yml
|-- requirements.txt
|-- setup.cfg
|-- run_pipeline.py
|-- run_dashboard.py
`-- README.md
```

## Quick Start

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Run the full pipeline:

```bash
python run_pipeline.py --records 50000
python view_outputs.py
```

Run a smaller smoke test:

```bash
python run_pipeline.py --records 1000 --skip-benchmark
```

Start the dashboard:

```bash
python run_dashboard.py
```

The launcher starts a FastAPI server on the first available local port from
`8000` onward.

Optional: run the pipeline with a separate generated-data root when the local
repo folder is locked by OneDrive, a running dashboard, or CI cleanup rules.

```powershell
$env:SUBSCRIPTION_PIPELINE_DATA_DIR="C:\tmp\subscription-intelligence-data"
python run_pipeline.py --records 1000 --skip-benchmark
```

## Docker

The Dockerfile builds a self-contained dashboard image. The container generates a
demo dataset and serves the dashboard on port `7860`.

```bash
docker build -t subscription-intelligence-pipeline .
docker run -p 7860:7860 subscription-intelligence-pipeline
```

## API

Key dashboard endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Service health check |
| `GET /api/overview` | Revenue KPIs and trend data |
| `GET /api/tenants` | Tenant-level LTV, MRR, and growth data |
| `GET /api/cohorts` | Cohort retention matrix |
| `GET /api/churn` | Current churn and risk summary |
| `GET /api/pipeline` | Pipeline job metadata |
| `GET /api/benchmark` | Pandas vs PySpark benchmark results |
| `POST /api/simulator/revenue` | Revenue projection scenario |

Example simulator request:

```bash
curl -X POST http://127.0.0.1:8000/api/simulator/revenue ^
  -H "Content-Type: application/json" ^
  -d "{\"tenants\":500,\"arpu\":129,\"monthly_growth_pct\":6,\"monthly_churn_pct\":2.5,\"months\":12}"
```

## Tests

```bash
pytest tests/ -v
flake8 src tests --jobs=1 --count --statistics
python src/ingestion/kafka_producer.py --dry-run --rate 2 --duration 5
```

## Metrics Computed

| Metric | Meaning | Implementation |
| --- | --- | --- |
| MRR | Monthly recurring revenue per tenant | Successful `invoice_paid` events grouped by tenant and month |
| ARR | Annual recurring revenue | `MRR * 12` |
| MRR growth | Month-over-month revenue movement | Lag window by tenant |
| Rolling MRR | 3-month moving average | Rolling window |
| ARPU | Average revenue per user | Average tenant monthly revenue |
| LTV | Estimated lifetime value | Average monthly MRR times active months |
| Churn rate | Cancelled tenants divided by active tenants | Monthly plan-level aggregation |
| Cohort retention | Retained tenants after signup month | Cohort month and months-since-start matrix |
| Revenue projection | Scenario-based future revenue | Compounded growth minus churn by month |

## Verified Results

These checks have passed in the local project environment:

```text
pytest: 17 passed
flake8 full project check: 0 issues
pipeline smoke run: passed
generator exact-count validation: passed
kafka dry run: passed
dashboard browser sanity check: passed
```

On Windows, local Spark can initialize when Java is available, but reliable
local Parquet writes require Hadoop native binaries. To keep local runs stable,
auto mode uses the Pandas fallback on Windows unless Spark is explicitly forced
in a correctly configured Linux, WSL, Docker, or cluster environment with
`SUBSCRIPTION_PIPELINE_ENGINE=spark`.

Latest local benchmark results:

| Records | Pandas seconds | PySpark seconds | Winner |
| --- | ---: | ---: | --- |
| 50,000 | 0.136 | N/A | Pandas (local Spark unavailable) |
| 100,000 | 0.196 | N/A | Pandas (local Spark unavailable) |
| 200,000 | 0.586 | N/A | Pandas (local Spark unavailable) |

## Design Decisions

### PySpark With Pandas Fallback

Spark is the target architecture for distributed processing, partitioned
storage, and large-scale transformations. Pandas is kept as a fallback so the
project remains runnable on laptops without a complete Spark and Hadoop native
runtime.

### Partitioned Parquet

Clean billing events are written by `event_year` and `event_month`, which keeps
monthly analytics efficient by limiting downstream scans to relevant
partitions.

### Multi-Tenant PostgreSQL Design

The SQL schema includes tenant-scoped tables, hot-query indexes, a materialized
view for revenue analytics, and row-level security policies. The design keeps
tenant isolation visible at the data layer instead of relying only on
application code.

### Churn And Cohort Modeling

MRR explains revenue size, while churn and cohort retention explain revenue
quality. The project tracks plan-level churn and tenant activity after signup
month so a SaaS business can understand where revenue is weakening.

### Interactive Revenue Simulator

The simulator exposes business levers directly in the dashboard. It lets users
change tenant count, ARPU, monthly growth, monthly churn, and the projection
window, then sends the scenario to the FastAPI backend for month-by-month
projection calculations.

## Production Improvements

- Add Airflow, Dagster, or Prefect for scheduled orchestration.
- Add Delta Lake, Apache Iceberg, or Hudi for ACID lakehouse tables.
- Add schema registry contracts with Avro or Protobuf for Kafka events.
- Add Great Expectations or Soda checks for richer data quality validation.
- Add dbt for SQL model versioning, tests, documentation, and lineage.
- Move batch storage to cloud object storage and run Spark on Kubernetes, EMR,
  or Dataproc.
- Add managed secrets, service authentication, and tenant-scoped dashboard
  authorization.
