# Subscription Intelligence Pipeline

Production-style data engineering project for SaaS billing analytics.

This project simulates how a subscription platform turns raw billing events into
business metrics such as MRR, ARR, LTV, churn, and cohort retention.

[![CI](https://github.com/asmitham/subscription-intelligence-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/asmitham/subscription-intelligence-pipeline/actions)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![PySpark 3.5](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org)
[![PostgreSQL 15](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://postgresql.org)

## Why This Project Exists

Subscription businesses need accurate answers to questions like:

- How much recurring revenue did we generate this month?
- Which customers are growing or shrinking?
- Which plans have the highest churn?
- Are newer customer cohorts retaining better than older cohorts?
- How should analytics pipelines be designed for multi-tenant SaaS data?

This repository answers those questions with an end-to-end pipeline:

```text
Raw CSV events
  -> validation and enrichment
  -> partitioned Parquet
  -> MRR / ARR / LTV transforms
  -> churn and cohort retention transforms
  -> dashboard API and PostgreSQL analytics schema
```

## What It Does

- Generates up to 200,000 synthetic SaaS billing events across 500 tenants.
- Ingests raw CSV billing data, validates required fields, and writes clean Parquet.
- Computes MRR, ARR, MRR growth, rolling MRR, ARPU, and tenant LTV.
- Computes churn by plan and monthly cohort retention.
- Includes a PostgreSQL schema with indexes, materialized view design, and row-level security.
- Includes a Kafka producer for real-time billing event simulation.
- Serves analytics through a FastAPI dashboard API.
- Includes pytest coverage and GitHub Actions CI.
- Benchmarks Pandas vs PySpark tradeoffs.

For interview answers, LinkedIn content, video script, demo flow, and the project improvement log, read:

[docs/PROJECT_SHOWCASE_REPORT.md](docs/PROJECT_SHOWCASE_REPORT.md)

For a timed screen-by-screen recording plan, read:

[docs/VIDEO_WALKTHROUGH_SCRIPT.md](docs/VIDEO_WALKTHROUGH_SCRIPT.md)

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
|   |   `-- spark_session.py      # Spark session with Pandas fallback support
|   `-- web/
|       |-- app.py                # FastAPI dashboard API
|       `-- templates/index.html  # dashboard UI
|-- sql/schema.sql                # PostgreSQL schema, indexes, RLS
|-- tests/
|   |-- test_generation.py        # synthetic data behavior
|   |-- test_spark_session.py     # Spark/Pandas engine selection
|   `-- test_transforms.py        # metric and validation logic
|-- docs/
|   |-- PROJECT_SHOWCASE_REPORT.md
|   `-- benchmark_results.json
|-- docker-compose.yml
|-- requirements.txt
|-- setup.cfg
|-- run_pipeline.py
|-- run_dashboard.py
`-- README.md
```

## Quick Start

### Local Run

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

python run_pipeline.py --records 50000
python view_outputs.py
```

For a smaller smoke test:

```bash
python run_pipeline.py --records 1000 --skip-benchmark
```

### Dashboard

```bash
python run_dashboard.py
```

The launcher starts a FastAPI server on the first available local port from
`8000` onward.

### Hugging Face Space / Docker

The repository includes a Dockerfile for Hugging Face Spaces. The container
build generates a 50K-record demo dataset and serves the dashboard on port
`7860`.

```bash
docker build -t subscription-intelligence-pipeline .
docker run -p 7860:7860 subscription-intelligence-pipeline
```

### Tests

```bash
pytest tests/ -v
```

### Critical Lint Check

```bash
flake8 src tests --jobs=1 --count --select=E9,F63,F7,F82 --show-source --statistics
```

### Kafka Dry Run

```bash
python src/ingestion/kafka_producer.py --dry-run --rate 2 --duration 5
```

## Metrics Computed

| Metric | Meaning | Implementation |
| --- | --- | --- |
| MRR | Monthly recurring revenue per tenant | successful `invoice_paid` grouped by tenant and month |
| ARR | Annual recurring revenue | `MRR * 12` |
| MRR growth | Month-over-month revenue movement | lag window by tenant |
| Rolling MRR | 3-month moving average | rolling window |
| ARPU | Average revenue per user | average tenant monthly revenue |
| LTV | Estimated lifetime value | average monthly MRR times active months |
| Churn rate | cancelled tenants divided by active tenants | monthly plan-level aggregation |
| Cohort retention | retained tenants after signup month | cohort month and months-since-start matrix |

## Verified Results

These checks were run successfully in the local project environment:

```text
pytest: 14 passed
flake8 full project check: 0 issues
full pipeline run: passed with 200,000 records
full generator run: produced exactly 200,000 billing events
kafka dry run: passed
```

Local note: I validated that Spark 3.5 can initialize with a local JDK, but
Windows local Parquet writes require proper Hadoop native binaries. To keep the
demo reliable, auto mode uses the Pandas fallback on Windows. Spark can still be
forced in a correctly configured Linux, WSL, Docker, or cluster environment with
`SUBSCRIPTION_PIPELINE_ENGINE=spark`.

Latest local benchmark results:

| Records | Pandas seconds | PySpark seconds | Winner |
| --- | ---: | ---: | --- |
| 50,000 | 0.136 | N/A | Pandas (local Spark unavailable) |
| 100,000 | 0.196 | N/A | Pandas (local Spark unavailable) |
| 200,000 | 0.586 | N/A | Pandas (local Spark unavailable) |

## Key Design Decisions

### PySpark with Pandas Fallback

Spark is the target architecture for distributed data processing, partitioned
storage, and large-scale transformations. Pandas is kept as a fallback so the
project can still run on laptops without a fully configured Spark runtime.

That tradeoff is intentional: small local datasets often run faster in Pandas,
while Spark becomes valuable when data size, distributed execution, and
fault-tolerant processing matter.

### Partitioned Parquet

Clean billing events are written by `event_year` and `event_month`. This makes
monthly analytics cheaper because downstream jobs can scan only the relevant
partitions instead of every historical event.

### Multi-Tenant PostgreSQL Design

The SQL schema includes tenant-scoped tables, hot-query indexes, a materialized
view for revenue analytics, and row-level security policies. The goal is to show
how billing analytics should protect tenant isolation beyond application code.

### Cohort Retention

MRR explains revenue size, but cohort retention explains revenue quality. The
project tracks tenant activity after signup month so the business can see when
customers drop off.

## Production Improvements I Would Add

- Airflow, Dagster, or Prefect for scheduled orchestration.
- Delta Lake, Apache Iceberg, or Hudi for ACID lakehouse tables.
- Schema registry with Avro or Protobuf for Kafka contracts.
- Great Expectations or Soda checks for richer data quality.
- dbt for SQL model versioning, tests, documentation, and lineage.
- Cloud object storage and Spark on Kubernetes, EMR, or Dataproc.
- Secrets management instead of local environment variables.
- Authentication, authorization, and tenant-scoped access controls for derived dashboard views.

## Interview Pitch

I built an end-to-end SaaS billing analytics pipeline that turns raw
subscription events into MRR, ARR, LTV, churn, and cohort-retention insights.
The project covers ingestion, validation, partitioned storage, analytical
transforms, multi-tenant database design, streaming simulation, dashboard
serving, tests, and CI.

## Author

**Asmitha M**
Data Engineer
Chennai, Tamil Nadu
[linkedin.com/in/asmitham](https://linkedin.com/in/asmitham) |
[github.com/asmitham](https://github.com/asmitham)
