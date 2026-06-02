# Video Walkthrough Script

Use this script for a 5 to 6 minute LinkedIn or portfolio video. Keep the browser at `http://127.0.0.1:8000/`, keep the GitHub repository open in another tab, and keep a terminal ready with the test output.

## Recording Setup

- Browser tab 1: GitHub repository README.
- Browser tab 2: Dashboard at `http://127.0.0.1:8000/`.
- Editor tab: project source files.
- Terminal: project root with `.venv` activated.

## 0:00-0:20 - Opening

**Show:** GitHub README title and first section.

**Say:**

Hi, I am Asmitha. This is my Subscription Intelligence Pipeline project. I built it to simulate how a SaaS subscription business converts raw billing events into useful metrics like MRR, ARR, LTV, churn, and cohort retention.

The goal was not just to make a dashboard. I wanted to show the full data engineering flow: ingestion, validation, partitioned storage, analytical transforms, dashboard serving, tests, CI, and multi-tenant database design.

## 0:20-0:50 - Problem And Architecture

**Show:** README `Why This Project Exists` and architecture diagram.

**Say:**

Subscription businesses need reliable answers to questions like: how much recurring revenue did we make, which customers are growing, which plans are churning, and whether newer customer cohorts are retaining better.

The architecture starts with synthetic billing events, then validates and enriches the raw data, writes clean partitioned Parquet, computes revenue and retention metrics, and serves them through a FastAPI dashboard API. I also included PostgreSQL schema design and a Kafka producer to show how this can evolve toward production-style systems.

## 0:50-1:20 - Run Pipeline

**Show:** Terminal. Run or show this command:

```bash
python run_pipeline.py --records 200000
```

**Say:**

This command runs the complete local pipeline. It generates 200,000 billing events across 500 tenants, ingests the raw CSV files, validates the records, writes Parquet, computes MRR and LTV, computes churn and cohort retention, and then runs the local Pandas versus Spark benchmark.

On this Windows machine, the pipeline automatically uses the Pandas fallback for local reliability. Spark is still part of the design, but local Spark writes on Windows require Hadoop native binaries, so I documented that limitation instead of hiding it.

## 1:20-1:55 - Dashboard Overview

**Show:** Dashboard `Overview` tab.

**Say:**

This is the dashboard view built with FastAPI and a browser UI. The top cards show total MRR, ARR, churn rate, and active tenants. These values come from the processed Parquet outputs, not hardcoded data.

The MRR chart shows the monthly revenue trend, the plan chart shows revenue distribution by plan, and the tenant table shows the highest LTV tenants with their current MRR and month-over-month trend.

## 1:55-2:30 - Cohort Retention

**Show:** Dashboard `Cohort Retention` tab.

**Say:**

This is the cohort retention matrix. Each row is a signup cohort, and each column shows how much of that cohort remains retained after a number of months.

Month zero is the baseline at 100 percent. After that, retention declines based on the first cancellation event for each tenant. This was an important fix because cohort logic must be honest and easy to explain. MRR tells revenue size, but cohort retention tells revenue quality.

## 2:30-3:05 - Pipeline Jobs

**Show:** Dashboard `Pipeline Jobs` tab.

**Say:**

This tab explains the pipeline jobs. Job 1 ingests and validates the raw data. Job 2 computes MRR, ARR, LTV, and revenue windows. Job 3 computes churn and cohort retention.

The PostgreSQL item is marked as schema design, not a fake running database job. The Kafka item is marked optional because the producer supports dry-run simulation and live Kafka mode, but it is not pretending that Kafka is running in the dashboard.

## 3:05-3:40 - Benchmark

**Show:** Dashboard `Benchmark` tab.

**Say:**

This benchmark compares local Pandas timings with PySpark availability. The honest result on this machine is that Pandas runs locally, while Spark is unavailable in auto mode because Windows Spark writes need Hadoop native binaries.

This is an important engineering decision. For small local data, Pandas can be faster and simpler. Spark becomes valuable when the data is too large for one machine, or when distributed processing, fault tolerance, and partition management matter.

## 3:40-4:10 - What-If Simulator

**Show:** Dashboard `Simulator` tab.

**Say:**

This is the interactive part of the project. It is not only a static dashboard. I can enter starting tenants, ARPU, growth rate, churn rate, and projection months. The frontend sends these inputs to a FastAPI endpoint, and the backend returns projected tenants, MRR, and ARR for each month.

This lets a product or revenue team ask questions like: what happens if churn drops by two percent, or if acquisition improves next month?

## 4:10-4:55 - Source Code Walkthrough

**Show:** Editor with these files in order:

1. `src/ingestion/generate_data.py`
2. `src/ingestion/etl_ingest.py`
3. `src/transforms/mrr_transform.py`
4. `src/transforms/churn_cohort.py`
5. `src/web/app.py`
6. `src/utils/spark_session.py`

**Say:**

The generator creates realistic subscription lifecycle events: invoices paid, invoices failed, upgrades, downgrades, cancellations, trials, and conversions. I seeded the generator so demo outputs are reproducible.

The ingestion job validates raw records and writes clean data into partitioned Parquet by event year and month. That partitioning makes monthly analytical queries cheaper.

The MRR transform filters successful invoice-paid events, groups them by tenant and month, and derives ARR, growth percentage, rolling MRR, cumulative MRR, ARPU, and estimated LTV.

The churn and cohort transform computes churn by plan and builds the retention matrix. The retention logic is based on each tenant's first cancellation event.

The web API includes the simulator endpoint, so the dashboard can accept user inputs and return a calculated projection instead of only displaying saved outputs.

The Spark session helper chooses the right local execution mode. It can use Spark when the environment is correctly configured, but it falls back safely for local demos.

## 4:55-5:25 - SQL, Tests, And CI

**Show:** `sql/schema.sql`, then terminal test output, then `.github/workflows/ci.yml`.

**Say:**

The SQL schema shows how this analytics design could support a multi-tenant product. It includes tenant-scoped tables, indexes for hot query paths, a materialized view for revenue dashboards, and row-level security policies.

I also added pytest coverage for data generation, transform logic, Spark/Pandas engine selection, churn dashboard consistency, and simulator calculations. The current test suite passes with 17 tests, and flake8 reports zero issues for the project.

The GitHub Actions workflow runs lint checks, unit tests, SQL schema validation against PostgreSQL, and a small data generation smoke test.

## 5:25-5:50 - Honest Production Gaps

**Show:** `docs/PROJECT_SHOWCASE_REPORT.md`, section `Honest Production Gaps`.

**Say:**

For production, I would add a scheduler like Airflow or Dagster, a lakehouse table format like Delta Lake or Iceberg, a schema registry for Kafka messages, stronger data quality checks, secret management, and authentication for the dashboard.

I documented these gaps because I want the project to be honest. This is a strong portfolio project, and the next step would be making it cloud-native and production-operated.

## 5:50-6:05 - Closing

**Show:** README top or dashboard overview.

**Say:**

This project helped me connect data engineering decisions with business metrics. It shows that I can build a complete analytics pipeline, explain the tradeoffs, test the logic, and present the results clearly.

Thank you for watching. I am open to data engineering and backend engineering opportunities in product-based companies.

## Short LinkedIn Caption

I built a Subscription Intelligence Pipeline for SaaS billing analytics.

It processes 200K simulated subscription events across 500 tenants and computes MRR, ARR, LTV, churn, and cohort retention using Python, PySpark-style transforms, Pandas fallback, Parquet, FastAPI, PostgreSQL schema design, Kafka simulation, tests, and CI.

The main lesson: good engineering is not only about using big tools. It is about choosing the right tool, validating the data, explaining tradeoffs honestly, and building something that maps to real business decisions.

#DataEngineering #Python #PySpark #SaaS #Analytics #ETL #FastAPI #PostgreSQL #Kafka #OpenToWork
