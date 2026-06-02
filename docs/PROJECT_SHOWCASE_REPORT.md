# Subscription Intelligence Pipeline - Project Showcase Report

Use this file as your interview preparation, LinkedIn post source, and video script. It is written to help you explain the project clearly without overclaiming.

## 1. Executive Summary

**Project:** Subscription Intelligence Pipeline  
**Domain:** SaaS billing analytics, subscription revenue, churn, retention  
**Core stack:** Python, PySpark, Pandas fallback, Parquet, PostgreSQL schema, Kafka producer, FastAPI dashboard, pytest, GitHub Actions

This project simulates a subscription billing analytics platform. It generates SaaS billing events, validates raw CSV data, stores clean data as partitioned Parquet, computes MRR/ARR/LTV/churn/cohort retention, exposes a dashboard API, includes a Kafka producer for streaming simulation, and documents production-oriented database design with row-level security.

The strongest interview angle is:

> "I built this to show that I understand the full lifecycle of subscription data: ingestion, validation, transformation, analytical modeling, tenant isolation, dashboard serving, testing, and the tradeoff between Pandas and Spark."

## 2. What I Improved In This Pass

- Fixed the synthetic data generator so `--records 200000` now produces exactly 200,000 billing events.
- Tuned the synthetic event mix so churn is visible without making the demo look unrealistically broken.
- Corrected cohort retention so Month 0 is the cohort baseline and later months decline after each tenant's first cancellation event.
- Made command-line scripts safer on Windows terminals by replacing emoji-heavy console output with ASCII-safe status messages.
- Added a reusable filesystem helper for local reruns where generated Parquet folders need to be recreated.
- Rewrote the Kafka producer into a clean ASCII-safe script with a working dry-run mode.
- Fixed a flake8 CI issue in `src/utils/spark_session.py` by importing `SparkSession` only for type checking.
- Seeded Faker and Python random together so demo company names and metrics are reproducible across runs.
- Cleaned the PostgreSQL schema comments and extended row-level security to the `tenants` table.
- Verified the test suite: `14 passed`.
- Verified full-project lint: `flake8 src tests` returned `0`.
- Verified full pipeline run with `--records 200000`.
- Verified generator full-scale output with exactly 200,000 events.
- Verified the dashboard with a full 200,000-event processed dataset.
- Refreshed `docs/benchmark_results.json` using the local machine.

## 3. Validation Results

Commands verified:

```bash
.venv\Scripts\python.exe -m pytest tests/ -v
.venv\Scripts\python.exe -m flake8 src tests --jobs=1 --count --select=E9,F63,F7,F82 --show-source --statistics
.venv\Scripts\python.exe run_pipeline.py --records 200000
.venv\Scripts\python.exe src\ingestion\kafka_producer.py --dry-run --rate 1 --duration 1
```

Results:

- Unit tests: `14 passed`.
- Full-project lint: `0` issues.
- Full pipeline run: completed successfully with `200,000` records in Pandas fallback mode.
- Full data generation: produced exactly `200,000` billing events and `500` tenants.
- Kafka dry run: produced sample billing events without requiring Kafka.

Local limitation:

- Spark 3.5 was validated to initialize with a local JDK, but Windows local Parquet writes still need proper Hadoop native binaries. Auto mode therefore uses the Pandas path on Windows so demos and tests remain reliable. Spark can be forced in a correctly configured Linux, WSL, Docker, or cluster environment with `SUBSCRIPTION_PIPELINE_ENGINE=spark`.

## 4. Architecture Explanation For Interviews

The pipeline has five layers:

1. **Data generation:** creates realistic multi-tenant SaaS events such as `invoice_paid`, `invoice_failed`, upgrades, downgrades, cancellations, and trials.
2. **Ingestion and validation:** reads raw CSV, enforces required fields, rejects invalid records, enriches events with year/month partitions, and writes Parquet.
3. **Revenue analytics:** computes MRR, ARR, MRR growth, rolling 3-month MRR, cumulative MRR, ARPU, and estimated LTV.
4. **Retention analytics:** computes churn by plan and cohort retention across monthly cohorts.
5. **Serving and production design:** FastAPI dashboard API, PostgreSQL schema with indexes and row-level security, Kafka producer for streaming simulation, and CI tests.

The design mirrors what a billing platform needs: correctness, tenant isolation, efficient query storage, and metrics that business teams actually use.

## 5. Key Technical Talking Points

**Why Parquet?**  
Parquet is columnar, compressed, and efficient for analytical scans. Partitioning by `event_year` and `event_month` means monthly revenue queries scan only relevant folders instead of the full event history.

**Why Spark if Pandas can run locally?**  
Pandas is faster for small local datasets because it avoids JVM and scheduling overhead. Spark becomes valuable when the data grows beyond single-machine memory or needs distributed execution, fault tolerance, partition management, and scalable joins.

**Why a Pandas fallback?**  
It makes the project easier to run on local laptops without a fully configured Spark runtime while preserving the same business logic. This improves developer experience and demo reliability.

**Why row-level security?**  
In a multi-tenant SaaS product, tenant data isolation should not depend only on application code. Database-level RLS adds a second layer of protection.

**Why cohort retention?**  
MRR tells revenue size, but cohort retention tells quality of growth. It answers whether newer customers are staying active after signup.

**Why Kafka?**  
Billing systems receive real-time events from webhooks and payment systems. Kafka shows how batch analytics can be extended toward streaming ingestion.

## 6. Issues Faced And How To Explain Them

**Issue 1: Windows terminal Unicode crash**  
Some scripts printed emoji directly. On Windows consoles using `cp1252`, this caused `UnicodeEncodeError`.  
**Fix:** Changed CLI output to ASCII-safe messages like `[OK]`, `[WARN]`, and `->`.

**Issue 2: Generator did not always honor requested record count**  
The original event timeline could end before enough records were created.  
**Fix:** Adjusted event spacing and capped final output so requested record counts are produced exactly.

**Issue 3: Local Spark on Windows needs more than Java**  
PySpark can start after Java is configured, but local Parquet writes on Windows still depend on Hadoop native binaries.  
**Fix:** Kept Spark as the scalable target, made auto mode use Pandas on Windows for reliable demos, and documented how to force Spark only in a correctly configured environment.

**Issue 4: OneDrive/Windows file lock on generated Parquet**  
Deleting generated output folders sometimes failed due to local file locks.  
**Fix:** Added a filesystem helper that retries after resetting write permissions. For one locked previous artifact, I cleared generated `data/processed` output and reran successfully.

**Issue 5: CI lint type annotation**  
Flake8 flagged a `SparkSession` type reference.  
**Fix:** Added a `TYPE_CHECKING` import so type hints remain clean without runtime dependency problems.

**Issue 6: Unrealistic churn and cohort retention**  
The original synthetic event mix created churn that was too high, while cohort retention could look too perfect or use invoice activity in a confusing way.  
**Fix:** Reduced cancellation frequency in the generator and changed cohort retention to use first cancellation as the retention cutoff.

## 7. Interview Q&A

**Q: What problem does this project solve?**  
A: It simulates the analytics backbone of a subscription SaaS company. It converts raw billing events into revenue, churn, and retention metrics that finance, product, and customer success teams can use.

**Q: What was the hardest part?**  
A: The hardest part was balancing demo reliability with production-style architecture. PySpark is powerful, but local Windows execution can fail without Hadoop native support even when Java is present. I solved that by keeping Spark as the scalable design while making Pandas the reliable local fallback.

**Q: How do you calculate MRR?**  
A: I filter successful `invoice_paid` events, group them by tenant and billing month, then sum amounts. From that monthly tenant-level table I derive ARR, growth percentage, rolling MRR, cumulative MRR, ARPU, and LTV.

**Q: How do you calculate churn?**  
A: I count active tenants with paid invoices by plan and month, count tenants with cancellation events in the same period, then compute `churned_tenants / active_tenants * 100`.

**Q: What would you change for production?**  
A: I would add Airflow for orchestration, Delta Lake or Iceberg for ACID table management, a schema registry for Kafka events, dbt for SQL models, proper secrets management, and stronger data quality checks with tools like Great Expectations.

**Q: How does this show product-company readiness?**  
A: It connects engineering decisions to business metrics. Product companies value engineers who can build reliable systems and understand why metrics like retention, churn, and MRR matter.

**Q: How would you scale it?**  
A: I would run Spark on Kubernetes or EMR/Dataproc, store bronze/silver/gold tables in a lakehouse format, partition by event month, compact small files, and separate batch jobs from streaming consumers.

**Q: What tests are included?**  
A: Unit tests cover validation, MRR calculation, churn logic, and date enrichment. CI also runs lint, tests, PostgreSQL schema validation, and a data-generation smoke test.

## 8. LinkedIn Post

I built a Subscription Intelligence Pipeline to understand how SaaS billing platforms turn raw events into business-ready analytics.

The project processes simulated subscription events across 500 SaaS tenants and computes:

- MRR and ARR
- tenant-level LTV
- churn by plan
- cohort retention
- rolling revenue trends
- benchmark comparison between Pandas and PySpark

What I focused on:

- Raw CSV ingestion with validation
- Partitioned Parquet outputs for analytics
- PySpark transformations with a Pandas fallback for local execution
- PostgreSQL schema design with row-level security for tenant isolation
- Kafka producer for real-time billing event simulation
- FastAPI dashboard API
- pytest and GitHub Actions CI

One lesson I liked from this build: Spark is not always the fastest tool for small local data. Pandas can win at small scale because Spark has JVM and scheduling overhead. Spark becomes valuable when data volume, distributed execution, fault tolerance, and partitioned processing matter.

This project helped me connect data engineering concepts with real SaaS business metrics like MRR, churn, retention, and customer lifetime value.

GitHub: [add your repository link]

#DataEngineering #Python #PySpark #SaaS #Analytics #ETL #Kafka #PostgreSQL #OpenToWork

## 9. Short Video Script

**Length:** 60-90 seconds

Hi, I am Asmitha. I built a Subscription Intelligence Pipeline to simulate how a SaaS billing company converts raw subscription events into analytics.

The pipeline starts by generating billing events for 500 tenants. These include paid invoices, failed invoices, upgrades, downgrades, cancellations, and trial events.

Then the ingestion job validates the raw CSV data, rejects bad records, enriches the data with year and month columns, and writes clean events into partitioned Parquet.

The analytics layer computes the main SaaS metrics: monthly recurring revenue, ARR, MRR growth, rolling revenue, tenant LTV, churn by plan, and cohort retention.

I also added a PostgreSQL schema with row-level security to show how tenant isolation would work in a multi-tenant product, and a Kafka producer to simulate real-time billing events.

One important learning from this project was choosing the right tool. Pandas is faster for small local data, but Spark is useful when the data grows and needs distributed processing.

I built this project to show end-to-end data engineering thinking: ingestion, validation, transformations, storage design, analytics, testing, and dashboard serving.

## 10. Resume Bullets

- Built an end-to-end SaaS subscription analytics pipeline in Python/PySpark to process 200K simulated billing events across 500 tenants.
- Designed ETL jobs for schema validation, rejected-row handling, partitioned Parquet storage, and monthly revenue transformations.
- Computed MRR, ARR, MRR growth, rolling revenue, LTV, churn by plan, and cohort retention metrics for subscription analytics.
- Added PostgreSQL schema with indexes, materialized view design, and row-level security policies for multi-tenant isolation.
- Implemented Kafka billing-event producer, FastAPI dashboard API, pytest coverage, and GitHub Actions CI checks.

## 11. Demo Flow

Use this order when showing the project:

1. Open the README and explain the domain.
2. Show `src/ingestion/generate_data.py` and run a small data generation command.
3. Show `run_pipeline.py` as the orchestrator.
4. Show `src/transforms/mrr_transform.py` for MRR and LTV logic.
5. Show `src/transforms/churn_cohort.py` for churn and retention.
6. Show `sql/schema.sql` for database design and RLS.
7. Show tests passing.
8. Show the dashboard or API if you want a visual demo.

## 12. Honest Production Gaps

These are good to mention because they show maturity:

- The orchestrator is a script; production should use Airflow, Dagster, or Prefect.
- Plain Parquet lacks ACID transactions; production should use Delta Lake, Apache Iceberg, or Hudi.
- Kafka messages are JSON; production should use Avro/Protobuf and a schema registry.
- Secrets and connection strings should move to a secret manager.
- Data quality rules should be expanded with freshness, uniqueness, accepted values, and anomaly checks.
- Dashboard access should include authentication, authorization, and tenant-scoped access controls for derived views.

## 13. One-Line Pitch

I built an end-to-end SaaS billing analytics pipeline that turns raw subscription events into MRR, ARR, LTV, churn, and cohort-retention insights using Python, PySpark-style transforms, Parquet, PostgreSQL design, Kafka simulation, FastAPI, tests, and CI.
