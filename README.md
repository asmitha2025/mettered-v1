# Subscription Intelligence Pipeline

**A production-grade PySpark ETL for SaaS billing analytics**  
Built to understand the data infrastructure behind subscription billing platforms like Chargebee.

[![CI](https://github.com/asmitham/subscription-intelligence-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/asmitham/subscription-intelligence-pipeline/actions)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![PySpark 3.5](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org)
[![PostgreSQL 15](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://postgresql.org)

---

## What This Project Does

Processes **200,000+ subscription billing events** across 500 simulated SaaS tenants through a complete data engineering pipeline:

- **Ingest** raw CSV billing events → validate schema → write partitioned **Parquet**
- **Compute** MRR, ARR, LTV, churn rate using **PySpark window functions**
- **Analyse** cohort retention across 12 monthly cohorts
- **Store** results in a multi-tenant **PostgreSQL schema** with row-level security
- **Stream** billing events via a **Kafka producer** (real-time layer)
- **Benchmark** Spark vs Pandas at 50K / 100K / 200K records with documented tradeoffs

---

## Architecture

```
Raw CSV (billing events, tenants)
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  Job 1: Ingest + Validate                           │
│  • Schema enforcement (StructType)                  │
│  • Data quality checks (nulls, invalid types,       │
│    negative amounts)                                │
│  • Rejected rows → dead-letter Parquet              │
│  • Clean rows → Parquet partitioned by year/month   │
└──────────────────────┬──────────────────────────────┘
                       │
        ┌──────────────┴───────────────┐
        ▼                              ▼
┌───────────────────┐      ┌──────────────────────────┐
│  Job 2: MRR/ARR   │      │  Job 3: Churn + Cohorts  │
│  • Monthly MRR    │      │  • Churn rate by plan     │
│  • MRR growth %   │      │  • Cohort retention       │
│  • 3-month rolling│      │    matrix (12×12)         │
│  • ARR, LTV, ARPU │      │  • Survival rate tracking │
└───────────────────┘      └──────────────────────────┘
        │                              │
        └──────────────┬───────────────┘
                       ▼
             PostgreSQL (analytics store)
             + Materialised view for dashboards

Kafka Producer ──→ billing-events topic
(real-time layer)   [streaming architecture]
```

---

## Project Structure

```
subscription-intelligence-pipeline/
├── src/
│   ├── ingestion/
│   │   ├── generate_data.py      # synthetic 200K billing events
│   │   ├── etl_ingest.py         # Job 1: CSV → Parquet
│   │   └── kafka_producer.py     # real-time event streaming
│   ├── transforms/
│   │   ├── mrr_transform.py      # Job 2: MRR/ARR/LTV
│   │   └── churn_cohort.py       # Job 3: churn + cohort retention
│   ├── analytics/
│   │   └── benchmark.py          # Pandas vs PySpark comparison
│   └── utils/
│       └── spark_session.py      # centralised Spark config
├── sql/
│   └── schema.sql                # PostgreSQL schema + RLS policies
├── tests/
│   └── test_transforms.py        # pytest unit tests
├── .github/
│   └── workflows/ci.yml          # GitHub Actions: lint + test + schema check
├── docker-compose.yml            # Spark + Postgres + Kafka + Jupyter
├── requirements.txt
├── run_pipeline.py               # master orchestrator
└── README.md
```

---

## Quick Start

### Option A: Local (no Docker)

```bash
# 1. Clone
git clone https://github.com/asmitham/subscription-intelligence-pipeline.git
cd subscription-intelligence-pipeline

# 2. Install
pip install -r requirements.txt

# 3. Run everything
python run_pipeline.py --records 50000    # quick run (50K records)
python run_pipeline.py                    # full run (200K records)
```

### Option B: Docker (full stack)

```bash
docker-compose up -d
docker exec spark-master spark-submit src/ingestion/etl_ingest.py
```

### Run tests

```bash
pytest tests/ -v
```

### Kafka streaming (optional)

```bash
# Start Kafka via docker-compose, then:
python src/ingestion/kafka_producer.py --rate 20

# Dry-run (no Kafka needed):
python src/ingestion/kafka_producer.py --dry-run
```

---

## Key Design Decisions

### Why PySpark over Pandas?

Short answer: at this scale, Pandas is actually faster. I benchmarked both:

| Records | Pandas | PySpark | Winner |
|---------|--------|---------|--------|
| 50,000  | 0.8s   | 4.2s    | Pandas |
| 100,000 | 1.6s   | 5.1s    | Pandas |
| 200,000 | 3.1s   | 2.4s    | PySpark ✓ |

**Conclusion:** Spark's distributed overhead (JVM startup, task scheduling, shuffle) costs ~3-4 seconds baseline. For files under 150K rows on a single machine, Pandas wins. Spark earns its cost at 150K+ rows when data no longer fits comfortably in memory, or when running across multiple nodes.

I chose PySpark for this project because:
1. The architecture is designed to scale to millions of events (as a real billing platform would)
2. Learning distributed execution model, partition management, and window functions at scale
3. The Parquet + partition strategy (year/month) means downstream queries scan only the partitions they need — a Pandas-based solution can't do this

### Why partition by year and month?

```python
enriched.write.partitionBy("event_year", "event_month").parquet(...)
```

A typical MRR query filters `WHERE event_month = 3 AND event_year = 2024`. With partitioned Parquet, Spark reads only those folders — **predicate pushdown**. For a 200K event dataset across 24 months, this means reading ~8K rows instead of 200K. At 10M events, this difference is critical.

### Why a materialised view in PostgreSQL?

```sql
CREATE MATERIALIZED VIEW mv_monthly_revenue AS ...
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_revenue;
```

Dashboard queries for MRR trends run in milliseconds on the materialised view versus 2-3 seconds on base tables. The `CONCURRENTLY` option allows refresh without locking reads — production-safe for a live billing dashboard.

### Why row-level security?

```sql
CREATE POLICY tenant_isolation_events ON billing_events
    USING (tenant_id = current_setting('app.tenant_id', TRUE));
```

In a multi-tenant SaaS platform, tenant A must never see tenant B's data — even if there's an application bug. Pushing this guarantee into the database (not just the application layer) means it can't be accidentally bypassed. This is how Chargebee isolates customer data at the platform level.

---

## Metrics Computed

| Metric | Description | Spark Feature Used |
|--------|-------------|-------------------|
| MRR | Monthly Recurring Revenue per tenant | `groupBy` + `sum` |
| MRR Growth % | Month-over-month change | `lag()` window function |
| Cumulative MRR | Running total per tenant | `sum` with unbounded window |
| Rolling 3M MRR | 3-month moving average | `avg` with rowsBetween window |
| ARR | Annual Recurring Revenue (MRR × 12) | derived column |
| ARPU | Average Revenue Per User | `avg` aggregation |
| LTV | Lifetime Value per tenant | ARPU × active months |
| Churn Rate | Cancelled / Active per month, per plan | two `groupBy` + join |
| Cohort Retention | % of cohort still active after N months | `join` + `pivot` logic |

---

## What I Learned / Would Do Differently in Production

1. **Airflow for orchestration** — `run_pipeline.py` is a script, not a scheduler. In production, each job would be an Airflow DAG task with dependency tracking, retries, and SLA alerts.

2. **Delta Lake instead of plain Parquet** — Delta adds ACID transactions, schema evolution, and time-travel. For a billing platform where late-arriving events are common, `MERGE INTO` is essential.

3. **Spark on YARN/Kubernetes** — `local[*]` mode uses one machine. Real scale means submitting to a cluster where Spark distributes work across many executors.

4. **dbt for the PostgreSQL transforms** — The materialised view refresh is manual. dbt would version-control the SQL, add tests, and document lineage automatically.

5. **Schema registry with Kafka** — The Kafka producer sends raw JSON. In production, Avro schemas + a Confluent Schema Registry would enforce contract between producers and consumers.

---

## CI Pipeline

Every push to `main` runs:

1. **Lint** — flake8 checks for syntax errors and undefined names
2. **Unit tests** — pytest with PySpark (schema validation, MRR calculations, churn logic)
3. **Schema check** — spins up a real PostgreSQL container and applies `schema.sql`
4. **Data gen smoke test** — generates 1K records, verifies output files exist

---

## Author

**Asmitha M** — Data Engineer  
Chennai, Tamil Nadu  
[linkedin.com/in/asmitham](https://linkedin.com/in/asmitham) · [github.com/asmitham](https://github.com/asmitham)
