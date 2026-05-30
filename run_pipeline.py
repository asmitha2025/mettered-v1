#!/usr/bin/env python3
"""
run_pipeline.py  —  Master Orchestrator
----------------------------------------
Runs all pipeline jobs in order:
  1. Generate synthetic data
  2. Ingest + validate → Parquet
  3. Compute MRR / ARR / LTV
  4. Compute Churn + Cohort retention
  5. Run benchmark (Pandas vs PySpark)

Usage:
  python run_pipeline.py              # full run, 200K records
  python run_pipeline.py --records 10000  # quick test with 10K
  python run_pipeline.py --skip-benchmark # skip benchmark step
"""

import argparse
import subprocess
import sys
import time

# Use the current Python interpreter to execute all step sub-processes
python_exe = sys.executable

STEPS = [
    ("Generate Data",        [python_exe, "src/ingestion/generate_data.py"]),
    ("Ingest -> Parquet",     [python_exe, "src/ingestion/etl_ingest.py"]),
    ("MRR / ARR / LTV",      [python_exe, "src/transforms/mrr_transform.py"]),
    ("Churn + Cohorts",      [python_exe, "src/transforms/churn_cohort.py"]),
    ("Benchmark",            [python_exe, "src/analytics/benchmark.py"]),
]

# Ensure UTF-8 output on Windows terminal
if sys.platform.startswith("win"):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


def run_step(name, cmd, records=None):
    print(f"\n{'='*60}")
    print(f"  >>>  {name}")
    print(f"{'='*60}\n")

    full_cmd = cmd.copy()
    if records and "generate_data" in cmd[1]:
        full_cmd += ["--records", str(records)]

    import os
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    start = time.time()
    result = subprocess.run(full_cmd, env=env, capture_output=False)
    elapsed = round(time.time() - start, 1)

    if result.returncode != 0:
        print(f"\n  [ERROR] {name} FAILED (exit code {result.returncode})")
        sys.exit(result.returncode)

    print(f"\n  [TIME] {name} completed in {elapsed}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=200000)
    parser.add_argument("--skip-benchmark", action="store_true")
    args = parser.parse_args()

    # Detect execution engine
    spark_available = False
    try:
        from pyspark.sql import SparkSession
        import shutil
        if shutil.which("java") is not None:
            spark_available = True
    except ImportError:
        pass

    engine_banner = """
|   Execution Engine: PySpark (Production Mode)            |""" if spark_available else """
|   Execution Engine: Pandas (Optimized Fallback Mode)     |
|   --> Triggered automatically because Java/Spark is      |
|       not installed in the local environment.            |"""

    print(f"""
+----------------------------------------------------------+
|   Subscription Intelligence Pipeline                     |
|   A PySpark ETL for SaaS Billing Analytics               |
|   github.com/asmitham/subscription-intelligence-pipeline |
+----------------------------------------------------------+
{engine_banner.strip()}
    """)

    steps = STEPS[:-1] if args.skip_benchmark else STEPS

    total_start = time.time()
    for name, cmd in steps:
        run_step(name, cmd, records=args.records)

    total = round(time.time() - total_start, 1)
    print(f"""
{'='*60}
  [OK] All steps complete in {total}s
  [FOLDER] Output: data/processed/
      ├── billing_events/     (partitioned Parquet)
      ├── mrr_by_tenant_month/
      ├── global_mrr_monthly/
      ├── tenant_ltv/
      ├── churn_by_plan_month/
      └── cohort_retention/
{'='*60}
    """)


if __name__ == "__main__":
    main()
