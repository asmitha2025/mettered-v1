"""
benchmark.py - Pandas vs PySpark Performance Comparison
----------------------------------------------------------
Runs MRR aggregation on 50K / 100K / 200K records
using both Pandas and PySpark and records wall-clock time.

KEY INSIGHT: This script documents when Spark is worth its overhead,
not just how to run Spark code.
At small local scales Pandas often wins because it avoids JVM and
scheduler overhead. Spark is mainly justified when distributed execution,
fault tolerance, partitioned processing, and larger-than-memory workloads
matter.

Run:
    python src/analytics/benchmark.py
"""

import time
import json
import pandas as pd
import os
import sys
import tempfile
from typing import Callable, Tuple, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pyspark.sql import functions as F, SparkSession
except ImportError:
    F = None
    SparkSession = None

from utils.spark_session import get_spark


def pandas_mrr(csv_path: str) -> float:
    """Compute MRR with Pandas."""
    df = pd.read_csv(csv_path, parse_dates=["event_date"])
    df = df[df["event_type"] == "invoice_paid"]
    df["month"] = df["event_date"].dt.to_period("M")
    result = df.groupby(["tenant_id", "month"])["amount"].sum().reset_index()
    return result["amount"].sum()


def spark_mrr(parquet_path: str, spark: "SparkSession") -> float:
    """Compute MRR with PySpark."""
    df = spark.read.parquet(parquet_path)
    result = (
        df.filter(F.col("event_type") == "invoice_paid")
          .groupBy("tenant_id", "event_year", "event_month")
          .agg(F.sum("amount").alias("mrr"))
    )
    return result.agg(F.sum("mrr")).collect()[0][0]


def time_it(fn: Callable, *args: Any, **kwargs: Any) -> Tuple[float, Any]:
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    elapsed = round(time.perf_counter() - start, 3)
    return elapsed, result


def run() -> None:
    print("\n[BENCHMARK] Pandas vs PySpark MRR Aggregation\n")

    spark = get_spark("Benchmark")

    if spark is None:
        print("  [WARN] Spark is unavailable or disabled for this local run. Running Pandas-only benchmark.\n")
        print(f"  {'Records':<12} {'Pandas (s)':<14} {'PySpark (s)':<14} {'Winner'}")
        print("  " + "-" * 55)
    else:
        print(f"  {'Records':<12} {'Pandas (s)':<14} {'PySpark (s)':<14} {'Winner'}")
        print("  " + "-" * 55)

    results = []
    temp_dir = tempfile.gettempdir()

    for n in [50_000, 100_000, 200_000]:
        # Sample from full data
        csv_path = "data/raw/billing_events.csv"
        if not os.path.exists(csv_path):
            print(f"  [ERROR] Raw billing events CSV not found at {csv_path}. Please run data generator first.")
            return

        full_df = pd.read_csv(csv_path)
        sample = full_df.sample(n=min(n, len(full_df)), random_state=42)
        sample_path = os.path.join(temp_dir, f"events_{n}.csv")
        sample.to_csv(sample_path, index=False)

        # Pandas timing
        t_pandas, _ = time_it(pandas_mrr, sample_path)
        spark_error = None

        if spark is None:
            t_spark = "N/A"
            winner = "Pandas (local Spark unavailable)"
            print(f"  {n:<12,} {t_pandas:<14} {t_spark:<14} {winner}")
        else:
            # PySpark timing (re-use existing parquet, or write sample)
            try:
                sample_spark = spark.createDataFrame(sample)
                sample_spark = (
                    sample_spark
                    .withColumn("event_date", F.to_timestamp("event_date", "yyyy-MM-dd HH:mm:ss"))
                    .withColumn("event_year",  F.year("event_date"))
                    .withColumn("event_month", F.month("event_date"))
                )
                pq_path = os.path.join(temp_dir, f"events_spark_{n}")
                sample_spark.write.mode("overwrite").parquet(pq_path)

                t_spark, _ = time_it(spark_mrr, pq_path, spark)
                winner = "Pandas" if t_pandas < t_spark else "PySpark"
                print(f"  {n:<12,} {t_pandas:<14} {t_spark:<14} {winner}")
            except Exception as exc:
                t_spark = "Error"
                winner = "Pandas (Spark write error)"
                spark_error = str(exc).splitlines()[0][:160]
                print(f"  {n:<12,} {t_pandas:<14} {t_spark:<14} {winner}")
                print(f"  [WARN] Spark benchmark failed: {spark_error}")

        row = {
            "records": n,
            "pandas_seconds": t_pandas,
            "spark_seconds": t_spark,
            "winner": winner
        }
        if spark is not None and t_spark == "Error":
            row["spark_error"] = spark_error
        results.append(row)

        # Cleanup temp files
        try:
            if os.path.exists(sample_path):
                os.remove(sample_path)
        except Exception:
            pass

    # Save results for README / notebook
    os.makedirs("docs", exist_ok=True)
    with open("docs/benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print("\n  [OK] Results saved -> docs/benchmark_results.json")
    if spark is not None:
        print("""
  KEY INSIGHT:
     Spark has real fixed costs: JVM startup, task scheduling,
     partition management, and file commit coordination. If Spark loses
     on small local data, that is expected. It becomes the right choice
     when distributed scale and operational guarantees justify the overhead.
      """)
    else:
        print("""
  KEY INSIGHT:
     Pandas was executed locally because Spark is unavailable or disabled
     for this environment. On Windows, local Spark writes also need proper
     Hadoop native binaries. Use Linux, WSL, Docker, or a managed Spark
     cluster for a cluster-backed Spark benchmark.
      """)

    if spark is not None:
        spark.stop()
    print("[OK] Benchmark complete.\n")


if __name__ == "__main__":
    run()
