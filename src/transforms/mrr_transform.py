"""
mrr_transform.py - Job 2: MRR / ARR / LTV Calculations
----------------------------------------------------------
Computes Monthly Recurring Revenue, Annual Recurring Revenue,
and Lifetime Value per tenant using Spark SQL window functions.

Run:
    python src/transforms/mrr_transform.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pyspark.sql import functions as F, Window
except ImportError:
    F = None
    Window = None

from utils.spark_session import get_spark
from utils.filesystem import recreate_dir
from utils.paths import data_path


def compute_mrr(spark):
    """
    MRR = sum of all successful invoice_paid amounts per tenant per month.
    Uses window functions to compute:
      - monthly MRR
      - MRR growth % vs prior month
      - cumulative MRR (running total)
      - 3-month rolling average MRR
    """
    events = spark.read.parquet(data_path("processed", "billing_events"))
    tenants = spark.read.parquet(data_path("processed", "tenants"))

    # Monthly revenue per tenant
    monthly_revenue = (
        events
        .filter(F.col("event_type") == "invoice_paid")
        .filter(F.col("status") == "success")
        .groupBy("tenant_id", "event_year", "event_month")
        .agg(
            F.sum("amount").alias("mrr"),
            F.count("invoice_id").alias("invoice_count"),
            F.avg("seats").alias("avg_seats")
        )
        .withColumn(
            "period",
            F.concat(F.col("event_year"), F.lit("-"), F.lpad(F.col("event_month"), 2, "0"))
        )
    )

    # Window: tenant ordered by time
    tenant_window = Window.partitionBy("tenant_id").orderBy("event_year", "event_month")
    tenant_window_unbounded = (
        Window.partitionBy("tenant_id")
        .orderBy("event_year", "event_month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    tenant_window_3m = (
        Window.partitionBy("tenant_id")
        .orderBy("event_year", "event_month")
        .rowsBetween(-2, 0)   # 3-month rolling window
    )

    mrr_enriched = (
        monthly_revenue
        .withColumn("prev_mrr",    F.lag("mrr", 1).over(tenant_window))
        .withColumn(
            "mrr_growth_pct",
            F.round(
                F.when(
                    (F.col("prev_mrr").isNotNull()) & (F.col("prev_mrr") != 0),
                    (F.col("mrr") - F.col("prev_mrr")) / F.col("prev_mrr") * 100
                ).otherwise(F.lit(None)),
                2,
            )
        )
        .withColumn("cumulative_mrr",  F.sum("mrr").over(tenant_window_unbounded))
        .withColumn("rolling_3m_mrr",  F.avg("mrr").over(tenant_window_3m))
        .withColumn("arr",             F.col("mrr") * 12)
    )

    return mrr_enriched, tenants


def compute_ltv(mrr_df, tenants_df):
    """
    LTV = Average MRR x average customer lifetime in months.
    Simple LTV model: LTV = ARPU / Churn Rate
    """
    avg_mrr_per_tenant = (
        mrr_df
        .groupBy("tenant_id")
        .agg(
            F.avg("mrr").alias("avg_monthly_mrr"),
            F.count("period").alias("active_months"),
            F.sum("mrr").alias("total_revenue")
        )
    )

    ltv = (
        avg_mrr_per_tenant
        .join(tenants_df.select("tenant_id", "company_name", "industry", "plan", "country"), "tenant_id")
        .withColumn("estimated_ltv", F.col("avg_monthly_mrr") * F.col("active_months"))
    )
    return ltv


def run_pandas():
    print("\n[JOB 2] MRR / ARR / LTV Computation (Pandas Fallback Engine)\n")
    import pandas as pd
    import numpy as np

    events_path = data_path("processed", "billing_events")
    tenants_path = data_path("processed", "tenants")

    if not os.path.exists(events_path) or not os.path.exists(tenants_path):
        print("  [ERROR] Processed data missing from Job 1. Please run etl_ingest.py first.")
        return

    # Read Parquets
    events = pd.read_parquet(events_path)
    events["event_year"] = events["event_year"].astype(int)
    events["event_month"] = events["event_month"].astype(int)
    tenants = pd.read_parquet(tenants_path)

    # Monthly revenue per tenant
    invoice_paid = events[(events["event_type"] == "invoice_paid") & (events["status"] == "success")]

    monthly_rev = (
        invoice_paid
        .groupby(["tenant_id", "event_year", "event_month"])
        .agg(
            mrr=("amount", "sum"),
            invoice_count=("invoice_id", "count"),
            avg_seats=("seats", "mean")
        )
        .reset_index()
    )

    monthly_rev["period"] = (
        monthly_rev["event_year"].astype(str) + "-" +
        monthly_rev["event_month"].astype(str).str.zfill(2)
    )

    # Window calculations
    # Sort to ensure window functions operate in order
    monthly_rev = monthly_rev.sort_values(["tenant_id", "event_year", "event_month"]).reset_index(drop=True)

    # Lag
    monthly_rev["prev_mrr"] = monthly_rev.groupby("tenant_id")["mrr"].shift(1)

    # Growth Pct
    monthly_rev["mrr_growth_pct"] = np.where(
        (monthly_rev["prev_mrr"].notna()) & (monthly_rev["prev_mrr"] != 0),
        ((monthly_rev["mrr"] - monthly_rev["prev_mrr"]) / monthly_rev["prev_mrr"] * 100).round(2),
        np.nan
    )

    # Cumulative Sum
    monthly_rev["cumulative_mrr"] = monthly_rev.groupby("tenant_id")["mrr"].cumsum()

    # 3-Month Rolling Average
    # Pandas rolling requires index-based operations. We use groupby and rolling.
    monthly_rev["rolling_3m_mrr"] = (
        monthly_rev.groupby("tenant_id")["mrr"]
        .rolling(window=3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # ARR
    monthly_rev["arr"] = monthly_rev["mrr"] * 12

    # LTV
    avg_mrr_per_tenant = (
        monthly_rev
        .groupby("tenant_id")
        .agg(
            avg_monthly_mrr=("mrr", "mean"),
            active_months=("period", "count"),
            total_revenue=("mrr", "sum")
        )
        .reset_index()
    )

    # Handle tenant join details
    tenants_select = tenants[["tenant_id", "company_name", "industry", "plan", "country"]]
    ltv = pd.merge(avg_mrr_per_tenant, tenants_select, on="tenant_id", how="inner")
    ltv["estimated_ltv"] = ltv["avg_monthly_mrr"] * ltv["active_months"]

    # Global MRR Summary
    global_mrr = (
        monthly_rev
        .groupby(["event_year", "event_month", "period"])
        .agg(
            total_mrr=("mrr", "sum"),
            total_arr=("arr", "sum"),
            paying_tenants=("tenant_id", "nunique"),
            arpu=("mrr", "mean")
        )
        .reset_index()
        .sort_values(["event_year", "event_month"])
    )

    print("  Global MRR by Month (last 6 periods):")
    print(global_mrr.sort_values("period", ascending=False).head(6).to_string(index=False))
    print("\n  Top 10 Tenants by Estimated LTV:")
    print(ltv.sort_values("estimated_ltv", ascending=False).head(10).to_string(index=False))

    # Write output parquets
    # Clean output directories if they exist
    for path in [
        data_path("processed", "mrr_by_tenant_month"),
        data_path("processed", "global_mrr_monthly"),
        data_path("processed", "tenant_ltv"),
    ]:
        recreate_dir(path)

    monthly_rev.to_parquet(os.path.join(data_path("processed", "mrr_by_tenant_month"), "part.parquet"), index=False)
    global_mrr.to_parquet(os.path.join(data_path("processed", "global_mrr_monthly"), "part.parquet"), index=False)
    ltv.to_parquet(os.path.join(data_path("processed", "tenant_ltv"), "part.parquet"), index=False)

    print("\n  [OK] MRR data written   -> data/processed/mrr_by_tenant_month/")
    print("  [OK] Global MRR written -> data/processed/global_mrr_monthly/")
    print("  [OK] LTV data written   -> data/processed/tenant_ltv/")
    print("\n[OK] Job 2 complete.\n")


def run():
    spark = get_spark("MRR-Transform")
    if spark is None:
        run_pandas()
        return

    print("\n[JOB 2] MRR / ARR / LTV Computation\n")

    mrr_df, tenants_df = compute_mrr(spark)

    # Global MRR summary.
    global_mrr = (
        mrr_df
        .groupBy("event_year", "event_month", "period")
        .agg(
            F.sum("mrr").alias("total_mrr"),
            F.sum("arr").alias("total_arr"),
            F.countDistinct("tenant_id").alias("paying_tenants"),
            F.avg("mrr").alias("arpu")   # Average Revenue Per User
        )
        .orderBy("event_year", "event_month")
    )

    print("  Global MRR by Month (last 6 periods):")
    global_mrr.orderBy(F.desc("period")).show(6, truncate=False)

    # LTV.
    ltv_df = compute_ltv(mrr_df, tenants_df)
    print("  Top 10 Tenants by Estimated LTV:")
    ltv_df.orderBy(F.desc("estimated_ltv")).show(10, truncate=False)

    # Write analytical datasets.
    mrr_df.write.mode("overwrite").parquet(data_path("processed", "mrr_by_tenant_month"))
    global_mrr.write.mode("overwrite").parquet(data_path("processed", "global_mrr_monthly"))
    ltv_df.write.mode("overwrite").parquet(data_path("processed", "tenant_ltv"))

    print("\n  [OK] MRR data written   -> data/processed/mrr_by_tenant_month/")
    print("  [OK] Global MRR written -> data/processed/global_mrr_monthly/")
    print("  [OK] LTV data written   -> data/processed/tenant_ltv/")
    print("\n[OK] Job 2 complete.\n")
    spark.stop()


if __name__ == "__main__":
    run()
