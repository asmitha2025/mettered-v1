"""
churn_cohort.py - Job 3: Churn Rate + Cohort Retention Matrix
---------------------------------------------------------------
Computes monthly churn rate per plan and builds a cohort
retention matrix with 12 monthly cohorts tracked over 12 months.

Cohort analysis is the gold standard for subscription
businesses to understand long-term retention health.

Run:
    python src/transforms/churn_cohort.py
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


def compute_churn_rate(spark):
    """
    Monthly churn rate = cancelled tenants / active tenants at start of month.
    Computed per plan tier.
    """
    events = spark.read.parquet("data/processed/billing_events")

    # Active tenants per month (had at least one invoice_paid)
    active = (
        events
        .filter(F.col("event_type") == "invoice_paid")
        .groupBy("tenant_id", "event_year", "event_month", "plan")
        .agg(F.count("*").alias("invoice_count"))
        .withColumn(
            "period",
            F.concat(F.col("event_year"), F.lit("-"), F.lpad(F.col("event_month"), 2, "0"))
        )
    )

    active_count = (
        active
        .groupBy("event_year", "event_month", "period", "plan")
        .agg(F.countDistinct("tenant_id").alias("active_tenants"))
    )

    # Churned tenants per month
    churned = (
        events
        .filter(F.col("event_type") == "subscription_cancelled")
        .groupBy("tenant_id", "event_year", "event_month", "plan")
        .agg(F.count("*").alias("cancels"))
        .withColumn(
            "period",
            F.concat(F.col("event_year"), F.lit("-"), F.lpad(F.col("event_month"), 2, "0"))
        )
    )

    churned_count = (
        churned
        .groupBy("event_year", "event_month", "period", "plan")
        .agg(F.countDistinct("tenant_id").alias("churned_tenants"))
    )

    churn_rate = (
        active_count
        .join(churned_count, ["event_year", "event_month", "period", "plan"], "left")
        .fillna(0, subset=["churned_tenants"])
        .withColumn(
            "churn_rate_pct",
            F.round(F.col("churned_tenants") / F.col("active_tenants") * 100, 2)
        )
        .orderBy("event_year", "event_month", "plan")
    )

    return churn_rate


def compute_cohort_retention(spark):
    """
    Cohort retention matrix:
    - Cohort = month of first subscription_created
    - Tracks what % of that cohort is still active N months later
    - Output: pivot table cohort x months_since_start
    """
    events = spark.read.parquet("data/processed/billing_events")

    # First event date per tenant = cohort assignment
    first_event = (
        events
        .filter(F.col("event_type") == "subscription_created")
        .groupBy("tenant_id")
        .agg(
            F.min("event_date").alias("first_event_date"),
        )
        .withColumn("cohort_year",  F.year("first_event_date"))
        .withColumn("cohort_month", F.month("first_event_date"))
        .withColumn(
            "cohort",
            F.concat(F.col("cohort_year"), F.lit("-"), F.lpad(F.col("cohort_month"), 2, "0"))
        )
    )

    # First cancellation month per tenant. This treats the first cancellation
    # as churn for retention purposes even if the synthetic stream has later events.
    first_cancel = (
        events
        .filter(F.col("event_type") == "subscription_cancelled")
        .groupBy("tenant_id")
        .agg(F.min("event_date").alias("first_cancel_date"))
        .withColumn("cancel_year", F.year("first_cancel_date"))
        .withColumn("cancel_month", F.month("first_cancel_date"))
    )

    offsets = spark.range(0, 12).withColumnRenamed("id", "months_since_start")
    tenant_months = (
        first_event
        .select("tenant_id", "cohort", "cohort_year", "cohort_month")
        .crossJoin(offsets)
        .join(first_cancel.select("tenant_id", "cancel_year", "cancel_month"), "tenant_id", "left")
        .withColumn(
            "cancel_offset",
            (F.col("cancel_year") - F.col("cohort_year")) * 12
            + (F.col("cancel_month") - F.col("cohort_month"))
        )
        .withColumn(
            "is_retained",
            (F.col("months_since_start") == 0)
            | F.col("cancel_offset").isNull()
            | (F.col("cancel_offset") > F.col("months_since_start"))
        )
    )

    # Cohort sizes
    cohort_sizes = (
        first_event
        .groupBy("cohort")
        .agg(F.countDistinct("tenant_id").alias("cohort_size"))
    )

    retention = (
        tenant_months
        .filter(F.col("is_retained"))
        .groupBy("cohort", "months_since_start")
        .agg(F.countDistinct("tenant_id").alias("retained_tenants"))
        .join(cohort_sizes, "cohort")
        .withColumn(
            "retention_pct",
            F.round(F.col("retained_tenants") / F.col("cohort_size") * 100, 1)
        )
    )
    return retention, cohort_sizes


def run_pandas():
    print("\n[JOB 3] Churn Rate + Cohort Retention (Pandas Fallback Engine)\n")
    import pandas as pd

    events_path = "data/processed/billing_events"
    if not os.path.exists(events_path):
        print("  [ERROR] Processed billing events missing from Job 1. Please run etl_ingest.py first.")
        return

    # Read events parquet
    events = pd.read_parquet(events_path)
    events["event_year"] = events["event_year"].astype(int)
    events["event_month"] = events["event_month"].astype(int)

    # Churn rate.
    # Active tenants per month (had at least one invoice_paid)
    active_events = events[events["event_type"] == "invoice_paid"]
    active_grouped = (
        active_events
        .groupby(["tenant_id", "event_year", "event_month", "plan"])
        .size()
        .reset_index(name="invoice_count")
    )
    active_grouped["period"] = (
        active_grouped["event_year"].astype(str) + "-" +
        active_grouped["event_month"].astype(str).str.zfill(2)
    )

    active_count = (
        active_grouped
        .groupby(["event_year", "event_month", "period", "plan"])["tenant_id"]
        .nunique()
        .reset_index(name="active_tenants")
    )

    # Churned tenants per month
    churn_events = events[events["event_type"] == "subscription_cancelled"]
    churn_grouped = (
        churn_events
        .groupby(["tenant_id", "event_year", "event_month", "plan"])
        .size()
        .reset_index(name="cancels")
    )
    churn_grouped["period"] = (
        churn_grouped["event_year"].astype(str) + "-" +
        churn_grouped["event_month"].astype(str).str.zfill(2)
    )

    churned_count = (
        churn_grouped
        .groupby(["event_year", "event_month", "period", "plan"])["tenant_id"]
        .nunique()
        .reset_index(name="churned_tenants")
    )

    # Merge active and churned counts
    churn_rate = pd.merge(
        active_count,
        churned_count,
        on=["event_year", "event_month", "period", "plan"],
        how="left"
    ).fillna({"churned_tenants": 0})

    churn_rate["churn_rate_pct"] = (
        (churn_rate["churned_tenants"] / churn_rate["active_tenants"] * 100).round(2)
    )
    churn_rate = churn_rate.sort_values(["event_year", "event_month", "plan"])

    print("  Churn Rate by Plan (sample for month 6):")
    print(churn_rate[churn_rate["event_month"] == 6].head(10).to_string(index=False))

    # Cohort retention.
    # First event date per tenant = cohort assignment
    created_events = events[events["event_type"] == "subscription_created"].copy()
    created_events["event_date"] = pd.to_datetime(created_events["event_date"])

    first_event = (
        created_events
        .groupby("tenant_id")["event_date"]
        .min()
        .reset_index(name="first_event_date")
    )

    first_event["cohort_year"] = first_event["first_event_date"].dt.year
    first_event["cohort_month"] = first_event["first_event_date"].dt.month
    first_event["cohort"] = (
        first_event["cohort_year"].astype(str) + "-" +
        first_event["cohort_month"].astype(str).str.zfill(2)
    )

    # First cancellation month per tenant. This makes retention decline after churn.
    cancel_events = events[events["event_type"] == "subscription_cancelled"].copy()
    cancel_events["event_date"] = pd.to_datetime(cancel_events["event_date"])
    first_cancel = (
        cancel_events
        .groupby("tenant_id")["event_date"]
        .min()
        .reset_index(name="first_cancel_date")
    )
    first_cancel["cancel_year"] = first_cancel["first_cancel_date"].dt.year
    first_cancel["cancel_month"] = first_cancel["first_cancel_date"].dt.month

    tenant_months = first_event.merge(first_cancel[["tenant_id", "cancel_year", "cancel_month"]], on="tenant_id", how="left")
    tenant_months = tenant_months.loc[tenant_months.index.repeat(12)].copy()
    tenant_months["months_since_start"] = list(range(12)) * len(first_event)
    tenant_months["cancel_offset"] = (
        (tenant_months["cancel_year"] - tenant_months["cohort_year"]) * 12
        + (tenant_months["cancel_month"] - tenant_months["cohort_month"])
    )
    tenant_months["is_retained"] = (
        (tenant_months["months_since_start"] == 0)
        | tenant_months["cancel_offset"].isna()
        | (tenant_months["cancel_offset"] > tenant_months["months_since_start"])
    )

    # Cohort sizes
    cohort_sizes = (
        first_event
        .groupby("cohort")["tenant_id"]
        .nunique()
        .reset_index(name="cohort_size")
    )

    # Retention per cohort per month. Month 0 is the cohort baseline.
    retention = (
        tenant_months[tenant_months["is_retained"]]
        .groupby(["cohort", "months_since_start"])["tenant_id"]
        .nunique()
        .reset_index(name="retained_tenants")
    )

    retention = pd.merge(retention, cohort_sizes, on="cohort", how="inner")
    retention["retention_pct"] = (
        (retention["retained_tenants"] / retention["cohort_size"] * 100).round(1)
    )

    print("\n  Cohort Retention (sample of first 20 rows):")
    print(retention.sort_values(["cohort", "months_since_start"]).head(20).to_string(index=False))

    # Write output parquets
    # Clean output directories if they exist
    for path in ["data/processed/churn_by_plan_month", "data/processed/cohort_retention", "data/processed/cohort_sizes"]:
        recreate_dir(path)

    churn_rate.to_parquet(os.path.join("data/processed/churn_by_plan_month", "part.parquet"), index=False)
    retention.to_parquet(os.path.join("data/processed/cohort_retention", "part.parquet"), index=False)
    cohort_sizes.to_parquet(os.path.join("data/processed/cohort_sizes", "part.parquet"), index=False)

    print("\n  [OK] Churn data written     -> data/processed/churn_by_plan_month/")
    print("  [OK] Retention written      -> data/processed/cohort_retention/")
    print("\n[OK] Job 3 complete.\n")


def run():
    spark = get_spark("Churn-Cohort")
    if spark is None:
        run_pandas()
        return

    print("\n[JOB 3] Churn Rate + Cohort Retention\n")

    # Churn rate.
    churn_df = compute_churn_rate(spark)
    print("  Churn Rate by Plan (sample):")
    churn_df.filter(F.col("event_month") == 6).show(10, truncate=False)

    # Cohort retention.
    retention_df, cohort_sizes_df = compute_cohort_retention(spark)
    print("  Cohort Retention (month 0 = signup month):")
    retention_df.orderBy("cohort", "months_since_start").show(20, truncate=False)

    # Write analytical datasets.
    churn_df.write.mode("overwrite").parquet("data/processed/churn_by_plan_month")
    retention_df.write.mode("overwrite").parquet("data/processed/cohort_retention")
    cohort_sizes_df.write.mode("overwrite").parquet("data/processed/cohort_sizes")

    print("\n  [OK] Churn data written     -> data/processed/churn_by_plan_month/")
    print("  [OK] Retention written      -> data/processed/cohort_retention/")
    print("\n[OK] Job 3 complete.\n")
    spark.stop()


if __name__ == "__main__":
    run()
