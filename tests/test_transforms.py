"""
test_transforms.py - Unit Tests
----------------------------------
Tests for ETL validation logic and MRR calculations.
Supports dual-engine execution: tests PySpark if Java is available,
or falls back to testing the equivalent Pandas logic if Spark is missing.
Run:
    pytest tests/ -v
"""

import pytest
import sys
import os
import pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType,
        IntegerType, TimestampType, BooleanType
    )
except ImportError:
    SparkSession = None
    F = None
    StructType = StructField = StringType = IntegerType = TimestampType = BooleanType = None


@pytest.fixture(scope="session")
def spark():
    """Single SparkSession shared across all tests. Returns None if Java is missing."""
    import shutil
    if SparkSession is not None and shutil.which("java") is not None:
        try:
            return (
                SparkSession.builder
                .appName("Tests")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
        except Exception:
            pass
    return None


# Test data.

if StructType is not None:
    EVENTS_SCHEMA = StructType([
        StructField("event_id",   StringType(),    False),
        StructField("tenant_id",  StringType(),    False),
        StructField("event_type", StringType(),    False),
        StructField("plan",       StringType(),    True),
        StructField("amount",     IntegerType(),   True),
        StructField("currency",   StringType(),    True),
        StructField("event_date", TimestampType(), True),
        StructField("status",     StringType(),    True),
        StructField("invoice_id", StringType(),    True),
        StructField("seats",      IntegerType(),   True),
        StructField("trial",      BooleanType(),   True),
    ])
else:
    EVENTS_SCHEMA = None

SAMPLE_EVENTS = [
    ("EVT_001", "TNT_001", "invoice_paid", "starter", 299, "INR", "2024-01-15 10:00:00", "success", "INV_001", 2, False),
    ("EVT_002", "TNT_001", "invoice_paid", "starter", 299, "INR", "2024-02-15 10:00:00", "success", "INV_002", 2, False),
    ("EVT_003", "TNT_002", "invoice_paid", "growth", 999, "INR", "2024-01-20 10:00:00", "success", "INV_003", 5, False),
    ("EVT_004", "TNT_001", "subscription_cancelled", "starter", 0, "INR", "2024-03-01 10:00:00", "success", "INV_004", 2, False),
    ("EVT_005", "TNT_003", "invoice_paid", "business", 2499, "INR", "2024-01-25 10:00:00", "success", "INV_005", 10, False),
    ("EVT_006", "TNT_999", "invoice_paid", "INVALID", -100, "INR", "2024-01-01 10:00:00", "success", "INV_006", 1, False),
]


def make_events_df(spark, rows=None):
    data = rows or SAMPLE_EVENTS
    # Parse timestamps from string
    from pyspark.sql import functions as F
    raw = spark.createDataFrame(data, schema=[
        "event_id", "tenant_id", "event_type", "plan", "amount",
        "currency", "event_date_str", "status", "invoice_id", "seats", "trial"
    ])
    return raw.withColumn("event_date", F.to_timestamp("event_date_str", "yyyy-MM-dd HH:mm:ss")).drop("event_date_str")


def make_events_df_pandas(rows=None):
    data = rows or SAMPLE_EVENTS
    df = pd.DataFrame(data, columns=[
        "event_id", "tenant_id", "event_type", "plan", "amount",
        "currency", "event_date_str", "status", "invoice_id", "seats", "trial"
    ])
    df["event_date"] = pd.to_datetime(df["event_date_str"])
    return df.drop(columns=["event_date_str"])


# Tests.

class TestValidation:

    def test_negative_amount_rejected(self, spark):
        """Rows with negative amounts must be flagged as invalid."""
        if spark is None:
            df = make_events_df_pandas()
            invalid = df[df["amount"] < 0]
            assert len(invalid) == 1, "Expected exactly 1 row with negative amount"
        else:
            df = make_events_df(spark)
            invalid = df.filter(F.col("amount") < 0)
            assert invalid.count() == 1, "Expected exactly 1 row with negative amount"

    def test_valid_events_count(self, spark):
        """Non-negative, known event_type rows should pass validation."""
        valid_types = {"subscription_created", "subscription_upgraded",
                       "subscription_downgraded", "subscription_cancelled",
                       "invoice_paid", "invoice_failed", "trial_started", "trial_converted"}
        if spark is None:
            df = make_events_df_pandas()
            clean = df[(df["amount"] >= 0) & (df["event_type"].isin(valid_types))]
            assert len(clean) == 5, f"Expected 5 clean rows, got {len(clean)}"
        else:
            df = make_events_df(spark)
            clean = df.filter(
                F.col("amount") >= 0
            ).filter(
                F.col("event_type").isin(list(valid_types))
            )
            assert clean.count() == 5, f"Expected 5 clean rows, got {clean.count()}"

    def test_no_null_tenant_ids(self, spark):
        """All sample rows should have non-null tenant_id."""
        if spark is None:
            df = make_events_df_pandas()
            nulls = df["tenant_id"].isna().sum()
            assert nulls == 0
        else:
            df = make_events_df(spark)
            nulls = df.filter(F.col("tenant_id").isNull()).count()
            assert nulls == 0


class TestMRRCalculation:

    def test_mrr_sum_correct(self, spark):
        """MRR for January should be 299 + 999 + 2499 = 3797."""
        if spark is None:
            df = make_events_df_pandas()
            jan_mrr = df[
                (df["event_type"] == "invoice_paid") &
                (df["status"] == "success") &
                (df["event_date"].dt.month == 1) &
                (df["amount"] >= 0)
            ]["amount"].sum()
            assert jan_mrr == 3797, f"Expected 3797, got {jan_mrr}"
        else:
            df = make_events_df(spark)
            jan_mrr = (
                df
                .filter(F.col("event_type") == "invoice_paid")
                .filter(F.col("status") == "success")
                .filter(F.month("event_date") == 1)
                .filter(F.col("amount") >= 0)
                .agg(F.sum("amount").alias("total"))
                .collect()[0]["total"]
            )
            assert jan_mrr == 3797, f"Expected 3797, got {jan_mrr}"

    def test_mrr_per_tenant(self, spark):
        """TNT_001 should have MRR of 299 in January."""
        if spark is None:
            df = make_events_df_pandas()
            tnt001_jan = df[
                (df["tenant_id"] == "TNT_001") &
                (df["event_type"] == "invoice_paid") &
                (df["event_date"].dt.month == 1)
            ]["amount"].sum()
            assert tnt001_jan == 299
        else:
            df = make_events_df(spark)
            tnt001_jan = (
                df
                .filter(F.col("tenant_id") == "TNT_001")
                .filter(F.col("event_type") == "invoice_paid")
                .filter(F.month("event_date") == 1)
                .agg(F.sum("amount"))
                .collect()[0][0]
            )
            assert tnt001_jan == 299

    def test_cancelled_tenant_zero_revenue(self, spark):
        """Cancelled event should contribute 0 to revenue."""
        if spark is None:
            df = make_events_df_pandas()
            cancel_revenue = df[df["event_type"] == "subscription_cancelled"]["amount"].sum()
            assert cancel_revenue == 0
        else:
            df = make_events_df(spark)
            cancel_revenue = (
                df
                .filter(F.col("event_type") == "subscription_cancelled")
                .agg(F.sum("amount"))
                .collect()[0][0]
            )
            assert cancel_revenue == 0 or cancel_revenue is None


class TestChurnLogic:

    def test_churn_event_count(self, spark):
        """Sample data has 1 cancellation event."""
        if spark is None:
            df = make_events_df_pandas()
            churn_count = len(df[df["event_type"] == "subscription_cancelled"])
            assert churn_count == 1
        else:
            df = make_events_df(spark)
            churn_count = df.filter(F.col("event_type") == "subscription_cancelled").count()
            assert churn_count == 1

    def test_churn_rate_below_100(self, spark):
        """Churn rate should never exceed 100%."""
        if spark is None:
            df = make_events_df_pandas()
            active = len(df[df["event_type"] == "invoice_paid"])
            churned = len(df[df["event_type"] == "subscription_cancelled"])
            rate = (churned / active * 100) if active > 0 else 0
            assert rate <= 100
        else:
            df = make_events_df(spark)
            active = df.filter(F.col("event_type") == "invoice_paid").count()
            churned = df.filter(F.col("event_type") == "subscription_cancelled").count()
            rate = (churned / active * 100) if active > 0 else 0
            assert rate <= 100


class TestEnrichment:

    def test_year_month_extraction(self, spark):
        """event_year and event_month should be correctly extracted."""
        if spark is None:
            df = make_events_df_pandas()
            df["yr"] = df["event_date"].dt.year
            df["mo"] = df["event_date"].dt.month
            jan_rows = len(df[df["mo"] == 1])
            assert jan_rows == 4  # EVT_001, EVT_003, EVT_005, and EVT_006 are in January
        else:
            df = make_events_df(spark)
            enriched = df.withColumn("yr", F.year("event_date")).withColumn("mo", F.month("event_date"))
            jan_rows = enriched.filter(F.col("mo") == 1).count()
            assert jan_rows == 4  # EVT_001, EVT_003, EVT_005, and EVT_006 are in January
