"""
etl_ingest.py - Job 1: Raw -> Validated -> Parquet
----------------------------------------------------
Reads raw CSVs, enforces schema, validates data quality,
and writes partitioned Parquet to data/processed/.

Run:
    spark-submit src/ingestion/etl_ingest.py
    OR
    python src/ingestion/etl_ingest.py
"""

import sys
import os
import logging
from typing import Tuple

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ETL-Ingest")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType,
        IntegerType, TimestampType, BooleanType
    )
except ImportError:
    DataFrame = None
    F = None
    StructType = StructField = StringType = IntegerType = TimestampType = BooleanType = None

from utils.spark_session import get_spark
from utils.filesystem import recreate_dir


if StructType is not None:
    EVENTS_SCHEMA = StructType([
        StructField("event_id",    StringType(),    False),
        StructField("tenant_id",   StringType(),    False),
        StructField("event_type",  StringType(),    False),
        StructField("plan",        StringType(),    True),
        StructField("amount",      IntegerType(),   True),
        StructField("currency",    StringType(),    True),
        StructField("event_date",  TimestampType(), True),
        StructField("status",      StringType(),    True),
        StructField("invoice_id",  StringType(),    True),
        StructField("seats",       IntegerType(),   True),
        StructField("trial",       BooleanType(),   True),
    ])

    TENANTS_SCHEMA = StructType([
        StructField("tenant_id",    StringType(), False),
        StructField("company_name", StringType(), True),
        StructField("industry",     StringType(), True),
        StructField("plan",         StringType(), True),
        StructField("country",      StringType(), True),
        StructField("signup_date",  StringType(), True),
    ])
else:
    EVENTS_SCHEMA = None
    TENANTS_SCHEMA = None

VALID_EVENT_TYPES = {
    "subscription_created", "subscription_upgraded", "subscription_downgraded",
    "subscription_cancelled", "invoice_paid", "invoice_failed",
    "trial_started", "trial_converted"
}

VALID_PLANS = {"starter", "growth", "business", "enterprise"}


def validate_events(df: "DataFrame") -> Tuple["DataFrame", "DataFrame"]:
    """Apply data quality checks. Returns (clean_df, rejected_df)."""
    # Tag rows with validation failures
    df = df.withColumn(
        "validation_error",
        F.when(F.col("event_id").isNull(), "missing event_id")
         .when(F.col("tenant_id").isNull(), "missing tenant_id")
         .when(~F.col("event_type").isin(list(VALID_EVENT_TYPES)), "invalid event_type")
         .when(F.col("amount") < 0, "negative amount")
         .when(F.col("event_date").isNull(), "null event_date")
         .otherwise(None)
    )
    clean = df.filter(F.col("validation_error").isNull()).drop("validation_error")
    rejected = df.filter(F.col("validation_error").isNotNull())
    return clean, rejected


def run_pandas() -> None:
    logger.info("Starting Job 1 ETL using Pandas fallback engine")
    import pandas as pd
    import numpy as np

    events_path = "data/raw/billing_events.csv"
    tenants_path = "data/raw/tenants.csv"

    if not os.path.exists(events_path) or not os.path.exists(tenants_path):
        logger.error("Raw source CSV files missing. Execute generate_data.py first.")
        return

    # Read CSVs
    events_raw = pd.read_csv(events_path, parse_dates=["event_date"], encoding="utf-8")
    tenants_raw = pd.read_csv(tenants_path, encoding="utf-8")

    logger.info(f"Loaded raw events shape: {events_raw.shape}")
    logger.info(f"Loaded raw tenants shape: {tenants_raw.shape}")

    # Validate
    validation_error = np.select(
        [
            events_raw["event_id"].isna(),
            events_raw["tenant_id"].isna(),
            ~events_raw["event_type"].isin(VALID_EVENT_TYPES),
            events_raw["amount"] < 0,
            events_raw["event_date"].isna()
        ],
        [
            "missing event_id",
            "missing tenant_id",
            "invalid event_type",
            "negative amount",
            "null event_date"
        ],
        default=None
    )

    # Cast to object to avoid string/None type comparison issues in np.select
    events_raw["validation_error"] = pd.Series(validation_error, dtype=object)
    clean = events_raw[events_raw["validation_error"].isna()].drop(columns=["validation_error"]).copy()
    rejected = events_raw[events_raw["validation_error"].notna()]

    rejected_count = len(rejected)
    if rejected_count > 0:
        logger.warning(f"Found {rejected_count:,} invalid rows violating schemas. Piping to DLQ.")
        os.makedirs("data/processed/rejected", exist_ok=True)
        rejected.to_parquet("data/processed/rejected/rejected.parquet", index=False)

    # Enrich
    clean["event_year"] = clean["event_date"].dt.year
    clean["event_month"] = clean["event_date"].dt.month
    clean["event_date_only"] = clean["event_date"].dt.date
    clean["is_revenue_event"] = clean["event_type"].isin(["invoice_paid"]).astype(bool)
    clean["is_churn_event"] = clean["event_type"].isin(["subscription_cancelled"]).astype(bool)

    # Write Partitioned Parquet
    for path in ["data/processed/billing_events", "data/processed/tenants"]:
        recreate_dir(path)

    clean.to_parquet("data/processed/billing_events", partition_cols=["event_year", "event_month"], index=False)
    tenants_raw.to_parquet(os.path.join("data/processed/tenants", "part.parquet"), index=False)

    logger.info("Partitioned clean events Parquet datasets successfully generated.")
    logger.info("Validated tenants list Parquet successfully generated.")
    logger.info("Job 1 complete successfully.")


def run() -> None:
    spark = get_spark("ETL-Ingest")
    if spark is None:
        run_pandas()
        return

    logger.info("Starting Job 1 ETL using PySpark engine")

    # Read source CSVs.
    events_raw = (
        spark.read
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(EVENTS_SCHEMA)
        .csv("data/raw/billing_events.csv")
    )

    tenants_raw = (
        spark.read
        .option("header", "true")
        .schema(TENANTS_SCHEMA)
        .csv("data/raw/tenants.csv")
    )

    logger.info("Raw source datasets successfully read into Spark environment.")

    # Validate rows and send invalid records to the dead-letter output.
    clean_events, rejected = validate_events(events_raw)
    rejected_count = rejected.count()
    if rejected_count > 0:
        logger.warning(f"Found {rejected_count:,} invalid rows violating schemas. Piping to DLQ.")
        rejected.write.mode("overwrite").parquet("data/processed/rejected")

    # Enrich events with partitions and metric flags.
    enriched = (
        clean_events
        .withColumn("event_year",  F.year("event_date"))
        .withColumn("event_month", F.month("event_date"))
        .withColumn("event_date_only", F.to_date("event_date"))
        .withColumn(
            "is_revenue_event",
            F.col("event_type").isin(["invoice_paid"]).cast("boolean"))
        .withColumn(
            "is_churn_event",
            F.col("event_type").isin(["subscription_cancelled"]).cast("boolean"))
    )

    # Write partitioned Parquet datasets.
    (
        enriched
        .write
        .mode("overwrite")
        .partitionBy("event_year", "event_month")
        .parquet("data/processed/billing_events")
    )

    (
        tenants_raw
        .write
        .mode("overwrite")
        .parquet("data/processed/tenants")
    )

    logger.info("Partitioned clean events Parquet datasets successfully generated.")
    logger.info("Validated tenants list Parquet successfully generated.")
    logger.info("Job 1 complete successfully.")
    spark.stop()


if __name__ == "__main__":
    run()
