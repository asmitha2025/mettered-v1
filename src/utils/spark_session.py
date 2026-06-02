"""
spark_session.py
----------------
Creates and returns a configured SparkSession.
Centralised here so every job uses the same config.
Safely falls back to returning None if PySpark, Java, or local platform
support is not ready.
"""

import os
import shutil
import sys
import logging
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SparkSessionProvider")


def java_available() -> bool:
    """Return True when Java can be found through PATH or JAVA_HOME."""
    if shutil.which("java") is not None:
        return True

    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        return False

    executable = "java.exe" if sys.platform.startswith("win") else "java"
    return os.path.exists(os.path.join(java_home, "bin", executable))


def spark_runtime_allowed(log_reason: bool = True) -> bool:
    """
    Decide whether local jobs should attempt PySpark.

    On Windows, Spark can initialize with Java but still fail on local Parquet
    commits without Hadoop native binaries. Auto mode prefers the reliable
    Pandas path there. Set SUBSCRIPTION_PIPELINE_ENGINE=spark to force Spark
    after configuring a proper Hadoop/winutils setup.
    """
    engine = os.environ.get("SUBSCRIPTION_PIPELINE_ENGINE", "auto").strip().lower()
    if engine in {"pandas", "fallback"}:
        if log_reason:
            logger.info("SUBSCRIPTION_PIPELINE_ENGINE=%s; using Pandas fallback.", engine)
        return False

    if engine not in {"auto", "spark"}:
        if log_reason:
            logger.warning("Unknown SUBSCRIPTION_PIPELINE_ENGINE=%r; using auto mode.", engine)
        engine = "auto"

    if sys.platform.startswith("win") and engine != "spark":
        if log_reason:
            logger.info(
                "Skipping PySpark auto mode on Windows. Local Spark writes need Hadoop native binaries; "
                "set SUBSCRIPTION_PIPELINE_ENGINE=spark to force Spark after configuring them."
            )
        return False

    return True


def get_spark(app_name: str = "SubscriptionIntelligence") -> Optional["SparkSession"]:
    """
    Tries to initialize and return a centralized SparkSession.
    Returns None if pyspark or Java runtime environment is missing.
    """
    if not spark_runtime_allowed():
        return None

    if not java_available():
        logger.warning("Java runtime is not available (falling back to Pandas-only engine).")
        return None

    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")        # tuned for local dev
            .config("spark.sql.adaptive.enabled", "true")       # AQE for auto-optimisation
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Successfully initialized PySpark Session: '{app_name}'")
        return spark
    except Exception as e:
        logger.warning(f"PySpark or Java runtime is not available (falling back to Pandas-only engine). Details: {e}")
        return None
