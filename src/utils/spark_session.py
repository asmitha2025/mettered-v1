"""
spark_session.py
----------------
Creates and returns a configured SparkSession.
Centralised here so every job uses the same config.
Safely falls back to returning None if PySpark or Java is missing.
"""

def get_spark(app_name: str = "SubscriptionIntelligence"):
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
        return spark
    except Exception as e:
        # Gracefully handle missing PySpark or missing Java
        return None

