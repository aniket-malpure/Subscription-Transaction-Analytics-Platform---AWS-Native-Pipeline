"""
EMR Spark Structured Streaming Job — Near Real-Time Subscription Events
Reads from Kinesis Data Streams, applies watermarking for late data,
writes micro-batches (5-min) to S3 curated layer in Parquet format.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
REGION = "us-east-1"
SUBSCRIPTION_STREAM_ARN = "arn:aws:kinesis:us-east-1:xxxx:stream/subscription-events"
TRANSACTION_STREAM_ARN = "arn:aws:kinesis:us-east-1:xxxx:stream/transaction-events"
CHECKPOINT_BASE = "s3://pv-checkpoints-bucket/streaming"
OUTPUT_BASE = "s3://pv-curated-bucket/streaming"
TRIGGER_INTERVAL = "5 minutes"
WATERMARK_DELAY = "10 minutes"
MAX_RECORDS_PER_TRIGGER = 10000


# ── Schemas ───────────────────────────────────────────────────────────────────
SUBSCRIPTION_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("plan", StringType(), True),
    StructField("region", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("previous_state", StringType(), True),
    StructField("new_state", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("source_system", StringType(), True),
])

TRANSACTION_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("transaction_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount_usd", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("region", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("status", StringType(), True),
    StructField("plan", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PV-Subscription-Streaming-ETL")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Kinesis connector config
        .config("spark.jars.packages", "com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0")
        .getOrCreate()
    )


def read_kinesis_stream(spark: SparkSession, stream_arn: str, schema: StructType):
    """
    Read from Kinesis using TRIM_HORIZON (start of stream).
    For production, use LATEST to read only new records after recovery.
    """
    df_raw = (
        spark.readStream
        .format("kinesis")
        .option("streamName", stream_arn.split("/")[-1])
        .option("endpointUrl", f"https://kinesis.{REGION}.amazonaws.com")
        .option("regionName", REGION)
        .option("startingPosition", "LATEST")
        .option("maxRecordsPerFetch", MAX_RECORDS_PER_TRIGGER)
        .option("kinesis.executor.maxFetchTimeInMs", "30000")
        .load()
    )

    # Kinesis data comes as binary — decode and parse JSON
    df_parsed = (
        df_raw
        .select(F.from_json(F.col("data").cast("string"), schema).alias("payload"))
        .select("payload.*")
    )
    return df_parsed


# ── Streaming Transformations ─────────────────────────────────────────────────
def transform_subscription_stream(df):
    return (
        df
        # Cast event_time to timestamp
        .withColumn("event_ts", F.to_timestamp("event_time"))
        # Apply watermark for late data tolerance
        .withWatermark("event_ts", WATERMARK_DELAY)
        # Derive useful columns
        .withColumn("is_churn_event", F.col("event_type") == "CANCELLED")
        .withColumn("is_reactivation", F.col("event_type") == "REACTIVATED")
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .withColumn("ingestion_ts", F.current_timestamp())
        # Drop null critical fields
        .filter(F.col("event_id").isNotNull() & F.col("customer_id").isNotNull())
        .dropDuplicates(["event_id", "customer_id"])
    )


def transform_transaction_stream(df):
    return (
        df
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withWatermark("event_ts", WATERMARK_DELAY)
        .withColumn("is_refund", F.col("transaction_type") == "REFUND")
        .withColumn(
            "net_amount_usd",
            F.when(F.col("is_refund") & (F.col("amount_usd") > 0), -F.col("amount_usd"))
             .otherwise(F.col("amount_usd"))
        )
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .withColumn("ingestion_ts", F.current_timestamp())
        .filter(
            F.col("event_id").isNotNull() &
            F.col("transaction_id").isNotNull() &
            F.col("customer_id").isNotNull()
        )
        .dropDuplicates(["transaction_id"])
    )


# ── Windowed Aggregations (5-min windows) ─────────────────────────────────────
def compute_windowed_subscription_agg(df):
    """
    Compute 5-minute window aggregations for near real-time dashboards.
    Uses tumbling windows with watermark-aware state management.
    """
    return (
        df
        .groupBy(
            F.window("event_ts", "5 minutes"),
            "region",
            "plan",
            "event_type"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.col("is_churn_event").cast("int")).alias("churn_count"),
            F.avg("price_usd").alias("avg_price_usd"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "region", "plan", "event_type",
            "event_count", "unique_customers", "churn_count", "avg_price_usd"
        )
    )


# ── Write Streams ─────────────────────────────────────────────────────────────
def write_stream_to_s3(df, stream_name: str, partition_cols: list):
    """Write streaming DataFrame to S3 in micro-batches."""
    checkpoint_path = f"{CHECKPOINT_BASE}/{stream_name}"
    output_path = f"{OUTPUT_BASE}/{stream_name}"

    return (
        df.writeStream
          .outputMode("append")
          .format("parquet")
          .option("path", output_path)
          .option("checkpointLocation", checkpoint_path)
          .partitionBy(*partition_cols)
          .trigger(processingTime=TRIGGER_INTERVAL)
          .start()
    )


def write_windowed_agg_to_s3(df, stream_name: str):
    """Write windowed aggregations — uses complete mode for accuracy."""
    checkpoint_path = f"{CHECKPOINT_BASE}/{stream_name}_agg"
    output_path = f"{OUTPUT_BASE}/{stream_name}_agg"

    return (
        df.writeStream
          .outputMode("append")   # append works with watermark + window
          .format("parquet")
          .option("path", output_path)
          .option("checkpointLocation", checkpoint_path)
          .trigger(processingTime=TRIGGER_INTERVAL)
          .start()
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting subscription streaming job...")

    # Read streams
    df_sub_raw = read_kinesis_stream(spark, SUBSCRIPTION_STREAM_ARN, SUBSCRIPTION_SCHEMA)
    df_txn_raw = read_kinesis_stream(spark, TRANSACTION_STREAM_ARN, TRANSACTION_SCHEMA)

    # Transform
    df_sub = transform_subscription_stream(df_sub_raw)
    df_txn = transform_transaction_stream(df_txn_raw)

    # Windowed aggregations
    df_sub_agg = compute_windowed_subscription_agg(df_sub)

    # Write raw enriched events
    query_sub = write_stream_to_s3(df_sub, "subscriptions", ["event_date", "event_hour"])
    query_txn = write_stream_to_s3(df_txn, "transactions", ["event_date", "event_hour"])

    # Write aggregations
    query_sub_agg = write_windowed_agg_to_s3(df_sub_agg, "subscription_agg")

    logger.info("Streaming queries started. Awaiting termination...")

    # Wait for all queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
