"""
EMR Spark Batch Job — Subscription & Transaction Analytics
Runs nightly at 2AM. Reads from S3 Raw layer, deduplicates,
applies business rules, and writes curated Parquet to S3.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
)
from pyspark.sql.window import Window
import logging
import sys
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
RAW_S3_BASE = "s3://pv-raw-bucket"
CURATED_S3_BASE = "s3://pv-curated-bucket"
CUSTOMER_DIM_PATH = "s3://pv-curated-bucket/dimensions/dim_customers/"
REDSHIFT_JDBC = "jdbc:redshift://pv-cluster.xxxx.us-east-1.redshift.amazonaws.com:5439/pvdb"
REDSHIFT_USER = "etl_user"
REDSHIFT_PASSWORD_SECRET = "arn:aws:secretsmanager:us-east-1:xxxx:secret:redshift-creds"
REDSHIFT_TEMP_DIR = "s3://pv-temp-bucket/redshift-temp/"


# ── Schemas ──────────────────────────────────────────────────────────────────
SUBSCRIPTION_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("plan", StringType(), True),
    StructField("region", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("previous_state", StringType(), True),
    StructField("new_state", StringType(), True),
    StructField("subscription_start_date", StringType(), True),
    StructField("subscription_end_date", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("source_system", StringType(), True),
    StructField("schema_version", StringType(), True),
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
    StructField("source_system", StringType(), True),
    StructField("schema_version", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PV-Subscription-Batch-ETL")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def get_processing_dates(lookback_days: int = 1):
    """Return list of dates to process. Default: yesterday."""
    today = datetime.utcnow().date()
    return [today - timedelta(days=i) for i in range(1, lookback_days + 1)]


def build_s3_path(base: str, dataset: str, date) -> str:
    return f"{base}/{dataset}/year={date.year}/month={date.month:02d}/day={date.day:02d}/"


# ── Extract ───────────────────────────────────────────────────────────────────
def read_subscription_events(spark: SparkSession, dates: list):
    paths = [build_s3_path(RAW_S3_BASE, "subscriptions", d) for d in dates]
    logger.info(f"Reading subscription events from: {paths}")
    return spark.read.schema(SUBSCRIPTION_SCHEMA).parquet(*paths)


def read_transaction_events(spark: SparkSession, dates: list):
    paths = [build_s3_path(RAW_S3_BASE, "transactions", d) for d in dates]
    logger.info(f"Reading transaction events from: {paths}")
    return spark.read.schema(TRANSACTION_SCHEMA).parquet(*paths)


def read_customer_dim(spark: SparkSession):
    return spark.read.parquet(CUSTOMER_DIM_PATH)


# ── Transform: Subscriptions ─────────────────────────────────────────────────
def transform_subscriptions(df_raw, df_customers):
    logger.info("Transforming subscription events...")

    # 1. Cast event_time to timestamp
    df = df_raw.withColumn("event_time", F.to_timestamp("event_time"))

    # 2. Deduplicate: keep latest event per event_id
    window_dedup = Window.partitionBy("event_id").orderBy(F.desc("event_time"))
    df = (
        df.withColumn("row_num", F.row_number().over(window_dedup))
          .filter(F.col("row_num") == 1)
          .drop("row_num")
    )

    # 3. Apply subscription state machine validation
    valid_transitions = {
        ("TRIAL", "ACTIVE"): True,
        ("ACTIVE", "CANCELLED"): True,
        ("ACTIVE", "SUSPENDED"): True,
        ("CANCELLED", "ACTIVE"): True,  # reactivation
        ("SUSPENDED", "ACTIVE"): True,
        ("SUSPENDED", "CANCELLED"): True,
    }
    valid_pairs = [F.struct(F.lit(k[0]), F.lit(k[1])) for k in valid_transitions]

    # Flag invalid transitions
    df = df.withColumn(
        "is_valid_transition",
        F.struct(F.col("previous_state"), F.col("new_state")).isin(valid_pairs)
    )

    # 4. Enrich with customer dimension
    df = df.join(
        df_customers.select("customer_id", "customer_segment", "acquisition_channel", "country"),
        on="customer_id",
        how="left"
    )

    # 5. Derive churn flag
    df = df.withColumn(
        "is_churn_event",
        F.when(F.col("event_type") == "CANCELLED", True).otherwise(False)
    )

    # 6. Add processing metadata
    df = (
        df.withColumn("processed_at", F.current_timestamp())
          .withColumn("event_date", F.to_date("event_time"))
          .withColumn("event_year", F.year("event_time"))
          .withColumn("event_month", F.month("event_time"))
          .withColumn("event_day", F.dayofmonth("event_time"))
    )

    # 7. Null handling — reject records without critical fields
    df_valid = df.filter(
        F.col("event_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("event_time").isNotNull()
    )
    df_invalid = df.filter(
        F.col("event_id").isNull() |
        F.col("customer_id").isNull() |
        F.col("event_time").isNull()
    )

    invalid_count = df_invalid.count()
    if invalid_count > 0:
        logger.warning(f"Rejecting {invalid_count} invalid subscription records")
        df_invalid.write.mode("append").parquet(
            f"{CURATED_S3_BASE}/rejected/subscriptions/"
        )

    return df_valid


# ── Transform: Transactions ──────────────────────────────────────────────────
def transform_transactions(df_raw, df_customers):
    logger.info("Transforming transaction events...")

    df = df_raw.withColumn("event_time", F.to_timestamp("event_time"))

    # Deduplicate on transaction_id (idempotency key)
    window_dedup = Window.partitionBy("transaction_id").orderBy(F.desc("event_time"))
    df = (
        df.withColumn("row_num", F.row_number().over(window_dedup))
          .filter(F.col("row_num") == 1)
          .drop("row_num")
    )

    # Flag refunds
    df = df.withColumn(
        "is_refund",
        F.when(F.col("transaction_type") == "REFUND", True).otherwise(False)
    )

    # Normalize amount: refunds should always be negative
    df = df.withColumn(
        "amount_usd",
        F.when(F.col("is_refund") & (F.col("amount_usd") > 0), -F.col("amount_usd"))
         .otherwise(F.col("amount_usd"))
    )

    # Enrich
    df = df.join(
        df_customers.select("customer_id", "customer_segment", "country"),
        on="customer_id",
        how="left"
    )

    df = (
        df.withColumn("processed_at", F.current_timestamp())
          .withColumn("event_date", F.to_date("event_time"))
          .withColumn("event_year", F.year("event_time"))
          .withColumn("event_month", F.month("event_time"))
          .withColumn("event_day", F.dayofmonth("event_time"))
    )

    return df.filter(
        F.col("event_id").isNotNull() &
        F.col("transaction_id").isNotNull() &
        F.col("customer_id").isNotNull()
    )


# ── Aggregations ─────────────────────────────────────────────────────────────
def compute_daily_churn_agg(df_subscriptions):
    return (
        df_subscriptions
        .groupBy("event_date", "region", "plan", "customer_segment")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.col("is_churn_event").cast("int")).alias("churn_events"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("price_usd").alias("avg_price_usd"),
        )
        .withColumn(
            "churn_rate",
            F.round(F.col("churn_events") / F.col("total_events"), 4)
        )
    )


def compute_daily_revenue_agg(df_transactions):
    return (
        df_transactions
        .filter(F.col("status") == "SUCCESS")
        .groupBy("event_date", "region", "plan", "payment_method")
        .agg(
            F.sum("amount_usd").alias("gross_revenue_usd"),
            F.sum(F.when(F.col("is_refund"), F.col("amount_usd")).otherwise(0)).alias("refund_amount_usd"),
            F.count("*").alias("transaction_count"),
            F.countDistinct("customer_id").alias("paying_customers"),
        )
        .withColumn(
            "net_revenue_usd",
            F.col("gross_revenue_usd") + F.col("refund_amount_usd")  # refunds are negative
        )
    )


# ── Load ──────────────────────────────────────────────────────────────────────
def write_to_s3_curated(df, dataset_name: str, partition_cols: list):
    path = f"{CURATED_S3_BASE}/{dataset_name}/"
    logger.info(f"Writing {dataset_name} to {path}")
    (
        df.repartition(50)
          .write
          .mode("overwrite")
          .partitionBy(*partition_cols)
          .parquet(path)
    )
    logger.info(f"Successfully wrote {dataset_name}")


def write_to_redshift(df, table_name: str, mode: str = "append"):
    logger.info(f"Loading {table_name} into Redshift...")
    (
        df.write
          .format("io.github.spark_redshift_community.spark.redshift")
          .option("url", REDSHIFT_JDBC)
          .option("user", REDSHIFT_USER)
          .option("dbtable", table_name)
          .option("tempdir", REDSHIFT_TEMP_DIR)
          .option("forward_spark_s3_credentials", "true")
          .mode(mode)
          .save()
    )
    logger.info(f"Successfully loaded {table_name}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    lookback_days = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    spark = create_spark_session()
    dates = get_processing_dates(lookback_days)
    logger.info(f"Processing dates: {dates}")

    # Extract
    df_sub_raw = read_subscription_events(spark, dates)
    df_txn_raw = read_transaction_events(spark, dates)
    df_customers = read_customer_dim(spark)

    # Transform
    df_sub = transform_subscriptions(df_sub_raw, df_customers)
    df_txn = transform_transactions(df_txn_raw, df_customers)

    # Aggregations
    df_churn_agg = compute_daily_churn_agg(df_sub)
    df_revenue_agg = compute_daily_revenue_agg(df_txn)

    # Cache aggregations before writing
    df_sub.cache()
    df_txn.cache()

    # Write to S3 curated
    write_to_s3_curated(df_sub, "fact_subscriptions", ["event_year", "event_month", "event_day"])
    write_to_s3_curated(df_txn, "fact_transactions", ["event_year", "event_month", "event_day"])
    write_to_s3_curated(df_churn_agg, "agg_daily_churn", ["event_date"])
    write_to_s3_curated(df_revenue_agg, "agg_daily_revenue", ["event_date"])

    # Write aggregates to Redshift
    write_to_redshift(df_churn_agg, "analytics.agg_daily_churn")
    write_to_redshift(df_revenue_agg, "analytics.agg_daily_revenue")

    logger.info("Batch ETL job completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
