"""
AWS Lambda — Data Quality Validator
Triggered by S3 PutObject events after Kinesis Firehose flushes.
Runs Great Expectations checkpoints. On failure: quarantine + SNS alert.
On success: trigger EMR job via Step Functions.
"""

import json
import boto3
import logging
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Tuple
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── AWS Clients ────────────────────────────────────────────────────────────────
s3 = boto3.client("s3")
sns = boto3.client("sns")
stepfunctions = boto3.client("stepfunctions")
dynamodb = boto3.resource("dynamodb")

# ── Config ────────────────────────────────────────────────────────────────────
QUARANTINE_BUCKET = "pv-quarantine-bucket"
SNS_ALERT_TOPIC_ARN = "arn:aws:sns:us-east-1:xxxx:pv-data-quality-alerts"
STEP_FUNCTION_ARN = "arn:aws:states:us-east-1:xxxx:stateMachine:pv-etl-pipeline"
METRICS_TABLE = "pv_data_quality_metrics"
ROLLING_WINDOW_DAYS = 7
VOLUME_VARIANCE_THRESHOLD = 0.20   # ±20% of 7-day rolling average
NULL_RATE_THRESHOLD = 0.02          # 2% max null rate on critical fields
CARDINALITY_VARIANCE_THRESHOLD = 0.15


# ── Great Expectations Context (in-memory, no filesystem) ─────────────────────
def build_ge_context() -> BaseDataContext:
    config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
        anonymous_usage_statistics={"enabled": False},
    )
    return gx.get_context(project_config=config)


# ── Expectation Suites ────────────────────────────────────────────────────────
def get_subscription_expectations(context: BaseDataContext, suite_name: str):
    suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    # Schema expectations
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column": "event_id"}
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column": "customer_id"}
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column": "event_time"}
    ))

    # Completeness expectations
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "event_id", "mostly": 1.0}
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "customer_id", "mostly": 1.0}
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "event_time", "mostly": 0.98}  # 98% non-null
    ))

    # Value set expectations
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "event_type",
            "value_set": ["ACTIVATED", "RENEWED", "CANCELLED", "REACTIVATED", "SUSPENDED"],
            "mostly": 0.99
        }
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "plan",
            "value_set": ["BASIC", "STANDARD", "PREMIUM", "FAMILY"],
            "mostly": 0.99
        }
    ))

    # Range expectations
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "price_usd", "min_value": 0.0, "max_value": 500.0, "mostly": 0.99}
    ))

    # Uniqueness
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "event_id"}
    ))

    context.save_expectation_suite(suite)
    return suite_name


def get_transaction_expectations(context: BaseDataContext, suite_name: str):
    suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    for col in ["event_id", "customer_id", "transaction_id", "event_time"]:
        suite.add_expectation(gx.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": col, "mostly": 1.0}
        ))

    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "transaction_id"}
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "status",
            "value_set": ["SUCCESS", "FAILED", "PENDING", "REFUNDED"],
            "mostly": 0.99
        }
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "amount_usd", "min_value": -1000.0, "max_value": 10000.0, "mostly": 0.99}
    ))

    context.save_expectation_suite(suite)
    return suite_name


# ── Volume Check Against 7-Day Rolling Average ────────────────────────────────
def check_volume_anomaly(dataset: str, current_count: int) -> Tuple[bool, str]:
    table = dynamodb.Table(METRICS_TABLE)

    # Fetch last 7 days of metrics
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("dataset").eq(dataset),
        ScanIndexForward=False,
        Limit=ROLLING_WINDOW_DAYS
    )
    items = response.get("Items", [])

    if len(items) < 3:
        logger.info(f"Insufficient history for volume check on {dataset}. Skipping.")
        return True, "INSUFFICIENT_HISTORY"

    avg_count = sum(int(item["row_count"]) for item in items) / len(items)
    variance = abs(current_count - avg_count) / avg_count if avg_count > 0 else 0

    if variance > VOLUME_VARIANCE_THRESHOLD:
        msg = (
            f"Volume anomaly on {dataset}: current={current_count}, "
            f"7d_avg={avg_count:.0f}, variance={variance:.2%}"
        )
        logger.warning(msg)
        return False, msg

    return True, f"Volume OK: {current_count} rows (avg: {avg_count:.0f})"


def save_metrics(dataset: str, row_count: int, ge_passed: bool, checks: dict):
    table = dynamodb.Table(METRICS_TABLE)
    table.put_item(Item={
        "dataset": dataset,
        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
        "ge_passed": ge_passed,
        "checks": json.dumps(checks),
    })


# ── S3 File Operations ────────────────────────────────────────────────────────
def read_s3_file_as_dataframe(bucket: str, key: str) -> pd.DataFrame:
    """Read Parquet or JSON file from S3 into pandas DataFrame."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    if key.endswith(".parquet"):
        return pd.read_parquet(obj["Body"])
    else:
        return pd.read_json(obj["Body"], lines=True)


def move_to_quarantine(source_bucket: str, source_key: str, reason: str):
    quarantine_key = f"quarantine/{source_key}"
    s3.copy_object(
        Bucket=QUARANTINE_BUCKET,
        CopySource={"Bucket": source_bucket, "Key": source_key},
        Key=quarantine_key,
        Metadata={"quarantine_reason": reason[:256]},
        MetadataDirective="REPLACE"
    )
    s3.delete_object(Bucket=source_bucket, Key=source_key)
    logger.info(f"File moved to quarantine: s3://{QUARANTINE_BUCKET}/{quarantine_key}")


def send_alert(subject: str, message: dict):
    sns.publish(
        TopicArn=SNS_ALERT_TOPIC_ARN,
        Subject=subject[:100],
        Message=json.dumps(message, indent=2),
        MessageAttributes={
            "severity": {"DataType": "String", "StringValue": "HIGH"},
            "team": {"DataType": "String", "StringValue": "data-engineering"},
        }
    )
    logger.info(f"SNS alert sent: {subject}")


def trigger_emr_job(dataset: str, s3_path: str):
    execution = stepfunctions.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        input=json.dumps({
            "dataset": dataset,
            "s3_path": s3_path,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "trigger_source": "lambda_quality_check"
        })
    )
    logger.info(f"Step Function started: {execution['executionArn']}")
    return execution["executionArn"]


# ── Core Quality Check Orchestrator ──────────────────────────────────────────
def run_quality_checks(bucket: str, key: str) -> Dict[str, Any]:
    dataset = "subscriptions" if "subscriptions" in key else "transactions"
    logger.info(f"Running quality checks for {dataset}: s3://{bucket}/{key}")

    # 1. Read file
    df = read_s3_file_as_dataframe(bucket, key)
    row_count = len(df)
    logger.info(f"File loaded: {row_count} rows")

    results = {"dataset": dataset, "bucket": bucket, "key": key, "row_count": row_count, "checks": {}}

    # 2. Great Expectations validation
    context = build_ge_context()
    suite_name = f"{dataset}_suite"

    if dataset == "subscriptions":
        get_subscription_expectations(context, suite_name)
    else:
        get_transaction_expectations(context, suite_name)

    datasource = context.sources.add_pandas("runtime_datasource")
    data_asset = datasource.add_dataframe_asset(name="runtime_asset")
    batch_request = data_asset.build_batch_request(dataframe=df)

    checkpoint = context.add_or_update_checkpoint(
        name="runtime_checkpoint",
        validations=[{
            "batch_request": batch_request,
            "expectation_suite_name": suite_name
        }]
    )

    checkpoint_result = checkpoint.run()
    ge_passed = checkpoint_result.success
    results["checks"]["great_expectations"] = {
        "passed": ge_passed,
        "statistics": checkpoint_result.to_json_dict().get("statistics", {})
    }
    logger.info(f"GE validation: {'PASSED' if ge_passed else 'FAILED'}")

    # 3. Volume anomaly check
    volume_ok, volume_msg = check_volume_anomaly(dataset, row_count)
    results["checks"]["volume"] = {"passed": volume_ok, "message": volume_msg}

    # 4. Save metrics regardless of outcome
    save_metrics(dataset, row_count, ge_passed, results["checks"])

    results["overall_passed"] = ge_passed and volume_ok
    return results


# ── Lambda Handler ────────────────────────────────────────────────────────────
def lambda_handler(event: dict, context) -> dict:
    logger.info(f"Event received: {json.dumps(event)}")

    responses = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        try:
            result = run_quality_checks(bucket, key)

            if result["overall_passed"]:
                logger.info(f"All checks passed for {key}. Triggering EMR job.")
                execution_arn = trigger_emr_job(result["dataset"], f"s3://{bucket}/{key}")
                result["emr_execution_arn"] = execution_arn
            else:
                failed_checks = {
                    k: v for k, v in result["checks"].items()
                    if not v.get("passed", True)
                }
                reason = json.dumps(failed_checks)
                logger.error(f"Quality checks FAILED: {reason}")

                # Quarantine the file
                move_to_quarantine(bucket, key, reason)

                # Alert
                send_alert(
                    subject=f"[DATA QUALITY FAILURE] {result['dataset']} | s3://{bucket}/{key}",
                    message={
                        "dataset": result["dataset"],
                        "file": f"s3://{bucket}/{key}",
                        "row_count": result["row_count"],
                        "failed_checks": failed_checks,
                        "action": "File quarantined. EMR job NOT triggered.",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )

            responses.append(result)

        except Exception as e:
            logger.error(f"Unexpected error processing {key}: {e}", exc_info=True)
            send_alert(
                subject=f"[LAMBDA ERROR] Quality check failed for {key}",
                message={"error": str(e), "key": key, "bucket": bucket}
            )
            raise

    return {
        "statusCode": 200,
        "body": json.dumps(responses, default=str)
    }
