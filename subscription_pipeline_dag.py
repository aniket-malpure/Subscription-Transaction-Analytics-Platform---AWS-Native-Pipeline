"""
Apache Airflow DAG — Subscription & Transaction Analytics Pipeline
Schedule: Daily at 2AM UTC
Dependency chain: ingest → validate → transform → load → materialize → notify
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
import boto3
import logging

logger = logging.getLogger(__name__)

# ── Airflow Variables ─────────────────────────────────────────────────────────
RAW_BUCKET = Variable.get("pv_raw_bucket", "pv-raw-bucket")
CURATED_BUCKET = Variable.get("pv_curated_bucket", "pv-curated-bucket")
EMR_LOG_BUCKET = Variable.get("emr_log_bucket", "pv-emr-logs")
REDSHIFT_CONN_ID = "redshift_pv"
AWS_CONN_ID = "aws_default"
SLACK_CONN_ID = "slack_pv_alerts"
SLA_MINUTES = 30


# ── EMR Cluster Configuration ─────────────────────────────────────────────────
EMR_JOB_FLOW_CONFIG = {
    "Name": "pv-subscription-analytics-{{ ds_nodash }}",
    "LogUri": f"s3://{EMR_LOG_BUCKET}/emr-logs/",
    "ReleaseLabel": "emr-6.10.0",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.2xlarge",
                "InstanceCount": 4,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [{
                        "VolumeSpecification": {"VolumeType": "gp2", "SizeInGB": 100},
                        "VolumesPerInstance": 1
                    }]
                }
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Configurations": [
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.shuffle.partitions": "200",
            }
        }
    ],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "VisibleToAllUsers": True,
    "Tags": [
        {"Key": "Project", "Value": "pv-subscription-analytics"},
        {"Key": "Environment", "Value": "production"},
    ],
}

EMR_BATCH_STEPS = [
    {
        "Name": "Run Subscription Batch ETL",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--executor-memory", "8g",
                "--num-executors", "8",
                "--executor-cores", "4",
                f"s3://{CURATED_BUCKET}/scripts/batch_job.py",
                "1",  # lookback_days
            ],
        },
    }
]


# ── Default Args ───────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["de-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(minutes=SLA_MINUTES),
}


# ── Python Callables ──────────────────────────────────────────────────────────
def check_raw_data_availability(**context):
    """Verify yesterday's raw data partitions exist in S3 before starting."""
    execution_date = context["ds"]  # YYYY-MM-DD
    year, month, day = execution_date.split("-")

    s3_client = boto3.client("s3")
    prefixes_to_check = [
        f"subscriptions/year={year}/month={month}/day={day}/",
        f"transactions/year={year}/month={month}/day={day}/",
    ]

    missing = []
    for prefix in prefixes_to_check:
        response = s3_client.list_objects_v2(Bucket=RAW_BUCKET, Prefix=prefix, MaxKeys=1)
        if response.get("KeyCount", 0) == 0:
            missing.append(prefix)

    if missing:
        raise FileNotFoundError(f"Missing raw data partitions: {missing}")

    logger.info(f"All raw partitions confirmed for {execution_date}")
    return "data_available"


def refresh_materialized_views(**context):
    """Trigger Redshift materialized view refresh after load."""
    import redshift_connector
    conn = redshift_connector.connect(
        host=Variable.get("redshift_host"),
        database="pvdb",
        user=Variable.get("redshift_user"),
        password=Variable.get("redshift_password"),
        port=5439,
    )
    cursor = conn.cursor()
    views = [
        "analytics.mv_7day_rolling_churn",
        "analytics.mv_monthly_revenue_by_plan",
    ]
    for view in views:
        logger.info(f"Refreshing {view}...")
        cursor.execute(f"REFRESH MATERIALIZED VIEW {view};")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("All materialized views refreshed.")


def send_success_notification(**context):
    execution_date = context["ds"]
    logger.info(f"Pipeline completed successfully for {execution_date}")


def send_failure_notification(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    execution_date = context["ds"]
    exception = context.get("exception", "Unknown error")
    logger.error(f"Task {task_id} in {dag_id} failed for {execution_date}: {exception}")


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="pv_subscription_analytics_pipeline",
    default_args=default_args,
    description="Daily subscription & transaction ETL pipeline",
    schedule_interval="0 2 * * *",        # 2AM UTC daily
    start_date=datetime(2025, 1, 1),
    catchup=True,                          # Backfill enabled
    max_active_runs=3,
    tags=["data-engineering", "subscriptions", "prime-video"],
    on_failure_callback=send_failure_notification,
) as dag:

    # ── 1. Start marker
    start = EmptyOperator(task_id="start")

    # ── 2. Check raw data exists before doing anything
    check_data = PythonOperator(
        task_id="check_raw_data_availability",
        python_callable=check_raw_data_availability,
        provide_context=True,
    )

    # ── 3. Create EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMR_JOB_FLOW_CONFIG,
        aws_conn_id=AWS_CONN_ID,
    )

    # ── 4. Submit Spark batch step
    submit_batch_step = EmrAddStepsOperator(
        task_id="submit_batch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        steps=EMR_BATCH_STEPS,
    )

    # ── 5. Wait for Spark step to complete
    wait_for_batch = EmrStepSensor(
        task_id="wait_for_batch_completion",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('submit_batch_step', key='return_value')[0] }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=7200,
    )

    # ── 6. Terminate EMR cluster
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",          # Always terminate, even on failure
    )

    # ── 7. Load staging → curated in Redshift
    load_dim_customers = RedshiftSQLOperator(
        task_id="load_dim_customers",
        sql="CALL curated.upsert_dim_customers();",
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    load_fact_subscriptions = RedshiftSQLOperator(
        task_id="load_fact_subscriptions",
        sql="""
            INSERT INTO curated.fact_subscriptions
            SELECT
                s.*,
                c.customer_sk
            FROM staging.stg_subscription_events s
            LEFT JOIN curated.dim_customers c
                ON s.customer_id = c.customer_id AND c.is_current = TRUE;
        """,
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    load_fact_transactions = RedshiftSQLOperator(
        task_id="load_fact_transactions",
        sql="""
            INSERT INTO curated.fact_transactions
            SELECT
                t.*,
                c.customer_sk
            FROM staging.stg_transaction_events t
            LEFT JOIN curated.dim_customers c
                ON t.customer_id = c.customer_id AND c.is_current = TRUE;
        """,
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    # ── 8. Refresh materialized views
    refresh_views = PythonOperator(
        task_id="refresh_materialized_views",
        python_callable=refresh_materialized_views,
        provide_context=True,
    )

    # ── 9. Success notification
    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_success_notification,
        provide_context=True,
    )

    # ── 10. End marker
    end = EmptyOperator(task_id="end")

    # ── DAG Dependency Chain ──────────────────────────────────────────────────
    (
        start
        >> check_data
        >> create_emr_cluster
        >> submit_batch_step
        >> wait_for_batch
        >> terminate_cluster
        >> load_dim_customers
        >> [load_fact_subscriptions, load_fact_transactions]
        >> refresh_views
        >> notify_success
        >> end
    )
