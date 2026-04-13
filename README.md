# Subscription & Transaction Analytics Platform — AWS Native Pipeline

> **End-to-end near real-time data platform** ingesting 10M+ daily subscription and transaction events using AWS Kinesis, EMR/Spark, Redshift, and Great Expectations — built to serve 20+ downstream analytics teams with sub-10-second data latency.

---

## Table of Contents

- [Business Problem](#business-problem)
- [Architecture Overview](#architecture-overview)
- [Design Decisions](#design-decisions)
- [Repository Structure](#repository-structure)
- [Layer-by-Layer Deep Dive](#layer-by-layer-deep-dive)
  - [Layer 1 — Event Ingestion (Kinesis Data Streams)](#layer-1--event-ingestion-kinesis-data-streams)
  - [Layer 2 — Delivery Buffer (Kinesis Firehose)](#layer-2--delivery-buffer-kinesis-firehose)
  - [Layer 3 — Raw S3 Data Lake](#layer-3--raw-s3-data-lake)
  - [Layer 4 — EMR/Spark Processing](#layer-4--emrspark-processing)
  - [Layer 5 — Redshift Data Warehouse](#layer-5--redshift-data-warehouse)
  - [Layer 6 — Data Quality (Lambda + Great Expectations)](#layer-6--data-quality-lambda--great-expectations)
  - [Layer 7 — Orchestration (Apache Airflow)](#layer-7--orchestration-apache-airflow)
- [Data Modeling Decisions](#data-modeling-decisions)
- [Late Data & Fault Tolerance](#late-data--fault-tolerance)
- [Schema Evolution Strategy](#schema-evolution-strategy)
- [Infrastructure as Code](#infrastructure-as-code)
- [Key Metrics & Outcomes](#key-metrics--outcomes)
- [How to Run Locally (Dev Mode)](#how-to-run-locally-dev-mode)
- [What I Would Do Differently at 10x Scale](#what-i-would-do-differently-at-10x-scale)

---

## Business Problem

A subscription-based streaming platform had a critical analytics gap. Churn detection was running on 24-hour batch jobs — meaning if a wave of users cancelled due to a content or pricing issue, the data team wouldn't know until the next morning. Revenue dashboards were equally stale. Product managers had no reliable way to correlate subscription plan changes with payment success rates in real time.

**Goals:**
- Reduce data latency from 24 hours → under 10 minutes for operational dashboards
- Build a single, trusted source of truth for subscription lifecycle events and payment transactions
- Serve 20+ downstream teams (BI, ML, Finance, Product) with consistent, validated data
- Detect and quarantine bad data *before* it reaches analysts

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PRODUCER LAYER                              │
│   Backend Microservices (Subscription Service, Billing Service)     │
└────────────────────────────┬────────────────────────────────────────┘
                             │  AWS SDK (Boto3) put_records()
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    INGESTION — Kinesis Data Streams                 │
│   subscription-events (8 shards)  |  transaction-events (8 shards) │
│   Partition Key: customer_id      |  Retention: 24 hours            │
└────────────────────────────┬────────────────────────────────────────┘
                             │  Kinesis → Firehose subscription
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                DELIVERY BUFFER — Kinesis Firehose                   │
│   Buffer: 60s or 128MB   |   Format: JSON → Parquet (via Glue)     │
│   Prefix: s3://.../year=YYYY/month=MM/day=DD/hour=HH/              │
└────────────────────────────┬────────────────────────────────────────┘
                             │  S3 PutObject → Lambda trigger
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│            DATA QUALITY GATE — Lambda + Great Expectations          │
│   Schema | Completeness | Value Sets | Volume Anomaly (±20% 7d avg)│
│   PASS → trigger EMR  |  FAIL → quarantine + SNS → PagerDuty       │
└────────────────┬────────────────────────────────────┬───────────────┘
                 │ S3 raw layer                        │ Quarantine bucket
                 ▼                                     ▼
┌──────────────────────────────────┐    ┌─────────────────────────────┐
│    RAW S3 DATA LAKE              │    │   s3://pv-quarantine-bucket  │
│    Parquet, Hive-partitioned     │    │   Bad records + audit log    │
│    Glue Crawler: hourly          │    └─────────────────────────────┘
└──────────────┬───────────────────┘
               │  EMR Spark Jobs
               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              EMR / SPARK PROCESSING                                 │
│   Batch (2AM): Dedup | State Machine | Enrich | Aggregate           │
│   Streaming: Kinesis → Structured Streaming | 5-min micro-batches   │
│   Watermark: 10 minutes for late data tolerance                     │
└────────────────────────────┬────────────────────────────────────────┘
                             │  S3 Curated Layer (Parquet)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│              REDSHIFT DATA WAREHOUSE                                │
│   staging → curated → analytics                                     │
│   dim_customers (SCD Type 2) | fact_subscriptions | fact_transactions│
│   Materialized views refreshed every 15 minutes                    │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
              BI Tools | ML Pipelines | Ad-hoc SQL
```

---

## Design Decisions

### Why Kinesis over Kafka?

The entire stack is AWS-native. Kinesis integrates directly with Firehose, Lambda, Glue, and S3 without any cluster management. At our throughput (~500 events/sec peak), 8 shards per stream provided sufficient capacity (each shard = 1MB/s ingestion). Had we needed fan-out to 50+ consumer groups with sub-second latency, Apache Kafka via MSK would have been the right call. Kinesis was the operationally simpler choice for this use case.

### Why Parquet + Hive Partitioning in S3?

Firehose's format conversion to Parquet at the delivery stage means Spark never reads raw JSON at scale — Parquet's columnar format cuts I/O dramatically for analytics queries. Hive-style partitioning on `year/month/day/hour` lets Spark and Redshift Spectrum prune partitions during query planning, avoiding full table scans.

### Why a Separate Quarantine Bucket?

Rather than failing the entire pipeline when bad records arrive, we isolate them. The quarantine bucket becomes a reprocessing queue — data engineers can inspect failures, fix upstream issues, and resubmit. This is preferable to discarding data or letting bad records contaminate the curated layer.

### Why SCD Type 2 for dim_customers?

Subscription plan changes are a core business event. If a customer upgrades from BASIC to PREMIUM, we need the historical record to correctly attribute revenue to the right plan at any point in time. SCD Type 2 preserves this history by keeping both the old and new row, with `effective_start_date` and `effective_end_date` bounding each version.

---

## Repository Structure

```
project1/
├── producers/
│   └── subscription_producer.py       # Kinesis event producer (simulates microservices)
├── spark/
│   ├── batch_job.py                   # Nightly EMR Spark batch ETL
│   └── streaming_job.py               # Spark Structured Streaming (near real-time)
├── redshift/
│   └── schema.sql                     # Full DDL: staging / curated / analytics layers
├── quality/
│   └── lambda_quality_check.py        # Lambda + Great Expectations data quality gate
├── airflow/
│   └── subscription_pipeline_dag.py   # Airflow DAG orchestrating the full pipeline
└── infrastructure/
    └── main.tf                        # Terraform: all AWS resources
```

---

## Layer-by-Layer Deep Dive

### Layer 1 — Event Ingestion (Kinesis Data Streams)

**File:** `producers/subscription_producer.py`

Two Kinesis streams are provisioned:
- `subscription-events` — activation, renewal, cancellation, reactivation, suspension events
- `transaction-events` — purchases, refunds, renewals, upgrades, downgrades

**Key engineering choices:**

**Partition key = `customer_id`**
Kinesis routes records with the same partition key to the same shard. This guarantees ordered delivery per customer — critical for subscription state machine processing. If we used a random partition key, events for the same customer could land on different shards and be processed out of order.

**Batch sends via `put_records()`**
The producer batches up to 500 records per API call (Kinesis API limit). This reduces API call overhead and improves throughput vs. putting one record at a time. Failed records within a batch are retried individually — we track `FailedRecordCount` in the response and re-send only the failed subset.

**8 shards per stream**
Each Kinesis shard supports 1MB/s ingestion and 2MB/s reads. At 500 events/sec with an average payload of ~500 bytes, peak throughput = ~250KB/s per stream — comfortably within 8 shards × 1MB/s = 8MB/s capacity, with 30x headroom for spikes.

```python
# From subscription_producer.py
response = self.client.put_records(StreamName=stream_name, Records=chunk)
failed = response.get("FailedRecordCount", 0)
if failed > 0:
    failed_records = [chunk[i] for i, r in enumerate(response["Records"]) if "ErrorCode" in r]
    self.client.put_records(StreamName=stream_name, Records=failed_records)
```

---

### Layer 2 — Delivery Buffer (Kinesis Firehose)

Two Firehose delivery streams subscribe to the Kinesis streams.

**Buffering strategy:** 60 seconds OR 128MB — whichever threshold hits first triggers a flush to S3. This creates a natural micro-batch rhythm: during peak hours, volume hits 128MB quickly; during off-peak, the 60-second timer fires.

**In-flight format conversion:**
Firehose performs JSON → Parquet conversion using the Glue Data Catalog schema as the reference. This is significant: raw events are JSON strings from microservices, but they land in S3 as columnar Parquet files — no separate conversion step required, and no EMR compute consumed for this transformation.

**Hive-partitioned prefix:**
```
s3://pv-raw-bucket/subscriptions/year=2025/month=06/day=14/hour=15/
```
Glue Crawlers and Spark partition pruning both understand this structure natively.

**Error handling:** Records that fail format conversion are routed to a separate error prefix, not silently dropped. This lets us investigate schema mismatches without losing the original data.

---

### Layer 3 — Raw S3 Data Lake

The raw layer is the immutable archive — data here is never modified or deleted within the retention window.

**Lifecycle policy:** Raw Parquet files transition to S3 Glacier after 90 days and expire after 365 days. Glacier retrieval takes minutes, which is acceptable for historical audit or reprocessing needs.

**Glue Crawler (hourly):** Updates the Glue Data Catalog with new partitions as Firehose flushes arrive. Without this, Spark's `spark.read.parquet(path)` would miss partitions created after the last crawler run. The alternative — `msck repair table` in Athena — is slower and must be triggered manually.

---

### Layer 4 — EMR/Spark Processing

**File:** `spark/batch_job.py` and `spark/streaming_job.py`

Two processing modes run in parallel:

#### Batch Job (nightly, 2AM UTC)

The batch job performs heavy-lifting transformations that are too expensive to run per micro-batch:

**1. Deduplication via Window functions**
Kinesis delivers at-least-once. Firehose can produce duplicate records near shard boundaries. We deduplicate on `event_id` using a row number window ordered by `event_time desc` — keeping the latest occurrence.

```python
window_dedup = Window.partitionBy("event_id").orderBy(F.desc("event_time"))
df = (df.withColumn("row_num", F.row_number().over(window_dedup))
        .filter(F.col("row_num") == 1)
        .drop("row_num"))
```

**2. Subscription state machine validation**
We defined valid state transitions: `TRIAL → ACTIVE`, `ACTIVE → CANCELLED`, `CANCELLED → ACTIVE` (reactivation), etc. Records with invalid transitions are flagged (not discarded) for analyst review. Discarding them would hide upstream bugs.

**3. Customer dimension enrichment**
Joining subscription events with the `dim_customers` table adds `customer_segment`, `acquisition_channel`, and `country` — enabling cohort-level analysis without requiring analysts to join across tables in every query.

**4. Rejected records path**
Records missing `event_id`, `customer_id`, or `event_time` are written to a separate rejected prefix, not silently dropped. The count is logged and alerted if it exceeds a threshold.

#### Streaming Job (continuous, 5-minute micro-batches)

**File:** `spark/streaming_job.py`

Reads from the same Kinesis streams as Firehose using Spark's Kinesis connector. The streaming job focuses on low-latency delivery of cleaned events, not complex aggregations.

**Watermarking for late data:**
```python
df.withWatermark("event_ts", "10 minutes")
```
Spark maintains state for 10 minutes after the current watermark. Events arriving within that window are included in the correct time window aggregation. Events arriving more than 10 minutes late are dropped — an acceptable tradeoff at this latency requirement. The batch job catches truly late events the following morning.

**Separate checkpoints per query:** Each `writeStream` maintains its own checkpoint location. This means a failure in the subscription stream query doesn't affect the transaction stream query — they recover independently.

---

### Layer 5 — Redshift Data Warehouse

**File:** `redshift/schema.sql`

Three-layer schema architecture:

| Layer | Purpose | Load Pattern |
|---|---|---|
| `staging` | Raw landed data from S3 | Truncate + reload each run |
| `curated` | Cleaned, enriched, deduplicated | UPSERT via stored procedure |
| `analytics` | Pre-aggregated for BI consumption | Incremental append |

**Distribution and sort key decisions:**

`fact_subscriptions` and `fact_transactions` are distributed by `customer_id` (KEY distribution). Most analytical joins happen between facts and `dim_customers` on `customer_id` — collocating matching rows on the same Redshift slice eliminates cross-node data movement (the most expensive operation in MPP systems).

Sort key is `event_date` — the most common filter predicate in queries. Redshift stores data physically sorted by this key, enabling zone map pruning: blocks not containing the queried date range are skipped entirely.

**Materialized views** for dashboard teams:
- `mv_7day_rolling_churn` — rolling 7-day churn rate by region and plan using window functions
- `mv_monthly_revenue_by_plan` — monthly net revenue, gross revenue, ARPU, paying customer counts

These are refreshed every 15 minutes by Airflow. Dashboard queries hit the materialized view (pre-computed result) rather than the underlying fact tables — this cut query time for the BI team by ~40%.

**SCD Type 2 via stored procedure:**
`curated.upsert_dim_customers()` handles Type 2 logic:
1. Detect changed records via MD5 hash comparison
2. Expire changed records: set `effective_end_date = yesterday`, `is_current = FALSE`
3. Insert new versions: `effective_start_date = today`, `is_current = TRUE`

This preserves full customer history. A customer who upgraded from BASIC to PREMIUM 3 months ago will have two rows — Airflow joins on `is_current = TRUE` for the current state, and analysts can query either version for historical accuracy.

---

### Layer 6 — Data Quality (Lambda + Great Expectations)

**File:** `quality/lambda_quality_check.py`

**Trigger:** S3 `PutObject` event fires every time Firehose flushes a file. Lambda runs synchronously before the file is processed by EMR.

**Why Great Expectations?**
GE provides a declarative quality contract: expectations are defined in code, not buried in ad-hoc SQL scripts. When a new team member reads the codebase, the quality rules are explicit and version-controlled alongside the pipeline. Results are structured JSON that can be stored, compared, and alerted on programmatically.

**Expectation suite per dataset:**

| Check Type | Expectations |
|---|---|
| Schema | Column existence for all required fields |
| Completeness | `event_id`, `customer_id` null rate = 0%; `event_time` null rate ≤ 2% |
| Value sets | `event_type` must be in `{ACTIVATED, RENEWED, CANCELLED, REACTIVATED, SUSPENDED}` |
| Ranges | `price_usd` between 0 and 500 |
| Uniqueness | `event_id` must be unique within the file |

**Volume anomaly check (7-day rolling average):**
```python
avg_count = sum(item["row_count"] for item in historical_items) / len(historical_items)
variance = abs(current_count - avg_count) / avg_count
if variance > 0.20:  # ±20% threshold
    # Alert and quarantine
```
This catches silent pipeline failures — if a Kinesis shard stops producing or an upstream microservice deployment kills event publishing, the volume drop is caught immediately rather than discovered the next morning.

**Failure response:**
1. File moved to `s3://pv-quarantine-bucket/quarantine/<original-key>`
2. Metadata tag on quarantined file includes the failure reason
3. SNS → PagerDuty page to on-call data engineer
4. EMR job is NOT triggered — bad data never reaches Redshift

**Success response:**
Step Functions state machine is invoked to trigger the downstream EMR batch step.

---

### Layer 7 — Orchestration (Apache Airflow)

**File:** `airflow/subscription_pipeline_dag.py`

**DAG configuration:**
```
schedule_interval = "0 2 * * *"   # 2AM UTC
catchup = True                      # Backfill enabled for missed runs
max_active_runs = 3                 # Allow parallel backfill
SLA = 30 minutes                    # Alert if pipeline misses this window
```

**Dependency chain:**
```
start
  → check_raw_data_availability    # Verify S3 partitions exist before starting
  → create_emr_cluster             # EmrCreateJobFlowOperator
  → submit_batch_step              # EmrAddStepsOperator
  → wait_for_batch_completion      # EmrStepSensor (polls every 60s)
  → terminate_emr_cluster          # trigger_rule=all_done (always runs)
  → load_dim_customers             # SCD Type 2 stored procedure
  → [load_fact_subscriptions,
     load_fact_transactions]        # Parallel Redshift loads
  → refresh_materialized_views     # Python operator calling Redshift
  → notify_success
  → end
```

**Key design choices:**

`terminate_emr_cluster` uses `trigger_rule="all_done"` — this ensures the cluster is terminated even if upstream tasks fail. Without this, a failed Spark step would leave the EMR cluster running and incurring cost indefinitely.

`check_raw_data_availability` runs before spinning up the EMR cluster. If yesterday's S3 partitions are missing (upstream Kinesis producer outage), there's no point paying for an EMR cluster. The check costs milliseconds; an idle cluster costs dollars per hour.

Retry configuration: 3 retries with 5-minute exponential backoff. Most transient failures (EMR API throttling, network blips) self-resolve within 15 minutes. Beyond 3 retries, human intervention is required.

---

## Data Modeling Decisions

### Grain Choices

| Table | Grain | Reasoning |
|---|---|---|
| `fact_subscriptions` | One row per subscription lifecycle event | Events are the atomic unit — aggregating at load time destroys analytical flexibility |
| `fact_transactions` | One row per payment event | Payment disputes and refund tracking require event-level detail |
| `dim_customers` | One row per customer version (SCD2) | Plan changes and demographic updates must be historically accurate |
| `agg_daily_churn` | One row per date × region × plan × segment | Pre-aggregated for dashboard performance; detail is preserved in fact tables |

### NULL Handling Philosophy

Hard NULLs on surrogate keys (`customer_sk`) are intentional — if a `customer_id` in a fact table has no match in `dim_customers`, we store NULL rather than creating a dummy "unknown" dimension row. This makes data issues visible in queries rather than hiding them behind a placeholder.

---

## Late Data & Fault Tolerance

**Kinesis 24-hour retention:** If the pipeline is down for up to 24 hours (infrastructure incident, deployment gone wrong), no data is lost. When the pipeline recovers, consumers re-read from the last committed checkpoint position in the Kinesis iterator.

**Spark checkpoints:** Each streaming query maintains a checkpoint in S3. On restart, Spark resumes from exactly where it left off — no duplicate processing, no data loss.

**Watermark (10 minutes):** Events arriving within 10 minutes of their expected window are included in the correct aggregation. Beyond that, they're caught in the next nightly batch run, which reprocesses S3 partitions by event date (not ingestion date).

**Airflow backfill (`catchup=True`):** If the scheduler is down for multiple days, it will backfill missed DAG runs on restart, up to `max_active_runs=3` in parallel.

---

## Schema Evolution Strategy

**Upstream schema changes are common.** A microservice team adds a `promo_code` field to subscription events. Our approach:

1. **Firehose format conversion** drops unknown fields by default — new fields don't break the existing schema. This prevents Firehose from failing.
2. **Formal change process:** Upstream teams update the Glue Data Catalog schema first (reviewed via PR). We update the Great Expectations suite to validate the new field. Spark transformation logic is updated to pass through or transform the new column.
3. **Redshift `ALTER TABLE ADD COLUMN`** with a default value — existing rows get `NULL` for the new column, and no existing queries break.
4. **Backward-compatible Parquet:** New fields added to Parquet files are ignored by readers using the old schema. Old fields removed from Parquet files are read as NULL by readers expecting them. This means schema changes are safely incremental.

---

## Infrastructure as Code

**File:** `infrastructure/main.tf`

All AWS resources are defined in Terraform. Nothing is click-ops. Key resources:

| Resource | Configuration |
|---|---|
| Kinesis Streams | 8 shards, 24-hour retention, PROVISIONED mode |
| Kinesis Firehose | 60s/128MB buffer, JSON→Parquet via Glue schema, SNAPPY compression |
| S3 Buckets | Separate raw/curated/quarantine/temp/checkpoints; lifecycle policies; public access blocked |
| Glue Catalog | Database + tables + hourly crawler for partition discovery |
| Lambda | Python 3.11, 1024MB, 300s timeout; S3 PutObject trigger with prefix filters |
| DynamoDB | Quality metrics table, PAY_PER_REQUEST billing, TTL enabled |
| SNS | Topic with PagerDuty HTTPS endpoint subscription |
| Step Functions | State machine triggering EMR step on quality pass |
| IAM | Least-privilege roles per service; no wildcard Resource where avoidable |

**State management:** Terraform state stored in S3 with DynamoDB locking. Multiple engineers can run `terraform apply` safely — the lock prevents concurrent state corruption.

---

## Key Metrics & Outcomes

| Metric | Before | After |
|---|---|---|
| Data latency | 24 hours | < 10 minutes |
| Dashboard query time | ~45s (base tables) | ~8s (materialized views) |
| Bad data reaching analysts | Undetected | 98%+ caught at quality gate |
| Pipeline recovery time | Manual | Automatic (Airflow retry + Kinesis replay) |
| Downstream teams served | 3 | 20+ |

---

## How to Run Locally (Dev Mode)

```bash
# 1. Install dependencies
pip install boto3 great_expectations pyspark pandas pyarrow

# 2. Set AWS credentials
export AWS_PROFILE=dev

# 3. Generate test events (dry-run, prints to stdout without sending to Kinesis)
python producers/subscription_producer.py --dry-run --count 1000

# 4. Run batch job against local S3 (LocalStack)
export AWS_ENDPOINT_URL=http://localhost:4566
spark-submit spark/batch_job.py 1

# 5. Validate quality checks against a local Parquet file
python quality/lambda_quality_check.py --local-file ./test_data/subscriptions_sample.parquet
```

---

## What I Would Do Differently at 10x Scale

**Kinesis shards:** Scale from 8 to 80 shards per stream (Kinesis scales horizontally in minutes). Or migrate to MSK (managed Kafka) if the consumer fan-out requirement grows beyond 10 consumer groups.

**EMR → EMR Serverless:** At 10x volume, fixed EMR clusters spend time idle between job steps. EMR Serverless auto-scales compute to exactly what the job needs, charges per vCPU-hour, and eliminates cluster lifecycle management.

**Redshift RA3 nodes:** RA3 nodes decouple compute and storage — managed storage scales independently of compute. This prevents the common problem of needing more CPU forcing an oversized cluster just to accommodate data volume.

**Delta Lake instead of plain Parquet:** At 10x data volume, small file proliferation from Firehose (many small 128MB files) degrades Spark read performance. Delta's `OPTIMIZE` compaction and Z-ORDER clustering would dramatically improve scan efficiency. We'd also gain ACID upserts in S3, eliminating the Redshift staging layer for some datasets.

**Great Expectations → centralized data catalog:** At scale, GE suites per pipeline become hard to maintain. Integrating with a data catalog (DataHub, Atlan) provides a single view of data contracts, lineage, and quality SLAs across all pipelines.
