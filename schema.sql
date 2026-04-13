-- ═══════════════════════════════════════════════════════════════════════════
-- Redshift Schema: Subscription & Transaction Analytics Platform
-- Layers: staging → curated → analytics
-- ═══════════════════════════════════════════════════════════════════════════

-- ── Create Schemas ────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS analytics;


-- ══════════════════════════════════════════════════════════════════════════════
-- STAGING LAYER — Raw landed data, truncated and reloaded each run
-- ══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS staging.stg_subscription_events (
    event_id          VARCHAR(64)     NOT NULL,
    customer_id       VARCHAR(64)     NOT NULL,
    event_type        VARCHAR(32),
    plan              VARCHAR(32),
    region            VARCHAR(32),
    event_time        TIMESTAMP,
    previous_state    VARCHAR(32),
    new_state         VARCHAR(32),
    price_usd         DECIMAL(10,2),
    is_churn_event    BOOLEAN,
    source_system     VARCHAR(64),
    schema_version    VARCHAR(10),
    processed_at      TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (event_time);


CREATE TABLE IF NOT EXISTS staging.stg_transaction_events (
    event_id          VARCHAR(64)     NOT NULL,
    customer_id       VARCHAR(64)     NOT NULL,
    transaction_id    VARCHAR(64)     NOT NULL,
    transaction_type  VARCHAR(32),
    amount_usd        DECIMAL(12,2),
    currency          VARCHAR(10)     DEFAULT 'USD',
    payment_method    VARCHAR(32),
    region            VARCHAR(32),
    event_time        TIMESTAMP,
    status            VARCHAR(32),
    plan              VARCHAR(32),
    is_refund         BOOLEAN,
    source_system     VARCHAR(64),
    processed_at      TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (event_time);


-- ══════════════════════════════════════════════════════════════════════════════
-- CURATED LAYER — Cleaned, deduplicated, business-rule-applied
-- ══════════════════════════════════════════════════════════════════════════════

-- Dimension: Customers (SCD Type 2)
CREATE TABLE IF NOT EXISTS curated.dim_customers (
    customer_sk           BIGINT          IDENTITY(1,1) PRIMARY KEY,
    customer_id           VARCHAR(64)     NOT NULL,
    customer_name         VARCHAR(256),
    email                 VARCHAR(256),
    country               VARCHAR(64),
    region                VARCHAR(32),
    customer_segment      VARCHAR(32),    -- ENTERPRISE, SMB, CONSUMER
    acquisition_channel   VARCHAR(64),    -- ORGANIC, PAID, REFERRAL
    first_subscription_date DATE,
    -- SCD Type 2 columns
    effective_start_date  DATE            NOT NULL,
    effective_end_date    DATE,
    is_current            BOOLEAN         DEFAULT TRUE,
    record_hash           VARCHAR(64)     -- MD5 hash for change detection
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (effective_start_date);


-- Fact: Subscription Events (grain = one row per subscription lifecycle event)
CREATE TABLE IF NOT EXISTS curated.fact_subscriptions (
    subscription_event_sk BIGINT          IDENTITY(1,1),
    event_id              VARCHAR(64)     NOT NULL,
    customer_id           VARCHAR(64)     NOT NULL,
    customer_sk           BIGINT,         -- FK to dim_customers
    event_type            VARCHAR(32),
    plan                  VARCHAR(32),
    region                VARCHAR(32),
    event_time            TIMESTAMP       NOT NULL,
    event_date            DATE,
    previous_state        VARCHAR(32),
    new_state             VARCHAR(32),
    price_usd             DECIMAL(10,2),
    is_churn_event        BOOLEAN         DEFAULT FALSE,
    is_reactivation       BOOLEAN         DEFAULT FALSE,
    is_valid_transition   BOOLEAN         DEFAULT TRUE,
    customer_segment      VARCHAR(32),
    acquisition_channel   VARCHAR(64),
    country               VARCHAR(64),
    processed_at          TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (event_date, plan, region);


-- Fact: Transactions (grain = one row per payment event)
CREATE TABLE IF NOT EXISTS curated.fact_transactions (
    transaction_event_sk  BIGINT          IDENTITY(1,1),
    event_id              VARCHAR(64)     NOT NULL,
    transaction_id        VARCHAR(64)     NOT NULL,
    customer_id           VARCHAR(64)     NOT NULL,
    customer_sk           BIGINT,
    transaction_type      VARCHAR(32),
    amount_usd            DECIMAL(12,2),
    net_amount_usd        DECIMAL(12,2),
    currency              VARCHAR(10),
    payment_method        VARCHAR(32),
    region                VARCHAR(32),
    event_time            TIMESTAMP       NOT NULL,
    event_date            DATE,
    status                VARCHAR(32),
    plan                  VARCHAR(32),
    is_refund             BOOLEAN         DEFAULT FALSE,
    customer_segment      VARCHAR(32),
    country               VARCHAR(64),
    processed_at          TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (event_date, transaction_type, status);


-- ══════════════════════════════════════════════════════════════════════════════
-- ANALYTICS LAYER — Pre-aggregated for BI consumption
-- ══════════════════════════════════════════════════════════════════════════════

-- Daily churn metrics
CREATE TABLE IF NOT EXISTS analytics.agg_daily_churn (
    report_date           DATE            NOT NULL,
    region                VARCHAR(32),
    plan                  VARCHAR(32),
    customer_segment      VARCHAR(32),
    total_events          BIGINT,
    churn_events          BIGINT,
    unique_customers      BIGINT,
    avg_price_usd         DECIMAL(10,4),
    churn_rate            DECIMAL(10,6),
    refreshed_at          TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (report_date);


-- Daily revenue metrics
CREATE TABLE IF NOT EXISTS analytics.agg_daily_revenue (
    report_date           DATE            NOT NULL,
    region                VARCHAR(32),
    plan                  VARCHAR(32),
    payment_method        VARCHAR(32),
    gross_revenue_usd     DECIMAL(14,2),
    refund_amount_usd     DECIMAL(14,2),
    net_revenue_usd       DECIMAL(14,2),
    transaction_count     BIGINT,
    paying_customers      BIGINT,
    refreshed_at          TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (report_date);


-- ══════════════════════════════════════════════════════════════════════════════
-- MATERIALIZED VIEWS — Refreshed every 15 minutes by Airflow
-- ══════════════════════════════════════════════════════════════════════════════

CREATE MATERIALIZED VIEW analytics.mv_7day_rolling_churn
AS
SELECT
    report_date,
    region,
    plan,
    SUM(churn_events) OVER (
        PARTITION BY region, plan
        ORDER BY report_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS churn_7day_rolling,
    SUM(unique_customers) OVER (
        PARTITION BY region, plan
        ORDER BY report_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS customers_7day_rolling,
    ROUND(
        CAST(SUM(churn_events) OVER (
            PARTITION BY region, plan
            ORDER BY report_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS DECIMAL) /
        NULLIF(SUM(unique_customers) OVER (
            PARTITION BY region, plan
            ORDER BY report_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0), 6
    ) AS churn_rate_7day
FROM analytics.agg_daily_churn;


CREATE MATERIALIZED VIEW analytics.mv_monthly_revenue_by_plan
AS
SELECT
    DATE_TRUNC('month', report_date)  AS report_month,
    plan,
    region,
    SUM(gross_revenue_usd)            AS gross_revenue_usd,
    SUM(refund_amount_usd)            AS total_refunds_usd,
    SUM(net_revenue_usd)              AS net_revenue_usd,
    SUM(paying_customers)             AS total_paying_customers,
    ROUND(SUM(net_revenue_usd) / NULLIF(SUM(paying_customers), 0), 2) AS arpu
FROM analytics.agg_daily_revenue
GROUP BY 1, 2, 3;


-- ══════════════════════════════════════════════════════════════════════════════
-- STORED PROCEDURES — Used by Airflow for SCD Type 2 merge
-- ══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE curated.upsert_dim_customers()
AS $$
BEGIN
    -- Step 1: Expire changed records
    UPDATE curated.dim_customers tgt
    SET
        effective_end_date = GETDATE()::DATE - 1,
        is_current = FALSE
    FROM staging.stg_customers src
    WHERE tgt.customer_id = src.customer_id
      AND tgt.is_current = TRUE
      AND tgt.record_hash != src.record_hash;

    -- Step 2: Insert new/changed records
    INSERT INTO curated.dim_customers (
        customer_id, customer_name, email, country, region,
        customer_segment, acquisition_channel, first_subscription_date,
        effective_start_date, effective_end_date, is_current, record_hash
    )
    SELECT
        src.customer_id, src.customer_name, src.email, src.country, src.region,
        src.customer_segment, src.acquisition_channel, src.first_subscription_date,
        GETDATE()::DATE, NULL, TRUE, src.record_hash
    FROM staging.stg_customers src
    LEFT JOIN curated.dim_customers tgt
        ON src.customer_id = tgt.customer_id AND tgt.is_current = TRUE
    WHERE tgt.customer_id IS NULL
       OR tgt.record_hash != src.record_hash;
END;
$$ LANGUAGE plpgsql;


-- ══════════════════════════════════════════════════════════════════════════════
-- HELPER QUERIES — For analysts
-- ══════════════════════════════════════════════════════════════════════════════

-- Monthly churn rate by plan
-- SELECT report_month, plan, churn_rate_7day
-- FROM analytics.mv_7day_rolling_churn
-- WHERE report_date >= CURRENT_DATE - 30
-- ORDER BY report_month, plan;

-- Top revenue plans last 90 days
-- SELECT plan, SUM(net_revenue_usd) as revenue
-- FROM analytics.agg_daily_revenue
-- WHERE report_date >= CURRENT_DATE - 90
-- GROUP BY plan ORDER BY revenue DESC;
