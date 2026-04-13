# ═══════════════════════════════════════════════════════════════════════════════
# Terraform — Subscription Analytics Platform (AWS)
# Resources: Kinesis, Firehose, S3, Glue, Lambda, SNS, IAM
# ═══════════════════════════════════════════════════════════════════════════════

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "pv-terraform-state"
    key    = "subscription-analytics/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.region
}

variable "region"      { default = "us-east-1" }
variable "environment" { default = "production" }
variable "project"     { default = "pv-subscription-analytics" }


# ── S3 Buckets ────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket = "${var.project}-raw-${var.environment}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "curated" {
  bucket = "${var.project}-curated-${var.environment}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "quarantine" {
  bucket = "${var.project}-quarantine-${var.environment}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "temp" {
  bucket = "${var.project}-temp-${var.environment}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "checkpoints" {
  bucket = "${var.project}-checkpoints-${var.environment}"
  tags   = local.common_tags
}

# Lifecycle policy: raw data to Glacier after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "archive-raw-data"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration {
      days = 365
    }
  }
}

# Block public access on all buckets
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# ── Kinesis Data Streams ──────────────────────────────────────────────────────
resource "aws_kinesis_stream" "subscription_events" {
  name             = "subscription-events"
  shard_count      = 8    # 8 shards = 8MB/s ingestion capacity
  retention_period = 24   # 24-hour retention for replay

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = local.common_tags
}

resource "aws_kinesis_stream" "transaction_events" {
  name             = "transaction-events"
  shard_count      = 8
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = local.common_tags
}


# ── Kinesis Firehose ──────────────────────────────────────────────────────────
resource "aws_kinesis_firehose_delivery_stream" "subscription_firehose" {
  name        = "subscription-events-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.subscription_events.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = aws_s3_bucket.raw.arn
    prefix             = "subscriptions/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/subscriptions/"
    buffering_interval = 60      # 60 seconds
    buffering_size     = 128     # 128 MB

    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {
            compression = "SNAPPY"
          }
        }
      }
      schema_configuration {
        database_name = aws_glue_catalog_database.pv_catalog.name
        table_name    = aws_glue_catalog_table.subscription_events.name
        role_arn      = aws_iam_role.firehose_role.arn
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = "/aws/kinesisfirehose/subscription-events"
      log_stream_name = "S3Delivery"
    }
  }

  tags = local.common_tags
}

resource "aws_kinesis_firehose_delivery_stream" "transaction_firehose" {
  name        = "transaction-events-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.transaction_events.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose_role.arn
    bucket_arn          = aws_s3_bucket.raw.arn
    prefix              = "transactions/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/transactions/"
    buffering_interval  = 60
    buffering_size      = 128

    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de { compression = "SNAPPY" }
        }
      }
      schema_configuration {
        database_name = aws_glue_catalog_database.pv_catalog.name
        table_name    = aws_glue_catalog_table.transaction_events.name
        role_arn      = aws_iam_role.firehose_role.arn
      }
    }
  }

  tags = local.common_tags
}


# ── AWS Glue Catalog ──────────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "pv_catalog" {
  name = "pv_data_catalog"
}

resource "aws_glue_catalog_table" "subscription_events" {
  name          = "subscription_events"
  database_name = aws_glue_catalog_database.pv_catalog.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"       = "parquet"
    "compressionType"      = "snappy"
    "typeOfData"           = "file"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.bucket}/subscriptions/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"      ; type = "string"
    }
    columns {
      name = "customer_id"   ; type = "string"
    }
    columns {
      name = "event_type"    ; type = "string"
    }
    columns {
      name = "plan"          ; type = "string"
    }
    columns {
      name = "region"        ; type = "string"
    }
    columns {
      name = "event_time"    ; type = "string"
    }
    columns {
      name = "previous_state"; type = "string"
    }
    columns {
      name = "new_state"     ; type = "string"
    }
    columns {
      name = "price_usd"     ; type = "double"
    }
    columns {
      name = "source_system" ; type = "string"
    }
  }

  partition_keys {
    name = "year"  ; type = "string"
  }
  partition_keys {
    name = "month" ; type = "string"
  }
  partition_keys {
    name = "day"   ; type = "string"
  }
  partition_keys {
    name = "hour"  ; type = "string"
  }
}

resource "aws_glue_catalog_table" "transaction_events" {
  name          = "transaction_events"
  database_name = aws_glue_catalog_database.pv_catalog.name
  table_type    = "EXTERNAL_TABLE"

  parameters = { "classification" = "parquet" }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.bucket}/transactions/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns { name = "event_id";         type = "string" }
    columns { name = "customer_id";      type = "string" }
    columns { name = "transaction_id";   type = "string" }
    columns { name = "transaction_type"; type = "string" }
    columns { name = "amount_usd";       type = "double" }
    columns { name = "currency";         type = "string" }
    columns { name = "payment_method";   type = "string" }
    columns { name = "region";           type = "string" }
    columns { name = "event_time";       type = "string" }
    columns { name = "status";           type = "string" }
    columns { name = "plan";             type = "string" }
  }

  partition_keys {
    name = "year"  ; type = "string"
  }
  partition_keys {
    name = "month" ; type = "string"
  }
  partition_keys {
    name = "day"   ; type = "string"
  }
  partition_keys {
    name = "hour"  ; type = "string"
  }
}


# ── Glue Crawler ──────────────────────────────────────────────────────────────
resource "aws_glue_crawler" "raw_crawler" {
  database_name = aws_glue_catalog_database.pv_catalog.name
  name          = "pv-raw-data-crawler"
  role          = aws_iam_role.glue_role.arn
  schedule      = "cron(0 * * * ? *)"   # Hourly

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/subscriptions/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/transactions/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = local.common_tags
}


# ── SNS Topic for Alerts ──────────────────────────────────────────────────────
resource "aws_sns_topic" "data_quality_alerts" {
  name = "pv-data-quality-alerts"
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "pagerduty" {
  topic_arn = aws_sns_topic.data_quality_alerts.arn
  protocol  = "https"
  endpoint  = "https://events.pagerduty.com/integration/xxxxx/enqueue"
}


# ── Lambda Function ───────────────────────────────────────────────────────────
resource "aws_lambda_function" "quality_checker" {
  filename         = "lambda_quality_check.zip"
  function_name    = "pv-data-quality-checker"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_quality_check.lambda_handler"
  runtime          = "python3.11"
  timeout          = 300
  memory_size      = 1024

  environment {
    variables = {
      QUARANTINE_BUCKET   = aws_s3_bucket.quarantine.bucket
      SNS_ALERT_TOPIC_ARN = aws_sns_topic.data_quality_alerts.arn
      STEP_FUNCTION_ARN   = aws_sfn_state_machine.etl_pipeline.arn
      METRICS_TABLE       = aws_dynamodb_table.quality_metrics.name
    }
  }

  layers = [aws_lambda_layer_version.great_expectations.arn]
  tags   = local.common_tags
}

# S3 trigger for Lambda
resource "aws_s3_bucket_notification" "raw_s3_trigger" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.quality_checker.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "subscriptions/"
    filter_suffix       = ".parquet"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.quality_checker.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "transactions/"
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.s3_invoke]
}

resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.quality_checker.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}


# ── DynamoDB — Quality Metrics ────────────────────────────────────────────────
resource "aws_dynamodb_table" "quality_metrics" {
  name         = "pv_data_quality_metrics"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "dataset"
  range_key    = "run_date"

  attribute {
    name = "dataset"
    type = "S"
  }
  attribute {
    name = "run_date"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = local.common_tags
}


# ── IAM Roles ─────────────────────────────────────────────────────────────────
resource "aws_iam_role" "firehose_role" {
  name = "pv-firehose-delivery-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "firehose_policy" {
  name = "pv-firehose-policy"
  role = aws_iam_role.firehose_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:ListShards"]
        Resource = [aws_kinesis_stream.subscription_events.arn, aws_kinesis_stream.transaction_events.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Resource = ["${aws_s3_bucket.raw.arn}", "${aws_s3_bucket.raw.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetTableVersion", "glue:GetTableVersions"]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role" "lambda_role" {
  name = "pv-lambda-quality-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role" "glue_role" {
  name = "pv-glue-crawler-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}


# ── Step Functions (ETL orchestration trigger from Lambda) ────────────────────
resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "pv-etl-pipeline"
  role_arn = aws_iam_role.sfn_role.arn

  definition = jsonencode({
    Comment = "Trigger EMR batch job after quality validation"
    StartAt = "RunEMRBatchJob"
    States = {
      RunEMRBatchJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::elasticmapreduce:addStep.sync"
        Parameters = {
          ClusterId  = "PLACEHOLDER"
          Step = {
            Name = "Batch ETL"
            ActionOnFailure = "TERMINATE_CLUSTER"
            HadoopJarStep = {
              Jar  = "command-runner.jar"
              Args = ["spark-submit", "--deploy-mode", "cluster", "s3://pv-curated-bucket/scripts/batch_job.py"]
            }
          }
        }
        End = true
      }
    }
  })

  tags = local.common_tags
}

resource "aws_iam_role" "sfn_role" {
  name = "pv-step-functions-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}


# ── Locals ────────────────────────────────────────────────────────────────────
locals {
  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
    Team        = "data-engineering"
  }
}


# ── Outputs ───────────────────────────────────────────────────────────────────
output "raw_bucket_name"              { value = aws_s3_bucket.raw.bucket }
output "curated_bucket_name"          { value = aws_s3_bucket.curated.bucket }
output "subscription_stream_arn"      { value = aws_kinesis_stream.subscription_events.arn }
output "transaction_stream_arn"       { value = aws_kinesis_stream.transaction_events.arn }
output "quality_checker_function_arn" { value = aws_lambda_function.quality_checker.arn }
output "sns_alert_topic_arn"          { value = aws_sns_topic.data_quality_alerts.arn }
