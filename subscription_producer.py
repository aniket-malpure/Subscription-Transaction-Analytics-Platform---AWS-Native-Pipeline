"""
Subscription & Transaction Event Producer
Simulates microservice pushing events to Kinesis Data Streams.
Streams: subscription-events, transaction-events
"""

import boto3
import json
import uuid
import random
import time
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REGION = "us-east-1"
SUBSCRIPTION_STREAM = "subscription-events"
TRANSACTION_STREAM = "transaction-events"

PLANS = ["BASIC", "STANDARD", "PREMIUM", "FAMILY"]
SUBSCRIPTION_STATES = ["ACTIVATED", "RENEWED", "CANCELLED", "REACTIVATED", "SUSPENDED"]
TRANSACTION_TYPES = ["PURCHASE", "REFUND", "RENEWAL", "UPGRADE", "DOWNGRADE"]
PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "APPLE_PAY", "GOOGLE_PAY"]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]


@dataclass
class SubscriptionEvent:
    event_id: str
    customer_id: str
    event_type: str
    plan: str
    region: str
    event_time: str
    previous_state: str
    new_state: str
    subscription_start_date: str
    subscription_end_date: str
    price_usd: float
    source_system: str = "subscription-service"
    schema_version: str = "1.0"


@dataclass
class TransactionEvent:
    event_id: str
    customer_id: str
    transaction_id: str
    transaction_type: str
    amount_usd: float
    currency: str
    payment_method: str
    region: str
    event_time: str
    status: str
    plan: str
    source_system: str = "billing-service"
    schema_version: str = "1.0"


class KinesisProducer:
    def __init__(self, region: str = REGION):
        self.client = boto3.client("kinesis", region_name=region)
        self.region = region

    def put_record(self, stream_name: str, data: dict, partition_key: str) -> dict:
        try:
            response = self.client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data).encode("utf-8"),
                PartitionKey=partition_key,
            )
            logger.info(
                f"Record sent to {stream_name} | ShardId: {response['ShardId']} | "
                f"SequenceNumber: {response['SequenceNumber']}"
            )
            return response
        except Exception as e:
            logger.error(f"Failed to send record to {stream_name}: {e}")
            raise

    def put_records_batch(self, stream_name: str, records: List[dict], partition_keys: List[str]) -> dict:
        """Batch send up to 500 records per Kinesis API call."""
        kinesis_records = [
            {"Data": json.dumps(record).encode("utf-8"), "PartitionKey": pk}
            for record, pk in zip(records, partition_keys)
        ]
        # Kinesis allows max 500 records per PutRecords call
        chunks = [kinesis_records[i:i + 500] for i in range(0, len(kinesis_records), 500)]
        responses = []
        for chunk in chunks:
            response = self.client.put_records(StreamName=stream_name, Records=chunk)
            failed = response.get("FailedRecordCount", 0)
            if failed > 0:
                logger.warning(f"{failed} records failed in batch. Retrying failed records...")
                # Retry failed records
                failed_records = [
                    chunk[i] for i, r in enumerate(response["Records"]) if "ErrorCode" in r
                ]
                if failed_records:
                    self.client.put_records(StreamName=stream_name, Records=failed_records)
            responses.append(response)
        return responses


def generate_subscription_event(customer_id: str = None) -> SubscriptionEvent:
    cid = customer_id or f"CUST-{uuid.uuid4().hex[:8].upper()}"
    plan = random.choice(PLANS)
    price_map = {"BASIC": 8.99, "STANDARD": 13.99, "PREMIUM": 17.99, "FAMILY": 22.99}
    now = datetime.now(timezone.utc)
    return SubscriptionEvent(
        event_id=str(uuid.uuid4()),
        customer_id=cid,
        event_type=random.choice(SUBSCRIPTION_STATES),
        plan=plan,
        region=random.choice(REGIONS),
        event_time=now.isoformat(),
        previous_state=random.choice(["ACTIVE", "CANCELLED", "TRIAL"]),
        new_state=random.choice(["ACTIVE", "CANCELLED", "SUSPENDED"]),
        subscription_start_date=now.strftime("%Y-%m-%d"),
        subscription_end_date=now.replace(month=(now.month % 12) + 1).strftime("%Y-%m-%d"),
        price_usd=price_map[plan],
    )


def generate_transaction_event(customer_id: str = None) -> TransactionEvent:
    cid = customer_id or f"CUST-{uuid.uuid4().hex[:8].upper()}"
    tx_type = random.choice(TRANSACTION_TYPES)
    amount = round(random.uniform(1.99, 99.99), 2)
    if tx_type == "REFUND":
        amount = -amount
    return TransactionEvent(
        event_id=str(uuid.uuid4()),
        customer_id=cid,
        transaction_id=f"TXN-{uuid.uuid4().hex[:12].upper()}",
        transaction_type=tx_type,
        amount_usd=amount,
        currency="USD",
        payment_method=random.choice(PAYMENT_METHODS),
        region=random.choice(REGIONS),
        event_time=datetime.now(timezone.utc).isoformat(),
        status=random.choice(["SUCCESS", "FAILED", "PENDING", "REFUNDED"]),
        plan=random.choice(PLANS),
    )


def run_producer(events_per_second: int = 100, duration_seconds: int = 60):
    """
    Continuously produce subscription and transaction events.
    Target: ~500 events/sec across both streams.
    """
    producer = KinesisProducer()
    total_sent = 0
    start_time = time.time()

    logger.info(f"Starting producer: {events_per_second} events/sec for {duration_seconds}s")

    while time.time() - start_time < duration_seconds:
        batch_sub_events = []
        batch_sub_keys = []
        batch_txn_events = []
        batch_txn_keys = []

        for _ in range(events_per_second):
            sub_event = generate_subscription_event()
            txn_event = generate_transaction_event(sub_event.customer_id)
            batch_sub_events.append(asdict(sub_event))
            batch_sub_keys.append(sub_event.customer_id)
            batch_txn_events.append(asdict(txn_event))
            batch_txn_keys.append(txn_event.customer_id)

        producer.put_records_batch(SUBSCRIPTION_STREAM, batch_sub_events, batch_sub_keys)
        producer.put_records_batch(TRANSACTION_STREAM, batch_txn_events, batch_txn_keys)

        total_sent += events_per_second * 2
        logger.info(f"Total events sent: {total_sent}")
        time.sleep(1)

    logger.info(f"Producer finished. Total events: {total_sent}")


if __name__ == "__main__":
    run_producer(events_per_second=100, duration_seconds=300)
