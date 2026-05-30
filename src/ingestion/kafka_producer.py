"""
kafka_producer.py  —  Billing Event Stream Producer
-----------------------------------------------------
Simulates real-time billing events being pushed to a Kafka topic.
This layer adds streaming capability on top of the batch pipeline —
mirroring how Chargebee processes webhook events in real time.

Architecture:
    SaaS App → Kafka Topic (billing-events) → Spark Structured Streaming → Parquet

Run (requires Kafka running):
    python src/ingestion/kafka_producer.py

Without Kafka:
    python src/ingestion/kafka_producer.py --dry-run
"""

import argparse
import json
import random
import time
from datetime import datetime

PLANS = ["starter", "growth", "business", "enterprise"]
PRICES = {"starter": 299, "growth": 999, "business": 2499, "enterprise": 7999}
EVENT_TYPES = [
    "subscription_created", "subscription_upgraded",
    "subscription_downgraded", "subscription_cancelled",
    "invoice_paid", "invoice_failed"
]


def generate_event():
    plan = random.choice(PLANS)
    event_type = random.choices(
        EVENT_TYPES,
        weights=[5, 10, 8, 3, 60, 14]
    )[0]
    return {
        "event_id":   f"EVT_{random.randint(100000000, 999999999)}",
        "tenant_id":  f"TNT_{random.randint(1, 500):05d}",
        "event_type": event_type,
        "plan":       plan,
        "amount":     PRICES[plan] if event_type == "invoice_paid" else 0,
        "currency":   "INR",
        "event_date": datetime.now().isoformat(),
        "status":     "success" if event_type != "invoice_failed" else "failed",
    }


def produce_dry_run(events_per_second=5, duration_seconds=10):
    """Print events to stdout — no Kafka required."""
    print(f"\n🔁  Dry-run mode: producing {events_per_second} events/sec for {duration_seconds}s\n")
    total = 0
    for _ in range(duration_seconds):
        for _ in range(events_per_second):
            event = generate_event()
            print(f"  → {event['event_type']:<30} tenant={event['tenant_id']}  amount=₹{event['amount']}")
            total += 1
        time.sleep(1)
    print(f"\n  ✅ Produced {total} events (dry-run)\n")


def produce_to_kafka(topic: str = "billing-events", events_per_second: int = 10):
    """Push events to Kafka. Requires kafka-python: pip install kafka-python"""
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("  ⚠  kafka-python not installed. Run: pip install kafka-python")
        print("  💡 Use --dry-run to simulate without Kafka.")
        return

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",                 # wait for all replicas to confirm
        retries=3,
        max_block_ms=5000
    )

    print(f"\n📡  Streaming to Kafka topic: '{topic}' at {events_per_second} events/sec")
    print("    Press Ctrl+C to stop.\n")

    produced = 0
    try:
        while True:
            for _ in range(events_per_second):
                event = generate_event()
                producer.send(topic, value=event)
                produced += 1
            producer.flush()
            print(f"  → {produced} events sent", end="\r")
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n\n  ✅ Stopped. Total events produced: {produced}\n")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print events without Kafka")
    parser.add_argument("--topic", default="billing-events")
    parser.add_argument("--rate", type=int, default=10, help="Events per second")
    parser.add_argument("--duration", type=int, default=15, help="Dry-run duration in seconds")
    args = parser.parse_args()

    if args.dry_run:
        produce_dry_run(args.rate, args.duration)
    else:
        produce_to_kafka(args.topic, args.rate)
