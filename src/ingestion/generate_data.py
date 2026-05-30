"""
generate_data.py
----------------
Generates realistic subscription billing events for 200K+ records.
Simulates a SaaS billing platform similar to Chargebee's domain.

Run:
    python src/ingestion/generate_data.py --records 200000
"""

import argparse
import csv
import random
import os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("en_IN")
random.seed(42)

PLANS = {
    "starter":    {"price": 299,  "tier": 1},
    "growth":     {"price": 999,  "tier": 2},
    "business":   {"price": 2499, "tier": 3},
    "enterprise": {"price": 7999, "tier": 4},
}

EVENT_TYPES = ["subscription_created", "subscription_upgraded",
               "subscription_downgraded", "subscription_cancelled",
               "invoice_paid", "invoice_failed", "trial_started", "trial_converted"]

INDUSTRIES = ["SaaS", "E-commerce", "EdTech", "FinTech", "HealthTech",
              "Logistics", "HR Tech", "Marketing Tech"]

def random_date(start, end):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def generate_tenants(n=500):
    tenants = []
    for i in range(n):
        tenant_id = f"TNT_{i+1:05d}"
        plan = random.choices(
            list(PLANS.keys()),
            weights=[40, 30, 20, 10]
        )[0]
        tenants.append({
            "tenant_id": tenant_id,
            "company_name": fake.company(),
            "industry": random.choice(INDUSTRIES),
            "plan": plan,
            "country": random.choice(["India", "USA", "UK", "Singapore", "Australia"]),
            "signup_date": random_date(
                datetime(2023, 1, 1),
                datetime(2024, 6, 1)
            ).strftime("%Y-%m-%d")
        })
    return tenants

def generate_events(tenants, total_records=200000):
    events = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 4, 30)

    records_per_tenant = total_records // len(tenants)

    for tenant in tenants:
        plan = tenant["plan"]
        price = PLANS[plan]["price"]
        event_date = datetime.strptime(tenant["signup_date"], "%Y-%m-%d")

        # First event: subscription created
        events.append({
            "event_id": f"EVT_{len(events)+1:08d}",
            "tenant_id": tenant["tenant_id"],
            "event_type": "subscription_created",
            "plan": plan,
            "amount": price,
            "currency": "INR",
            "event_date": event_date.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "success",
            "invoice_id": f"INV_{len(events)+1:08d}",
            "seats": random.randint(1, 20),
            "trial": random.choice([True, False])
        })

        # Subsequent events
        for _ in range(records_per_tenant - 1):
            event_date += timedelta(days=random.randint(1, 45))
            if event_date > end_date:
                break

            # Weighted event selection based on plan lifecycle
            rand = random.random()
            if rand < 0.45:
                event_type = "invoice_paid"
            elif rand < 0.55:
                event_type = "invoice_failed"
            elif rand < 0.65:
                event_type = "subscription_upgraded"
                plan_keys = list(PLANS.keys())
                curr_idx = plan_keys.index(plan)
                if curr_idx < len(plan_keys) - 1:
                    plan = plan_keys[curr_idx + 1]
                    price = PLANS[plan]["price"]
            elif rand < 0.72:
                event_type = "subscription_downgraded"
                plan_keys = list(PLANS.keys())
                curr_idx = plan_keys.index(plan)
                if curr_idx > 0:
                    plan = plan_keys[curr_idx - 1]
                    price = PLANS[plan]["price"]
            elif rand < 0.80:
                event_type = "subscription_cancelled"
                price = 0
            else:
                event_type = random.choice(["trial_started", "trial_converted"])

            events.append({
                "event_id": f"EVT_{len(events)+1:08d}",
                "tenant_id": tenant["tenant_id"],
                "event_type": event_type,
                "plan": plan,
                "amount": price if event_type == "invoice_paid" else 0,
                "currency": "INR",
                "event_date": event_date.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "success" if event_type != "invoice_failed" else "failed",
                "invoice_id": f"INV_{len(events)+1:08d}",
                "seats": random.randint(1, 20),
                "trial": False
            })

    random.shuffle(events)
    return events

def save_csv(data, path, filename):
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, filename)
    if not data:
        return
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"  ✓ Saved {len(data):,} records → {filepath}")

def main(records=200000):
    print(f"\n🔧 Generating {records:,} subscription events across 500 tenants...\n")
    tenants = generate_tenants(500)
    events = generate_events(tenants, records)

    save_csv(tenants, "data/raw", "tenants.csv")
    save_csv(events,  "data/raw", "billing_events.csv")
    print(f"\n✅ Done. {len(events):,} events, {len(tenants)} tenants generated.\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=200000)
    args = parser.parse_args()
    main(args.records)
