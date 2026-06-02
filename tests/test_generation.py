import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingestion import generate_data


def test_generate_events_honors_requested_record_count():
    generate_data.reset_seed(42)
    tenants = generate_data.generate_tenants(25)

    events = generate_data.generate_events(tenants, total_records=1000)

    assert len(events) == 1000


def test_synthetic_churn_mix_stays_demo_realistic():
    generate_data.reset_seed(42)
    tenants = generate_data.generate_tenants(50)

    events = generate_data.generate_events(tenants, total_records=5000)
    cancellations = [
        event for event in events
        if event["event_type"] == "subscription_cancelled"
    ]
    churn_event_ratio = len(cancellations) / len(events)

    assert 0.005 <= churn_event_ratio <= 0.05
