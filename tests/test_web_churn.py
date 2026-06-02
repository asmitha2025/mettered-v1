import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.web import app as web_app


def test_churn_api_uses_latest_period_with_churn_activity(monkeypatch):
    churn_df = pd.DataFrame([
        {"period": "2025-03", "plan": "starter", "active_tenants": 10, "churned_tenants": 2, "churn_rate_pct": 20.0},
        {"period": "2025-03", "plan": "growth", "active_tenants": 20, "churned_tenants": 0, "churn_rate_pct": 0.0},
        {"period": "2025-04", "plan": "starter", "active_tenants": 10, "churned_tenants": 0, "churn_rate_pct": 0.0},
        {"period": "2025-04", "plan": "growth", "active_tenants": 20, "churned_tenants": 0, "churn_rate_pct": 0.0},
    ])
    events_df = pd.DataFrame([
        {"tenant_id": "TNT_001", "event_type": "subscription_cancelled", "event_date": "2025-03-12"},
        {"tenant_id": "TNT_002", "event_type": "invoice_failed", "event_date": "2025-04-01"},
    ])
    tenants_df = pd.DataFrame([
        {"tenant_id": "TNT_002", "company_name": "Acme Analytics"},
    ])

    def fake_load_parquet(path):
        if path.endswith("churn_by_plan_month"):
            return churn_df
        if path.endswith("billing_events"):
            return events_df
        if path.endswith("tenant_ltv"):
            return tenants_df
        return None

    monkeypatch.setattr(web_app, "load_parquet", fake_load_parquet)

    result = web_app.get_churn()

    assert result["selected_period"] == "2025-03"
    assert result["kpis"][0]["val"] == "6.7%"
    assert result["kpis"][3]["val"] == "2"
