import os
import sys
import json
import time
import subprocess
import threading
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np

app = FastAPI(title="Subscription Intelligence Dashboard API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Project paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
RAW_DIR = os.path.join(DATA_DIR, "raw")
DOCS_DIR = os.path.join(BASE_DIR, "docs")
LOG_FILE_PATH = os.path.join(BASE_DIR, "data", "pipeline_run.log")

# Pipeline status variables
pipeline_status = {
    "status": "IDLE",  # IDLE, RUNNING, SUCCESS, FAILED
    "progress": 0,     # 0 to 100
    "step": "Not started",
    "start_time": None,
    "end_time": None,
    "elapsed": 0,
    "records": 50000
}

pipeline_lock = threading.Lock()

def load_parquet(path: str) -> Optional[pd.DataFrame]:
    full_path = os.path.join(BASE_DIR, path)
    if not os.path.exists(full_path):
        return None
    try:
        return pd.read_parquet(full_path)
    except Exception as e:
        print(f"Error loading parquet at {full_path}: {e}")
        return None

def format_inr(val) -> str:
    """Formats values into Indian Rupees (Lakhs/Crores/Standard)"""
    if val is None or np.isnan(val):
        return "INR 0"

    val = float(val)
    if val >= 10_000_000:
        return f"INR {val / 10_000_000:.2f}Cr"
    elif val >= 100_000:
        return f"INR {val / 100_000:.2f}L"
    elif val >= 1_000:
        return f"INR {val / 1_000:.1f}k"
    else:
        return f"INR {val:.0f}"

def format_trend(pct) -> dict:
    """Generates standard trend indicator styling details"""
    if pct is None or np.isnan(pct):
        return {"text": "-", "class": "neutral", "up": False}
    pct = float(pct)
    if pct > 0:
        return {"text": f"+{pct:.1f}%", "class": "up", "up": True}
    elif pct < 0:
        return {"text": f"-{abs(pct):.1f}%", "class": "dn", "up": False}
    else:
        return {"text": "0.0%", "class": "neutral", "up": False}

# Background thread runner for pipeline orchestrator
def run_pipeline_worker(records: int):
    global pipeline_status

    with pipeline_lock:
        pipeline_status["status"] = "RUNNING"
        pipeline_status["progress"] = 5
        pipeline_status["step"] = "1/5: Generating Data"
        pipeline_status["start_time"] = time.time()
        pipeline_status["end_time"] = None
        pipeline_status["elapsed"] = 0
        pipeline_status["records"] = records

    python_exe = sys.executable

    # Open the log file
    os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
    with open(LOG_FILE_PATH, "w", encoding="utf-8") as log_file:
        log_file.write(f"=== Subscription Intelligence Pipeline Run Started (Records: {records}) ===\n")
        log_file.flush()

        steps = [
            ("Generating Data", [python_exe, "src/ingestion/generate_data.py", "--records", str(records)], 25),
            ("Ingest -> Parquet", [python_exe, "src/ingestion/etl_ingest.py"], 50),
            ("MRR / ARR / LTV", [python_exe, "src/transforms/mrr_transform.py"], 75),
            ("Churn + Cohorts", [python_exe, "src/transforms/churn_cohort.py"], 90),
            ("Benchmark", [python_exe, "src/analytics/benchmark.py"], 100),
        ]

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        success = True
        for i, (name, cmd, prog) in enumerate(steps):
            with pipeline_lock:
                pipeline_status["step"] = f"{i+1}/5: {name}"
                pipeline_status["progress"] = max(pipeline_status["progress"], int(prog - 15))

            log_file.write(f"\n>>> Running Step {i+1}: {name}...\n")
            log_file.write(f"Command: {' '.join(cmd)}\n")
            log_file.flush()

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=BASE_DIR,
                env=env,
                text=True
            )

            while True:
                line = proc.stdout.readline()
                if not line:
                    break
                log_file.write(line)
                log_file.flush()

                # Check elapsed
                with pipeline_lock:
                    pipeline_status["elapsed"] = round(time.time() - pipeline_status["start_time"], 1)

            proc.wait()
            if proc.returncode != 0:
                success = False
                log_file.write(f"\n[ERROR] Step '{name}' failed with exit code {proc.returncode}\n")
                log_file.flush()
                break

            with pipeline_lock:
                pipeline_status["progress"] = prog
                pipeline_status["elapsed"] = round(time.time() - pipeline_status["start_time"], 1)

        with pipeline_lock:
            pipeline_status["end_time"] = time.time()
            pipeline_status["elapsed"] = round(pipeline_status["end_time"] - pipeline_status["start_time"], 1)
            if success:
                pipeline_status["status"] = "SUCCESS"
                pipeline_status["progress"] = 100
                pipeline_status["step"] = "Completed successfully"
                log_file.write("\n=== Pipeline completed successfully! ===\n")
            else:
                pipeline_status["status"] = "FAILED"
                pipeline_status["step"] = "Pipeline failed"
                log_file.write("\n=== Pipeline execution failed! ===\n")
            log_file.flush()

@app.post("/api/pipeline/run")
def trigger_pipeline(background_tasks: BackgroundTasks, records: int = 50000):
    global pipeline_status
    with pipeline_lock:
        if pipeline_status["status"] == "RUNNING":
            raise HTTPException(status_code=400, detail="Pipeline is already running.")

    background_tasks.add_task(run_pipeline_worker, records)
    return {"message": "Pipeline run triggered", "status": "RUNNING"}

@app.get("/api/pipeline/status")
def get_pipeline_status():
    global pipeline_status
    with pipeline_lock:
        if pipeline_status["status"] == "RUNNING":
            pipeline_status["elapsed"] = round(time.time() - pipeline_status["start_time"], 1)
        return pipeline_status

@app.get("/api/pipeline/logs")
def get_pipeline_logs():
    if not os.path.exists(LOG_FILE_PATH):
        return {"logs": "No pipeline run logs available yet. Trigger the pipeline first."}
    try:
        with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
            return {"logs": f.read()}
    except Exception as e:
        return {"logs": f"Error reading logs: {str(e)}"}

@app.get("/api/overview")
def get_overview(period_filter: str = "1y"):
    """
    Returns KPIs, MRR monthly trend, Revenue by Plan, and Top Tenants
    """
    mrr_summary_df = load_parquet("data/processed/global_mrr_monthly")
    tenant_ltv_df = load_parquet("data/processed/tenant_ltv")
    mrr_tenant_df = load_parquet("data/processed/mrr_by_tenant_month")

    if mrr_summary_df is None or len(mrr_summary_df) == 0:
        return JSONResponse(status_code=404, content={"detail": "No processed data found. Please run the ETL pipeline."})

    mrr_summary_df = mrr_summary_df.sort_values(["event_year", "event_month"]).reset_index(drop=True)

    # Filter periods based on selection
    if period_filter == "6m":
        filtered_summary = mrr_summary_df.tail(6)
    elif period_filter == "1y":
        filtered_summary = mrr_summary_df.tail(12)
    else:
        filtered_summary = mrr_summary_df

    # Get latest active values
    latest_month = mrr_summary_df.iloc[-1]
    prev_month = mrr_summary_df.iloc[-2] if len(mrr_summary_df) > 1 else latest_month

    total_mrr = latest_month["total_mrr"]
    total_arr = latest_month["total_arr"]
    paying_tenants = latest_month["paying_tenants"]

    # MoM calculations
    mrr_pct = ((total_mrr - prev_month["total_mrr"]) / prev_month["total_mrr"] * 100) if prev_month["total_mrr"] > 0 else 0
    arr_pct = ((total_arr - prev_month["total_arr"]) / prev_month["total_arr"] * 100) if prev_month["total_arr"] > 0 else 0
    tenants_diff = paying_tenants - prev_month["paying_tenants"]

    # Dynamic Churn
    churn_df = load_parquet("data/processed/churn_by_plan_month")
    latest_churn_pct = 0.0
    churn_trend = {"text": "-", "class": "neutral"}

    if churn_df is not None and len(churn_df) > 0:
        latest_period = latest_month["period"]
        latest_churn_rows = churn_df[churn_df["period"] == latest_period]
        if len(latest_churn_rows) > 0:
            total_active = latest_churn_rows["active_tenants"].sum()
            total_churned = latest_churn_rows["churned_tenants"].sum()
            latest_churn_pct = (total_churned / total_active * 100) if total_active > 0 else 0.0

        # Get prior month churn
        prev_period = prev_month["period"]
        prev_churn_rows = churn_df[churn_df["period"] == prev_period]
        if len(prev_churn_rows) > 0:
            prev_active = prev_churn_rows["active_tenants"].sum()
            prev_churned = prev_churn_rows["churned_tenants"].sum()
            prev_churn_pct = (prev_churned / prev_active * 100) if prev_active > 0 else 0.0
            churn_diff = latest_churn_pct - prev_churn_pct
            if churn_diff > 0:
                churn_trend = {"text": f"+{churn_diff:.1f}% vs last mo", "class": "dn"}  # Churn going up is BAD (danger)
            elif churn_diff < 0:
                churn_trend = {"text": f"-{abs(churn_diff):.1f}% vs last mo", "class": "up"}
            else:
                churn_trend = {"text": "No change", "class": "neutral"}

    # Format KPI blocks
    kpis = [
        {
            "label": "Total MRR",
            "val": format_inr(total_mrr),
            "trend_text": format_trend(mrr_pct)["text"],
            "trend_class": format_trend(mrr_pct)["class"]
        },
        {
            "label": "ARR",
            "val": format_inr(total_arr),
            "trend_text": format_trend(arr_pct)["text"],
            "trend_class": format_trend(arr_pct)["class"]
        },
        {
            "label": "Churn Rate",
            "val": f"{latest_churn_pct:.1f}%",
            "trend_text": churn_trend["text"],
            "trend_class": churn_trend["class"]
        },
        {
            "label": "Active Tenants",
            "val": str(int(paying_tenants)),
            "trend_text": f"+{int(tenants_diff)} this month" if tenants_diff >= 0 else f"-{int(abs(tenants_diff))} this month",
            "trend_class": "up" if tenants_diff >= 0 else "dn"
        }
    ]

    # 2. Monthly Trend Chart Bars
    mrr_trend = []
    max_mrr = filtered_summary["total_mrr"].max() if len(filtered_summary) > 0 else 1
    for _, row in filtered_summary.iterrows():
        pct_height = int((row["total_mrr"] / max_mrr) * 90)  # Max height 92px
        # Palette: gradients from light blue to dark blue based on height
        color = "#B5D4F4"
        if pct_height > 75:
            color = "#0C447C"
        elif pct_height > 50:
            color = "#185FA5"
        elif pct_height > 30:
            color = "#378ADD"
        elif pct_height > 15:
            color = "#85B7EB"

        mrr_trend.append({
            "period": row["period"],
            "month_name": pd.to_datetime(row["period"] + "-01").strftime("%b"),
            "mrr": float(row["total_mrr"]),
            "mrr_formatted": format_inr(row["total_mrr"]),
            "pct_height": pct_height,
            "color": color
        })

    # 3. Revenue by Plan
    plan_revenue = []
    if mrr_tenant_df is not None and len(mrr_tenant_df) > 0:
        latest_period = latest_month["period"]
        latest_tenant_mrr = mrr_tenant_df[mrr_tenant_df["period"] == latest_period]

        # We need to join with tenant info to get their plans
        if tenant_ltv_df is not None and len(tenant_ltv_df) > 0:
            joined = pd.merge(latest_tenant_mrr, tenant_ltv_df[["tenant_id", "plan"]], on="tenant_id", how="inner")
            plan_grouped = joined.groupby("plan")["mrr"].sum().reset_index()

            # Map standard plans
            plan_colors = {
                "enterprise": "#3C3489",
                "business": "#185FA5",
                "growth": "#0F6E56",
                "starter": "#633806"
            }

            total_plan_rev = plan_grouped["mrr"].sum()
            for _, row in plan_grouped.iterrows():
                p = row["plan"]
                plan_revenue.append({
                    "plan": p.capitalize(),
                    "raw_mrr": float(row["mrr"]),
                    "mrr": format_inr(row["mrr"]),
                    "pct": int((row["mrr"] / total_plan_rev * 100)) if total_plan_rev > 0 else 0,
                    "color": plan_colors.get(p.lower(), "#185FA5")
                })
            # Sort plans by tier
            tier_order = {"Enterprise": 4, "Business": 3, "Growth": 2, "Starter": 1}
            plan_revenue.sort(key=lambda x: tier_order.get(x["plan"], 0), reverse=True)

    # Default plan revenue if missing
    if not plan_revenue:
        plan_revenue = [
            {"plan": "Enterprise", "mrr": "INR 0", "pct": 0, "color": "#3C3489"},
            {"plan": "Business", "mrr": "INR 0", "pct": 0, "color": "#185FA5"},
            {"plan": "Growth", "mrr": "INR 0", "pct": 0, "color": "#0F6E56"},
            {"plan": "Starter", "mrr": "INR 0", "pct": 0, "color": "#633806"}
        ]

    # 4. Top Tenants by LTV
    top_tenants = []
    if tenant_ltv_df is not None and len(tenant_ltv_df) > 0:
        sorted_ltv = tenant_ltv_df.sort_values("estimated_ltv", ascending=False).head(5)
        for _, row in sorted_ltv.iterrows():
            # Get latest month MRR
            tid = row["tenant_id"]
            latest_mrr = 0.0
            mom_growth = 0.0

            if mrr_tenant_df is not None:
                t_history = mrr_tenant_df[mrr_tenant_df["tenant_id"] == tid].sort_values(["event_year", "event_month"])
                if len(t_history) > 0:
                    latest_mrr = t_history.iloc[-1]["mrr"]
                    mom_growth = t_history.iloc[-1]["mrr_growth_pct"]

            top_tenants.append({
                "company_name": row["company_name"],
                "plan": row["plan"].capitalize(),
                "ltv": format_inr(row["estimated_ltv"]),
                "mrr": format_inr(latest_mrr),
                "mom_growth": format_trend(mom_growth)["text"],
                "mom_class": format_trend(mom_growth)["class"]
            })

    return {
        "kpis": kpis,
        "mrr_trend": mrr_trend,
        "plan_revenue": plan_revenue,
        "top_tenants": top_tenants
    }

@app.get("/api/tenants")
def get_tenants(search: str = "", plan: str = "all", page: int = 1, limit: int = 15):
    tenant_ltv_df = load_parquet("data/processed/tenant_ltv")
    mrr_tenant_df = load_parquet("data/processed/mrr_by_tenant_month")

    if tenant_ltv_df is None or len(tenant_ltv_df) == 0:
        return {"tenants": [], "total_count": 0, "active_count": 0, "avg_ltv": "INR 0", "arpu": "INR 0"}

    # Standard metrics
    total_count = len(tenant_ltv_df)
    avg_ltv = tenant_ltv_df["estimated_ltv"].mean()

    # Active/Churned determination
    # An active tenant had payment in latest period
    latest_period = "2025-04"
    if mrr_tenant_df is not None and len(mrr_tenant_df) > 0:
        latest_period = mrr_tenant_df["period"].max()
        active_ids = mrr_tenant_df[mrr_tenant_df["period"] == latest_period]["tenant_id"].unique()
        tenant_ltv_df["status"] = np.where(tenant_ltv_df["tenant_id"].isin(active_ids), "Active", "Churned")
    else:
        tenant_ltv_df["status"] = "Active"

    active_count = len(tenant_ltv_df[tenant_ltv_df["status"] == "Active"])
    avg_mrr = tenant_ltv_df["avg_monthly_mrr"].mean()

    # Apply search filter
    filtered_df = tenant_ltv_df.copy()
    if search:
        filtered_df = filtered_df[
            filtered_df["company_name"].str.contains(search, case=False, na=False) |
            filtered_df["tenant_id"].str.contains(search, case=False, na=False)
        ]

    # Apply plan filter
    if plan != "all":
        filtered_df = filtered_df[filtered_df["plan"].str.lower() == plan.lower()]

    # Pagination
    total_filtered = len(filtered_df)
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    paginated_df = filtered_df.sort_values("estimated_ltv", ascending=False).iloc[start_idx:end_idx]

    tenants_list = []
    for _, row in paginated_df.iterrows():
        # Look up latest MRR
        tid = row["tenant_id"]
        latest_mrr = 0.0
        if mrr_tenant_df is not None and len(mrr_tenant_df) > 0:
            latest_rows = mrr_tenant_df[(mrr_tenant_df["tenant_id"] == tid) & (mrr_tenant_df["period"] == latest_period)]
            if len(latest_rows) > 0:
                latest_mrr = latest_rows.iloc[0]["mrr"]

        tenants_list.append({
            "tenant_id": row["tenant_id"],
            "company_name": row["company_name"],
            "plan": row["plan"].capitalize(),
            "mrr": format_inr(latest_mrr if latest_mrr > 0 else row["avg_monthly_mrr"]),
            "active_months": int(row["active_months"]),
            "ltv": format_inr(row["estimated_ltv"]),
            "status": row["status"]
        })

    return {
        "tenants": tenants_list,
        "total_count": total_count,
        "active_count": active_count,
        "avg_ltv": format_inr(avg_ltv),
        "arpu": format_inr(avg_mrr),
        "total_filtered": total_filtered,
        "page": page,
        "limit": limit
    }

@app.get("/api/cohorts")
def get_cohorts():
    cohort_df = load_parquet("data/processed/cohort_retention")
    cohort_sizes_df = load_parquet("data/processed/cohort_sizes")

    if cohort_df is None or len(cohort_df) == 0:
        return {"cohorts": [], "months": []}

    cohort_df["months_since_start"] = cohort_df["months_since_start"].astype(int)

    # Pivot cohort matrix
    pivot = cohort_df.pivot(index="cohort", columns="months_since_start", values="retention_pct").reset_index()
    pivot = pivot.sort_values("cohort", ascending=True)

    # Merge with cohort size
    if cohort_sizes_df is not None:
        pivot = pd.merge(pivot, cohort_sizes_df, on="cohort", how="left")
    else:
        pivot["cohort_size"] = 25

    cohorts_list = []
    for _, row in pivot.iterrows():
        retention_months = []
        for m in range(12):
            val = row.get(m, None)
            retention_months.append(float(val) if pd.notna(val) else None)

        cohorts_list.append({
            "cohort": row["cohort"],
            "cohort_size": int(row["cohort_size"]) if pd.notna(row["cohort_size"]) else 25,
            "retention": retention_months
        })

    return {
        "cohorts": cohorts_list,
        "months": [f"M{m}" for m in range(12)]
    }

@app.get("/api/churn")
def get_churn():
    churn_df = load_parquet("data/processed/churn_by_plan_month")
    events_df = load_parquet("data/processed/billing_events")
    tenants_df = load_parquet("data/processed/tenant_ltv")

    if churn_df is None or len(churn_df) == 0:
        return {"kpis": [], "plan_churn": [], "churn_risk": []}

    # Get recent month churn rates
    latest_period = churn_df["period"].max()
    latest_churn = churn_df[churn_df["period"] == latest_period]

    total_active = latest_churn["active_tenants"].sum()
    total_churned = latest_churn["churned_tenants"].sum()
    overall_churn_rate = (total_churned / total_active * 100) if total_active > 0 else 0.0

    # Plan breakdown
    starter_rows = latest_churn[latest_churn["plan"] == "starter"]
    starter_churn = starter_rows.iloc[0]["churn_rate_pct"] if len(starter_rows) > 0 else 0.0

    ent_rows = latest_churn[latest_churn["plan"] == "enterprise"]
    ent_churn = ent_rows.iloc[0]["churn_rate_pct"] if len(ent_rows) > 0 else 0.0

    # Total unique churned tenants across all history
    total_historical_churned = 0
    if events_df is not None:
        total_historical_churned = events_df[events_df["event_type"] == "subscription_cancelled"]["tenant_id"].nunique()

    kpis = [
        {"label": "Overall Churn", "val": f"{overall_churn_rate:.1f}%", "sub": "Average this month"},
        {"label": "Starter Churn", "val": f"{starter_churn:.1f}%", "sub": "Highest plan churn"},
        {"label": "Enterprise Churn", "val": f"{ent_churn:.1f}%", "sub": "Lowest plan churn"},
        {"label": "Churned Tenants", "val": str(total_historical_churned), "sub": "All-time cancelled"}
    ]

    plan_churn = []
    plan_colors = {
        "starter": "#E24B4A",
        "growth": "#EF9F27",
        "business": "#85B7EB",
        "enterprise": "#1D9E75"
    }

    for plan_name in ["starter", "growth", "business", "enterprise"]:
        p_row = latest_churn[latest_churn["plan"] == plan_name]
        pct = float(p_row.iloc[0]["churn_rate_pct"]) if len(p_row) > 0 else 0.0
        plan_churn.append({
            "plan": plan_name.capitalize(),
            "rate": f"{pct:.1f}%",
            "bar_width": int(min(pct * 10, 100)),
            "color": plan_colors.get(plan_name, "#85B7EB")
        })

    # Dynamic Churn Risk Tenants
    churn_risk = []
    if events_df is not None and tenants_df is not None:
        # Find tenants with recent invoice failures
        failed_events = events_df[events_df["event_type"] == "invoice_failed"].sort_values("event_date", ascending=False)
        # Select unique tenant ids with failed invoices
        unique_failed = failed_events["tenant_id"].unique()[:4]

        reasons = [
            "60d no payment",
            "45d no payment",
            "Downgraded plan",
            "Invoice failed 2x"
        ]

        for i, tid in enumerate(unique_failed):
            # Look up tenant details
            t_row = tenants_df[tenants_df["tenant_id"] == tid]
            if len(t_row) > 0:
                reason = reasons[i % len(reasons)]
                # Customize reason if they actually have failed events
                t_failures = len(failed_events[failed_events["tenant_id"] == tid])
                if t_failures > 1:
                    reason = f"Invoice failed {t_failures}x"
                elif t_failures == 1:
                    reason = "Failed billing attempt"

                churn_risk.append({
                    "company_name": t_row.iloc[0]["company_name"],
                    "reason": reason,
                    "color": "var(--color-text-danger)" if "fail" in reason.lower() or "failed" in reason.lower() else "var(--color-text-warning)"
                })

    if not churn_risk:
        churn_risk = [
            {
                "company_name": "No high-risk accounts found",
                "reason": "No recent failed invoices in processed data",
                "color": "var(--color-text-tertiary)"
            }
        ]

    return {
        "kpis": kpis,
        "plan_churn": plan_churn,
        "churn_risk": churn_risk
    }

@app.get("/api/pipeline")
def get_pipeline_details():
    events_df = load_parquet("data/processed/billing_events")
    tenants_df = load_parquet("data/processed/tenant_ltv")
    cohort_df = load_parquet("data/processed/cohort_retention")
    rejected_df = load_parquet("data/processed/rejected")

    total_events = len(events_df) if events_df is not None else 0
    paying_tenants = len(tenants_df) if tenants_df is not None else 0
    total_cohorts = cohort_df["cohort"].nunique() if cohort_df is not None else 0
    rejected_rows = len(rejected_df) if rejected_df is not None else 0
    data_status = "Done" if total_events else "Waiting for data"
    data_status_class = "st-ok" if total_events else "st-idle"

    jobs = [
        {
            "name": "Job 1 - Ingest + Validate (ETL Core)",
            "meta": f"{total_events:,} clean events available | {rejected_rows:,} rejected rows | Parquet partitioned by year/month",
            "bar_width": 100 if total_events else 0,
            "bar_color": "#1D9E75",
            "status": data_status_class,
            "status_text": data_status
        },
        {
            "name": "Job 2 - MRR / ARR / LTV Computations",
            "meta": f"Computed dynamic window metrics for {paying_tenants} tenants | Derived ARR and lifetime estimations",
            "bar_width": 100 if paying_tenants else 0,
            "bar_color": "#185FA5",
            "status": "st-ok" if paying_tenants else "st-idle",
            "status_text": "Done" if paying_tenants else "Waiting for data"
        },
        {
            "name": "Job 3 - Churn + Cohort Retention Grid",
            "meta": f"Tracked survival metrics across {total_cohorts} cohort month signups over a rolling 12 month timeframe",
            "bar_width": 100 if total_cohorts else 0,
            "bar_color": "#534AB7",
            "status": "st-ok" if total_cohorts else "st-idle",
            "status_text": "Done" if total_cohorts else "Waiting for data"
        },
        {
            "name": "PostgreSQL Schema + Materialized View Design",
            "meta": "Schema includes tenant-scoped tables, indexes, materialized revenue view, and RLS policies",
            "bar_width": 100,
            "bar_color": "#BA7517",
            "status": "st-ok",
            "status_text": "Designed"
        },
        {
            "name": "Kafka Producer - Real-time event streaming topic",
            "meta": "Producer supports live Kafka mode and dry-run event simulation; not running unless started",
            "bar_width": 40,
            "bar_color": "#D85A30",
            "status": "st-idle",
            "status_text": "Optional"
        }
    ]
    return jobs

@app.get("/api/benchmark")
def get_benchmark():
    bench_results_path = os.path.join(DOCS_DIR, "benchmark_results.json")
    if not os.path.exists(bench_results_path):
        return {
            "records": [
                {"records": "50,000", "pandas": "Not run", "spark": "Not run", "winner": "Run benchmark", "winner_class": "w-pandas", "p_width": 20, "s_width": 20},
                {"records": "100,000", "pandas": "Not run", "spark": "Not run", "winner": "Run benchmark", "winner_class": "w-pandas", "p_width": 20, "s_width": 20},
                {"records": "200,000", "pandas": "Not run", "spark": "Not run", "winner": "Run benchmark", "winner_class": "w-pandas", "p_width": 20, "s_width": 20}
            ]
        }

    try:
        with open(bench_results_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        formatted_records = []
        for row in data:
            recs = row["records"]
            p_val = row["pandas_seconds"]
            s_val = row["spark_seconds"]

            p_str = f"{p_val:.2f}s" if isinstance(p_val, (int, float)) else str(p_val)
            s_str = f"{s_val:.2f}s" if isinstance(s_val, (int, float)) else str(s_val)

            # Compute bar sizes based on standard values
            p_width = int(min(float(p_val) * 30, 120)) if isinstance(p_val, (int, float)) else 20
            s_width = int(min(float(s_val) * 30, 120)) if isinstance(s_val, (int, float)) else 80

            winner = row["winner"]
            w_class = "w-pandas" if "pandas" in winner.lower() else "w-spark"

            formatted_records.append({
                "records": f"{recs:,}",
                "pandas": p_str,
                "spark": s_str,
                "winner": winner,
                "winner_class": w_class,
                "p_width": p_width,
                "s_width": s_width
            })
        return {"records": formatted_records}
    except Exception as e:
        print(f"Error loading benchmark: {e}")
        return {
            "records": [
                {"records": "50,000", "pandas": "Error", "spark": "Error", "winner": "Check benchmark file", "p_width": 20, "s_width": 20, "winner_class": "w-pandas"},
                {"records": "100,000", "pandas": "Error", "spark": "Error", "winner": "Check benchmark file", "p_width": 20, "s_width": 20, "winner_class": "w-pandas"},
                {"records": "200,000", "pandas": "Error", "spark": "Error", "winner": "Check benchmark file", "p_width": 20, "s_width": 20, "winner_class": "w-pandas"}
            ]
        }

@app.get("/", response_class=HTMLResponse)
def serve_index():
    index_path = os.path.join(BASE_DIR, "src", "web", "templates", "index.html")
    if not os.path.exists(index_path):
        return HTMLResponse("<h2>Error: src/web/templates/index.html not found!</h2>", status_code=404)

    with open(index_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
