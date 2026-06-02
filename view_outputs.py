import pandas as pd
import os
import sys

# Ensure UTF-8 output on Windows terminal
if sys.platform.startswith("win"):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

outputs = {
    "Global MRR Monthly Summary": "data/processed/global_mrr_monthly",
    "Top 5 Tenants by Estimated LTV": "data/processed/tenant_ltv",
    "Churn Rates by Plan (Sample)": "data/processed/churn_by_plan_month",
    "Cohort Retention Matrix (Sample)": "data/processed/cohort_retention"
}

def main() -> None:
    print("\n--- Subscription Intelligence Pipeline Output Preview ---")
    for name, path in outputs.items():
        print(f"\n======================================================================")
        print(f"  >>> {name}")
        print(f"======================================================================")
        if os.path.exists(path):
            df = pd.read_parquet(path)

            # Convert categorical partition columns to standard integers for presentation
            for col in ["event_year", "event_month", "months_since_start"]:
                if col in df.columns:
                    df[col] = df[col].astype(int)

            # Safe rounding for arpu or any floats if they exist
            if "arpu" in df.columns:
                df["arpu"] = df["arpu"].astype(float).round(2)

            if "estimated_ltv" in df.columns:
                df["estimated_ltv"] = df["estimated_ltv"].astype(float).round(2)

            if "LTV" in name and "estimated_ltv" in df.columns:
                # Show top 5 by LTV
                print(df.sort_values("estimated_ltv", ascending=False).head(5).to_string(index=False))
            elif "Cohort" in name and "cohort" in df.columns and "months_since_start" in df.columns:
                # Show first cohort months
                print(df.sort_values(["cohort", "months_since_start"]).head(12).to_string(index=False))
            elif "Churn" in name and "active_tenants" in df.columns:
                # Filter out empty periods for clean output.
                clean_churn = df[df["active_tenants"] > 0]
                print(clean_churn.head(8).to_string(index=False))
            else:
                print(df.head(8).to_string(index=False))
        else:
            print(f"  [ERROR] Output directory not found at: {path}")

if __name__ == "__main__":
    main()
