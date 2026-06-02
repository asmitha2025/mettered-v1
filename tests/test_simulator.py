from src.web.app import RevenueSimulationRequest, simulate_revenue


def test_revenue_simulator_projects_growth_and_churn():
    result = simulate_revenue(RevenueSimulationRequest(
        starting_tenants=100,
        monthly_arpu=1000,
        monthly_growth_pct=10,
        monthly_churn_pct=5,
        months=2,
    ))

    assert result["summary"]["starting_mrr"] == "INR 1.00L"
    assert result["summary"]["ending_tenants"] == 110.2
    assert result["summary"]["ending_mrr"] == "INR 1.10L"
    assert len(result["projection"]) == 2


def test_revenue_simulator_handles_high_churn():
    result = simulate_revenue(RevenueSimulationRequest(
        starting_tenants=10,
        monthly_arpu=500,
        monthly_growth_pct=0,
        monthly_churn_pct=100,
        months=1,
    ))

    assert result["summary"]["ending_tenants"] == 0
    assert result["summary"]["ending_mrr"] == "INR 0"
