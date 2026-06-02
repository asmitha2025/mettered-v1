-- =============================================================================
-- Subscription Intelligence Pipeline - PostgreSQL Schema
-- =============================================================================
-- Multi-tenant SaaS billing schema with:
--   - Row-level security for tenant isolation
--   - Indexes on hot query columns
--   - Materialized view for fast revenue dashboards
--
-- Run:
--   psql -h localhost -U billing_user -d billing_db -f sql/schema.sql
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Clean slate for local development.
DROP MATERIALIZED VIEW IF EXISTS mv_monthly_revenue;
DROP TABLE IF EXISTS billing_events CASCADE;
DROP TABLE IF EXISTS invoices CASCADE;
DROP TABLE IF EXISTS subscriptions CASCADE;
DROP TABLE IF EXISTS tenants CASCADE;

-- =============================================================================
-- 1. TENANTS - dimension table
-- =============================================================================
CREATE TABLE tenants (
    tenant_id       VARCHAR(20)  PRIMARY KEY,
    company_name    VARCHAR(255) NOT NULL,
    industry        VARCHAR(100),
    plan            VARCHAR(50)  NOT NULL CHECK (plan IN ('starter','growth','business','enterprise')),
    country         VARCHAR(100),
    signup_date     DATE         NOT NULL,
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_tenants_plan    ON tenants(plan);
CREATE INDEX idx_tenants_country ON tenants(country);
CREATE INDEX idx_tenants_signup  ON tenants(signup_date);

-- =============================================================================
-- 2. SUBSCRIPTIONS - one row per active subscription period
-- =============================================================================
CREATE TABLE subscriptions (
    subscription_id UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       VARCHAR(20)  NOT NULL REFERENCES tenants(tenant_id),
    plan            VARCHAR(50)  NOT NULL CHECK (plan IN ('starter','growth','business','enterprise')),
    status          VARCHAR(30)  NOT NULL CHECK (status IN ('active','cancelled','paused','trial')),
    monthly_amount  INTEGER      NOT NULL,
    seats           INTEGER      DEFAULT 1,
    start_date      DATE         NOT NULL,
    end_date        DATE,
    trial_end_date  DATE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_subs_tenant     ON subscriptions(tenant_id);
CREATE INDEX idx_subs_status     ON subscriptions(status);
CREATE INDEX idx_subs_plan       ON subscriptions(plan);
CREATE INDEX idx_subs_start_date ON subscriptions(start_date);

-- =============================================================================
-- 3. INVOICES - one row per billing cycle
-- =============================================================================
CREATE TABLE invoices (
    invoice_id      VARCHAR(20)  PRIMARY KEY,
    tenant_id       VARCHAR(20)  NOT NULL REFERENCES tenants(tenant_id),
    subscription_id UUID         REFERENCES subscriptions(subscription_id),
    amount          INTEGER      NOT NULL,
    currency        CHAR(3)      DEFAULT 'INR',
    status          VARCHAR(20)  NOT NULL CHECK (status IN ('paid','failed','pending','refunded')),
    billing_date    DATE         NOT NULL,
    due_date        DATE,
    paid_date       DATE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_inv_tenant       ON invoices(tenant_id);
CREATE INDEX idx_inv_billing_date ON invoices(billing_date);
CREATE INDEX idx_inv_status       ON invoices(status);
CREATE INDEX idx_inv_mrr_query    ON invoices(tenant_id, billing_date, status);

-- =============================================================================
-- 4. BILLING_EVENTS - append-only event log
-- =============================================================================
CREATE TABLE billing_events (
    event_id        VARCHAR(20)  PRIMARY KEY,
    tenant_id       VARCHAR(20)  NOT NULL REFERENCES tenants(tenant_id),
    event_type      VARCHAR(50)  NOT NULL,
    plan            VARCHAR(50),
    amount          INTEGER      DEFAULT 0,
    currency        CHAR(3)      DEFAULT 'INR',
    event_date      TIMESTAMPTZ  NOT NULL,
    status          VARCHAR(20)  DEFAULT 'success',
    invoice_id      VARCHAR(20)  REFERENCES invoices(invoice_id),
    seats           INTEGER,
    trial           BOOLEAN      DEFAULT FALSE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- For larger event volumes, this table can be recreated with:
-- PARTITION BY RANGE (event_date)

CREATE INDEX idx_events_tenant      ON billing_events(tenant_id);
CREATE INDEX idx_events_type        ON billing_events(event_type);
CREATE INDEX idx_events_date        ON billing_events(event_date);
CREATE INDEX idx_events_tenant_date ON billing_events(tenant_id, event_date);

-- =============================================================================
-- 5. MATERIALIZED VIEW - monthly revenue summary
-- =============================================================================
CREATE MATERIALIZED VIEW mv_monthly_revenue AS
SELECT
    t.tenant_id,
    t.company_name,
    t.industry,
    t.plan,
    t.country,
    DATE_TRUNC('month', i.billing_date) AS billing_month,
    COUNT(i.invoice_id)                 AS invoice_count,
    SUM(i.amount)                       AS total_mrr,
    SUM(i.amount) * 12                  AS arr,
    AVG(i.amount)                       AS avg_invoice_amount
FROM invoices i
JOIN tenants t ON i.tenant_id = t.tenant_id
WHERE i.status = 'paid'
GROUP BY
    t.tenant_id,
    t.company_name,
    t.industry,
    t.plan,
    t.country,
    DATE_TRUNC('month', i.billing_date);

CREATE UNIQUE INDEX idx_mv_revenue_unique
    ON mv_monthly_revenue(tenant_id, billing_month);

-- Refresh with:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_revenue;

-- =============================================================================
-- 6. ROW-LEVEL SECURITY - tenant isolation
-- =============================================================================
-- Application sessions should set:
--   SET app.tenant_id = 'TNT_00001';

ALTER TABLE tenants        ENABLE ROW LEVEL SECURITY;
ALTER TABLE billing_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE invoices       ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriptions  ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_tenants ON tenants
    USING (tenant_id = current_setting('app.tenant_id', TRUE));

CREATE POLICY tenant_isolation_events ON billing_events
    USING (tenant_id = current_setting('app.tenant_id', TRUE));

CREATE POLICY tenant_isolation_invoices ON invoices
    USING (tenant_id = current_setting('app.tenant_id', TRUE));

CREATE POLICY tenant_isolation_subs ON subscriptions
    USING (tenant_id = current_setting('app.tenant_id', TRUE));

-- =============================================================================
-- 7. SAMPLE ANALYTICAL QUERIES
-- =============================================================================

-- MRR trend for a tenant:
-- SELECT billing_month, total_mrr, arr
-- FROM mv_monthly_revenue
-- WHERE tenant_id = 'TNT_00001'
-- ORDER BY billing_month;

-- Top 10 tenants by lifetime revenue:
-- SELECT tenant_id, company_name, SUM(total_mrr) AS lifetime_value
-- FROM mv_monthly_revenue
-- GROUP BY tenant_id, company_name
-- ORDER BY lifetime_value DESC
-- LIMIT 10;

-- Churn candidates with no invoice in the last 60 days:
-- SELECT t.tenant_id, t.company_name, MAX(i.billing_date) AS last_payment
-- FROM tenants t
-- LEFT JOIN invoices i ON t.tenant_id = i.tenant_id AND i.status = 'paid'
-- GROUP BY t.tenant_id, t.company_name
-- HAVING MAX(i.billing_date) < NOW() - INTERVAL '60 days'
--    OR MAX(i.billing_date) IS NULL;
