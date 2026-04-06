-- Xero data sync tables

CREATE TABLE IF NOT EXISTS xero_bank_transactions (
    id SERIAL PRIMARY KEY,
    xero_id VARCHAR UNIQUE NOT NULL,
    tenant_id VARCHAR NOT NULL,
    type VARCHAR,
    contact_id VARCHAR,
    contact_name VARCHAR,
    bank_account_id VARCHAR,
    bank_account_name VARCHAR,
    date DATE,
    reference VARCHAR,
    status VARCHAR,
    sub_total DECIMAL(12,2),
    total_tax DECIMAL(12,2),
    total DECIMAL(12,2),
    is_reconciled BOOLEAN DEFAULT FALSE,
    line_items JSONB,
    updated_date_utc TIMESTAMP,
    synced_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xero_invoices (
    id SERIAL PRIMARY KEY,
    xero_id VARCHAR UNIQUE NOT NULL,
    tenant_id VARCHAR NOT NULL,
    type VARCHAR,
    contact_id VARCHAR,
    contact_name VARCHAR,
    invoice_number VARCHAR,
    reference VARCHAR,
    date DATE,
    due_date DATE,
    status VARCHAR,
    sub_total DECIMAL(12,2),
    total_tax DECIMAL(12,2),
    total DECIMAL(12,2),
    amount_due DECIMAL(12,2),
    amount_paid DECIMAL(12,2),
    amount_credited DECIMAL(12,2),
    line_items JSONB,
    updated_date_utc TIMESTAMP,
    synced_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xero_contacts (
    id SERIAL PRIMARY KEY,
    xero_id VARCHAR UNIQUE NOT NULL,
    tenant_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    account_number VARCHAR,
    tax_number VARCHAR,
    is_supplier BOOLEAN DEFAULT FALSE,
    is_customer BOOLEAN DEFAULT FALSE,
    contact_status VARCHAR,
    updated_date_utc TIMESTAMP,
    synced_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xero_payments (
    id SERIAL PRIMARY KEY,
    xero_id VARCHAR UNIQUE NOT NULL,
    tenant_id VARCHAR NOT NULL,
    invoice_id VARCHAR,
    account_id VARCHAR,
    date DATE,
    amount DECIMAL(12,2),
    reference VARCHAR,
    status VARCHAR,
    payment_type VARCHAR,
    updated_date_utc TIMESTAMP,
    synced_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xero_assets (
    id SERIAL PRIMARY KEY,
    xero_id VARCHAR UNIQUE NOT NULL,
    tenant_id VARCHAR NOT NULL,
    asset_name VARCHAR NOT NULL,
    asset_number VARCHAR,
    asset_type_id VARCHAR,
    asset_type_name VARCHAR,
    status VARCHAR,
    purchase_date DATE,
    purchase_price DECIMAL(12,2),
    disposal_date DATE,
    disposal_price DECIMAL(12,2),
    depreciation_method VARCHAR,
    averaging_method VARCHAR,
    depreciation_rate DECIMAL(8,4),
    effective_life_years INT,
    cost_limit DECIMAL(12,2),
    residual_value DECIMAL(12,2),
    book_value DECIMAL(12,2),
    current_accum_depreciation DECIMAL(12,2),
    prior_accum_depreciation DECIMAL(12,2),
    current_depreciation DECIMAL(12,2),
    updated_date_utc TIMESTAMP,
    synced_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xero_asset_types (
    id SERIAL PRIMARY KEY,
    xero_id VARCHAR UNIQUE NOT NULL,
    tenant_id VARCHAR NOT NULL,
    asset_type_name VARCHAR NOT NULL,
    fixed_asset_account_id VARCHAR,
    depreciation_expense_account_id VARCHAR,
    accumulated_depreciation_account_id VARCHAR,
    depreciation_method VARCHAR,
    averaging_method VARCHAR,
    depreciation_rate DECIMAL(8,4),
    effective_life_years INT,
    synced_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xero_journals (
    id SERIAL PRIMARY KEY,
    xero_id VARCHAR UNIQUE NOT NULL,
    tenant_id VARCHAR NOT NULL,
    journal_date DATE,
    journal_number INT,
    source_id VARCHAR,
    source_type VARCHAR,
    reference VARCHAR,
    journal_lines JSONB,
    created_date_utc TIMESTAMP,
    synced_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xero_tracking_categories (
    id SERIAL PRIMARY KEY,
    xero_id VARCHAR UNIQUE NOT NULL,
    tenant_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    status VARCHAR,
    options JSONB,
    synced_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xero_sync_log (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR NOT NULL,
    endpoint VARCHAR NOT NULL,
    last_sync_at TIMESTAMP NOT NULL DEFAULT NOW(),
    records_synced INT DEFAULT 0,
    status VARCHAR DEFAULT 'success',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xero_reports_cache (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR NOT NULL,
    report_type VARCHAR NOT NULL,
    params VARCHAR,
    report_data JSONB NOT NULL,
    cached_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(tenant_id, report_type, params)
);
