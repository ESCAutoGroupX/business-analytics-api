DROP TABLE IF EXISTS ap_entries_v2;
DROP TABLE IF EXISTS ap_entries;

CREATE TABLE ap_entries (
  id SERIAL PRIMARY KEY,
  vendor_id VARCHAR,
  vendor_name VARCHAR,
  statement_document_id INTEGER,
  period_start DATE,
  period_end DATE,
  total_amount NUMERIC(12,2),
  matched_amount NUMERIC(12,2) DEFAULT 0,
  unmatched_amount NUMERIC(12,2) DEFAULT 0,
  invoice_count INTEGER DEFAULT 0,
  matched_invoice_count INTEGER DEFAULT 0,
  status VARCHAR DEFAULT 'open',
  authorized_by VARCHAR,
  authorized_at TIMESTAMP,
  paid_at TIMESTAMP,
  payment_method VARCHAR,
  payment_reference VARCHAR,
  bank_account VARCHAR DEFAULT 'Regions Advantage',
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Verify both tables
SELECT tablename FROM pg_tables WHERE tablename IN ('ap_entries', 'statement_line_items');
SELECT name, normalized_name, billing_frequency FROM vendors WHERE normalized_name IS NOT NULL LIMIT 5;
