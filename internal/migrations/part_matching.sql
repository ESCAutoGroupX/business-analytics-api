-- Part Matching Engine & Vendor Receivables/Credits
-- Migration: part_matching.sql

CREATE TABLE IF NOT EXISTS vendor_receivables (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  shop_id INTEGER,
  shop_name VARCHAR,
  vendor_id INTEGER,
  vendor_name VARCHAR NOT NULL,
  invoice_document_id INTEGER REFERENCES documents(id),
  invoice_number VARCHAR,
  invoice_date DATE,
  part_number VARCHAR,
  part_number_normalized VARCHAR,
  description VARCHAR,
  quantity NUMERIC(8,2),
  unit_price NUMERIC(12,2),
  total_amount NUMERIC(12,2) NOT NULL,
  receivable_type VARCHAR NOT NULL,
  status VARCHAR DEFAULT 'open',
  ro_number VARCHAR,
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS vendor_credits (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  vendor_id INTEGER,
  vendor_name VARCHAR NOT NULL,
  credit_document_id INTEGER REFERENCES documents(id),
  credit_memo_number VARCHAR,
  credit_date DATE,
  total_amount NUMERIC(12,2) NOT NULL,
  remaining_amount NUMERIC(12,2),
  reference_invoice_number VARCHAR,
  reference_po_number VARCHAR,
  status VARCHAR DEFAULT 'open',
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS vendor_credit_applications (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  credit_id UUID REFERENCES vendor_credits(id),
  receivable_id UUID REFERENCES vendor_receivables(id),
  amount NUMERIC(12,2) NOT NULL,
  applied_by VARCHAR,
  applied_at TIMESTAMP DEFAULT NOW(),
  notes TEXT
);

CREATE TABLE IF NOT EXISTS part_match_results (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  document_id INTEGER REFERENCES documents(id),
  line_item_index INTEGER,
  vendor_part_number VARCHAR,
  vendor_part_normalized VARCHAR,
  matched_ro_number VARCHAR,
  matched_ro_line_item_id VARCHAR,
  matched_part_number VARCHAR,
  match_score INTEGER,
  match_rule VARCHAR,
  match_confidence NUMERIC(4,2),
  ai_tiebreaker_used BOOLEAN DEFAULT false,
  ai_reasoning TEXT,
  status VARCHAR DEFAULT 'pending',
  confirmed_by VARCHAR,
  confirmed_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS vendor_part_mappings (
  id SERIAL PRIMARY KEY,
  vendor_name VARCHAR NOT NULL,
  vendor_part_number VARCHAR NOT NULL,
  vendor_part_normalized VARCHAR NOT NULL,
  internal_part_number VARCHAR,
  description VARCHAR,
  match_count INTEGER DEFAULT 1,
  auto_match_enabled BOOLEAN DEFAULT false,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(vendor_name, vendor_part_normalized)
);

CREATE INDEX IF NOT EXISTS idx_vendor_receivables_vendor ON vendor_receivables(vendor_id);
CREATE INDEX IF NOT EXISTS idx_vendor_receivables_status ON vendor_receivables(status);
CREATE INDEX IF NOT EXISTS idx_vendor_receivables_shop ON vendor_receivables(shop_id);
CREATE INDEX IF NOT EXISTS idx_vendor_credits_vendor ON vendor_credits(vendor_id);
CREATE INDEX IF NOT EXISTS idx_vendor_credits_status ON vendor_credits(status);
CREATE INDEX IF NOT EXISTS idx_part_match_results_doc ON part_match_results(document_id);
CREATE INDEX IF NOT EXISTS idx_vendor_part_mappings_vendor ON vendor_part_mappings(vendor_name);
