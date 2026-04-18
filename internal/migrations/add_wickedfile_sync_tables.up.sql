-- Migration: add_wickedfile_sync_tables (UP)
--
-- Adds Postgres index fields for the WickedFile MongoDB sync. Raw OCR
-- text, phrases, and line items stay in Mongo and are loaded lazily —
-- Postgres only keeps what's needed for fast list/filter/join.
--
-- Every statement is idempotent (IF NOT EXISTS / ON CONFLICT DO NOTHING).
-- Safe to run multiple times.
--
-- Deviations from the original spec:
--   * match_results → wf_match_results
--   * part_audits → wf_part_audits
--   * part_match_results → wf_part_match_results
--   The unprefixed names already exist in this database for the unrelated
--   document/transaction matching system. All WickedFile-sync tables and
--   indexes are namespaced with wf_ to avoid collisions.
--
--   * wf_match_results.transaction_id is VARCHAR (not BIGINT). transactions.id
--   is VARCHAR here, and an FK has to match the referenced column type.

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- 2B: extend documents with WickedFile sync columns
-- ─────────────────────────────────────────────────────────────────────

ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_scan_page_id         VARCHAR(24);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_location_id          VARCHAR(36);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_s3_key               VARCHAR(255);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_ocr_agent_version    VARCHAR(32);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_ocr_confidence       NUMERIC(4,3);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_ml_parsed            BOOLEAN DEFAULT FALSE;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_ml_reviewed          BOOLEAN DEFAULT FALSE;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_invoice_number       VARCHAR(64);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_po_number            VARCHAR(64);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_subtotal             NUMERIC(14,2);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_tax                  NUMERIC(14,2);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_line_item_count      INTEGER;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_synced_at            TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_documents_wf_scan_page_id ON documents(wf_scan_page_id);
CREATE INDEX IF NOT EXISTS idx_documents_wf_location_id  ON documents(wf_location_id);
CREATE INDEX IF NOT EXISTS idx_documents_wf_synced_at    ON documents(wf_synced_at);

-- ─────────────────────────────────────────────────────────────────────
-- 2C: wf_match_results
-- ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS wf_match_results (
    id                  BIGSERIAL PRIMARY KEY,
    wf_audit_id         VARCHAR(24) NOT NULL UNIQUE,
    document_id         BIGINT REFERENCES documents(id) ON DELETE SET NULL,
    transaction_id      VARCHAR REFERENCES transactions(id) ON DELETE SET NULL,
    match_kind          VARCHAR(24) NOT NULL,
    match_category      VARCHAR(16) NOT NULL,
    doc_type            VARCHAR(24),
    confidence          NUMERIC(5,2),
    matched_amount      NUMERIC(14,2),
    matched_date        DATE,
    wf_source_txn_id    VARCHAR(64),
    wf_matched_scan_id  VARCHAR(24),
    wf_matched_by       VARCHAR(32),
    wf_risk_score       INTEGER,
    wf_risk_category    VARCHAR(8),
    wf_updated_at       TIMESTAMPTZ,
    synced_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wf_match_results_document_id     ON wf_match_results(document_id);
CREATE INDEX IF NOT EXISTS idx_wf_match_results_transaction_id  ON wf_match_results(transaction_id);
CREATE INDEX IF NOT EXISTS idx_wf_match_results_kind_category   ON wf_match_results(match_kind, match_category);
CREATE INDEX IF NOT EXISTS idx_wf_match_results_wf_updated_at   ON wf_match_results(wf_updated_at);

-- ─────────────────────────────────────────────────────────────────────
-- 2D: wf_part_audits
-- ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS wf_part_audits (
    id                   BIGSERIAL PRIMARY KEY,
    wf_audit_id          VARCHAR(24) NOT NULL UNIQUE,
    wf_object_id         VARCHAR(24),
    document_id          BIGINT REFERENCES documents(id) ON DELETE SET NULL,
    location_id          VARCHAR(36),
    audit_type           VARCHAR(32),
    audit_category       VARCHAR(32),
    product_code         VARCHAR(128),
    product_code_search  VARCHAR(128),
    description          TEXT,
    quantity             NUMERIC(10,3),
    unit_price           NUMERIC(14,4),
    amount               NUMERIC(14,2),
    invoice_id           VARCHAR(64),
    purchase_order       VARCHAR(64),
    vendor_name          VARCHAR(255),
    ro_object_id         VARCHAR(24),
    ro_number            VARCHAR(64),
    start_date           DATE,
    end_date             DATE,
    wf_updated_at        TIMESTAMPTZ,
    synced_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wf_part_audits_document_id    ON wf_part_audits(document_id);
CREATE INDEX IF NOT EXISTS idx_wf_part_audits_wf_object_id   ON wf_part_audits(wf_object_id);
CREATE INDEX IF NOT EXISTS idx_wf_part_audits_product_code   ON wf_part_audits(product_code_search);
CREATE INDEX IF NOT EXISTS idx_wf_part_audits_type_category  ON wf_part_audits(audit_type, audit_category);
CREATE INDEX IF NOT EXISTS idx_wf_part_audits_ro_object_id   ON wf_part_audits(ro_object_id) WHERE ro_object_id IS NOT NULL;

-- ─────────────────────────────────────────────────────────────────────
-- 2E: wf_part_match_results
-- ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS wf_part_match_results (
    id                        BIGSERIAL PRIMARY KEY,
    wf_match_id               VARCHAR(24) NOT NULL UNIQUE,
    invoice_part_audit_id     BIGINT REFERENCES wf_part_audits(id) ON DELETE CASCADE,
    match_part_audit_id       BIGINT REFERENCES wf_part_audits(id) ON DELETE CASCADE,
    wf_invoice_part_audit_id  VARCHAR(24),
    wf_match_part_audit_id    VARCHAR(24),
    invoice_type              VARCHAR(32),
    match_type                VARCHAR(32),
    score                     INTEGER,
    algo                      VARCHAR(64),
    matched_by                VARCHAR(32),
    invoice_amount            NUMERIC(14,2),
    match_amount              NUMERIC(14,2),
    invoice_quantity          NUMERIC(10,3),
    match_quantity            NUMERIC(10,3),
    run_id                    VARCHAR(64),
    wf_updated_at             TIMESTAMPTZ,
    synced_at                 TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wf_part_match_invoice_audit  ON wf_part_match_results(invoice_part_audit_id);
CREATE INDEX IF NOT EXISTS idx_wf_part_match_match_audit    ON wf_part_match_results(match_part_audit_id);
CREATE INDEX IF NOT EXISTS idx_wf_part_match_wf_invoice     ON wf_part_match_results(wf_invoice_part_audit_id);
CREATE INDEX IF NOT EXISTS idx_wf_part_match_wf_match       ON wf_part_match_results(wf_match_part_audit_id);
CREATE INDEX IF NOT EXISTS idx_wf_part_match_type_pair      ON wf_part_match_results(invoice_type, match_type);

-- ─────────────────────────────────────────────────────────────────────
-- 2F: mongo_sync_state + seed rows
-- ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS mongo_sync_state (
    collection_name         VARCHAR(64) PRIMARY KEY,
    last_synced_updated_at  TIMESTAMPTZ,
    last_run_started_at     TIMESTAMPTZ,
    last_run_finished_at    TIMESTAMPTZ,
    last_run_status         VARCHAR(16),
    last_run_records        INTEGER,
    last_run_error          TEXT,
    total_synced            BIGINT DEFAULT 0
);

INSERT INTO mongo_sync_state (collection_name) VALUES
    ('scanPage'),
    ('statementAudit'),
    ('partAudit'),
    ('partMatch')
ON CONFLICT (collection_name) DO NOTHING;

COMMIT;
