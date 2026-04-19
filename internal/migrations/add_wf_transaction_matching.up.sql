-- Migration: add_wf_transaction_matching (UP)
--
-- Schema foundation for the WickedFile transaction matcher (Step 5.1).
-- Three tables:
--   wf_vendor_aliases          — canonical-name store for vendor normalization
--   wf_transaction_matches  — M:N transactions ↔ documents with score audit
--   wf_matcher_runs         — matcher-job metadata + resumable state
--
-- No matching logic yet; this just creates the storage. Every statement is
-- idempotent (IF NOT EXISTS) so a re-run of psql -f is safe.
--
-- Deviation from spec: table 1 was originally `vendor_aliases`, but a
-- different `vendor_aliases` table with 11 live rows already exists on
-- prod (Plaid/WF vendor→alias linker, FK to vendors.id, UUID PK). The
-- new table is namespaced under wf_* to match the other two matcher
-- tables and avoid the collision.

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- wf_vendor_aliases
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wf_vendor_aliases (
    id              BIGSERIAL PRIMARY KEY,
    alias           VARCHAR(255) NOT NULL,
    canonical       VARCHAR(255) NOT NULL,
    source          VARCHAR(32)  NOT NULL,    -- 'manual' | 'ai_approved' | 'ai_suggested'
    confidence      NUMERIC(5,2),             -- 0-100; NULL for manual
    notes           TEXT,
    created_by      VARCHAR(128),
    approved_by     VARCHAR(128),
    approved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (alias, canonical)
);

CREATE INDEX IF NOT EXISTS idx_wf_vendor_aliases_alias     ON wf_vendor_aliases(LOWER(alias));
CREATE INDEX IF NOT EXISTS idx_wf_vendor_aliases_canonical ON wf_vendor_aliases(LOWER(canonical));
CREATE INDEX IF NOT EXISTS idx_wf_vendor_aliases_source    ON wf_vendor_aliases(source);

-- ─────────────────────────────────────────────────────────────────────
-- wf_transaction_matches
-- ─────────────────────────────────────────────────────────────────────
-- transactions.id is VARCHAR in this schema (see Step 2 migration notes);
-- the FK type has to match.
CREATE TABLE IF NOT EXISTS wf_transaction_matches (
    id                        BIGSERIAL PRIMARY KEY,
    transaction_id            VARCHAR NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    document_id               BIGINT  NOT NULL REFERENCES documents(id)    ON DELETE CASCADE,

    -- Match outcome
    match_status              VARCHAR(16) NOT NULL,      -- 'matched' | 'suspect' | 'ambiguous'
    score                     INTEGER     NOT NULL,      -- 0–250+
    match_algorithm_version   VARCHAR(32) NOT NULL,      -- 'v1' initially; bump when algo changes

    -- Credit-card surcharge handling
    amount_mode               VARCHAR(16) NOT NULL,      -- 'exact' | 'surcharge'
    surcharge_flag            BOOLEAN     NOT NULL DEFAULT FALSE,
    actual_invoice_amount     NUMERIC(14,2),             -- populated only when amount_mode='surcharge'
    surcharge_pct             NUMERIC(5,2),              -- 0.00-3.00 typically; NULL for exact

    -- Score component audit trail
    amount_score_component    INTEGER     NOT NULL,
    vendor_score_component    INTEGER     NOT NULL,
    date_score_component      INTEGER     NOT NULL,
    vendor_similarity_pct     NUMERIC(5,2),              -- 0-100 before bonus
    days_apart                INTEGER,                   -- 0–7

    -- Runner-up for ambiguity detection
    runner_up_score           INTEGER,                   -- best OTHER candidate's score; NULL if none

    -- Provenance
    alias_matched_id          BIGINT REFERENCES wf_vendor_aliases(id) ON DELETE SET NULL,
    matched_by                VARCHAR(32) NOT NULL,      -- 'system_v1' | 'user:bob@...'
    run_id                    VARCHAR(64),               -- batch run identifier

    -- User override fields (for later UI work)
    user_confirmed            BOOLEAN DEFAULT FALSE,
    user_rejected             BOOLEAN DEFAULT FALSE,
    user_override_note        TEXT,

    created_at                TIMESTAMPTZ DEFAULT NOW(),
    updated_at                TIMESTAMPTZ DEFAULT NOW(),

    -- Prevent duplicate matches of the same transaction to the same document
    UNIQUE (transaction_id, document_id)
);

CREATE INDEX IF NOT EXISTS idx_wf_txn_matches_transaction_id
    ON wf_transaction_matches(transaction_id);
CREATE INDEX IF NOT EXISTS idx_wf_txn_matches_document_id
    ON wf_transaction_matches(document_id);
CREATE INDEX IF NOT EXISTS idx_wf_txn_matches_status
    ON wf_transaction_matches(match_status);
CREATE INDEX IF NOT EXISTS idx_wf_txn_matches_surcharge
    ON wf_transaction_matches(surcharge_flag) WHERE surcharge_flag = TRUE;
CREATE INDEX IF NOT EXISTS idx_wf_txn_matches_run_id
    ON wf_transaction_matches(run_id);

-- ─────────────────────────────────────────────────────────────────────
-- wf_matcher_runs
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wf_matcher_runs (
    id                    BIGSERIAL PRIMARY KEY,
    run_id                VARCHAR(64) UNIQUE NOT NULL,
    started_at            TIMESTAMPTZ DEFAULT NOW(),
    finished_at           TIMESTAMPTZ,
    status                VARCHAR(16) NOT NULL,       -- 'running' | 'success' | 'failed' | 'preview'
    mode                  VARCHAR(16) NOT NULL,       -- 'preview' (dry-run) | 'live'

    -- Scope
    transaction_filter    TEXT,                       -- e.g., "unmatched only", "date range", "all"
    transactions_scanned  INTEGER DEFAULT 0,
    candidates_considered INTEGER DEFAULT 0,

    -- Outcomes
    matched_count         INTEGER DEFAULT 0,
    suspect_count         INTEGER DEFAULT 0,          -- surcharge-flagged or borderline
    ambiguous_count       INTEGER DEFAULT 0,
    unmatched_count       INTEGER DEFAULT 0,

    -- Error trail
    error_count           INTEGER DEFAULT 0,
    first_error           TEXT,

    algorithm_version     VARCHAR(32) NOT NULL,
    elapsed_ms            BIGINT
);

CREATE INDEX IF NOT EXISTS idx_matcher_runs_status ON wf_matcher_runs(status);

COMMIT;
