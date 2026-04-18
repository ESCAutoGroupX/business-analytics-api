package sync

import (
	"context"
	"log"
	"time"

	"gorm.io/gorm"
)

// AutoMigrate re-runs the idempotent schema from the prior migration file
// and then:
//   1. adds the UNIQUE index on documents.wf_scan_page_id that the upsert
//      ON CONFLICT clause needs, and
//   2. backfills wf_scan_page_id from the legacy wf_scan_id column for any
//      row that already has a 24-char Mongo ObjectId there.
//
// Every statement is idempotent. Runs on server boot from main.go.
func AutoMigrate(db *gorm.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Guard against a zombie session holding a lock on documents — if we
	// can't ADD a column in 5s, warn and move on. Same pattern as
	// DocumentMatchHandler's AutoMigrate.
	conn, err := db.WithContext(ctx).DB()
	if err != nil {
		log.Printf("WF sync AutoMigrate: cannot get sql.DB: %v", err)
		return
	}
	sqlConn, err := conn.Conn(ctx)
	if err != nil {
		log.Printf("WF sync AutoMigrate: cannot acquire conn: %v", err)
		return
	}
	defer sqlConn.Close()
	if _, err := sqlConn.ExecContext(ctx, `SET lock_timeout = '5s'`); err != nil {
		log.Printf("WF sync AutoMigrate: set lock_timeout: %v", err)
	}

	stmts := []string{
		// documents columns — already added by prior migration, repeated for
		// fresh dev DBs.
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_scan_page_id VARCHAR(24)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_location_id VARCHAR(36)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_s3_key VARCHAR(255)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_ocr_agent_version VARCHAR(32)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_ocr_confidence NUMERIC(4,3)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_ml_parsed BOOLEAN DEFAULT FALSE`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_ml_reviewed BOOLEAN DEFAULT FALSE`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_invoice_number VARCHAR(64)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_po_number VARCHAR(64)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_subtotal NUMERIC(14,2)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_tax NUMERIC(14,2)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_line_item_count INTEGER`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_synced_at TIMESTAMPTZ`,

		// mongo_sync_state + seed rows
		`CREATE TABLE IF NOT EXISTS mongo_sync_state (
			collection_name         VARCHAR(64) PRIMARY KEY,
			last_synced_updated_at  TIMESTAMPTZ,
			last_run_started_at     TIMESTAMPTZ,
			last_run_finished_at    TIMESTAMPTZ,
			last_run_status         VARCHAR(16),
			last_run_records        INTEGER,
			last_run_error          TEXT,
			total_synced            BIGINT DEFAULT 0
		)`,
		`INSERT INTO mongo_sync_state (collection_name) VALUES
			('scanPage'), ('statementAudit'), ('partAudit'), ('partMatch')
		 ON CONFLICT (collection_name) DO NOTHING`,

		// Backfill: for rows that already carry a Mongo ObjectId in the legacy
		// wf_scan_id column, copy it to wf_scan_page_id so the upsert conflict
		// target lines up and we don't duplicate existing documents.
		`UPDATE documents
		    SET wf_scan_page_id = wf_scan_id
		  WHERE wf_scan_page_id IS NULL
		    AND wf_scan_id ~ '^[a-f0-9]{24}$'`,

		// UNIQUE index is required for INSERT ... ON CONFLICT (wf_scan_page_id).
		// Postgres treats NULLs as distinct in unique indexes, so a full
		// (non-partial) unique index allows many NULL wf_scan_page_id rows
		// AND satisfies the ON CONFLICT (wf_scan_page_id) inference rule,
		// which a partial predicate-qualified index does not.
		// Drop any prior partial variant before creating the full one.
		`DROP INDEX IF EXISTS uq_documents_wf_scan_page_id`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_wf_scan_page_id
		   ON documents(wf_scan_page_id)`,
	}
	for _, s := range stmts {
		if _, err := sqlConn.ExecContext(ctx, s); err != nil {
			log.Printf("WF sync AutoMigrate: %v\n  SQL: %.120s", err, s)
		}
	}

	// wf_match_results needs composite uniqueness — one audit record explodes
	// into multiple result rows, so a single-column unique on wf_audit_id
	// would reject every second row. Only safe to change the uniqueness while
	// the table is empty; gate on COUNT(*) = 0 and WARN out otherwise.
	var existing int
	if err := sqlConn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM wf_match_results`).Scan(&existing); err != nil {
		log.Printf("WF sync AutoMigrate: cannot count wf_match_results: %v", err)
		return
	}
	if _, err := sqlConn.ExecContext(ctx,
		`ALTER TABLE wf_match_results ADD COLUMN IF NOT EXISTS match_index INTEGER NOT NULL DEFAULT 0`); err != nil {
		log.Printf("WF sync AutoMigrate: add match_index: %v", err)
	}
	if existing > 0 {
		log.Printf("WF sync AutoMigrate: wf_match_results has %d rows — refusing to change unique key", existing)
		return
	}
	// Drop the old single-column unique constraint / index if either remains.
	for _, s := range []string{
		`ALTER TABLE wf_match_results DROP CONSTRAINT IF EXISTS wf_match_results_wf_audit_id_key`,
		`DROP INDEX IF EXISTS wf_match_results_wf_audit_id_key`,
		`CREATE UNIQUE INDEX IF NOT EXISTS wf_match_results_audit_kind_idx
		   ON wf_match_results (wf_audit_id, match_kind, match_index)`,
	} {
		if _, err := sqlConn.ExecContext(ctx, s); err != nil {
			log.Printf("WF sync AutoMigrate: %v\n  SQL: %.120s", err, s)
		}
	}
}
