package sync

import (
	"context"
	"fmt"
	"log"
	"strings"

	"gorm.io/gorm"
)

// MatchResultWriter upserts a batch of exploded MatchResultRow into
// wf_match_results. Behind an interface so tests don't need Postgres.
type MatchResultWriter interface {
	UpsertBatch(ctx context.Context, rows []MatchResultRow) (inserted, updated int, err error)
}

// FKResolver populates DocumentID / TransactionID on a batch of rows
// using whatever lookup the backend needs. Exposed as an interface so the
// engine test can inject a deterministic resolver.
type FKResolver interface {
	Resolve(ctx context.Context, rows []MatchResultRow) ([]MatchResultRow, error)
}

// PostgresFKResolver resolves FK IDs against the real documents and
// transactions tables using two set-based queries per batch (no N+1).
type PostgresFKResolver struct {
	DB                   *gorm.DB
	WarnNoTxnLookup      bool   // flipped true once we notice we can't resolve txn IDs
	TransactionsHasPlaid bool   // probed once in NewPostgresFKResolver
}

// NewPostgresFKResolver probes the transactions table so we know whether
// we can do the plaid-id lookup at all, and logs a one-time warning if
// we can't.
func NewPostgresFKResolver(db *gorm.DB) *PostgresFKResolver {
	r := &PostgresFKResolver{DB: db}
	var exists bool
	err := db.Raw(
		`SELECT EXISTS (
		   SELECT 1 FROM information_schema.columns
		    WHERE table_name = 'transactions'
		      AND column_name IN ('plaid_id','id')
		 )`,
	).Scan(&exists).Error
	if err != nil || !exists {
		log.Printf("WF match sync: no plaid_id/id column on transactions — transaction_id FKs will be NULL until resolved in Step 4.5")
	}
	r.TransactionsHasPlaid = exists
	return r
}

func (r *PostgresFKResolver) Resolve(ctx context.Context, rows []MatchResultRow) ([]MatchResultRow, error) {
	if len(rows) == 0 {
		return rows, nil
	}

	// ── documents lookup ─────────────────────────────────────────────
	scanSet := map[string]struct{}{}
	for _, row := range rows {
		if row.ScanPageIDHex != "" {
			scanSet[row.ScanPageIDHex] = struct{}{}
		}
	}
	docByScan := map[string]int64{}
	if len(scanSet) > 0 {
		ids := setKeys(scanSet)
		type docRow struct {
			ID           int64  `gorm:"column:id"`
			WfScanPageID string `gorm:"column:wf_scan_page_id"`
		}
		var docs []docRow
		if err := r.DB.WithContext(ctx).Raw(
			`SELECT id, wf_scan_page_id FROM documents WHERE wf_scan_page_id = ANY(?)`, ids,
		).Scan(&docs).Error; err != nil {
			return nil, fmt.Errorf("documents lookup: %w", err)
		}
		for _, d := range docs {
			docByScan[d.WfScanPageID] = d.ID
		}
	}

	// ── transactions lookup (id OR plaid_id) ────────────────────────
	txnByKey := map[string]string{}
	if r.TransactionsHasPlaid {
		txnSet := map[string]struct{}{}
		for _, row := range rows {
			if row.WfSourceTxnID != nil && *row.WfSourceTxnID != "" {
				txnSet[*row.WfSourceTxnID] = struct{}{}
			}
		}
		if len(txnSet) > 0 {
			ids := setKeys(txnSet)
			type txnRow struct {
				ID      string  `gorm:"column:id"`
				PlaidID *string `gorm:"column:plaid_id"`
			}
			var txns []txnRow
			if err := r.DB.WithContext(ctx).Raw(
				`SELECT id, plaid_id FROM transactions
				  WHERE id = ANY(?) OR plaid_id = ANY(?)`, ids, ids,
			).Scan(&txns).Error; err != nil {
				return nil, fmt.Errorf("transactions lookup: %w", err)
			}
			for _, t := range txns {
				txnByKey[t.ID] = t.ID
				if t.PlaidID != nil {
					txnByKey[*t.PlaidID] = t.ID
				}
			}
		}
	}

	// ── populate rows ────────────────────────────────────────────────
	for i := range rows {
		if id, ok := docByScan[rows[i].ScanPageIDHex]; ok {
			rows[i].DocumentID = &id
		}
		if rows[i].WfSourceTxnID != nil {
			if txnID, ok := txnByKey[*rows[i].WfSourceTxnID]; ok {
				copy := txnID
				rows[i].TransactionID = &copy
			}
		}
	}
	return rows, nil
}

func setKeys(s map[string]struct{}) []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	return out
}

// PostgresMatchResultWriter upserts exploded rows against wf_match_results
// using INSERT … ON CONFLICT (wf_audit_id, match_kind, match_index) DO UPDATE.
type PostgresMatchResultWriter struct {
	DB *gorm.DB
}

func (w *PostgresMatchResultWriter) UpsertBatch(ctx context.Context, rows []MatchResultRow) (int, int, error) {
	if len(rows) == 0 {
		return 0, 0, nil
	}

	const colCount = 16
	args := make([]interface{}, 0, len(rows)*colCount)
	placeholders := make([]string, 0, len(rows))

	for i, r := range rows {
		base := i * colCount
		ph := make([]string, colCount)
		for j := 0; j < colCount; j++ {
			ph[j] = fmt.Sprintf("$%d", base+j+1)
		}
		placeholders = append(placeholders, "("+strings.Join(ph, ",")+")")

		args = append(args,
			r.WfAuditID,       // wf_audit_id
			r.MatchIndex,      // match_index
			r.MatchKind,       // match_kind
			r.MatchCategory,   // match_category
			r.DocType,         // doc_type
			r.Confidence,      // confidence
			r.MatchedAmount,   // matched_amount
			r.MatchedDate,     // matched_date
			r.WfSourceTxnID,   // wf_source_txn_id
			r.WfMatchedScanID, // wf_matched_scan_id
			r.WfMatchedBy,     // wf_matched_by
			r.WfRiskScore,     // wf_risk_score
			r.WfRiskCategory,  // wf_risk_category
			r.WfUpdatedAt,     // wf_updated_at
			r.DocumentID,      // document_id
			r.TransactionID,   // transaction_id
		)
	}

	sql := `
INSERT INTO wf_match_results (
    wf_audit_id, match_index, match_kind, match_category,
    doc_type, confidence, matched_amount, matched_date,
    wf_source_txn_id, wf_matched_scan_id, wf_matched_by,
    wf_risk_score, wf_risk_category, wf_updated_at,
    document_id, transaction_id
) VALUES ` + strings.Join(placeholders, ",") + `
ON CONFLICT (wf_audit_id, match_kind, match_index) DO UPDATE SET
    match_category      = EXCLUDED.match_category,
    doc_type            = EXCLUDED.doc_type,
    confidence          = EXCLUDED.confidence,
    matched_amount      = EXCLUDED.matched_amount,
    matched_date        = EXCLUDED.matched_date,
    wf_source_txn_id    = EXCLUDED.wf_source_txn_id,
    wf_matched_scan_id  = EXCLUDED.wf_matched_scan_id,
    wf_matched_by       = EXCLUDED.wf_matched_by,
    wf_risk_score       = EXCLUDED.wf_risk_score,
    wf_risk_category    = EXCLUDED.wf_risk_category,
    wf_updated_at       = EXCLUDED.wf_updated_at,
    document_id         = EXCLUDED.document_id,
    transaction_id      = EXCLUDED.transaction_id,
    synced_at           = NOW()
RETURNING (xmax = 0) AS inserted
`

	type result struct {
		Inserted bool `gorm:"column:inserted"`
	}
	var results []result
	if err := w.DB.WithContext(ctx).Raw(sql, args...).Scan(&results).Error; err != nil {
		return 0, 0, err
	}
	var inserted, updated int
	for _, r := range results {
		if r.Inserted {
			inserted++
		} else {
			updated++
		}
	}
	return inserted, updated, nil
}
