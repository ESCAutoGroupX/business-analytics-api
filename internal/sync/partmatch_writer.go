package sync

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// PartMatchWriter upserts a batch of PartMatchRow.
type PartMatchWriter interface {
	UpsertBatch(ctx context.Context, rows []PartMatchRow) (inserted, updated int, err error)
}

// PartMatchFKResolver populates invoice_part_audit_id and
// match_part_audit_id via a single per-batch SELECT against wf_part_audits.
type PartMatchFKResolver interface {
	Resolve(ctx context.Context, rows []PartMatchRow) ([]PartMatchRow, error)
}

// PostgresPartMatchFKResolver is the live-DB resolver.
type PostgresPartMatchFKResolver struct{ DB *gorm.DB }

func (r *PostgresPartMatchFKResolver) Resolve(ctx context.Context, rows []PartMatchRow) ([]PartMatchRow, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	// Collect every distinct referenced wf_part_audits.wf_audit_id across both
	// invoice and match sides — single round trip resolves both.
	set := map[string]struct{}{}
	for _, row := range rows {
		if row.WfInvoicePartAuditID != nil && *row.WfInvoicePartAuditID != "" {
			set[*row.WfInvoicePartAuditID] = struct{}{}
		}
		if row.WfMatchPartAuditID != nil && *row.WfMatchPartAuditID != "" {
			set[*row.WfMatchPartAuditID] = struct{}{}
		}
	}
	if len(set) == 0 {
		return rows, nil
	}
	ids := setKeys(set)
	type pa struct {
		ID         int64  `gorm:"column:id"`
		WfAuditID  string `gorm:"column:wf_audit_id"`
	}
	var pas []pa
	if err := r.DB.WithContext(ctx).Raw(
		`SELECT id, wf_audit_id FROM wf_part_audits WHERE wf_audit_id IN (?)`, ids,
	).Scan(&pas).Error; err != nil {
		return nil, fmt.Errorf("wf_part_audits lookup: %w", err)
	}
	byHex := make(map[string]int64, len(pas))
	for _, p := range pas {
		byHex[p.WfAuditID] = p.ID
	}
	for i := range rows {
		if rows[i].WfInvoicePartAuditID != nil {
			if id, ok := byHex[*rows[i].WfInvoicePartAuditID]; ok {
				rows[i].InvoicePartAuditID = &id
			}
		}
		if rows[i].WfMatchPartAuditID != nil {
			if id, ok := byHex[*rows[i].WfMatchPartAuditID]; ok {
				rows[i].MatchPartAuditID = &id
			}
		}
	}
	return rows, nil
}

// PostgresPartMatchWriter upserts into wf_part_match_results.
type PostgresPartMatchWriter struct{ DB *gorm.DB }

func (w *PostgresPartMatchWriter) UpsertBatch(ctx context.Context, rows []PartMatchRow) (int, int, error) {
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
			r.WfMatchID,            // wf_match_id
			r.InvoicePartAuditID,   // invoice_part_audit_id
			r.MatchPartAuditID,     // match_part_audit_id
			r.WfInvoicePartAuditID, // wf_invoice_part_audit_id
			r.WfMatchPartAuditID,   // wf_match_part_audit_id
			r.InvoiceType,          // invoice_type
			r.MatchType,            // match_type
			r.Score,                // score
			r.Algo,                 // algo
			r.MatchedBy,            // matched_by
			r.InvoiceAmount,        // invoice_amount
			r.MatchAmount,          // match_amount
			r.InvoiceQuantity,      // invoice_quantity
			r.MatchQuantity,        // match_quantity
			r.RunID,                // run_id
			r.WfUpdatedAt,          // wf_updated_at
		)
	}

	sql := `
INSERT INTO wf_part_match_results (
    wf_match_id, invoice_part_audit_id, match_part_audit_id,
    wf_invoice_part_audit_id, wf_match_part_audit_id,
    invoice_type, match_type, score, algo, matched_by,
    invoice_amount, match_amount, invoice_quantity, match_quantity,
    run_id, wf_updated_at
) VALUES ` + strings.Join(placeholders, ",") + `
ON CONFLICT (wf_match_id) DO UPDATE SET
    invoice_part_audit_id    = EXCLUDED.invoice_part_audit_id,
    match_part_audit_id      = EXCLUDED.match_part_audit_id,
    wf_invoice_part_audit_id = EXCLUDED.wf_invoice_part_audit_id,
    wf_match_part_audit_id   = EXCLUDED.wf_match_part_audit_id,
    invoice_type             = EXCLUDED.invoice_type,
    match_type               = EXCLUDED.match_type,
    score                    = EXCLUDED.score,
    algo                     = EXCLUDED.algo,
    matched_by               = EXCLUDED.matched_by,
    invoice_amount           = EXCLUDED.invoice_amount,
    match_amount             = EXCLUDED.match_amount,
    invoice_quantity         = EXCLUDED.invoice_quantity,
    match_quantity           = EXCLUDED.match_quantity,
    run_id                   = EXCLUDED.run_id,
    wf_updated_at            = EXCLUDED.wf_updated_at,
    synced_at                = NOW()
RETURNING (xmax = 0) AS inserted
`
	type result struct {
		Inserted bool `gorm:"column:inserted"`
	}
	var results []result
	if err := w.DB.WithContext(ctx).Raw(sql, args...).Scan(&results).Error; err != nil {
		return 0, 0, err
	}
	var ins, upd int
	for _, r := range results {
		if r.Inserted {
			ins++
		} else {
			upd++
		}
	}
	return ins, upd, nil
}
