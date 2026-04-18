package sync

import (
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// Column caps on wf_part_match_results.
const (
	lenMatchInvoiceType = 32
	lenMatchMatchType   = 32
	lenMatchAlgo        = 64
	lenMatchMatchedBy   = 32
	lenMatchRunID       = 64
)

// PartMatchRow is one wf_part_match_results row. The BIGINT FK fields
// (InvoicePartAuditID, MatchPartAuditID) start nil; the resolver fills
// them from wf_part_audits via the stored wf_invoice_part_audit_id /
// wf_match_part_audit_id hex values.
type PartMatchRow struct {
	WfMatchID             string
	InvoicePartAuditID    *int64
	MatchPartAuditID      *int64
	WfInvoicePartAuditID  *string
	WfMatchPartAuditID    *string
	InvoiceType           *string
	MatchType             *string
	Score                 *int
	Algo                  *string
	MatchedBy             *string
	InvoiceAmount         *float64
	MatchAmount           *float64
	InvoiceQuantity       *float64
	MatchQuantity         *float64
	RunID                 *string
	WfUpdatedAt           time.Time
}

// MapPartMatch converts a Mongo partMatch to a PartMatchRow. Pure, no I/O.
func MapPartMatch(p *mongodb.PartMatch) PartMatchRow {
	row := PartMatchRow{
		WfMatchID:   p.ID.Hex(),
		WfUpdatedAt: p.UpdatedAt,
	}
	if p.InvoicePartAuditID != nil {
		hex := p.InvoicePartAuditID.Hex()
		row.WfInvoicePartAuditID = &hex
	}
	if p.MatchPartAuditID != nil {
		hex := p.MatchPartAuditID.Hex()
		row.WfMatchPartAuditID = &hex
	}
	if v := strOrNil(p.InvoiceType); v != nil {
		t := truncate("invoice_type", *v, lenMatchInvoiceType)
		row.InvoiceType = &t
	}
	if v := strOrNil(p.MatchType); v != nil {
		t := truncate("match_type", *v, lenMatchMatchType)
		row.MatchType = &t
	}
	if p.Score != nil {
		row.Score = p.Score
	}
	if v := strOrNil(p.Algo); v != nil {
		t := truncate("algo", *v, lenMatchAlgo)
		row.Algo = &t
	}
	if v := strOrNil(p.MatchedBy); v != nil {
		t := truncate("matched_by", *v, lenMatchMatchedBy)
		row.MatchedBy = &t
	}
	if p.InvoiceAmount != nil {
		row.InvoiceAmount = p.InvoiceAmount
	}
	if p.MatchAmount != nil {
		row.MatchAmount = p.MatchAmount
	}
	if p.InvoiceQuantity != nil {
		row.InvoiceQuantity = p.InvoiceQuantity
	}
	if p.MatchQuantity != nil {
		row.MatchQuantity = p.MatchQuantity
	}
	if v := strOrNil(p.RunID); v != nil {
		t := truncate("run_id", *v, lenMatchRunID)
		row.RunID = &t
	}
	return row
}
