package sync

import (
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// MatchKind is the discriminator on wf_match_results distinguishing the
// three match-array sources on a statementAudit record.
const (
	MatchKindTransaction = "transaction"
	MatchKindScan        = "scan"
	MatchKindAccounting  = "accounting"
	MatchKindNone        = "none"
)

// MatchResultRow is one exploded wf_match_results row. All FK fields
// (DocumentID, TransactionID) are left nil by the mapping — the engine
// resolves them in a batch lookup before the upsert.
type MatchResultRow struct {
	WfAuditID        string
	MatchIndex       int
	MatchKind        string
	MatchCategory    string
	DocType          *string
	Confidence       *float64 // percentage (0-100) per spec
	MatchedAmount    *float64
	MatchedDate      *time.Time
	WfSourceTxnID    *string
	WfMatchedScanID  *string
	WfMatchedBy      *string
	WfRiskScore      *int
	WfRiskCategory   *string
	WfUpdatedAt      time.Time

	// Populated by the engine's FK resolver.
	DocumentID    *int64
	TransactionID *string // transactions.id is VARCHAR

	// The Mongo scanPageId hex that the engine uses to resolve DocumentID.
	ScanPageIDHex string
}

// ExplodeAudit converts one StatementAudit into the wf_match_results rows
// it produces. At least one row is always emitted — if all three match
// arrays are empty, a match_kind='none' row records the audit existed.
func ExplodeAudit(a *mongodb.StatementAudit) []MatchResultRow {
	base := baseRowFromAudit(a)

	var out []MatchResultRow

	for i, m := range a.TransactionMatches {
		r := base
		r.MatchIndex = i
		r.MatchKind = MatchKindTransaction
		if m.Percentage != nil {
			r.Confidence = m.Percentage
		}
		r.WfMatchedBy = coalesceMatchedBy(m.MatchedBy, payloadBy(m.Match))
		if m.Match != nil {
			if s := strOrNil(m.Match.SourceTransactionID); s != nil {
				r.WfSourceTxnID = s
			}
		}
		out = append(out, r)
	}

	for i, m := range a.ScanMatches {
		r := base
		r.MatchIndex = i
		r.MatchKind = MatchKindScan
		if m.Percentage != nil {
			r.Confidence = m.Percentage
		}
		r.WfMatchedBy = coalesceMatchedBy(m.MatchedBy, "")
		scanID := m.ID.Hex()
		r.WfMatchedScanID = &scanID
		out = append(out, r)
	}

	for i, m := range a.AccountingMatches {
		r := base
		r.MatchIndex = i
		r.MatchKind = MatchKindAccounting
		if m.Percentage != nil {
			r.Confidence = m.Percentage
		}
		r.WfMatchedBy = coalesceMatchedBy(m.MatchedBy, "")
		out = append(out, r)
	}

	if len(out) == 0 {
		r := base
		r.MatchKind = MatchKindNone
		out = append(out, r)
	}
	return out
}

func baseRowFromAudit(a *mongodb.StatementAudit) MatchResultRow {
	r := MatchResultRow{
		WfAuditID:     a.ID.Hex(),
		MatchCategory: a.Category,
		WfUpdatedAt:   a.UpdatedAt,
	}
	if v := strOrNil(a.Type); v != nil {
		r.DocType = v
	}
	if a.Amount != nil {
		r.MatchedAmount = a.Amount
	}
	if a.Date != nil {
		r.MatchedDate = a.Date
	}
	if a.Risk != nil {
		if a.Risk.Score != nil {
			r.WfRiskScore = a.Risk.Score
		}
		if v := strOrNil(a.Risk.Category); v != nil {
			// wf_risk_category is VARCHAR(8) — truncate defensively.
			trunc := truncate("wf_risk_category", *v, 8)
			r.WfRiskCategory = &trunc
		}
	}
	if a.ScanPageID != nil {
		r.ScanPageIDHex = a.ScanPageID.Hex()
	}
	return r
}

// coalesceMatchedBy prefers the entry-level 'by' and falls back to the
// nested match.by, then truncates to wf_matched_by's VARCHAR(32).
func coalesceMatchedBy(entry, payload string) *string {
	v := entry
	if v == "" {
		v = payload
	}
	if v == "" {
		return nil
	}
	t := truncate("wf_matched_by", v, 32)
	return &t
}

// payloadBy pulls 'by' from an optional TransactionMatchPayload.
func payloadBy(p *mongodb.TransactionMatchPayload) string {
	if p == nil {
		return ""
	}
	return p.By
}
