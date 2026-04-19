package models

import "time"

// WFVendorAlias — canonical-name store for vendor normalization used by
// the transaction matcher. Alias and canonical are stored as-entered; the
// matcher lowercases at query time (functional index on LOWER(alias) /
// LOWER(canonical) supports that).
//
// Named WFVendorAlias (table wf_vendor_aliases) to avoid a collision with
// the pre-existing vendor_aliases table (Plaid/WF vendor→alias linker,
// UUID PK, FK to vendors.id).
type WFVendorAlias struct {
	ID         int64      `gorm:"primaryKey" json:"id"`
	Alias      string     `json:"alias"`
	Canonical  string     `json:"canonical"`
	Source     string     `json:"source"` // 'manual' | 'ai_approved' | 'ai_suggested'
	Confidence *float64   `json:"confidence"`
	Notes      *string    `json:"notes"`
	CreatedBy  *string    `gorm:"column:created_by"  json:"created_by"`
	ApprovedBy *string    `gorm:"column:approved_by" json:"approved_by"`
	ApprovedAt *time.Time `gorm:"column:approved_at" json:"approved_at"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

func (WFVendorAlias) TableName() string { return "wf_vendor_aliases" }

// WFTransactionMatch — one row per transaction ↔ document pairing, with the
// full score-component breakdown recorded so a match that looks wrong can
// be debugged without re-running the matcher.
type WFTransactionMatch struct {
	ID                    int64 `gorm:"primaryKey" json:"id"`
	TransactionID         string   `gorm:"column:transaction_id" json:"transaction_id"`
	DocumentID            int64    `gorm:"column:document_id"    json:"document_id"`
	MatchStatus           string   `gorm:"column:match_status"   json:"match_status"`
	Score                 int      `json:"score"`
	MatchAlgorithmVersion string   `gorm:"column:match_algorithm_version" json:"match_algorithm_version"`
	AmountMode            string   `gorm:"column:amount_mode"            json:"amount_mode"`
	SurchargeFlag         bool     `gorm:"column:surcharge_flag"         json:"surcharge_flag"`
	ActualInvoiceAmount   *float64 `gorm:"column:actual_invoice_amount"  json:"actual_invoice_amount"`
	SurchargePct          *float64 `gorm:"column:surcharge_pct"          json:"surcharge_pct"`
	AmountScoreComponent  int      `gorm:"column:amount_score_component" json:"amount_score_component"`
	VendorScoreComponent  int      `gorm:"column:vendor_score_component" json:"vendor_score_component"`
	DateScoreComponent    int      `gorm:"column:date_score_component"   json:"date_score_component"`
	VendorSimilarityPct   *float64 `gorm:"column:vendor_similarity_pct"  json:"vendor_similarity_pct"`
	DaysApart             *int     `gorm:"column:days_apart"             json:"days_apart"`
	RunnerUpScore         *int     `gorm:"column:runner_up_score"        json:"runner_up_score"`
	AliasMatchedID        *int64   `gorm:"column:alias_matched_id"       json:"alias_matched_id"`
	MatchedBy             string   `gorm:"column:matched_by"             json:"matched_by"`
	RunID                 *string  `gorm:"column:run_id"                 json:"run_id"`
	UserConfirmed         bool     `gorm:"column:user_confirmed"         json:"user_confirmed"`
	UserRejected          bool     `gorm:"column:user_rejected"          json:"user_rejected"`
	UserOverrideNote      *string  `gorm:"column:user_override_note"     json:"user_override_note"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}

func (WFTransactionMatch) TableName() string { return "wf_transaction_matches" }

// WFMatcherRun — metadata for a single matcher execution. Mirrors the
// wf_matcher_runs table. Used for audit + resumable state.
type WFMatcherRun struct {
	ID                   int64      `gorm:"primaryKey" json:"id"`
	RunID                string     `gorm:"column:run_id"                 json:"run_id"`
	StartedAt            time.Time  `gorm:"column:started_at"             json:"started_at"`
	FinishedAt           *time.Time `gorm:"column:finished_at"            json:"finished_at"`
	Status               string     `json:"status"`
	Mode                 string     `json:"mode"`
	TransactionFilter    *string    `gorm:"column:transaction_filter"     json:"transaction_filter"`
	TransactionsScanned  int        `gorm:"column:transactions_scanned"   json:"transactions_scanned"`
	CandidatesConsidered int        `gorm:"column:candidates_considered"  json:"candidates_considered"`
	MatchedCount         int        `gorm:"column:matched_count"          json:"matched_count"`
	SuspectCount         int        `gorm:"column:suspect_count"          json:"suspect_count"`
	AmbiguousCount       int        `gorm:"column:ambiguous_count"        json:"ambiguous_count"`
	UnmatchedCount       int        `gorm:"column:unmatched_count"        json:"unmatched_count"`
	ErrorCount           int        `gorm:"column:error_count"            json:"error_count"`
	FirstError           *string    `gorm:"column:first_error"            json:"first_error"`
	AlgorithmVersion     string     `gorm:"column:algorithm_version"      json:"algorithm_version"`
	ElapsedMs            *int64     `gorm:"column:elapsed_ms"             json:"elapsed_ms"`
}

func (WFMatcherRun) TableName() string { return "wf_matcher_runs" }
