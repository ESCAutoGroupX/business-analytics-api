package matcher

import "time"

// CandidateView is the compact per-candidate detail surfaced in the log.
// Populated for up to the top-3 scored candidates of each transaction so
// a reviewer can see the selected match AND the runners-up side-by-side.
type CandidateView struct {
	DocumentID           int64
	Score                int
	DocDate              time.Time
	DocAmount            float64
	DocType              string
	DocVendor            string
	DocInvoiceNum        string
	DocScanPageID        string
	AmountMode           string
	AmountScoreComponent int
	VendorScoreComponent int
	DateScoreComponent   int
	VendorSimilarityPct  float64
	SurchargeFlag        bool
	SurchargePct         *float64
	ActualInvoiceAmount  *float64
}

// MatchProposal captures one candidate's scoring, classification, and the
// context needed to write the human-review log entry. Preview mode
// returns a slice of these; live mode will insert them into
// wf_transaction_matches (later step).
type MatchProposal struct {
	TransactionID string
	DocumentID    int64
	Score         int
	Status        string // 'matched' | 'suspect' | 'ambiguous' | 'unmatched'

	AmountMode           string // 'exact' | 'surcharge'
	SurchargeFlag        bool
	ActualInvoiceAmount  *float64
	SurchargePct         *float64
	AmountScoreComponent int
	VendorScoreComponent int
	DateScoreComponent   int
	VendorSimilarityPct  float64
	DaysApart            int
	RunnerUpScore        *int
	AliasMatchedID       *int64

	// Context — used by the log formatter; not persisted.
	TxnDate       time.Time
	TxnAmount     float64
	TxnVendor     string
	TxnAccount    string
	DocDate       time.Time
	DocAmount     float64
	DocVendor     string
	DocType       string
	DocInvoiceNum string
	DocScanPageID string

	// TopN is the top-3 scored candidates (0th duplicates the selected match).
	// Used by the log formatter to surface runners-up; empty on UNMATCHED
	// transactions that had no qualifying candidates at all.
	TopN []CandidateView
}

// RunStats aggregates a run's outcome for the status endpoint and the
// tail of the log file.
type RunStats struct {
	TransactionsScanned  int    `json:"transactions_scanned"`
	CandidatesConsidered int    `json:"candidates_considered"`
	MatchedCount         int    `json:"matched_count"`
	SuspectCount         int    `json:"suspect_count"`
	AmbiguousCount       int    `json:"ambiguous_count"`
	UnmatchedCount       int    `json:"unmatched_count"`
	ErrorCount           int    `json:"error_count"`
	FirstError           string `json:"first_error,omitempty"`
	ElapsedMs            int64  `json:"elapsed_ms"`
}

// PreviewOpts tunes a RunPreview invocation.
type PreviewOpts struct {
	Limit         int        // 0 = no cap
	DateFrom      *time.Time // optional txn date filter
	DateTo        *time.Time
	OnlyUnmatched bool   // default TRUE at caller; kept explicit for tests
	LogPath       string // "" = no log file written
}
