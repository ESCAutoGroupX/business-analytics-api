// Package matcher implements the WickedFile transaction ↔ document
// matcher. The algorithm scores each candidate on three dimensions
// (amount, vendor, date proximity), handles credit-card-surcharge
// offsets, and classifies the best candidate as matched / suspect /
// ambiguous / unmatched.
//
// Scoring is pure — no I/O — and exercised by table tests. The runner
// layer (runner.go) orchestrates DB reads and writes the human-review
// log file.
package matcher

import (
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/xrash/smetrics"
)

// AlgorithmVersion is stamped on every proposal so old rows can be
// identified + re-run when the algorithm changes.
const AlgorithmVersion = "v1"

// Classification bucket constants.
const (
	StatusMatched   = "matched"
	StatusSuspect   = "suspect"
	StatusAmbiguous = "ambiguous"
	StatusUnmatched = "unmatched"
)

// Vendor-match thresholds.
const (
	JaroWinklerThreshold = 0.85 // below this, award 0 vendor points
	VendorContainsScore  = 85   // one name contains the other after normalization
	VendorExactScore     = 100  // equal after normalization
)

// punct matches anything that's not alphanum or whitespace.
var punct = regexp.MustCompile(`[^\p{L}\p{N}\s]+`)
var ws = regexp.MustCompile(`\s+`)

// normalizeVendor lowercases, strips punctuation, and collapses runs of
// whitespace. Used before comparing two vendor strings.
func normalizeVendor(s string) string {
	s = strings.ToLower(s)
	s = punct.ReplaceAllString(s, " ")
	s = ws.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

// vendorContainsEitherWay returns true if one string contains the other
// as a substring (after normalization). Catches "worldpac" vs
// "worldpac atlanta".
func vendorContainsEitherWay(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	return strings.Contains(a, b) || strings.Contains(b, a)
}

// jaroWinkler delegates to smetrics. Broken out so tests can pin the
// implementation and so future swaps (e.g. to a local copy) are local.
func jaroWinkler(a, b string) float64 {
	return smetrics.JaroWinkler(a, b, 0.7, 4)
}

// CreditAccountFields is the minimum shape scoreAmount needs to decide
// whether a transaction is on a credit card account. Real transactions
// (with their 50+ columns) aren't needed here.
type CreditAccountFields struct {
	AccountType    string
	AccountSubtype string
	AccountName    string
}

// isCreditAccount decides whether a transaction is on a credit card
// account. Heuristic on three account-* fields — intentionally string-based
// per spec; refinable later with a join against plaid_items.
func isCreditAccount(f CreditAccountFields) bool {
	t := strings.ToLower(f.AccountType)
	s := strings.ToLower(f.AccountSubtype)
	n := strings.ToLower(f.AccountName)
	if t == "credit" || t == "credit card" {
		return true
	}
	if s == "credit card" || s == "credit" {
		return true
	}
	for _, substr := range []string{"credit", "card", "amex", "platinum"} {
		if strings.Contains(n, substr) {
			return true
		}
	}
	return false
}

// AmountScore captures every output of the amount-scoring rule.
type AmountScore struct {
	Score               int
	Mode                string   // 'exact' | 'surcharge'
	SurchargeFlag       bool
	ActualInvoiceAmount *float64
	SurchargePct        *float64
	Skip                bool // true = don't score this candidate at all
}

// scoreAmount applies the amount rule: exact match (100pts), or on a
// credit-card account only, a 0-3% surcharge pass (70pts). Anything else
// returns Skip=true so the caller filters the candidate out entirely.
func scoreAmount(txnAmount, docAmount float64, isCredit bool) AmountScore {
	absTxn := math.Abs(txnAmount)
	if docAmount <= 0 {
		return AmountScore{Skip: true}
	}
	// Float compare — tolerate 1-cent rounding.
	if math.Abs(absTxn-docAmount) < 0.005 {
		return AmountScore{Score: 100, Mode: "exact"}
	}
	if isCredit && absTxn > docAmount && absTxn <= docAmount*1.03 {
		actual := docAmount
		pct := round2((absTxn - docAmount) / docAmount * 100)
		return AmountScore{
			Score:               70,
			Mode:                "surcharge",
			SurchargeFlag:       true,
			ActualInvoiceAmount: &actual,
			SurchargePct:        &pct,
		}
	}
	return AmountScore{Skip: true}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// VendorScore captures vendor-scoring outputs.
type VendorScore struct {
	Score          int
	SimilarityPct  float64
	AliasMatchedID *int64
}

// AliasKey is a normalized alias pair — both strings lowercased + trimmed.
type AliasKey struct {
	Alias     string
	Canonical string
}

// scoreVendor applies the vendor rule: exact after normalization (100),
// contains either-way (85), Jaro-Winkler ≥ 0.85 (proportional), else 0.
// An alias-table hit lifts the floor to 85 and records which alias row
// confirmed the match.
func scoreVendor(txnStr, docStr string, aliasMap map[AliasKey]int64) VendorScore {
	if txnStr == "" || docStr == "" {
		return VendorScore{}
	}
	normTxn := normalizeVendor(txnStr)
	normDoc := normalizeVendor(docStr)
	if normTxn == "" || normDoc == "" {
		return VendorScore{}
	}

	var baseScore int
	var simPct float64
	switch {
	case normTxn == normDoc:
		baseScore, simPct = VendorExactScore, 100
	case vendorContainsEitherWay(normTxn, normDoc):
		baseScore, simPct = VendorContainsScore, VendorContainsScore
	default:
		sim := jaroWinkler(normTxn, normDoc)
		simPct = round2(sim * 100)
		if sim >= JaroWinklerThreshold {
			baseScore = int(sim * 100)
		}
	}

	// Alias lookup: checked both directions. A hit is a floor, not a replacement.
	var aliasID *int64
	if len(aliasMap) > 0 {
		if id, ok := aliasMap[AliasKey{Alias: normTxn, Canonical: normDoc}]; ok {
			aliasID = &id
		} else if id, ok := aliasMap[AliasKey{Alias: normDoc, Canonical: normTxn}]; ok {
			aliasID = &id
		}
		if aliasID != nil && baseScore < VendorContainsScore {
			baseScore = VendorContainsScore
		}
	}

	return VendorScore{
		Score:          baseScore,
		SimilarityPct:  simPct,
		AliasMatchedID: aliasID,
	}
}

// scoreDate awards (7 - daysApart) * 2, so 0 days = 14, 7 days = 0.
// Candidates outside the window are filtered at the DB layer.
func scoreDate(txnDate, docDate time.Time) (score int, daysApart int) {
	da := int(txnDate.Sub(docDate).Hours() / 24)
	if da < 0 {
		da = 0 // future-invoice case; query layer should prevent but be safe
	}
	if da > 7 {
		da = 7
	}
	return (7 - da) * 2, da
}

// ClassifyInput is the minimum shape classifyProposal needs — both the
// top candidate's score / flags and the runner-up's score.
type ClassifyInput struct {
	TopScore         int
	TopVendorScore   int
	TopSurchargeFlag bool
	TopAmountScore   int // used to distinguish surcharge(70) from exact(100)
	RunnerUpScore    *int
}

// classifyProposal assigns matched / suspect / ambiguous / unmatched.
// Rule order is load-bearing — surcharge must short-circuit BEFORE the
// clear-match check, otherwise a well-scored surcharge (e.g. 184 points
// with a clean vendor) gets auto-matched instead of yellow-flagged.
func classifyProposal(in ClassifyInput) string {
	top := in.TopScore

	// Rule 0: surcharge always flags yellow, regardless of absolute score —
	// a credit-card over-charge should never be auto-confirmed without a
	// human eyeballing that the surcharge pct matches the processor's rate.
	if in.TopSurchargeFlag && top >= 100 && in.TopVendorScore > 0 {
		return StatusSuspect
	}
	// Rule 1: clear match — score ≥ 150, vendor > 0, runner-up gap > 20.
	if top >= 150 && in.TopVendorScore > 0 {
		if in.RunnerUpScore == nil || top-*in.RunnerUpScore > 20 {
			return StatusMatched
		}
	}
	// Rule 2: ambiguous — two candidates within 20 points, top ≥ 100.
	if top >= 100 && in.RunnerUpScore != nil && top-*in.RunnerUpScore <= 20 {
		return StatusAmbiguous
	}
	// Rule 3: decent-but-not-confident.
	if top >= 100 {
		return StatusSuspect
	}
	return StatusUnmatched
}
