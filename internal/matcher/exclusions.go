package matcher

import "strings"

// ExcludedNamePatterns are case-insensitive LIKE patterns applied to
// transactions.name. Any transaction whose name matches one of these is
// skipped before scoring — these are operational cash-flow rows
// (settlements, ACH payroll, bank fees, checks with no vendor memo)
// that will never have an AP-invoice counterpart.
//
// Extend in place; eventually this list should live in a config table.
var ExcludedNamePatterns = []string{
	"worldpay%",                     // merchant-settlement deposits (AR, not AP)
	"amex epayment%",                // Amex statement payment
	"amex ach pmt%",
	"amex payment%",
	"%american express settlement%",
	"check #%",                      // checks with no vendor memo
	"deposit%",                      // bare deposits
	"zelle payment%",
	"wire transfer%",
	"intuit payroll%",
	"intuit pymt%",
	"paychex%",
	"gusto%",
	"atm withdrawal%",
	"overdraft%",
	"nsf fee%",
	"service charge%",
	"analysis fee%",
	"interest paid%",
	"interest earned%",
}

// ExcludedAccountTypes are account_type values that are skipped entirely
// regardless of name. Loans are a separate domain.
var ExcludedAccountTypes = []string{"loan"}

// shouldSkipTxn is the Go-side mirror of the SQL exclusion predicate.
// Used only by unit tests; production skipping happens at the query
// level. Kept parallel so either half can be verified in isolation.
//
// Sign convention (verified against prod): amount > 0 = money OUT,
// amount < 0 = money IN. Deposits into depository/checking are money
// IN and can't be AP-invoice payers.
func shouldSkipTxn(name, accountType string, amount float64) bool {
	accountLower := strings.ToLower(strings.TrimSpace(accountType))
	for _, ex := range ExcludedAccountTypes {
		if accountLower == ex {
			return true
		}
	}
	if amount < 0 && (accountLower == "depository" || accountLower == "checking") {
		return true
	}
	nameLower := strings.ToLower(strings.TrimSpace(name))
	for _, pat := range ExcludedNamePatterns {
		if likeMatch(nameLower, pat) {
			return true
		}
	}
	return false
}

// likeMatch is a tiny SQL-LIKE implementation (only % wildcard supported).
// Mirrors Postgres LIKE semantics closely enough for the patterns above.
func likeMatch(s, pattern string) bool {
	// Split pattern on % into literal chunks; each chunk must appear in s
	// in order. Leading/trailing empty chunks allow leading/trailing % to
	// match any prefix/suffix.
	if pattern == "" {
		return s == ""
	}
	parts := strings.Split(pattern, "%")
	pos := 0
	for i, part := range parts {
		if part == "" {
			// Empty part means % at start/end or adjacent %%.
			continue
		}
		idx := strings.Index(s[pos:], part)
		if idx == -1 {
			return false
		}
		// First literal chunk must anchor at start unless pattern led with %.
		if i == 0 && idx != 0 {
			return false
		}
		pos += idx + len(part)
	}
	// Last literal chunk must reach the end unless pattern ended with %.
	if !strings.HasSuffix(pattern, "%") && pos != len(s) {
		return false
	}
	return true
}
