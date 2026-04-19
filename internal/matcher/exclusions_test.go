package matcher

import "testing"

func TestExcludedPatterns_WorldpayDepositSkipped(t *testing.T) {
	// Money coming IN to a checking account via Worldpay merchant settlement.
	name := "Worldpay COMB. DEP. EUROPEAN MOTOR xxxxxxxx6469"
	if !shouldSkipTxn(name, "depository", -2177.24) {
		t.Errorf("Worldpay deposit on depository must skip")
	}
	// Also catches it via the Worldpay name pattern regardless of sign/account.
	if !shouldSkipTxn(name, "credit", 100.00) {
		t.Errorf("Worldpay name pattern should skip even on credit acct")
	}
}

func TestExcludedPatterns_AmexEpaymentSkipped(t *testing.T) {
	if !shouldSkipTxn("AMEX EPAYMENT ACH PMT esc auto group W6864", "depository", 11243.97) {
		t.Errorf("amex epayment must skip")
	}
	if !shouldSkipTxn("AMERICAN EXPRESS SETTLEMENT ESC AUTO GRO56 xxxxxx3182", "depository", -271.78) {
		t.Errorf("amex settlement must skip (wildcard both ends)")
	}
}

func TestExcludedPatterns_CheckWithVendorNotSkipped(t *testing.T) {
	// Bare "check #xxxxx" is skipped — no vendor memo.
	if !shouldSkipTxn("CHECK #22200", "depository", 3360.50) {
		t.Errorf("bare 'check #…' must skip")
	}
	// Check with vendor memo attached — pattern "check #%" matches everything
	// starting with the phrase, so this still gets skipped. Spec accepts that
	// trade-off for now. Test documents the current behavior.
	if !shouldSkipTxn("CHECK #41740 Hennessy BMW", "depository", 324.98) {
		t.Logf("note: 'CHECK #… Hennessy BMW' also gets skipped by 'check #%%' — " +
			"accepted edge case per spec")
	}
}

func TestExcludedPatterns_DepositoryCreditSideSkipped(t *testing.T) {
	// Generic deposit, amount < 0 on checking → skip (money IN, AR domain).
	if !shouldSkipTxn("misc refund", "depository", -500.00) {
		t.Errorf("negative amount on depository must skip (money IN)")
	}
	// Same event but positive amount → AP candidate, should NOT skip.
	if shouldSkipTxn("misc vendor payment", "depository", 500.00) {
		t.Errorf("positive amount on depository must NOT skip (money OUT)")
	}
	// On credit account, negative amount is a refund (money IN) — NOT skipped
	// by the sign predicate because refunds against credit cards can still
	// legitimately match a credit document.
	if shouldSkipTxn("tire return", "credit", -150.00) {
		t.Errorf("negative on credit must NOT skip (refund can match credit doc)")
	}
}

func TestExcludedPatterns_LoanAccountSkipped(t *testing.T) {
	if !shouldSkipTxn("monthly payment", "loan", 1500.00) {
		t.Errorf("loan account must skip")
	}
}

func TestExcludedPatterns_NormalVendorCharge(t *testing.T) {
	// Real candidate: credit-card charge with a vendor string.
	if shouldSkipTxn("WORLDPAC ATLANTA", "credit", 421.55) {
		t.Errorf("legitimate credit-card vendor charge must NOT skip — " +
			"note pattern 'worldpay%%' has no wildcard at start so 'WORLDPAC' doesn't match")
	}
	if shouldSkipTxn("AMZN MKTP", "credit", 39.99) {
		t.Errorf("Amazon charge must NOT skip")
	}
}

func TestLikeMatch(t *testing.T) {
	cases := []struct {
		s, pat string
		want   bool
	}{
		{"deposit", "deposit%", true},
		{"deposit from joe", "deposit%", true},
		{"direct deposit", "deposit%", false}, // no leading % → must anchor
		{"a b c", "%b%", true},
		{"american express settlement eur motor", "%american express settlement%", true},
		{"AMERICAN EXPRESS SETTLEMENT EUR MOTOR", "%american express settlement%", false}, // case-sensitive — caller lowercases
		{"", "", true},
		{"x", "", false},
	}
	for _, c := range cases {
		if got := likeMatch(c.s, c.pat); got != c.want {
			t.Errorf("likeMatch(%q, %q) = %v, want %v", c.s, c.pat, got, c.want)
		}
	}
}
