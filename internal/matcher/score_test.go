package matcher

import (
	"testing"
	"time"
)

// ── normalizeVendor ──────────────────────────────────────────────

func TestNormalizeVendor_StripsPunctuation(t *testing.T) {
	got := normalizeVendor("World-Pac, Inc.")
	if got != "world pac inc" {
		t.Errorf("normalizeVendor = %q, want %q", got, "world pac inc")
	}
}

func TestNormalizeVendor_CollapseWhitespace(t *testing.T) {
	got := normalizeVendor("  Global    Imports   BMW  ")
	if got != "global imports bmw" {
		t.Errorf("normalizeVendor = %q, want %q", got, "global imports bmw")
	}
}

// ── scoreAmount ──────────────────────────────────────────────────

func TestScoreAmount_Exact(t *testing.T) {
	r := scoreAmount(213.63, 213.63, false)
	if r.Skip {
		t.Fatalf("Skip=true on exact match")
	}
	if r.Score != 100 || r.Mode != "exact" || r.SurchargeFlag {
		t.Errorf("got score=%d mode=%q flag=%v; want 100 exact false", r.Score, r.Mode, r.SurchargeFlag)
	}
	if r.ActualInvoiceAmount != nil || r.SurchargePct != nil {
		t.Errorf("exact mode should leave actual/pct nil")
	}
}

func TestScoreAmount_SurchargeWithin3Pct(t *testing.T) {
	r := scoreAmount(103.00, 100.00, true)
	if r.Skip {
		t.Fatalf("Skip=true; should accept 3%% surcharge")
	}
	if r.Score != 70 || r.Mode != "surcharge" || !r.SurchargeFlag {
		t.Errorf("got score=%d mode=%q flag=%v; want 70 surcharge true", r.Score, r.Mode, r.SurchargeFlag)
	}
	if r.ActualInvoiceAmount == nil || *r.ActualInvoiceAmount != 100.00 {
		t.Errorf("ActualInvoiceAmount = %v, want 100", r.ActualInvoiceAmount)
	}
	if r.SurchargePct == nil || *r.SurchargePct != 3.00 {
		t.Errorf("SurchargePct = %v, want 3.00", r.SurchargePct)
	}
}

func TestScoreAmount_SurchargeOn4Pct_NotCandidate(t *testing.T) {
	r := scoreAmount(104.00, 100.00, true)
	if !r.Skip {
		t.Errorf("4%% surcharge should skip, got %+v", r)
	}
}

func TestScoreAmount_SurchargeOnDebitAccount_NotCandidate(t *testing.T) {
	r := scoreAmount(103.00, 100.00, false) // debit account, surcharge not allowed
	if !r.Skip {
		t.Errorf("non-credit account should not get surcharge pass, got %+v", r)
	}
}

func TestScoreAmount_TxnLessThanInvoice(t *testing.T) {
	// Credit account, but txn amount < invoice amount (we charge up, not down).
	r := scoreAmount(99.00, 100.00, true)
	if !r.Skip {
		t.Errorf("txn < invoice should skip, got %+v", r)
	}
}

// ── scoreVendor ──────────────────────────────────────────────────

func TestScoreVendor_ExactAfterNormalization(t *testing.T) {
	v := scoreVendor("World-Pac, Inc.", "worldpac inc", nil)
	// "world pac inc" vs "worldpac inc" — different tokenization → not exact,
	// but contains_either_way is also false since neither is substring of the
	// other. Should fall through to Jaro-Winkler ≥ 0.85.
	if v.Score < 85 || v.Score > 100 {
		t.Errorf("expected Jaro-Winkler-high score, got %d (sim=%.1f)", v.Score, v.SimilarityPct)
	}
}

func TestScoreVendor_ExactEqual(t *testing.T) {
	v := scoreVendor("FCP Euro", "FCP Euro", nil)
	if v.Score != 100 || v.SimilarityPct != 100 {
		t.Errorf("got score=%d sim=%.1f; want 100/100", v.Score, v.SimilarityPct)
	}
}

func TestScoreVendor_ContainsEitherWay(t *testing.T) {
	v := scoreVendor("WorldPac Atlanta", "WorldPac", nil)
	if v.Score != 85 {
		t.Errorf("got score=%d, want 85 (contains either way)", v.Score)
	}
}

func TestScoreVendor_JaroWinklerBelow85(t *testing.T) {
	v := scoreVendor("NAPA", "Walmart Supercenter", nil)
	if v.Score != 0 {
		t.Errorf("got score=%d, want 0 (below JW threshold)", v.Score)
	}
}

func TestScoreVendor_EmptyStrings(t *testing.T) {
	v := scoreVendor("", "FCP Euro", nil)
	if v.Score != 0 || v.SimilarityPct != 0 {
		t.Errorf("got score=%d sim=%.1f; want 0/0 for empty", v.Score, v.SimilarityPct)
	}
}

func TestScoreVendor_AliasLookup(t *testing.T) {
	// Production loadAliasMap normalizes both sides with normalizeVendor;
	// mirror that here so the in-memory lookup keys match what scoreVendor
	// actually compares against.
	aliases := map[AliasKey]int64{
		{Alias: normalizeVendor("AMZN MKTP"), Canonical: normalizeVendor("Amazon.com")}: 42,
	}
	v := scoreVendor("AMZN MKTP", "Amazon.com", aliases)
	if v.Score < 85 {
		t.Errorf("alias hit should raise score to >=85, got %d", v.Score)
	}
	if v.AliasMatchedID == nil || *v.AliasMatchedID != 42 {
		t.Errorf("AliasMatchedID = %v, want 42", v.AliasMatchedID)
	}
}

// ── scoreDate ────────────────────────────────────────────────────

func TestScoreDate_SameDay(t *testing.T) {
	d := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	score, days := scoreDate(d, d)
	if score != 14 || days != 0 {
		t.Errorf("same day: got score=%d days=%d; want 14 / 0", score, days)
	}
}

func TestScoreDate_7DaysPrior(t *testing.T) {
	txn := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	doc := txn.AddDate(0, 0, -7)
	score, days := scoreDate(txn, doc)
	if score != 0 || days != 7 {
		t.Errorf("7 days prior: got score=%d days=%d; want 0 / 7", score, days)
	}
}

func TestScoreDate_FutureInvoice_ClampedToZeroDays(t *testing.T) {
	// Query layer should filter these out, but the function must not panic
	// or produce negative days.
	txn := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	doc := txn.AddDate(0, 0, 3) // invoice in the future
	score, days := scoreDate(txn, doc)
	if days < 0 {
		t.Errorf("days = %d, want >= 0", days)
	}
	// Future dates get daysApart=0 in our clamp, so score = 14.
	if score < 0 || score > 14 {
		t.Errorf("score out of range: %d", score)
	}
}

// ── isCreditAccount ──────────────────────────────────────────────

func TestIsCreditAccount_ByType(t *testing.T) {
	if !isCreditAccount(CreditAccountFields{AccountType: "credit"}) {
		t.Errorf("account_type='credit' must be credit")
	}
}

func TestIsCreditAccount_ByNameAmex(t *testing.T) {
	if !isCreditAccount(CreditAccountFields{AccountName: "Business AMEX"}) {
		t.Errorf("AMEX account name must be credit")
	}
}

func TestIsCreditAccount_Checking(t *testing.T) {
	if isCreditAccount(CreditAccountFields{AccountType: "depository", AccountSubtype: "checking", AccountName: "Lifegreen Business Checking"}) {
		t.Errorf("checking account must not be credit")
	}
}

// ── classifyProposal ─────────────────────────────────────────────

func TestClassify_ClearMatch(t *testing.T) {
	rs := 120
	got := classifyProposal(ClassifyInput{TopScore: 214, TopVendorScore: 100, TopAmountScore: 100, RunnerUpScore: &rs})
	if got != StatusMatched {
		t.Errorf("got %q, want matched (gap > 20)", got)
	}
}

func TestClassify_SurchargeFlagged(t *testing.T) {
	got := classifyProposal(ClassifyInput{
		TopScore: 184, TopVendorScore: 100, TopAmountScore: 70, TopSurchargeFlag: true,
	})
	if got != StatusSuspect {
		t.Errorf("got %q, want suspect (surcharge)", got)
	}
}

func TestClassify_AmbiguousCloseSecond(t *testing.T) {
	rs := 200
	got := classifyProposal(ClassifyInput{TopScore: 210, TopVendorScore: 100, TopAmountScore: 100, RunnerUpScore: &rs})
	if got != StatusAmbiguous {
		t.Errorf("got %q, want ambiguous (gap <= 20 at score 210/200)", got)
	}
}

func TestClassify_NoVendorEvenIfAmount(t *testing.T) {
	rs := 0
	got := classifyProposal(ClassifyInput{TopScore: 114, TopVendorScore: 0, TopAmountScore: 100, RunnerUpScore: &rs})
	if got == StatusMatched {
		t.Errorf("no vendor match must never be 'matched'; got %q", got)
	}
}

func TestClassify_UnmatchedBelow100(t *testing.T) {
	got := classifyProposal(ClassifyInput{TopScore: 80, TopVendorScore: 0, TopAmountScore: 70})
	if got != StatusUnmatched {
		t.Errorf("got %q, want unmatched", got)
	}
}
