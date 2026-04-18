package sync

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// ── fakes ────────────────────────────────────────────────────────

type fakeAuditSource struct {
	audits []*mongodb.StatementAudit
	idx    int
	err    error
}

func (f *fakeAuditSource) Next(ctx context.Context) bool {
	return f.idx < len(f.audits)
}
func (f *fakeAuditSource) Decode(out *mongodb.StatementAudit) error {
	*out = *f.audits[f.idx]
	f.idx++
	return nil
}
func (f *fakeAuditSource) Err() error                     { return f.err }
func (f *fakeAuditSource) Close(ctx context.Context) error { return nil }

// recordingResolver captures every Resolve call and returns rows unmodified
// so tests can assert batch granularity (no N+1).
type recordingResolver struct {
	calls [][]MatchResultRow
}

func (r *recordingResolver) Resolve(ctx context.Context, rows []MatchResultRow) ([]MatchResultRow, error) {
	cpy := make([]MatchResultRow, len(rows))
	copy(cpy, rows)
	r.calls = append(r.calls, cpy)
	return rows, nil
}

type fakeMatchWriter struct {
	calls [][]MatchResultRow
}

func (w *fakeMatchWriter) UpsertBatch(ctx context.Context, rows []MatchResultRow) (int, int, error) {
	cpy := make([]MatchResultRow, len(rows))
	copy(cpy, rows)
	w.calls = append(w.calls, cpy)
	return len(rows), 0, nil
}

// ── explode / mapping tests ─────────────────────────────────────

func mkObjID() primitive.ObjectID { return primitive.NewObjectID() }

func pctPtr(v float64) *float64 { return &v }

func mkAudit(typ, cat string, updatedAt time.Time) *mongodb.StatementAudit {
	scanID := mkObjID()
	return &mongodb.StatementAudit{
		ID:         mkObjID(),
		ScanPageID: &scanID,
		Type:       typ,
		Category:   cat,
		UpdatedAt:  updatedAt,
	}
}

func TestExplodeAudit_TransactionMatches(t *testing.T) {
	a := mkAudit("invoice", "matched", time.Now())
	a.TransactionMatches = []mongodb.TransactionMatchEntry{
		{ID: mkObjID(), Percentage: pctPtr(95), MatchedBy: "system", Match: &mongodb.TransactionMatchPayload{SourceTransactionID: "plaid-1"}},
		{ID: mkObjID(), Percentage: pctPtr(80), MatchedBy: "user", Match: &mongodb.TransactionMatchPayload{SourceTransactionID: "plaid-2"}},
	}
	rows := ExplodeAudit(a)
	if len(rows) != 2 {
		t.Fatalf("len=%d, want 2", len(rows))
	}
	for i, r := range rows {
		if r.MatchKind != MatchKindTransaction {
			t.Errorf("row %d kind=%q", i, r.MatchKind)
		}
		if r.MatchIndex != i {
			t.Errorf("row %d MatchIndex=%d, want %d", i, r.MatchIndex, i)
		}
		if r.WfSourceTxnID == nil {
			t.Errorf("row %d wf_source_txn_id nil", i)
		}
	}
}

func TestExplodeAudit_MixedMatches(t *testing.T) {
	a := mkAudit("invoice", "matched", time.Now())
	a.TransactionMatches = []mongodb.TransactionMatchEntry{
		{ID: mkObjID(), Percentage: pctPtr(95), Match: &mongodb.TransactionMatchPayload{SourceTransactionID: "plaid-1"}},
	}
	a.ScanMatches = []mongodb.ScanMatchEntry{
		{ID: mkObjID(), Percentage: pctPtr(88)},
		{ID: mkObjID(), Percentage: pctPtr(77)},
	}
	a.AccountingMatches = []mongodb.AccountingMatchEntry{
		{ID: mkObjID(), Percentage: pctPtr(60), Match: bson.M{"foo": 1}},
	}
	rows := ExplodeAudit(a)
	if len(rows) != 4 {
		t.Fatalf("len=%d, want 4 (1 tx + 2 scan + 1 acc)", len(rows))
	}
	kinds := []string{rows[0].MatchKind, rows[1].MatchKind, rows[2].MatchKind, rows[3].MatchKind}
	want := []string{MatchKindTransaction, MatchKindScan, MatchKindScan, MatchKindAccounting}
	for i := range want {
		if kinds[i] != want[i] {
			t.Errorf("row %d kind=%q want %q", i, kinds[i], want[i])
		}
	}
	if rows[0].MatchIndex != 0 || rows[1].MatchIndex != 0 || rows[2].MatchIndex != 1 {
		t.Errorf("MatchIndex should reset per kind; got %d/%d/%d", rows[0].MatchIndex, rows[1].MatchIndex, rows[2].MatchIndex)
	}
}

func TestExplodeAudit_NoMatches(t *testing.T) {
	a := mkAudit("credit", "matched", time.Now())
	rows := ExplodeAudit(a)
	if len(rows) != 1 {
		t.Fatalf("len=%d, want 1 (match_kind=none)", len(rows))
	}
	if rows[0].MatchKind != MatchKindNone {
		t.Errorf("kind=%q, want %q", rows[0].MatchKind, MatchKindNone)
	}
}

func TestMapAuditEntry_SourceTxnIdOnlyForTransactionKind(t *testing.T) {
	a := mkAudit("invoice", "matched", time.Now())
	a.TransactionMatches = []mongodb.TransactionMatchEntry{
		{ID: mkObjID(), Match: &mongodb.TransactionMatchPayload{SourceTransactionID: "plaid-1"}},
	}
	a.ScanMatches = []mongodb.ScanMatchEntry{
		{ID: mkObjID()},
	}
	rows := ExplodeAudit(a)
	if rows[0].WfSourceTxnID == nil || *rows[0].WfSourceTxnID != "plaid-1" {
		t.Errorf("transaction row wf_source_txn_id = %v, want plaid-1", rows[0].WfSourceTxnID)
	}
	if rows[1].WfSourceTxnID != nil {
		t.Errorf("scan row wf_source_txn_id should be nil, got %v", rows[1].WfSourceTxnID)
	}
}

func TestMapAuditEntry_MatchedScanIdOnlyForScanKind(t *testing.T) {
	a := mkAudit("invoice", "matched", time.Now())
	a.TransactionMatches = []mongodb.TransactionMatchEntry{{ID: mkObjID(), Match: &mongodb.TransactionMatchPayload{SourceTransactionID: "plaid-1"}}}
	a.ScanMatches = []mongodb.ScanMatchEntry{{ID: mkObjID()}}
	rows := ExplodeAudit(a)
	if rows[0].WfMatchedScanID != nil {
		t.Errorf("transaction row wf_matched_scan_id should be nil")
	}
	if rows[1].WfMatchedScanID == nil {
		t.Errorf("scan row wf_matched_scan_id must be set")
	}
}

func TestMapAuditEntry_PercentageAsConfidence(t *testing.T) {
	a := mkAudit("invoice", "matched", time.Now())
	a.TransactionMatches = []mongodb.TransactionMatchEntry{
		{ID: mkObjID(), Percentage: pctPtr(87.5), Match: &mongodb.TransactionMatchPayload{SourceTransactionID: "plaid-1"}},
	}
	rows := ExplodeAudit(a)
	if rows[0].Confidence == nil || *rows[0].Confidence != 87.5 {
		t.Errorf("confidence = %v, want 87.5 (stored as-is, not /100)", rows[0].Confidence)
	}
}

// ── engine tests ────────────────────────────────────────────────

func TestDocumentIdResolution_BatchLookup(t *testing.T) {
	// 30 audits, batch size 10 → resolver should get called exactly 3 times.
	audits := make([]*mongodb.StatementAudit, 30)
	for i := range audits {
		a := mkAudit("invoice", "matched", time.Now().Add(time.Duration(i)*time.Second))
		a.TransactionMatches = []mongodb.TransactionMatchEntry{
			{ID: mkObjID(), Percentage: pctPtr(90), Match: &mongodb.TransactionMatchPayload{SourceTransactionID: "plaid-" + primitive.NewObjectID().Hex()}},
		}
		audits[i] = a
	}
	src := &fakeAuditSource{audits: audits}
	resolver := &recordingResolver{}
	writer := &fakeMatchWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.StatementAuditSource, error) { return src, nil }

	if _, err := SyncStatementAudits(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: false, BatchSize: 10}); err != nil {
		t.Fatalf("sync err: %v", err)
	}
	if len(resolver.calls) != 3 {
		t.Errorf("resolver calls = %d, want 3 (batch-granular, not N+1)", len(resolver.calls))
	}
}

func TestSyncStatementAudits_DryRunWritesNothing(t *testing.T) {
	audits := []*mongodb.StatementAudit{
		mkAudit("invoice", "matched", time.Now()),
		mkAudit("credit", "matched", time.Now()),
	}
	resolver := &recordingResolver{}
	writer := &fakeMatchWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.StatementAuditSource, error) {
		return &fakeAuditSource{audits: audits}, nil
	}
	res, err := SyncStatementAudits(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: true, BatchSize: 10})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(writer.calls) != 0 {
		t.Errorf("writer.calls = %d, want 0 on dry-run", len(writer.calls))
	}
	if len(resolver.calls) != 0 {
		t.Errorf("resolver.calls = %d, want 0 on dry-run (no FK lookup)", len(resolver.calls))
	}
	if res.Scanned != 2 {
		t.Errorf("Scanned = %d, want 2", res.Scanned)
	}
	if len(state.watermarks) != 0 {
		t.Errorf("state.watermarks should be empty on dry-run, got %d", len(state.watermarks))
	}
}

func TestSyncStatementAudits_AdvancesWatermark(t *testing.T) {
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	audits := []*mongodb.StatementAudit{
		mkAudit("invoice", "matched", base),
		mkAudit("invoice", "matched", base.Add(5*time.Minute)),
		mkAudit("invoice", "matched", base.Add(10*time.Minute)),
	}
	resolver := &recordingResolver{}
	writer := &fakeMatchWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.StatementAuditSource, error) {
		return &fakeAuditSource{audits: audits}, nil
	}
	res, err := SyncStatementAudits(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: false, BatchSize: 2})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := base.Add(10 * time.Minute)
	if !res.HighWaterMark.Equal(want) {
		t.Errorf("HighWaterMark = %s, want %s", res.HighWaterMark, want)
	}
	if final := state.watermarks[len(state.watermarks)-1]; !final.Equal(want) {
		t.Errorf("persisted watermark = %s, want %s", final, want)
	}
}

func TestSyncStatementAudits_RespectsLimit(t *testing.T) {
	audits := make([]*mongodb.StatementAudit, 20)
	for i := range audits {
		audits[i] = mkAudit("invoice", "matched", time.Now().Add(time.Duration(i)*time.Second))
	}
	resolver := &recordingResolver{}
	writer := &fakeMatchWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.StatementAuditSource, error) {
		return &fakeAuditSource{audits: audits}, nil
	}
	res, err := SyncStatementAudits(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: false, BatchSize: 3, Limit: 5})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Scanned != 5 {
		t.Errorf("Scanned = %d, want 5", res.Scanned)
	}
}
