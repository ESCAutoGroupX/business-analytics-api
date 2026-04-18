package sync

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// ── fakes ────────────────────────────────────────────────────────

type fakePartMatchSource struct {
	rows []*mongodb.PartMatch
	idx  int
	err  error
}

func (f *fakePartMatchSource) Next(ctx context.Context) bool { return f.idx < len(f.rows) }
func (f *fakePartMatchSource) Decode(out *mongodb.PartMatch) error {
	*out = *f.rows[f.idx]
	f.idx++
	return nil
}
func (f *fakePartMatchSource) Err() error                     { return f.err }
func (f *fakePartMatchSource) Close(ctx context.Context) error { return nil }

// tableDrivenPMResolver maps wf_audit_id hex → wf_part_audits.id. Calls
// is incremented on every batch to assert batch granularity.
type tableDrivenPMResolver struct {
	byHex map[string]int64
	calls int
}

func (r *tableDrivenPMResolver) Resolve(ctx context.Context, rows []PartMatchRow) ([]PartMatchRow, error) {
	r.calls++
	for i := range rows {
		if rows[i].WfInvoicePartAuditID != nil {
			if id, ok := r.byHex[*rows[i].WfInvoicePartAuditID]; ok {
				rows[i].InvoicePartAuditID = &id
			}
		}
		if rows[i].WfMatchPartAuditID != nil {
			if id, ok := r.byHex[*rows[i].WfMatchPartAuditID]; ok {
				rows[i].MatchPartAuditID = &id
			}
		}
	}
	return rows, nil
}

type fakePMWriter struct{ calls [][]PartMatchRow }

func (w *fakePMWriter) UpsertBatch(ctx context.Context, rows []PartMatchRow) (int, int, error) {
	cpy := make([]PartMatchRow, len(rows))
	copy(cpy, rows)
	w.calls = append(w.calls, cpy)
	return len(rows), 0, nil
}

// ── mapping tests ───────────────────────────────────────────────

func TestMapPartMatchToColumns_HappyPath(t *testing.T) {
	inv, mat := primitive.NewObjectID(), primitive.NewObjectID()
	score := 95
	invAmt, matAmt := 100.00, 98.50
	invQty, matQty := 1.0, 1.0
	pm := &mongodb.PartMatch{
		ID:                 primitive.NewObjectID(),
		RunID:              "run-1",
		InvoicePartAuditID: &inv,
		MatchPartAuditID:   &mat,
		InvoiceType:        "invoice",
		MatchType:          "rma",
		Score:              &score,
		Algo:               "exact-part",
		MatchedBy:          "system",
		InvoiceAmount:      &invAmt,
		MatchAmount:        &matAmt,
		InvoiceQuantity:    &invQty,
		MatchQuantity:      &matQty,
		UpdatedAt:          time.Now().UTC(),
	}
	r := MapPartMatch(pm)

	if r.WfMatchID != pm.ID.Hex() {
		t.Errorf("WfMatchID = %q", r.WfMatchID)
	}
	if r.WfInvoicePartAuditID == nil || *r.WfInvoicePartAuditID != inv.Hex() {
		t.Errorf("WfInvoicePartAuditID = %v", r.WfInvoicePartAuditID)
	}
	if r.WfMatchPartAuditID == nil || *r.WfMatchPartAuditID != mat.Hex() {
		t.Errorf("WfMatchPartAuditID = %v", r.WfMatchPartAuditID)
	}
	if r.Score == nil || *r.Score != 95 {
		t.Errorf("Score = %v", r.Score)
	}
	if r.RunID == nil || *r.RunID != "run-1" {
		t.Errorf("RunID = %v", r.RunID)
	}
	// FK fields should start unresolved — resolver fills them.
	if r.InvoicePartAuditID != nil || r.MatchPartAuditID != nil {
		t.Errorf("FK fields should start nil")
	}
}

func TestMapPartMatchToColumns_NullableIDs(t *testing.T) {
	pm := &mongodb.PartMatch{
		ID:        primitive.NewObjectID(),
		UpdatedAt: time.Now().UTC(),
	}
	r := MapPartMatch(pm)
	if r.WfInvoicePartAuditID != nil {
		t.Errorf("WfInvoicePartAuditID should be nil when InvoicePartAuditID is nil")
	}
	if r.WfMatchPartAuditID != nil {
		t.Errorf("WfMatchPartAuditID should be nil when MatchPartAuditID is nil")
	}
	if r.RunID != nil {
		t.Errorf("RunID empty string should become nil, got %v", r.RunID)
	}
}

func TestMapPartMatchToColumns_Truncation(t *testing.T) {
	long := strings.Repeat("z", 128)
	pm := &mongodb.PartMatch{
		ID:        primitive.NewObjectID(),
		Algo:      long,
		MatchedBy: long,
		RunID:     long,
		UpdatedAt: time.Now().UTC(),
	}
	r := MapPartMatch(pm)
	if r.Algo == nil || len(*r.Algo) != lenMatchAlgo {
		t.Errorf("Algo not truncated to %d", lenMatchAlgo)
	}
	if r.MatchedBy == nil || len(*r.MatchedBy) != lenMatchMatchedBy {
		t.Errorf("MatchedBy not truncated to %d", lenMatchMatchedBy)
	}
	if r.RunID == nil || len(*r.RunID) != lenMatchRunID {
		t.Errorf("RunID not truncated to %d", lenMatchRunID)
	}
}

// ── FK resolution / batch-granularity tests ─────────────────────

func TestPartAuditFKResolution_BatchLookup(t *testing.T) {
	// 30 partMatch records, batch size 10 → resolver called exactly 3 times.
	rows := make([]*mongodb.PartMatch, 30)
	for i := range rows {
		inv, mat := primitive.NewObjectID(), primitive.NewObjectID()
		rows[i] = &mongodb.PartMatch{
			ID:                 primitive.NewObjectID(),
			InvoicePartAuditID: &inv,
			MatchPartAuditID:   &mat,
			UpdatedAt:          time.Now().Add(time.Duration(i) * time.Second),
		}
	}
	resolver := &tableDrivenPMResolver{byHex: map[string]int64{}}
	writer := &fakePMWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.PartMatchSource, error) {
		return &fakePartMatchSource{rows: rows}, nil
	}
	if _, err := SyncPartMatches(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: false, BatchSize: 10}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if resolver.calls != 3 {
		t.Errorf("resolver.calls = %d, want 3 (batch-granular)", resolver.calls)
	}
}

func TestPartAuditFKResolution_MissingAuditLeavesNull(t *testing.T) {
	// Only one of the two referenced wf_audit_ids exists in the table.
	existsInvHex := primitive.NewObjectID()
	missingMatchHex := primitive.NewObjectID()
	var existsID int64 = 42

	pm := &mongodb.PartMatch{
		ID:                 primitive.NewObjectID(),
		InvoicePartAuditID: &existsInvHex,
		MatchPartAuditID:   &missingMatchHex,
		UpdatedAt:          time.Now(),
	}
	resolver := &tableDrivenPMResolver{byHex: map[string]int64{existsInvHex.Hex(): existsID}}
	writer := &fakePMWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.PartMatchSource, error) {
		return &fakePartMatchSource{rows: []*mongodb.PartMatch{pm}}, nil
	}
	if _, err := SyncPartMatches(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: false, BatchSize: 10}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(writer.calls) != 1 {
		t.Fatalf("writer.calls = %d, want 1", len(writer.calls))
	}
	row := writer.calls[0][0]
	if row.InvoicePartAuditID == nil || *row.InvoicePartAuditID != existsID {
		t.Errorf("InvoicePartAuditID should resolve to %d, got %v", existsID, row.InvoicePartAuditID)
	}
	if row.MatchPartAuditID != nil {
		t.Errorf("MatchPartAuditID should stay nil when not in wf_part_audits, got %v", row.MatchPartAuditID)
	}
}

// ── engine tests ────────────────────────────────────────────────

func mkPM(updated time.Time) *mongodb.PartMatch {
	return &mongodb.PartMatch{
		ID:        primitive.NewObjectID(),
		UpdatedAt: updated,
	}
}

func TestSyncPartMatches_DryRunWritesNothing(t *testing.T) {
	rows := []*mongodb.PartMatch{mkPM(time.Now()), mkPM(time.Now())}
	resolver := &tableDrivenPMResolver{byHex: map[string]int64{}}
	writer := &fakePMWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.PartMatchSource, error) {
		return &fakePartMatchSource{rows: rows}, nil
	}
	res, err := SyncPartMatches(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: true, BatchSize: 10})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(writer.calls) != 0 {
		t.Errorf("writer.calls = %d, want 0 on dry-run", len(writer.calls))
	}
	if resolver.calls != 0 {
		t.Errorf("resolver.calls = %d, want 0 on dry-run", resolver.calls)
	}
	if res.Scanned != 2 {
		t.Errorf("Scanned = %d", res.Scanned)
	}
	if len(state.watermarks) != 0 {
		t.Errorf("watermark should not persist on dry-run")
	}
}

func TestSyncPartMatches_AdvancesWatermark(t *testing.T) {
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	rows := []*mongodb.PartMatch{mkPM(base), mkPM(base.Add(time.Minute)), mkPM(base.Add(3 * time.Minute))}
	resolver := &tableDrivenPMResolver{byHex: map[string]int64{}}
	writer := &fakePMWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.PartMatchSource, error) {
		return &fakePartMatchSource{rows: rows}, nil
	}
	res, err := SyncPartMatches(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: false, BatchSize: 2})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := base.Add(3 * time.Minute)
	if !res.HighWaterMark.Equal(want) {
		t.Errorf("HighWaterMark = %s, want %s", res.HighWaterMark, want)
	}
}
