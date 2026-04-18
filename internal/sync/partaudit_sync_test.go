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

type fakePartAuditSource struct {
	rows []*mongodb.PartAudit
	idx  int
	err  error
}

func (f *fakePartAuditSource) Next(ctx context.Context) bool { return f.idx < len(f.rows) }
func (f *fakePartAuditSource) Decode(out *mongodb.PartAudit) error {
	*out = *f.rows[f.idx]
	f.idx++
	return nil
}
func (f *fakePartAuditSource) Err() error                     { return f.err }
func (f *fakePartAuditSource) Close(ctx context.Context) error { return nil }

type fakePartAuditResolver struct{ calls int }

func (r *fakePartAuditResolver) Resolve(ctx context.Context, rows []PartAuditRow) ([]PartAuditRow, error) {
	r.calls++
	return rows, nil
}

type fakePartAuditWriter struct{ calls [][]PartAuditRow }

func (w *fakePartAuditWriter) UpsertBatch(ctx context.Context, rows []PartAuditRow) (int, int, error) {
	cpy := make([]PartAuditRow, len(rows))
	copy(cpy, rows)
	w.calls = append(w.calls, cpy)
	return len(rows), 0, nil
}

// ── mapping tests ───────────────────────────────────────────────

func mkPAObjID() primitive.ObjectID { return primitive.NewObjectID() }

func TestMapPartAuditToColumns_HappyPath(t *testing.T) {
	sp := mkPAObjID()
	ro := mkPAObjID()
	qty := 2.5
	up := 17.75
	amt := 44.375
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)
	pa := &mongodb.PartAudit{
		ID:                mkPAObjID(),
		ObjectID:          &sp,
		LocationID:        "loc-xyz",
		Type:              "invoice",
		Category:          "matched",
		ProductCode:       "ABC-123",
		ProductCodeSearch: "abc123",
		Description:       "Brake pad front",
		Quantity:          &qty,
		UnitPrice:         &up,
		Amount:            &amt,
		InvoiceID:         "INV-42",
		PurchaseOrder:     "PO-42",
		VendorName:        "Acme Parts",
		ROObjectID:        &ro,
		RONumber:          "RO-1",
		StartDate:         &start,
		EndDate:           &end,
		UpdatedAt:         time.Now().UTC(),
	}
	r := MapPartAudit(pa)

	if r.WfAuditID != pa.ID.Hex() {
		t.Errorf("WfAuditID = %q", r.WfAuditID)
	}
	if r.WfObjectID == nil || *r.WfObjectID != sp.Hex() {
		t.Errorf("WfObjectID = %v", r.WfObjectID)
	}
	if r.ROObjectID == nil || *r.ROObjectID != ro.Hex() {
		t.Errorf("ROObjectID = %v", r.ROObjectID)
	}
	if r.AuditType == nil || *r.AuditType != "invoice" {
		t.Errorf("AuditType = %v", r.AuditType)
	}
	if r.ProductCode == nil || *r.ProductCode != "ABC-123" {
		t.Errorf("ProductCode = %v", r.ProductCode)
	}
	if r.Quantity == nil || *r.Quantity != qty {
		t.Errorf("Quantity = %v", r.Quantity)
	}
	if r.StartDate == nil || !r.StartDate.Equal(start) {
		t.Errorf("StartDate = %v", r.StartDate)
	}
}

func TestMapPartAuditToColumns_NullableIDsBecomeEmpty(t *testing.T) {
	pa := &mongodb.PartAudit{
		ID:        mkPAObjID(),
		UpdatedAt: time.Now().UTC(),
	}
	r := MapPartAudit(pa)
	if r.WfObjectID != nil {
		t.Errorf("WfObjectID should be nil when ObjectID is nil, got %v", r.WfObjectID)
	}
	if r.ROObjectID != nil {
		t.Errorf("ROObjectID should be nil when ROObjectID is nil, got %v", r.ROObjectID)
	}
	if r.LocationID != nil {
		t.Errorf("LocationID empty string should become nil")
	}
	if r.AuditType != nil {
		t.Errorf("AuditType empty string should become nil")
	}
}

func TestMapPartAuditToColumns_Truncation(t *testing.T) {
	long := strings.Repeat("x", 300)
	pa := &mongodb.PartAudit{
		ID:          mkPAObjID(),
		ProductCode: long,
		VendorName:  long,
		RONumber:    long,
		UpdatedAt:   time.Now().UTC(),
	}
	r := MapPartAudit(pa)
	if r.ProductCode == nil || len(*r.ProductCode) != lenProductCode {
		t.Errorf("ProductCode not truncated to %d", lenProductCode)
	}
	if r.VendorName == nil || len(*r.VendorName) != lenVendorName {
		t.Errorf("VendorName not truncated to %d", lenVendorName)
	}
	if r.RONumber == nil || len(*r.RONumber) != lenRONumber {
		t.Errorf("RONumber not truncated to %d", lenRONumber)
	}
}

// ── engine tests ────────────────────────────────────────────────

func mkPA(updated time.Time) *mongodb.PartAudit {
	return &mongodb.PartAudit{
		ID:        mkPAObjID(),
		Type:      "invoice",
		UpdatedAt: updated,
	}
}

func TestSyncPartAudits_DryRunWritesNothing(t *testing.T) {
	rows := []*mongodb.PartAudit{mkPA(time.Now()), mkPA(time.Now())}
	resolver := &fakePartAuditResolver{}
	writer := &fakePartAuditWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.PartAuditSource, error) {
		return &fakePartAuditSource{rows: rows}, nil
	}
	res, err := SyncPartAudits(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: true, BatchSize: 10})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(writer.calls) != 0 {
		t.Errorf("writer called %d times on dry-run", len(writer.calls))
	}
	if resolver.calls != 0 {
		t.Errorf("resolver called %d times on dry-run", resolver.calls)
	}
	if res.Scanned != 2 {
		t.Errorf("Scanned = %d", res.Scanned)
	}
	if len(state.watermarks) != 0 {
		t.Errorf("watermark should not persist on dry-run")
	}
}

func TestSyncPartAudits_AdvancesWatermark(t *testing.T) {
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	rows := []*mongodb.PartAudit{
		mkPA(base),
		mkPA(base.Add(time.Minute)),
		mkPA(base.Add(2 * time.Minute)),
	}
	resolver := &fakePartAuditResolver{}
	writer := &fakePartAuditWriter{}
	state := &fakeState{}
	factory := func(ctx context.Context, wm time.Time, bs int) (mongodb.PartAuditSource, error) {
		return &fakePartAuditSource{rows: rows}, nil
	}
	res, err := SyncPartAudits(context.Background(), state, factory, resolver, writer,
		SyncOpts{DryRun: false, BatchSize: 2})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := base.Add(2 * time.Minute)
	if !res.HighWaterMark.Equal(want) {
		t.Errorf("HighWaterMark = %s, want %s", res.HighWaterMark, want)
	}
}
