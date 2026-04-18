package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// ── fakes ────────────────────────────────────────────────────────

type fakeSource struct {
	pages []*mongodb.ScanPage
	idx   int
	err   error
}

func (f *fakeSource) Next(ctx context.Context) bool {
	return f.idx < len(f.pages)
}
func (f *fakeSource) Decode(out *mongodb.ScanPage) error {
	*out = *f.pages[f.idx]
	f.idx++
	return nil
}
func (f *fakeSource) Err() error                     { return f.err }
func (f *fakeSource) Close(ctx context.Context) error { return nil }

type fakeWriter struct {
	calls [][]DocumentRow
}

func (w *fakeWriter) UpsertBatch(ctx context.Context, rows []DocumentRow) (int, int, error) {
	cpy := make([]DocumentRow, len(rows))
	copy(cpy, rows)
	w.calls = append(w.calls, cpy)
	return len(rows), 0, nil
}

type fakeState struct {
	watermarks      []time.Time
	runStarted      bool
	runFinished     bool
	runFailedMsg    string
	initialMark     time.Time
}

func (s *fakeState) MarkRunStarted(ctx context.Context, name string) error {
	if s.runStarted {
		return ErrAlreadyRunning
	}
	s.runStarted = true
	return nil
}
func (s *fakeState) LoadWatermark(ctx context.Context, name string) (time.Time, error) {
	return s.initialMark, nil
}
func (s *fakeState) PersistWatermark(ctx context.Context, name string, wm time.Time, batch int) error {
	s.watermarks = append(s.watermarks, wm)
	return nil
}
func (s *fakeState) MarkRunFinished(ctx context.Context, name string) error {
	s.runFinished = true
	return nil
}
func (s *fakeState) MarkRunFailed(ctx context.Context, name string, msg string) error {
	s.runFailedMsg = msg
	return nil
}

// ── test helpers ─────────────────────────────────────────────────

func makePages(n int) []*mongodb.ScanPage {
	out := make([]*mongodb.ScanPage, n)
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		id := primitive.NewObjectID()
		updated := base.Add(time.Duration(i) * time.Minute)
		out[i] = &mongodb.ScanPage{
			ID:        id,
			Type:      "invoice",
			UpdatedAt: updated,
		}
	}
	return out
}

func makeFactory(src mongodb.ScanPageSource) ScanPageSourceFactory {
	return func(ctx context.Context, wm time.Time, bs int) (mongodb.ScanPageSource, error) {
		return src, nil
	}
}

// ── tests ────────────────────────────────────────────────────────

func TestSyncScanPages_DryRunWritesNothing(t *testing.T) {
	src := &fakeSource{pages: makePages(12)}
	writer := &fakeWriter{}
	state := &fakeState{}
	res, err := SyncScanPages(context.Background(), state, makeFactory(src),
		writer, SyncOpts{DryRun: true, BatchSize: 5})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(writer.calls) != 0 {
		t.Errorf("writer.calls = %d, want 0 (dry-run)", len(writer.calls))
	}
	if res.Scanned != 12 {
		t.Errorf("Scanned = %d, want 12", res.Scanned)
	}
	if res.Inserted != 0 || res.Updated != 0 {
		t.Errorf("inserted/updated should be 0 on dry run, got %d/%d", res.Inserted, res.Updated)
	}
	if !state.runFinished {
		t.Errorf("expected runFinished=true")
	}
}

func TestSyncScanPages_AdvancesWatermark(t *testing.T) {
	pages := makePages(7)
	src := &fakeSource{pages: pages}
	writer := &fakeWriter{}
	state := &fakeState{}
	res, err := SyncScanPages(context.Background(), state, makeFactory(src),
		writer, SyncOpts{DryRun: false, BatchSize: 3})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	want := pages[len(pages)-1].UpdatedAt
	if !res.HighWaterMark.Equal(want) {
		t.Errorf("HighWaterMark = %s, want %s", res.HighWaterMark, want)
	}
	if len(state.watermarks) == 0 {
		t.Errorf("state.watermarks is empty, expected at least 1 persisted")
	}
	if final := state.watermarks[len(state.watermarks)-1]; !final.Equal(want) {
		t.Errorf("final persisted watermark = %s, want %s", final, want)
	}
}

func TestSyncScanPages_RespectsLimit(t *testing.T) {
	src := &fakeSource{pages: makePages(50)}
	writer := &fakeWriter{}
	state := &fakeState{}
	res, err := SyncScanPages(context.Background(), state, makeFactory(src),
		writer, SyncOpts{DryRun: false, BatchSize: 10, Limit: 25})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Scanned != 25 {
		t.Errorf("Scanned = %d, want 25", res.Scanned)
	}
	totalWritten := 0
	for _, call := range writer.calls {
		totalWritten += len(call)
	}
	if totalWritten != 25 {
		t.Errorf("totalWritten = %d, want 25", totalWritten)
	}
}

func TestSyncScanPages_AlreadyRunning(t *testing.T) {
	src := &fakeSource{pages: makePages(3)}
	writer := &fakeWriter{}
	state := &fakeState{runStarted: true}
	_, err := SyncScanPages(context.Background(), state, makeFactory(src),
		writer, SyncOpts{DryRun: true, BatchSize: 10})
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Errorf("err = %v, want ErrAlreadyRunning", err)
	}
}
