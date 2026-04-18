package sync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestScheduler_RegisterJobs(t *testing.T) {
	s := NewScheduler()
	s.RegisterJob("a", nopJob)
	s.RegisterJob("b", nopJob)
	s.RegisterJob("c", nopJob)

	got := s.Jobs()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("Jobs() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Jobs()[%d] = %q, want %q (order matters)", i, got[i], want[i])
		}
	}
}

func TestScheduler_RunsSequentially(t *testing.T) {
	var mu sync.Mutex
	var order []string

	record := func(name string) JobFunc {
		return func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
			mu.Lock()
			order = append(order, name+":start")
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			order = append(order, name+":end")
			mu.Unlock()
			return &SyncResult{}, nil
		}
	}

	s := NewScheduler()
	s.RegisterJob("one", record("one"))
	s.RegisterJob("two", record("two"))
	s.RegisterJob("three", record("three"))

	s.SyncAll(context.Background())

	want := []string{"one:start", "one:end", "two:start", "two:end", "three:start", "three:end"}
	if len(order) != len(want) {
		t.Fatalf("order len = %d, want %d: %v", len(order), len(want), order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("order[%d] = %q, want %q (jobs should not overlap)", i, order[i], want[i])
		}
	}
}

func TestScheduler_OneFailureDoesNotStopChain(t *testing.T) {
	var mu sync.Mutex
	var called []string

	ok := func(name string) JobFunc {
		return func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
			mu.Lock()
			called = append(called, name)
			mu.Unlock()
			return &SyncResult{}, nil
		}
	}
	bad := func(name string) JobFunc {
		return func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
			mu.Lock()
			called = append(called, name)
			mu.Unlock()
			return nil, errors.New("boom")
		}
	}

	s := NewScheduler()
	s.RegisterJob("first", ok("first"))
	s.RegisterJob("second", bad("second"))
	s.RegisterJob("third", ok("third"))

	s.SyncAll(context.Background())

	if len(called) != 3 {
		t.Errorf("called = %v, want all three", called)
	}
	if called[2] != "third" {
		t.Errorf("third job never ran — chain stopped after failure")
	}
}

func TestScheduler_RespectsPerJobTimeout(t *testing.T) {
	// Hook for tests: override the per-job timeout to something tiny.
	// Without this we'd have to sleep 20 minutes to see the timeout fire.
	// We instead assert the ctx passed into the job has a deadline.
	var ctxHadDeadline bool
	s := NewScheduler()
	s.RegisterJob("deadline-check", func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
		_, ok := ctx.Deadline()
		ctxHadDeadline = ok
		return &SyncResult{}, nil
	})
	s.SyncAll(context.Background())
	if !ctxHadDeadline {
		t.Errorf("job ctx has no deadline — per-job timeout not applied")
	}
}

func TestScheduler_StartIsIdempotent(t *testing.T) {
	s := NewScheduler()
	s.RegisterJob("noop", nopJob)
	if err := s.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("second Start should be no-op, got err: %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestScheduler_AlreadyRunningIsSkippedNotFailed(t *testing.T) {
	// If a job returns ErrAlreadyRunning (because a manual run has locked
	// that collection), the scheduler should log INFO and move on — the
	// chain must still complete.
	var mu sync.Mutex
	var called []string

	s := NewScheduler()
	s.RegisterJob("locked", func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
		mu.Lock()
		called = append(called, "locked")
		mu.Unlock()
		return nil, ErrAlreadyRunning
	})
	s.RegisterJob("after", func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
		mu.Lock()
		called = append(called, "after")
		mu.Unlock()
		return &SyncResult{}, nil
	})
	s.SyncAll(context.Background())

	if len(called) != 2 || called[0] != "locked" || called[1] != "after" {
		t.Errorf("ErrAlreadyRunning broke the chain: called=%v", called)
	}
}

func nopJob(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
	return &SyncResult{}, nil
}
