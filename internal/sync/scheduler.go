package sync

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// CronSpec runs every hour at :15. :00 is avoided because the existing
// Xero and Plaid crons cluster there.
const CronSpec = "15 * * * *"

// ScheduledJob pairs a name with the JobFunc the scheduler invokes.
type ScheduledJob struct {
	Name string
	Fn   JobFunc
}

// Scheduler runs all four WickedFile syncs sequentially once per hour.
// The schedule is env-gated at main.go; once a Scheduler is constructed
// + started it always runs, but it's harmless if no jobs are registered.
type Scheduler struct {
	cron     *cron.Cron
	mu       sync.Mutex
	jobs     []ScheduledJob
	running  bool
	started  bool
	lastRun  time.Time
	entryID  cron.EntryID
}

// NewScheduler constructs a Scheduler with CronSpec prewired but no jobs.
// Register jobs before calling Start.
func NewScheduler() *Scheduler {
	return &Scheduler{
		cron: cron.New(),
	}
}

// RegisterJob appends a job. Order matters — SyncAll runs jobs in the
// order they were registered so FK-dependent syncs can be chained.
func (s *Scheduler) RegisterJob(name string, fn JobFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = append(s.jobs, ScheduledJob{Name: name, Fn: fn})
}

// Jobs returns the registered job names in order.
func (s *Scheduler) Jobs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.jobs))
	for i, j := range s.jobs {
		out[i] = j.Name
	}
	return out
}

// Start launches the cron scheduler. Idempotent — subsequent calls are no-ops.
func (s *Scheduler) Start() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.mu.Unlock()

	id, err := s.cron.AddFunc(CronSpec, func() {
		s.SyncAll(context.Background())
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.entryID = id
	s.mu.Unlock()
	s.cron.Start()
	return nil
}

// Stop gracefully stops the cron scheduler. Waits up to 5 minutes for any
// in-flight SyncAll to finish; returns an error if that timeout elapses.
func (s *Scheduler) Stop() error {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	ctx := s.cron.Stop()
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(5 * time.Minute):
		return errors.New("scheduler: in-flight job did not finish within 5 minutes")
	}
}

// NextRun reports when the next cron fire will happen.
func (s *Scheduler) NextRun() time.Time {
	s.mu.Lock()
	id := s.entryID
	s.mu.Unlock()
	if id == 0 {
		return time.Time{}
	}
	entry := s.cron.Entry(id)
	return entry.Next
}

// LastRun reports the timestamp of the most recent SyncAll invocation
// (successful or not). Zero value if it hasn't fired yet.
func (s *Scheduler) LastRun() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRun
}

// Running reports whether a SyncAll invocation is currently in flight.
func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// SyncAll runs every registered job sequentially with a 20-minute per-job
// timeout. Each job is wrapped in its own recover so one failure doesn't
// stop the chain. Safe to invoke manually for testing.
func (s *Scheduler) SyncAll(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		log.Printf("Scheduler: SyncAll requested while already running — skipping")
		return
	}
	s.running = true
	s.lastRun = time.Now()
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	log.Printf("Scheduler: starting SyncAll across %d job(s)", len(s.jobs))
	for _, job := range s.jobs {
		s.runJob(ctx, job)
	}
	log.Printf("Scheduler: SyncAll complete")
}

func (s *Scheduler) runJob(ctx context.Context, job ScheduledJob) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Scheduler: %s panicked: %v", job.Name, r)
		}
	}()

	jobCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()

	opts := SyncOpts{DryRun: false, BatchSize: 500, Limit: 0}
	start := time.Now()
	log.Printf("Scheduler: starting %s sync", job.Name)
	result, err := job.Fn(jobCtx, opts)
	if err != nil {
		// Another process (e.g. a manual /admin/mongo/sync/<name> call)
		// could be running the same collection. Log INFO, not ERROR, and
		// move on — the next hourly tick will catch what was missed.
		if errors.Is(err, ErrAlreadyRunning) {
			log.Printf("Scheduler: %s skipped — another run in progress", job.Name)
			return
		}
		log.Printf("Scheduler: %s FAILED after %v: %v", job.Name, time.Since(start), err)
		return
	}
	rows := 0
	if result != nil {
		rows = result.Inserted + result.Updated
	}
	log.Printf("Scheduler: %s done records=%d elapsed=%v", job.Name, rows, time.Since(start))
}
