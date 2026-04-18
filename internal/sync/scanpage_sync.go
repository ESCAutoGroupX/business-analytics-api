package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// ErrAlreadyRunning is returned when a sync for the same collection is
// already in flight. Handlers should surface this as 409 Conflict.
var ErrAlreadyRunning = errors.New("sync already running")

// StateStore persists the per-collection high-water mark and run metadata
// in mongo_sync_state. Abstracted out of gorm.DB so unit tests can run the
// engine in memory.
type StateStore interface {
	MarkRunStarted(ctx context.Context, collection string) error
	LoadWatermark(ctx context.Context, collection string) (time.Time, error)
	PersistWatermark(ctx context.Context, collection string, wm time.Time, batchRecords int) error
	MarkRunFinished(ctx context.Context, collection string) error
	MarkRunFailed(ctx context.Context, collection string, errMsg string) error
}

// GormStateStore is the production implementation backed by mongo_sync_state.
type GormStateStore struct{ DB *gorm.DB }

func (s *GormStateStore) MarkRunStarted(ctx context.Context, name string) error {
	res := s.DB.WithContext(ctx).Exec(
		`UPDATE mongo_sync_state
		    SET last_run_started_at = NOW(),
		        last_run_status = 'running',
		        last_run_error = NULL,
		        last_run_records = 0
		  WHERE collection_name = ?
		    AND (last_run_status IS DISTINCT FROM 'running')`, name,
	)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("%w: %s", ErrAlreadyRunning, name)
	}
	return nil
}

func (s *GormStateStore) LoadWatermark(ctx context.Context, name string) (time.Time, error) {
	var row struct{ LastSyncedUpdatedAt *time.Time }
	err := s.DB.WithContext(ctx).Raw(
		`SELECT last_synced_updated_at AS last_synced_updated_at
		   FROM mongo_sync_state WHERE collection_name = ?`, name,
	).Scan(&row).Error
	if err != nil || row.LastSyncedUpdatedAt == nil {
		return time.Time{}, err
	}
	return *row.LastSyncedUpdatedAt, nil
}

func (s *GormStateStore) PersistWatermark(ctx context.Context, name string, wm time.Time, batchRecords int) error {
	return s.DB.WithContext(ctx).Exec(
		`UPDATE mongo_sync_state
		    SET last_synced_updated_at = ?,
		        last_run_records = COALESCE(last_run_records, 0) + ?,
		        total_synced = COALESCE(total_synced, 0) + ?
		  WHERE collection_name = ?`,
		wm, batchRecords, batchRecords, name,
	).Error
}

func (s *GormStateStore) MarkRunFinished(ctx context.Context, name string) error {
	return s.DB.WithContext(ctx).Exec(
		`UPDATE mongo_sync_state
		    SET last_run_finished_at = NOW(),
		        last_run_status = 'success'
		  WHERE collection_name = ?`, name,
	).Error
}

func (s *GormStateStore) MarkRunFailed(ctx context.Context, name string, msg string) error {
	return s.DB.WithContext(ctx).Exec(
		`UPDATE mongo_sync_state
		    SET last_run_finished_at = NOW(),
		        last_run_status = 'failed',
		        last_run_error = ?
		  WHERE collection_name = ?`, msg, name,
	).Error
}

// SyncOpts controls a single run.
type SyncOpts struct {
	DryRun    bool
	BatchSize int // default 500
	Limit     int // 0 = no cap
}

// SyncResult captures what a run did. Safe to log at completion.
type SyncResult struct {
	Scanned       int           `json:"scanned"`
	Inserted      int           `json:"inserted"`
	Updated       int           `json:"updated"`
	Skipped       int           `json:"skipped"`
	Errors        int           `json:"errors"`
	FirstErrors   []string      `json:"first_errors,omitempty"`
	HighWaterMark time.Time     `json:"high_water_mark"`
	Elapsed       time.Duration `json:"elapsed"`
	DryRun        bool          `json:"dry_run"`
}

// The orchestrator calls a factory to build its source and writer. This
// lets tests inject an in-memory cursor without touching mongo-driver or
// Postgres.
type ScanPageSourceFactory func(ctx context.Context, watermark time.Time, batchSize int) (mongodb.ScanPageSource, error)

// SyncScanPages runs one sync pass against the provided seams. Watermark
// comes from the StateStore; a zero watermark means full-first-run.
// Progress is persisted per batch so a crash mid-run doesn't lose ground.
func SyncScanPages(
	ctx context.Context,
	state StateStore,
	newSource ScanPageSourceFactory,
	writer DocumentWriter,
	opts SyncOpts,
) (*SyncResult, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	start := time.Now()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	if err := state.MarkRunStarted(ctx, "scanPage"); err != nil {
		return nil, fmt.Errorf("mark run started: %w", err)
	}

	result := &SyncResult{DryRun: opts.DryRun}
	watermark, err := state.LoadWatermark(ctx, "scanPage")
	if err != nil {
		_ = state.MarkRunFailed(ctx, "scanPage", err.Error())
		return nil, err
	}
	result.HighWaterMark = watermark

	src, err := newSource(ctx, watermark, opts.BatchSize)
	if err != nil {
		_ = state.MarkRunFailed(ctx, "scanPage", err.Error())
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer src.Close(ctx)

	batch := make([]DocumentRow, 0, opts.BatchSize)
	batchWatermark := watermark
	batchIdx := 0
	samplesLogged := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if !opts.DryRun {
			ins, upd, ferr := writer.UpsertBatch(ctx, batch)
			if ferr != nil {
				return ferr
			}
			result.Inserted += ins
			result.Updated += upd
		}
		if batchWatermark.After(result.HighWaterMark) {
			result.HighWaterMark = batchWatermark
			if err := state.PersistWatermark(ctx, "scanPage", result.HighWaterMark, len(batch)); err != nil {
				return err
			}
		}
		batch = batch[:0]
		batchIdx++
		if batchIdx%10 == 0 {
			log.Printf("WF sync scanPage: batch %d complete, scanned=%d inserted=%d updated=%d watermark=%s",
				batchIdx, result.Scanned, result.Inserted, result.Updated, result.HighWaterMark.Format(time.RFC3339))
		}
		return nil
	}

	for src.Next(ctx) {
		var sp mongodb.ScanPage
		if err := src.Decode(&sp); err != nil {
			result.Errors++
			recordErr(result, fmt.Errorf("decode: %w", err))
			continue
		}
		result.Scanned++

		row := MapScanPage(&sp)

		if opts.DryRun && samplesLogged < 3 {
			if b, err := json.Marshal(row); err == nil {
				log.Printf("WF sync scanPage (dry-run sample %d): %s", samplesLogged+1, string(b))
			}
			samplesLogged++
		}

		batch = append(batch, row)
		if sp.UpdatedAt.After(batchWatermark) {
			batchWatermark = sp.UpdatedAt
		}

		if len(batch) >= opts.BatchSize {
			if err := flush(); err != nil {
				_ = state.MarkRunFailed(ctx, "scanPage", err.Error())
				return nil, err
			}
		}
		if opts.Limit > 0 && result.Scanned >= opts.Limit {
			break
		}
	}
	if err := src.Err(); err != nil {
		_ = state.MarkRunFailed(ctx, "scanPage", err.Error())
		return nil, fmt.Errorf("cursor err: %w", err)
	}
	if err := flush(); err != nil {
		_ = state.MarkRunFailed(ctx, "scanPage", err.Error())
		return nil, err
	}

	result.Elapsed = time.Since(start)
	if err := state.MarkRunFinished(ctx, "scanPage"); err != nil {
		log.Printf("WF sync scanPage: mark finished: %v", err)
	}
	return result, nil
}

// recordErr appends to FirstErrors with a hard cap of 20.
func recordErr(r *SyncResult, err error) {
	if len(r.FirstErrors) >= 20 {
		return
	}
	r.FirstErrors = append(r.FirstErrors, err.Error())
}

// ── mongo_sync_state helpers ────────────────────────────────────

func loadWatermark(ctx context.Context, db *gorm.DB, name string) (time.Time, error) {
	var row struct{ LastSyncedUpdatedAt *time.Time }
	err := db.WithContext(ctx).Raw(
		`SELECT last_synced_updated_at AS last_synced_updated_at
		   FROM mongo_sync_state WHERE collection_name = ?`, name,
	).Scan(&row).Error
	if err != nil {
		return time.Time{}, err
	}
	if row.LastSyncedUpdatedAt == nil {
		return time.Time{}, nil
	}
	return *row.LastSyncedUpdatedAt, nil
}

func markRunStarted(ctx context.Context, db *gorm.DB, name string) error {
	// Conflict out if a run is already in-flight so the caller can 409.
	res := db.WithContext(ctx).Exec(
		`UPDATE mongo_sync_state
		    SET last_run_started_at = NOW(),
		        last_run_status = 'running',
		        last_run_error = NULL,
		        last_run_records = 0
		  WHERE collection_name = ?
		    AND (last_run_status IS DISTINCT FROM 'running')`, name,
	)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("%w: %s", ErrAlreadyRunning, name)
	}
	return nil
}

func persistWatermark(ctx context.Context, db *gorm.DB, name string, wm time.Time, batchRecords int) error {
	return db.WithContext(ctx).Exec(
		`UPDATE mongo_sync_state
		    SET last_synced_updated_at = ?,
		        last_run_records = COALESCE(last_run_records, 0) + ?,
		        total_synced = COALESCE(total_synced, 0) + ?
		  WHERE collection_name = ?`,
		wm, batchRecords, batchRecords, name,
	).Error
}

func markRunFinished(ctx context.Context, db *gorm.DB, name string, r *SyncResult) error {
	return db.WithContext(ctx).Exec(
		`UPDATE mongo_sync_state
		    SET last_run_finished_at = NOW(),
		        last_run_status = 'success'
		  WHERE collection_name = ?`, name,
	).Error
}

func finishWithErr(ctx context.Context, db *gorm.DB, name string, r *SyncResult, err error) error {
	msg := err.Error()
	_ = db.WithContext(ctx).Exec(
		`UPDATE mongo_sync_state
		    SET last_run_finished_at = NOW(),
		        last_run_status = 'failed',
		        last_run_error = ?
		  WHERE collection_name = ?`, msg, name,
	).Error
	return err
}

