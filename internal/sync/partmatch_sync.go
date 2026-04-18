package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// PartMatchSourceFactory mirrors other source factories.
type PartMatchSourceFactory func(ctx context.Context, watermark time.Time, batchSize int) (mongodb.PartMatchSource, error)

// SyncPartMatches runs one pass over partMatch:
//   - cursor-sorts by updatedAt ASC from the prior watermark,
//   - resolves invoice_part_audit_id / match_part_audit_id via one batch
//     lookup into wf_part_audits,
//   - upserts on wf_match_id,
//   - advances the watermark per batch.
//
// The raw Mongo hex values stay in wf_invoice_part_audit_id /
// wf_match_part_audit_id so a partAudit that hasn't synced yet can still
// be back-resolved on a later run.
func SyncPartMatches(
	ctx context.Context,
	state StateStore,
	newSource PartMatchSourceFactory,
	resolver PartMatchFKResolver,
	writer PartMatchWriter,
	opts SyncOpts,
) (*SyncResult, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	start := time.Now()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	if err := state.MarkRunStarted(ctx, "partMatch"); err != nil {
		return nil, fmt.Errorf("mark run started: %w", err)
	}

	result := &SyncResult{DryRun: opts.DryRun}
	watermark, err := state.LoadWatermark(ctx, "partMatch")
	if err != nil {
		_ = state.MarkRunFailed(ctx, "partMatch", err.Error())
		return nil, err
	}
	result.HighWaterMark = watermark

	src, err := newSource(ctx, watermark, opts.BatchSize)
	if err != nil {
		_ = state.MarkRunFailed(ctx, "partMatch", err.Error())
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer src.Close(ctx)

	batch := make([]PartMatchRow, 0, opts.BatchSize)
	batchWatermark := watermark
	batchIdx := 0
	samplesLogged := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if !opts.DryRun {
			resolved, ferr := resolver.Resolve(ctx, batch)
			if ferr != nil {
				return ferr
			}
			ins, upd, werr := writer.UpsertBatch(ctx, resolved)
			if werr != nil {
				return werr
			}
			result.Inserted += ins
			result.Updated += upd
		}
		if batchWatermark.After(result.HighWaterMark) {
			result.HighWaterMark = batchWatermark
		}
		if !opts.DryRun {
			if err := state.PersistWatermark(ctx, "partMatch", result.HighWaterMark, len(batch)); err != nil {
				return err
			}
		}
		batchIdx++
		if batchIdx%10 == 0 {
			log.Printf("WF sync partMatch: batch %d complete, scanned=%d inserted=%d updated=%d watermark=%s",
				batchIdx, result.Scanned, result.Inserted, result.Updated, result.HighWaterMark.Format(time.RFC3339))
		}
		batch = batch[:0]
		return nil
	}

	for src.Next(ctx) {
		var pm mongodb.PartMatch
		if err := src.Decode(&pm); err != nil {
			result.Errors++
			recordErr(result, fmt.Errorf("decode: %w", err))
			continue
		}
		result.Scanned++
		row := MapPartMatch(&pm)

		if opts.DryRun && samplesLogged < 3 {
			if b, err := json.Marshal(row); err == nil {
				log.Printf("WF sync partMatch (dry-run sample %d): %s", samplesLogged+1, string(b))
			}
			samplesLogged++
		}

		batch = append(batch, row)
		if pm.UpdatedAt.After(batchWatermark) {
			batchWatermark = pm.UpdatedAt
		}

		if len(batch) >= opts.BatchSize {
			if err := flush(); err != nil {
				_ = state.MarkRunFailed(ctx, "partMatch", err.Error())
				return nil, err
			}
		}
		if opts.Limit > 0 && result.Scanned >= opts.Limit {
			break
		}
	}
	if err := src.Err(); err != nil {
		_ = state.MarkRunFailed(ctx, "partMatch", err.Error())
		return nil, fmt.Errorf("cursor err: %w", err)
	}
	if err := flush(); err != nil {
		_ = state.MarkRunFailed(ctx, "partMatch", err.Error())
		return nil, err
	}

	result.Elapsed = time.Since(start)
	if err := state.MarkRunFinished(ctx, "partMatch"); err != nil {
		log.Printf("WF sync partMatch: mark finished: %v", err)
	}
	return result, nil
}
