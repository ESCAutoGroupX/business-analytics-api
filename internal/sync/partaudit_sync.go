package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// PartAuditSourceFactory mirrors other source factories.
type PartAuditSourceFactory func(ctx context.Context, watermark time.Time, batchSize int) (mongodb.PartAuditSource, error)

// SyncPartAudits runs one pass over partAudit:
//   - cursor-sorts by updatedAt ASC from the prior watermark,
//   - resolves document_id FKs per batch via wf_object_id → documents.wf_scan_page_id,
//   - upserts on wf_audit_id,
//   - advances the watermark per batch for crash-safe resume.
//
// Dry-run guards are identical to scanPage / statementAudit.
func SyncPartAudits(
	ctx context.Context,
	state StateStore,
	newSource PartAuditSourceFactory,
	resolver PartAuditFKResolver,
	writer PartAuditWriter,
	opts SyncOpts,
) (*SyncResult, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	start := time.Now()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	if err := state.MarkRunStarted(ctx, "partAudit"); err != nil {
		return nil, fmt.Errorf("mark run started: %w", err)
	}

	result := &SyncResult{DryRun: opts.DryRun}
	watermark, err := state.LoadWatermark(ctx, "partAudit")
	if err != nil {
		_ = state.MarkRunFailed(ctx, "partAudit", err.Error())
		return nil, err
	}
	result.HighWaterMark = watermark

	src, err := newSource(ctx, watermark, opts.BatchSize)
	if err != nil {
		_ = state.MarkRunFailed(ctx, "partAudit", err.Error())
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer src.Close(ctx)

	batch := make([]PartAuditRow, 0, opts.BatchSize)
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
			if err := state.PersistWatermark(ctx, "partAudit", result.HighWaterMark, len(batch)); err != nil {
				return err
			}
		}
		batchIdx++
		if batchIdx%10 == 0 {
			log.Printf("WF sync partAudit: batch %d complete, scanned=%d inserted=%d updated=%d watermark=%s",
				batchIdx, result.Scanned, result.Inserted, result.Updated, result.HighWaterMark.Format(time.RFC3339))
		}
		batch = batch[:0]
		return nil
	}

	for src.Next(ctx) {
		var pa mongodb.PartAudit
		if err := src.Decode(&pa); err != nil {
			result.Errors++
			recordErr(result, fmt.Errorf("decode: %w", err))
			continue
		}
		result.Scanned++
		row := MapPartAudit(&pa)

		if opts.DryRun && samplesLogged < 3 {
			if b, err := json.Marshal(row); err == nil {
				log.Printf("WF sync partAudit (dry-run sample %d): %s", samplesLogged+1, string(b))
			}
			samplesLogged++
		}

		batch = append(batch, row)
		if pa.UpdatedAt.After(batchWatermark) {
			batchWatermark = pa.UpdatedAt
		}

		if len(batch) >= opts.BatchSize {
			if err := flush(); err != nil {
				_ = state.MarkRunFailed(ctx, "partAudit", err.Error())
				return nil, err
			}
		}
		if opts.Limit > 0 && result.Scanned >= opts.Limit {
			break
		}
	}
	if err := src.Err(); err != nil {
		_ = state.MarkRunFailed(ctx, "partAudit", err.Error())
		return nil, fmt.Errorf("cursor err: %w", err)
	}
	if err := flush(); err != nil {
		_ = state.MarkRunFailed(ctx, "partAudit", err.Error())
		return nil, err
	}

	result.Elapsed = time.Since(start)
	if err := state.MarkRunFinished(ctx, "partAudit"); err != nil {
		log.Printf("WF sync partAudit: mark finished: %v", err)
	}
	return result, nil
}
