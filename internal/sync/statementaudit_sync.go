package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// StatementAuditSourceFactory mirrors ScanPageSourceFactory — lets tests
// supply an in-memory cursor without hitting Mongo.
type StatementAuditSourceFactory func(ctx context.Context, watermark time.Time, batchSize int) (mongodb.StatementAuditSource, error)

// SyncStatementAudits runs one pass over statementAudit:
//   - cursor-sorts by updatedAt ASC from the prior watermark,
//   - explodes each record's three match arrays into N wf_match_results rows,
//   - resolves documents / transactions FKs per batch,
//   - upserts on the composite (wf_audit_id, match_kind, match_index) key,
//   - advances the watermark per batch so a mid-run crash resumes cleanly.
//
// Dry-run logs the first 3 exploded rows as JSON samples and skips both
// the upsert and the watermark persist (same guard as scanPage).
func SyncStatementAudits(
	ctx context.Context,
	state StateStore,
	newSource StatementAuditSourceFactory,
	resolver FKResolver,
	writer MatchResultWriter,
	opts SyncOpts,
) (*SyncResult, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	start := time.Now()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	if err := state.MarkRunStarted(ctx, "statementAudit"); err != nil {
		return nil, fmt.Errorf("mark run started: %w", err)
	}

	result := &SyncResult{DryRun: opts.DryRun}
	watermark, err := state.LoadWatermark(ctx, "statementAudit")
	if err != nil {
		_ = state.MarkRunFailed(ctx, "statementAudit", err.Error())
		return nil, err
	}
	result.HighWaterMark = watermark

	src, err := newSource(ctx, watermark, opts.BatchSize)
	if err != nil {
		_ = state.MarkRunFailed(ctx, "statementAudit", err.Error())
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer src.Close(ctx)

	auditsInBatch := 0
	rowsInBatch := make([]MatchResultRow, 0, opts.BatchSize*2)
	batchWatermark := watermark
	batchIdx := 0
	samplesLogged := 0

	flush := func() error {
		if len(rowsInBatch) == 0 {
			return nil
		}
		if !opts.DryRun {
			resolved, ferr := resolver.Resolve(ctx, rowsInBatch)
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
			if err := state.PersistWatermark(ctx, "statementAudit", result.HighWaterMark, len(rowsInBatch)); err != nil {
				return err
			}
		}
		batchIdx++
		if batchIdx%10 == 0 {
			log.Printf("WF sync statementAudit: batch %d complete, audits=%d rows_out=%d inserted=%d updated=%d watermark=%s",
				batchIdx, result.Scanned, result.Inserted+result.Updated+len(rowsInBatch), result.Inserted, result.Updated,
				result.HighWaterMark.Format(time.RFC3339))
		}
		auditsInBatch = 0
		rowsInBatch = rowsInBatch[:0]
		return nil
	}

	for src.Next(ctx) {
		var a mongodb.StatementAudit
		if err := src.Decode(&a); err != nil {
			result.Errors++
			recordErr(result, fmt.Errorf("decode: %w", err))
			continue
		}
		result.Scanned++
		auditsInBatch++

		exploded := ExplodeAudit(&a)

		if opts.DryRun && samplesLogged < 3 {
			if b, err := json.Marshal(exploded); err == nil {
				log.Printf("WF sync statementAudit (dry-run sample %d, %d row(s)): %s",
					samplesLogged+1, len(exploded), string(b))
			}
			samplesLogged++
		}

		rowsInBatch = append(rowsInBatch, exploded...)
		if a.UpdatedAt.After(batchWatermark) {
			batchWatermark = a.UpdatedAt
		}

		if auditsInBatch >= opts.BatchSize {
			if err := flush(); err != nil {
				_ = state.MarkRunFailed(ctx, "statementAudit", err.Error())
				return nil, err
			}
		}
		if opts.Limit > 0 && result.Scanned >= opts.Limit {
			break
		}
	}
	if err := src.Err(); err != nil {
		_ = state.MarkRunFailed(ctx, "statementAudit", err.Error())
		return nil, fmt.Errorf("cursor err: %w", err)
	}
	if err := flush(); err != nil {
		_ = state.MarkRunFailed(ctx, "statementAudit", err.Error())
		return nil, err
	}

	result.Elapsed = time.Since(start)
	if err := state.MarkRunFinished(ctx, "statementAudit"); err != nil {
		log.Printf("WF sync statementAudit: mark finished: %v", err)
	}
	return result, nil
}
