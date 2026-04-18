package sync

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// JobFunc is the shared shape for every pre-wired sync — once all the
// dependencies are closed over, a caller only needs a context and a
// SyncOpts to run the sync. The admin handler and the scheduler both
// register JobFuncs, so the wiring lives in exactly one place (below).
type JobFunc func(ctx context.Context, opts SyncOpts) (*SyncResult, error)

// NewScanPageRunner returns a JobFunc that runs SyncScanPages against the
// given *gorm.DB with the production Mongo cursor + Postgres writer.
func NewScanPageRunner(db *gorm.DB) JobFunc {
	return func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
		factory := func(ctx context.Context, wm time.Time, batch int) (mongodb.ScanPageSource, error) {
			mdb, err := mongodb.WickedFileDB()
			if err != nil {
				return nil, err
			}
			return mongodb.NewScanPageCursor(ctx, mdb, wm, int32(batch))
		}
		return SyncScanPages(ctx, &GormStateStore{DB: db}, factory, &PostgresDocumentWriter{DB: db}, opts)
	}
}

// NewStatementAuditRunner returns a JobFunc for SyncStatementAudits.
func NewStatementAuditRunner(db *gorm.DB) JobFunc {
	return func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
		factory := func(ctx context.Context, wm time.Time, batch int) (mongodb.StatementAuditSource, error) {
			mdb, err := mongodb.WickedFileDB()
			if err != nil {
				return nil, err
			}
			return mongodb.NewStatementAuditCursor(ctx, mdb, wm, int32(batch))
		}
		return SyncStatementAudits(ctx, &GormStateStore{DB: db}, factory, NewPostgresFKResolver(db), &PostgresMatchResultWriter{DB: db}, opts)
	}
}

// NewPartAuditRunner returns a JobFunc for SyncPartAudits.
func NewPartAuditRunner(db *gorm.DB) JobFunc {
	return func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
		factory := func(ctx context.Context, wm time.Time, batch int) (mongodb.PartAuditSource, error) {
			mdb, err := mongodb.WickedFileDB()
			if err != nil {
				return nil, err
			}
			return mongodb.NewPartAuditCursor(ctx, mdb, wm, int32(batch))
		}
		return SyncPartAudits(ctx, &GormStateStore{DB: db}, factory, &PostgresPartAuditFKResolver{DB: db}, &PostgresPartAuditWriter{DB: db}, opts)
	}
}

// NewPartMatchRunner returns a JobFunc for SyncPartMatches.
func NewPartMatchRunner(db *gorm.DB) JobFunc {
	return func(ctx context.Context, opts SyncOpts) (*SyncResult, error) {
		factory := func(ctx context.Context, wm time.Time, batch int) (mongodb.PartMatchSource, error) {
			mdb, err := mongodb.WickedFileDB()
			if err != nil {
				return nil, err
			}
			return mongodb.NewPartMatchCursor(ctx, mdb, wm, int32(batch))
		}
		return SyncPartMatches(ctx, &GormStateStore{DB: db}, factory, &PostgresPartMatchFKResolver{DB: db}, &PostgresPartMatchWriter{DB: db}, opts)
	}
}
