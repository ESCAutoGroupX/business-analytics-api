package handlers

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
	syncpkg "github.com/ESCAutoGroupX/business-analytics-api/internal/sync"
)

// Collections counted by /admin/mongo/status.
var mongoStatusCollections = []string{"scanPage", "statementAudit", "partAudit", "partMatch"}

// CountFunc counts documents in a Mongo collection — behind an indirection so
// unit tests can inject a fake without a live Mongo.
type CountFunc func(ctx context.Context, collection string) (int64, error)

// SyncRunner kicks off a scanPage sync with the given options. Production
// wiring uses syncpkg.SyncScanPages against live Mongo + Postgres; tests
// inject a canned implementation.
type SyncRunner func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error)

// MongoAdminHandler carries every seam used by the admin endpoints in this
// file. Production code assembles a fully-wired instance in routes.Register;
// tests construct it directly with fakes.
type MongoAdminHandler struct {
	Count       CountFunc
	GormDB      *gorm.DB
	RunScanPage SyncRunner

	runMu   sync.Mutex
	running map[string]bool
}

// NewMongoAdminHandler wires the production counter + scanPage sync runner.
func NewMongoAdminHandler(db *gorm.DB) *MongoAdminHandler {
	h := &MongoAdminHandler{
		Count:   defaultMongoCount,
		GormDB:  db,
		running: map[string]bool{},
	}
	h.RunScanPage = func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		factory := func(ctx context.Context, wm time.Time, batch int) (mongodb.ScanPageSource, error) {
			mdb, err := mongodb.WickedFileDB()
			if err != nil {
				return nil, err
			}
			return mongodb.NewScanPageCursor(ctx, mdb, wm, int32(batch))
		}
		writer := &syncpkg.PostgresDocumentWriter{DB: db}
		state := &syncpkg.GormStateStore{DB: db}
		return syncpkg.SyncScanPages(ctx, state, factory, writer, opts)
	}
	return h
}

func defaultMongoCount(ctx context.Context, name string) (int64, error) {
	db, err := mongodb.WickedFileDB()
	if err != nil {
		return 0, err
	}
	return db.Collection(name).CountDocuments(ctx, bson.D{})
}

// GET /admin/mongo/status
func (h *MongoAdminHandler) MongoStatus(c *gin.Context) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	counts := make(map[string]int64, len(mongoStatusCollections))
	var mu sync.Mutex
	g, gCtx := errgroup.WithContext(ctx)
	for _, name := range mongoStatusCollections {
		name := name
		g.Go(func() error {
			n, err := h.Count(gCtx, name)
			if err != nil {
				return err
			}
			mu.Lock()
			counts[name] = n
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"connected":  false,
			"db":         mongoDBName(),
			"error":      err.Error(),
			"latency_ms": time.Since(start).Milliseconds(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"connected":   true,
		"db":          mongoDBName(),
		"latency_ms":  time.Since(start).Milliseconds(),
		"collections": counts,
	})
}

// POST /admin/mongo/sync/scanpage?dry_run=&limit=&batch_size=
//
// Dry-run defaults to true so accidental requests don't write 166K rows.
func (h *MongoAdminHandler) StartScanPageSync(c *gin.Context) {
	opts := syncpkg.SyncOpts{
		DryRun:    parseBoolDefault(c.Query("dry_run"), true),
		Limit:     parseIntDefault(c.Query("limit"), 0),
		BatchSize: parseIntDefault(c.Query("batch_size"), 500),
	}

	h.runMu.Lock()
	if h.running == nil {
		h.running = map[string]bool{}
	}
	if h.running["scanPage"] {
		h.runMu.Unlock()
		c.JSON(http.StatusConflict, gin.H{"error": "scanPage sync already running"})
		return
	}
	h.running["scanPage"] = true
	h.runMu.Unlock()

	runID := time.Now().UTC().Format("20060102T150405Z")
	go func() {
		// Detached context so the request-scoped ctx cancelling doesn't kill
		// the long-running sync.
		ctx := context.Background()
		defer func() {
			h.runMu.Lock()
			delete(h.running, "scanPage")
			h.runMu.Unlock()
			if r := recover(); r != nil {
				log.Printf("WF sync scanPage: panic: %v", r)
			}
		}()
		res, err := h.RunScanPage(ctx, opts)
		if err != nil {
			if errors.Is(err, syncpkg.ErrAlreadyRunning) {
				log.Printf("WF sync scanPage: db guard reports already running")
				return
			}
			log.Printf("WF sync scanPage: %v", err)
			return
		}
		log.Printf("WF sync scanPage: done run=%s scanned=%d inserted=%d updated=%d elapsed=%s",
			runID, res.Scanned, res.Inserted, res.Updated, res.Elapsed)
	}()

	c.JSON(http.StatusAccepted, gin.H{
		"started":    true,
		"dry_run":    opts.DryRun,
		"batch_size": opts.BatchSize,
		"limit":      opts.Limit,
		"run_id":     runID,
	})
}

// GET /admin/mongo/sync/status — returns all 4 rows of mongo_sync_state.
func (h *MongoAdminHandler) SyncStatus(c *gin.Context) {
	type row struct {
		CollectionName      string     `json:"collection_name"`
		LastSyncedUpdatedAt *time.Time `json:"last_synced_updated_at"`
		LastRunStartedAt    *time.Time `json:"last_run_started_at"`
		LastRunFinishedAt   *time.Time `json:"last_run_finished_at"`
		LastRunStatus       *string    `json:"last_run_status"`
		LastRunRecords      *int64     `json:"last_run_records"`
		LastRunError        *string    `json:"last_run_error"`
		TotalSynced         int64      `json:"total_synced"`
	}
	if h.GormDB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db not wired"})
		return
	}
	var rows []row
	err := h.GormDB.WithContext(c.Request.Context()).Raw(
		`SELECT collection_name, last_synced_updated_at, last_run_started_at,
		        last_run_finished_at, last_run_status, last_run_records,
		        last_run_error, COALESCE(total_synced, 0) AS total_synced
		   FROM mongo_sync_state
		  ORDER BY collection_name`,
	).Scan(&rows).Error
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"collections": rows})
}

func mongoDBName() string {
	if v := os.Getenv("WF_MONGO_DB"); v != "" {
		return v
	}
	return "a6fadc1b-c134-4cbb-b2a2-277f0595d7d6"
}

func parseBoolDefault(raw string, def bool) bool {
	if raw == "" {
		return def
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return b
}

