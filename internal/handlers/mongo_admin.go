package handlers

import (
	"context"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/sync/errgroup"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// Collections counted by /admin/mongo/status.
var mongoStatusCollections = []string{"scanPage", "statementAudit", "partAudit", "partMatch"}

// CountFunc counts documents in a Mongo collection — behind an indirection so
// unit tests can inject a fake without a live Mongo.
type CountFunc func(ctx context.Context, collection string) (int64, error)

// MongoAdminHandler carries the seam used by MongoStatus.
type MongoAdminHandler struct {
	Count CountFunc
}

// NewMongoAdminHandler wires the production counter against WickedFileDB.
func NewMongoAdminHandler() *MongoAdminHandler {
	return &MongoAdminHandler{Count: defaultMongoCount}
}

func defaultMongoCount(ctx context.Context, name string) (int64, error) {
	db, err := mongodb.WickedFileDB()
	if err != nil {
		return 0, err
	}
	return db.Collection(name).CountDocuments(ctx, bson.D{})
}

// MongoStatus returns connectivity + document counts for the four primary
// WickedFile collections. Counts run in parallel with a 10s ctx per call.
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

func mongoDBName() string {
	if v := os.Getenv("WF_MONGO_DB"); v != "" {
		return v
	}
	return "a6fadc1b-c134-4cbb-b2a2-277f0595d7d6"
}
