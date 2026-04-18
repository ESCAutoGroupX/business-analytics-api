package handlers_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/testutil"
)

// mongoStatusRouter wires MongoStatus with an injected Count seam so tests
// don't need a live Mongo.
func mongoStatusRouter(count handlers.CountFunc) *gin.Engine {
	h := &handlers.MongoAdminHandler{Count: count}
	return testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.GET("/admin/mongo/status", h.MongoStatus)
	})
}

func TestMongoStatus_RequiresAuth(t *testing.T) {
	router := mongoStatusRouter(func(ctx context.Context, name string) (int64, error) {
		t.Fatalf("count should not be called without auth")
		return 0, nil
	})
	req := testutil.UnauthenticatedRequest(t, "GET", "/admin/mongo/status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMongoStatus_ReturnsShape(t *testing.T) {
	counts := map[string]int64{
		"scanPage":       330842,
		"statementAudit": 301056,
		"partAudit":      408908,
		"partMatch":      469720,
	}
	router := mongoStatusRouter(func(ctx context.Context, name string) (int64, error) {
		return counts[name], nil
	})
	req := testutil.AuthedRequest(t, "GET", "/admin/mongo/status", nil, "user-1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var payload struct {
		Connected   bool             `json:"connected"`
		DB          string           `json:"db"`
		LatencyMs   int64            `json:"latency_ms"`
		Collections map[string]int64 `json:"collections"`
	}
	testutil.DecodeJSON(t, w.Body, &payload)

	if !payload.Connected {
		t.Errorf("connected = false, want true")
	}
	if payload.DB == "" {
		t.Errorf("db is empty")
	}
	if payload.LatencyMs < 0 {
		t.Errorf("latency_ms = %d, want >= 0", payload.LatencyMs)
	}
	for _, want := range []string{"scanPage", "statementAudit", "partAudit", "partMatch"} {
		if _, ok := payload.Collections[want]; !ok {
			t.Errorf("collections missing %q", want)
		}
		if payload.Collections[want] != counts[want] {
			t.Errorf("collections[%q] = %d, want %d", want, payload.Collections[want], counts[want])
		}
	}
}
