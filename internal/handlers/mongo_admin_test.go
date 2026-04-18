package handlers_test

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	syncpkg "github.com/ESCAutoGroupX/business-analytics-api/internal/sync"
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

// ── StartScanPageSync handler tests ─────────────────────────────

// syncHandler wires StartScanPageSync with an injected RunScanPage seam so
// tests never touch Mongo or Postgres.
func syncHandler(run handlers.SyncRunner) *handlers.MongoAdminHandler {
	return &handlers.MongoAdminHandler{RunScanPage: run}
}

func TestSyncScanPage_RequiresAuth(t *testing.T) {
	h := syncHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		t.Fatalf("run should not be called without auth")
		return nil, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/scanpage", h.StartScanPageSync)
	})
	req := testutil.UnauthenticatedRequest(t, "POST", "/admin/mongo/sync/scanpage", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestSyncScanPage_DryRunDefault(t *testing.T) {
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	h := syncHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		defer wg.Done()
		if !opts.DryRun {
			t.Errorf("opts.DryRun = false, want true by default")
		}
		<-release
		return &syncpkg.SyncResult{DryRun: true}, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/scanpage", h.StartScanPageSync)
	})
	req := testutil.AuthedRequest(t, "POST", "/admin/mongo/sync/scanpage", nil, "user-1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 202 {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Started bool `json:"started"`
		DryRun  bool `json:"dry_run"`
	}
	testutil.DecodeJSON(t, w.Body, &resp)
	if !resp.Started {
		t.Errorf("started = false, want true")
	}
	if !resp.DryRun {
		t.Errorf("dry_run = false, want true by default")
	}

	close(release)
	wg.Wait()
}

// ── StartStatementAuditSync handler tests ───────────────────────

func saHandler(run handlers.SyncRunner) *handlers.MongoAdminHandler {
	return &handlers.MongoAdminHandler{RunStatementAudit: run}
}

func TestSyncStatementAudit_RequiresAuth(t *testing.T) {
	h := saHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		t.Fatalf("run should not be called without auth")
		return nil, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/statementaudit", h.StartStatementAuditSync)
	})
	req := testutil.UnauthenticatedRequest(t, "POST", "/admin/mongo/sync/statementaudit", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestSyncStatementAudit_DryRunDefault(t *testing.T) {
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	h := saHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		defer wg.Done()
		if !opts.DryRun {
			t.Errorf("opts.DryRun = false, want true by default")
		}
		<-release
		return &syncpkg.SyncResult{DryRun: true}, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/statementaudit", h.StartStatementAuditSync)
	})
	req := testutil.AuthedRequest(t, "POST", "/admin/mongo/sync/statementaudit", nil, "user-1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 202 {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Started bool `json:"started"`
		DryRun  bool `json:"dry_run"`
	}
	testutil.DecodeJSON(t, w.Body, &resp)
	if !resp.Started || !resp.DryRun {
		t.Errorf("resp = %+v", resp)
	}
	close(release)
	wg.Wait()
}

func TestSyncStatementAudit_ReturnsConflictWhenRunning(t *testing.T) {
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	h := saHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		defer wg.Done()
		<-release
		return &syncpkg.SyncResult{}, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/statementaudit", h.StartStatementAuditSync)
	})
	req1 := testutil.AuthedRequest(t, "POST", "/admin/mongo/sync/statementaudit", nil, "user-1")
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	if w1.Code != 202 {
		t.Fatalf("first call: expected 202, got %d", w1.Code)
	}
	time.Sleep(20 * time.Millisecond)
	req2 := testutil.AuthedRequest(t, "POST", "/admin/mongo/sync/statementaudit", nil, "user-1")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != 409 {
		t.Fatalf("second call: expected 409, got %d: %s", w2.Code, w2.Body.String())
	}
	close(release)
	wg.Wait()
}

// ── StartPartAuditSync handler tests ────────────────────────────

func paHandler(run handlers.SyncRunner) *handlers.MongoAdminHandler {
	return &handlers.MongoAdminHandler{RunPartAudit: run}
}

func TestSyncPartAudit_RequiresAuth(t *testing.T) {
	h := paHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		t.Fatalf("run should not be called without auth")
		return nil, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/partaudit", h.StartPartAuditSync)
	})
	req := testutil.UnauthenticatedRequest(t, "POST", "/admin/mongo/sync/partaudit", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestSyncPartAudit_DryRunDefault(t *testing.T) {
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	h := paHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		defer wg.Done()
		if !opts.DryRun {
			t.Errorf("opts.DryRun = false, want true by default")
		}
		<-release
		return &syncpkg.SyncResult{DryRun: true}, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/partaudit", h.StartPartAuditSync)
	})
	req := testutil.AuthedRequest(t, "POST", "/admin/mongo/sync/partaudit", nil, "user-1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 202 {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	close(release)
	wg.Wait()
}

// ── StartPartMatchSync handler tests ────────────────────────────

func pmHandler(run handlers.SyncRunner) *handlers.MongoAdminHandler {
	return &handlers.MongoAdminHandler{RunPartMatch: run}
}

func TestSyncPartMatch_RequiresAuth(t *testing.T) {
	h := pmHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		t.Fatalf("run should not be called without auth")
		return nil, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/partmatch", h.StartPartMatchSync)
	})
	req := testutil.UnauthenticatedRequest(t, "POST", "/admin/mongo/sync/partmatch", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestSyncPartMatch_DryRunDefault(t *testing.T) {
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	h := pmHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		defer wg.Done()
		if !opts.DryRun {
			t.Errorf("opts.DryRun = false, want true by default")
		}
		<-release
		return &syncpkg.SyncResult{DryRun: true}, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/partmatch", h.StartPartMatchSync)
	})
	req := testutil.AuthedRequest(t, "POST", "/admin/mongo/sync/partmatch", nil, "user-1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 202 {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	close(release)
	wg.Wait()
}

func TestSyncScanPage_ReturnsConflictWhenRunning(t *testing.T) {
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	h := syncHandler(func(ctx context.Context, opts syncpkg.SyncOpts) (*syncpkg.SyncResult, error) {
		defer wg.Done()
		<-release
		return &syncpkg.SyncResult{}, nil
	})
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/mongo/sync/scanpage", h.StartScanPageSync)
	})

	req1 := testutil.AuthedRequest(t, "POST", "/admin/mongo/sync/scanpage", nil, "user-1")
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	if w1.Code != 202 {
		t.Fatalf("first call: expected 202, got %d", w1.Code)
	}

	time.Sleep(20 * time.Millisecond)

	req2 := testutil.AuthedRequest(t, "POST", "/admin/mongo/sync/scanpage", nil, "user-1")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != 409 {
		t.Fatalf("second call: expected 409, got %d: %s", w2.Code, w2.Body.String())
	}

	close(release)
	wg.Wait()
}
