package handlers_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/testutil"
)

func TestMatcherPreview_RequiresAuth(t *testing.T) {
	h := handlers.NewMatcherAdminHandler(nil)
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/matcher/preview", h.StartPreview)
	})
	req := testutil.UnauthenticatedRequest(t, "POST", "/admin/matcher/preview", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestMatcherPreviewStatus_RequiresAuth(t *testing.T) {
	h := handlers.NewMatcherAdminHandler(nil)
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.GET("/admin/matcher/preview/status", h.PreviewStatus)
	})
	req := testutil.UnauthenticatedRequest(t, "GET", "/admin/matcher/preview/status?run_id=x", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestMatcherPreviewStatus_UnknownRunID(t *testing.T) {
	h := handlers.NewMatcherAdminHandler(nil)
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.GET("/admin/matcher/preview/status", h.PreviewStatus)
	})
	req := testutil.AuthedRequest(t, "GET", "/admin/matcher/preview/status?run_id=nope", nil, "user-1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMatcherPreviewStatus_MissingRunID(t *testing.T) {
	h := handlers.NewMatcherAdminHandler(nil)
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.GET("/admin/matcher/preview/status", h.PreviewStatus)
	})
	req := testutil.AuthedRequest(t, "GET", "/admin/matcher/preview/status", nil, "user-1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// TestMatcherPreview_BodyParsing checks that empty-body / malformed-body
// doesn't crash the handler — the goroutine it kicks off WILL try to
// use GormDB==nil and fail, but the HTTP-level response is 202 either way.
// We can't exercise the run completion here without a live DB; the
// runner's behavior is covered by the score tests + an e2e smoke test.
func TestMatcherPreview_EmptyBodyReturns202(t *testing.T) {
	// GormDB is nil; the background goroutine will panic when it tries to
	// query. The HTTP response should still be 202 and contain a run_id.
	h := handlers.NewMatcherAdminHandler(nil)
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/admin/matcher/preview", h.StartPreview)
	})
	req := testutil.AuthedRequest(t, "POST", "/admin/matcher/preview", nil, "user-1")
	req.Body = nil // empty body
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 202 {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	// Quick sanity: response should at least mention run_id.
	if !bytes.Contains(w.Body.Bytes(), []byte(`run_id`)) {
		t.Errorf("response missing run_id: %s", w.Body.String())
	}
}
