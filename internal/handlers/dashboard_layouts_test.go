package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/testutil"
)

func setupDashboardLayoutsRouter(t *testing.T) (*gin.Engine, func()) {
	t.Helper()
	db := testutil.SetupDB(t)
	testutil.Truncate(t, db, "user_dashboard_layouts")
	h := &handlers.DashboardLayoutHandler{GormDB: db}
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.GET("/dashboard-layouts/:page", h.Get)
		protected.PUT("/dashboard-layouts/:page", h.Put)
		protected.DELETE("/dashboard-layouts/:page", h.Delete)
	})
	cleanup := func() { testutil.Truncate(t, db, "user_dashboard_layouts") }
	return router, cleanup
}

func TestDashboardLayouts_GetMissing_ReturnsNull(t *testing.T) {
	router, cleanup := setupDashboardLayoutsRouter(t)
	defer cleanup()

	req := testutil.AuthedRequest(t, http.MethodGet, "/dashboard-layouts/dashboard", nil, "user-a")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, "null", w.Body.String())
}

func TestDashboardLayouts_Unauthenticated(t *testing.T) {
	router, cleanup := setupDashboardLayoutsRouter(t)
	defer cleanup()

	req := testutil.UnauthenticatedRequest(t, http.MethodGet, "/dashboard-layouts/dashboard", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestDashboardLayouts_PutCreatesThenGetReturnsIt(t *testing.T) {
	router, cleanup := setupDashboardLayoutsRouter(t)
	defer cleanup()

	body := map[string]any{
		"layout": []map[string]any{
			{"i": "bank-balance", "x": 0, "y": 0, "w": 4, "h": 3},
		},
		"is_locked": false,
	}
	putReq := testutil.AuthedRequest(t, http.MethodPut, "/dashboard-layouts/dashboard", body, "user-a")
	putW := httptest.NewRecorder()
	router.ServeHTTP(putW, putReq)
	require.Equal(t, http.StatusOK, putW.Code, "put body=%s", putW.Body.String())

	getReq := testutil.AuthedRequest(t, http.MethodGet, "/dashboard-layouts/dashboard", nil, "user-a")
	getW := httptest.NewRecorder()
	router.ServeHTTP(getW, getReq)
	require.Equal(t, http.StatusOK, getW.Code)

	var resp struct {
		UserID   string          `json:"user_id"`
		Page     string          `json:"page"`
		Layout   json.RawMessage `json:"layout"`
		IsLocked bool            `json:"is_locked"`
	}
	testutil.DecodeJSON(t, getW.Body, &resp)
	assert.Equal(t, "user-a", resp.UserID)
	assert.Equal(t, "dashboard", resp.Page)
	assert.False(t, resp.IsLocked)
	assert.Contains(t, string(resp.Layout), "bank-balance")
}

func TestDashboardLayouts_PutRejectsEmptyLayout(t *testing.T) {
	router, cleanup := setupDashboardLayoutsRouter(t)
	defer cleanup()

	req := testutil.AuthedRequest(t, http.MethodPut, "/dashboard-layouts/dashboard", map[string]any{"is_locked": true}, "user-a")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestDashboardLayouts_PutIsolatesByUserAndPage(t *testing.T) {
	router, cleanup := setupDashboardLayoutsRouter(t)
	defer cleanup()

	bodyA := map[string]any{
		"layout":    []map[string]any{{"i": "w1", "x": 0, "y": 0, "w": 4, "h": 3}},
		"is_locked": true,
	}
	bodyB := map[string]any{
		"layout":    []map[string]any{{"i": "w2", "x": 0, "y": 0, "w": 6, "h": 4}},
		"is_locked": false,
	}

	for _, r := range []*http.Request{
		testutil.AuthedRequest(t, http.MethodPut, "/dashboard-layouts/dashboard", bodyA, "user-a"),
		testutil.AuthedRequest(t, http.MethodPut, "/dashboard-layouts/documents", bodyB, "user-a"),
		testutil.AuthedRequest(t, http.MethodPut, "/dashboard-layouts/dashboard", bodyB, "user-b"),
	} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		require.Equal(t, http.StatusOK, w.Code)
	}

	// user-a/dashboard should still hold bodyA (is_locked=true, contains w1)
	req := testutil.AuthedRequest(t, http.MethodGet, "/dashboard-layouts/dashboard", nil, "user-a")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Layout   json.RawMessage `json:"layout"`
		IsLocked bool            `json:"is_locked"`
	}
	testutil.DecodeJSON(t, w.Body, &resp)
	assert.True(t, resp.IsLocked)
	assert.Contains(t, string(resp.Layout), "w1")
}

func TestDashboardLayouts_DeleteRemoves(t *testing.T) {
	router, cleanup := setupDashboardLayoutsRouter(t)
	defer cleanup()

	body := map[string]any{
		"layout":    []map[string]any{{"i": "w1", "x": 0, "y": 0, "w": 4, "h": 3}},
		"is_locked": false,
	}
	putW := httptest.NewRecorder()
	router.ServeHTTP(putW, testutil.AuthedRequest(t, http.MethodPut, "/dashboard-layouts/dashboard", body, "user-a"))
	require.Equal(t, http.StatusOK, putW.Code)

	delW := httptest.NewRecorder()
	router.ServeHTTP(delW, testutil.AuthedRequest(t, http.MethodDelete, "/dashboard-layouts/dashboard", nil, "user-a"))
	require.Equal(t, http.StatusOK, delW.Code)

	getW := httptest.NewRecorder()
	router.ServeHTTP(getW, testutil.AuthedRequest(t, http.MethodGet, "/dashboard-layouts/dashboard", nil, "user-a"))
	require.Equal(t, http.StatusOK, getW.Code)
	assert.JSONEq(t, "null", getW.Body.String())
}
