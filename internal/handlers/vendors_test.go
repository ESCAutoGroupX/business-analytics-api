package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/testutil"
)

func setupVendorsRouter(t *testing.T) (*gin.Engine, func()) {
	t.Helper()
	db := testutil.SetupDB(t)
	testutil.Truncate(t, db, "vendors")
	h := &handlers.VendorHandler{GormDB: db}
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.GET("/vendors", h.ListVendors)
		protected.GET("/vendors/lookup", h.LookupVendor)
	})
	cleanup := func() { testutil.Truncate(t, db, "vendors") }
	return router, cleanup
}

func TestVendors_ListEmpty(t *testing.T) {
	router, cleanup := setupVendorsRouter(t)
	defer cleanup()

	req := testutil.AuthedRequest(t, http.MethodGet, "/vendors", nil, "user-a")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, "[]", w.Body.String())
}

func TestVendors_ListReturnsSeeded(t *testing.T) {
	db := testutil.SetupDB(t)
	testutil.Truncate(t, db, "vendors")
	h := &handlers.VendorHandler{GormDB: db}
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.GET("/vendors", h.ListVendors)
	})

	// Seed two vendors directly via GORM
	require.NoError(t, db.Create(&models.Vendor{Name: "Worldpac"}).Error)
	require.NoError(t, db.Create(&models.Vendor{Name: "NAPA"}).Error)

	req := testutil.AuthedRequest(t, http.MethodGet, "/vendors", nil, "user-a")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp []map[string]any
	testutil.DecodeJSON(t, w.Body, &resp)
	assert.Len(t, resp, 2)

	testutil.Truncate(t, db, "vendors")
}

func TestVendors_Lookup_MissingNameReturns400(t *testing.T) {
	router, cleanup := setupVendorsRouter(t)
	defer cleanup()

	req := testutil.AuthedRequest(t, http.MethodGet, "/vendors/lookup", nil, "user-a")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "name query parameter required")
}

func TestVendors_Lookup_FoundByName(t *testing.T) {
	db := testutil.SetupDB(t)
	testutil.Truncate(t, db, "vendors")
	h := &handlers.VendorHandler{GormDB: db}
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.GET("/vendors/lookup", h.LookupVendor)
	})

	require.NoError(t, db.Create(&models.Vendor{Name: "Worldpac"}).Error)

	req := testutil.AuthedRequest(t, http.MethodGet, "/vendors/lookup?name=worldpac", nil, "user-a")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Found  bool           `json:"found"`
		Vendor map[string]any `json:"vendor"`
	}
	testutil.DecodeJSON(t, w.Body, &resp)
	assert.True(t, resp.Found)
	assert.Equal(t, "Worldpac", resp.Vendor["name"])

	testutil.Truncate(t, db, "vendors")
}

func TestVendors_Lookup_NotFoundReturnsFalseNotError(t *testing.T) {
	router, cleanup := setupVendorsRouter(t)
	defer cleanup()

	req := testutil.AuthedRequest(t, http.MethodGet, "/vendors/lookup?name=never-exists", nil, "user-a")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Found bool `json:"found"`
	}
	testutil.DecodeJSON(t, w.Body, &resp)
	assert.False(t, resp.Found)
}

func TestVendors_List_Unauthenticated(t *testing.T) {
	router, cleanup := setupVendorsRouter(t)
	defer cleanup()

	req := testutil.UnauthenticatedRequest(t, http.MethodGet, "/vendors", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
