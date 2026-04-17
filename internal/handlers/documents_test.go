package handlers_test

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/testutil"
)

func setupDocumentsRouter(t *testing.T) (*gin.Engine, func()) {
	t.Helper()
	db := testutil.SetupDB(t)
	testutil.Truncate(t, db, "documents")
	h := &handlers.DocumentHandler{GormDB: db, Cfg: &config.Config{}}
	router := testutil.NewRouter(func(protected *gin.RouterGroup) {
		protected.POST("/documents/upload", h.Upload)
		protected.GET("/documents/summary", h.Summary)
	})
	cleanup := func() { testutil.Truncate(t, db, "documents") }
	return router, cleanup
}

// buildMultipart returns a request body + content-type for a multipart upload.
func buildMultipart(t *testing.T, field, filename string, content []byte) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	if field != "" {
		fw, err := w.CreateFormFile(field, filename)
		require.NoError(t, err)
		_, err = fw.Write(content)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return &buf, w.FormDataContentType()
}

func TestDocuments_Upload_MissingFileReturns400(t *testing.T) {
	router, cleanup := setupDocumentsRouter(t)
	defer cleanup()

	body, ct := buildMultipart(t, "", "", nil)
	req, err := http.NewRequest(http.MethodPost, "/documents/upload", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Authorization", "Bearer "+testutil.MakeJWT(t, "user-a"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "file is required")
}

func TestDocuments_Upload_UnsupportedExtReturns400(t *testing.T) {
	router, cleanup := setupDocumentsRouter(t)
	defer cleanup()

	body, ct := buildMultipart(t, "file", "notes.txt", []byte("hello"))
	req, err := http.NewRequest(http.MethodPost, "/documents/upload", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Authorization", "Bearer "+testutil.MakeJWT(t, "user-a"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "unsupported file type")
}

func TestDocuments_Upload_Unauthenticated(t *testing.T) {
	router, cleanup := setupDocumentsRouter(t)
	defer cleanup()

	body, ct := buildMultipart(t, "file", "invoice.pdf", []byte("%PDF-1.4"))
	req, err := http.NewRequest(http.MethodPost, "/documents/upload", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestDocuments_Summary_EmptyTable(t *testing.T) {
	router, cleanup := setupDocumentsRouter(t)
	defer cleanup()

	req := testutil.AuthedRequest(t, http.MethodGet, "/documents/summary", nil, "user-a")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Total     int `json:"total"`
		Pending   int `json:"pending"`
		Matched   int `json:"matched"`
		Unmatched int `json:"unmatched"`
	}
	testutil.DecodeJSON(t, w.Body, &resp)
	assert.Equal(t, 0, resp.Total)
	assert.Equal(t, 0, resp.Pending)
	assert.Equal(t, 0, resp.Matched)
	assert.Equal(t, 0, resp.Unmatched)
}

func TestDocuments_Summary_Unauthenticated(t *testing.T) {
	router, cleanup := setupDocumentsRouter(t)
	defer cleanup()

	req := testutil.UnauthenticatedRequest(t, http.MethodGet, "/documents/summary", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
