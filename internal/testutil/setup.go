// Package testutil provides shared helpers for handler tests.
//
// Tests talk to a dedicated Postgres database (default: autoaccount_test) via
// the TEST_DATABASE_URL environment variable. When the variable is unset, the
// helpers mark the test as skipped so go test ./... stays green in environments
// without a test DB (CI without Postgres, laptops without local setup).
//
// Typical usage inside a _test.go file:
//
//	db := testutil.SetupDB(t)
//	testutil.Truncate(t, db, "user_dashboard_layouts")
//	router := testutil.NewRouter(db, func(r *gin.RouterGroup) {
//	    r.GET("/dashboard-layouts/:page", handler.Get)
//	})
//	req := testutil.AuthedRequest(t, "GET", "/dashboard-layouts/dashboard", nil, "user-1")
//	w := httptest.NewRecorder()
//	router.ServeHTTP(w, req)
package testutil

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/middleware"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

// TestSecret is the HMAC secret used for JWTs in tests. Arbitrary and
// non-sensitive — tests sign with it, middleware verifies with it.
const TestSecret = "test-secret-do-not-use-in-prod"

var migrationModels = []interface{}{
	&models.UserDashboardLayout{},
	&models.Vendor{},
	&models.Document{},
	&models.IntegrationSetting{},
}

// SetupDB connects to the test DB, runs migrations, and returns a *gorm.DB.
// If TEST_DATABASE_URL is unset, the calling test is skipped.
func SetupDB(t *testing.T) *gorm.DB {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping DB-backed test")
	}
	gin.SetMode(gin.TestMode)

	db, err := gorm.Open(postgres.Open(url), &gorm.Config{
		Logger:                                   gormlogger.Default.LogMode(gormlogger.Silent),
		SkipDefaultTransaction:                   true,
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	for _, m := range migrationModels {
		if err := db.AutoMigrate(m); err != nil {
			t.Fatalf("migrate %T: %v", m, err)
		}
	}
	return db
}

// Truncate wipes the listed tables. Safe no-op for tables that don't exist.
func Truncate(t *testing.T, db *gorm.DB, tables ...string) {
	t.Helper()
	for _, tbl := range tables {
		if err := db.Exec("TRUNCATE TABLE " + tbl + " RESTART IDENTITY CASCADE").Error; err != nil {
			t.Logf("truncate %s: %v (continuing)", tbl, err)
		}
	}
}

// NewRouter builds a gin engine with the real auth middleware applied to a
// protected group. Register routes via the provided callback. Tests must sign
// requests with TestSecret (use AuthedRequest).
func NewRouter(register func(protected *gin.RouterGroup)) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	protected := r.Group("/")
	protected.Use(middleware.Auth(TestSecret))
	register(protected)
	return r
}

// MakeJWT returns a valid HMAC-signed token for the given user id.
func MakeJWT(t *testing.T, userID string) string {
	t.Helper()
	claims := jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(TestSecret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

// AuthedRequest builds an *http.Request with JSON body (nil = no body) and a
// signed Bearer token for userID.
func AuthedRequest(t *testing.T, method, path string, body any, userID string) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, path, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+MakeJWT(t, userID))
	return req
}

// UnauthenticatedRequest builds a request with no Authorization header.
func UnauthenticatedRequest(t *testing.T, method, path string, body any) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, path, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

// DecodeJSON reads the response body as JSON into dst.
func DecodeJSON(t *testing.T, body io.Reader, dst any) {
	t.Helper()
	if err := json.NewDecoder(body).Decode(dst); err != nil {
		t.Fatalf("decode json: %v", err)
	}
}
