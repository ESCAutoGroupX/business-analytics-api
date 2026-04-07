package middleware

import (
	"net/http"
)

var allowedOrigins = map[string]bool{
	"http://localhost:5173":                 true,
	"https://businessanalyticsinc.com":      true,
	"https://www.businessanalyticsinc.com":  true,
	"https://d12s1zq4q2sj8a.cloudfront.net": true,
}

// CORS wraps an http.Handler so that CORS headers are set before Gin's
// router runs.  This ensures redirects (e.g. trailing-slash 301s) and
// 404s also carry the correct Access-Control-* headers.
func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		if allowedOrigins[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}

		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
