package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func CORS() gin.HandlerFunc {
	allowedOrigins := map[string]bool{
		"http://localhost:5173":                    true,
		"https://businessanalyticsinc.com":         true,
		"https://www.businessanalyticsinc.com":     true,
	}

	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		if allowedOrigins[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
		}

		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Header("Access-Control-Allow-Credentials", "true")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
