package routes

import (
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/middleware"
)

func Register(r *gin.Engine, db *pgxpool.Pool, secretKey string) {
	authHandler := &handlers.AuthHandler{
		DB:        db,
		SecretKey: secretKey,
	}

	// Public routes
	r.GET("/health", handlers.Health)
	r.POST("/auth/signin", authHandler.SignIn)

	// Protected routes
	protected := r.Group("/")
	protected.Use(middleware.Auth(secretKey))
	{
		protected.GET("/dashboard/bank-balance", handlers.BankBalance)
	}
}
