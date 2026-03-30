package main

import (
	"log"

	"github.com/gin-gonic/gin"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/database"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/middleware"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/routes"
)

func main() {
	cfg := config.Load()

	db, err := database.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	r := gin.Default()
	r.Use(middleware.CORS())

	routes.Register(r, db, cfg.SecretKey)

	log.Printf("Server starting on port %s", cfg.Port)
	if err := r.Run(":" + cfg.Port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
