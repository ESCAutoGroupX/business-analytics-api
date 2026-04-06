package main

import (
	"log"

	"github.com/gin-gonic/gin"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	xerocron "github.com/ESCAutoGroupX/business-analytics-api/internal/cron"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/database"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/middleware"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/routes"
)

func main() {
	cfg := config.Load()

	gormDB, err := database.ConnectGORM(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	r := gin.Default()
	r.Use(middleware.CORS())

	routes.Register(r, gormDB, cfg.SecretKey, cfg)

	scheduler := xerocron.Start(gormDB, cfg)
	defer scheduler.Stop()

	log.Printf("Server starting on port %s", cfg.Port)
	if err := r.Run(":" + cfg.Port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
