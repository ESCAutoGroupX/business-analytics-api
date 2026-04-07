package main

import (
	"log"
	"net/http"

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

	routes.Register(r, gormDB, cfg.SecretKey, cfg)

	scheduler := xerocron.Start(gormDB, cfg)
	defer scheduler.Stop()

	// CORS wraps the Gin engine so headers are set before Gin's router,
	// ensuring redirects (trailing-slash 301s) also carry CORS headers.
	log.Printf("Server starting on port %s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, middleware.CORS(r)); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
