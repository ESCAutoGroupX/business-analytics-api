package cron

import (
	"log"
	"runtime/debug"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

// Start initializes and starts the Xero background sync scheduler.
// All incremental syncs only fetch records changed since the last successful sync.
func Start(gormDB *gorm.DB, cfg *config.Config) *cron.Cron {
	h := &handlers.XeroSyncHandler{GormDB: gormDB, Cfg: cfg}

	c := cron.New()

	// Every 15 minutes: bank transactions and invoices (incremental)
	c.AddFunc("*/15 * * * *", wrapJob(h, "bank-transactions", h.SyncBankTransactions))
	c.AddFunc("*/15 * * * *", wrapJob(h, "invoices", h.SyncInvoices))

	// Every 30 minutes: payments, contacts (incremental)
	c.AddFunc("*/30 * * * *", wrapJob(h, "payments", h.SyncPayments))
	c.AddFunc("*/30 * * * *", wrapJob(h, "contacts", h.SyncContacts))

	// Every 60 minutes: journals, tracking categories (incremental)
	c.AddFunc("0 * * * *", wrapJob(h, "journals", h.SyncManualJournals))
	c.AddFunc("0 * * * *", wrapJob(h, "tracking-categories", h.SyncTrackingCategories))

	// Daily at 2am: full resync — clears sync state and re-fetches everything
	c.AddFunc("0 2 * * *", wrapSyncAll(h))

	c.Start()
	log.Println("Xero sync cron scheduler started (incremental + rate-limited)")
	return c
}

func wrapJob(h *handlers.XeroSyncHandler, name string, fn func(*models.XeroConnection) error) func() {
	return func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in cron job %s: %v\n%s", name, r, debug.Stack())
			}
		}()

		conn, err := h.GetActiveConnection()
		if err != nil {
			log.Printf("Cron %s: skipping, no active connection: %v", name, err)
			return
		}

		log.Printf("Cron %s: starting (incremental)", name)
		if err := fn(conn); err != nil {
			log.Printf("Cron %s: failed: %v", name, err)
		} else {
			log.Printf("Cron %s: completed", name)
		}
	}
}

func wrapSyncAll(h *handlers.XeroSyncHandler) func() {
	return func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in cron job sync-all: %v\n%s", r, debug.Stack())
			}
		}()

		conn, err := h.GetActiveConnection()
		if err != nil {
			log.Printf("Cron sync-all: skipping, no active connection: %v", err)
			return
		}

		log.Printf("Cron sync-all: starting full resync")
		h.SyncAll(conn, true)
		log.Printf("Cron sync-all: completed")
	}
}
