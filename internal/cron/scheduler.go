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
func Start(gormDB *gorm.DB, cfg *config.Config) *cron.Cron {
	h := &handlers.XeroSyncHandler{GormDB: gormDB, Cfg: cfg}

	c := cron.New()

	// Every hour on the hour: bank transactions and invoices
	c.AddFunc("0 * * * *", wrapJob(h, "bank-transactions", h.SyncBankTransactions))
	c.AddFunc("0 * * * *", wrapJob(h, "invoices", h.SyncInvoices))

	// Every 4 hours: contacts, journals, payments
	c.AddFunc("0 */4 * * *", wrapJob(h, "contacts", h.SyncContacts))
	c.AddFunc("0 */4 * * *", wrapJob(h, "journals", h.SyncManualJournals))
	c.AddFunc("0 */4 * * *", wrapJob(h, "payments", h.SyncPayments))

	// Daily at 2am: full sync
	c.AddFunc("0 2 * * *", wrapSyncAll(h))

	c.Start()
	log.Println("Xero sync cron scheduler started")
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

		log.Printf("Cron %s: starting", name)
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

		log.Printf("Cron sync-all: starting")
		h.SyncAll(conn)
		log.Printf("Cron sync-all: completed")
	}
}
