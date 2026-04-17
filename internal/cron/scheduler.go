package cron

import (
	"log"
	"runtime/debug"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/notifications"
)

// Start initializes and starts the Xero background sync scheduler.
// All incremental syncs only fetch records changed since the last successful sync.
func Start(gormDB *gorm.DB, cfg *config.Config) *cron.Cron {
	h := &handlers.XeroSyncHandler{GormDB: gormDB, Cfg: cfg}
	plaid := &handlers.PlaidHandler{GormDB: gormDB, Cfg: cfg}

	c := cron.New()

	// Every 30 minutes: bank transactions and invoices (incremental)
	c.AddFunc("*/30 * * * *", wrapJob(h, "bank-transactions", h.SyncBankTransactions))
	c.AddFunc("*/30 * * * *", wrapJob(h, "invoices", h.SyncInvoices))

	// Every 30 minutes: payments, contacts (incremental)
	c.AddFunc("*/30 * * * *", wrapJob(h, "payments", h.SyncPayments))
	c.AddFunc("*/30 * * * *", wrapJob(h, "contacts", h.SyncContacts))

	// Every 60 minutes: journals, tracking categories (at :00)
	c.AddFunc("0 * * * *", wrapJob(h, "journals", h.SyncManualJournals))
	c.AddFunc("0 * * * *", wrapJob(h, "tracking-categories", h.SyncTrackingCategories))

	// Accounts sync at :05 past each hour (staggered to avoid rate limit contention)
	c.AddFunc("5 * * * *", wrapJob(h, "accounts", h.SyncAccounts))

	// Daily at 2am: full resync — clears sync state and re-fetches everything
	c.AddFunc("0 2 * * *", wrapSyncAll(h))

	// Every 15 minutes: Plaid transaction sync (cursor-based)
	c.AddFunc("*/15 * * * *", wrapSimpleJob("plaid-tx-sync", plaid.SyncPlaidTransactions))

	// Daily balance snapshot at 11pm
	c.AddFunc("0 23 * * *", wrapSimpleJob("balance-snapshot", plaid.TakeDailyBalanceSnapshot))

	// Email alerts
	emailSender := &notifications.EmailSender{GormDB: gormDB, Cfg: cfg}
	c.AddFunc("0 7 * * *", wrapSimpleJob("alert-overdue-bills", emailSender.CheckOverdueBills))
	c.AddFunc("0 7 * * *", wrapSimpleJob("alert-low-balance", emailSender.CheckLowBankBalance))
	c.AddFunc("0 7 * * 1", wrapSimpleJob("alert-reconciliation", emailSender.CheckReconciliationAlert))

	// Document auto-match — run every 30 minutes
	docMatchHandler := &handlers.DocumentMatchHandler{GormDB: gormDB}
	c.AddFunc("*/30 * * * *", wrapSimpleJob("doc-auto-match", docMatchHandler.MatchDocumentsToTransactions))

	// Daily at 2:30am: extract customer numbers from new statement PDFs, then resolve locations
	c.AddFunc("30 2 * * *", wrapSimpleJob("customer-number-extract", func() {
		handlers.BackfillCustomerNumbersSync(gormDB, cfg)
	}))
	c.AddFunc("35 2 * * *", wrapSimpleJob("resolve-locations", func() {
		handlers.ResolveLocationsSync(gormDB, cfg)
	}))

	c.Start()
	log.Println("Cron scheduler started (sync + snapshots + alerts)")

	// Run balance snapshot immediately on startup
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in startup balance snapshot: %v", r)
			}
		}()
		log.Println("Running balance snapshot on startup...")
		plaid.TakeDailyBalanceSnapshot()
	}()

	return c
}

// wrapSimpleJob wraps a no-arg function for cron
func wrapSimpleJob(name string, fn func()) func() {
	return func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in cron job %s: %v\n%s", name, r, debug.Stack())
			}
		}()
		log.Printf("Cron %s: starting", name)
		fn()
		log.Printf("Cron %s: completed", name)
	}
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
