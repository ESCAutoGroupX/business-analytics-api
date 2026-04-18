package routes

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/handlers"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/middleware"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/notifications"
	syncpkg "github.com/ESCAutoGroupX/business-analytics-api/internal/sync"
)

func Register(r *gin.Engine, gormDB *gorm.DB, secretKey string, cfg *config.Config) {
	authHandler := &handlers.AuthHandler{
		GormDB:    gormDB,
		SecretKey: secretKey,
	}
	vendorHandler := &handlers.VendorHandler{GormDB: gormDB}
	permissionHandler := &handlers.PermissionHandler{GormDB: gormDB}
	assetHandler := &handlers.AssetHandler{GormDB: gormDB}
	userHandler := &handlers.UserHandler{GormDB: gormDB}
	plaidHandler := &handlers.PlaidHandler{GormDB: gormDB, Cfg: cfg}
	plaidHandler.AutoMigrate()
	cardAssignmentHandler := &handlers.CardAssignmentHandler{GormDB: gormDB}
	xeroHandler := &handlers.XeroHandler{GormDB: gormDB, Cfg: cfg}
	xeroSyncHandler := &handlers.XeroSyncHandler{GormDB: gormDB, Cfg: cfg}
	xeroAPIHandler := &handlers.XeroAPIHandler{GormDB: gormDB, Sync: xeroSyncHandler}
	assetImportHandler := &handlers.AssetImportHandler{GormDB: gormDB}
	assetAIHandler := &handlers.AssetAIHandler{GormDB: gormDB, Cfg: cfg}
	journalHandler := &handlers.JournalHandler{GormDB: gormDB}
	paymentMethodHandler := &handlers.PaymentMethodHandler{GormDB: gormDB}
	cardHandler := &handlers.CardHandler{GormDB: gormDB}
	twoFAHandler := &handlers.TwoFactorAuthHandler{GormDB: gormDB, SecretKey: secretKey}
	reportHandler := &handlers.ReportHandler{GormDB: gormDB}
	locationHandler := &handlers.LocationHandler{GormDB: gormDB}
	accountingHandler := &handlers.AccountingHandler{GormDB: gormDB}
	payrollHandler := &handlers.PayrollHandler{GormDB: gormDB}
	reconciliationHandler := &handlers.ReconciliationHandler{GormDB: gormDB}
	transactionHandler := &handlers.TransactionHandler{GormDB: gormDB}
	paybillHandler := &handlers.PayBillHandler{GormDB: gormDB}
	tekmetricHandler := &handlers.TekmetricHandler{GormDB: gormDB, Cfg: cfg}
	dashboardHandler := &handlers.DashboardHandler{GormDB: gormDB, Cfg: cfg}
	wfProxyHandler := handlers.NewWickedFileProxyHandler(gormDB, cfg)
	documentHandler := &handlers.DocumentHandler{GormDB: gormDB, Cfg: cfg, WFProxy: wfProxyHandler}
	apHandler := &handlers.APHandler{GormDB: gormDB}
	apHandler.AutoMigrate()
	matchingHandler := &handlers.MatchingHandler{GormDB: gormDB, Cfg: cfg}
	matchingHandler.AutoMigrate()
	receivablesHandler := &handlers.ReceivablesHandler{GormDB: gormDB, Cfg: cfg}
	receivablesHandler.AutoMigrate()
	docMatchHandler := &handlers.DocumentMatchHandler{GormDB: gormDB}
	docMatchHandler.AutoMigrate()
	bulkImportHandler := &handlers.BulkImportHandler{
		GormDB:   gormDB,
		Cfg:      cfg,
		Pipeline: documentHandler,
		Matcher:  docMatchHandler,
	}
	bulkImportHandler.AutoMigrate()
	dashboardLayoutHandler := &handlers.DashboardLayoutHandler{GormDB: gormDB}
	notificationHandler := &handlers.NotificationHandler{
		Email: &notifications.EmailSender{GormDB: gormDB, Cfg: cfg},
	}

	// Public routes
	r.GET("/health", handlers.Health)
	r.POST("/auth/signin", authHandler.SignIn)
	r.POST("/auth/signup", authHandler.Signup)
	r.POST("/auth/login", authHandler.Login)
	r.POST("/auth/login-direct", authHandler.LoginDirect)
	r.POST("/auth/forgot-password", authHandler.ForgotPassword)
	r.POST("/auth/reset-password", authHandler.ResetPassword)

	// Xero OAuth (public — no auth middleware, user initiates and Xero redirects back)
	xero := r.Group("/xero")
	{
		xero.GET("/authorize", xeroHandler.Authorize)
		xero.GET("/callback", xeroHandler.Callback)
	}

	// 2FA routes (public — used during login flow before token is issued)
	twoFA := r.Group("/2fa")
	{
		twoFA.POST("/mobile-otp", twoFAHandler.SendMobileOTP)
		twoFA.POST("/email-verification", twoFAHandler.SendEmailVerification)
		twoFA.POST("/authenticator-setup", twoFAHandler.SetupAuthenticator)
		twoFA.POST("/verify-otp", twoFAHandler.VerifyOTP)
		twoFA.POST("/resend-otp", twoFAHandler.ResendOTP)
	}

	// Protected routes
	protected := r.Group("/")
	protected.Use(middleware.Auth(secretKey))
	{
		// Dashboard
		dashboard := protected.Group("/dashboard")
		{
			dashboard.GET("/bank-balance", dashboardHandler.GetBankBalance)
			dashboard.GET("/payment-method", dashboardHandler.GetPaymentMethod)
			dashboard.GET("/credit-card-Balance-list", dashboardHandler.GetCreditCardBalanceList)
			dashboard.GET("/api/bank-balance-trans", dashboardHandler.GetBankBalanceTrans)
			dashboard.GET("/api/bank-balance-trends", dashboardHandler.GetBankBalanceTrends)
			dashboard.GET("/bank-ledger", dashboardHandler.GetBankLedger)
			dashboard.GET("/credit-card-ledger", dashboardHandler.GetCreditCardLedger)
			dashboard.GET("/vendor/:vendor_id/ledger", dashboardHandler.GetVendorLedger)
			dashboard.GET("/credit-card-balances-monthly", dashboardHandler.GetCreditCardBalancesMonthly)
			dashboard.GET("/credit-card-balances-weekly", dashboardHandler.GetCreditCardBalancesWeekly)
			dashboard.GET("/credit", dashboardHandler.GetCredit)
			dashboard.GET("/api/credit_cards_due_soon", dashboardHandler.GetCreditCardsDueSoon)
			dashboard.GET("/low-balance-account", dashboardHandler.GetLowBalanceAccount)
			dashboard.GET("/overdue-payables", dashboardHandler.GetOverduePayables)
			dashboard.GET("/accounts-payable", dashboardHandler.GetAccountsPayable)
			dashboard.GET("/items-need-attention", dashboardHandler.GetItemsNeedAttention)
			dashboard.GET("/accounts-payable-by-vendor", dashboardHandler.GetAccountsPayableByVendor)
			dashboard.GET("/account-receivable/dashboard", dashboardHandler.GetAccountReceivableDashboard)
			dashboard.GET("/account-receivable/summary", dashboardHandler.GetAccountReceivableSummary)
			dashboard.GET("/account-receivable/shop/:shop_id", dashboardHandler.GetAccountReceivableByShop)
			dashboard.GET("/account-receivable/aging_receivables", dashboardHandler.GetAgingReceivables)
			dashboard.GET("/account-receivable/graph", dashboardHandler.GetAccountReceivableGraph)
			dashboard.GET("/revenue-expenses", dashboardHandler.GetRevenueExpenses)
			dashboard.GET("/revenue-detail", dashboardHandler.GetRevenueDetail)
		}

		// Vendors
		vendors := protected.Group("/vendors")
		{
            vendors.POST("/import", vendorHandler.ImportVendors)
			vendors.POST("", vendorHandler.CreateVendor)
			vendors.GET("", vendorHandler.ListVendors)
			vendors.GET("/lookup", vendorHandler.LookupVendor)
			vendors.GET("/:vendor_id", vendorHandler.GetVendor)
			vendors.PATCH("/:vendor_id", vendorHandler.PatchVendor)
			vendors.DELETE("/:vendor_id", vendorHandler.DeleteVendor)
		}

		// Permissions & Roles
		perms := protected.Group("/api/permissions")
		{
			perms.POST("", permissionHandler.CreatePermission)
			perms.GET("", permissionHandler.GetAllPermissions)
			perms.POST("/roles", permissionHandler.CreateRole)
			perms.GET("/roles", permissionHandler.GetAllRoles)
			perms.GET("/roles/:role_id", permissionHandler.GetRole)
			perms.PUT("/roles/:role_id/permissions", permissionHandler.AssignPermissions)
		}

		// Assets
		assets := protected.Group("/assets")
		{
			assets.POST("", assetHandler.CreateAsset)
			assets.GET("", assetHandler.GetAllAssets)
			assets.GET("/:asset_id", assetHandler.GetAsset)
			assets.PATCH("/:asset_id", assetHandler.UpdateAsset)
			assets.DELETE("/:asset_id", assetHandler.DeleteAsset)
		}

		// Users
		users := protected.Group("/users")
		{
			users.GET("", userHandler.ListUsers)
			users.POST("", userHandler.CreateUser)
			users.GET("/me", userHandler.GetMyProfile)
			users.PATCH("/me", userHandler.EditMyProfile)
			users.POST("/me/change-password", userHandler.ChangePassword)
			users.GET("/:user_id", userHandler.GetUser)
			users.PATCH("/:user_id", userHandler.PatchUser)
			users.POST("/:user_id/reset-password", userHandler.ResetUserPassword)
			users.DELETE("/:user_id", userHandler.DeleteUser)
		}

		// Plaid
		plaid := protected.Group("/plaid")
		{
			plaid.POST("/exchange_public_token", plaidHandler.ExchangePublicToken)
			plaid.POST("/fetch_transactions", plaidHandler.FetchTransactions)
			plaid.GET("/sync_transactions", plaidHandler.SyncTransactions)
			plaid.POST("/link-token", plaidHandler.CreateLinkToken)
			plaid.GET("/items", plaidHandler.ListPlaidItems)
			plaid.DELETE("/items/:id", plaidHandler.DeletePlaidItem)
			plaid.POST("/sandbox/connect-bank", plaidHandler.SandboxConnectBank)
			plaid.POST("/import-csv", plaidHandler.ImportCSV)
			plaid.GET("/balance-history", plaidHandler.BalanceHistory)
			plaid.POST("/snapshot-now", plaidHandler.TriggerSnapshot)
		}

		// Notifications
		protected.POST("/notifications/test-email", notificationHandler.TestEmail)

		// Xero (authenticated)
		protected.GET("/xero/connections", xeroHandler.ListConnections)
		protected.DELETE("/xero/connections/:id", xeroHandler.DeleteConnection)
		protected.POST("/xero/refresh", xeroHandler.RefreshToken)

		// Xero Sync
		protected.POST("/xero/sync", xeroSyncHandler.TriggerSyncAll)
		protected.POST("/xero/sync/:endpoint", xeroSyncHandler.TriggerSyncEndpoint)
		protected.GET("/xero/sync-status", xeroSyncHandler.GetSyncStatus)

		// Xero Data API
		protected.GET("/xero/bank-transactions", xeroAPIHandler.ListBankTransactions)
		protected.GET("/xero/invoices", xeroAPIHandler.ListInvoices)
		protected.GET("/xero/contacts", xeroAPIHandler.ListContacts)
		protected.GET("/xero/payments", xeroAPIHandler.ListPayments)
		protected.GET("/xero/assets", xeroAPIHandler.ListAssets)
		protected.POST("/xero/assets/import-csv", assetImportHandler.ImportCSV)
		protected.POST("/xero/assets", assetImportHandler.CreateAsset)
		protected.POST("/xero/assets/ai-classify", assetAIHandler.ClassifyAssets)
		protected.POST("/xero/assets/calculate-depreciation", assetAIHandler.CalculateDepreciation)
		protected.PATCH("/xero/assets/:id", assetAIHandler.PatchAsset)
		protected.GET("/xero/asset-types", xeroAPIHandler.ListAssetTypes)
		protected.GET("/xero/accounts", xeroAPIHandler.ListAccounts)
		protected.GET("/xero/journals", xeroAPIHandler.ListJournals)
		protected.POST("/xero/journals", journalHandler.CreateJournal)
		protected.PATCH("/xero/journals/:id", journalHandler.UpdateJournal)
		protected.DELETE("/xero/journals/:id", journalHandler.DeleteJournal)
		protected.GET("/xero/tracking-categories", xeroAPIHandler.ListTrackingCategories)
		protected.GET("/xero/reports/:type", xeroAPIHandler.GetReport)
		protected.GET("/xero/match-transactions", xeroAPIHandler.MatchTransactions)
		protected.GET("/xero/reconciliation-summary", xeroAPIHandler.ReconciliationSummary)
		protected.POST("/xero/reconciliation-override", xeroAPIHandler.SaveReconciliationOverride)
		protected.GET("/xero/reconciliation-overrides", xeroAPIHandler.ListReconciliationOverrides)

		// Card Assignments
		cardAssignments := protected.Group("/card-assignments")
		{
			cardAssignments.GET("", cardAssignmentHandler.ListCardAssignments)
			cardAssignments.POST("", cardAssignmentHandler.CreateCardAssignment)
			cardAssignments.PUT("/:id", cardAssignmentHandler.UpdateCardAssignment)
			cardAssignments.DELETE("/:id", cardAssignmentHandler.DeleteCardAssignment)
		}

		// Payment Methods
		pm := protected.Group("/payment-methods")
		{
			pm.POST("", paymentMethodHandler.CreatePaymentMethod)
			pm.GET("", paymentMethodHandler.ListPaymentMethods)
			pm.GET("/:payment_method_id", paymentMethodHandler.GetPaymentMethod)
			pm.PATCH("/:payment_method_id", paymentMethodHandler.UpdatePaymentMethod)
			pm.DELETE("/:payment_method_id", paymentMethodHandler.DeletePaymentMethod)
		}

		// Cards
		cards := protected.Group("/cards")
		{
			cards.GET("/custom-cycle", cardHandler.GetCustomCycleCards)
			cards.POST("", cardHandler.CreateCard)
			cards.GET("", cardHandler.GetAllCards)
			cards.GET("/:card_id", cardHandler.GetCard)
			cards.PATCH("/:card_id", cardHandler.UpdateCard)
			cards.DELETE("/:card_id", cardHandler.DeleteCard)
		}

		// Reports
		reports := protected.Group("/reports")
		{
			reports.GET("/profit-loss", reportHandler.ProfitLossReport)
			reports.GET("/credit-card-summary", reportHandler.CreditCardSummary)
		}

		// Accounting (Chart of Accounts)
		accounting := protected.Group("/accounting")
		{
			accounting.POST("", accountingHandler.CreateAccount)
			accounting.GET("", accountingHandler.ListAccounts)
			accounting.POST("/import-accounts/", accountingHandler.ImportAccounts)
			accounting.GET("/:account_id", accountingHandler.GetAccount)
			accounting.PATCH("/:account_id", accountingHandler.UpdateAccount)
			accounting.DELETE("/:account_id", accountingHandler.DeleteAccount)
		}

		// Payroll
		payroll := protected.Group("/payroll")
		{
			payroll.POST("", payrollHandler.CreatePayroll)
			payroll.GET("", payrollHandler.GetAllPayrolls)
			payroll.POST("/adjustments", payrollHandler.CreateAdjustment)
			payroll.GET("/adjustments", payrollHandler.ListAdjustments)
			payroll.GET("/adjustments/:adjustment_id", payrollHandler.GetAdjustment)
			payroll.PATCH("/adjustments/:adjustment_id", payrollHandler.UpdateAdjustment)
			payroll.DELETE("/adjustments/:adjustment_id", payrollHandler.DeleteAdjustment)
			payroll.GET("/:payroll_id", payrollHandler.GetPayroll)
			payroll.PATCH("/:payroll_id", payrollHandler.UpdatePayroll)
			payroll.DELETE("/:payroll_id", payrollHandler.DeletePayroll)
		}

		// Reconciliation
		reconciliation := protected.Group("/api/reconciliation")
		{
			reconciliation.GET("/daily-match", reconciliationHandler.DailyMatch)
			reconciliation.GET("/deposit-detail", reconciliationHandler.DepositDetail)
		}

		// Transactions
		transactions := protected.Group("/transactions")
		{
			transactions.POST("", transactionHandler.CreateTransaction)
			transactions.GET("", transactionHandler.ListTransactions)
			transactions.POST("/import-data", transactionHandler.ImportData)
			transactions.POST("/reverse-change", transactionHandler.ReverseChange)
			transactions.PUT("/liability-minimum-balance/:liability_id", transactionHandler.UpdateLiabilityMinimumBalance)
			transactions.GET("/changes/:transaction_id", transactionHandler.ListTransactionChanges)
			transactions.GET("/:transaction_id", transactionHandler.GetTransaction)
			transactions.PATCH("/:transaction_id", transactionHandler.UpdateTransaction)
			transactions.DELETE("/:transaction_id", transactionHandler.DeleteTransaction)
			transactions.POST("/:transaction_id/upload-document", transactionHandler.UploadDocument)
		}

		// Admin
		matchingEngineHandler := &handlers.MatchingEngineHandler{GormDB: gormDB}
		custNumExtractor := &handlers.CustomerNumberExtractor{GormDB: gormDB, Cfg: cfg}
		visionExtractor := &handlers.VisionExtractor{GormDB: gormDB, Cfg: cfg}
		mongoAdminHandler := handlers.NewMongoAdminHandler(gormDB)
		// Hourly scheduler is env-gated so Bob can disable without a rebuild.
		// When enabled, wire all four WickedFile sync runners in FK-order and
		// start the cron immediately.
		if os.Getenv("SYNC_SCHEDULE_ENABLED") == "true" {
			sched := syncpkg.NewScheduler()
			sched.RegisterJob("scanPage", syncpkg.NewScanPageRunner(gormDB))
			sched.RegisterJob("statementAudit", syncpkg.NewStatementAuditRunner(gormDB))
			sched.RegisterJob("partAudit", syncpkg.NewPartAuditRunner(gormDB))
			sched.RegisterJob("partMatch", syncpkg.NewPartMatchRunner(gormDB))
			if err := sched.Start(); err != nil {
				log.Printf("Scheduler: start failed: %v", err)
			} else {
				log.Printf("Scheduler: enabled, running at :15 each hour")
				mongoAdminHandler.Scheduler = sched
			}
		} else {
			log.Printf("Scheduler: disabled (set SYNC_SCHEDULE_ENABLED=true to enable)")
		}
		admin := protected.Group("/admin")
		{
			admin.GET("/mongo/status", mongoAdminHandler.MongoStatus)
			admin.POST("/mongo/sync/scanpage", mongoAdminHandler.StartScanPageSync)
			admin.POST("/mongo/sync/statementaudit", mongoAdminHandler.StartStatementAuditSync)
			admin.POST("/mongo/sync/partaudit", mongoAdminHandler.StartPartAuditSync)
			admin.POST("/mongo/sync/partmatch", mongoAdminHandler.StartPartMatchSync)
			admin.GET("/mongo/sync/status", mongoAdminHandler.SyncStatus)
			admin.GET("/mongo/sync/schedule", mongoAdminHandler.SyncScheduleStatus)
			admin.POST("/mongo/sync/schedule/trigger", mongoAdminHandler.TriggerSchedule)
			admin.POST("/migrate-plaid-transactions", transactionHandler.MigratePlaidTransactions)
			admin.POST("/run-matching", matchingEngineHandler.RunMatching)
			admin.GET("/match-stats", matchingEngineHandler.MatchStats)
			admin.GET("/statement-completeness", matchingEngineHandler.StatementCompleteness)
			admin.POST("/vendor-location-accounts", matchingEngineHandler.CreateVendorLocationAccount)
			admin.GET("/vendor-location-accounts", matchingEngineHandler.ListVendorLocationAccounts)
			admin.POST("/extract-customer-numbers", custNumExtractor.StartExtraction)
			admin.GET("/extract-customer-numbers/status", custNumExtractor.GetExtractionStatus)
			admin.POST("/extract-customer-numbers/assign/:doc_id", custNumExtractor.AssignCustomerNumber)
			admin.POST("/resolve-locations-from-customer-numbers", custNumExtractor.ResolveLocations)
			admin.GET("/customer-number-stats", custNumExtractor.CustomerNumberStats)
			admin.POST("/vision-extract-statements", visionExtractor.StartVisionExtraction)
			admin.GET("/vision-extract-statements/status", visionExtractor.GetVisionExtractionStatus)
			admin.POST("/vision-extract-statements/test/:doc_id", visionExtractor.TestVisionSingleDoc)
			admin.POST("/vision-extract-amounts", visionExtractor.ExtractAmounts)
			admin.GET("/missing-pages-review", matchingEngineHandler.MissingPagesReview)
			admin.POST("/statements/:group_id/force-match", matchingEngineHandler.ForceMatchStatement)
		}

		// PayBills
		paybills := protected.Group("/paybills")
		{
			paybills.POST("", paybillHandler.CreatePayBill)
			paybills.GET("", paybillHandler.ListPayBills)
			paybills.POST("/schedule-payment/", paybillHandler.CreateSchedulePayment)
			paybills.GET("/schedule-payment/", paybillHandler.ListSchedulePayments)
			paybills.GET("/schedule-payment/:schedule_payment_id", paybillHandler.GetSchedulePayment)
			paybills.PATCH("/schedule-payment/:schedule_payment_id", paybillHandler.UpdateSchedulePayment)
			paybills.DELETE("/schedule-payment/:schedule_payment_id", paybillHandler.DeleteSchedulePayment)
			paybills.GET("/reminders/", paybillHandler.ListReminders)
			paybills.PATCH("/reminders/:reminder_id/acknowledge", paybillHandler.AcknowledgeReminder)
			paybills.POST("/manual-bills/", paybillHandler.CreateManualBill)
			paybills.GET("/manual-bills/", paybillHandler.ListManualBills)
			paybills.GET("/manual-bills/:bill_id", paybillHandler.GetManualBill)
			paybills.PATCH("/manual-bills/:bill_id", paybillHandler.UpdateManualBill)
			paybills.DELETE("/manual-bills/:bill_id", paybillHandler.DeleteManualBill)
			paybills.GET("/:paybill_id", paybillHandler.GetPayBill)
			paybills.PATCH("/:paybill_id", paybillHandler.UpdatePayBill)
			paybills.DELETE("/:paybill_id", paybillHandler.DeletePayBill)
		}

		// Tekmetric
		tekmetric := protected.Group("/tekmetric")
		{
			tekmetric.GET("/shops", tekmetricHandler.GetShops)
			tekmetric.GET("/repair-orders", tekmetricHandler.GetRepairOrders)
			tekmetric.PATCH("/repair-orders/bulk/", tekmetricHandler.BulkPatchRepairOrders)
			tekmetric.GET("/repair-orders/:repair_order_id", tekmetricHandler.GetRepairOrderByID)
			tekmetric.PATCH("/repair-orders/:repair_order_id", tekmetricHandler.PatchRepairOrder)
			tekmetric.GET("/custo", tekmetricHandler.GetAllCustomersParallel)
			tekmetric.GET("/jobs", tekmetricHandler.GetJobs)
			tekmetric.GET("/jobs/:job_id", tekmetricHandler.GetJobByID)
		}

		// Documents
		docs := protected.Group("/documents")
		{
			docs.POST("/upload", documentHandler.Upload)
			docs.GET("", documentHandler.List)
			docs.GET("/summary", documentHandler.Summary)
			docs.POST("/rescan-all", documentHandler.RescanAll)
			docs.GET("/rescan-status", documentHandler.RescanStatus)
			docs.POST("/match-all", docMatchHandler.MatchAll)
			docs.GET("/match-status", docMatchHandler.MatchStatus)
			docs.POST("/bulk-import-wf", bulkImportHandler.BulkImportWF)
			docs.GET("/bulk-import-status", bulkImportHandler.BulkImportStatus)
			docs.POST("/bulk-ocr", bulkImportHandler.BulkOCR)
			docs.GET("/bulk-ocr-status", bulkImportHandler.BulkOCRStatus)
			docs.POST("/import-from-wf", bulkImportHandler.ImportFromWF)
			docs.GET("/vendor-corrections", documentHandler.ListVendorCorrections)
			docs.GET("/:id", documentHandler.Get)
			docs.PATCH("/:id", documentHandler.Update)
			docs.DELETE("/:id", documentHandler.Delete)
			docs.POST("/:id/match", documentHandler.Match)
			docs.POST("/:id/reprocess", documentHandler.Reprocess)
			docs.POST("/:id/record-payment", documentHandler.RecordPayment)
			docs.GET("/:id/file", documentHandler.ServeFile)
			docs.GET("/:id/pdf", documentHandler.ServePDF)
		}

		// Accounts Payable
		ap := protected.Group("/ap")
		{
			ap.GET("", apHandler.ListEntries)
			ap.GET("/aging", apHandler.AgingReport)
			ap.GET("/:id", apHandler.GetEntry)
			ap.POST("/:id/authorize", apHandler.Authorize)
			ap.POST("/:id/mark-paid", apHandler.MarkPaid)
		}

		// Part Matching
		matching := protected.Group("/matching")
		{
			matching.POST("/invoice-to-ro", matchingHandler.MatchInvoiceToRO)
			matching.POST("/confirm", matchingHandler.ConfirmMatch)
			matching.GET("/results/:document_id", matchingHandler.GetResults)
			matching.GET("/vendor-mappings/:vendor_name", matchingHandler.GetVendorMappings)
		}

		// Receivables & Credits
		receivables := protected.Group("/receivables")
		{
			receivables.POST("", receivablesHandler.CreateReceivable)
			receivables.GET("", receivablesHandler.ListReceivables)
			receivables.GET("/aging", receivablesHandler.AgingReport)
			receivables.PATCH("/:id", receivablesHandler.PatchReceivable)
		}
		credits := protected.Group("/credits")
		{
			credits.POST("", receivablesHandler.CreateCredit)
			credits.GET("", receivablesHandler.ListCredits)
			credits.POST("/:id/apply", receivablesHandler.ApplyCredit)
		}

		// Document Matching
		protected.GET("/transactions/:transaction_id/document-status", docMatchHandler.GetDocumentStatus)
		protected.POST("/transactions/:transaction_id/document-match", docMatchHandler.MatchDocument)
		protected.GET("/documents/unmatched-bucket", docMatchHandler.UnmatchedBucket)

		// Settings / Integrations
		settings := protected.Group("/settings")
		{
			settings.GET("/integrations", documentHandler.GetIntegrationSettings)
			settings.PUT("/integrations", documentHandler.SaveIntegrationSettings)
			settings.POST("/integrations/test-wickedfile", documentHandler.TestWickedFileConnection)
			settings.PUT("/wickedfile-cookies", wfProxyHandler.SaveCookies)
			settings.GET("/wickedfile-cookies/status", wfProxyHandler.CookieStatus)
		}

		// WickedFile proxy + session auth
		wf := protected.Group("/wf")
		{
			wf.GET("/document/:scanPageId/pdf", wfProxyHandler.ProxyPDF)
			wf.GET("/document/:scanPageId/metadata", wfProxyHandler.ProxyMetadata)

			wf.POST("/auth/login", wfProxyHandler.Login)
			wf.POST("/auth/verify-2fa", wfProxyHandler.Verify2FA)
			wf.GET("/auth/status", wfProxyHandler.AuthStatus)
			wf.PUT("/auth/credentials", wfProxyHandler.SaveCredentials)

			wf.POST("/documents/sync-metadata", wfProxyHandler.StartMetadataSync)
			wf.GET("/documents/sync-status", wfProxyHandler.MetadataSyncStatus)
		}

		// Dashboard Layouts (per-user, per-page)
		layouts := protected.Group("/dashboard-layouts")
		{
			layouts.GET("/:page", dashboardLayoutHandler.Get)
			layouts.PUT("/:page", dashboardLayoutHandler.Put)
			layouts.DELETE("/:page", dashboardLayoutHandler.Delete)
		}

		// Locations
		locations := protected.Group("/locations")
		{
			locations.POST("", locationHandler.CreateLocation)
			locations.GET("", locationHandler.GetAllLocations)
			locations.POST("/shop-info/", locationHandler.CreateShopInfo)
			locations.GET("/shop-info/", locationHandler.GetAllShopInfos)
			locations.GET("/shop-info/:shop_info_id", locationHandler.GetShopInfo)
			locations.PATCH("/shop-info/:shop_info_id", locationHandler.UpdateShopInfo)
			locations.DELETE("/shop-info/:shop_info_id", locationHandler.DeleteShopInfo)
			locations.GET("/:location_id", locationHandler.GetLocation)
			locations.PATCH("/:location_id", locationHandler.UpdateLocation)
			locations.DELETE("/:location_id", locationHandler.DeleteLocation)
		}
	}
}
