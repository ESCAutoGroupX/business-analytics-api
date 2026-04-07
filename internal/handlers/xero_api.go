package handlers

import (
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type XeroAPIHandler struct {
	GormDB *gorm.DB
	Sync   *XeroSyncHandler
}

func xeroPaginate(c *gin.Context) (page int, pageSize int, offset int) {
	page, _ = strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ = strconv.Atoi(c.DefaultQuery("page_size", "50"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}
	offset = (page - 1) * pageSize
	return
}

// GET /xero/bank-transactions
func (h *XeroAPIHandler) ListBankTransactions(c *gin.Context) {
	page, pageSize, offset := xeroPaginate(c)

	query := h.GormDB.Model(&models.XeroBankTransaction{})

	if v := c.Query("start_date"); v != "" {
		query = query.Where("date >= ?", v)
	}
	if v := c.Query("end_date"); v != "" {
		query = query.Where("date <= ?", v)
	}
	if v := c.Query("contact_name"); v != "" {
		query = query.Where("contact_name ILIKE ?", "%"+v+"%")
	}
	if v := c.Query("is_reconciled"); v != "" {
		query = query.Where("is_reconciled = ?", v == "true")
	}

	var total int64
	query.Count(&total)

	var results []models.XeroBankTransaction
	query.Order("date DESC").Offset(offset).Limit(pageSize).Find(&results)

	c.JSON(http.StatusOK, gin.H{
		"data":      results,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// GET /xero/invoices
func (h *XeroAPIHandler) ListInvoices(c *gin.Context) {
	page, pageSize, offset := xeroPaginate(c)

	query := h.GormDB.Model(&models.XeroInvoice{})

	if v := c.Query("type"); v != "" {
		query = query.Where("type = ?", v)
	}
	if v := c.Query("status"); v != "" {
		query = query.Where("status = ?", v)
	}
	if v := c.Query("contact_name"); v != "" {
		query = query.Where("contact_name ILIKE ?", "%"+v+"%")
	}
	if v := c.Query("start_date"); v != "" {
		query = query.Where("date >= ?", v)
	}
	if v := c.Query("end_date"); v != "" {
		query = query.Where("date <= ?", v)
	}

	var total int64
	query.Count(&total)

	var results []models.XeroInvoice
	query.Order("date DESC").Offset(offset).Limit(pageSize).Find(&results)

	c.JSON(http.StatusOK, gin.H{
		"data":      results,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// GET /xero/contacts
func (h *XeroAPIHandler) ListContacts(c *gin.Context) {
	page, pageSize, offset := xeroPaginate(c)

	query := h.GormDB.Model(&models.XeroContact{})

	if v := c.Query("search"); v != "" {
		query = query.Where("name ILIKE ?", "%"+v+"%")
	}
	if v := c.Query("is_supplier"); v != "" {
		query = query.Where("is_supplier = ?", v == "true")
	}
	if v := c.Query("is_customer"); v != "" {
		query = query.Where("is_customer = ?", v == "true")
	}

	var total int64
	query.Count(&total)

	var results []models.XeroContact
	query.Order("name ASC").Offset(offset).Limit(pageSize).Find(&results)

	c.JSON(http.StatusOK, gin.H{
		"data":      results,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// GET /xero/payments
func (h *XeroAPIHandler) ListPayments(c *gin.Context) {
	page, pageSize, offset := xeroPaginate(c)

	query := h.GormDB.Model(&models.XeroPayment{})

	if v := c.Query("start_date"); v != "" {
		query = query.Where("date >= ?", v)
	}
	if v := c.Query("end_date"); v != "" {
		query = query.Where("date <= ?", v)
	}

	var total int64
	query.Count(&total)

	var results []models.XeroPayment
	query.Order("date DESC").Offset(offset).Limit(pageSize).Find(&results)

	c.JSON(http.StatusOK, gin.H{
		"data":      results,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// GET /xero/assets
func (h *XeroAPIHandler) ListAssets(c *gin.Context) {
	page, pageSize, offset := xeroPaginate(c)

	query := h.GormDB.Model(&models.XeroAsset{})

	if v := c.Query("status"); v != "" {
		query = query.Where("status = ?", v)
	}
	if v := c.Query("asset_type"); v != "" {
		query = query.Where("asset_type_name ILIKE ?", "%"+v+"%")
	}

	var total int64
	query.Count(&total)

	var results []models.XeroAsset
	query.Order("asset_name ASC").Offset(offset).Limit(pageSize).Find(&results)

	c.JSON(http.StatusOK, gin.H{
		"data":      results,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// GET /xero/asset-types
func (h *XeroAPIHandler) ListAssetTypes(c *gin.Context) {
	var results []models.XeroAssetType
	h.GormDB.Order("asset_type_name ASC").Find(&results)
	c.JSON(http.StatusOK, results)
}

// GET /xero/journals
func (h *XeroAPIHandler) ListJournals(c *gin.Context) {
	page, pageSize, offset := xeroPaginate(c)

	query := h.GormDB.Model(&models.XeroJournal{})

	if v := c.Query("start_date"); v != "" {
		query = query.Where("journal_date >= ?", v)
	}
	if v := c.Query("end_date"); v != "" {
		query = query.Where("journal_date <= ?", v)
	}

	var total int64
	query.Count(&total)

	var results []models.XeroJournal
	query.Order("journal_date DESC").Offset(offset).Limit(pageSize).Find(&results)

	c.JSON(http.StatusOK, gin.H{
		"data":      results,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// GET /xero/tracking-categories
func (h *XeroAPIHandler) ListTrackingCategories(c *gin.Context) {
	var results []models.XeroTrackingCategory
	h.GormDB.Order("name ASC").Find(&results)
	c.JSON(http.StatusOK, results)
}

// GET /xero/reports/:type
func (h *XeroAPIHandler) GetReport(c *gin.Context) {
	reportType := c.Param("type")

	validTypes := map[string]bool{
		"ProfitAndLoss":            true,
		"BalanceSheet":             true,
		"TrialBalance":             true,
		"BankSummary":              true,
		"AgedPayablesByContact":    true,
		"AgedReceivablesByContact": true,
		"BudgetSummary":            true,
		"ExecutiveSummary":         true,
	}
	if !validTypes[reportType] {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid report type"})
		return
	}

	conn, err := h.Sync.GetActiveConnection()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	queryParams := c.Request.URL.RawQuery

	data, err := h.Sync.GetCachedOrFetchReport(conn.TenantID, conn.AccessToken, reportType, queryParams)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": err.Error()})
		return
	}

	c.Data(http.StatusOK, "application/json", data)
}

// GET /xero/match-transactions
func (h *XeroAPIHandler) MatchTransactions(c *gin.Context) {
	checkNumber := c.Query("check_number")
	amount := c.Query("amount")
	date := c.Query("date")

	query := h.GormDB.Model(&models.XeroBankTransaction{})

	if checkNumber != "" {
		query = query.Where("reference ILIKE ?", "%"+checkNumber+"%")
	}
	if amount != "" {
		if amtFloat, err := strconv.ParseFloat(amount, 64); err == nil {
			query = query.Where("ABS(total - ?) < 0.01", amtFloat)
		}
	}
	if date != "" {
		query = query.Where("date = ?", date)
	}

	var results []models.XeroBankTransaction
	query.Limit(20).Find(&results)

	c.JSON(http.StatusOK, results)
}

// GET /xero/reconciliation-summary
func (h *XeroAPIHandler) ReconciliationSummary(c *gin.Context) {
	now := time.Now()
	startDate := now.AddDate(0, 0, -30).Format("2006-01-02")

	// 1. Fetch Plaid transactions from last 30 days
	var plaidTxns []models.Transaction
	h.GormDB.Where("date >= ? AND pending = false", startDate).
		Order("date DESC").
		Find(&plaidTxns)

	if len(plaidTxns) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"total_plaid_transactions": 0,
			"matched":                 0,
			"unmatched":               0,
			"match_rate":              0,
			"unmatched_transactions":  []interface{}{},
			"by_account":             []interface{}{},
		})
		return
	}

	// 2. Fetch Xero bank transactions for the same window (±1 day buffer)
	xeroStart := now.AddDate(0, 0, -31).Format("2006-01-02")
	xeroEnd := now.AddDate(0, 0, 1).Format("2006-01-02")

	var xeroTxns []models.XeroBankTransaction
	h.GormDB.Where("date >= ? AND date <= ?", xeroStart, xeroEnd).Find(&xeroTxns)

	// 3. Build lookup: for each Xero txn, key by rounded amount
	type xeroEntry struct {
		date  time.Time
		total float64
	}
	xeroByAmount := map[int64][]xeroEntry{}
	for _, xt := range xeroTxns {
		if xt.Total == nil || xt.Date == nil {
			continue
		}
		// Key by amount in cents for fast lookup
		cents := int64(math.Round(*xt.Total * 100))
		xeroByAmount[cents] = append(xeroByAmount[cents], xeroEntry{date: *xt.Date, total: *xt.Total})
	}

	// 4. Match each Plaid transaction
	type unmatchedTxn struct {
		Date        string  `json:"date"`
		Amount      float64 `json:"amount"`
		Description string  `json:"description"`
		Account     string  `json:"account"`
		PlaidTxnID  string  `json:"plaid_transaction_id"`
	}

	type accountStats struct {
		Total     int `json:"total"`
		Matched   int `json:"matched"`
		Unmatched int `json:"unmatched"`
	}

	matched := 0
	unmatched := 0
	var unmatchedList []unmatchedTxn
	byAccount := map[string]*accountStats{}

	for _, pt := range plaidTxns {
		if pt.Amount == nil || pt.Date == nil {
			continue
		}

		acctName := ""
		if pt.AccountName != nil {
			acctName = *pt.AccountName
		}
		if _, ok := byAccount[acctName]; !ok {
			byAccount[acctName] = &accountStats{}
		}
		byAccount[acctName].Total++

		plaidDate, err := time.Parse("2006-01-02", *pt.Date)
		if err != nil {
			continue
		}

		// Plaid amounts: positive = money out, Xero: could be either sign
		// Check both the amount and negated amount
		plaidAmt := *pt.Amount
		found := false
		for _, sign := range []float64{1, -1} {
			cents := int64(math.Round(plaidAmt * sign * 100))
			for _, delta := range []int64{0, 1, -1} {
				candidates := xeroByAmount[cents+delta]
				for _, xe := range candidates {
					dayDiff := plaidDate.Sub(xe.date).Hours() / 24
					if dayDiff >= -1 && dayDiff <= 1 {
						found = true
						break
					}
				}
				if found {
					break
				}
			}
			if found {
				break
			}
		}

		if found {
			matched++
			byAccount[acctName].Matched++
		} else {
			unmatched++
			byAccount[acctName].Unmatched++
			if len(unmatchedList) < 50 {
				desc := ""
				if pt.MerchantName != nil {
					desc = *pt.MerchantName
				} else if pt.Name != nil {
					desc = *pt.Name
				}
				plaidID := ""
				if pt.PlaidID != nil {
					plaidID = *pt.PlaidID
				}
				unmatchedList = append(unmatchedList, unmatchedTxn{
					Date:        *pt.Date,
					Amount:      plaidAmt,
					Description: desc,
					Account:     acctName,
					PlaidTxnID:  plaidID,
				})
			}
		}
	}

	total := matched + unmatched
	matchRate := float64(0)
	if total > 0 {
		matchRate = math.Round(float64(matched)/float64(total)*1000) / 10
	}

	// Convert byAccount map to slice
	type accountOut struct {
		AccountName string `json:"account_name"`
		Total       int    `json:"total"`
		Matched     int    `json:"matched"`
		Unmatched   int    `json:"unmatched"`
	}
	var byAccountList []accountOut
	for name, stats := range byAccount {
		displayName := name
		if displayName == "" {
			displayName = "Unknown Account"
		}
		byAccountList = append(byAccountList, accountOut{
			AccountName: displayName,
			Total:       stats.Total,
			Matched:     stats.Matched,
			Unmatched:   stats.Unmatched,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"total_plaid_transactions": total,
		"matched":                 matched,
		"unmatched":               unmatched,
		"match_rate":              matchRate,
		"unmatched_transactions":  unmatchedList,
		"by_account":             byAccountList,
	})
}
