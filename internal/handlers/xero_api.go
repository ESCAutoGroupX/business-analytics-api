package handlers

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
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
		query = query.Where("asset_type_name ILIKE ? OR asset_type ILIKE ?", "%"+v+"%", "%"+v+"%")
	}
	if v := c.Query("location"); v != "" {
		query = query.Where("location ILIKE ?", "%"+v+"%")
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

// parseFlexDate handles date strings in various formats that GORM/pgx may return.
func parseFlexDate(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{
		"2006-01-02",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05-07:00",
		time.RFC3339,
		time.RFC3339Nano,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// GET /xero/reconciliation-summary
func (h *XeroAPIHandler) ReconciliationSummary(c *gin.Context) {
	now := time.Now()
	startDate := now.AddDate(0, 0, -30).Format("2006-01-02")

	// 1. Fetch Plaid transactions from last 30 days
	var plaidTxns []models.Transaction
	h.GormDB.Where("date >= ? AND (pending = false OR pending IS NULL)", startDate).
		Order("date DESC").
		Find(&plaidTxns)

	log.Printf("Reconciliation: fetched %d Plaid transactions since %s", len(plaidTxns), startDate)

	if len(plaidTxns) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"total_plaid_transactions": 0,
			"matched":                 0,
			"unmatched":               0,
			"match_rate":              0,
			"unmatched_transactions":  []interface{}{},
			"by_account":              []interface{}{},
		})
		return
	}

	// 2. Fetch Xero bank transactions for the same window (±1 day buffer)
	xeroStart := now.AddDate(0, 0, -31).Format("2006-01-02")
	xeroEnd := now.AddDate(0, 0, 1).Format("2006-01-02")

	var xeroTxns []models.XeroBankTransaction
	h.GormDB.Where("date >= ? AND date <= ?", xeroStart, xeroEnd).Find(&xeroTxns)

	log.Printf("Reconciliation: fetched %d Xero transactions (%s to %s)", len(xeroTxns), xeroStart, xeroEnd)

	// 3. Build lookup: key by ABS amount in cents for fast matching.
	//    Xero totals are always positive (SPEND and RECEIVE both > 0).
	type xeroEntry struct {
		date  time.Time
		total float64
	}
	xeroByAmount := map[int64][]xeroEntry{}
	for _, xt := range xeroTxns {
		if xt.Total == nil || xt.Date == nil {
			continue
		}
		cents := int64(math.Round(*xt.Total * 100))
		xeroByAmount[cents] = append(xeroByAmount[cents], xeroEntry{date: *xt.Date, total: *xt.Total})
	}

	// 4. Match each Plaid transaction against Xero by ABS(amount) ±$0.01 and date ±1 day.
	//    Plaid: positive = debit/spend, negative = credit/receive.
	//    Xero: total is always positive regardless of type.
	//    So we compare ABS(plaid_amount) against xero_total.
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
	skipped := 0
	var unmatchedList []unmatchedTxn
	byAccount := map[string]*accountStats{}

	for _, pt := range plaidTxns {
		if pt.Amount == nil || pt.Date == nil {
			skipped++
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

		plaidDate, ok := parseFlexDate(*pt.Date)
		if !ok {
			skipped++
			continue
		}

		// Compare ABS(plaid_amount) vs xero_total, with ±$0.01 tolerance
		absCents := int64(math.Round(math.Abs(*pt.Amount) * 100))
		found := false
		for _, delta := range []int64{0, 1, -1} {
			candidates := xeroByAmount[absCents+delta]
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
					Date:        plaidDate.Format("2006-01-02"),
					Amount:      *pt.Amount,
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

	log.Printf("Reconciliation: total=%d matched=%d unmatched=%d skipped=%d rate=%.1f%%",
		total, matched, unmatched, skipped, matchRate)

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

	// 5. Check overrides table — resolved overrides reduce the unmatched count
	var overrides []models.ReconciliationOverride
	h.GormDB.Find(&overrides)
	overrideMap := map[string]models.ReconciliationOverride{}
	for _, o := range overrides {
		overrideMap[o.PlaidID] = o
	}

	resolved := 0
	var finalUnmatched []unmatchedTxn
	for _, ut := range unmatchedList {
		if o, ok := overrideMap[ut.PlaidTxnID]; ok {
			s := o.MatchStatus
			if s == "manually_matched" || strings.HasPrefix(s, "excluded_") {
				resolved++
				continue
			}
		}
		finalUnmatched = append(finalUnmatched, ut)
	}

	// Also count overrides for plaid IDs not in our top-50 list
	for _, o := range overrides {
		s := o.MatchStatus
		if s == "manually_matched" || strings.HasPrefix(s, "excluded_") {
			alreadyCounted := false
			for _, ut := range unmatchedList {
				if ut.PlaidTxnID == o.PlaidID {
					alreadyCounted = true
					break
				}
			}
			if !alreadyCounted {
				resolved++
			}
		}
	}

	adjustedUnmatched := unmatched - resolved
	if adjustedUnmatched < 0 {
		adjustedUnmatched = 0
	}
	adjustedTotal := matched + adjustedUnmatched
	adjustedRate := float64(0)
	if adjustedTotal > 0 {
		adjustedRate = math.Round(float64(matched)/float64(adjustedTotal)*1000) / 10
	}

	// Enrich unmatched list with override data
	type enrichedUnmatched struct {
		Date        string  `json:"date"`
		Amount      float64 `json:"amount"`
		Description string  `json:"description"`
		Account     string  `json:"account"`
		PlaidTxnID  string  `json:"plaid_transaction_id"`
		VendorName  string  `json:"vendor_name,omitempty"`
		MatchStatus string  `json:"match_status,omitempty"`
	}
	var enrichedList []enrichedUnmatched
	for _, ut := range finalUnmatched {
		e := enrichedUnmatched{
			Date: ut.Date, Amount: ut.Amount, Description: ut.Description,
			Account: ut.Account, PlaidTxnID: ut.PlaidTxnID,
		}
		if o, ok := overrideMap[ut.PlaidTxnID]; ok {
			e.VendorName = o.VendorName
			e.MatchStatus = o.MatchStatus
		}
		enrichedList = append(enrichedList, e)
	}

	c.JSON(http.StatusOK, gin.H{
		"total_plaid_transactions": total,
		"matched":                 matched,
		"unmatched":               adjustedUnmatched,
		"resolved":                resolved,
		"match_rate":              adjustedRate,
		"unmatched_transactions":  enrichedList,
		"by_account":              byAccountList,
	})
}

// POST /xero/reconciliation-override
func (h *XeroAPIHandler) SaveReconciliationOverride(c *gin.Context) {
	var req struct {
		PlaidID       string `json:"plaid_id" binding:"required"`
		VendorName    string `json:"vendor_name"`
		Description   string `json:"description"`
		GLAccountCode string `json:"gl_account_code"`
		Notes         string `json:"notes"`
		MatchStatus   string `json:"match_status"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "plaid_id is required"})
		return
	}

	if req.MatchStatus == "" {
		req.MatchStatus = "unmatched"
	}

	userID := ""
	if uid, exists := c.Get("user_id"); exists {
		userID = fmt.Sprintf("%v", uid)
	}

	override := models.ReconciliationOverride{
		PlaidID:       req.PlaidID,
		VendorName:    req.VendorName,
		Description:   req.Description,
		GLAccountCode: req.GLAccountCode,
		Notes:         req.Notes,
		MatchStatus:   req.MatchStatus,
		UpdatedBy:     userID,
	}

	result := h.GormDB.Save(&override)
	if result.Error != nil {
		log.Printf("ERROR saving reconciliation override: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to save override"})
		return
	}

	c.JSON(http.StatusOK, override)
}

// GET /xero/reconciliation-overrides
func (h *XeroAPIHandler) ListReconciliationOverrides(c *gin.Context) {
	var overrides []models.ReconciliationOverride
	h.GormDB.Order("updated_at DESC").Find(&overrides)
	c.JSON(http.StatusOK, overrides)
}
