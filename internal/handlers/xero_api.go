package handlers

import (
	"net/http"
	"strconv"

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

	conn, err := h.Sync.getActiveConnection()
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
