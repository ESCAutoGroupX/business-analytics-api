package handlers

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/services"
)

// ReceivablesHandler handles vendor receivable and credit endpoints.
type ReceivablesHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

func (h *ReceivablesHandler) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

// AutoMigrate creates the receivables tables if they do not exist.
func (h *ReceivablesHandler) AutoMigrate() {
	db := h.sqlDB()
	if db == nil {
		return
	}

	migrations := []string{
		`CREATE TABLE IF NOT EXISTS vendor_receivables (
			id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
			shop_id INTEGER,
			shop_name VARCHAR,
			vendor_id VARCHAR,
			vendor_name VARCHAR NOT NULL,
			invoice_document_id INTEGER REFERENCES documents(id),
			invoice_number VARCHAR,
			invoice_date DATE,
			part_number VARCHAR,
			part_number_normalized VARCHAR,
			description VARCHAR,
			quantity NUMERIC(8,2),
			unit_price NUMERIC(12,2),
			total_amount NUMERIC(12,2) NOT NULL,
			receivable_type VARCHAR NOT NULL,
			status VARCHAR DEFAULT 'open',
			ro_number VARCHAR,
			notes TEXT,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS vendor_credits (
			id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
			vendor_id VARCHAR,
			vendor_name VARCHAR NOT NULL,
			credit_document_id INTEGER REFERENCES documents(id),
			credit_memo_number VARCHAR,
			credit_date DATE,
			total_amount NUMERIC(12,2) NOT NULL,
			remaining_amount NUMERIC(12,2),
			reference_invoice_number VARCHAR,
			reference_po_number VARCHAR,
			status VARCHAR DEFAULT 'open',
			notes TEXT,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS vendor_credit_applications (
			id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
			credit_id UUID REFERENCES vendor_credits(id),
			receivable_id UUID REFERENCES vendor_receivables(id),
			amount NUMERIC(12,2) NOT NULL,
			applied_by VARCHAR,
			applied_at TIMESTAMP DEFAULT NOW(),
			notes TEXT
		)`,
	}

	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			log.Printf("[ReceivablesHandler] migration error: %v", err)
		}
	}
}

// ── Receivables CRUD ────────────────────────────────────────

// CreateReceivable handles POST /receivables.
func (h *ReceivablesHandler) CreateReceivable(c *gin.Context) {
	var rec models.VendorReceivable
	if err := c.ShouldBindJSON(&rec); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	rec.ID = uuid.New().String()

	// Normalize part number if provided
	if rec.PartNumber != nil && *rec.PartNumber != "" {
		normalized := services.NormalizePartNumber(*rec.PartNumber, rec.VendorName)
		rec.PartNumberNormalized = &normalized
	}

	if err := h.GormDB.Create(&rec).Error; err != nil {
		log.Printf("ERROR: failed to create receivable: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create receivable", "error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, rec)
}

// ListReceivables handles GET /receivables.
func (h *ReceivablesHandler) ListReceivables(c *gin.Context) {
	query := h.GormDB.Preload("Vendor").Order("created_at DESC")

	if status := c.Query("status"); status != "" {
		query = query.Where("status = ?", status)
	}
	if vendorID := c.Query("vendor_id"); vendorID != "" {
		query = query.Where("vendor_id = ?", vendorID)
	}
	if shopID := c.Query("shop_id"); shopID != "" {
		query = query.Where("shop_id = ?", shopID)
	}

	var results []models.VendorReceivable
	if err := query.Find(&results).Error; err != nil {
		log.Printf("ERROR: failed to query receivables: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query receivables", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"receivables": results})
}

// PatchReceivable handles PATCH /receivables/:id.
func (h *ReceivablesHandler) PatchReceivable(c *gin.Context) {
	id := c.Param("id")

	var rec models.VendorReceivable
	if err := h.GormDB.First(&rec, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Receivable not found"})
		return
	}

	var updates map[string]interface{}
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Only allow specific fields
	allowed := map[string]bool{"status": true, "notes": true, "ro_number": true}
	filtered := map[string]interface{}{}
	for k, v := range updates {
		if allowed[k] {
			filtered[k] = v
		}
	}

	if len(filtered) == 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": "no valid fields to update"})
		return
	}

	filtered["updated_at"] = time.Now().UTC()

	if err := h.GormDB.Model(&rec).Updates(filtered).Error; err != nil {
		log.Printf("ERROR: failed to update receivable %s: %v", id, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update receivable", "error": err.Error()})
		return
	}

	// Reload the record
	h.GormDB.First(&rec, "id = ?", id)
	c.JSON(http.StatusOK, rec)
}

// AgingReport handles GET /receivables/aging.
func (h *ReceivablesHandler) AgingReport(c *gin.Context) {
	sqlDB := h.sqlDB()
	if sqlDB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database error"})
		return
	}

	rows, err := sqlDB.Query(`SELECT vendor_name,
		SUM(CASE WHEN created_at >= NOW() - INTERVAL '30 days' THEN total_amount ELSE 0 END) as current_30,
		SUM(CASE WHEN created_at >= NOW() - INTERVAL '60 days' AND created_at < NOW() - INTERVAL '30 days' THEN total_amount ELSE 0 END) as days_31_60,
		SUM(CASE WHEN created_at >= NOW() - INTERVAL '90 days' AND created_at < NOW() - INTERVAL '60 days' THEN total_amount ELSE 0 END) as days_61_90,
		SUM(CASE WHEN created_at < NOW() - INTERVAL '90 days' THEN total_amount ELSE 0 END) as over_90,
		SUM(total_amount) as total
		FROM vendor_receivables
		WHERE status = 'open'
		GROUP BY vendor_name
		ORDER BY total DESC`)
	if err != nil {
		log.Printf("ERROR: aging query failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "aging query failed", "error": err.Error()})
		return
	}
	defer rows.Close()

	var results []gin.H
	for rows.Next() {
		var vendorName string
		var current30, days3160, days6190, over90, total float64
		if err := rows.Scan(&vendorName, &current30, &days3160, &days6190, &over90, &total); err != nil {
			log.Printf("ERROR: aging row scan error: %v", err)
			continue
		}
		results = append(results, gin.H{
			"vendor_name": vendorName,
			"current_30":  current30,
			"days_31_60":  days3160,
			"days_61_90":  days6190,
			"over_90":     over90,
			"total":       total,
		})
	}
	if results == nil {
		results = []gin.H{}
	}

	c.JSON(http.StatusOK, gin.H{"aging": results})
}

// ── Credits CRUD ────────────────────────────────────────────

// CreateCredit handles POST /credits.
func (h *ReceivablesHandler) CreateCredit(c *gin.Context) {
	var credit models.VendorCredit
	if err := c.ShouldBindJSON(&credit); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	credit.ID = uuid.New().String()

	// Set remaining_amount = total_amount initially
	remaining := credit.TotalAmount
	credit.RemainingAmount = &remaining

	if err := h.GormDB.Create(&credit).Error; err != nil {
		log.Printf("ERROR: failed to create credit: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create credit", "error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, credit)
}

// ListCredits handles GET /credits.
func (h *ReceivablesHandler) ListCredits(c *gin.Context) {
	query := h.GormDB.Preload("Vendor").Order("created_at DESC")

	if status := c.Query("status"); status != "" {
		query = query.Where("status = ?", status)
	}
	if vendorID := c.Query("vendor_id"); vendorID != "" {
		query = query.Where("vendor_id = ?", vendorID)
	}

	var results []models.VendorCredit
	if err := query.Find(&results).Error; err != nil {
		log.Printf("ERROR: failed to query credits: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query credits", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"credits": results})
}

// ── Apply Credit ────────────────────────────────────────────

type applyCreditRequest struct {
	ReceivableID string  `json:"receivable_id" binding:"required"`
	Amount       float64 `json:"amount" binding:"required"`
	Notes        *string `json:"notes"`
}

// ApplyCredit handles POST /credits/:id/apply.
func (h *ReceivablesHandler) ApplyCredit(c *gin.Context) {
	creditID := c.Param("id")

	var req applyCreditRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// 1. Find credit and check status
	var credit models.VendorCredit
	if err := h.GormDB.First(&credit, "id = ?", creditID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Credit not found"})
		return
	}
	if credit.Status == "fully_applied" {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": "Credit is already fully applied"})
		return
	}

	// 2. Find receivable and check status
	var receivable models.VendorReceivable
	if err := h.GormDB.First(&receivable, "id = ?", req.ReceivableID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Receivable not found"})
		return
	}
	if receivable.Status != "open" {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": "Receivable is not open"})
		return
	}

	// 3. Validate amount against remaining credit
	remaining := credit.TotalAmount
	if credit.RemainingAmount != nil {
		remaining = *credit.RemainingAmount
	}
	if req.Amount > remaining {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": fmt.Sprintf("Amount %.2f exceeds remaining credit %.2f", req.Amount, remaining)})
		return
	}

	// 4. Get user email for applied_by
	var appliedBy *string
	if email, exists := c.Get("email"); exists {
		emailStr := fmt.Sprintf("%v", email)
		appliedBy = &emailStr
	}

	// 5. Create application record
	application := models.VendorCreditApplication{
		ID:           uuid.New().String(),
		CreditID:     creditID,
		ReceivableID: req.ReceivableID,
		Amount:       req.Amount,
		AppliedBy:    appliedBy,
		Notes:        req.Notes,
	}
	if err := h.GormDB.Create(&application).Error; err != nil {
		log.Printf("ERROR: failed to create credit application: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to apply credit", "error": err.Error()})
		return
	}

	// 6. Update credit remaining
	newRemaining := remaining - req.Amount
	credit.RemainingAmount = &newRemaining
	if newRemaining <= 0 {
		credit.Status = "fully_applied"
	} else {
		credit.Status = "partially_applied"
	}
	credit.UpdatedAt = time.Now().UTC()
	if err := h.GormDB.Save(&credit).Error; err != nil {
		log.Printf("ERROR: failed to update credit %s: %v", creditID, err)
	}

	// 7. Update receivable status
	receivable.Status = "credited"
	receivable.UpdatedAt = time.Now().UTC()
	if err := h.GormDB.Save(&receivable).Error; err != nil {
		log.Printf("ERROR: failed to update receivable %s: %v", req.ReceivableID, err)
	}

	c.JSON(http.StatusOK, application)
}

// AutoMatchCredits handles POST /credits/auto-match.
func (h *ReceivablesHandler) AutoMatchCredits(c *gin.Context) {
	var credits []models.VendorCredit
	h.GormDB.Where("status = ?", "open").Find(&credits)

	var receivables []models.VendorReceivable
	h.GormDB.Where("status = ?", "open").Find(&receivables)

	matched := 0
	for _, credit := range credits {
		match, _ := services.MatchCreditToReceivable(credit, receivables)
		if match != nil {
			matched++
		}
	}

	c.JSON(http.StatusOK, gin.H{"matched": matched})
}
