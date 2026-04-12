package handlers

import (
	"database/sql"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/services"
)

type MatchingHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

func (h *MatchingHandler) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

// AutoMigrate creates the part matching tables if they do not exist.
func (h *MatchingHandler) AutoMigrate() {
	db := h.sqlDB()
	if db == nil {
		return
	}

	migrations := []string{
		`CREATE TABLE IF NOT EXISTS part_match_results (
			id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
			document_id INTEGER REFERENCES documents(id),
			line_item_index INTEGER,
			vendor_part_number VARCHAR,
			vendor_part_normalized VARCHAR,
			matched_ro_number VARCHAR,
			matched_ro_line_item_id VARCHAR,
			matched_part_number VARCHAR,
			match_score INTEGER,
			match_rule VARCHAR,
			match_confidence NUMERIC(4,2),
			ai_tiebreaker_used BOOLEAN DEFAULT false,
			ai_reasoning TEXT,
			status VARCHAR DEFAULT 'pending',
			confirmed_by VARCHAR,
			confirmed_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS vendor_part_mappings (
			id SERIAL PRIMARY KEY,
			vendor_name VARCHAR NOT NULL,
			vendor_part_number VARCHAR NOT NULL,
			vendor_part_normalized VARCHAR NOT NULL,
			internal_part_number VARCHAR,
			description VARCHAR,
			match_count INTEGER DEFAULT 1,
			auto_match_enabled BOOLEAN DEFAULT false,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW(),
			UNIQUE(vendor_name, vendor_part_normalized)
		)`,
	}

	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			log.Printf("[MatchingHandler] migration error: %v", err)
		}
	}
}

// MatchInvoiceToRO handles POST /matching/invoice-to-ro
func (h *MatchingHandler) MatchInvoiceToRO(c *gin.Context) {
	var req struct {
		DocumentID int    `json:"document_id" binding:"required"`
		ShopID     int    `json:"shop_id" binding:"required"`
		DateFrom   string `json:"date_from" binding:"required"`
		DateTo     string `json:"date_to" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	dateFrom, err := time.Parse("2006-01-02", req.DateFrom)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid date_from format, use YYYY-MM-DD"})
		return
	}
	dateTo, err := time.Parse("2006-01-02", req.DateTo)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid date_to format, use YYYY-MM-DD"})
		return
	}

	results, err := services.MatchInvoiceToRO(c.Request.Context(), h.GormDB, h.Cfg,
		req.DocumentID, req.ShopID, dateFrom, dateTo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"results": results})
}

// ConfirmMatch handles POST /matching/confirm
func (h *MatchingHandler) ConfirmMatch(c *gin.Context) {
	var req struct {
		MatchResultID string `json:"match_result_id" binding:"required"`
		Confirmed     bool   `json:"confirmed"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var result models.PartMatchResult
	if err := h.GormDB.First(&result, "id = ?", req.MatchResultID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "match result not found"})
		return
	}

	now := time.Now()
	if req.Confirmed {
		result.Status = "confirmed"
		result.ConfirmedBy = matchStrPtr("user")
		result.ConfirmedAt = &now

		// Update learning system
		if result.VendorPartNumber != nil && result.MatchedPartNumber != nil && result.VendorPartNormalized != nil {
			// Get vendor name from document
			vendorName := ""
			if result.DocumentID != nil {
				var doc models.Document
				if err := h.GormDB.First(&doc, *result.DocumentID).Error; err == nil && doc.VendorName != nil {
					vendorName = *doc.VendorName
				}
			}

			if vendorName != "" {
				var mapping models.VendorPartMapping
				err := h.GormDB.Where("vendor_name = ? AND vendor_part_normalized = ?",
					vendorName, *result.VendorPartNormalized).First(&mapping).Error
				if err != nil {
					// Create new mapping
					mapping = models.VendorPartMapping{
						VendorName:           vendorName,
						VendorPartNumber:     *result.VendorPartNumber,
						VendorPartNormalized: *result.VendorPartNormalized,
						InternalPartNumber:   result.MatchedPartNumber,
						MatchCount:           1,
					}
					h.GormDB.Create(&mapping)
				} else {
					// Increment count; auto-enable at 3 confirmations
					mapping.MatchCount++
					if mapping.MatchCount >= 3 {
						mapping.AutoMatchEnabled = true
					}
					h.GormDB.Save(&mapping)
				}
			}
		}
	} else {
		result.Status = "rejected"
		result.ConfirmedBy = matchStrPtr("user")
		result.ConfirmedAt = &now
	}

	h.GormDB.Save(&result)
	c.JSON(http.StatusOK, gin.H{"status": result.Status})
}

// GetResults handles GET /matching/results/:document_id
func (h *MatchingHandler) GetResults(c *gin.Context) {
	docID := c.Param("document_id")
	var results []models.PartMatchResult
	h.GormDB.Where("document_id = ?", docID).Order("line_item_index ASC").Find(&results)
	c.JSON(http.StatusOK, results)
}

// GetVendorMappings handles GET /matching/vendor-mappings/:vendor_name
func (h *MatchingHandler) GetVendorMappings(c *gin.Context) {
	vendorName := c.Param("vendor_name")
	var mappings []models.VendorPartMapping
	h.GormDB.Where("vendor_name = ?", vendorName).Order("match_count DESC").Find(&mappings)
	c.JSON(http.StatusOK, mappings)
}

func matchStrPtr(s string) *string {
	return &s
}
