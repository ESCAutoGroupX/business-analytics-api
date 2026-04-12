package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/services"
)

// PartMatchingHandler handles invoice-to-RO part matching endpoints.
type PartMatchingHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

// ── Request / helper types ──────────────────────────────────

type matchRequest struct {
	DocumentID int    `json:"document_id" binding:"required"`
	ShopID     int    `json:"shop_id" binding:"required"`
	DateFrom   string `json:"date_from" binding:"required"`
	DateTo     string `json:"date_to" binding:"required"`
}

type confirmRequest struct {
	MatchResultID string `json:"match_result_id" binding:"required"`
	Confirmed     bool   `json:"confirmed"`
}

type lineItem struct {
	PartNumber  string  `json:"part_number"`
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	UnitPrice   float64 `json:"unit_price"`
	Total       float64 `json:"total"`
}

// ── POST /matching/invoice-to-ro ────────────────────────────

func (h *PartMatchingHandler) InvoiceToRO(c *gin.Context) {
	var req matchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// 1. Fetch document
	var doc models.Document
	if err := h.GormDB.First(&doc, "id = ?", req.DocumentID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Document not found"})
		return
	}

	// 2. Parse line items from document JSON
	var rawItems []lineItem
	if doc.LineItems != nil && *doc.LineItems != "" {
		if err := json.Unmarshal([]byte(*doc.LineItems), &rawItems); err != nil {
			log.Printf("ERROR: failed to parse line items for document %d: %v", req.DocumentID, err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to parse document line items", "error": err.Error()})
			return
		}
	}

	// 3. Convert to service types
	invoiceParts := make([]services.InvoicePartLine, len(rawItems))
	for i, li := range rawItems {
		invoiceParts[i] = services.InvoicePartLine{
			PartNumber:  li.PartNumber,
			Description: li.Description,
			Quantity:    li.Quantity,
			UnitPrice:   li.UnitPrice,
			Total:       li.Total,
		}
	}

	// 4. Parse date range
	dateFrom, err := time.Parse("2006-01-02", req.DateFrom)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": fmt.Sprintf("invalid date_from format: %v", err)})
		return
	}
	dateTo, err := time.Parse("2006-01-02", req.DateTo)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": fmt.Sprintf("invalid date_to format: %v", err)})
		return
	}

	// 5. Fetch ROs from SMS API (graceful degradation on failure)
	smsClient := services.NewSMSClient(h.Cfg)
	ros, err := smsClient.GetPostedROs(req.ShopID, dateFrom, dateTo)
	if err != nil {
		log.Printf("ERROR: SMS API failed for shop %d: %v", req.ShopID, err)
		ros = nil // graceful degradation -- proceed with empty RO list
	}

	// 6. Flatten RO line items
	var roParts []services.InvoicePartLine
	for _, ro := range ros {
		for _, li := range ro.LineItems {
			roParts = append(roParts, services.InvoicePartLine{
				PartNumber:  li.PartNumber,
				Description: li.Description,
				Quantity:    float64(li.Quantity),
				UnitPrice:   li.UnitPrice,
				Total:       li.Total,
				RONumber:    ro.RONumber,
				LineItemID:  li.ID,
			})
		}
	}

	// 7. Determine vendor name
	vendorName := ""
	if doc.VendorName != nil {
		vendorName = *doc.VendorName
	}

	// 8. Run matching
	matchResults, err := services.MatchInvoiceToROs(c.Request.Context(), h.Cfg, h.GormDB, invoiceParts, roParts, req.DocumentID, vendorName)
	if err != nil {
		log.Printf("ERROR: matching failed for document %d: %v", req.DocumentID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "matching failed", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"results": matchResults})
}

// ── POST /matching/confirm ──────────────────────────────────

func (h *PartMatchingHandler) ConfirmMatch(c *gin.Context) {
	var req confirmRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// 1. Find match result
	var result models.PartMatchResult
	if err := h.GormDB.First(&result, "id = ?", req.MatchResultID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Match result not found"})
		return
	}

	now := time.Now().UTC()

	if req.Confirmed {
		// 2. Confirm
		result.Status = "confirmed"
		result.ConfirmedAt = &now

		// Get user email from auth context
		if email, exists := c.Get("email"); exists {
			emailStr := fmt.Sprintf("%v", email)
			result.ConfirmedBy = &emailStr
		}

		// 3. Upsert vendor_part_mappings for learning system
		if result.VendorPartNormalized != nil && *result.VendorPartNormalized != "" {
			var mapping models.VendorPartMapping
			findErr := h.GormDB.Where("vendor_part_normalized = ?", *result.VendorPartNormalized).First(&mapping).Error

			if findErr == nil {
				// Existing mapping -- increment match_count
				mapping.MatchCount++
				if mapping.MatchCount >= 3 {
					mapping.AutoMatchEnabled = true
				}
				mapping.UpdatedAt = now
				if err := h.GormDB.Save(&mapping).Error; err != nil {
					log.Printf("ERROR: failed to update vendor part mapping: %v", err)
				}
			} else {
				// Create new mapping
				vendorName := ""
				if result.DocumentID != nil {
					var doc models.Document
					if h.GormDB.First(&doc, "id = ?", *result.DocumentID).Error == nil && doc.VendorName != nil {
						vendorName = *doc.VendorName
					}
				}

				vendorPartNumber := ""
				if result.VendorPartNumber != nil {
					vendorPartNumber = *result.VendorPartNumber
				}

				newMapping := models.VendorPartMapping{
					VendorName:           vendorName,
					VendorPartNumber:     vendorPartNumber,
					VendorPartNormalized: *result.VendorPartNormalized,
					InternalPartNumber:   result.MatchedPartNumber,
					MatchCount:           1,
					AutoMatchEnabled:     false,
				}
				if err := h.GormDB.Create(&newMapping).Error; err != nil {
					log.Printf("ERROR: failed to create vendor part mapping: %v", err)
				}
			}
		}
	} else {
		// 4. Reject
		result.Status = "rejected"
	}

	if err := h.GormDB.Save(&result).Error; err != nil {
		log.Printf("ERROR: failed to save match result: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update match result", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// ── GET /matching/results/:document_id ──────────────────────

func (h *PartMatchingHandler) GetResults(c *gin.Context) {
	docIDStr := c.Param("document_id")
	docID, err := strconv.Atoi(docIDStr)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": "invalid document_id"})
		return
	}

	var results []models.PartMatchResult
	if err := h.GormDB.Where("document_id = ?", docID).Order("line_item_index ASC").Find(&results).Error; err != nil {
		log.Printf("ERROR: failed to query match results for document %d: %v", docID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query results", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"results": results})
}

// ── GET /matching/vendor-mappings/:vendor_name ──────────────

func (h *PartMatchingHandler) GetVendorMappings(c *gin.Context) {
	vendorName := c.Param("vendor_name")

	var mappings []models.VendorPartMapping
	if err := h.GormDB.Where("vendor_name = ?", vendorName).Order("match_count DESC").Find(&mappings).Error; err != nil {
		log.Printf("ERROR: failed to query vendor mappings for %s: %v", vendorName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query vendor mappings", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"mappings": mappings})
}
