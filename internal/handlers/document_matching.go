package handlers

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/services"
)

// DocumentMatchHandler handles document-to-transaction matching endpoints.
type DocumentMatchHandler struct {
	GormDB *gorm.DB
}

func (h *DocumentMatchHandler) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

// AutoMigrate adds document match columns to transactions table.
func (h *DocumentMatchHandler) AutoMigrate() {
	db := h.sqlDB()
	if db == nil {
		return
	}
	migrations := []string{
		`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS matched_document_id INTEGER`,
		`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS document_match_score INTEGER`,
		`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS document_match_status VARCHAR DEFAULT 'none'`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS is_financial BOOLEAN DEFAULT true`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS customer_name VARCHAR`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_folder_path VARCHAR`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_keywords JSONB`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_category VARCHAR`,
	}
	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			log.Printf("[DocumentMatchHandler] migration warning: %v", err)
		}
	}
}

// GetDocumentStatus handles GET /transactions/:id/document-status
func (h *DocumentMatchHandler) GetDocumentStatus(c *gin.Context) {
	txID := c.Param("id")

	var tx models.Transaction
	if err := h.GormDB.First(&tx, "id = ?", txID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "transaction not found"})
		return
	}

	status := "none"
	if tx.DocumentMatchStatus != nil {
		status = *tx.DocumentMatchStatus
	}

	result := gin.H{"status": status, "score": 0}

	if tx.MatchedDocumentID != nil && *tx.MatchedDocumentID > 0 {
		var doc models.Document
		if err := h.GormDB.First(&doc, *tx.MatchedDocumentID).Error; err == nil {
			result["document_id"] = doc.ID
			if doc.DocumentType != nil {
				result["document_type"] = *doc.DocumentType
			}
			if doc.VendorName != nil {
				result["vendor_name"] = *doc.VendorName
			}
			if doc.TotalAmount != nil {
				result["amount"] = *doc.TotalAmount
			}
			result["thumbnail_url"] = fmt.Sprintf("/documents/%d/file", doc.ID)
		}
		if tx.DocumentMatchScore != nil {
			result["score"] = *tx.DocumentMatchScore
		}
	}

	c.JSON(http.StatusOK, result)
}

// MatchDocument handles POST /transactions/:id/document-match
func (h *DocumentMatchHandler) MatchDocument(c *gin.Context) {
	h.SetDocumentMatch(c)
}

// SetDocumentMatch handles the match/unmatch/explicit_unmatch actions.
func (h *DocumentMatchHandler) SetDocumentMatch(c *gin.Context) {
	txID := c.Param("id")

	var req struct {
		DocumentID int    `json:"document_id"`
		Action     string `json:"action" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var tx models.Transaction
	if err := h.GormDB.First(&tx, "id = ?", txID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "transaction not found"})
		return
	}

	switch req.Action {
	case "match":
		score := 100
		status := "matched"
		h.GormDB.Model(&tx).Updates(map[string]interface{}{
			"matched_document_id":   req.DocumentID,
			"document_match_score":  score,
			"document_match_status": status,
		})
		// Also update document
		txnID := tx.ID
		h.GormDB.Model(&models.Document{}).Where("id = ?", req.DocumentID).Updates(map[string]interface{}{
			"matched_transaction_id": txnID,
			"status":                 "matched",
		})
		c.JSON(http.StatusOK, gin.H{"status": status, "score": score})

	case "unmatch":
		// Clear document link if it pointed to this transaction
		if tx.MatchedDocumentID != nil {
			h.GormDB.Model(&models.Document{}).Where("id = ? AND matched_transaction_id = ?", *tx.MatchedDocumentID, tx.ID).
				Updates(map[string]interface{}{"matched_transaction_id": nil})
		}
		h.GormDB.Model(&tx).Updates(map[string]interface{}{
			"matched_document_id":   nil,
			"document_match_score":  nil,
			"document_match_status": "none",
		})
		c.JSON(http.StatusOK, gin.H{"status": "none"})

	case "explicit_unmatch":
		h.GormDB.Model(&tx).Updates(map[string]interface{}{
			"document_match_status": "unmatched_explicit",
		})
		c.JSON(http.StatusOK, gin.H{"status": "unmatched_explicit"})

	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "action must be match, unmatch, or explicit_unmatch"})
	}
}

// UnmatchedBucket handles GET /documents/unmatched-bucket
func (h *DocumentMatchHandler) UnmatchedBucket(c *gin.Context) {
	// All unmatched financial documents
	var allUnmatched []models.Document
	h.GormDB.Where("(is_financial IS NULL OR is_financial = true)").
		Where("matched_transaction_id IS NULL").
		Where("status IN ('pending', 'unmatched', 'needs_review', 'auto_matched')").
		Order("created_at DESC").
		Limit(100).
		Find(&allUnmatched)

	// Build closest matches — for first 50, find best candidate transaction
	type closestMatch struct {
		Document    gin.H `json:"document"`
		Candidate   gin.H `json:"candidate_transaction"`
		Score       int   `json:"score"`
		Rule        string `json:"rule"`
	}
	var closestMatches []closestMatch

	limit := 50
	if len(allUnmatched) < limit {
		limit = len(allUnmatched)
	}

	for _, doc := range allUnmatched[:limit] {
		if doc.TotalAmount == nil || *doc.TotalAmount == 0 {
			continue
		}
		amt := *doc.TotalAmount
		vendorName := ""
		if doc.VendorName != nil {
			vendorName = *doc.VendorName
		}
		dateStr := ""
		if doc.DocumentDate != nil {
			dateStr = *doc.DocumentDate
		}

		// Find candidate transactions
		query := h.GormDB.Model(&models.Transaction{}).
			Where("(document_match_status IS NULL OR document_match_status IN ('none', ''))").
			Where("matched_document_id IS NULL").
			Where("ABS(amount - ?) < ? OR ABS(amount + ?) < ?",
				amt, amt*0.10+0.01, amt, amt*0.10+0.01)

		if dateStr != "" {
			if docDate, err := time.Parse("2006-01-02", dateStr); err == nil {
				from := docDate.AddDate(0, 0, -14).Format("2006-01-02")
				to := docDate.AddDate(0, 0, 14).Format("2006-01-02")
				query = query.Where("date >= ? AND date <= ?", from, to)
			}
		}

		var candidates []models.Transaction
		query.Limit(5).Find(&candidates)

		bestScore := 0
		var bestTx *models.Transaction
		for i := range candidates {
			s := scoreDocToTransaction(doc, candidates[i], dateStr, vendorName)
			if s > bestScore {
				bestScore = s
				bestTx = &candidates[i]
			}
		}

		if bestTx != nil && bestScore > 0 {
			closestMatches = append(closestMatches, closestMatch{
				Document: gin.H{
					"id":            doc.ID,
					"vendor_name":   doc.VendorName,
					"document_type": doc.DocumentType,
					"total_amount":  doc.TotalAmount,
					"document_date": doc.DocumentDate,
				},
				Candidate: gin.H{
					"id":     bestTx.ID,
					"name":   bestTx.Name,
					"amount": bestTx.Amount,
					"date":   bestTx.Date,
				},
				Score: bestScore,
				Rule:  "multi_factor",
			})
		}
	}

	// Build all_unmatched response
	var unmatchedResp []gin.H
	for _, doc := range allUnmatched {
		unmatchedResp = append(unmatchedResp, gin.H{
			"id":            doc.ID,
			"vendor_name":   doc.VendorName,
			"document_type": doc.DocumentType,
			"total_amount":  doc.TotalAmount,
			"document_date": doc.DocumentDate,
			"status":        doc.Status,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"closest_matches": closestMatches,
		"all_unmatched":   unmatchedResp,
	})
}

// scoreDocToTransaction computes a match score between a document and transaction.
// Used by the batch MatchDocumentsToTransactions job.
func scoreDocToTransaction(doc models.Document, tx models.Transaction, dateStr, vendorName string) int {
	score := 0

	// Amount scoring
	if doc.TotalAmount != nil && tx.Amount != nil {
		docAmt := *doc.TotalAmount
		txAmt := math.Abs(*tx.Amount)
		diff := math.Abs(docAmt - txAmt)

		if diff < 0.01 {
			score += 40 // Exact match
		} else if docAmt > 0 && diff < docAmt*0.01 {
			score += 35 // Within 1%
		} else if docAmt > 0 && diff < docAmt*0.05 {
			score += 25 // Within 5%
		}
	}

	// Vendor scoring
	if vendorName != "" {
		vnLower := strings.ToLower(vendorName)
		txName := ""
		if tx.Name != nil {
			txName = strings.ToLower(*tx.Name)
		}
		txMerchant := ""
		if tx.MerchantName != nil {
			txMerchant = strings.ToLower(*tx.MerchantName)
		}
		txVendor := ""
		if tx.Vendor != nil {
			txVendor = strings.ToLower(*tx.Vendor)
		}

		if txName == vnLower || txMerchant == vnLower || txVendor == vnLower {
			score += 35 // Exact vendor match
		} else if strings.Contains(txName, vnLower) || strings.Contains(txMerchant, vnLower) || strings.Contains(txVendor, vnLower) {
			score += 25 // Contains match
		} else {
			// Fuzzy check
			sim := services.BidirectionalSimilarity(vnLower, txName)
			if sim2 := services.BidirectionalSimilarity(vnLower, txMerchant); sim2 > sim {
				sim = sim2
			}
			if sim >= 0.80 {
				score += 25
			} else if sim >= 0.60 {
				score += 15
			}
		}
	}

	// Date scoring
	if dateStr != "" && tx.Date != nil {
		docDate, err1 := time.Parse("2006-01-02", dateStr)
		txDate, err2 := time.Parse("2006-01-02", *tx.Date)
		if err1 == nil && err2 == nil {
			daysDiff := math.Abs(docDate.Sub(txDate).Hours() / 24)
			if daysDiff <= 3 {
				score += 20
			} else if daysDiff <= 7 {
				score += 15
			} else if daysDiff <= 14 {
				score += 10
			}
		}
	}

	return score
}

// MatchDocumentsToTransactions is the background auto-match job.
// It runs after document upload and on Plaid sync.
func (h *DocumentMatchHandler) MatchDocumentsToTransactions() {
	log.Println("[DocMatch] Starting auto-match job")

	// Get unmatched financial documents
	var docs []models.Document
	h.GormDB.Where("(is_financial IS NULL OR is_financial = true)").
		Where("matched_transaction_id IS NULL").
		Where("status IN ('pending', 'unmatched', 'auto_matched', 'needs_review')").
		Where("total_amount IS NOT NULL AND total_amount > 0").
		Find(&docs)

	matched := 0
	suspect := 0

	for _, doc := range docs {
		amt := *doc.TotalAmount
		vendorName := ""
		if doc.VendorName != nil {
			vendorName = *doc.VendorName
		}
		dateStr := ""
		if doc.DocumentDate != nil {
			dateStr = *doc.DocumentDate
		}

		// Query candidate transactions
		query := h.GormDB.Model(&models.Transaction{}).
			Where("(document_match_status IS NULL OR document_match_status = 'none')").
			Where("ABS(amount - ?) < ? OR ABS(amount + ?) < ?",
				amt, amt*0.05+0.01, amt, amt*0.05+0.01)

		if dateStr != "" {
			if docDate, err := time.Parse("2006-01-02", dateStr); err == nil {
				from := docDate.AddDate(0, 0, -14).Format("2006-01-02")
				to := docDate.AddDate(0, 0, 14).Format("2006-01-02")
				query = query.Where("date >= ? AND date <= ?", from, to)
			}
		}

		if vendorName != "" {
			vLike := "%" + strings.ToLower(vendorName) + "%"
			query = query.Where("LOWER(name) LIKE ? OR LOWER(merchant_name) LIKE ? OR LOWER(vendor) LIKE ?",
				vLike, vLike, vLike)
		}

		var candidates []models.Transaction
		query.Limit(5).Find(&candidates)

		bestScore := 0
		var bestTx *models.Transaction
		for i := range candidates {
			s := scoreDocToTransaction(doc, candidates[i], dateStr, vendorName)
			if s > bestScore {
				bestScore = s
				bestTx = &candidates[i]
			}
		}

		if bestTx != nil && bestScore >= 40 {
			status := "suspect"
			if bestScore >= 75 {
				status = "matched"
				matched++
			} else {
				suspect++
			}

			h.GormDB.Model(bestTx).Updates(map[string]interface{}{
				"matched_document_id":   doc.ID,
				"document_match_score":  bestScore,
				"document_match_status": status,
			})

			if status == "matched" {
				h.GormDB.Model(&doc).Update("status", "auto_matched")
			}
		}
	}

	log.Printf("[DocMatch] Auto-match complete: %d matched, %d suspect out of %d documents", matched, suspect, len(docs))
}
